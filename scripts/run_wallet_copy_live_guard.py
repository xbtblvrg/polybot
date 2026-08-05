#!/usr/bin/env python3
"""Continuous operator-gated live runner for the primary wallet-copy candidate.

This guard keeps the paper proof state intact. Each cycle refreshes the current
runtime-admission wallet into a compact live runtime history state, then calls the
guarded live execution entrypoint. The live execution script still owns the
CopyIntent parity, token mapping, freshness, operator gate, and ledger dedupe.
"""

from __future__ import annotations

import argparse
import copy
import asyncio
import ctypes
from concurrent.futures import ThreadPoolExecutor
from collections import Counter, deque
import datetime as dt
import fcntl
import hashlib
import importlib.util
import inspect
import json
import math
import os
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.participation import (  # noqa: E402
    PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
    annotate_participation_item,
    annotate_participation_window,
    summarize_adjusted_participation,
)
from src.wallet_copy.gate_registry import LIVE_DROUGHT_FUNNEL_GATE_KEYS  # noqa: E402

AUTO_DEGRADE_ACTIVE_SET_STATE = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
WALLET_COPY_LIVE_EXECUTION_STATE = ROOT / "data/research/wallet_copy_live_execution_state.json"
WEEKEND_PROVEN_SEAT_STATE = ROOT / "data/research/weekend_proven_seat_admission_state.json"
ORDER135_FINGERPRINT_EVIDENCE = ROOT / "data/research/wide_policy_fingerprint_evidence_latest.json"
ORDER135_DEADMAN_STATE = ROOT / "data/research/order_flow_deadman_state.json"
ORDER135_FREEZE_SHADOW = ROOT / "data/research/frozen_fingerprint_f2_prewarm_backup_rank_shadow_latest.json"
ORDER135_SIDECAR_STATE = ROOT / "data/research/copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json"
ORDER135_GATE_LOG = "data/research/order135_direct_gate_passes.jsonl"
ORDER135_DIRECT_JOURNAL = ROOT / "data/research/wide_direct_handoff_journal.jsonl"
ORDER135_DIRECT_PACKET = ROOT / "data/research/wide_exact_policy_paper_state.json"
ORDER135_DIRECTION_ID = "2026-08-01T09:12:00Z"
ORDER135_PACKET_MAX_AGE_S = 30.0
ORDER135_DEADMAN_EVIDENCE_MAX_AGE_S = 900.0
ORDER135_EXPIRES_AT = dt.datetime(2026, 8, 2, tzinfo=dt.timezone.utc)
_ORDER135_DIRECT_PROJECTION_CACHE: dict[str, Any] = {}
STATE_DIGEST_JSON = Path("data/research/state_digest.json")
ACTIVE_SET_ROTATION_PACKET_STATE = ROOT / "data/research/active_set_rotation_packet_latest.json"
ACTIVE_SET_EXTERNAL_LIVENESS_STATE = ROOT / "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json"
MISSION_CONTRACT_PATH = ROOT / "src/wallet_copy/mission.py"
CONFIG_GENERATION_PATH = ROOT / "data/research/wallet_copy_config_generation.json"
LIVE_GUARD_GENERATION_FILES = (
    Path(__file__).resolve(),
    ROOT / "scripts/run_wallet_copy_live_execution.py",
    ROOT / "scripts/start_live_guard.sh",
    ROOT / "launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist",
    ROOT / "src/trade_executor.py",
    ROOT / "src/wallet_copy/execution.py",
    MISSION_CONTRACT_PATH,
    ROOT / "src/wallet_copy/pnl_truth.py",
)
WALLET_TEMPORAL_PROFITABILITY_STATE = ROOT / "data/research/wallet_temporal_profitability_latest.json"
SELECTION_PRIORITY_FREEZE_STATE = ROOT / "data/research/wallet_copy_selection_priority_freeze_state.json"
AUTO_DEGRADE_BAND_FILTER_GLOB = "data/research/active_set_expansion_next5_band_filtered_*.json"
AUTO_DEGRADE_MAX_ACTIVE_MEMBERS = 9
ACTIVE_SET_EXTERNAL_LIVENESS_MAX_AGE_H = 24.0
LATEST_ADMISSION_PRIORITY_MAX_LAG_S = 60.0
LAST_SUCCESSFUL_NONDENIED_MEMBER_MAX_AGE_S = 1800.0
ACTIVE_SET_SELECTION_PIN_MAX_AGE_S = 3600.0
ACTIVE_SET_SELECTION_PIN_REFRESH_AFTER_S = 1800.0
ACTIVE_SET_SELECTION_PRIORITY_FREEZE_MAX_AGE_S = 7200.0
TOTAL_LOSS_AUTO_DISABLE_MIN_RESOLVED_FILLS = 3
ALTERNATE_SOURCE_ATTRIBUTION_START_TS = dt.datetime(
    2026, 7, 24, 13, 22, tzinfo=dt.timezone.utc
).timestamp()
FABLE_1317_E4_TRIPWIRE_DIRECTION_FRAGMENT = "2026-07-11t13:17z-fable-e4-tripwire"
FABLE_1351_E4_HOLD_DIRECTION_ID = "2026-07-11T13:51Z-fable-hold-e4-not-crossed"
FABLE_1351_E4_HOLD_REVERT_STATUS = "REVERTED_BY_FABLE_1351_E4_NOT_CROSSED"
FABLE_1413_E4_RATIFIED_DIRECTION_ID = "2026-07-11T14:13Z-fable-e4-ratified-a95b-stands"
FABLE_1413_A95B_WALLET = "0xa95b27b0626973f7bc7600e65cd88cd7b42d0d14"
FABLE_1413_A95B_CAP_MAX_ORDER_USD = 1.0
FABLE_1413_A95B_CAP_RESTORE_VALUE = 2.0
FABLE_1413_A95B_CAP_RESTORE_CONDITION = "day_actual > -35 AND since_topup_headroom >= 5.0"
DEFAULT_WATCH_TIER_WALLETS_CONFIG = "configs/wallet_copy/watch_tier_wallets.json"
DEFAULT_WATCH_TIER_POLLER_STATE = "data/research/wallet_copy_watch_tier_poller_state.json"
DEFAULT_WATCH_TIER_HISTORY_STATE = "data/research/wallet_copy_watch_tier_history_state.json"
DEFAULT_WATCH_TIER_HISTORY_WINDOW_INDEX = "data/research/wallet_copy_watch_tier_history_window_index.json"
DEFAULT_WATCH_TIER_WALLET_EVENT_LOG = "data/research/wallet_copy_watch_tier_wallet_events.jsonl"
DEFAULT_WATCH_TIER_FIRST_SEEN = "data/research/watch_tier_first_seen.jsonl"
DEFAULT_WATCH_TIER_STAKEOUT_STATE = ROOT / "data/research/wallet_copy_watch_tier_stakeout_state.json"
DEFAULT_LIVE_GUARD_HOT_HISTORY_STATE = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_LIVE_GUARD_COPY_INTENTS_STATE = "data/research/wallet_copy_live_guard_copy_intents_state.json"
DEFAULT_LIVE_GUARD_HOT_HISTORY_WINDOW_INDEX = "data/research/wallet_copy_live_guard_hot_history_window_index.json"
DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_EVENTS = 8192
DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_COPY_INTENTS = 4096
DEFAULT_INTENT_TIME_COPYABILITY_PROOF_STATE = "data/research/wallet_copy_intent_time_copyability_proof_state.json"
DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_MAX_MEMBERS = 3
DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_SLOW_PATH_MEMBER_CAP = 6
PARTICIPATION_RETENTION_TARGET_HOURS = 25.0
PARTICIPATION_RETENTION_MAX_ROWS = 8192
PARTICIPATION_EVENT_ROLLUP_MAX_ROWS = 288
DEFAULT_ORDERFILLED_FAST_LANE_WAKE_SOCKET = "/tmp/polymarket_orderfilled_fast_lane.sock"
DEFAULT_ORDERFILLED_FAST_LANE_STATE = "data/research/wallet_copy_orderfilled_fast_lane_state.json"
DEFAULT_COPY_SOURCE_WAKE_ACTIVATION_STATE = (
    "data/research/copy_source_wake_activation_latest.json"
)
DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE = (
    "data/research/copy_source_identity_reconciliation_router_latest.json"
)
_ORDERFILLED_CURSOR_LOCK = threading.Lock()
_ORDERFILLED_FAST_LANE_CONSUMED_LOCK = threading.Lock()
_ORDERFILLED_FAST_LANE_CONSUMED_ORDER: deque[str] = deque(maxlen=20_000)
_ORDERFILLED_FAST_LANE_CONSUMED: set[str] = set()

from scripts.merge_rtds_wallet_events import (  # noqa: E402
    DEFAULT_COLD_TAIL_BYTES,
    run_multi_wallet_merge as _run_rtds_multi_wallet_merge,
    run_merge as _run_rtds_merge,
)
from scripts.merge_dataapi_active_set_events import (  # noqa: E402
    DEFAULT_STATE as DEFAULT_ACTIVE_SET_DATAAPI_POLLER_STATE,
    run_poll as _run_active_set_dataapi_poll,
)
from scripts.reconcile_self_wallet_feed import (  # noqa: E402
    DEFAULT_OUTPUT as DEFAULT_SELF_FEED_VS_LEDGER_STATE,
    DEFAULT_SELF_FEED_LOG,
    run_reconcile as _run_self_feed_reconcile,
)
from scripts.report_fill_quality import build_fill_quality_report  # noqa: E402
from scripts.report_routing_shadow_validation import (  # noqa: E402
    DEFAULT_MIN_VALIDATION_HOURS as DEFAULT_ROUTING_SHADOW_MIN_VALIDATION_HOURS,
    DEFAULT_OUTPUT as DEFAULT_ROUTING_SHADOW_VALIDATION_STATE,
    DEFAULT_RETAIN_ROWS as DEFAULT_ROUTING_SHADOW_RETAIN_ROWS,
    DEFAULT_SHADOW_CANDIDATE_SEATS as DEFAULT_ROUTING_SHADOW_CANDIDATE_SEATS,
    build_routing_shadow_validation_from_guard,
    load_shadow_candidate_seats,
)
from scripts.run_maker_first_btc5m_paper_lane import (  # noqa: E402
    build_quote_signals as _e5_build_quote_signals,
    load_recent_events as _e5_load_recent_events,
    maker_signal_to_intent as _e5_maker_signal_to_intent,
)
from scripts.run_btc5m_cross_exchange_probability_edge_paper_lane import (  # noqa: E402
    probability_signal_to_intent as _cross_exchange_probability_signal_to_intent,
)
from scripts.arbitrate_btc5m_promoted_cells import build_arbiter as _build_promoted_cell_arbiter  # noqa: E402
from scripts.run_wallet_copy_live_execution import (  # noqa: E402
    DEFAULT_PROMOTION_ROTATION_STATE,
    DEFAULT_RTDS_SIGNAL_WATERMARK_STATE,
    INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S,
    LIVE_BUILD_MAX_OBSERVED_AGE_S,
    PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S,
    PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S,
    _apply_inventory_best_ask_gate,
    _apply_live_hard_buy_price_cap,
    _apply_live_hard_buy_price_floor,
    _candidate_source_wallet,
    _candidate_runtime_freshness_summary,
    _drop_already_live_submitted_intents,
    _floor_live_min_order_intents,
    _fresh_intents,
    _intent_runtime_diagnostics,
    _intent_token_maps,
    _mission_active_member_candidate as _live_execution_mission_active_member_candidate,
    _policy_from_mission_member as _live_execution_policy_from_mission_member,
    _mission_runtime_candidate_policy as _live_execution_mission_runtime_candidate_policy,
    _parity_capsules,
    _promotion_rotation_runtime_candidate,
    _selected_candidate,
    _live_trade_executor,
    run_live_execution as _run_live_execution,
    submit_preplanned_live_execution as _submit_preplanned_live_execution,
)
from src.wallet_copy.execution import (  # noqa: E402
    CopyExecutionAdapter,
    ExecutionGate,
    LiveAdmissionSnapshot,
    LiveExecutionLedgerConfig,
    LiveWalletCopyLifecycle,
    promote_intent_for_live,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.mission import mission_contract  # noqa: E402
from src.wallet_copy.models import CopyIntent, WalletEvent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.profit_engine import CandidatePolicy, policy_accepts_event  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.own_positions import (  # noqa: E402
    POLYMARKET_CTF,
    split_position_calldata,
)
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.realtime_feed import (  # noqa: E402
    decode_polygon_orderfilled_v2,
    normalize_polygon_orderfilled_row,
)
from src.wallet_copy.source_route import (  # noqa: E402
    source_route_allows_live_execution,
    source_route_live_operator_approval,
    source_route_status,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.venue_executability import venue_gate_summary  # noqa: E402

_CROSS_EXCHANGE_LANE = "paper_struct_btc5m_cross_exchange_probability_edge_v1"
_CROSS_EXCHANGE_SOURCE = "BTC5M_CROSS_EXCHANGE_PROBABILITY_EDGE_V1"
_CROSS_EXCHANGE_MODEL_CHECKSUM = "848f22460923a497d0026973be9b1f6e2f43659ccdd1556979907a4ac9e98170"
_CROSS_EXCHANGE_ACTIVATION_ID = "2026-07-25T03:35:24Z-fable-cross-exchange-emergency-probe"
_CROSS_EXCHANGE_SILENT_WALLET = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"
_CROSS_EXCHANGE_MIN_DEADMAN_IDLE_S = 3600.0
_CROSS_EXCHANGE_ACTIVATION_TTL_S = 3600.0
_CROSS_EXCHANGE_ORDER_USD = 1.0
_CROSS_EXCHANGE_MIN_PRICE = 0.25
_CROSS_EXCHANGE_MAX_PRICE = 0.50
_WIDE_FAMILY_LANE = "wide_positive_slice_family"
_WIDE_FAMILY_SOURCE = "WIDE_POSITIVE_SLICE_FAMILY"
_WIDE_FAMILY_MAX_SIGNAL_AGE_S = 5.0

_MISSION_HOT_RELOAD_STATE: dict[str, Any] = {
    "mission_mtime_ns": None,
    "config_generation_mtime_ns": None,
    "reload_count": 0,
    "last_status": "UNINITIALIZED",
}


def _path_mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _selected_candidate_override_state_path(base_path: str | Path, candidate_id: str) -> str:
    path = Path(str(base_path or "data/research/wallet_copy_live_guard_selected_candidate_override.json"))
    safe_candidate = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(candidate_id or "unselected")).strip("._")
    if not safe_candidate:
        safe_candidate = "unselected"
    suffix = path.suffix or ".json"
    stem = path.stem if path.suffix else path.name
    return str(path.with_name(f"{stem}.{safe_candidate[:96]}{suffix}"))


def _mission_contract_schema_version() -> Any:
    try:
        contract = mission_contract()
    except Exception:
        return None
    return contract.get("schema_version") if isinstance(contract, dict) else None


def _load_mission_contract_function_from_path(path: Path):
    source = path.read_text(encoding="utf-8")
    compile(source, str(path), "exec")
    module_name = f"_wallet_copy_mission_live_reload_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot build mission contract spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fn = getattr(module, "mission_contract")
    contract = fn()
    if not isinstance(contract, dict):
        raise RuntimeError("mission_contract() did not return a dict")
    return fn, contract.get("schema_version")


def _mission_contract_hot_reload_check(*, cycle: int) -> dict[str, Any]:
    global mission_contract
    mission_mtime_ns = _path_mtime_ns(MISSION_CONTRACT_PATH)
    config_mtime_ns = _path_mtime_ns(CONFIG_GENERATION_PATH)
    previous_mission_mtime_ns = _MISSION_HOT_RELOAD_STATE.get("mission_mtime_ns")
    previous_config_mtime_ns = _MISSION_HOT_RELOAD_STATE.get("config_generation_mtime_ns")
    changed = (
        previous_mission_mtime_ns is not None
        and (mission_mtime_ns != previous_mission_mtime_ns or config_mtime_ns != previous_config_mtime_ns)
    )
    reload_count = int(_MISSION_HOT_RELOAD_STATE.get("reload_count") or 0)
    payload = {
        "flow_stage": "LIVE/SELF-DEV",
        "enabled": True,
        "cycle": cycle,
        "mission_path": _display_path(MISSION_CONTRACT_PATH),
        "mission_mtime_ns": mission_mtime_ns,
        "previous_mission_mtime_ns": previous_mission_mtime_ns,
        "config_generation_path": _display_path(CONFIG_GENERATION_PATH),
        "config_generation_mtime_ns": config_mtime_ns,
        "previous_config_generation_mtime_ns": previous_config_mtime_ns,
        "changed": bool(changed),
        "reloaded": False,
        "reload_count": reload_count,
        "schema_version": _mission_contract_schema_version(),
    }
    if previous_mission_mtime_ns is None:
        _MISSION_HOT_RELOAD_STATE.update(
            {
                "mission_mtime_ns": mission_mtime_ns,
                "config_generation_mtime_ns": config_mtime_ns,
                "last_status": "INITIALIZED",
            }
        )
        return {**payload, "status": "INITIALIZED", "rule": "watch mission.py/config_generation mtime each cycle"}
    if not changed:
        return {**payload, "status": "UNCHANGED", "rule": "watch mission.py/config_generation mtime each cycle"}
    old_fn = mission_contract
    try:
        new_fn, schema_version = _load_mission_contract_function_from_path(MISSION_CONTRACT_PATH)
    except Exception as exc:  # pragma: no cover - live guard must keep the previous good contract.
        mission_contract = old_fn
        _MISSION_HOT_RELOAD_STATE["last_status"] = "RELOAD_ERROR"
        return {
            **payload,
            "status": "RELOAD_ERROR",
            "error": f"{type(exc).__name__}: {exc}",
            "next_action": "repair mission.py syntax/contract; guard keeps previous good mission contract",
        }
    mission_contract = new_fn
    reload_count += 1
    _MISSION_HOT_RELOAD_STATE.update(
        {
            "mission_mtime_ns": mission_mtime_ns,
            "config_generation_mtime_ns": config_mtime_ns,
            "reload_count": reload_count,
            "last_status": "RELOADED",
        }
    )
    return {
        **payload,
        "status": "RELOADED",
        "reloaded": True,
        "reload_count": reload_count,
        "schema_version": schema_version,
        "rule": "mission/config change picked up without process restart",
    }


def _primary_live_candidate_contract() -> dict[str, Any]:
    runtime_phase = mission_contract().get("current_runtime_phase_contract")
    runtime_phase = runtime_phase if isinstance(runtime_phase, dict) else {}
    candidate = runtime_phase.get("primary_live_candidate")
    return candidate if isinstance(candidate, dict) else {}


def _member_wallet(member: dict[str, Any]) -> str:
    return str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()


def _weekend_bench_auto_return(member: dict[str, Any], *, now: dt.datetime | None = None) -> dict[str, Any]:
    status = str(member.get("status") or "").upper()
    bench = member.get("weekend_bench") if isinstance(member.get("weekend_bench"), dict) else {}
    if not bench and not status.startswith("WEEKEND_BENCHED"):
        return {}
    auto_return_at = _parse_iso_datetime(bench.get("auto_return_at") or member.get("auto_return_at"))
    if auto_return_at is None:
        return {"eligible": False, "reason": "auto_return_at_missing"}
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    if now_dt < auto_return_at:
        return {
            "eligible": False,
            "reason": "auto_return_not_due",
            "auto_return_at": auto_return_at.isoformat(),
        }
    if bench.get("weekday_seat_preserved") is not True and "WEEKDAY_SEAT_PRESERVED" not in status:
        return {"eligible": False, "reason": "weekday_seat_not_preserved"}
    wallet = _member_wallet(member)
    temporal = load_json(WALLET_TEMPORAL_PROFITABILITY_STATE, default={})
    rows = temporal.get("wallets") if isinstance(temporal, dict) and isinstance(temporal.get("wallets"), list) else []
    profile = next(
        (row for row in rows if isinstance(row, dict) and str(row.get("wallet") or "").strip().lower() == wallet),
        {},
    )
    if not profile:
        return {
            "eligible": False,
            "reason": "temporal_profile_missing",
            "auto_return_at": auto_return_at.isoformat(),
            "wallet": wallet,
        }
    regimes = profile.get("regime_profiles") if isinstance(profile.get("regime_profiles"), dict) else {}
    weekday = regimes.get("weekday") if isinstance(regimes.get("weekday"), dict) else {}
    try:
        weekday_pnl = float(weekday.get("pnl_usd") or 0.0)
    except (TypeError, ValueError):
        weekday_pnl = 0.0
    try:
        weekday_fills = int(weekday.get("resolved_trades") or 0)
    except (TypeError, ValueError):
        weekday_fills = 0
    if weekday_pnl <= 0.0 or weekday_fills <= 0:
        return {
            "eligible": False,
            "reason": "weekday_record_not_positive",
            "auto_return_at": auto_return_at.isoformat(),
            "wallet": wallet,
            "weekday_pnl_usd": round(weekday_pnl, 6),
            "weekday_resolved_trades": weekday_fills,
            "classification": profile.get("classification"),
        }
    return {
        "eligible": True,
        "reason": "weekday_record_positive_at_auto_return",
        "auto_return_at": auto_return_at.isoformat(),
        "wallet": wallet,
        "weekday_pnl_usd": round(weekday_pnl, 6),
        "weekday_resolved_trades": weekday_fills,
        "weekday_latest_event_ts": weekday.get("latest_event_ts"),
        "classification": profile.get("classification"),
        "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
    }


def _hour_band_bench_auto_return(member: dict[str, Any], *, now: dt.datetime | None = None) -> dict[str, Any]:
    status = str(member.get("status") or "").upper()
    bench = member.get("hour_band_bench") if isinstance(member.get("hour_band_bench"), dict) else {}
    if not bench and not status.startswith("HOUR_BENCHED"):
        return {}
    auto_return_at = _parse_iso_datetime(bench.get("auto_return_at") or member.get("auto_return_at"))
    if auto_return_at is None:
        return {"eligible": False, "reason": "hour_band_auto_return_at_missing"}
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    wallet = _member_wallet(member)
    if now_dt < auto_return_at:
        return {
            "eligible": False,
            "reason": "hour_band_auto_return_not_due",
            "auto_return_at": auto_return_at.isoformat(),
            "source_wallet": wallet,
            "classification": bench.get("classification"),
        }
    if bench.get("seat_preserved") is not True and "SEAT_PRESERVED" not in status:
        return {
            "eligible": False,
            "reason": "hour_band_seat_not_preserved",
            "auto_return_at": auto_return_at.isoformat(),
            "source_wallet": wallet,
            "classification": bench.get("classification"),
        }
    return {
        "eligible": True,
        "reason": "hour_band_auto_return_due_seat_preserved",
        "auto_return_at": auto_return_at.isoformat(),
        "source_wallet": wallet,
        "classification": bench.get("classification"),
        "fable_direction_id": bench.get("fable_direction_id"),
        "state": str(AUTO_DEGRADE_ACTIVE_SET_STATE),
    }


def _temporal_profiles_by_wallet() -> dict[str, dict[str, Any]]:
    temporal = load_json(WALLET_TEMPORAL_PROFITABILITY_STATE, default={})
    rows = temporal.get("wallets") if isinstance(temporal, dict) and isinstance(temporal.get("wallets"), list) else []
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            profiles[wallet] = row
    return profiles


def _temporal_active_slice_names(now: dt.datetime | None = None) -> list[str]:
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    slices = ["weekend" if now_dt.weekday() >= 5 else "weekday"]
    if 18 <= now_dt.hour < 22:
        slices.append("dead_band_18_22_utc")
    return slices


def _coverage_hunger_activation_hours(member: dict[str, Any]) -> list[int]:
    activation = member.get("coverage_hunger_activation")
    activation = activation if isinstance(activation, dict) else {}
    raw_hours = activation.get("activation_hours_utc") or member.get("activation_hours_utc") or []
    if not isinstance(raw_hours, list):
        return []
    hours: list[int] = []
    for raw in raw_hours:
        if isinstance(raw, bool):
            continue
        try:
            hour = int(raw)
        except (TypeError, ValueError):
            continue
        if 0 <= hour <= 23 and hour not in hours:
            hours.append(hour)
    return hours


def _band_scoped_admission_override(member: dict[str, Any]) -> dict[str, Any]:
    """Validate an OP-TOMORROW positive-band include-list fail closed."""

    admission = (
        member.get("band_scoped_admission")
        if isinstance(member.get("band_scoped_admission"), dict)
        else {}
    )
    if str(admission.get("status") or "").upper() != "ACTIVE":
        return {"valid": False, "reason": "band_scoped_admission_inactive"}
    if str(admission.get("direction_id") or "") != "2026-07-28T07:55Z-OP-TOMORROW":
        return {"valid": False, "reason": "band_scoped_admission_direction_mismatch"}
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    policy_bands = sorted(
        {
            str(value)
            for value in policy.get("move_slice_keys") or []
            if str(value)
        }
    )
    evidence_rows = [
        row
        for row in admission.get("admitted_bands") or []
        if isinstance(row, dict)
    ]
    evidence_bands = sorted(
        {
            str(row.get("move_slice_key") or "")
            for row in evidence_rows
            if str(row.get("move_slice_key") or "")
        }
    )
    if not policy_bands or policy_bands != evidence_bands:
        return {"valid": False, "reason": "band_scoped_policy_evidence_mismatch"}
    failed_bands: list[dict[str, Any]] = []
    for row in evidence_rows:
        summary = venue_gate_summary(row)
        passed = bool(
            summary.get("f1_pass") is True
            and int(summary.get("resolved") or 0) >= 200
            and _float_or_default(summary.get("post_fee_pnl_usd"), 0.0) > 0.0
            and _float_or_default(summary.get("roi_pct"), 0.0) > 0.0
            and _float_or_default(
                summary.get("first_half_post_fee_pnl_usd"), 0.0
            )
            > 0.0
            and _float_or_default(
                summary.get("second_half_post_fee_pnl_usd"), 0.0
            )
            > 0.0
        )
        if not passed:
            failed_bands.append(
                {
                    "move_slice_key": row.get("move_slice_key"),
                    "venue_executable_full_stream_rescore": summary,
                }
            )
    if failed_bands:
        authority_missing = any(
            (row.get("venue_executable_full_stream_rescore") or {}).get("reason")
            == "venue_authority_missing_stale_artifact"
            for row in failed_bands
        )
        return {
            "valid": False,
            "reason": (
                "venue_authority_missing_stale_artifact"
                if authority_missing
                else "band_scoped_evidence_gate_failed"
            ),
            "failed_bands": failed_bands,
        }
    return {
        "valid": True,
        "reason": "op_tomorrow_positive_band_include_list",
        "direction_id": admission.get("direction_id"),
        "move_slice_keys": policy_bands,
        "evidence_artifact": admission.get("evidence_artifact"),
        "wide_policy_fingerprint": admission.get("wide_policy_fingerprint"),
        "loss_line_usd": admission.get("per_member_loss_line_usd"),
        "evidence_standard": (
            "each included band: resolved>=200, aggregate PnL/ROI>0, "
            "and both chronological halves>0"
        ),
    }


def _cell_scoped_admission_override(member: dict[str, Any]) -> dict[str, Any]:
    """Validate a T2 exact-fingerprint cell admission fail closed."""

    admission = (
        member.get("cell_scoped_admission")
        if isinstance(member.get("cell_scoped_admission"), dict)
        else {}
    )
    if str(admission.get("status") or "").upper() != "ACTIVE":
        return {"valid": False, "reason": "cell_scoped_admission_inactive"}
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    policy_slices = sorted(
        {str(value) for value in policy.get("move_slice_keys") or [] if str(value)}
    )
    evidence_slices = sorted(
        {
            str(value)
            for value in admission.get("move_slice_keys") or []
            if str(value)
        }
    )
    summary = venue_gate_summary(admission)
    valid = bool(
        policy_slices
        and policy_slices == evidence_slices
        and summary.get("f1_pass") is True
        and int(summary.get("resolved") or 0) >= 200
        and _float_or_default(summary.get("post_fee_pnl_usd"), 0.0) > 0.0
        and _float_or_default(summary.get("roi_pct"), 0.0) > 0.0
        and _float_or_default(summary.get("first_half_post_fee_pnl_usd"), 0.0)
        > 0.0
        and _float_or_default(summary.get("second_half_post_fee_pnl_usd"), 0.0)
        > 0.0
    )
    if not valid:
        return {
            "valid": False,
            "reason": (
                "venue_authority_missing_stale_artifact"
                if summary.get("reason")
                == "venue_authority_missing_stale_artifact"
                else "cell_scoped_evidence_gate_failed"
            ),
            "policy_move_slice_keys": policy_slices,
            "evidence_move_slice_keys": evidence_slices,
            "venue_executable_full_stream_rescore": summary,
        }
    return {
        "valid": True,
        "reason": "op_tomorrow_exact_fingerprint_cell",
        "direction_id": admission.get("direction_id"),
        "move_slice_keys": policy_slices,
        "evidence_artifact": admission.get("evidence_artifact"),
        "wide_policy_fingerprint": admission.get("wide_policy_fingerprint"),
        "loss_line_usd": admission.get("per_cell_loss_line_usd"),
        "first_slice_kill": admission.get("first_slice_kill"),
        "evidence_standard": (
            "exact fingerprint cell: resolved>=200, aggregate PnL/ROI>0, "
            "and both chronological halves>0"
        ),
    }


def _temporal_slice_exclusion(
    member: dict[str, Any],
    *,
    profiles: dict[str, dict[str, Any]] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    wallet = _member_wallet(member)
    active_slices = _temporal_active_slice_names(now_dt)
    overlay = _load_auto_degrade_active_set_overlay()
    selection_pin = (
        overlay.get("selection_pin")
        if isinstance(overlay.get("selection_pin"), dict)
        else {}
    )
    pin_expires_at = _parse_iso_datetime(selection_pin.get("expires_at"))
    midnight_hold_pin_active = (
        member.get("operator_emergency_seat") is True
        and str(member.get("status") or "")
        == "POLICY_CHOKE_RUNG_DIRECT_EMERGENCY_ADMISSION"
        and selection_pin.get("enabled") is True
        and str(selection_pin.get("source_wallet") or "").strip().lower() == wallet
        and str(selection_pin.get("candidate_id") or "")
        == str(member.get("candidate_id") or "")
        and str(selection_pin.get("direction_id") or "")
        == "2026-08-03T22:51Z-fable-midnight-bind-hold-4462"
        and pin_expires_at is not None
        and now_dt <= pin_expires_at
    )
    if midnight_hold_pin_active:
        return {
            "excluded": False,
            "reason": "fable_midnight_hold_emergency_selection_pin",
            "source_wallet": wallet,
            "classification": "FABLE-BOUNDED-EMERGENCY-HOLD",
            "active_slices": active_slices,
            "evaluated_slices": [],
            "selection_pin": {
                "candidate_id": selection_pin.get("candidate_id"),
                "direction_id": selection_pin.get("direction_id"),
                "expires_at": selection_pin.get("expires_at"),
                "pin_id": selection_pin.get("pin_id"),
            },
            "state": str(AUTO_DEGRADE_ACTIVE_SET_STATE),
        }
    profiles = profiles if profiles is not None else _temporal_profiles_by_wallet()
    profile = profiles.get(wallet) if wallet else None
    cell_scoped_override = _cell_scoped_admission_override(member)
    band_scoped_override = _band_scoped_admission_override(member)
    scoped_override_valid = (
        cell_scoped_override.get("valid") is True
        or band_scoped_override.get("valid") is True
    )
    if scoped_override_valid and isinstance(profile, dict):
        labels = (
            profile.get("venue_slice_labels")
            if isinstance(profile.get("venue_slice_labels"), dict)
            else profile.get("slice_labels")
            if isinstance(profile.get("slice_labels"), dict)
            else {}
        )
        proven_negative_rows: list[dict[str, Any]] = []
        for slice_name in active_slices:
            label_row = labels.get(slice_name) if isinstance(labels.get(slice_name), dict) else {}
            if str(label_row.get("label") or "UNPROVEN").upper() != "PROVEN-NEGATIVE":
                continue
            proven_negative_rows.append(
                {
                    "slice": slice_name,
                    "label": "PROVEN-NEGATIVE",
                    "resolved_trades": label_row.get("resolved_trades"),
                    "roi_pct": label_row.get("roi_pct"),
                    "pnl_usd": label_row.get("pnl_usd"),
                    "label_reason": label_row.get("reason"),
                }
            )
        if proven_negative_rows:
            matched = proven_negative_rows[0]
            return {
                "excluded": True,
                "reason": f"incumbent_would_fail_readmission_{matched['slice']}_proven_negative",
                "source_wallet": wallet,
                "classification": profile.get("classification"),
                "active_slices": active_slices,
                "matched_slice": matched,
                "matched_slices": proven_negative_rows,
                "evaluated_slices": proven_negative_rows,
                "readmission_check": "active_temporal_not_proven_negative=false",
                "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
            }
    if cell_scoped_override.get("valid") is True:
        return {
            "excluded": False,
            "reason": "cell_scoped_positive_include_list",
            "source_wallet": wallet,
            "classification": "MEASURED-POSITIVE-EXACT-CELL",
            "active_slices": active_slices,
            "evaluated_slices": [],
            "cell_scoped_admission": cell_scoped_override,
            "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
        }
    if band_scoped_override.get("valid") is True:
        return {
            "excluded": False,
            "reason": "band_scoped_positive_include_list",
            "source_wallet": wallet,
            "classification": "MEASURED-POSITIVE-BANDS-ONLY",
            "active_slices": active_slices,
            "evaluated_slices": [],
            "band_scoped_admission": band_scoped_override,
            "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
        }
    activation_hours = _coverage_hunger_activation_hours(member)
    if activation_hours and now_dt.hour not in activation_hours:
        return {
            "excluded": True,
            "reason": "coverage_hunger_activation_hour_inactive",
            "source_wallet": wallet,
            "classification": "COVERAGE-HUNGER-HOUR-GATED",
            "active_slices": active_slices,
            "activation_hours_utc": activation_hours,
            "matched_slice": {
                "slice": "utc_hour",
                "label": "INACTIVE",
                "hour": now_dt.hour,
            },
            "evaluated_slices": [
                {
                    "slice": "utc_hour",
                    "label": "INACTIVE",
                    "hour": now_dt.hour,
                    "activation_hours_utc": activation_hours,
                }
            ],
            "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
        }
    if not isinstance(profile, dict) or not profile:
        if activation_hours:
            return {
                "excluded": False,
                "reason": "coverage_hunger_alive_hour_probe",
                "source_wallet": wallet,
                "classification": "COVERAGE-HUNGER-ALIVE-HOUR",
                "active_slices": active_slices,
                "activation_hours_utc": activation_hours,
                "evaluated_slices": [
                    {
                        "slice": "utc_hour",
                        "label": "PROVEN-POSITIVE",
                        "hour": now_dt.hour,
                        "label_reason": "explicit Fable coverage-hunger activation limited to recorded alive hour",
                    }
                ],
                "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
            }
        return {
            "excluded": False,
            "reason": "temporal_profile_missing_or_unproven",
            "source_wallet": wallet,
            "active_slices": active_slices,
            "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
        }
    labels = (
        profile.get("venue_slice_labels")
        if isinstance(profile.get("venue_slice_labels"), dict)
        else profile.get("slice_labels")
        if isinstance(profile.get("slice_labels"), dict)
        else {}
    )
    evaluated: list[dict[str, Any]] = []
    proven_negative_rows: list[dict[str, Any]] = []
    for slice_name in active_slices:
        label_row = labels.get(slice_name) if isinstance(labels.get(slice_name), dict) else {}
        label = str(label_row.get("label") or "UNPROVEN").upper()
        row = {
            "slice": slice_name,
            "label": label,
            "resolved_trades": label_row.get("resolved_trades"),
            "roi_pct": label_row.get("roi_pct"),
            "pnl_usd": label_row.get("pnl_usd"),
            "label_reason": label_row.get("reason"),
        }
        evaluated.append(row)
        if label == "PROVEN-NEGATIVE":
            proven_negative_rows.append(row)
    if proven_negative_rows:
        matched = proven_negative_rows[0]
        return {
            "excluded": True,
            "reason": f"temporal_slice_{matched['slice']}_proven_negative",
            "source_wallet": wallet,
            "classification": profile.get("classification"),
            "active_slices": active_slices,
            "matched_slice": matched,
            "matched_slices": proven_negative_rows,
            "evaluated_slices": evaluated,
            "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
        }
    return {
        "excluded": False,
        "reason": "no_active_temporal_slice_proven_negative",
        "source_wallet": wallet,
        "classification": profile.get("classification"),
        "active_slices": active_slices,
        "evaluated_slices": evaluated,
        "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
    }


def _apply_temporal_slice_exclusions(
    members: list[dict[str, Any]],
    *,
    profiles: dict[str, dict[str, Any]] | None = None,
    now: dt.datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    profiles = profiles if profiles is not None else _temporal_profiles_by_wallet()
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    active_slices = _temporal_active_slice_names(now_dt)
    for member in members:
        decision = _temporal_slice_exclusion(member, profiles=profiles, now=now_dt)
        if decision.get("excluded") is True:
            excluded.append(
                {
                    "candidate_id": str(member.get("candidate_id") or ""),
                    "source_wallet": _member_wallet(member),
                    "policy_id": str(member.get("policy_id") or ""),
                    "reason": decision.get("reason"),
                    "classification": decision.get("classification"),
                    "matched_slice": decision.get("matched_slice"),
                    "matched_slices": decision.get("matched_slices"),
                    "evaluated_slices": decision.get("evaluated_slices"),
                }
            )
            continue
        member = dict(member)
        member["temporal_slice_evaluation"] = decision
        included.append(member)
    return included, {
        "enabled": True,
        "flow_stage": "LIVE/LEARN/ROTATE",
        "rule": "exclude active-set member from live copy when an active temporal slice is PROVEN-NEGATIVE; UNPROVEN does not exclude",
        "as_of": now_dt.isoformat(),
        "active_slices": active_slices,
        "state": str(WALLET_TEMPORAL_PROFITABILITY_STATE),
        "excluded_count": len(excluded),
        "included_count": len(included),
        "excluded_wallets": [row["source_wallet"] for row in excluded],
        "excluded_members": excluded,
    }


def _external_liveness_state() -> tuple[dict[str, Any], Path]:
    state = load_json(ACTIVE_SET_EXTERNAL_LIVENESS_STATE, default={})
    return state if isinstance(state, dict) else {}, ACTIVE_SET_EXTERNAL_LIVENESS_STATE


def _external_liveness_rows_by_wallet(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows_by_wallet: dict[str, dict[str, Any]] = {}
    candidates: list[Any] = []
    for key in ("rows", "selected_rows"):
        values = state.get(key)
        if isinstance(values, list):
            candidates.extend(values)
    remote = state.get("remote_dataapi_24h") if isinstance(state.get("remote_dataapi_24h"), dict) else {}
    if isinstance(remote.get("rows"), list):
        candidates.extend(remote["rows"])
    if isinstance(state.get("paper_shadow_enrollments"), list):
        candidates.extend(state["paper_shadow_enrollments"])

    def row_rank(row: dict[str, Any]) -> tuple[int, int, float]:
        try:
            trade_ts = float(row.get("latest_btc5m_trade_ts") or row.get("latest_trade_ts") or 0.0)
        except (TypeError, ValueError):
            trade_ts = 0.0
        has_trade_ts = 1 if trade_ts > 0 else 0
        selected = 1 if row.get("selected_this_run") is True else 0
        try:
            fetched_at_s = float(row.get("fetched_at_s") or 0.0)
        except (TypeError, ValueError):
            fetched_at_s = 0.0
        if row.get("checkpoint_carryover") is True:
            selected -= 1
        return has_trade_ts, selected, fetched_at_s

    for row in candidates:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or row.get("source_wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            existing = rows_by_wallet.get(wallet)
            if existing is None or row_rank(row) >= row_rank(existing):
                rows_by_wallet[wallet] = row
    return rows_by_wallet


def _external_liveness_trade_age_h(
    row: dict[str, Any],
    *,
    now: dt.datetime,
    state_generated_at: dt.datetime | None = None,
) -> tuple[float | None, str]:
    for key in ("latest_btc5m_trade_ts", "latest_trade_ts"):
        try:
            trade_ts = float(row.get(key) or 0.0)
        except (TypeError, ValueError):
            trade_ts = 0.0
        if trade_ts > 0:
            return max(0.0, (now.timestamp() - trade_ts) / 3600.0), f"computed_from_{key}"
    for key in ("latest_trade_age_h", "remote_dataapi_latest_trade_age_h"):
        try:
            age_h = float(row.get(key))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(age_h) or age_h < 0:
            continue
        fetched_at = _parse_iso_datetime(row.get("fetched_at"))
        evidence_at = fetched_at or state_generated_at
        if evidence_at is not None:
            age_h += max(0.0, (now - evidence_at).total_seconds() / 3600.0)
            suffix = "row_age" if fetched_at is not None else "state_age_fallback"
            return age_h, f"{key}_plus_{suffix}"
        return age_h, key
    return None, "missing_latest_btc5m_trade"


def _external_liveness_gate_for_wallet(
    wallet: str,
    *,
    rows_by_wallet: dict[str, dict[str, Any]] | None = None,
    state: dict[str, Any] | None = None,
    state_path: Path | None = None,
    now: dt.datetime | None = None,
    max_age_h: float = ACTIVE_SET_EXTERNAL_LIVENESS_MAX_AGE_H,
) -> dict[str, Any]:
    wallet = str(wallet or "").strip().lower()
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    loaded_state = state if isinstance(state, dict) else None
    loaded_path = state_path
    if loaded_state is None:
        loaded_state, loaded_path = _external_liveness_state()
    loaded_path = loaded_path or ACTIVE_SET_EXTERNAL_LIVENESS_STATE
    rows = rows_by_wallet if rows_by_wallet is not None else _external_liveness_rows_by_wallet(loaded_state)
    generated_at = _parse_iso_datetime(loaded_state.get("generated_at")) if isinstance(loaded_state, dict) else None
    row = rows.get(wallet) if wallet else None
    if not wallet:
        return {
            "passed": False,
            "reason": "external_liveness_wallet_missing",
            "state": str(loaded_path),
            "max_age_h": max_age_h,
        }
    if not isinstance(row, dict):
        return {
            "passed": False,
            "reason": "external_liveness_row_missing",
            "source_wallet": wallet,
            "state": str(loaded_path),
            "generated_at": loaded_state.get("generated_at") if isinstance(loaded_state, dict) else None,
            "max_age_h": max_age_h,
        }
    status = str(row.get("status") or "").upper()
    censored = bool(row.get("censored")) or status == "CENSORED_PAGINATION_CAP"
    age_h, age_source = _external_liveness_trade_age_h(row, now=now_dt, state_generated_at=generated_at)
    try:
        btc5m_trades = int(
            row.get("btc5m_trades_24h")
            or row.get("remote_dataapi_btc5m_trades_24h")
            or row.get("btc5m_buys_24h")
            or row.get("remote_dataapi_btc5m_buys_24h")
            or 0
        )
    except (TypeError, ValueError):
        btc5m_trades = 0
    reason = "external_liveness_pass"
    passed = True
    if status == "ERROR":
        passed = False
        reason = "external_liveness_error"
    elif censored:
        passed = False
        reason = "external_liveness_censored"
    elif age_h is None:
        passed = False
        reason = "external_liveness_no_btc5m_trade_ts"
    elif age_h >= float(max_age_h):
        passed = False
        reason = "external_liveness_age_gte_24h"
    elif btc5m_trades <= 0:
        passed = False
        reason = "external_liveness_zero_btc5m_trades_24h"
    return {
        "passed": passed,
        "reason": reason,
        "source_wallet": wallet,
        "status": status or None,
        "censored": row.get("censored") or "",
        "fetched_at": row.get("fetched_at"),
        "latest_trade_age_h": None if age_h is None else round(age_h, 6),
        "latest_trade_age_source": age_source,
        "latest_btc5m_trade_ts": row.get("latest_btc5m_trade_ts") or row.get("latest_trade_ts"),
        "btc5m_trades_24h": btc5m_trades,
        "btc5m_buys_24h": row.get("btc5m_buys_24h") or row.get("remote_dataapi_btc5m_buys_24h") or 0,
        "max_age_h": max_age_h,
        "state": str(loaded_path),
        "generated_at": loaded_state.get("generated_at") if isinstance(loaded_state, dict) else None,
        "rule": "Fable 2026-07-14T11:34Z: no active/enabled wallet path without external BTC-5m last trade age <24h",
    }


def _f418_readmission_liveness_exemption(member: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(member, dict):
        return {}
    wallet = _member_wallet(member)
    if wallet != _F418_PROBE_WALLET:
        return {}
    activation = member.get("f418_readmission_activation")
    if not isinstance(activation, dict):
        return {}
    try:
        resolved = int(activation.get("measurable_resolved_intents") or 0)
    except (TypeError, ValueError):
        resolved = 0
    try:
        post_fee_pnl = float(activation.get("post_fee_pnl_usd") or 0.0)
    except (TypeError, ValueError):
        post_fee_pnl = 0.0
    packet_status = str(activation.get("packet_status") or "").upper()
    basis = str(activation.get("counterfactual_basis") or "")
    if (
        packet_status != "PRE_RULED_ADMIT_F418_ACTIVATION"
        or basis != "routing_shadow_member_attribution"
        or resolved < 10
        or post_fee_pnl <= 0.0
    ):
        return {}
    return {
        "enabled": True,
        "flow_stage": "PROMOTE/LIVE/DEFEND",
        "direction_id": activation.get("direction_id") or "2026-07-16T05:28Z-fable-f418-activation-branch-a",
        "packet_status": packet_status,
        "counterfactual_basis": basis,
        "measurable_resolved_intents": resolved,
        "post_fee_pnl_usd": round(post_fee_pnl, 6),
        "basis_packet": activation.get("basis_packet"),
        "auto_demote_rule": activation.get("auto_demote_rule"),
        "rule": "newer Fable f418 routing-shadow readmission evidence supersedes level-only external source quiet until the post-activation tripwire resolves",
    }


def _apply_f418_readmission_liveness_exemption(
    decision: dict[str, Any],
    member: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(decision, dict) or decision.get("passed") is True:
        return decision
    exemption = _f418_readmission_liveness_exemption(member)
    if not exemption:
        return decision
    updated = dict(decision)
    updated["passed"] = True
    updated["original_passed"] = decision.get("passed")
    updated["original_reason"] = decision.get("reason")
    updated["reason"] = "external_liveness_exempt_f418_routing_shadow_readmission"
    updated["f418_readmission_liveness_exemption"] = exemption
    updated["rule"] = exemption["rule"]
    return updated


def _apply_operator_emergency_seat_liveness_exemption(
    decision: dict[str, Any],
    member: dict[str, Any],
    *,
    now: dt.datetime,
) -> dict[str, Any]:
    """Prefer fresh direct-source proof over a stale remote liveness cache.

    The exception is valid only for the operator's bounded empty-seat recovery:
    a $1 member, active one-hour pin, explicit kill line, and a direct receipt
    no older than the 90-minute Rung-A measurement window.
    """
    if decision.get("passed") is True or member.get("operator_emergency_seat") is not True:
        return decision
    evidence = (
        member.get("source_liveness_evidence")
        if isinstance(member.get("source_liveness_evidence"), dict)
        else {}
    )
    observed_at = _parse_iso_datetime(evidence.get("latest_receipt_at"))
    kill_line = member.get("kill_line") if isinstance(member.get("kill_line"), dict) else {}
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    selected_size_exception = (
        member.get("selected_only_size_exception")
        if isinstance(member.get("selected_only_size_exception"), dict)
        else {}
    )
    try:
        cap = float(member.get("max_order_usd") or policy.get("max_order_usd") or 0.0)
    except (TypeError, ValueError):
        cap = 0.0
    age_s = (now - observed_at).total_seconds() if observed_at is not None else None
    if (
        observed_at is None
        or age_s is None
        or not 0.0 <= age_s <= 5400.0
        or not (
            0.0 < cap <= 1.0
            or (
                0.0 < cap <= 2.5
                and float(selected_size_exception.get("max_order_usd") or 0.0) == cap
                and selected_size_exception.get("authority") == "fable_2026-08-03T08:14Z_D1c"
                and selected_size_exception.get("reason") == "market_buy_precision_infeasible_at_1usd"
            )
        )
        or float(kill_line.get("post_fee_pnl_usd_lte") or 0.0) != -4.0
        or kill_line.get("active_temporal_proven_negative") is not True
    ):
        return decision
    updated = dict(decision)
    updated.update(
        {
            "passed": True,
            "original_passed": decision.get("passed"),
            "original_reason": decision.get("reason"),
            "reason": "operator_emergency_seat_direct_source_liveness",
            "operator_emergency_seat_liveness_exemption": {
                "authority": "2026-08-01T12:40Z-operator-absolute-seat-fill-preemption",
                "latest_receipt_at": observed_at.isoformat(),
                "receipt_age_s": round(age_s, 6),
                "max_receipt_age_s": 5400.0,
                "max_order_usd": cap,
                "selected_only_size_exception": selected_size_exception or None,
                "kill_line": kill_line,
            },
        }
    )
    return updated


def _apply_external_liveness_sweep(
    members: list[dict[str, Any]],
    *,
    now: dt.datetime | None = None,
    scope: str = "active_set",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    state, state_path = _external_liveness_state()
    rows_by_wallet = _external_liveness_rows_by_wallet(state)
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    output: list[dict[str, Any]] = []
    disabled: list[dict[str, Any]] = []
    exempted: list[dict[str, Any]] = []
    passed = 0
    pre_disabled = 0
    for member in members:
        if not isinstance(member, dict):
            continue
        updated = dict(member)
        wallet = _member_wallet(updated)
        if _active_set_member_is_disabled(updated, now=now_dt):
            pre_disabled += 1
            output.append(updated)
            continue
        decision = _external_liveness_gate_for_wallet(
            wallet,
            rows_by_wallet=rows_by_wallet,
            state=state,
            state_path=state_path,
            now=now_dt,
        )
        decision = _apply_f418_readmission_liveness_exemption(decision, updated)
        decision = _apply_operator_emergency_seat_liveness_exemption(
            decision, updated, now=now_dt
        )
        updated["external_liveness_gate"] = decision
        if decision.get("passed") is True:
            exemption = decision.get("f418_readmission_liveness_exemption")
            operator_exemption = decision.get(
                "operator_emergency_seat_liveness_exemption"
            )
            if isinstance(exemption, dict):
                updated["external_liveness_exemption"] = exemption
                exempted.append(
                    {
                        "candidate_id": str(updated.get("candidate_id") or ""),
                        "source_wallet": wallet,
                        "original_reason": decision.get("original_reason"),
                        "latest_trade_age_h": decision.get("latest_trade_age_h"),
                        "packet_status": exemption.get("packet_status"),
                        "post_fee_pnl_usd": exemption.get("post_fee_pnl_usd"),
                    }
                )
            elif isinstance(operator_exemption, dict):
                updated["external_liveness_exemption"] = operator_exemption
                exempted.append(
                    {
                        "candidate_id": str(updated.get("candidate_id") or ""),
                        "source_wallet": wallet,
                        "original_reason": decision.get("original_reason"),
                        "latest_trade_age_h": decision.get("latest_trade_age_h"),
                        "packet_status": "OPERATOR_EMERGENCY_SEAT_DIRECT_SOURCE",
                        "post_fee_pnl_usd": None,
                    }
                )
            passed += 1
            output.append(updated)
            continue
        prior_status = str(updated.get("status") or "")
        updated["enabled"] = False
        updated["status"] = "AUTO_DISABLED_EXTERNAL_LIVENESS_FABLE_20260714T1134"
        updated["external_liveness_disabled"] = {
            "flow_stage": "LIVE/DEFEND/PROMOTE",
            "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
            "prior_status": prior_status,
            "reason": decision.get("reason"),
            "next_action": "refresh external Data API liveness and re-enable only after BTC-5m last trade age <24h",
        }
        disabled.append(
            {
                "candidate_id": str(updated.get("candidate_id") or ""),
                "source_wallet": wallet,
                "prior_status": prior_status,
                "reason": decision.get("reason"),
                "latest_trade_age_h": decision.get("latest_trade_age_h"),
                "status": decision.get("status"),
                "censored": decision.get("censored"),
            }
        )
        output.append(updated)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE/DEFEND/PROMOTE",
        "scope": scope,
        "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
        "rule": "NO wallet may be enabled/activated anywhere unless external BTC-5m last-trade age is <24h",
        "as_of": now_dt.isoformat(),
        "state": str(state_path),
        "generated_at": state.get("generated_at"),
        "row_count": len(rows_by_wallet),
        "max_age_h": ACTIVE_SET_EXTERNAL_LIVENESS_MAX_AGE_H,
        "input_members": len([member for member in members if isinstance(member, dict)]),
        "pre_disabled_count": pre_disabled,
        "passed_count": passed,
        "exempted_count": len(exempted),
        "exempted_wallets": [row["source_wallet"] for row in exempted],
        "exempted_members": exempted,
        "disabled_count": len(disabled),
        "disabled_wallets": [row["source_wallet"] for row in disabled],
        "disabled_members": disabled,
    }


def _clamp_a689_canary_probe_caps(member: dict[str, Any]) -> dict[str, Any]:
    # 2026-07-20T07:31Z fable PROBE_SIZE ruling, read-path enforcement: the admission-time
    # clamp in _auto_degrade_member_from_runtime_member only runs when a new auto-degrade
    # member is built, so a persisted member carrying a pre-ruling cap would trade at that
    # cap forever. Clamp on every load so persisted state can never widen the probe cap.
    wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    policy_id = str(policy.get("policy_id") or member.get("policy_id") or "")
    if wallet != _A689_CANARY_WALLET or policy_id != _A689_CANARY_POLICY_ID:
        return member
    clamped = dict(member)
    prior_cap = _float_or_default(clamped.get("max_order_usd"), _A689_FABLE_PROBE_CAP_USD)
    cap = min(prior_cap if prior_cap > 0 else _A689_FABLE_PROBE_CAP_USD, _A689_FABLE_PROBE_CAP_USD)
    clamped["max_order_usd"] = cap
    clamped["fable_cap_max_order_usd"] = cap
    if isinstance(clamped.get("policy"), dict):
        clamped_policy = dict(clamped["policy"])
        policy_cap = _float_or_default(clamped_policy.get("max_order_usd"), cap)
        clamped_policy["max_order_usd"] = min(policy_cap if policy_cap > 0 else cap, cap)
        clamped["policy"] = clamped_policy
    if prior_cap > cap:
        clamped["a689_probe_cap_read_clamp"] = {
            "direction_id": "2026-07-20T08:16Z-fable-a689-probe-cap-fail-safe",
            "prior_max_order_usd": prior_cap,
            "clamped_max_order_usd": cap,
        }
    return clamped


def _normalize_active_set_member_runtime(member: dict[str, Any], *, now: dt.datetime | None = None) -> dict[str, Any]:
    normalized = _clamp_a689_canary_probe_caps(dict(member))
    auto_return = _weekend_bench_auto_return(normalized, now=now)
    if auto_return:
        normalized["weekend_bench_auto_return"] = auto_return
        if auto_return.get("eligible") is True:
            normalized["enabled"] = True
            normalized["status"] = "WEEKEND_BENCH_AUTO_RETURNED"
    hour_auto_return = _hour_band_bench_auto_return(normalized, now=now)
    if hour_auto_return:
        normalized["hour_band_bench_auto_return"] = hour_auto_return
        if hour_auto_return.get("eligible") is True:
            normalized["enabled"] = True
            normalized["status"] = "HOUR_BENCH_AUTO_RETURNED"
    activation_gate = _active_set_member_activation_not_before_gate(normalized, now=now)
    if activation_gate.get("configured") is True:
        normalized["activation_not_before_gate"] = activation_gate
        if activation_gate.get("held") is True:
            normalized["activation_held_prior_status"] = str(normalized.get("status") or "")
            normalized["enabled"] = False
            normalized["status"] = "PENDING_ACTIVATION_NOT_BEFORE_UTC"
    return normalized


def _active_set_member_activation_not_before_gate(
    member: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    raw = (
        member.get("activate_not_before_utc")
        or member.get("activation_not_before_utc")
        or (
            member.get("activation_gate", {}).get("activate_not_before_utc")
            if isinstance(member.get("activation_gate"), dict)
            else ""
        )
    )
    activation_at = _parse_iso_datetime(raw)
    if activation_at is None:
        return {"configured": False, "held": False}
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    held = now_dt < activation_at
    return {
        "configured": True,
        "held": held,
        "status": "HELD_UNTIL_ACTIVATION_TIME" if held else "ACTIVATION_TIME_REACHED",
        "activate_not_before_utc": activation_at.isoformat(),
        "checked_at": now_dt.isoformat(),
        "remaining_s": round(max(0.0, (activation_at - now_dt).total_seconds()), 6),
        "flow_stage": "LIVE/DEFEND",
        "rule": "enabled restart-payload members are not runtime eligible before their Fable activation timestamp",
    }


def _active_set_member_is_disabled(member: dict[str, Any], *, now: dt.datetime | None = None) -> bool:
    if _active_set_member_activation_not_before_gate(member, now=now).get("held") is True:
        return True
    status = str(member.get("status") or "").upper()
    if status.startswith(("DEMOTED", "DISABLED", "AUTO_DISABLED")):
        return True
    if status.startswith("WEEKEND_BENCH_AUTO_RETURNED"):
        return False
    if status.startswith("HOUR_BENCH_AUTO_RETURNED"):
        return False
    if status.startswith("WEEKEND_BENCHED") or isinstance(member.get("weekend_bench"), dict):
        auto_return = member.get("weekend_bench_auto_return")
        if not isinstance(auto_return, dict):
            auto_return = _weekend_bench_auto_return(member, now=now)
        return auto_return.get("eligible") is not True
    if status.startswith("HOUR_BENCHED") or isinstance(member.get("hour_band_bench"), dict):
        auto_return = member.get("hour_band_bench_auto_return")
        if not isinstance(auto_return, dict):
            auto_return = _hour_band_bench_auto_return(member, now=now)
        return auto_return.get("eligible") is not True
    return (
        member.get("enabled") is False
    )


def _active_set_member_queue_position(member: dict[str, Any]) -> float | None:
    raw = member.get("queue_position")
    if isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _sort_active_set_overlay_members(members: list[Any]) -> list[dict[str, Any]]:
    indexed_members = [(index, dict(member)) for index, member in enumerate(members) if isinstance(member, dict)]

    def sort_key(indexed_member: tuple[int, dict[str, Any]]) -> tuple[int, float, int]:
        index, member = indexed_member
        queue_position = _active_set_member_queue_position(member)
        if queue_position is None:
            return (1, 0.0, index)
        return (0, queue_position, index)

    return [member for _, member in sorted(indexed_members, key=sort_key)]


def _auto_degrade_active_member_limit(overlay: dict[str, Any]) -> int:
    for key in ("max_active_members", "target_member_count_max"):
        try:
            value = int(overlay.get(key))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return max(AUTO_DEGRADE_MAX_ACTIVE_MEMBERS, value)
    return AUTO_DEGRADE_MAX_ACTIVE_MEMBERS


def _promote_active_selection_pin_member(
    members: list[dict[str, Any]],
    overlay: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    current = now or dt.datetime.now(dt.timezone.utc)
    expires_at = _parse_iso_datetime(pin.get("expires_at"))
    created_at = _parse_iso_datetime(pin.get("created_at") or pin.get("updated_at"))
    quiet_clock = pin.get("quiet_clock") if isinstance(pin.get("quiet_clock"), dict) else {}
    quiet_clock_controls_expiry = (
        str(quiet_clock.get("reset_predicate") or "")
        == "selected_submit_eligible_copyintent"
    )
    age_valid = bool(
        created_at is None
        or (current - created_at).total_seconds() <= ACTIVE_SET_SELECTION_PIN_MAX_AGE_S
        or quiet_clock_controls_expiry
    )
    wallet = str(pin.get("source_wallet") or pin.get("wallet") or "").strip().lower()
    active = bool(
        pin.get("enabled") is True
        and wallet
        and (expires_at is None or current <= expires_at)
        and age_valid
    )
    if not active:
        return members, {"applied": False, "reason": "selection_pin_inactive"}
    pinned_index = next(
        (
            index
            for index, member in enumerate(members)
            if str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
            == wallet
        ),
        None,
    )
    if pinned_index is None:
        return members, {
            "applied": False,
            "reason": "selection_pin_member_absent",
            "source_wallet": wallet,
        }
    pinned_member = members[pinned_index]
    return [pinned_member, *members[:pinned_index], *members[pinned_index + 1 :]], {
        "applied": pinned_index > 0,
        "reason": "selection_pin_member_promoted_before_capacity",
        "source_wallet": wallet,
        "original_index": pinned_index,
    }


def _fable_cap_max_order_usd(member: dict[str, Any]) -> float | None:
    payloads: list[Any] = [
        member,
        member.get("fable_1413_defensive_cap") if isinstance(member.get("fable_1413_defensive_cap"), dict) else {},
    ]
    selected_size_exception = (
        member.get("selected_only_size_exception")
        if isinstance(member.get("selected_only_size_exception"), dict)
        else {}
    )
    if (
        selected_size_exception.get("authority") == "fable_2026-08-03T08:14Z_D1c"
        and selected_size_exception.get("reason") == "market_buy_precision_infeasible_at_1usd"
    ):
        payloads.append(
            {"fable_cap_max_order_usd": selected_size_exception.get("max_order_usd")}
        )
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        raw = payload.get("fable_cap_max_order_usd")
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value > 0:
            return value
    return None


def _apply_active_set_defensive_sizing(active_set: dict[str, Any]) -> dict[str, Any]:
    sizing = active_set.get("overnight_defensive_sizing")
    sizing = sizing if isinstance(sizing, dict) else {}
    if not sizing.get("enabled"):
        return active_set
    try:
        multiplier = float(sizing.get("budget_multiplier"))
    except (TypeError, ValueError):
        multiplier = 1.0
    if multiplier <= 0 or multiplier >= 1:
        return active_set
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    adjusted_members: list[dict[str, Any]] = []
    for row in members:
        member = dict(row) if isinstance(row, dict) else row
        if not isinstance(member, dict):
            adjusted_members.append(member)
            continue
        explicit_fable_cap = _fable_cap_max_order_usd(member)
        if explicit_fable_cap is not None:
            if member.get("max_order_usd") is not None:
                member["max_order_usd"] = min(float(member.get("max_order_usd") or 0.0), explicit_fable_cap)
            policy = dict(member.get("policy")) if isinstance(member.get("policy"), dict) else {}
            if policy.get("max_order_usd") is not None:
                policy["max_order_usd"] = min(float(policy.get("max_order_usd") or 0.0), explicit_fable_cap)
                member["policy"] = policy
            member["defensive_sizing"] = {
                "direction_id": sizing.get("direction_id"),
                "budget_multiplier": multiplier,
                "strong_tier_suspended": bool(sizing.get("strong_tier_suspended")),
                "explicit_fable_cap_max_order_usd": explicit_fable_cap,
                "budget_multiplier_skipped_for_explicit_fable_cap": True,
            }
            adjusted_members.append(member)
            continue
        for key in ("max_order_usd",):
            if member.get(key) is not None:
                member[key] = round(float(member.get(key) or 0.0) * multiplier, 6)
        policy = dict(member.get("policy")) if isinstance(member.get("policy"), dict) else {}
        if policy.get("max_order_usd") is not None:
            policy["max_order_usd"] = round(float(policy.get("max_order_usd") or 0.0) * multiplier, 6)
            member["policy"] = policy
        member["defensive_sizing"] = {
            "direction_id": sizing.get("direction_id"),
            "budget_multiplier": multiplier,
            "strong_tier_suspended": bool(sizing.get("strong_tier_suspended")),
        }
        adjusted_members.append(member)
    active_set = dict(active_set)
    active_set["members"] = adjusted_members
    active_set["defensive_sizing_applied"] = {
        "direction_id": sizing.get("direction_id"),
        "budget_multiplier": multiplier,
        "strong_tier_suspended": bool(sizing.get("strong_tier_suspended")),
    }
    return active_set


def _active_live_set_contract(*, now: dt.datetime | None = None) -> dict[str, Any]:
    runtime_phase = mission_contract().get("current_runtime_phase_contract")
    runtime_phase = runtime_phase if isinstance(runtime_phase, dict) else {}
    active_set = runtime_phase.get("active_live_set")
    active_set = dict(active_set) if isinstance(active_set, dict) else {}
    base_members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    liveness_reports: list[dict[str, Any]] = []
    if base_members:
        normalized_base_members = [
            _normalize_active_set_member_runtime(row, now=now)
            for row in base_members
            if isinstance(row, dict)
        ]
        active_set["members"], base_liveness = _apply_external_liveness_sweep(
            normalized_base_members,
            now=now,
            scope="mission_active_live_set",
        )
        liveness_reports.append(base_liveness)
    overlay = _load_auto_degrade_active_set_overlay()
    overlay_members = overlay.get("members") if isinstance(overlay.get("members"), list) else []
    if overlay_members and not _active_live_set_is_empty(active_set):
        normalized_overlay_members = [
            _normalize_active_set_member_runtime(member, now=now)
            for member in overlay_members
            if isinstance(member, dict)
        ]
        overlay_members, overlay_liveness = _apply_external_liveness_sweep(
            normalized_overlay_members,
            now=now,
            scope="auto_degrade_overlay_members",
        )
        liveness_reports.append(overlay_liveness)
        overlay_members = _sort_active_set_overlay_members(overlay_members)
        base_members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
        suppressed_base_wallets = {
            str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
            for member in overlay_members
            if isinstance(member, dict)
            and _active_set_member_is_disabled(member, now=now)
            and member.get("auto_degrade_suppresses_existing_wallet") is True
        }
        enabled_base_members = [
            row
            for row in base_members
            if not _active_set_member_is_disabled(row, now=now)
            and str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
            not in suppressed_base_wallets
        ]
        disabled_base_members = [row for row in base_members if _active_set_member_is_disabled(row, now=now)]
        existing_wallets = {
            str(row.get("source_wallet") or row.get("wallet") or "").lower()
            for row in enabled_base_members
        }
        disabled_base_wallets = {
            str(row.get("source_wallet") or row.get("wallet") or "").lower()
            for row in disabled_base_members
        }
        merged_members = [dict(row) for row in enabled_base_members]
        overlay_added = 0
        overlay_replaced_disabled_wallets: set[str] = set()
        max_active_members = _auto_degrade_active_member_limit(overlay)
        for member in overlay_members:
            if not isinstance(member, dict):
                continue
            if _active_set_member_is_disabled(member, now=now):
                continue
            wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
            if not wallet:
                continue
            if wallet in existing_wallets and member.get("auto_degrade_replaces_existing_wallet") is True:
                merged_members = [
                    row
                    for row in merged_members
                    if str(row.get("source_wallet") or row.get("wallet") or "").lower() != wallet
                ]
                existing_wallets.discard(wallet)
            if wallet in existing_wallets:
                continue
            merged_members.append(dict(member))
            existing_wallets.add(wallet)
            if wallet in disabled_base_wallets:
                overlay_replaced_disabled_wallets.add(wallet)
            overlay_added += 1
        if overlay_added > 0 or suppressed_base_wallets:
            merged_members.extend(
                dict(row)
                for row in disabled_base_members
                if str(row.get("source_wallet") or row.get("wallet") or "").lower()
                not in overlay_replaced_disabled_wallets
            )
            merged_members, pin_capacity_promotion = _promote_active_selection_pin_member(
                merged_members,
                overlay,
                now=now,
            )
            active_set["members"] = merged_members[:max_active_members]
            overflow_members = merged_members[max_active_members:]
            if overflow_members:
                active_set["auto_degrade_overflow_members"] = overflow_members
            active_set["target_member_count_max"] = min(
                max_active_members,
                max(int(active_set.get("target_member_count_max") or 3), len(enabled_base_members) + overlay_added),
            )
            try:
                overlay_path = str(AUTO_DEGRADE_ACTIVE_SET_STATE.relative_to(ROOT))
            except ValueError:
                overlay_path = str(AUTO_DEGRADE_ACTIVE_SET_STATE)
            active_set["auto_degrade_overlay"] = {
                "path": overlay_path,
                "member_count": overlay_added,
                "disabled_base_replacements": len(overlay_replaced_disabled_wallets),
                "suppressed_base_wallets": sorted(suppressed_base_wallets),
                "overflow_member_count": len(overflow_members),
                "explicit_queue_position": any(
                    _active_set_member_queue_position(member) is not None for member in overlay_members
                ),
                "selection_pin_capacity_promotion": pin_capacity_promotion,
                "updated_at": overlay.get("updated_at"),
            }
    if liveness_reports:
        raw_disabled_wallets = list(
            dict.fromkeys(
                wallet
                for report in liveness_reports
                for wallet in (report.get("disabled_wallets") or [])
                if wallet
            )
        )
        exempted_wallets = list(
            dict.fromkeys(
                wallet
                for report in liveness_reports
                for wallet in (report.get("exempted_wallets") or [])
                if wallet
            )
        )
        exempted_wallet_set = set(exempted_wallets)
        effective_disabled_wallets = [wallet for wallet in raw_disabled_wallets if wallet not in exempted_wallet_set]
        active_set["external_liveness_sweep"] = {
            "enabled": True,
            "flow_stage": "LIVE/DEFEND/PROMOTE",
            "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
            "reports": liveness_reports,
            "raw_disabled_count": sum(int(report.get("disabled_count") or 0) for report in liveness_reports),
            "raw_disabled_wallets": raw_disabled_wallets,
            "exempted_count": sum(int(report.get("exempted_count") or 0) for report in liveness_reports),
            "exempted_wallets": exempted_wallets,
            "disabled_count": len(effective_disabled_wallets),
            "disabled_wallets": effective_disabled_wallets,
            "rule": "external BTC-5m last-trade age <24h is required before runtime selection unless a newer explicit Fable readmission packet records its own bounded live tripwire",
        }
    return _apply_active_set_defensive_sizing(active_set)


def _active_live_set_is_empty(active_set: dict[str, Any] | None = None) -> bool:
    active_set = active_set if isinstance(active_set, dict) else _active_live_set_contract()
    return bool(active_set.get("live_set_empty_until_replacement"))


def _active_live_set_members_contract() -> list[dict[str, Any]]:
    active_set = _active_live_set_contract()
    if _active_live_set_is_empty(active_set):
        return []
    rows = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    members = [dict(row) for row in rows if isinstance(row, dict) and not _active_set_member_is_disabled(row)]
    if members:
        members, _ = _apply_temporal_slice_exclusions(members)
        return members
    if rows:
        return []
    primary = _primary_live_candidate_contract()
    if not primary:
        return []
    members, _ = _apply_temporal_slice_exclusions([primary])
    return members


def _candidate_live_protection_refs(source_wallet: str) -> list[dict[str, Any]]:
    wallet = str(source_wallet or "").strip().lower()
    if not wallet:
        return []
    refs: list[dict[str, Any]] = []
    runtime_phase = mission_contract().get("current_runtime_phase_contract")
    runtime_phase = runtime_phase if isinstance(runtime_phase, dict) else {}
    primary = runtime_phase.get("primary_live_candidate")
    if isinstance(primary, dict) and _member_wallet(primary) == wallet:
        row = dict(primary)
        row["_protection_source"] = "mission_primary_live_candidate"
        refs.append(row)
    active_set = runtime_phase.get("active_live_set")
    active_set = active_set if isinstance(active_set, dict) else {}
    for member in active_set.get("members") or []:
        if isinstance(member, dict) and _member_wallet(member) == wallet:
            row = dict(member)
            row["_protection_source"] = "mission_active_live_set"
            refs.append(row)
    overlay = _load_auto_degrade_active_set_overlay()
    latest = overlay.get("latest_admission")
    if isinstance(latest, dict) and _member_wallet(latest) == wallet:
        row = dict(latest)
        row["_protection_source"] = "auto_degrade_latest_admission"
        refs.append(row)
    for member in overlay.get("members") or []:
        if isinstance(member, dict) and _member_wallet(member) == wallet:
            row = dict(member)
            row["_protection_source"] = "auto_degrade_overlay_members"
            refs.append(row)
    return refs


def _candidate_live_protection_gate(candidate: dict[str, Any], *, now: dt.datetime | None = None) -> dict[str, Any]:
    source_wallet = _candidate_source_wallet(candidate) if candidate else ""
    refs = _candidate_live_protection_refs(source_wallet)
    disabled_refs = [ref for ref in refs if _active_set_member_is_disabled(ref, now=now)]
    temporal_member = dict(candidate or {})
    if source_wallet and not _member_wallet(temporal_member):
        temporal_member["source_wallet"] = source_wallet
    emergency_ref = next(
        (
            ref
            for ref in refs
            if ref.get("operator_emergency_seat") is True
            and str(ref.get("candidate_id") or "")
            == str(candidate.get("candidate_id") or "")
        ),
        None,
    )
    if emergency_ref is not None:
        temporal_member["operator_emergency_seat"] = True
        temporal_member["status"] = emergency_ref.get("status")
    for scoped_key in ("band_scoped_admission", "cell_scoped_admission"):
        if isinstance(temporal_member.get(scoped_key), dict):
            continue
        scoped_ref = next(
            (
                ref
                for ref in refs
                if isinstance(ref.get(scoped_key), dict)
            ),
            None,
        )
        if scoped_ref is not None:
            temporal_member[scoped_key] = dict(scoped_ref[scoped_key])
            scoped_policy = (
                scoped_ref.get("policy")
                if isinstance(scoped_ref.get("policy"), dict)
                else {}
            )
            temporal_policy = (
                dict(temporal_member.get("policy"))
                if isinstance(temporal_member.get("policy"), dict)
                else {}
            )
            if isinstance(scoped_policy.get("move_slice_keys"), list):
                temporal_policy["move_slice_keys"] = list(
                    scoped_policy["move_slice_keys"]
                )
            temporal_member["policy"] = temporal_policy
    temporal = (
        _temporal_slice_exclusion(temporal_member, now=now)
        if source_wallet and now is not None and _candidate_status_live_admissible(candidate)
        else {}
    )
    external_liveness = (
        _external_liveness_gate_for_wallet(source_wallet, now=now)
        if source_wallet and _candidate_status_live_admissible(candidate)
        else {}
    )
    if external_liveness and external_liveness.get("passed") is not True:
        for ref in [dict(candidate or {})] + refs:
            external_liveness = _apply_f418_readmission_liveness_exemption(external_liveness, ref)
            external_liveness = _apply_operator_emergency_seat_liveness_exemption(
                external_liveness,
                ref,
                now=now or dt.datetime.now(dt.timezone.utc),
            )
            if external_liveness.get("passed") is True:
                break
    failed_checks: list[str] = []
    if disabled_refs and len(disabled_refs) == len(refs):
        failed_checks.append("shared_live_gate_disabled_or_demoted")
    if temporal.get("excluded") is True:
        failed_checks.append("shared_live_gate_temporal_slice")
    if external_liveness and external_liveness.get("passed") is not True:
        failed_checks.append("shared_live_gate_external_liveness")
    return {
        "passed": not failed_checks,
        "failed_checks": failed_checks,
        "source_wallet": source_wallet,
        "reference_count": len(refs),
        "disabled_reference_count": len(disabled_refs),
        "disabled_references": [
            {
                "candidate_id": str(ref.get("candidate_id") or ""),
                "status": str(ref.get("status") or ""),
                "enabled": ref.get("enabled"),
                "source": str(ref.get("_protection_source") or ""),
            }
            for ref in disabled_refs
        ],
        "temporal_slice_evaluation": temporal,
        "external_liveness_gate": external_liveness,
        "rule": "every live candidate source path must pass disabled/demoted state, external liveness, and active temporal slice protections before submit",
    }


def _active_set_generation_id(active_set: dict[str, Any] | None = None) -> str:
    active_set = active_set if isinstance(active_set, dict) else _active_live_set_contract()
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    fingerprint = {
        "status": active_set.get("status"),
        "refill_direction_id": active_set.get("refill_direction_id"),
        "empty_direction_id": active_set.get("empty_direction_id"),
        "members": [
            {
                "candidate_id": row.get("candidate_id"),
                "source_wallet": str(row.get("source_wallet") or row.get("wallet") or "").lower(),
                "policy_id": row.get("policy_id"),
                "late_window_stop_s": (row.get("policy") if isinstance(row.get("policy"), dict) else {}).get(
                    "late_window_stop_s"
                ),
            }
            for row in members
            if isinstance(row, dict)
        ],
    }
    return stable_id("active_set_gen", fingerprint, length=16)


_AUTO_DEGRADE_OVERLAY_CACHE: tuple[tuple[int, int, int], dict[str, Any]] | None = None


def _load_auto_degrade_active_set_overlay() -> dict[str, Any]:
    global _AUTO_DEGRADE_OVERLAY_CACHE
    if not AUTO_DEGRADE_ACTIVE_SET_STATE.exists():
        _AUTO_DEGRADE_OVERLAY_CACHE = None
        return {}
    try:
        stat = AUTO_DEGRADE_ACTIVE_SET_STATE.stat()
        signature = (int(stat.st_ino), int(stat.st_mtime_ns), int(stat.st_size))
    except OSError:
        _AUTO_DEGRADE_OVERLAY_CACHE = None
        return {}
    if _AUTO_DEGRADE_OVERLAY_CACHE is not None and _AUTO_DEGRADE_OVERLAY_CACHE[0] == signature:
        return _AUTO_DEGRADE_OVERLAY_CACHE[1]
    # This overlay is a hot control plane.  The function-level inode/mtime/size
    # cache already avoids unchanged reads; the generic large-JSON object cache
    # must not sit underneath it because that cache intentionally returns the
    # original object and can hide a pin renewal from the resident guard.
    loaded = load_json(AUTO_DEGRADE_ACTIVE_SET_STATE, default={}, cache_readonly=False)
    if not isinstance(loaded, dict):
        _AUTO_DEGRADE_OVERLAY_CACHE = None
        return {}
    if _fable_1413_e4_ratified_applies(loaded):
        normalized = _normalize_ratified_e4_active_set_overlay(loaded)
    else:
        normalized = _normalize_reverted_e4_active_set_overlay(loaded)
    _AUTO_DEGRADE_OVERLAY_CACHE = (signature, normalized)
    return normalized


def _fence_latest_mechanical_demotion(
    payload: dict[str, Any],
    *,
    current: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Prevent a stale guard-cycle overlay from resurrecting a demoted pin."""
    current_demotion = (
        current.get("latest_mechanical_temporal_loss_demotion")
        if isinstance(current.get("latest_mechanical_temporal_loss_demotion"), dict)
        else {}
    )
    if str(current_demotion.get("status") or "").upper() != "APPLIED":
        return payload, {"status": "NO_ACTIVE_MECHANICAL_DEMOTION"}
    current_at = _parse_iso_datetime(current_demotion.get("generated_at"))
    if current_at is None:
        return payload, {"status": "MECHANICAL_DEMOTION_TIMESTAMP_MISSING"}
    current_until = _parse_iso_datetime(current_demotion.get("cooloff_until"))
    if current_until is not None and dt.datetime.now(dt.timezone.utc) >= current_until:
        return payload, {
            "status": "MECHANICAL_DEMOTION_COOL_OFF_EXPIRED",
            "target_wallet": current_demotion.get("target_wallet"),
            "cooloff_until": current_demotion.get("cooloff_until"),
        }
    proposed_demotion = (
        payload.get("latest_mechanical_temporal_loss_demotion")
        if isinstance(payload.get("latest_mechanical_temporal_loss_demotion"), dict)
        else {}
    )
    proposed_at = _parse_iso_datetime(proposed_demotion.get("generated_at"))
    proposed_target = str(
        proposed_demotion.get("target_wallet") or ""
    ).strip().lower()
    current_target_wallet = str(
        current_demotion.get("target_wallet") or ""
    ).strip().lower()
    if (
        proposed_at is not None
        and proposed_at > current_at
        and proposed_target != current_target_wallet
    ):
        return payload, {"status": "MECHANICAL_DEMOTION_ALREADY_CURRENT"}
    authoritative_demotion = (
        proposed_demotion
        if (
            proposed_at is not None
            and proposed_at >= current_at
            and proposed_target == current_target_wallet
            and str(proposed_demotion.get("status") or "").upper() == "APPLIED"
        )
        else current_demotion
    )
    target_wallet = str(
        authoritative_demotion.get("target_wallet") or ""
    ).strip().lower()
    if not target_wallet:
        return payload, {"status": "MECHANICAL_DEMOTION_TARGET_MISSING"}

    pin = payload.get("selection_pin")
    pin_created_at = (
        _parse_iso_datetime(pin.get("created_at")) if isinstance(pin, dict) else None
    )
    pin_targets_wallet = bool(
        isinstance(pin, dict)
        and pin.get("enabled") is True
        and str(pin.get("source_wallet") or pin.get("wallet") or "").strip().lower()
        == target_wallet
        and str(pin.get("candidate_id") or "").startswith("policy_choke_rung_direct_")
        and pin_created_at is not None
        and pin_created_at > current_at
    )
    target_admission = next(
        (
            row
            for row in payload.get("members") or []
            if isinstance(row, dict)
            and str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
            == target_wallet
            and row.get("enabled") is True
        ),
        {},
    )
    admission_summary = (
        target_admission.get("summary")
        if isinstance(target_admission.get("summary"), dict)
        else {}
    )
    admission_evidence = (
        admission_summary.get("evidence")
        if isinstance(admission_summary.get("evidence"), dict)
        else {}
    )
    admission_checks = (
        admission_evidence.get("checks")
        if isinstance(admission_evidence.get("checks"), dict)
        else {}
    )
    required_all_pass_checks = (
        "f1_measured_positive_regime_cell",
        "f1_venue_reachable_admissible",
        "f1_walk_forward_admissible",
        "f2_fresh_rows_and_own_policy_copyable",
        "f3_not_enabled_or_cooloff_or_fading",
        "f4_external_liveness",
        "own_evidenced_policy_available",
        "both_resolved_halves_positive",
        "not_terminal_park_red_clock_or_measured_loser",
        "active_temporal_not_proven_negative",
    )
    if pin_targets_wallet and all(
        admission_checks.get(check) is True for check in required_all_pass_checks
    ):
        return payload, {
            "status": "NEWER_ALL_PASS_DIRECT_ADMISSION_SUPERSEDES_DEMOTION",
            "target_wallet": target_wallet,
            "demotion_generated_at": authoritative_demotion.get("generated_at"),
            "admission_created_at": pin.get("created_at"),
        }

    current_target = next(
        (
            dict(row)
            for row in current.get("members") or []
            if isinstance(row, dict)
            and str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
            == target_wallet
        ),
        {},
    )
    members: list[Any] = []
    target_seen = False
    for raw in payload.get("members") or []:
        if not isinstance(raw, dict):
            members.append(raw)
            continue
        member = dict(raw)
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if wallet == target_wallet:
            target_seen = True
            member["enabled"] = False
            member["status"] = str(
                authoritative_demotion.get("target_status")
                or "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
            )
            member["mechanical_temporal_loss_demotion"] = (
                current_target.get("mechanical_temporal_loss_demotion")
                if isinstance(current_target.get("mechanical_temporal_loss_demotion"), dict)
                else {
                    "flow_stage": "LIVE/ROTATE",
                    "direction_id": authoritative_demotion.get("direction_id"),
                    "generated_at": authoritative_demotion.get("generated_at"),
                    "reason": authoritative_demotion.get("reason"),
                    "evidence": authoritative_demotion.get("evidence"),
                    "readmission_rule": "requires a newer evidence-positive Fable DIRECTION",
                }
            )
        members.append(member)
    if not target_seen and current_target:
        members.append(current_target)

    fenced = dict(payload)
    fenced["members"] = members
    fenced["latest_mechanical_temporal_loss_demotion"] = authoritative_demotion
    fenced["last_action"] = "MECHANICAL_TEMPORAL_LOSS_MEMBER_DEMOTION"
    pin = fenced.get("selection_pin")
    if (
        isinstance(pin, dict)
        and str(pin.get("source_wallet") or pin.get("wallet") or "").strip().lower()
        == target_wallet
    ):
        fenced["selection_pin"] = {
            **pin,
            "enabled": False,
            "disabled_at": authoritative_demotion.get("generated_at"),
            "disabled_reason": "mechanical_demotion_generation_fence",
        }
    fence = {
        "status": "STALE_OVERLAY_WRITE_FENCED",
        "flow_stage": "LIVE/ROTATE/SELF-DEV",
        "target_wallet": target_wallet,
        "demotion_generated_at": authoritative_demotion.get("generated_at"),
        "writer_payload_updated_at": payload.get("updated_at"),
        "rule": (
            "guard overlay writers may not re-enable a wallet or selection pin after "
            "a newer mechanical demotion generation"
        ),
    }
    fenced["demotion_generation_fence"] = fence
    return fenced, fence


def _atomic_write_auto_degrade_overlay(payload: dict[str, Any]) -> dict[str, Any]:
    current = load_json(AUTO_DEGRADE_ACTIVE_SET_STATE, default={})
    current = current if isinstance(current, dict) else {}
    fenced, fence = _fence_latest_mechanical_demotion(payload, current=current)
    atomic_write_json(AUTO_DEGRADE_ACTIVE_SET_STATE, fenced)
    return fence


def _fable_1413_e4_ratified_applies(overlay: dict[str, Any]) -> bool:
    values: list[Any] = [overlay.get("direction_id")]
    latest_admission = overlay.get("latest_admission") if isinstance(overlay.get("latest_admission"), dict) else {}
    values.extend(
        [
            latest_admission.get("direction_id"),
            latest_admission.get("status"),
            latest_admission.get("fable_live_defend_status"),
            latest_admission.get("fable_cap_direction_id"),
            latest_admission.get("promotion_basis"),
        ]
    )
    freeze = overlay.get("selection_priority_freeze") if isinstance(overlay.get("selection_priority_freeze"), dict) else {}
    values.extend([freeze.get("direction_id"), freeze.get("reason")])
    frozen_wallets = freeze.get("frozen_wallets") if isinstance(freeze.get("frozen_wallets"), list) else []
    for row in frozen_wallets:
        if isinstance(row, dict):
            values.extend([row.get("direction_id"), row.get("status"), row.get("reason")])
    defensive_cap = (
        overlay.get("latest_a95b_defensive_cap")
        if isinstance(overlay.get("latest_a95b_defensive_cap"), dict)
        else {}
    )
    values.extend(
        [
            defensive_cap.get("direction_id"),
            defensive_cap.get("fable_cap_direction_id"),
            defensive_cap.get("reason"),
            defensive_cap.get("restore_rule"),
        ]
    )
    members = overlay.get("members") if isinstance(overlay.get("members"), list) else []
    for member in members:
        if not isinstance(member, dict):
            continue
        values.extend([member.get("status"), member.get("fable_live_defend_status")])
        cap = member.get("fable_1413_defensive_cap") if isinstance(member.get("fable_1413_defensive_cap"), dict) else {}
        suppression = (
            member.get("suppressed_by_fable_1413")
            if isinstance(member.get("suppressed_by_fable_1413"), dict)
            else {}
        )
        values.extend([cap.get("direction_id"), suppression.get("direction_id")])
    haystack = " ".join(str(value or "") for value in values).lower()
    return "fable_1413" in haystack or FABLE_1413_E4_RATIFIED_DIRECTION_ID.lower() in haystack


def _fable_1413_a95b_cap_fields() -> dict[str, Any]:
    return {
        "fable_cap_max_order_usd": FABLE_1413_A95B_CAP_MAX_ORDER_USD,
        "fable_cap_restore_value": FABLE_1413_A95B_CAP_RESTORE_VALUE,
        "fable_cap_restore_condition": FABLE_1413_A95B_CAP_RESTORE_CONDITION,
        "fable_cap_direction_id": FABLE_1413_E4_RATIFIED_DIRECTION_ID,
    }


def _apply_fable_1413_a95b_cap_provenance(member: dict[str, Any]) -> dict[str, Any]:
    wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
    if wallet != FABLE_1413_A95B_WALLET:
        return member
    updated = dict(member)
    updated.update(_fable_1413_a95b_cap_fields())
    updated["max_order_usd"] = FABLE_1413_A95B_CAP_MAX_ORDER_USD
    if updated.get("min_order_usd") is not None:
        updated["min_order_usd"] = min(
            _float_or_default(updated.get("min_order_usd"), FABLE_1413_A95B_CAP_MAX_ORDER_USD),
            FABLE_1413_A95B_CAP_MAX_ORDER_USD,
        )
    policy = dict(updated.get("policy")) if isinstance(updated.get("policy"), dict) else {}
    policy["max_order_usd"] = FABLE_1413_A95B_CAP_MAX_ORDER_USD
    if policy.get("min_order_usd") is not None:
        policy["min_order_usd"] = min(
            _float_or_default(policy.get("min_order_usd"), FABLE_1413_A95B_CAP_MAX_ORDER_USD),
            FABLE_1413_A95B_CAP_MAX_ORDER_USD,
        )
    updated["policy"] = policy
    cap = dict(updated.get("fable_1413_defensive_cap")) if isinstance(updated.get("fable_1413_defensive_cap"), dict) else {}
    cap.update(
        {
            "direction_id": FABLE_1413_E4_RATIFIED_DIRECTION_ID,
            "runtime_target_max_order_usd": FABLE_1413_A95B_CAP_MAX_ORDER_USD,
            "overlay_max_order_usd": FABLE_1413_A95B_CAP_MAX_ORDER_USD,
            "restore_rule": (
                f"restore runtime max_order_usd={FABLE_1413_A95B_CAP_RESTORE_VALUE:.1f} only when "
                f"{FABLE_1413_A95B_CAP_RESTORE_CONDITION}"
            ),
            **_fable_1413_a95b_cap_fields(),
        }
    )
    updated["fable_1413_defensive_cap"] = cap
    return updated


def _set_gate_recognized_member_status(
    member: dict[str, Any],
    *,
    gate_status: str,
    provenance_status: str,
    direction_id: str,
    reason: str,
) -> dict[str, Any]:
    updated = dict(member)
    prior_status = updated.get("status")
    updated["status"] = gate_status
    updated["status_provenance"] = {
        "flow_stage": "LIVE/DEFEND",
        "direction_id": direction_id,
        "gate_status": gate_status,
        "provenance_status": provenance_status,
        "prior_status": prior_status,
        "reason": reason,
        "rule": "live-member status uses gate-recognized vocabulary; Fable/mechanical labels live in provenance fields",
    }
    return updated


def _normalize_ratified_e4_active_set_overlay(overlay: dict[str, Any]) -> dict[str, Any]:
    payload = dict(overlay)
    payload["direction_id"] = FABLE_1413_E4_RATIFIED_DIRECTION_ID
    latest_admission = payload.get("latest_admission")
    if isinstance(latest_admission, dict):
        payload["latest_admission"] = _apply_fable_1413_a95b_cap_provenance(latest_admission)
    members = payload.get("members")
    if isinstance(members, list):
        payload["members"] = [
            _apply_fable_1413_a95b_cap_provenance(member) if isinstance(member, dict) else member
            for member in members
        ]
    defensive_cap = payload.get("latest_a95b_defensive_cap")
    if isinstance(defensive_cap, dict):
        updated_cap = dict(defensive_cap)
        updated_cap.update(_fable_1413_a95b_cap_fields())
        updated_cap["overlay_max_order_usd"] = FABLE_1413_A95B_CAP_MAX_ORDER_USD
        updated_cap["to_runtime_max_order_usd"] = FABLE_1413_A95B_CAP_MAX_ORDER_USD
        updated_cap["restore_rule"] = (
            f"restore runtime max_order_usd={FABLE_1413_A95B_CAP_RESTORE_VALUE:.1f} only when "
            f"{FABLE_1413_A95B_CAP_RESTORE_CONDITION}"
        )
        payload["latest_a95b_defensive_cap"] = updated_cap
    execution = overlay.get("latest_e4_tripwire_execution")
    if isinstance(execution, dict):
        updated_execution = dict(execution)
        updated_execution["status"] = "RATIFIED_BY_FABLE_1413"
        updated_execution["ratified_by_fable"] = {
            "direction_id": FABLE_1413_E4_RATIFIED_DIRECTION_ID,
            "reason": "Fable 14:13Z confirmed E4 streak=3 and ruled a95b fallback live-defend member stands.",
        }
        payload["latest_e4_tripwire_execution"] = updated_execution
    normalization = payload.get("reverted_e4_runtime_normalization")
    if isinstance(normalization, dict):
        updated_normalization = dict(normalization)
        updated_normalization["superseded_by"] = FABLE_1413_E4_RATIFIED_DIRECTION_ID
        updated_normalization["superseded_reason"] = "Fable 14:13Z ratified the later E4 fire after the 13:51Z HOLD audit."
        payload["reverted_e4_runtime_normalization"] = updated_normalization
    return payload


def _reverted_e4_hold_applies(overlay: dict[str, Any]) -> bool:
    if _fable_1413_e4_ratified_applies(overlay):
        return False
    escalation = overlay.get("latest_e4_tripwire_escalation")
    escalation = escalation if isinstance(escalation, dict) else {}
    status = str(escalation.get("status") or "").strip().upper()
    revert_direction_id = str(escalation.get("revert_direction_id") or "").strip()
    if status == FABLE_1351_E4_HOLD_REVERT_STATUS:
        return True
    return revert_direction_id == FABLE_1351_E4_HOLD_DIRECTION_ID and "E4" in status and "REVERT" in status


def _row_has_reverted_e4_direction(row: dict[str, Any]) -> bool:
    values: list[Any] = [
        row.get("direction_id"),
        row.get("promotion_direction_id"),
        row.get("demotion_direction_id"),
        row.get("status"),
        row.get("demotion_reason"),
        row.get("promotion_basis"),
    ]
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
    values.extend(
        [
            summary.get("direction_id"),
            summary.get("promotion_basis"),
            summary.get("demotion_direction_id"),
            summary.get("demotion_reason"),
        ]
    )
    haystack = " ".join(str(value or "") for value in values).lower()
    return FABLE_1317_E4_TRIPWIRE_DIRECTION_FRAGMENT in haystack or "fable_1317_e4_tripwire" in haystack


def _normalize_reverted_e4_active_set_overlay(overlay: dict[str, Any]) -> dict[str, Any]:
    if not _reverted_e4_hold_applies(overlay):
        return overlay

    escalation = overlay.get("latest_e4_tripwire_escalation")
    escalation = escalation if isinstance(escalation, dict) else {}
    suppressed_wallet = str(escalation.get("suppressed_wallet") or "").strip().lower()
    promoted_wallet = str(escalation.get("promoted_wallet") or "").strip().lower()
    payload = dict(overlay)
    previous_normalization = overlay.get("reverted_e4_runtime_normalization")
    previous_normalization = previous_normalization if isinstance(previous_normalization, dict) else {}
    restored_member: dict[str, Any] | None = None
    removed_promoted_member = False
    members: list[Any] = []
    raw_members = overlay.get("members")
    for member in raw_members if isinstance(raw_members, list) else []:
        if not isinstance(member, dict):
            members.append(member)
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if promoted_wallet and wallet == promoted_wallet and _row_has_reverted_e4_direction(member):
            removed_promoted_member = True
            continue
        if suppressed_wallet and wallet == suppressed_wallet and _row_has_reverted_e4_direction(member):
            updated = _set_gate_recognized_member_status(
                member,
                gate_status="PASS",
                provenance_status="FABLE_1351_HOLD_RESTORED_FROM_REVERTED_E4",
                direction_id=FABLE_1351_E4_HOLD_DIRECTION_ID,
                reason="Fable 13:51Z audited E4 as not crossed; restore suppressed wallet under a gate-recognized status.",
            )
            updated["enabled"] = True
            updated.pop("disabled_at", None)
            updated.pop("demotion_direction_id", None)
            updated.pop("demotion_reason", None)
            updated["fable_restore_status"] = "FABLE_1351_HOLD_RESTORED_FROM_REVERTED_E4"
            updated["next_action"] = (
                "continue Fable 13:51 HOLD; publish E4 streak and keep the 18:00Z dead-band observation scheduled"
            )
            updated["reverted_e4_runtime_normalization"] = {
                "direction_id": FABLE_1351_E4_HOLD_DIRECTION_ID,
                "reason": "Fable 13:51Z audited E4 as not crossed; ignore the earlier E4 suppression path at runtime.",
                "prior_status": member.get("status"),
                "restored_member_status": "PASS",
                "restored_member_provenance_status": "FABLE_1351_HOLD_RESTORED_FROM_REVERTED_E4",
            }
            restored_member = updated
            members.append(updated)
            continue
        members.append(member)
    payload["members"] = members

    latest_admission = overlay.get("latest_admission")
    if (
        isinstance(latest_admission, dict)
        and promoted_wallet
        and str(latest_admission.get("source_wallet") or latest_admission.get("wallet") or "").strip().lower()
        == promoted_wallet
        and _row_has_reverted_e4_direction(latest_admission)
    ):
        if restored_member is not None:
            payload["latest_admission"] = restored_member
        else:
            payload.pop("latest_admission", None)

    freeze = overlay.get("selection_priority_freeze")
    if isinstance(freeze, dict):
        frozen_wallets = freeze.get("frozen_wallets") if isinstance(freeze.get("frozen_wallets"), list) else []
        freeze_direction = str(freeze.get("direction_id") or "").strip().lower()
        freezes_suppressed_wallet = any(
            isinstance(row, dict)
            and str(row.get("source_wallet") or row.get("wallet") or "").strip().lower() == suppressed_wallet
            for row in frozen_wallets
        )
        if freezes_suppressed_wallet and FABLE_1317_E4_TRIPWIRE_DIRECTION_FRAGMENT in freeze_direction:
            updated_freeze = dict(freeze)
            updated_freeze["enabled"] = False
            updated_freeze["disabled_by_fable_hold"] = {
                "direction_id": FABLE_1351_E4_HOLD_DIRECTION_ID,
                "reason": "Fable 13:51Z says E4 did not cross; stale ac05 priority freeze must not affect selection.",
            }
            payload["selection_priority_freeze"] = updated_freeze

    execution = overlay.get("latest_e4_tripwire_execution")
    if isinstance(execution, dict):
        updated_execution = dict(execution)
        updated_execution["status"] = "IGNORED_BY_FABLE_1351_HOLD"
        updated_execution["ignored_by_fable_hold"] = {
            "direction_id": FABLE_1351_E4_HOLD_DIRECTION_ID,
            "reason": "Fable 13:51Z supersedes the earlier E4 execution path until a new E1-E4 tripwire fires.",
        }
        payload["latest_e4_tripwire_execution"] = updated_execution

    payload["direction_id"] = FABLE_1351_E4_HOLD_DIRECTION_ID
    payload["reverted_e4_runtime_normalization"] = {
        "enabled": True,
        "flow_stage": "LIVE/DEFEND",
        "direction_id": FABLE_1351_E4_HOLD_DIRECTION_ID,
        "suppressed_wallet_restored": bool(restored_member) or bool(previous_normalization.get("suppressed_wallet_restored")),
        "promoted_wallet_removed": bool(removed_promoted_member) or bool(previous_normalization.get("promoted_wallet_removed")),
        "selection_priority_freeze_disabled": (
            isinstance(payload.get("selection_priority_freeze"), dict)
            and payload["selection_priority_freeze"].get("enabled") is False
        )
        or bool(previous_normalization.get("selection_priority_freeze_disabled")),
    }
    return payload


def _live_build_max_observed_age_s(args: argparse.Namespace) -> float:
    return float(getattr(args, "live_build_max_observed_age_s", LIVE_BUILD_MAX_OBSERVED_AGE_S))


def _shadow_max_event_age_s(args: argparse.Namespace) -> float:
    return float(getattr(args, "shadow_max_event_age_s", getattr(args, "max_event_age_s", 30.0)))


def _shadow_live_build_max_observed_age_s(args: argparse.Namespace) -> float:
    return float(getattr(args, "shadow_live_build_max_observed_age_s", _live_build_max_observed_age_s(args)))


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
    primary_live_candidate = _primary_live_candidate_contract()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--promotion-rotation-state", default=DEFAULT_PROMOTION_ROTATION_STATE)
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--history-window-index", default="data/research/wallet_copy_history_window_index.json")
    parser.add_argument(
        "--live-guard-hot-history",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Route live guard runtime feed merges to a compact history state instead of rewriting the research history.",
    )
    parser.add_argument("--live-guard-hot-history-state", default=DEFAULT_LIVE_GUARD_HOT_HISTORY_STATE)
    parser.add_argument("--live-guard-copy-intents-state", default=DEFAULT_LIVE_GUARD_COPY_INTENTS_STATE)
    parser.add_argument("--live-guard-hot-history-window-index", default=DEFAULT_LIVE_GUARD_HOT_HISTORY_WINDOW_INDEX)
    parser.add_argument(
        "--live-guard-hot-history-retain-events",
        type=int,
        default=DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_EVENTS,
    )
    parser.add_argument(
        "--live-guard-hot-history-retain-copy-intents",
        type=int,
        default=DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_COPY_INTENTS,
    )
    parser.add_argument("--source-route-state", default="data/research/wallet_copy_source_route_state.json")
    parser.add_argument("--state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--event-log", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--lock-file", default="data/research/wallet_copy_live_guard.lock")
    parser.add_argument("--wallet-event-log", default="data/research/wallet_copy_live_guard_wallet_events.jsonl")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_live_guard_paper_state.json")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_live_guard_paper_events.jsonl")
    parser.add_argument("--live-arm-state", default="data/research/wallet_copy_live_execution_arm_state.json")
    parser.add_argument(
        "--selected-candidate-override-state",
        default="data/research/wallet_copy_live_guard_selected_candidate_override.json",
    )
    parser.add_argument("--live-ledger-state", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--live-ledger-event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--shadow-lanes", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--shadow-state", default="data/research/wallet_copy_guard_shadow_lanes_state.json")
    parser.add_argument("--shadow-event-log", default="data/research/wallet_copy_guard_shadow_lanes_events.jsonl")
    parser.add_argument("--shadow-max-intents-per-lane", type=int, default=6)
    parser.add_argument("--shadow-max-event-age-s", type=float, default=300.0)
    parser.add_argument("--shadow-live-build-max-observed-age-s", type=float, default=300.0)
    parser.add_argument("--shadow-retain-rows", type=int, default=10_000)
    parser.add_argument(
        "--routing-router-mode",
        choices=("shadow", "off"),
        default="shadow",
        help="Stage-1 per-signal router mode. shadow records would-route decisions and submits no orders.",
    )
    parser.add_argument("--routing-shadow-validation-state", default=DEFAULT_ROUTING_SHADOW_VALIDATION_STATE)
    parser.add_argument("--routing-shadow-candidate-seats", default=DEFAULT_ROUTING_SHADOW_CANDIDATE_SEATS)
    parser.add_argument(
        "--routing-shadow-validation-min-hours",
        type=float,
        default=DEFAULT_ROUTING_SHADOW_MIN_VALIDATION_HOURS,
    )
    parser.add_argument(
        "--routing-shadow-validation-retain-rows",
        type=int,
        default=DEFAULT_ROUTING_SHADOW_RETAIN_ROWS,
    )
    parser.add_argument("--e5-shadow-lane-state", default="data/research/maker_first_btc5m_paper_state.json")
    parser.add_argument(
        "--e5-live-intents-state",
        default="data/research/maker_first_btc5m_live_intents_latest.json",
    )
    parser.add_argument(
        "--e5-live-actuator-state",
        default="data/research/e5_maker_first_live_actuator_latest.json",
    )
    parser.add_argument(
        "--e5-5share-regrade-state",
        default="data/research/e5_maker_first_5share_regrade_latest.json",
    )
    parser.add_argument("--e5-live-actuator", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--cross-exchange-paper-state",
        default="data/research/btc5m_cross_exchange_probability_edge_paper_lane_state.json",
    )
    parser.add_argument(
        "--cross-exchange-live-actuator-state",
        default="data/research/btc5m_cross_exchange_probability_edge_live_actuator_latest.json",
    )
    parser.add_argument(
        "--cross-exchange-promoted-cell-state",
        default="data/research/btc5m_promoted_cell_active_selector.json",
    )
    parser.add_argument(
        "--cross-exchange-promoted-cell-sources",
        default=(
            "data/research/btc5m_cross_exchange_promoted_cell_latest.json,"
            "data/research/btc5m_multivenue_ttl_passive_residual_selector.json,"
            "data/research/btc5m_multivenue_ttl_maker_first_residual_selector.json,"
            "data/research/btc5m_perp_microstructure_selector.json,"
            "data/research/btc5m_native_aggressor_sweep_continuation_selector.json"
        ),
    )
    parser.add_argument(
        "--cross-exchange-deadman-state",
        default="data/research/order_flow_deadman_state.json",
    )
    parser.add_argument(
        "--cross-exchange-live-actuator",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--cross-exchange-live-actuator-ttl-s",
        type=float,
        default=_CROSS_EXCHANGE_ACTIVATION_TTL_S,
    )
    parser.add_argument(
        "--wide-family-live-actuator",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--wide-family-state",
        default="data/research/wide_positive_slice_family_state.json",
    )
    parser.add_argument(
        "--wide-family-live-actuator-state",
        default="data/research/wide_positive_slice_family_live_actuator_latest.json",
    )
    parser.add_argument("--e6-shadow-lane-state", default="data/research/e6_whale_net_flow_paper_lane_state.json")
    parser.add_argument("--e6-shadow-paper-state", default="data/research/e6_whale_net_flow_paper_state.json")
    parser.add_argument(
        "--structural-scalp-lane-state",
        default="data/research/btc5m_structural_scalp_paper_lane_state.json",
    )
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--operator-approval-id", default=os.getenv("WALLET_COPY_OPERATOR_APPROVAL_ID", ""))
    parser.add_argument(
        "--candidate-id",
        default=str(primary_live_candidate.get("candidate_id") or ""),
        help="Optional live primary candidate pin. The candidate must still be a PASS single-wallet candidate.",
    )
    parser.add_argument(
        "--source-wallet",
        default=str(primary_live_candidate.get("source_wallet") or ""),
        help="Optional live primary source wallet pin. Used to prevent accidental live rotation.",
    )
    parser.add_argument("--policy-id", default=str(primary_live_candidate.get("policy_id") or ""))
    parser.add_argument(
        "--active-set",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run all mission active-live-set members through this same single guard process.",
    )
    parser.add_argument(
        "--active-set-member-limit",
        type=int,
        default=0,
        help="0 means use active_live_set.target_member_count_max from the mission contract.",
    )
    parser.add_argument(
        "--disable-total-loss-member-auto-disable",
        action="store_true",
        help="Disable the guard-cycle filter that removes members with 3+ resolved fills and a 100%% total-loss rate.",
    )
    parser.add_argument(
        "--state-digest-json",
        default=str(STATE_DIGEST_JSON),
        help="State digest JSON used for mechanical live-defense tripwires such as probe caps.",
    )
    parser.add_argument(
        "--total-loss-auto-disable-min-resolved-fills",
        type=int,
        default=TOTAL_LOSS_AUTO_DISABLE_MIN_RESOLVED_FILLS,
    )
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--explicit-live-operator-go", action="store_true")
    parser.add_argument("--live-orders-allowed", action="store_true")
    parser.add_argument("--iterations", type=int, default=0, help="0 means run until stopped.")
    parser.add_argument("--sleep-s", type=float, default=2.0)
    parser.add_argument(
        "--event-triggered-cycle-scheduler",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="RULING21b: after a fresh copy event lands, wake the same live guard loop immediately.",
    )
    parser.add_argument(
        "--event-triggered-cycle-trigger-sleep-s",
        type=float,
        default=0.0,
        help="Sleep used after a fresh in-window BTC-5m copy event is detected.",
    )
    parser.add_argument(
        "--event-triggered-cycle-max-signal-age-s",
        type=float,
        default=0.0,
        help="0 means reuse --live-build-max-observed-age-s when deciding whether an event is fresh enough to wake.",
    )
    parser.add_argument("--max-runtime-s", type=float, default=0.0, help="0 disables the runtime cap.")
    parser.add_argument(
        "--guard-slow-path-every-n-cycles",
        type=int,
        default=4,
        help="Run non-submission reporting paths every N guard cycles; the live hot path still runs every cycle.",
    )
    parser.add_argument(
        "--cap-step-revert-every-n-cycles",
        type=int,
        default=16,
        help="Cadence the ledger/resolution-backed cap-step revert audit off the submit hot path.",
    )
    parser.add_argument(
        "--active-set-runtime-refresh-every-n-cycles",
        type=int,
        default=16,
        help="Refresh active-set selection every N cycles and reuse the selected live member between refreshes.",
    )
    parser.add_argument(
        "--post-live-reporting-every-n-cycles",
        type=int,
        default=8,
        help="Cadence alternate-source and coverage reporting outside the live submit path.",
    )
    parser.add_argument("--pipeline-limit", type=int, default=100)
    parser.add_argument("--pipeline-pages", type=int, default=1)
    parser.add_argument("--pipeline-timeout-s", type=float, default=45.0)
    parser.add_argument("--live-timeout-s", type=float, default=45.0)
    parser.add_argument("--pipeline-data-api-timeout-s", type=float, default=1.5)
    parser.add_argument("--pipeline-data-api-retries", type=int, default=1)
    parser.add_argument("--pipeline-data-api-trade-query-keys", default="user")
    parser.add_argument("--pipeline-include-activity", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--max-event-age-s", type=float, default=30.0)
    parser.add_argument("--live-build-max-observed-age-s", type=float, default=30.0)
    parser.add_argument("--min-live-order-usd", type=float, default=1.0)
    parser.add_argument("--max-intents", type=int, default=6)
    parser.add_argument("--wallet-fraction", type=float, default=0.10)
    parser.add_argument("--max-order-usd", type=float, default=8.0)
    parser.add_argument("--alpha-decay-report", default="data/research/alpha_decay_report.json")
    parser.add_argument("--toxicity-denylist-config", default="configs/wallet_copy/toxicity_denylist.json")
    parser.add_argument("--enable-drift-buffer", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-drift-buffer-price", type=float, default=0.05)
    parser.add_argument("--enable-maker-fallback", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--copy-model",
        choices=("per_order", "inventory", "drip"),
        default="drip",
        help="LIVE copy model. drip emits inventory micro-tranches through the same guard.",
    )
    parser.add_argument("--inventory-late-window-stop-s", type=float, default=60.0)
    parser.add_argument("--inventory-max-converge-orders-per-window", type=int, default=6)
    parser.add_argument("--inventory-best-ask-timeout-s", type=float, default=1.0)
    parser.add_argument("--inventory-future-window-lookahead-s", type=float, default=INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S)
    parser.add_argument("--drip-min-tranche-usd", type=float, default=1.0)
    parser.add_argument("--drip-max-tranche-usd", type=float, default=2.5)
    parser.add_argument("--drip-max-tranches-per-window", type=int, default=12)
    parser.add_argument(
        "--per-window-fill-cap",
        type=int,
        default=_env_int("WALLET_COPY_PER_WINDOW_FILL_CAP", 1),
        help="Fable DEFEND cap: max filled-or-pending live rows per BTC-5m market_slug across all members; 0 disables.",
    )
    parser.add_argument(
        "--profit-latency-window-time-suppress-gte-s",
        type=float,
        default=_env_float(
            "WALLET_COPY_PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S",
            PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S,
        ),
        help="Suppress live BUY submissions once BTC-5m window time reaches this threshold; 0 disables.",
    )
    parser.add_argument(
        "--profit-latency-signal-age-suppress-gte-s",
        type=float,
        default=_env_float(
            "WALLET_COPY_PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S",
            PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S,
        ),
        help="Suppress live BUY submissions whose source signal age reaches this threshold; 0 disables.",
    )
    parser.add_argument(
        "--price-band-decision-since",
        default=os.getenv("WALLET_COPY_PRICE_BAND_DECISION_SINCE", "2026-07-04T15:01:00Z"),
        help="Live <=max-price measurement window persisted in guard output.",
    )
    parser.add_argument(
        "--price-band-decision-max-price",
        type=float,
        default=float(os.getenv("WALLET_COPY_PRICE_BAND_DECISION_MAX_PRICE", "0.50")),
    )
    parser.add_argument(
        "--price-band-decision-min-price",
        type=float,
        default=float(os.getenv("WALLET_COPY_PRICE_BAND_DECISION_MIN_PRICE", "0.25")),
    )
    parser.add_argument("--price-band-decision-min-resolved", type=int, default=10)
    parser.add_argument("--price-band-decision-min-fill-rate-pct", type=float, default=40.0)
    parser.add_argument("--price-band-decision-refresh-s", type=float, default=30.0)
    parser.add_argument(
        "--rtds-jsonl",
        default=DEFAULT_RTDS_ACTIVITY_JSONL,
        help="Optional RTDS activity jsonl used as the live detection history refresh source.",
    )
    parser.add_argument("--rtds-scan-limit", type=int, default=50_000)
    parser.add_argument("--rtds-max-new-events", type=int, default=500)
    parser.add_argument("--rtds-tail-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--rtds-cold-tail-bytes", type=int, default=DEFAULT_COLD_TAIL_BYTES)
    parser.add_argument("--rtds-offset-state", default="")
    parser.add_argument("--rtds-watermark-state", default="data/research/wallet_copy_rtds_observation_watermarks.json")
    parser.add_argument("--rtds-signal-watermark-state", default=DEFAULT_RTDS_SIGNAL_WATERMARK_STATE)
    parser.add_argument("--polygon-ws-premerge-jsonl", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--polygon-ws-premerge-tail-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument(
        "--orderfilled-sidecar-jsonl",
        default="data/research/polygon_orderfilled_ws_orderfilled_only.jsonl",
    )
    parser.add_argument(
        "--orderfilled-sidecar-gate-state",
        default="data/research/orderfilled_sidecar_parity_shadow_state.json",
    )
    parser.add_argument(
        "--orderfilled-sidecar-live-cursor-state",
        default="data/research/wallet_copy_orderfilled_sidecar_live_cursor_state.json",
    )
    parser.add_argument(
        "--orderfilled-fast-lane-wake-socket",
        default=DEFAULT_ORDERFILLED_FAST_LANE_WAKE_SOCKET,
    )
    parser.add_argument(
        "--orderfilled-fast-lane-state",
        default=DEFAULT_ORDERFILLED_FAST_LANE_STATE,
    )
    parser.add_argument(
        "--copy-source-wake-activation-state",
        default=DEFAULT_COPY_SOURCE_WAKE_ACTIVATION_STATE,
    )
    parser.add_argument(
        "--copy-source-identity-router-state",
        default=DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
    )
    parser.add_argument("--orderfilled-fast-lane-poll-s", type=float, default=0.1)
    parser.add_argument(
        "--orderfilled-fast-lane",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Consume gate-passed OrderFilled rows immediately in a generation-fenced thread inside the sole guard.",
    )
    parser.add_argument(
        "--active-member-orderfilled-hot-source-state",
        default="data/research/active_member_orderfilled_hot_source_shadow_state.json",
        help="Corrected paper-gated Polygon OrderFilled source; ignored until its preregistered gate passes.",
    )
    parser.add_argument(
        "--active-member-orderfilled-hot-source-cursor-state",
        default="data/research/active_member_orderfilled_hot_source_live_cursor.json",
    )
    parser.add_argument(
        "--active-member-orderfilled-hot-source-accumulator",
        default="data/research/active_member_orderfilled_hot_source_shadow_accumulator.json",
    )
    parser.add_argument(
        "--active-member-orderfilled-hot-source-bootstrap-tail-bytes",
        type=int,
        default=8 * 1024 * 1024,
    )
    parser.add_argument(
        "--active-member-orderfilled-accumulator-state",
        default="data/research/active_member_orderfilled_hot_source_shadow_accumulator.json",
    )
    parser.add_argument(
        "--active-member-orderfilled-direct-cursor-state",
        default="data/research/wallet_copy_orderfilled_direct_cursor_state.json",
    )
    parser.add_argument("--active-member-orderfilled-direct-bootstrap-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument(
        "--active-set-rtds-premerge",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Refresh RTDS/user-channel history for every active-set member before the selected member builds live intents.",
    )
    parser.add_argument(
        "--active-set-rtds-premerge-member-limit",
        type=int,
        default=0,
        help="0 means refresh every active-set member selected by the live contract.",
    )
    parser.add_argument("--active-set-dataapi-poller", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--active-set-dataapi-poller-state", default=DEFAULT_ACTIVE_SET_DATAAPI_POLLER_STATE)
    parser.add_argument("--active-set-dataapi-first-seen-jsonl", default="data/research/dataapi_first_seen.jsonl")
    parser.add_argument(
        "--active-set-live-execution-probes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run paper-only per-member live-execution probes before same-cycle guarded promotion.",
    )
    parser.add_argument(
        "--active-set-live-execution-probe-max-members",
        type=int,
        default=DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_MAX_MEMBERS,
    )
    parser.add_argument(
        "--active-set-evaluate-all-runtime-members-per-cycle",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Opt-in managed-restart mode: evaluate every non-selected runtime member "
            "paper-only each guard cycle while the selected member remains the sole live lane."
        ),
    )
    parser.add_argument("--active-set-dataapi-poller-interval-s", type=float, default=1.0)
    parser.add_argument("--active-set-dataapi-poller-limit", type=int, default=500)
    parser.add_argument("--active-set-dataapi-poller-pages", type=int, default=2)
    parser.add_argument("--active-set-dataapi-poller-timeout-s", type=float, default=2.0)
    parser.add_argument("--active-set-dataapi-poller-retries", type=int, default=1)
    parser.add_argument("--active-set-dataapi-poller-max-workers", type=int, default=8)
    parser.add_argument("--active-set-dataapi-poller-every-n-cycles", type=int, default=4)
    parser.add_argument("--active-set-live-execution-probes-every-n-cycles", type=int, default=16)
    parser.add_argument(
        "--active-set-dataapi-poller-cycle-offset",
        type=int,
        default=1,
        help="Cadence the read-only active-set Data API poller on the guard slow path.",
    )
    parser.add_argument(
        "--active-set-dataapi-poller-disable-source-base-overrides",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--active-set-dataapi-poller-trade-query-keys",
        default="",
        help="Defaults to --pipeline-data-api-trade-query-keys when empty.",
    )
    parser.add_argument(
        "--watch-tier-dataapi-poller",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Measure-only watch-tier Data API poller; writes only separate watch-tier files.",
    )
    parser.add_argument("--watch-tier-wallets-config", default=DEFAULT_WATCH_TIER_WALLETS_CONFIG)
    parser.add_argument("--watch-tier-history-state", default=DEFAULT_WATCH_TIER_HISTORY_STATE)
    parser.add_argument("--watch-tier-history-window-index", default=DEFAULT_WATCH_TIER_HISTORY_WINDOW_INDEX)
    parser.add_argument("--watch-tier-wallet-event-log", default=DEFAULT_WATCH_TIER_WALLET_EVENT_LOG)
    parser.add_argument("--watch-tier-dataapi-first-seen-jsonl", default=DEFAULT_WATCH_TIER_FIRST_SEEN)
    parser.add_argument("--watch-tier-dataapi-poller-state", default=DEFAULT_WATCH_TIER_POLLER_STATE)
    parser.add_argument("--watch-tier-dataapi-poller-interval-s", type=float, default=5.0)
    parser.add_argument("--watch-tier-dataapi-poller-every-n-cycles", type=int, default=24)
    parser.add_argument(
        "--watch-tier-dataapi-poller-cycle-offset",
        type=int,
        default=0,
        help="Stagger the watch-tier poller within the slow-path cadence so read-only jobs do not stack.",
    )
    parser.add_argument(
        "--watch-tier-dataapi-poller-max-wallets-per-cycle",
        type=int,
        default=4,
        help="Cap measure-only watch-tier Data API polling per due cycle; rotates through the list.",
    )
    parser.add_argument("--self-feed-ledger-diff", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--self-feed-ledger-diff-state", default=DEFAULT_SELF_FEED_VS_LEDGER_STATE)
    parser.add_argument("--self-feed-log", default=DEFAULT_SELF_FEED_LOG)
    parser.add_argument("--self-feed-ledger-diff-limit", type=int, default=200)
    parser.add_argument("--self-feed-ledger-diff-pages", type=int, default=10)
    parser.add_argument("--self-feed-ledger-diff-timeout-s", type=float, default=2.0)
    parser.add_argument("--self-feed-ledger-missing-grace-s", type=float, default=300.0)
    parser.add_argument("--self-feed-polygon", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--self-feed-polygon-lookback-blocks", type=int, default=700)
    parser.add_argument("--self-feed-ledger-diff-every-n-cycles", type=int, default=16)
    parser.add_argument("--self-feed-ledger-diff-cycle-offset", type=int, default=0)
    parser.add_argument("--shadow-lanes-every-n-cycles", type=int, default=16)
    parser.add_argument("--shadow-lanes-cycle-offset", type=int, default=2)
    parser.add_argument(
        "--fuse-hot-path",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run RTDS merge and live arm in-process to avoid per-cycle Python spawn latency.",
    )
    return parser.parse_args()


def _append_jsonl(path: str, payload: dict[str, Any]) -> None:
    target = ROOT / path if not Path(path).is_absolute() else Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return int(default)
    return parsed if parsed > 0 else int(default)


def _live_guard_runtime_history(args: argparse.Namespace) -> dict[str, Any]:
    enabled = bool(getattr(args, "live_guard_hot_history", True))
    research_history = str(
        getattr(args, "research_history_state", "")
        or getattr(args, "history_state", "")
        or "data/research/wallet_copy_history_state.json"
    )
    research_index = str(
        getattr(args, "research_history_window_index", "")
        or getattr(args, "history_window_index", "")
        or "data/research/wallet_copy_history_window_index.json"
    )
    hot_history = str(getattr(args, "live_guard_hot_history_state", "") or DEFAULT_LIVE_GUARD_HOT_HISTORY_STATE)
    hot_index = str(
        getattr(args, "live_guard_hot_history_window_index", "") or DEFAULT_LIVE_GUARD_HOT_HISTORY_WINDOW_INDEX
    )
    runtime_history = hot_history if enabled else research_history
    runtime_index = hot_index if enabled else research_index
    return {
        "enabled": enabled,
        "flow_stage": "LIVE/SELF-DEV",
        "history_state": runtime_history,
        "history_window_index": runtime_index,
        "research_history_state": research_history,
        "research_history_window_index": research_index,
        "retain_events": _positive_int(
            getattr(args, "live_guard_hot_history_retain_events", DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_EVENTS),
            DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_EVENTS,
        )
        if enabled
        else 250_000,
        "retain_copy_intents": _positive_int(
            getattr(
                args,
                "live_guard_hot_history_retain_copy_intents",
                DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_COPY_INTENTS,
            ),
            DEFAULT_LIVE_GUARD_HOT_HISTORY_RETAIN_COPY_INTENTS,
        )
        if enabled
        else 250_000,
        "rule": "live guard owns a compact runtime history; research history remains the offline source of record",
    }


def _with_live_guard_runtime_history(args: argparse.Namespace) -> argparse.Namespace:
    runtime = _live_guard_runtime_history(args)
    if not runtime["enabled"]:
        return args
    run_args = argparse.Namespace(**vars(args))
    run_args.research_history_state = runtime["research_history_state"]
    run_args.research_history_window_index = runtime["research_history_window_index"]
    run_args.history_state = runtime["history_state"]
    run_args.history_window_index = runtime["history_window_index"]
    run_args.live_guard_runtime_history = runtime
    return run_args


def _history_retain_events(args: argparse.Namespace) -> int:
    runtime = _live_guard_runtime_history(args)
    return int(runtime["retain_events"])


def _history_retain_copy_intents(args: argparse.Namespace) -> int:
    runtime = _live_guard_runtime_history(args)
    return int(runtime["retain_copy_intents"])


def _persist_built_copy_intents(args: argparse.Namespace, live_stdout: dict[str, Any]) -> dict[str, Any]:
    built_intents = live_stdout.get("built_intents")
    built_intents = built_intents if isinstance(built_intents, list) else []
    target = str(
        getattr(args, "live_guard_copy_intents_state", "")
        or DEFAULT_LIVE_GUARD_COPY_INTENTS_STATE
    )
    if not built_intents:
        return {
            "status": "NO_BUILT_COPY_INTENTS",
            "inserted": 0,
            "state": target,
        }
    try:
        existing = load_json(target, default={})
        existing = existing if isinstance(existing, dict) else {}
        by_id = {
            str(row.get("intent_id")): row
            for row in existing.get("copy_intents") or []
            if isinstance(row, dict) and str(row.get("intent_id") or "")
        }
        before = len(by_id)
        for row in built_intents:
            if isinstance(row, dict) and str(row.get("intent_id") or ""):
                by_id[str(row["intent_id"])] = row
        retain = _history_retain_copy_intents(args)
        rows = sorted(
            by_id.values(),
            key=lambda row: (float(row.get("observed_ts") or 0.0), str(row.get("intent_id") or "")),
        )[-retain:]
        guard_code_identity = (
            getattr(args, "guard_code_identity", {})
            if isinstance(getattr(args, "guard_code_identity", {}), dict)
            else {}
        )
        status = "COPY_INTENTS_SIDECAR_UPDATED"
        payload = {
            "schema_version": 1,
            "kind": "wallet_copy_live_guard_copy_intents_state",
            "flow_stage": "LIVE/LEARN/SELF-DEV",
            "generated_at": utc_now_iso(),
            "status": status,
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
            "writer_pid": os.getpid(),
            "writer_guard_started_at": guard_code_identity.get("started_at_utc"),
            "writer_git_head_at_launch": guard_code_identity.get("git_head_at_launch"),
            "writer_live_guard_generation_sha256": guard_code_identity.get(
                "live_guard_generation_sha256"
            ),
            "copy_intents": rows,
            "copy_intent_count": len(rows),
            "inserted": max(0, len(by_id) - before),
        }
        atomic_write_json(target, payload)
        return {
            "status": status,
            "inserted": payload["inserted"],
            "copy_intent_count": len(rows),
            "state": target,
        }
    except Exception as exc:  # pragma: no cover - evidence persistence cannot stop live submission.
        return {
            "status": "COPY_INTENT_HISTORY_MERGE_ERROR",
            "inserted": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _cadence_every_n(args: argparse.Namespace, attr: str) -> int:
    fallback = int(getattr(args, "guard_slow_path_every_n_cycles", 4) or 4)
    raw = int(getattr(args, attr, 0) or fallback)
    return max(1, raw)


def _cadence_offset(args: argparse.Namespace, attr: str, *, every_n: int) -> int:
    raw = int(getattr(args, attr, 0) or 0)
    return max(0, raw) % max(1, int(every_n))


def _cadence_due(cycle: int, every_n: int, offset: int = 0) -> bool:
    return every_n <= 1 or (max(1, int(cycle)) - 1 - max(0, int(offset))) % every_n == 0


def _cadence_payload(
    payload: dict[str, Any],
    *,
    name: str,
    generated_at: str,
    cycle: int,
    every_n: int,
    offset: int = 0,
    executed: bool,
) -> dict[str, Any]:
    out = dict(payload) if isinstance(payload, dict) else {}
    if not out:
        out = {
            "flow_stage": "LIVE/LEARN",
            "status": "CADENCE_WAITING_FOR_FIRST_RUN",
            "paper_only": True,
            "live_orders_allowed": False,
        }
    prior_cadence = out.get("cadence") if isinstance(out.get("cadence"), dict) else {}
    out["cadence"] = {
        **prior_cadence,
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "name": name,
        "cycle": int(cycle),
        "every_n_cycles": int(every_n),
        "cycle_offset": int(offset),
        "executed_this_cycle": bool(executed),
        "checked_at": generated_at,
        "reason": "cadence_reports_per_task_due_status; every_n_1_runs_on_hot_path",
    }
    if executed:
        out["cadence"]["last_executed_at"] = generated_at
        out["cadence"]["last_executed_cycle"] = int(cycle)
    return out


def _cadenced_root_mirror(
    current: dict[str, Any],
    previous: dict[str, Any],
    *,
    generated_at: str,
    executed_this_cycle: bool,
) -> dict[str, Any]:
    if current:
        out = dict(current)
        out["last_observed_at"] = generated_at
        out["executed_this_cycle"] = bool(executed_this_cycle)
        return out
    out = dict(previous) if isinstance(previous, dict) else {}
    if not out:
        out = {
            "status": "CADENCE_WAITING_FOR_FIRST_OBSERVATION",
            "last_observed_at": None,
        }
    out["executed_this_cycle"] = False
    out["staleness"] = {
        "status": "CARRIED_FORWARD_FROM_PREVIOUS_STATE",
        "checked_at": generated_at,
    }
    return out


def _detect_interval_values(
    cycle_duration_series: list[dict[str, Any]],
    *,
    default_every_n: int,
) -> list[float]:
    values: list[float] = []
    for row in cycle_duration_series:
        if not isinstance(row, dict) or not isinstance(row.get("cycle_duration_s"), (int, float)):
            continue
        effective = round(float(row["cycle_duration_s"]), 6)
        row["active_set_dataapi_poller_every_n_cycles"] = max(
            1,
            int(row.get("active_set_dataapi_poller_every_n_cycles") or default_every_n),
        )
        row["effective_detect_interval_s"] = effective
        values.append(effective)
    return values


def _interval_health(values: list[float], *, freshness_budget_s: float) -> dict[str, Any]:
    sorted_values = sorted(values)
    median = None
    if sorted_values:
        mid = len(sorted_values) // 2
        median = (
            sorted_values[mid]
            if len(sorted_values) % 2
            else (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
        )
    return {
        "status": (
            "WARN_DETECT_INTERVAL_EXCEEDS_FRESHNESS_BUDGET"
            if values and max(values) > freshness_budget_s
            else "OK"
        ),
        "sample_count": len(values),
        "required_sample_count": 12,
        "median_s": None if median is None else round(median, 6),
        "max_s": round(max(values), 6) if values else None,
        "freshness_budget_s": freshness_budget_s,
        "rule": "effective detect interval is guard cycle duration multiplied by active-set dataapi cadence",
    }


def _last_json(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = text.rfind("\n{")
    if start >= 0:
        try:
            return json.loads(text[start + 1 :])
        except json.JSONDecodeError:
            return {}
    return {}


_BENIGN_NO_SUBMIT_INTENT_BLOCKERS = {
    "drift_buffer_filtered_candidate_copy_intents",
    "hard_entry_cap_filtered_candidate_copy_intents",
    "hard_entry_floor_filtered_candidate_copy_intents",
    "entry_price_band_gate_filtered_candidate_copy_intents",
    "inventory_best_ask_filtered_candidate_copy_intents",
    "market_buy_precision_infeasible_candidate_copy_intents",
    "profit_latency_suppression_filtered_candidate_copy_intents",
    "window_fill_cap_filtered_candidate_copy_intents",
}


_MISSION_ACTIVE_SET_ADMISSION_STATUSES = {
    "AUTO_DEGRADE_PROTECTION_BOUNDED",
    "AUTO_DEGRADE_RUNTIME_ROSTER_PROTECTION_BOUNDED",
    "EXPANSION_MEMBER_PROBATION",
    "FABLE_0608_ADMITTED_STANDARD_POLICY",
    "FABLE_1134_LIVING_QUEUE_REFILL_ADMITTED",
    "FABLE_1415_BACKFILL_RANKED_QUEUE_TOP",
    "FABLE_1622_RANK1_READY_QUEUE_ADMITTED",
    "FABLE_1718_RANK2_READY_QUEUE_ADMITTED",
    "FABLE_1908_REMOTE_FLOW_RETAINED_DELIVERY_FIX_DUE",
    "FABLE_1908_REMOTE_VERIFIED_HALF_SIZE_ADMITTED",
    "FABLE_1934_HALF_SIZE_PIN_CLEARANCE_READY",
    "FABLE_1925_REMOTE_VERIFIED_HALF_SIZE_ADMITTED",
    "FABLE_2145_BUCKET_CONCENTRATION_READMIT_PROVEN_CELL",
    "FABLE_1413_A95B_LIVE_DEFEND_CAP1",
    "FABLE_ROTATION_READY_QUEUE_ADMITTED_BY_DEADMAN_MECHANICAL_RULE",
    "FABLE_PIN_BOOKCOVERED_PASS",
    "MEMBER_BAR_QUALIFIED",
    "PRE_LOCK_MEMBER",
}


_FABLE_LIVE_STATUS_REGISTRY = {
    "POLICY_CHOKE_RUNG_DIRECT_EMERGENCY_ADMISSION": {
        "direction_id": "2026-08-01T12:40Z-operator-absolute-seat-fill-preemption",
        "live_admissible": True,
    },
    "FABLE_1413_A95B_LIVE_DEFEND_CAP1": {
        "direction_id": FABLE_1413_E4_RATIFIED_DIRECTION_ID,
        "live_admissible": True,
    },
    "FABLE_1554_3048_COMPLETE_HISTORY_1USD_PROBE_CONFIG_RELOAD": {
        "direction_id": "2026-07-13T15:54Z-fable-3048-complete-history-probe",
        "live_admissible": True,
    },
    "FRIDAY_GOLDEN_RESTORED_FABLE_20260713T1532": {
        "direction_id": "2026-07-13T15:32Z-fable-friday-golden-restore",
        "live_admissible": True,
    },
    "FABLE_1912_DEADMAN_CORRECTED_MICROPROBE": {
        "direction_id": "2026-07-11T19:12Z-fable-corrected-copyability-gate-promote-c50d",
        "live_admissible": True,
    },
    "FABLE_0142_WEEKEND_PROVEN_SEAT_ADMITTED": {
        "direction_id": "2026-08-02T01:42Z-fable-weekend-proven-seat-admission",
        "live_admissible": True,
    },
    "FABLE_0831_PATH_B_LIVENESS_EXEMPT_RUNTIME_ADMISSION": {
        "direction_id": "2026-08-03T08:14Z-fable-01a-align-feedstock-reseat",
        "live_admissible": True,
    },
    # Registered fail-closed: ORDER (10) explicitly forbids authorizing these
    # other currently enabled overlay statuses.
    "AUTO_DEGRADE_RUNTIME_ROSTER_PROTECTION_BOUNDED": {
        "direction_id": "2026-07-09T12:57Z-fable-runtime-roster-auto-degrade",
        "live_admissible": False,
    },
    "FABLE_1507_READY_QUEUE_ROTATION_ADMITTED": {
        "direction_id": "2026-07-09T15:07Z-fable-soak-failure-checkpoint2",
        "live_admissible": False,
    },
    "FABLE_1704_MASS_ADMISSION_WAVE_TOP10_COMPLETE_HISTORY": {
        "direction_id": "2026-07-14T17:04Z-fable-mass-admission-wave",
        "live_admissible": False,
    },
    "FABLE_1711_EE888F_WEEKDAY_ADMISSION_ARMED_NOT_BEFORE": {
        "direction_id": "2026-07-20T17:11Z-fable-ee888f-weekday-admission",
        "live_admissible": False,
    },
    "FABLE_1704_MASS_ADMISSION_WAVE_TOP10_COMPLETE_HISTORY|FABLE_0831_01A_MAX_PRICE_CLAMP": {
        "direction_id": "2026-08-03T08:14Z-fable-01a-align-feedstock-reseat",
        "live_admissible": False,
    },
    "FABLE_1711_EE888F_WEEKDAY_ADMISSION_ARMED_NOT_BEFORE|FABLE_0831_01A_MAX_PRICE_CLAMP": {
        "direction_id": "2026-08-03T08:14Z-fable-01a-align-feedstock-reseat",
        "live_admissible": False,
    },
    "AUTO_DISABLED_CELL_FIRST_SLICE_BREACH": {
        "direction_id": "2026-07-31T03:52Z-fable-persist-82c8-first-slice-kill",
        "live_admissible": False,
    },
    "DEMOTED_FABLE_SUBSTITUTE_ROTATION": {
        "direction_id": "AUTONOMOUS_FLOW_ROTATE_ROLLING_LOSS_DEMOTION",
        "live_admissible": False,
    },
}

_EXPLICIT_FABLE_LIVE_ADMISSIBLE_STATUSES = frozenset(
    status
    for status, registration in _FABLE_LIVE_STATUS_REGISTRY.items()
    if registration.get("live_admissible") is True
)


def _unregistered_enabled_overlay_statuses(overlay: dict[str, Any]) -> list[str]:
    return sorted(
        {
            str(member.get("status") or "")
            for member in overlay.get("members") or []
            if isinstance(member, dict)
            and member.get("enabled") is not False
            and str(member.get("status") or "") not in _FABLE_LIVE_STATUS_REGISTRY
        }
    )


def _live_status_registration_audit(overlay: dict[str, Any]) -> dict[str, Any]:
    unregistered = _unregistered_enabled_overlay_statuses(overlay)
    return {
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "status": "PASS" if not unregistered else "UNREGISTERED_ENABLED_STATUS",
        "unregistered_enabled_statuses": unregistered,
        "registered_status_count": len(_FABLE_LIVE_STATUS_REGISTRY),
        "live_admissible_statuses": sorted(_EXPLICIT_FABLE_LIVE_ADMISSIBLE_STATUSES),
        "rule": "enabled overlay member statuses must be explicitly registered live-admissible or fail-closed",
    }


def _candidate_status_live_admissible(candidate: dict[str, Any]) -> bool:
    status = str(candidate.get("status") or "")
    if status == "PASS":
        return True
    if status in _EXPLICIT_FABLE_LIVE_ADMISSIBLE_STATUSES:
        return True
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return metadata.get("source") == "mission_active_live_set" and status in _MISSION_ACTIVE_SET_ADMISSION_STATUSES


def _apply_weekend_proven_seat_admission(
    members: list[dict[str, Any]], *, now: dt.datetime
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Cycle-scoped admission; never persists or broadens the July status."""
    state, state_path = _external_liveness_state()
    own_state = {
        key: state.get(key)
        for key in ("generated_at", "rows", "selected_rows", "remote_dataapi_24h")
        if key in state
    }
    own_rows = _external_liveness_rows_by_wallet(own_state)
    output: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for raw in members:
        member = dict(raw)
        wallet = _member_wallet(member)
        prior_status = str(member.get("status") or "")
        temporal = member.get("temporal_slice_evaluation") if isinstance(member.get("temporal_slice_evaluation"), dict) else {}
        evaluated = temporal.get("evaluated_slices") if isinstance(temporal.get("evaluated_slices"), list) else []
        positive = next(
            (
                row for row in evaluated
                if isinstance(row, dict)
                and str(row.get("slice") or "") == "weekend"
                and str(row.get("label") or "").upper() == "PROVEN-POSITIVE"
                and int(row.get("resolved_trades") or 0) >= 30
                and float(row.get("roi_pct") or 0.0) > 0.0
            ),
            None,
        )
        liveness = _external_liveness_gate_for_wallet(
            wallet,
            rows_by_wallet=own_rows,
            state=own_state,
            state_path=state_path,
            now=now,
            max_age_h=6.0,
        )
        checks = {
            "prior_status_is_fable_1704": prior_status == "FABLE_1704_MASS_ADMISSION_WAVE_TOP10_COMPLETE_HISTORY",
            "uncensored_own_liveness_lt_6h": liveness.get("passed") is True and not liveness.get("censored"),
            "weekend_proven_positive_n_gte_30_roi_gt_0": positive is not None,
            "not_temporal_excluded": temporal.get("excluded") is not True,
            "total_loss_clean_by_upstream_construction": True,
        }
        admitted = all(checks.values())
        tripwire = (
            _weekend_proven_seat_tripwire(
                wallet,
                now=now,
                state_path=WEEKEND_PROVEN_SEAT_STATE,
                ledger_path=WALLET_COPY_LIVE_EXECUTION_STATE,
            )
            if admitted
            else {"allows_admission": True, "status": "NOT_EVALUATED_BAR_FAILED"}
        )
        admitted = admitted and tripwire.get("allows_admission") is True
        decision = {
            "source_wallet": wallet,
            "admitted": admitted,
            "checks": checks,
            "prior_status": prior_status,
            "liveness": liveness,
            "temporal_evidence": positive,
            "direction_id": "2026-08-02T01:42Z-fable-weekend-proven-seat-admission",
            "tripwires": {
                "first_8_accepted_net_lte_usd": -4.0,
                "zero_accepted_after_h": 6.0,
                "zero_accepted_verdict": "INSUFFICIENT_SUPPLY",
                "revert_status": "FABLE_1704_MASS_ADMISSION_WAVE_TOP10_COMPLETE_HISTORY",
                "identity_cooloff_h": 24.0,
            },
            "tripwire_state": tripwire,
        }
        if admitted:
            member["status"] = "FABLE_0142_WEEKEND_PROVEN_SEAT_ADMITTED"
            member["weekend_proven_seat_admission"] = decision
        output.append(member)
        decisions.append(decision)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE/PROMOTE/DEFEND",
        "admitted_wallets": [row["source_wallet"] for row in decisions if row["admitted"]],
        "decisions": decisions,
        "self_reverting": True,
        "rule": "runtime-only stamp is recomputed every cycle; any failed bar restores prior persisted status",
    }


def _weekend_proven_seat_tripwire(
    wallet: str,
    *,
    now: dt.datetime,
    state_path: Path = WEEKEND_PROVEN_SEAT_STATE,
    ledger_path: Path = WALLET_COPY_LIVE_EXECUTION_STATE,
    resolutions_path: Path = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
) -> dict[str, Any]:
    state = load_json(state_path, default={})
    state = state if isinstance(state, dict) else {}
    cooloff_until = _parse_iso_datetime(state.get("cooloff_until"))
    if cooloff_until is not None and now < cooloff_until:
        return {**state, "allows_admission": False, "status": "IDENTITY_COOLOFF"}
    first_pass = _parse_iso_datetime(state.get("first_pass_at"))
    if first_pass is None or (cooloff_until is not None and now >= cooloff_until):
        first_pass = now
        state = {
            "flow_stage": "LIVE/PROMOTE/ROTATE",
            "direction_id": "2026-08-02T01:42Z-fable-weekend-proven-seat-admission",
            "source_wallet": wallet,
            "first_pass_at": now.isoformat(),
            "status": "WATCH",
        }
    ledger = load_json(ledger_path, default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) and isinstance(ledger.get("orders"), list) else []
    accepted: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict) or _member_wallet(order) != wallet:
            continue
        submitted = _parse_iso_datetime(order.get("submitted_at") or order.get("created_at"))
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if submitted is None or submitted < first_pass or status in {"", "REJECTED", "REFUSED", "SKIPPED", "BLOCKED"}:
            continue
        if not str(order.get("order_id") or "").strip() or bool(order.get("paper_only")):
            continue
        accepted.append(order)
    accepted.sort(key=lambda row: str(row.get("submitted_at") or row.get("created_at") or ""))
    first_eight = accepted[:8]
    resolutions = load_resolutions(str(resolutions_path)) if first_eight else {}
    scored = [score_order(order, resolutions) for order in first_eight]
    resolved = [row for row in scored if row.get("resolved")]
    net_usd = round(sum(float(row.get("pnl_usd") or 0.0) for row in resolved), 6)
    age_h = max(0.0, (now - first_pass).total_seconds() / 3600.0)
    outcome = "WATCH"
    if net_usd <= -4.0:
        outcome = "REVERTED_FIRST_8_NET_LTE_MINUS_4"
    elif not accepted and age_h >= 6.0:
        outcome = "INSUFFICIENT_SUPPLY"
    if outcome != "WATCH":
        state.update({
            "status": outcome,
            "verdict": outcome,
            "reverted_at": now.isoformat(),
            "cooloff_until": (now + dt.timedelta(hours=24)).isoformat(),
        })
    state.update({
        "accepted_orders": len(first_eight),
        "resolved_orders": len(resolved),
        "net_resolved_pnl_usd": net_usd,
        "age_h": round(age_h, 6),
        "allows_admission": outcome == "WATCH",
    })
    atomic_write_json(state_path, state)
    return state


def _live_execution_filtered_no_submit(live_stdout: dict[str, Any]) -> bool:
    if str(live_stdout.get("status") or "") not in {
        "CORRECTION",
        "LIVE_PLAN_PROTECTED_NO_SURVIVOR",
    }:
        return False
    intent_blockers = {str(blocker) for blocker in live_stdout.get("intent_blockers") or [] if str(blocker)}
    if not intent_blockers or not intent_blockers.issubset(_BENIGN_NO_SUBMIT_INTENT_BLOCKERS):
        return False
    for key in ("proof_blockers", "operator_gate_blockers", "token_mapping_blockers"):
        if live_stdout.get(key):
            return False
    return True


def _live_drought_funnel(
    *,
    live_stdout: dict[str, Any],
    dataapi_poll_result: dict[str, Any],
    active_set_rtds_premerge: dict[str, Any],
    window_participation: dict[str, Any],
) -> dict[str, Any]:
    intent_summary = live_stdout.get("candidate_intent_summary")
    intent_summary = intent_summary if isinstance(intent_summary, dict) else {}
    poll_summary = dataapi_poll_result.get("summary") if isinstance(dataapi_poll_result.get("summary"), dict) else {}
    reject_taxonomy: dict[str, int] = {}
    for key in LIVE_DROUGHT_FUNNEL_GATE_KEYS:
        summary = intent_summary.get(key) if isinstance(intent_summary.get(key), dict) else live_stdout.get(key)
        if not isinstance(summary, dict):
            continue
        for taxonomy, count in (summary.get("taxonomy_counts") or {}).items():
            reject_taxonomy[str(taxonomy)] = reject_taxonomy.get(str(taxonomy), 0) + int(count or 0)
        blocked = int(summary.get("blocked_intents") or summary.get("filtered_intents") or 0)
        reason = str(summary.get("reject_reason") or key)
        if blocked > 0 and reason not in reject_taxonomy:
            reject_taxonomy[reason] = reject_taxonomy.get(reason, 0) + blocked
    prefilter = intent_summary.get("live_event_prefilter")
    if not isinstance(prefilter, dict):
        prefilter = live_stdout.get("live_event_prefilter") if isinstance(live_stdout.get("live_event_prefilter"), dict) else {}
    prefilter_or_policy_rejects = 0
    for prefix, counts in (
        ("prefilter", prefilter.get("skip_counts") if isinstance(prefilter, dict) else {}),
        ("policy", prefilter.get("policy_reject_counts") if isinstance(prefilter, dict) else {}),
    ):
        if not isinstance(counts, dict):
            continue
        for reason, count in counts.items():
            value = int(count or 0)
            if value <= 0:
                continue
            prefilter_or_policy_rejects += value
            key = f"{prefix}:{reason}"
            reject_taxonomy[key] = reject_taxonomy.get(key, 0) + value
    window_reasons = window_participation.get("dominant_skip_reason_counts")
    window_reasons = window_reasons if isinstance(window_reasons, dict) else {}
    for reason, count in window_reasons.items():
        reject_taxonomy.setdefault(f"window:{reason}", int(count or 0))

    return {
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN",
        "status": "PASS",
        "events_read": int(intent_summary.get("history_events") or 0),
        "source_events": int(intent_summary.get("source_events") or 0),
        "active_set_signal_rows": int(poll_summary.get("poll_only_signals") or 0),
        "active_set_fresh_signal_rows": int(poll_summary.get("fresh_poll_only_signals") or 0),
        "active_set_rtds_new_matching_events": int(active_set_rtds_premerge.get("new_matching_events") or 0),
        "candidate_build_events": int(intent_summary.get("candidate_build_events") or 0),
        "base_intents": int(intent_summary.get("base_intents") or 0),
        "candidate_intents": int(intent_summary.get("candidate_intents") or 0),
        "fresh_candidate_intents": int(live_stdout.get("fresh_candidate_intents") or intent_summary.get("fresh_candidate_intents") or 0),
        "fresh_after_drift_buffer": int(intent_summary.get("fresh_candidate_intents_after_drift_buffer") or 0),
        "fresh_after_inventory_best_ask_gate": int(
            intent_summary.get("fresh_candidate_intents_after_inventory_best_ask_gate") or 0
        ),
        "fresh_after_hard_entry_cap": int(intent_summary.get("fresh_candidate_intents_after_hard_entry_cap") or 0),
        "fresh_after_toxicity_protection": int(intent_summary.get("fresh_candidate_intents_after_toxicity_protection") or 0),
        "fresh_after_expected_fee_gate": int(intent_summary.get("fresh_candidate_intents_after_expected_fee_gate") or 0),
        "new_live_candidate_intents": int(live_stdout.get("new_live_candidate_intents") or 0),
        "orders_submitted": int(live_stdout.get("orders_submitted") or 0),
        "reject_taxonomy_counts": dict(sorted(reject_taxonomy.items())),
        "intent_blockers": sorted({str(item) for item in live_stdout.get("intent_blockers") or [] if str(item)}),
        "diagnosis_hint": (
            "toxicity_gate_dominates"
            if int(reject_taxonomy.get("toxicity_protection") or 0) > 0
            else "event_prefilter_or_policy_gate"
            if prefilter_or_policy_rejects > 0 and int(intent_summary.get("candidate_intents") or 0) <= 0
            else "candidate_events_filtered_before_intent"
            if int(intent_summary.get("source_events") or 0) > 0 and int(intent_summary.get("candidate_intents") or 0) <= 0
            else "signal_or_intent_build_drought"
            if int(intent_summary.get("candidate_build_events") or 0) <= 0
            and int(intent_summary.get("candidate_intents") or 0) <= 0
            else "post_build_filter_or_dedupe_drought"
        ),
        "next_action": "compare this funnel across consecutive cycles before changing admission or pricing rules",
    }


def _run_command(argv: list[str], *, timeout_s: float) -> dict[str, Any]:
    started = time.time()
    try:
        completed = subprocess.run(
            argv,
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=max(1.0, float(timeout_s)),
            check=False,
        )
        return {
            "argv": argv,
            "returncode": completed.returncode,
            "duration_s": round(time.time() - started, 6),
            "stdout_json": _last_json(completed.stdout),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "argv": argv,
            "returncode": 124,
            "duration_s": round(time.time() - started, 6),
            "stdout_json": _last_json(exc.stdout or ""),
            "stdout_tail": str(exc.stdout or "")[-4000:],
            "stderr_tail": str(exc.stderr or "")[-4000:],
            "timeout_s": float(timeout_s),
        }


def _callable_result(
    *,
    argv: list[str],
    started: float,
    returncode: int,
    stdout_json: dict[str, Any],
    mode: str,
    stderr_tail: str = "",
) -> dict[str, Any]:
    stdout_tail = json.dumps(stdout_json, indent=2, sort_keys=True, default=str)
    return {
        "argv": argv,
        "returncode": int(returncode),
        "duration_s": round(time.time() - started, 6),
        "stdout_json": stdout_json,
        "stdout_tail": stdout_tail[-4000:],
        "stderr_tail": stderr_tail[-4000:],
        "execution_mode": mode,
    }


def _run_fused_pipeline(args: argparse.Namespace, *, source_wallet: str) -> dict[str, Any]:
    argv = _pipeline_command(args, source_wallet=source_wallet)
    if not (bool(getattr(args, "fuse_hot_path", True)) and str(getattr(args, "rtds_jsonl", "") or "")):
        return _run_command(argv, timeout_s=float(args.pipeline_timeout_s))
    started = time.time()
    wallet_name = f"live_primary_{source_wallet[-8:]}" if source_wallet else "live_primary"
    offset_state = _rtds_offset_state(args, source_wallet=source_wallet)
    merge_args = argparse.Namespace(
        rtds_jsonl=str(args.rtds_jsonl),
        source_wallet=source_wallet,
        wallet_name=wallet_name,
        history_state=args.history_state,
        history_window_index=str(
            getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")
        ),
        wallet_event_log=args.wallet_event_log,
        scan_limit=int(args.rtds_scan_limit),
        tail_bytes=_rtds_tail_backfill_bytes(args),
        cold_tail_bytes=int(getattr(args, "rtds_cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES) or DEFAULT_COLD_TAIL_BYTES),
        offset_state=offset_state,
        watermark_state=str(getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")),
        max_new_events=int(args.rtds_max_new_events),
        history_retain_events=_history_retain_events(args),
        history_retain_copy_intents=_history_retain_copy_intents(args),
    )
    try:
        summary = _run_rtds_merge(merge_args)
        return _callable_result(
            argv=argv,
            started=started,
            returncode=0,
            stdout_json=summary,
            mode="in_process",
        )
    except Exception as exc:  # pragma: no cover - defensive live evidence
        return _callable_result(
            argv=argv,
            started=started,
            returncode=1,
            stdout_json={},
            mode="in_process",
            stderr_tail=f"{type(exc).__name__}: {exc}",
        )


def _active_set_poll_wallets(active_set_runtime: dict[str, Any], *, fallback_wallet: str = "") -> list[str]:
    runtime_members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    members = runtime_members if runtime_members else _active_live_set_members_contract()
    wallets: list[str] = []
    for member in members:
        if not isinstance(member, dict):
            continue
        member = _normalize_active_set_member_runtime(member)
        if _active_set_member_is_disabled(member):
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            wallets.append(wallet)
    if not wallets and fallback_wallet and not runtime_members:
        wallet = str(fallback_wallet or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            wallets.append(wallet)
    return list(dict.fromkeys(wallets))


def _selected_wallet_first(wallets: list[str], selected_wallet: str = "") -> list[str]:
    selected = str(selected_wallet or "").strip().lower()
    ordered = list(dict.fromkeys(str(wallet or "").strip().lower() for wallet in wallets if wallet))
    if not (selected.startswith("0x") and len(selected) == 42):
        return ordered
    if selected not in ordered:
        return ordered
    return [selected, *(wallet for wallet in ordered if wallet != selected)]


def _latest_auto_degrade_admission_wallet() -> str:
    overlay = _load_auto_degrade_active_set_overlay()
    latest = overlay.get("latest_admission") if isinstance(overlay.get("latest_admission"), dict) else {}
    if not latest or _active_set_member_is_disabled(latest):
        return ""
    wallet = str(latest.get("source_wallet") or latest.get("wallet") or "").strip().lower()
    if wallet.startswith("0x") and len(wallet) == 42:
        return wallet
    return ""


def _meta_first_number(payload: Any, keys: set[str]) -> float | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key) in keys:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
            found = _meta_first_number(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _meta_first_number(item, keys)
            if found is not None:
                return found
    return None


def _parse_iso_datetime(raw: Any) -> dt.datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _order_source_wallet(order: dict[str, Any]) -> str:
    candidates: list[Any] = [
        order.get("source_wallet"),
        order.get("wallet"),
        (order.get("wallet_copy") if isinstance(order.get("wallet_copy"), dict) else {}).get("source_wallet"),
        (order.get("wallet_copy_inventory") if isinstance(order.get("wallet_copy_inventory"), dict) else {}).get(
            "source_wallet"
        ),
    ]
    trade_decision = order.get("trade_decision") if isinstance(order.get("trade_decision"), dict) else {}
    wallet_copy = trade_decision.get("wallet_copy") if isinstance(trade_decision.get("wallet_copy"), dict) else {}
    candidates.append(wallet_copy.get("source_wallet"))
    metadata = wallet_copy.get("metadata") if isinstance(wallet_copy.get("metadata"), dict) else {}
    inventory_v2 = metadata.get("inventory_v2") if isinstance(metadata.get("inventory_v2"), dict) else {}
    candidates.append(inventory_v2.get("source_wallet"))
    for raw in candidates:
        wallet = str(raw or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            return wallet
    return ""


def _reconcile_alternate_source_rotation(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
) -> dict[str, Any]:
    """Persist source-correct alternate-fill PnL and its mechanical rotation result."""
    ledger = load_json(str(args.live_ledger_state), default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) and isinstance(ledger.get("orders"), list) else []
    resolutions = load_resolutions(str(args.resolutions))
    member_by_wallet = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower(): row
        for row in active_set_runtime.get("members") or []
        if isinstance(row, dict)
    }
    resolved_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    ledger_changed = False
    attributed_orders = 0
    for order in orders:
        if not isinstance(order, dict):
            continue
        attr = (
            order.get("alternate_transport_attribution")
            if isinstance(order.get("alternate_transport_attribution"), dict)
            else {}
        )
        capsule = order.get("parity_capsule") if isinstance(order.get("parity_capsule"), dict) else {}
        paper_intent = capsule.get("paper_intent") if isinstance(capsule.get("paper_intent"), dict) else {}
        metadata = paper_intent.get("metadata") if isinstance(paper_intent.get("metadata"), dict) else {}
        accepted_at_raw = attr.get("accepted_at") or order.get("submitted_at") or order.get("updated_at")
        accepted_at_dt = _parse_iso_datetime(accepted_at_raw)
        accepted_ts = accepted_at_dt.timestamp() if accepted_at_dt is not None else 0.0
        wallet = str(
            attr.get("source_wallet")
            or _order_source_wallet(order)
            or paper_intent.get("source_wallet")
            or ""
        ).lower()
        persisted_preplanned = bool(attr.get("plan_hash_verified"))
        if attr and not persisted_preplanned and accepted_ts < ALTERNATE_SOURCE_ATTRIBUTION_START_TS:
            order.pop("alternate_transport_attribution", None)
            ledger_changed = True
            continue
        is_source_scoped = bool(wallet) and accepted_ts >= ALTERNATE_SOURCE_ATTRIBUTION_START_TS
        is_alternate = persisted_preplanned or (
            accepted_ts >= ALTERNATE_SOURCE_ATTRIBUTION_START_TS
            and bool(
            metadata.get("alternate_detection_source") or metadata.get("alternate_observed_ts")
            )
        )
        # Source identity outranks the selected seat. Once the source-scoped
        # mechanic's clock starts, every resolved fill from that source must
        # participate even if an accepted order lost its optional alternate
        # transport annotation during terminal persistence.
        if not (is_alternate or is_source_scoped):
            continue
        member = member_by_wallet.get(wallet) or {}
        policy_id = str(
            attr.get("policy_id")
            or paper_intent.get("policy_id")
            or member.get("policy_id")
            or ""
        )
        candidate_id = str(attr.get("candidate_id") or member.get("candidate_id") or "")
        planned_hash = str(attr.get("planned_intent_hash") or "")
        if not planned_hash and paper_intent:
            planned_hash = hashlib.sha256(
                json.dumps([paper_intent], sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
        accepted_at = str(accepted_at_raw or "")
        event = score_order(order, resolutions)
        resolved = bool(event.get("resolved")) and str(
            event.get("status") or event.get("final_status") or order.get("final_status") or ""
        ).upper() == "FILLED"
        pnl = None
        if resolved:
            cost = _float_or_default(event.get("cost_usd"), 0.0)
            shares = _float_or_default(event.get("shares"), 0.0)
            if cost <= 0:
                cost = _float_or_default(
                    order.get("response_filled_size_usd")
                    or order.get("filled_size_usd")
                    or order.get("requested_size_usd")
                    or (order.get("expected_vs_realized_fee") or {}).get("response_cost_usd"),
                    0.0,
                )
            if shares <= 0:
                shares = _float_or_default(
                    order.get("response_fill_size_shares")
                    or order.get("filled_shares")
                    or order.get("requested_shares")
                    or (order.get("expected_vs_realized_fee") or {}).get("response_fill_size_shares"),
                    0.0,
                )
            pnl = round(
                (shares if bool(event.get("win")) else 0.0) - cost
                if _float_or_default(event.get("cost_usd"), 0.0) <= 0 and cost > 0
                else _float_or_default(event.get("pnl_usd"), 0.0),
                6,
            )
        updated_attr = {
            "schema_version": 1,
            "flow_stage": "LIVE/ROTATE",
            "source_wallet": wallet,
            "candidate_id": candidate_id,
            "policy_id": policy_id,
            "planned_intent_hash": planned_hash,
            "plan_hash_verified": bool(attr.get("plan_hash_verified")),
            "attribution_basis": (
                "alternate_transport_annotation"
                if is_alternate
                else "source_wallet_post_attribution_start"
            ),
            "hash_basis": "persisted_preplanned_hash" if attr.get("plan_hash_verified") else "reconstructed_exact_paper_intent",
            "accepted_at": accepted_at,
            "resolved_post_fee_pnl_usd": pnl,
            "resolution_status": "RESOLVED" if resolved else "PENDING",
        }
        if attr != updated_attr:
            order["alternate_transport_attribution"] = updated_attr
            ledger_changed = True
        attributed_orders += 1
        if resolved:
            resolved_by_key.setdefault((wallet, policy_id), []).append(
                {
                    "accepted_at": accepted_at,
                    "order_id": order.get("order_id"),
                    "intent_id": order.get("intent_id"),
                    "candidate_id": candidate_id,
                    "planned_intent_hash": planned_hash,
                    "pnl_usd": pnl,
                }
            )
    if ledger_changed:
        atomic_write_json(str(args.live_ledger_state), ledger)

    threshold = -8.0
    source_rows: list[dict[str, Any]] = []
    excluded_wallets: set[str] = set()
    for (wallet, policy_id), rows in sorted(resolved_by_key.items()):
        rows.sort(key=lambda row: str(row.get("accepted_at") or ""))
        rolling = rows[-20:]
        pnl = round(sum(float(row.get("pnl_usd") or 0.0) for row in rolling), 6)
        fired = pnl <= threshold
        if fired:
            excluded_wallets.add(wallet)
        source_rows.append(
            {
                "source_wallet": wallet,
                "policy_id": policy_id,
                "resolved_fills": len(rolling),
                "rolling_window_fills": 20,
                "rolling_realized_pnl_usd": pnl,
                "loss_threshold_usd": threshold,
                "rotation_triggered": fired,
                "action": "EXCLUDE_ALTERNATE_SOURCE_AND_FALL_THROUGH" if fired else "RETAIN_BELOW_TRIGGER",
                "rows": rolling,
            }
        )
    summary = {
        "schema_version": 1,
        "flow_stage": "LIVE/ROTATE",
        "status": "ROTATION_TRIGGERED" if excluded_wallets else "WATCH",
        "rule": "AUTONOMOUS_FLOW ROTATE: rolling last 20 resolved source fills below -$8",
        "attributed_orders": attributed_orders,
        "source_rows": source_rows,
        "excluded_wallets": sorted(excluded_wallets),
        "source_identity_outranks_seat_identity": True,
    }
    overlay = _load_auto_degrade_active_set_overlay()
    valid_excluded_wallets = {
        wallet
        for wallet in excluded_wallets
        if wallet.startswith("0x") and len(wallet) == 42
    }
    demoted_members: list[dict[str, Any]] = []
    overlay_members_changed = False
    if valid_excluded_wallets:
        updated_members: list[dict[str, Any]] = []
        source_row_by_wallet = {
            str(row.get("source_wallet") or ""): row for row in source_rows
        }
        for raw_member in overlay.get("members") or []:
            if not isinstance(raw_member, dict):
                continue
            member = dict(raw_member)
            wallet = str(
                member.get("source_wallet") or member.get("wallet") or ""
            ).lower()
            if wallet in valid_excluded_wallets:
                evidence = source_row_by_wallet.get(wallet) or {}
                member["enabled"] = False
                member["status"] = "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
                member["auto_degrade_suppresses_existing_wallet"] = True
                member["mechanical_loss_demotion"] = {
                    "flow_stage": "LIVE/ROTATE",
                    "rule": summary["rule"],
                    "reason": "mechanical_loss_demotion",
                    "resolved_fills": evidence.get("resolved_fills"),
                    "rolling_realized_pnl_usd": evidence.get(
                        "rolling_realized_pnl_usd"
                    ),
                    "loss_threshold_usd": threshold,
                    "source_identity_outranks_seat_identity": True,
                }
                demoted_members.append(
                    {
                        "candidate_id": member.get("candidate_id"),
                        "source_wallet": wallet,
                        **member["mechanical_loss_demotion"],
                    }
                )
            updated_members.append(member)
        overlay_members_changed = overlay.get("members") != updated_members
        overlay["members"] = updated_members
    summary["mechanical_demotion"] = {
        "status": "APPLIED" if demoted_members else "NOT_APPLICABLE",
        "demoted_members": demoted_members,
        "remaining_enabled_overlay_members": sum(
            1
            for member in overlay.get("members") or []
            if isinstance(member, dict) and member.get("enabled") is not False
        ),
        "remaining_enabled_runtime_members": sum(
            1
            for member in active_set_runtime.get("members") or []
            if isinstance(member, dict)
            and member.get("enabled") is not False
            and str(
                member.get("source_wallet") or member.get("wallet") or ""
            ).lower()
            not in valid_excluded_wallets
        ),
        "single_submitter_preserved": True,
    }
    if overlay.get("alternate_source_rotation") != summary or overlay_members_changed:
        overlay["alternate_source_rotation"] = summary
        _atomic_write_auto_degrade_overlay(overlay)
    return summary


def _latest_successful_nondenied_wallet(
    args: argparse.Namespace,
    member_wallets: set[str],
    *,
    max_age_s: float = LAST_SUCCESSFUL_NONDENIED_MEMBER_MAX_AGE_S,
) -> dict[str, Any]:
    ledger_path = Path(str(getattr(args, "live_ledger_state", "data/research/wallet_copy_live_execution_state.json")))
    if not ledger_path.is_absolute():
        ledger_path = ROOT / ledger_path
    ledger = load_json(ledger_path, default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) and isinstance(ledger.get("orders"), list) else []
    now = dt.datetime.now(dt.timezone.utc)
    success_statuses = {"FILLED", "SUBMITTED", "LIVE_SUBMITTED"}
    denied_statuses = {"REJECTED", "LIVE_REJECTED", "DENIED", "ERROR", "FAILED"}
    for order in reversed(orders):
        if not isinstance(order, dict):
            continue
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if status in denied_statuses or status not in success_statuses:
            continue
        wallet = _order_source_wallet(order)
        if not wallet or wallet not in member_wallets:
            continue
        ts = _parse_iso_datetime(order.get("updated_at") or order.get("submitted_at") or order.get("created_at"))
        if ts is None:
            continue
        age_s = (now - ts).total_seconds()
        if age_s < 0:
            age_s = 0.0
        if age_s > max_age_s:
            return {}
        return {
            "wallet": wallet,
            "status": status,
            "order_id": order.get("order_id"),
            "age_s": round(age_s, 6),
            "ts": ts.isoformat(),
            "state": str(ledger_path),
        }
    return {}


def _selection_pin_quiet_clock_hours(quiet: dict[str, Any]) -> float:
    try:
        quiet_hours = float(quiet.get("quiet_hours") or quiet.get("quiet_window_h") or quiet.get("quiet_window_hours"))
    except (TypeError, ValueError):
        quiet_hours = 0.0
    if quiet_hours > 0:
        return quiet_hours
    anchor = _parse_iso_datetime(quiet.get("anchor_iso"))
    earliest_fire = _parse_iso_datetime(quiet.get("earliest_fire_iso"))
    if anchor is not None and earliest_fire is not None:
        return max(0.0, (earliest_fire - anchor).total_seconds() / 3600.0) or 4.0
    return 4.0


def _selection_pin_order_timestamp_candidates(order: dict[str, Any]) -> list[dt.datetime]:
    candidates: list[dt.datetime] = []
    for key in ("ts", "submitted_at", "created_at", "updated_at"):
        parsed = _parse_iso_datetime(order.get(key))
        if parsed is not None:
            candidates.append(parsed)
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    parsed = _parse_iso_datetime(trade_result.get("timestamp"))
    if parsed is not None:
        candidates.append(parsed)
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    for step in lifecycle:
        if not isinstance(step, dict):
            continue
        parsed = _parse_iso_datetime(step.get("ts") or step.get("timestamp") or step.get("created_at"))
        if parsed is not None:
            candidates.append(parsed)
    return candidates


def _latest_selected_submit_attempt_for_quiet_clock(
    live_execution: dict[str, Any],
    selected_wallet: str,
) -> dict[str, Any]:
    selected_wallet = str(selected_wallet or "").strip().lower()
    if not selected_wallet:
        return {}
    orders = live_execution.get("orders") if isinstance(live_execution.get("orders"), list) else []
    best: dict[str, Any] = {}
    best_ts: dt.datetime | None = None
    accepted_markers = ("SUBMIT", "FILL", "ACCEPT")
    for order in orders:
        if not isinstance(order, dict) or _order_source_wallet(order) != selected_wallet:
            continue
        status = str(order.get("status") or "").upper()
        final_status = str(order.get("final_status") or "").upper()
        if not any(marker in f"{status} {final_status}" for marker in accepted_markers):
            continue
        timestamps = _selection_pin_order_timestamp_candidates(order)
        if not timestamps:
            continue
        ts = max(timestamps)
        if best_ts is not None and ts <= best_ts:
            continue
        best_ts = ts
        best = {
            "ts": ts,
            "iso": _utc_iso(ts),
            "order_id": order.get("order_id"),
            "intent_id": order.get("intent_id")
            or (order.get("intent") if isinstance(order.get("intent"), dict) else {}).get("intent_id"),
            "status": status or None,
            "final_status": final_status or None,
            "market_slug": order.get("market_slug")
            or (order.get("intent") if isinstance(order.get("intent"), dict) else {}).get("market_slug"),
        }
    return best


def _refresh_selection_pin_quiet_clock_expiry(
    *,
    pin: dict[str, Any],
    overlay: dict[str, Any],
    now: dt.datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    quiet = pin.get("quiet_clock") if isinstance(pin.get("quiet_clock"), dict) else {}
    if str(quiet.get("reset_predicate") or "") != "selected_submit_eligible_copyintent":
        return pin, overlay, {"status": "NO_QUIET_CLOCK_RESET_PREDICATE"}
    wallet = _member_wallet(pin)
    if not wallet:
        return pin, overlay, {"status": "NO_PIN_WALLET"}
    old_expires_at = _parse_iso_datetime(pin.get("expires_at"))
    old_anchor = _parse_iso_datetime(quiet.get("anchor_iso"))
    old_earliest_fire = _parse_iso_datetime(quiet.get("earliest_fire_iso"))
    live_execution = load_json(WALLET_COPY_LIVE_EXECUTION_STATE, default={})
    live_execution = live_execution if isinstance(live_execution, dict) else {}
    latest_submit = _latest_selected_submit_attempt_for_quiet_clock(live_execution, wallet)
    submit_ts = latest_submit.get("ts") if isinstance(latest_submit.get("ts"), dt.datetime) else None
    anchor = max([ts for ts in (old_anchor, submit_ts) if ts is not None], default=None)
    if anchor is None:
        return pin, overlay, {"status": "NO_QUIET_CLOCK_ANCHOR"}
    quiet_hours = _selection_pin_quiet_clock_hours(quiet)
    earliest_fire = anchor + dt.timedelta(hours=quiet_hours)
    drift = bool(old_expires_at is not None and now > old_expires_at and now < earliest_fire)
    changed = (
        old_expires_at is None
        or old_earliest_fire is None
        or abs((earliest_fire - old_expires_at).total_seconds()) >= 1.0
        or abs((earliest_fire - old_earliest_fire).total_seconds()) >= 1.0
        or (old_anchor is not None and abs((anchor - old_anchor).total_seconds()) >= 1.0)
    )
    status = {
        "status": "PIN_QUIET_CLOCK_EXPIRY_CURRENT",
        "wallet": wallet,
        "old_anchor_iso": None if old_anchor is None else _utc_iso(old_anchor),
        "new_anchor_iso": _utc_iso(anchor),
        "old_expires_at": pin.get("expires_at"),
        "new_expires_at": _utc_iso(earliest_fire),
        "quiet_hours": quiet_hours,
        "latest_selected_submit_attempt": {
            key: value for key, value in latest_submit.items() if key != "ts" and value is not None
        },
    }
    if not changed:
        return pin, overlay, status
    updated_pin = dict(pin)
    updated_quiet = dict(quiet)
    updated_quiet.update(
        {
            "anchor_iso": _utc_iso(anchor),
            "anchor_ts": anchor.timestamp(),
            "earliest_fire_iso": _utc_iso(earliest_fire),
            "earliest_fire_ts": earliest_fire.timestamp(),
            "quiet_hours": quiet_hours,
            "reset_predicate": "selected_submit_eligible_copyintent",
            "refreshed_at": _utc_iso(now),
            "refresh_direction_id": "2026-07-18T09:08Z-fable-pin-follows-quiet-clock",
        }
    )
    if latest_submit:
        updated_quiet["latest_selected_submit_attempt"] = status["latest_selected_submit_attempt"]
    updated_pin.update(
        {
            "quiet_clock": updated_quiet,
            "expires_at": _utc_iso(earliest_fire),
            "quiet_clock_expiry_derived": True,
            "updated_at": _utc_iso(now),
        }
    )
    updated_overlay = dict(overlay)
    updated_overlay["selection_pin"] = updated_pin
    status["status"] = "PIN_EXPIRED_BEFORE_QUIET_CLOCK_FIRE_REPAIRED" if drift else "PIN_QUIET_CLOCK_EXPIRY_REFRESHED"
    if drift:
        status["drift_signature"] = "PIN_EXPIRED_BEFORE_QUIET_CLOCK_FIRE"
    updated_overlay["selection_pin_quiet_clock_expiry_refresh"] = status
    updated_overlay["updated_at"] = _utc_iso(now)
    _atomic_write_auto_degrade_overlay(updated_overlay)
    return updated_pin, updated_overlay, status


def _safe_probe_label(value: str, *, fallback: str = "member") -> str:
    label = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value or ""))
    return label[:80] or fallback


def _live_execution_probe_path(member: dict[str, Any]) -> Path:
    candidate_id = str(member.get("candidate_id") or "")
    wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
    label = _safe_probe_label(candidate_id or wallet[-12:], fallback="member")
    return ROOT / "data/research" / f"wallet_copy_live_execution_probe_{label}.json"


def _live_execution_probe_override_path(probe_path: Path) -> Path:
    return probe_path.with_suffix(".selected_candidate_override.json")


def _write_live_execution_probe_override(
    args: argparse.Namespace,
    member: dict[str, Any],
    *,
    override_path: Path,
) -> str:
    candidate_id = str(member.get("candidate_id") or "")
    source_wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
    policy_id = str(member.get("policy_id") or getattr(args, "policy_id", "") or "")
    selected = dict(member)
    selected_runtime_policy = _live_execution_policy_from_mission_member(
        selected,
        fallback_policy_id=policy_id,
    )
    defended_policy = getattr(args, "active_set_selected_member_policy", None)
    if isinstance(defended_policy, dict):
        selected_runtime_policy.update(defended_policy)
    for key in ("policy_id", "max_order_usd", "max_price", "wallet_fraction", "min_live_order_usd"):
        if selected.get(key) is not None:
            selected_runtime_policy[key] = selected.get(key)
    if "min_live_order_usd" in selected_runtime_policy and "min_order_usd" not in selected_runtime_policy:
        selected_runtime_policy["min_order_usd"] = selected_runtime_policy.get("min_live_order_usd")
    _apply_live_min_order_cap_floor(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        process_min_live_order_usd=float(getattr(args, "min_live_order_usd", 1.0) or 1.0),
        source="active_set_live_execution_probe_override",
    )
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_live_guard_selected_candidate_override",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "candidate": {
            "candidate_id": candidate_id,
            "candidate_type": str(selected.get("candidate_type") or "SINGLE_WALLET"),
            "status": "PASS",
            "source_wallet": source_wallet,
            "policy_id": policy_id,
            "policy": selected_runtime_policy,
            "metadata": {
                "source": "active_set_live_execution_probe_member",
                "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py",
                "probe_live_orders_allowed": False,
                "active_set_member_status": str(selected.get("status") or ""),
            },
            "live_target_profile": {
                "status": "PASS",
                "blockers": [],
                "operator_approval_id": str(getattr(args, "operator_approval_id", "") or "OP-LIVE-20260703-BELA"),
                "source": "active_set_live_execution_probe_member",
            },
        },
        "rule": "read-only active-set member probes pin candidate lookup to the probed member",
    }
    atomic_write_json(override_path, payload)
    return str(override_path)


def _active_set_selection_pin(member_wallets: set[str]) -> dict[str, Any]:
    overlay = _load_auto_degrade_active_set_overlay()
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    if not pin.get("enabled"):
        return {}
    wallet = str(pin.get("source_wallet") or pin.get("wallet") or "").strip().lower()
    if not wallet or wallet not in member_wallets:
        return {}
    now = dt.datetime.now(dt.timezone.utc)
    pin, overlay, quiet_refresh = _refresh_selection_pin_quiet_clock_expiry(
        pin=pin,
        overlay=overlay,
        now=now,
    )
    expires_at = _parse_iso_datetime(pin.get("expires_at"))
    if expires_at is not None and now > expires_at:
        return {}
    created_at = _parse_iso_datetime(pin.get("created_at") or pin.get("updated_at") or overlay.get("updated_at"))
    if created_at is not None:
        age_s = max(0.0, (now - created_at).total_seconds())
        quiet_clock = pin.get("quiet_clock") if isinstance(pin.get("quiet_clock"), dict) else {}
        quiet_clock_controls_expiry = (
            str(quiet_clock.get("reset_predicate") or "") == "selected_submit_eligible_copyintent"
        )
        if age_s > ACTIVE_SET_SELECTION_PIN_MAX_AGE_S and not quiet_clock_controls_expiry:
            return {}
    else:
        age_s = None
    try:
        state_path = str(AUTO_DEGRADE_ACTIVE_SET_STATE.relative_to(ROOT))
    except ValueError:
        state_path = str(AUTO_DEGRADE_ACTIVE_SET_STATE)
    return {
        "wallet": wallet,
        "pin_id": pin.get("pin_id") or pin.get("direction_id"),
        "reason": pin.get("reason") or "active_set_selection_pin",
        "created_at": pin.get("created_at"),
        "expires_at": pin.get("expires_at"),
        "age_s": None if age_s is None else round(age_s, 6),
        "state": state_path,
        "quiet_clock": pin.get("quiet_clock") if isinstance(pin.get("quiet_clock"), dict) else None,
        "quiet_clock_expiry_refresh": quiet_refresh,
    }


def _active_set_selection_pin_view() -> dict[str, Any]:
    """Return a non-ambiguous guard-state view of the current selection pin."""
    overlay = _load_auto_degrade_active_set_overlay()
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    now = dt.datetime.now(dt.timezone.utc)
    expires_at = _parse_iso_datetime(pin.get("expires_at"))
    wallet = str(pin.get("source_wallet") or pin.get("wallet") or "").strip().lower()
    if not pin.get("enabled") or not wallet or (expires_at is not None and now > expires_at):
        return {"pin_status": "EXPIRED_OR_ABSENT"}
    view = {
        "pin_status": "ACTIVE",
        "enabled": True,
        "source_wallet": wallet,
        "pin_id": pin.get("pin_id") or pin.get("direction_id"),
    }
    if pin.get("expires_at") is not None:
        view["expires_at"] = pin.get("expires_at")
    return view


def _selection_priority_freeze_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not bool(payload.get("enabled", True)):
        return []
    rows = payload.get("frozen_wallets")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, dict)]
    row = payload.get("selection_priority_freeze")
    if isinstance(row, dict):
        return [dict(row)]
    if payload.get("source_wallet") or payload.get("wallet"):
        return [dict(payload)]
    return []


def _active_set_selection_priority_freeze(member_wallets: set[str]) -> dict[str, Any]:
    payloads: list[tuple[dict[str, Any], str]] = []
    state = load_json(SELECTION_PRIORITY_FREEZE_STATE, default={})
    if isinstance(state, dict):
        try:
            state_path = str(SELECTION_PRIORITY_FREEZE_STATE.relative_to(ROOT))
        except ValueError:
            state_path = str(SELECTION_PRIORITY_FREEZE_STATE)
        payloads.append((state, state_path))
    overlay = _load_auto_degrade_active_set_overlay()
    overlay_freeze = overlay.get("selection_priority_freeze") if isinstance(overlay, dict) else {}
    if isinstance(overlay_freeze, dict):
        try:
            overlay_path = str(AUTO_DEGRADE_ACTIVE_SET_STATE.relative_to(ROOT))
        except ValueError:
            overlay_path = str(AUTO_DEGRADE_ACTIVE_SET_STATE)
        payloads.append((overlay_freeze, overlay_path))

    now = dt.datetime.now(dt.timezone.utc)
    frozen: list[dict[str, Any]] = []
    for payload, state_path in payloads:
        for row in _selection_priority_freeze_rows(payload):
            if row.get("enabled") is False:
                continue
            wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
            if not wallet or wallet not in member_wallets:
                continue
            expires_at = _parse_iso_datetime(row.get("expires_at") or payload.get("expires_at"))
            if expires_at is not None and now > expires_at:
                continue
            expiry = row.get("expiry") if isinstance(row.get("expiry"), dict) else {}
            expiry_policy = str(row.get("expiry_policy") or payload.get("expiry_policy") or "").strip().lower()
            condition_based_no_clock_expiry = bool(
                expiry_policy == "condition_based_no_clock_expiry"
                or str(expiry.get("mode") or "").strip().lower() == "condition_based"
                or expiry.get("no_self_service_unfreeze") is True
            )
            created_at = _parse_iso_datetime(
                row.get("created_at")
                or row.get("updated_at")
                or payload.get("created_at")
                or payload.get("updated_at")
            )
            age_s = None
            if created_at is not None:
                age_s = max(0.0, (now - created_at).total_seconds())
                if age_s > ACTIVE_SET_SELECTION_PRIORITY_FREEZE_MAX_AGE_S and not condition_based_no_clock_expiry:
                    continue
            frozen.append(
                {
                    "source_wallet": wallet,
                    "candidate_id": row.get("candidate_id"),
                    "reason": row.get("reason") or payload.get("reason") or "active_set_selection_priority_freeze",
                    "direction_id": row.get("direction_id") or payload.get("direction_id"),
                    "created_at": row.get("created_at") or payload.get("created_at"),
                    "expires_at": row.get("expires_at") or payload.get("expires_at"),
                    "expiry_policy": row.get("expiry_policy") or payload.get("expiry_policy"),
                    "age_s": None if age_s is None else round(age_s, 6),
                    "state": state_path,
                }
            )
    if not frozen:
        return {}
    return {
        "enabled": True,
        "wallets": sorted({row["source_wallet"] for row in frozen}),
        "rows": frozen,
    }


def _active_set_member_effective_max_order_usd(member: dict[str, Any], args: argparse.Namespace) -> float:
    selected_policy = _live_execution_policy_from_mission_member(
        member,
        fallback_policy_id=str(member.get("policy_id") or getattr(args, "policy_id", "") or ""),
    )
    for key in ("policy_id", "max_order_usd", "max_price", "wallet_fraction", "min_live_order_usd"):
        if member.get(key) is not None:
            selected_policy[key] = member.get(key)
    max_order = _float_or_default(selected_policy.get("max_order_usd"), 0.0)
    member_max_order = _float_or_default(member.get("max_order_usd"), max_order)
    cap_order = (
        min(value for value in (max_order, member_max_order) if value and value > 0)
        if (max_order > 0 or member_max_order > 0)
        else 0.0
    )
    if cap_order <= 0:
        return 0.0
    return min(max_order if max_order > 0 else cap_order, cap_order)


def _active_set_member_executable_filter(members: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    try:
        configured_min_live_order_usd = max(0.0, float(getattr(args, "min_live_order_usd", 1.0) or 1.0))
    except (TypeError, ValueError):
        configured_min_live_order_usd = 1.0
    executable_indices: list[int] = []
    skipped: list[dict[str, Any]] = []
    for idx, member in enumerate(members):
        runtime_policy = _live_execution_policy_from_mission_member(
            member,
            fallback_policy_id=str(member.get("policy_id") or getattr(args, "policy_id", "") or ""),
        )
        for key in ("policy_id", "max_order_usd", "max_price", "wallet_fraction", "min_live_order_usd"):
            if member.get(key) is not None:
                runtime_policy[key] = member.get(key)
        if "min_live_order_usd" in runtime_policy and "min_order_usd" not in runtime_policy:
            runtime_policy["min_order_usd"] = runtime_policy.get("min_live_order_usd")
        _apply_live_min_order_cap_floor(
            selected=member,
            selected_runtime_policy=runtime_policy,
            process_min_live_order_usd=configured_min_live_order_usd,
            source="active_set_member_executable_filter",
        )
        effective_max_order_usd = _active_set_member_effective_max_order_usd(member, args)
        if configured_min_live_order_usd <= 0 or effective_max_order_usd >= configured_min_live_order_usd:
            executable_indices.append(idx)
            continue
        skipped.append(
            {
                "selected_member_index": idx,
                "candidate_id": str(member.get("candidate_id") or ""),
                "source_wallet": str(member.get("source_wallet") or member.get("wallet") or "").lower(),
                "policy_id": str(member.get("policy_id") or ""),
                "effective_max_order_usd": round(effective_max_order_usd, 9),
                "configured_min_live_order_usd": round(configured_min_live_order_usd, 9),
                "reason": "max_below_live_minimum",
            }
        )
    return {
        "enabled": True,
        "flow_stage": "LIVE/DEFEND",
        "rule": "runtime selection falls through unsubmittable members when an executable active-set member exists",
        "configured_min_live_order_usd": round(configured_min_live_order_usd, 9),
        "applied": bool(executable_indices and skipped),
        "executable_indices": executable_indices,
        "skipped_unsubmittable_members": skipped,
    }


def _fresh_runtime_member_selection_index(members: list[dict[str, Any]], args: argparse.Namespace) -> tuple[int | None, dict[str, Any]]:
    if not hasattr(args, "active_set_dataapi_poller_state"):
        return None, {"enabled": True, "applied": False, "reason": "active_set_dataapi_poller_state_unset"}
    state_path = Path(str(getattr(args, "active_set_dataapi_poller_state", DEFAULT_ACTIVE_SET_DATAAPI_POLLER_STATE)))
    if not state_path.is_absolute():
        state_path = ROOT / state_path
    state = load_json(state_path, default={})
    fetch_meta = state.get("fetch_meta") if isinstance(state, dict) and isinstance(state.get("fetch_meta"), dict) else {}
    summary = state.get("summary") if isinstance(state, dict) and isinstance(state.get("summary"), dict) else {}
    try:
        fresh_poll_only_signals = int(summary.get("fresh_poll_only_signals") or 0)
    except (TypeError, ValueError):
        fresh_poll_only_signals = 0
    latest_admission_wallet = _latest_auto_degrade_admission_wallet() if fresh_poll_only_signals == 0 else ""
    best: tuple[int, float, int, str] | None = None
    best_policy_compatible: tuple[int, float, int, str] | None = None
    best_policy_compatible_after_toxicity: tuple[int, float, int, str, int] | None = None
    lag_fallback: tuple[float, int, str] | None = None
    latest_admission_lag_s: float | None = None
    member_wallets = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        for member in members
        if isinstance(member, dict)
    }
    last_success = _latest_successful_nondenied_wallet(args, member_wallets)
    selection_pin = _active_set_selection_pin(member_wallets)
    priority_freeze = _active_set_selection_priority_freeze(member_wallets)
    priority_frozen_wallets = set(priority_freeze.get("wallets") or [])
    executable_filter = _active_set_member_executable_filter(members, args)
    executable_indices = set(executable_filter.get("executable_indices") or [])
    enforce_executable = bool(executable_filter.get("applied"))
    if last_success and last_success.get("wallet") in priority_frozen_wallets:
        last_success = {}
    for idx, member in enumerate(members):
        if enforce_executable and idx not in executable_indices:
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if wallet in priority_frozen_wallets:
            continue
        meta = fetch_meta.get(wallet) if wallet else None
        if not isinstance(meta, dict):
            meta = {}
        if not meta:
            continue
        fresh_by_source = meta.get("fresh_buy_rows_le_10s_by_source")
        fresh_rows = sum(int(value or 0) for value in fresh_by_source.values()) if isinstance(fresh_by_source, dict) else 0
        lag_by_source = meta.get("freshest_buy_lag_s_by_source")
        lags = []
        if isinstance(lag_by_source, dict):
            for value in lag_by_source.values():
                try:
                    lags.append(float(value))
                except (TypeError, ValueError):
                    continue
        best_lag = min(lags) if lags else float("inf")
        compatible_rows = _meta_first_number(
            meta,
            {
                "fresh_policy_compatible_buy_rows_le_30s",
                "policy_compatible_fresh_buy_rows_le_30s",
                "policy_compatible_fresh_le_30",
                "policy_compatible_fresh_le_30s",
                "recent_policy_compatible_fresh_buy_events_le_30s",
            },
        )
        compatible_lag = _meta_first_number(
            meta,
            {
                "freshest_policy_compatible_buy_lag_s",
                "freshest_policy_compatible_lag_s",
                "policy_compatible_freshest_buy_lag_s",
            },
        )
        compatible_lag_s = compatible_lag if compatible_lag is not None else best_lag
        if compatible_rows is not None and compatible_rows > 0:
            policy_candidate = (-int(compatible_rows), compatible_lag_s, idx, wallet)
            if best_policy_compatible is None or policy_candidate < best_policy_compatible:
                best_policy_compatible = policy_candidate
            compatible_after_toxicity = _meta_first_number(
                meta,
                {
                    "fresh_after_toxicity_policy_compatible_buy_rows_le_30s",
                    "fresh_candidate_intents_after_toxicity_protection",
                    "policy_compatible_fresh_after_toxicity_buy_rows_le_30s",
                    "policy_compatible_fresh_after_toxicity_le_30",
                    "policy_compatible_fresh_after_toxicity_le_30s",
                },
            )
            if compatible_after_toxicity is not None and compatible_after_toxicity > 0:
                surviving_candidate = (
                    -int(compatible_after_toxicity),
                    compatible_lag_s,
                    idx,
                    wallet,
                    int(compatible_rows),
                )
                if (
                    best_policy_compatible_after_toxicity is None
                    or surviving_candidate < best_policy_compatible_after_toxicity
                ):
                    best_policy_compatible_after_toxicity = surviving_candidate
        if latest_admission_wallet and wallet == latest_admission_wallet and best_lag != float("inf"):
            latest_admission_lag_s = best_lag
        if fresh_rows <= 0:
            if best_lag != float("inf"):
                lag_candidate = (best_lag, idx, wallet)
                if lag_fallback is None or lag_candidate < lag_fallback:
                    lag_fallback = lag_candidate
            continue
        candidate = (-fresh_rows, best_lag, idx, wallet)
        if best is None or candidate < best:
            best = candidate
    if selection_pin:
        for idx, member in enumerate(members):
            if enforce_executable and idx not in executable_indices:
                continue
            wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
            if wallet in priority_frozen_wallets:
                continue
            if wallet == selection_pin.get("wallet"):
                return idx, {
                    "enabled": True,
                    "applied": True,
                    "reason": "runtime_member_selection_pin",
                    "selected_member_index": idx,
                    "selected_wallet": wallet,
                    "selection_pin_id": selection_pin.get("pin_id"),
                    "selection_pin_reason": selection_pin.get("reason"),
                    "selection_pin_age_s": selection_pin.get("age_s"),
                    "selection_pin_expires_at": selection_pin.get("expires_at"),
                    "state": selection_pin.get("state"),
                }
    if best_policy_compatible_after_toxicity is not None:
        fresh_rows_neg, best_lag, idx, wallet, compatible_rows = best_policy_compatible_after_toxicity
        return idx, {
            "enabled": True,
            "applied": True,
            "reason": "runtime_member_policy_compatible_fresh_after_toxicity_le_30",
            "selected_wallet": wallet,
            "policy_compatible_fresh_after_toxicity_buy_rows_le_30s": -fresh_rows_neg,
            "policy_compatible_fresh_buy_rows_le_30s": compatible_rows,
            "freshest_policy_compatible_buy_lag_s": None if best_lag == float("inf") else best_lag,
            "selection_priority_freeze": priority_freeze or None,
        }
    if last_success:
        for idx, member in enumerate(members):
            if enforce_executable and idx not in executable_indices:
                continue
            wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
            if wallet == last_success.get("wallet"):
                return idx, {
                    "enabled": True,
                    "applied": True,
                    "reason": "runtime_member_last_successful_nondenied_priority",
                    "selected_member_index": idx,
                    "selected_wallet": wallet,
                    "last_successful_nondenied_order_id": last_success.get("order_id"),
                    "last_successful_nondenied_status": last_success.get("status"),
                    "last_successful_nondenied_age_s": last_success.get("age_s"),
                    "selection_priority_freeze": priority_freeze or None,
                    "state": last_success.get("state"),
                }
    if best_policy_compatible is not None:
        fresh_rows_neg, best_lag, idx, wallet = best_policy_compatible
        return idx, {
            "enabled": True,
            "applied": True,
            "reason": "runtime_member_policy_compatible_fresh_le_30",
            "selected_wallet": wallet,
            "policy_compatible_fresh_buy_rows_le_30s": -fresh_rows_neg,
            "freshest_policy_compatible_buy_lag_s": None if best_lag == float("inf") else best_lag,
            "toxicity_survivability": "unknown_or_zero_after_toxicity_no_last_success_override",
            "selection_priority_freeze": priority_freeze or None,
        }
    if best is None:
        if latest_admission_wallet:
            for idx, member in enumerate(members):
                wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
                if wallet in priority_frozen_wallets:
                    continue
                if wallet == latest_admission_wallet:
                    lag_is_stale = (
                        latest_admission_lag_s is not None
                        and latest_admission_lag_s > LATEST_ADMISSION_PRIORITY_MAX_LAG_S
                    )
                    if not lag_is_stale or lag_fallback is None:
                        return idx, {
                            "enabled": True,
                            "applied": True,
                            "reason": "auto_degrade_latest_admission_priority",
                            "selected_member_index": idx,
                            "selected_wallet": wallet,
                            "fresh_poll_only_signals": fresh_poll_only_signals,
                            "freshest_buy_lag_s": latest_admission_lag_s,
                            "selection_priority_freeze": priority_freeze or None,
                            "state": str(state_path),
                        }
                    break
        if lag_fallback is not None:
            selected_index = lag_fallback[1]
            selection = {
                "enabled": True,
                "applied": True,
                "reason": "runtime_member_freshest_buy_lag_fallback",
                "selected_member_index": selected_index,
                "selected_wallet": lag_fallback[2],
                "freshest_buy_lag_s": lag_fallback[0],
                "selection_priority_freeze": priority_freeze or None,
                "state": str(state_path),
            }
            if latest_admission_wallet:
                selection["latest_admission_priority"] = {
                    "wallet": latest_admission_wallet,
                    "skipped": True,
                    "reason": "latest_admission_lag_exceeds_max",
                    "freshest_buy_lag_s": latest_admission_lag_s,
                    "max_lag_s": LATEST_ADMISSION_PRIORITY_MAX_LAG_S,
                }
            return selected_index, selection
        return None, {
            "enabled": True,
            "applied": False,
            "reason": "no_runtime_member_fresh_buy_rows_le_10s",
            "selection_priority_freeze": priority_freeze or None,
        }
    selected_index = best[2]
    return selected_index, {
        "enabled": True,
        "applied": True,
        "reason": "runtime_member_fresh_buy_rows_le_10s",
        "selected_member_index": selected_index,
        "selected_wallet": best[3],
        "selection_priority_freeze": priority_freeze or None,
        "state": str(state_path),
    }


def _dataapi_poll_args(
    args: argparse.Namespace,
    *,
    wallets: list[str],
    history_state: str,
    history_window_index: str,
    wallet_event_log: str,
    first_seen_jsonl: str,
    state: str,
    poll_interval_s: float,
    policy_by_wallet: dict[str, dict[str, Any]] | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        source_wallet=[],
        source_wallets=",".join(wallets),
        history_state=str(history_state),
        history_window_index=str(history_window_index),
        wallet_event_log=str(wallet_event_log),
        dataapi_first_seen_jsonl=str(first_seen_jsonl),
        observation_watermark_state=str(
            getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")
        ),
        state=str(state),
        limit=int(getattr(args, "active_set_dataapi_poller_limit", 500)),
        pages=int(getattr(args, "active_set_dataapi_poller_pages", 2)),
        timeout_s=float(getattr(args, "active_set_dataapi_poller_timeout_s", 2.0)),
        retries=int(getattr(args, "active_set_dataapi_poller_retries", 1)),
        trade_query_keys=str(
            getattr(args, "active_set_dataapi_poller_trade_query_keys", "")
            or getattr(args, "pipeline_data_api_trade_query_keys", "user")
            or "user"
        ),
        parallel_sources=True,
        disable_source_base_overrides=_active_set_dataapi_disable_source_base_overrides(args),
        max_workers=int(getattr(args, "active_set_dataapi_poller_max_workers", 8)),
        poll_interval_s=float(poll_interval_s),
        force=False,
        max_event_age_s=float(getattr(args, "max_event_age_s", 30.0)),
        history_retain_events=_history_retain_events(args),
        active_set_policy_by_wallet=policy_by_wallet or {},
        live_orders_allowed=False,
    )


def _run_dataapi_poll_for_wallets(
    args: argparse.Namespace,
    *,
    wallets: list[str],
    history_state: str,
    history_window_index: str,
    wallet_event_log: str,
    first_seen_jsonl: str,
    state: str,
    poll_interval_s: float,
    label: str,
    policy_by_wallet: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    poll_args = _dataapi_poll_args(
        args,
        wallets=wallets,
        history_state=history_state,
        history_window_index=history_window_index,
        wallet_event_log=wallet_event_log,
        first_seen_jsonl=first_seen_jsonl,
        state=state,
        poll_interval_s=poll_interval_s,
        policy_by_wallet=policy_by_wallet,
    )
    try:
        result = _run_active_set_dataapi_poll(poll_args, source_wallets=wallets)
        if isinstance(result, dict):
            result = dict(result)
            result["poller_label"] = label
            result["source_wallet_count"] = len(wallets)
            result["measure_only"] = True
            result["paper_only"] = True
            result["live_orders_allowed"] = False
        return result
    except Exception as exc:  # pragma: no cover - live guard must keep running if poll source fails.
        return {
            "status": "ERROR",
            "error": f"{type(exc).__name__}: {exc}",
            "source_wallets": wallets,
            "source_wallet_count": len(wallets),
            "summary": {
                "active_set_wallets": len(wallets),
                "poll_only_signals": 0,
                "fresh_poll_only_signals": 0,
            },
            "poller_label": label,
            "measure_only": True,
            "paper_only": True,
            "live_orders_allowed": False,
        }


def _run_active_set_dataapi_poller(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    fallback_wallet: str = "",
) -> dict[str, Any]:
    if not bool(getattr(args, "active_set_dataapi_poller", True)):
        return {
            "status": "DISABLED",
            "summary": {"active_set_wallets": 0, "poll_only_signals": 0},
            "paper_only": True,
            "live_orders_allowed": False,
        }
    wallets = _active_set_poll_wallets(active_set_runtime, fallback_wallet=fallback_wallet)
    prioritized_wallets = _selected_wallet_first(wallets, fallback_wallet)
    policy_by_wallet = active_set_runtime.get("policy_by_wallet")
    policy_by_wallet = policy_by_wallet if isinstance(policy_by_wallet, dict) else {}
    result = _run_dataapi_poll_for_wallets(
        args,
        wallets=prioritized_wallets,
        history_state=str(args.history_state),
        history_window_index=str(
            getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")
        ),
        wallet_event_log=str(args.wallet_event_log),
        first_seen_jsonl=str(getattr(args, "active_set_dataapi_first_seen_jsonl", "data/research/dataapi_first_seen.jsonl")),
        state=str(getattr(args, "active_set_dataapi_poller_state", DEFAULT_ACTIVE_SET_DATAAPI_POLLER_STATE)),
        poll_interval_s=float(getattr(args, "active_set_dataapi_poller_interval_s", 1.0)),
        label="active_set",
        policy_by_wallet=policy_by_wallet,
    )
    if isinstance(result, dict):
        result["selected_wallet_first_poll"] = {
            "enabled": True,
            "flow_stage": "LIVE/LEARN",
            "selected_wallet": str(fallback_wallet or "").strip().lower() or None,
            "applied": prioritized_wallets != wallets,
            "rule": "poll selected runtime wallet before the full active-set Data API sweep; measurement only",
            "paper_only": True,
            "live_orders_allowed": False,
        }
    return result


def _selected_fast_pipe_latest_observed_ts(
    active_set_rtds_premerge: dict[str, Any],
    selected_wallet: str,
) -> float | None:
    selected_wallet = str(selected_wallet or "").strip().lower()
    if not selected_wallet:
        return None
    rows = active_set_rtds_premerge.get("rows") if isinstance(active_set_rtds_premerge, dict) else []
    latest: float | None = None
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").strip().lower()
        if wallet != selected_wallet:
            continue
        try:
            observed = float(row.get("latest_observed_ts"))
        except (TypeError, ValueError):
            continue
        latest = observed if latest is None else max(latest, observed)
    return latest


def _selected_policy_compatible_fresh_rows(meta: dict[str, Any]) -> int:
    keys = {
        "fresh_policy_compatible_buy_rows_le_30s",
        "policy_compatible_fresh_buy_rows_le_30s",
        "policy_compatible_fresh_le_30",
        "policy_compatible_fresh_le_30s",
        "recent_policy_compatible_fresh_buy_events_le_30s",
    }
    value = _meta_first_number(meta, keys)
    if value is None and isinstance(meta.get("policy_feedback"), dict):
        value = _meta_first_number(meta.get("policy_feedback"), keys)
    return int(value or 0)


def _selected_freshest_buy_lag_s(meta: dict[str, Any]) -> float | None:
    lags: list[float] = []
    lag_by_source = meta.get("freshest_buy_lag_s_by_source")
    if isinstance(lag_by_source, dict):
        for value in lag_by_source.values():
            try:
                lags.append(float(value))
            except (TypeError, ValueError):
                continue
    for key in ("freshest_buy_lag_s", "freshest_policy_compatible_buy_lag_s"):
        try:
            value = meta.get(key)
            if value is not None:
                lags.append(float(value))
        except (TypeError, ValueError):
            continue
    return min(lags) if lags else None


def _active_set_detection_acceptance_discriminator(
    *,
    dataapi_poll_result: dict[str, Any],
    active_set_rtds_premerge: dict[str, Any],
    previous_state: dict[str, Any],
    selected_wallet: str,
) -> dict[str, Any]:
    selected_wallet = str(selected_wallet or "").strip().lower()
    fetch_meta = dataapi_poll_result.get("fetch_meta") if isinstance(dataapi_poll_result.get("fetch_meta"), dict) else {}
    current_meta = fetch_meta.get(selected_wallet) if isinstance(fetch_meta.get(selected_wallet), dict) else {}
    previous_poller = (
        previous_state.get("active_set_dataapi_poller")
        if isinstance(previous_state.get("active_set_dataapi_poller"), dict)
        else {}
    )
    previous_fetch_meta = (
        previous_poller.get("fetch_meta")
        if isinstance(previous_poller.get("fetch_meta"), dict)
        else {}
    )
    previous_meta = previous_fetch_meta.get(selected_wallet) if isinstance(previous_fetch_meta.get(selected_wallet), dict) else {}
    previous_premerge = (
        previous_state.get("active_set_rtds_premerge")
        if isinstance(previous_state.get("active_set_rtds_premerge"), dict)
        else {}
    )
    current_policy_rows = _selected_policy_compatible_fresh_rows(current_meta)
    current_lag = _selected_freshest_buy_lag_s(current_meta)
    previous_lag = _selected_freshest_buy_lag_s(previous_meta)
    current_fast_ts = _selected_fast_pipe_latest_observed_ts(active_set_rtds_premerge, selected_wallet)
    previous_fast_ts = _selected_fast_pipe_latest_observed_ts(previous_premerge, selected_wallet)
    lag_decreased = (
        current_lag is not None
        and previous_lag is not None
        and current_lag < previous_lag - 1e-6
    )
    fast_pipe_advanced = (
        current_fast_ts is not None
        and previous_fast_ts is not None
        and current_fast_ts > previous_fast_ts + 1e-6
    )
    source_had_new_buy = bool(lag_decreased or fast_pipe_advanced)
    if current_policy_rows > 0:
        verdict = "TRANSPORT_OK"
    elif fast_pipe_advanced and current_policy_rows <= 0:
        verdict = "TRANSPORT_INADEQUATE"
    else:
        verdict = "SOURCE_QUIET_OR_INCONCLUSIVE"
    return {
        "flow_stage": "LIVE/DEFEND",
        "status": verdict,
        "selected_wallet": selected_wallet or None,
        "policy_compatible_fresh_buy_rows_le_30s": current_policy_rows,
        "freshest_buy_lag_s": None if current_lag is None else round(current_lag, 6),
        "previous_freshest_buy_lag_s": None if previous_lag is None else round(previous_lag, 6),
        "source_had_new_buy_since_last_poll": source_had_new_buy,
        "lag_decreased_since_last_poll": bool(lag_decreased),
        "fast_pipe_latest_observed_ts": None if current_fast_ts is None else round(current_fast_ts, 6),
        "previous_fast_pipe_latest_observed_ts": None if previous_fast_ts is None else round(previous_fast_ts, 6),
        "fast_pipe_latest_observed_ts_advanced": bool(fast_pipe_advanced),
        "transport_change_authorized": verdict == "TRANSPORT_INADEQUATE",
        "rule": "transport work requires fast-pipe advancement while dataapi policy-compatible fresh rows stay zero; source-quiet zeroes are inconclusive",
    }


def _watch_tier_poll_wallets(config_path: str | Path) -> list[str]:
    config = load_json(str(config_path), default={})
    if not isinstance(config, dict):
        return []
    wallets: list[str] = []
    for row in config.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            wallets.append(wallet)
    return list(dict.fromkeys(wallets))


def _watch_tier_dataapi_poller_member_cap(args: argparse.Namespace, wallet_count: int) -> int:
    raw = int(getattr(args, "watch_tier_dataapi_poller_max_wallets_per_cycle", 4) or 0)
    if raw <= 0:
        return max(0, int(wallet_count))
    return min(max(0, int(wallet_count)), raw)


def _watch_tier_round_robin_wallets(
    wallets: list[str],
    *,
    max_wallets: int,
    round_robin_offset: int = 0,
) -> tuple[list[str], dict[str, Any]]:
    total = len(wallets)
    if total <= 0:
        return [], {
            "enabled": True,
            "wallet_count_total": 0,
            "max_wallets_per_cycle": max(0, int(max_wallets)),
            "round_robin_offset": 0,
            "selected_wallet_count": 0,
            "next_round_robin_offset": 0,
        }
    cap = max(0, int(max_wallets))
    if cap <= 0 or cap >= total:
        return list(wallets), {
            "enabled": False,
            "wallet_count_total": total,
            "max_wallets_per_cycle": cap,
            "round_robin_offset": 0,
            "selected_wallet_count": total,
            "next_round_robin_offset": 0,
        }
    offset = max(0, int(round_robin_offset or 0)) % total
    rotated = wallets[offset:] + wallets[:offset]
    selected = rotated[:cap]
    return selected, {
        "enabled": True,
        "wallet_count_total": total,
        "max_wallets_per_cycle": cap,
        "round_robin_offset": offset,
        "selected_wallet_count": len(selected),
        "next_round_robin_offset": (offset + cap) % total,
    }


def _watch_tier_stakeout_wallets(config_path: str | Path) -> dict[str, dict[str, Any]]:
    config = load_json(str(config_path), default={})
    if not isinstance(config, dict):
        return {}
    stakeout: dict[str, dict[str, Any]] = {}
    for row in config.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        meta = row.get("stakeout") if isinstance(row.get("stakeout"), dict) else {}
        if row.get("wake_up_alert") is not True and meta.get("wake_up_alert") is not True:
            continue
        wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            stakeout[wallet] = dict(row)
    return stakeout


def _build_watch_tier_stakeout_state(config_path: str | Path, result: dict[str, Any]) -> dict[str, Any]:
    stakeout = _watch_tier_stakeout_wallets(config_path)
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    fresh_by_wallet = summary.get("fresh_poll_only_by_wallet") if isinstance(summary.get("fresh_poll_only_by_wallet"), dict) else {}
    poll_by_wallet = summary.get("poll_only_by_wallet") if isinstance(summary.get("poll_only_by_wallet"), dict) else {}
    rows: list[dict[str, Any]] = []
    fresh_alerts: list[dict[str, Any]] = []
    poll_alerts: list[dict[str, Any]] = []
    for wallet, config_row in stakeout.items():
        fresh_count = int(fresh_by_wallet.get(wallet) or 0)
        poll_count = int(poll_by_wallet.get(wallet) or 0)
        row = {
            "source_wallet": wallet,
            "fresh_poll_only_signals": fresh_count,
            "poll_only_signals": poll_count,
            "selection_reason": config_row.get("selection_reason"),
            "next": "immediate Fable ruling packet on first fresh signal"
            if fresh_count > 0
            else "continue watch-tier stakeout",
        }
        rows.append(row)
        if fresh_count > 0:
            fresh_alerts.append(row)
        elif poll_count > 0:
            poll_alerts.append(row)
    status = "FRESH_WAKE_UP_ALERT" if fresh_alerts else "POLL_WAKE_UP_SEEN" if poll_alerts else "ARMED"
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "kind": "wallet_copy_watch_tier_stakeout",
        "flow_stage": "DISCOVER/PROMOTE/LIVE",
        "status": status,
        "config": str(config_path),
        "stakeout_wallet_count": len(stakeout),
        "stakeout_wallets": rows,
        "fresh_alerts": fresh_alerts,
        "poll_alerts": poll_alerts,
        "paper_only": True,
        "live_orders_allowed": False,
        "next_action": "ask Fable with fresh-signal age, cell expectancy, and copyability"
        if fresh_alerts
        else "keep stakeout armed until first fresh weekend-specialist signal",
    }


def _run_watch_tier_dataapi_poller(
    args: argparse.Namespace,
    *,
    round_robin_offset: int = 0,
) -> dict[str, Any]:
    if not bool(getattr(args, "watch_tier_dataapi_poller", True)):
        return {
            "status": "DISABLED",
            "source_wallet_count": 0,
            "summary": {"watch_tier_wallets": 0, "poll_only_signals": 0},
            "measure_only": True,
            "paper_only": True,
            "live_orders_allowed": False,
        }
    config_path = str(getattr(args, "watch_tier_wallets_config", DEFAULT_WATCH_TIER_WALLETS_CONFIG))
    wallets = _watch_tier_poll_wallets(config_path)
    if not wallets:
        return {
            "status": "SKIPPED_NO_WATCH_TIER_WALLETS",
            "watch_tier_wallets_config": config_path,
            "source_wallets": [],
            "source_wallet_count": 0,
            "summary": {"watch_tier_wallets": 0, "poll_only_signals": 0, "fresh_poll_only_signals": 0},
            "measure_only": True,
            "paper_only": True,
            "live_orders_allowed": False,
        }
    selected_wallets, chunk = _watch_tier_round_robin_wallets(
        wallets,
        max_wallets=_watch_tier_dataapi_poller_member_cap(args, len(wallets)),
        round_robin_offset=round_robin_offset,
    )
    result = _run_dataapi_poll_for_wallets(
        args,
        wallets=selected_wallets,
        history_state=str(getattr(args, "watch_tier_history_state", DEFAULT_WATCH_TIER_HISTORY_STATE)),
        history_window_index=str(getattr(args, "watch_tier_history_window_index", DEFAULT_WATCH_TIER_HISTORY_WINDOW_INDEX)),
        wallet_event_log=str(getattr(args, "watch_tier_wallet_event_log", DEFAULT_WATCH_TIER_WALLET_EVENT_LOG)),
        first_seen_jsonl=str(getattr(args, "watch_tier_dataapi_first_seen_jsonl", DEFAULT_WATCH_TIER_FIRST_SEEN)),
        state=str(getattr(args, "watch_tier_dataapi_poller_state", DEFAULT_WATCH_TIER_POLLER_STATE)),
        poll_interval_s=float(getattr(args, "watch_tier_dataapi_poller_interval_s", 5.0)),
        label="watch_tier",
    )
    result["watch_tier_wallets_config"] = config_path
    result["watch_tier_chunk"] = chunk
    stakeout_state = _build_watch_tier_stakeout_state(config_path, result)
    result["watch_tier_stakeout"] = stakeout_state
    atomic_write_json(DEFAULT_WATCH_TIER_STAKEOUT_STATE, stakeout_state)
    result["separate_file_contract"] = {
        "history_state": str(getattr(args, "watch_tier_history_state", DEFAULT_WATCH_TIER_HISTORY_STATE)),
        "history_window_index": str(
            getattr(args, "watch_tier_history_window_index", DEFAULT_WATCH_TIER_HISTORY_WINDOW_INDEX)
        ),
        "wallet_event_log": str(getattr(args, "watch_tier_wallet_event_log", DEFAULT_WATCH_TIER_WALLET_EVENT_LOG)),
        "first_seen_jsonl": str(getattr(args, "watch_tier_dataapi_first_seen_jsonl", DEFAULT_WATCH_TIER_FIRST_SEEN)),
        "state": str(getattr(args, "watch_tier_dataapi_poller_state", DEFAULT_WATCH_TIER_POLLER_STATE)),
        "copyintent_path_shared": False,
        "live_order_submitter": "none_measure_only",
    }
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    summary["watch_tier_wallets"] = len(selected_wallets)
    summary["watch_tier_wallets_total"] = len(wallets)
    summary["watch_tier_wallets_remaining_after_this_cycle"] = max(0, len(wallets) - len(selected_wallets))
    result["summary"] = summary
    return result


def _premerge_substage_profile_totals(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    totals: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        for stage, row in profile.items():
            if not isinstance(row, dict):
                continue
            stage_total = totals.setdefault(stage, {"duration_s": 0.0, "samples": 0})
            stage_total["samples"] += 1
            try:
                stage_total["duration_s"] = round(
                    float(stage_total["duration_s"]) + float(row.get("duration_s") or 0.0),
                    6,
                )
            except (TypeError, ValueError):
                pass
            try:
                count_stage_counters = float(row.get("duration_s") or 0.0) > 0.0
            except (TypeError, ValueError):
                count_stage_counters = True
            for key in (
                "bytes_read",
                "bytes_requested",
                "line_count",
                "scanned_rows",
                "parsed_rows",
                "json_decode_errors",
                "matching_events",
                "existing_events",
                "new_events",
                "merged_events",
            ):
                if not count_stage_counters:
                    continue
                if key not in row:
                    continue
                try:
                    stage_total[key] = int(stage_total.get(key) or 0) + int(row.get(key) or 0)
                except (TypeError, ValueError):
                    continue
    return {
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "status": "PASS" if totals else "EMPTY",
        "wallet_profiles": len([profile for profile in profiles if isinstance(profile, dict) and profile]),
        "stages": totals,
    }


def _run_active_set_rtds_premerge(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    selected_wallet: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    selected_wallet = str(selected_wallet or "").strip().lower()
    if not str(getattr(args, "rtds_jsonl", "") or ""):
        selected_result = _run_fused_pipeline(args, source_wallet=selected_wallet) if selected_wallet else {}
        return selected_result, {
            "status": "DISABLED",
            "flow_stage": "LIVE/OBSERVE",
            "reason": "rtds_jsonl_not_configured",
            "paper_only": True,
            "live_orders_allowed": False,
        }
    if not bool(getattr(args, "active_set_rtds_premerge", True)):
        selected_result = _run_fused_pipeline(args, source_wallet=selected_wallet) if selected_wallet else {}
        return selected_result, {
            "status": "DISABLED",
            "flow_stage": "LIVE/OBSERVE",
            "reason": "active_set_rtds_premerge_disabled",
            "paper_only": True,
            "live_orders_allowed": False,
        }

    runtime_members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    wallets = _active_set_poll_wallets(active_set_runtime, fallback_wallet=selected_wallet)
    if selected_wallet and selected_wallet not in wallets and not runtime_members:
        wallets.insert(0, selected_wallet)
    wallets = _selected_wallet_first(wallets, selected_wallet)
    limit = int(getattr(args, "active_set_rtds_premerge_member_limit", 0) or 0)
    if limit > 0:
        wallets = wallets[:limit]
    wallets = [
        wallet
        for wallet in wallets
        if isinstance(wallet, str) and wallet.startswith("0x") and len(wallet) == 42
    ]
    if not wallets:
        selected_result = _run_fused_pipeline(args, source_wallet=selected_wallet) if selected_wallet else {}
        return selected_result, {
            "status": "SKIPPED_NO_VALID_WALLETS",
            "flow_stage": "LIVE/OBSERVE",
            "reason": "active_set_has_no_valid_0x_wallet",
            "selected_wallet": selected_wallet or None,
            "wallets_refreshed": 0,
            "paper_only": True,
            "live_orders_allowed": False,
            "submitter_invariant": "history refresh only; scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
        }

    rows: list[dict[str, Any]] = []
    selected_result: dict[str, Any] = {}
    status_counts: dict[str, int] = {}
    max_lag_s: float | None = None
    new_matching_events = 0
    retained_matching_rows = 0
    wallet_results: dict[str, dict[str, Any]] = {}
    batch_summary: dict[str, Any] = {}
    priority_status: dict[str, Any] = {
        "enabled": bool(selected_wallet),
        "selected_wallet": selected_wallet or None,
        "applied": False,
        "reason": "selected_wallet_missing" if selected_wallet and selected_wallet not in wallets else None,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    batch_wallets = list(wallets)
    if selected_wallet and selected_wallet in wallets:
        priority_status.update(
            {
                "applied": True,
                "reason": "selected_wallet_included_in_single_batch_premerge",
            }
        )
    if batch_wallets:
        started = time.time()
        batch_args = argparse.Namespace(
            rtds_jsonl=str(args.rtds_jsonl),
            source_wallets=batch_wallets,
            wallet_names={wallet: f"live_primary_{wallet[-8:]}" for wallet in batch_wallets},
            history_state=args.history_state,
            history_window_index=str(
                getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")
            ),
            wallet_event_log=args.wallet_event_log,
            scan_limit=int(args.rtds_scan_limit),
            tail_bytes=_rtds_tail_backfill_bytes(args),
            cold_tail_bytes=int(
                getattr(args, "rtds_cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES) or DEFAULT_COLD_TAIL_BYTES
            ),
            offset_states={wallet: _rtds_offset_state(args, source_wallet=wallet) for wallet in batch_wallets},
            aggregate_offset_state=_rtds_offset_state(args),
            watermark_state=str(
                getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")
            ),
            polygon_jsonl=str(getattr(args, "polygon_ws_premerge_jsonl", "") or ""),
            polygon_tail_bytes=int(getattr(args, "polygon_ws_premerge_tail_bytes", 64 * 1024 * 1024) or 0),
            max_new_events=int(args.rtds_max_new_events),
            history_retain_events=_history_retain_events(args),
            history_retain_copy_intents=_history_retain_copy_intents(args),
        )
        try:
            batch_summary = _run_rtds_multi_wallet_merge(batch_args)
            summaries = batch_summary.get("wallet_summaries") if isinstance(batch_summary, dict) else {}
            summaries = summaries if isinstance(summaries, dict) else {}
            for wallet in batch_wallets:
                summary = summaries.get(wallet) if isinstance(summaries.get(wallet), dict) else {}
                wallet_results[wallet] = _callable_result(
                    argv=_pipeline_command(args, source_wallet=wallet),
                    started=started,
                    returncode=0,
                    stdout_json=summary,
                    mode="in_process_batch",
                )
        except Exception as exc:  # pragma: no cover - live guard must keep running if batch premerge fails.
            return selected_result, {
                "status": "DEGRADED",
                "flow_stage": "LIVE/OBSERVE",
                "cause_closed": "active_set_rtds_batch_premerge_error",
                "error": f"{type(exc).__name__}: {exc}",
                "rtds_jsonl": str(getattr(args, "rtds_jsonl", "") or ""),
                "wallets_refreshed": len(wallet_results),
                "selected_wallet": selected_wallet,
                "new_matching_events": 0,
                "retained_matching_rows": 0,
                "status_counts": {"ERROR": 1},
                "rows": [],
                "selected_wallet_priority_premerge": priority_status,
                "paper_only": True,
                "live_orders_allowed": False,
                "submitter_invariant": "history refresh only; scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
            }
    for wallet in wallets:
        result = wallet_results.get(wallet, {})
        stdout = result.get("stdout_json") if isinstance(result, dict) else {}
        stdout = stdout if isinstance(stdout, dict) else {}
        status = str(stdout.get("status") or ("PASS" if int(result.get("returncode") or 0) == 0 else "ERROR"))
        status_counts[status] = status_counts.get(status, 0) + 1
        try:
            lag = stdout.get("rtds_catchup_lag_s")
            lag_value = None if lag is None else max(0.0, float(lag))
        except (TypeError, ValueError):
            lag_value = None
        if lag_value is not None:
            max_lag_s = lag_value if max_lag_s is None else max(max_lag_s, lag_value)
        try:
            new_matching_events += int(stdout.get("new_matching_events") or 0)
            retained_matching_rows += int(stdout.get("retained_matching_rows") or 0)
        except (TypeError, ValueError):
            pass
        rows.append(
            {
                "source_wallet": wallet,
                "status": status,
                "returncode": int(result.get("returncode") or 0),
                "execution_mode": result.get("execution_mode"),
                "ingest_mode": stdout.get("ingest_mode"),
                "new_matching_events": int(stdout.get("new_matching_events") or 0),
                "retained_matching_rows": int(stdout.get("retained_matching_rows") or 0),
                "history_write_skipped": bool(stdout.get("history_write_skipped")),
                "latest_event_ts": stdout.get("latest_event_ts"),
                "latest_observed_ts": stdout.get("latest_observed_ts"),
                "rtds_catchup_lag_s": lag_value,
                "offset_state": stdout.get("offset_state"),
                "premerge_substage_profile": stdout.get("premerge_substage_profile")
                if isinstance(stdout.get("premerge_substage_profile"), dict)
                else {},
            }
        )
        if wallet == selected_wallet:
            selected_result = result

    if selected_wallet and not selected_result:
        selected_result = wallet_results.get(selected_wallet, {})
    if selected_wallet and selected_result:
        selected_stdout = selected_result.get("stdout_json") if isinstance(selected_result, dict) else {}
        selected_stdout = selected_stdout if isinstance(selected_stdout, dict) else {}
        priority_status.update(
            {
                "returncode": int(selected_result.get("returncode") or 0),
                "duration_s": selected_result.get("duration_s"),
                "new_matching_events": int(selected_stdout.get("new_matching_events") or 0),
                "retained_matching_rows": int(selected_stdout.get("retained_matching_rows") or 0),
                "rtds_catchup_lag_s": selected_stdout.get("rtds_catchup_lag_s"),
            }
        )
    failed = [row for row in rows if int(row.get("returncode") or 0) != 0]
    premerge_profiles = [
        row.get("premerge_substage_profile")
        for row in rows
        if isinstance(row.get("premerge_substage_profile"), dict) and row.get("premerge_substage_profile")
    ]
    return selected_result, {
        "status": "PASS" if not failed else "DEGRADED",
        "flow_stage": "LIVE/OBSERVE",
        "cause_closed": "selected_member_only_rtds_refresh_was_replaced_by_active_set_user_channel_premerge",
        "rtds_jsonl": str(getattr(args, "rtds_jsonl", "") or ""),
        "wallets_refreshed": len(rows),
        "selected_wallet": selected_wallet,
        "new_matching_events": new_matching_events,
        "retained_matching_rows": retained_matching_rows,
        "max_rtds_catchup_lag_s": None if max_lag_s is None else round(float(max_lag_s), 6),
        "status_counts": dict(sorted(status_counts.items())),
        "premerge_substage_profile_totals": _premerge_substage_profile_totals(premerge_profiles),
        "rows": rows,
        "matching_events_delta": [
            row
            for row in batch_summary.get("matching_events_delta") or []
            if isinstance(row, dict)
        ][-max(1, int(getattr(args, "rtds_max_new_events", 1) or 1)) :],
        "new_events_delta": [
            row
            for row in batch_summary.get("new_events_delta") or []
            if isinstance(row, dict)
        ][-max(1, int(getattr(args, "rtds_max_new_events", 1) or 1)) :],
        "selected_wallet_priority_premerge": priority_status,
        "paper_only": True,
        "live_orders_allowed": False,
        "submitter_invariant": "history refresh only; scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
    }


def _wallet_event_identity_keys(row: dict[str, Any]) -> set[str]:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    transaction_hash = str(
        row.get("transaction_hash") or raw.get("transactionHash") or raw.get("transaction_hash") or ""
    ).strip().lower()
    log_index = raw.get("log_index")
    if transaction_hash and isinstance(log_index, int):
        keys = {f"polygon_log:{transaction_hash}|{log_index}"}
        event_id = str(row.get("event_id") or "").strip().lower()
        if event_id:
            keys.add(f"event:{event_id}")
        return keys
    values = (
        ("fingerprint", row.get("source_fingerprint")),
        ("event", row.get("event_id")),
        ("tx", transaction_hash),
    )
    return {
        f"{kind}:{str(value).strip().lower()}"
        for kind, value in values
        if str(value or "").strip()
    }


def _active_member_orderfilled_hot_source_delta(
    state_path: str | Path,
    *,
    source_jsonl: str | Path,
    cursor_state_path: str | Path,
    accumulator_path: str | Path,
    active_set_runtime: dict[str, Any],
    now_ts: float,
    bootstrap_tail_bytes: int = 8 * 1024 * 1024,
    max_seen_identities: int = 5_000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read fresh gate-qualified Polygon rows directly into the protected bridge."""
    state = load_json(str(state_path), default={}, cache_readonly=False)
    gate_passed = bool(
        isinstance(state, dict)
        and state.get("live_source_wiring_gate_passed") is True
        and int(state.get("unique_resolved_source_events") or 0)
        >= int(state.get("required_unique_resolved_source_events") or 100)
        and int(state.get("token_mapping_missing") or 0) == 0
        and int(state.get("identity_market_outcome_parity_violations") or 0) == 0
    )
    base_report = {
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "state_path": str(state_path),
        "gate_passed": gate_passed,
        "unique_resolved_source_events": int(state.get("unique_resolved_source_events") or 0)
        if isinstance(state, dict)
        else 0,
        "required_unique_resolved_source_events": int(
            state.get("required_unique_resolved_source_events") or 100
        )
        if isinstance(state, dict)
        else 100,
        "token_mapping_missing": int(state.get("token_mapping_missing") or 0)
        if isinstance(state, dict)
        else 0,
        "identity_market_outcome_parity_violations": int(
            state.get("identity_market_outcome_parity_violations") or 0
        )
        if isinstance(state, dict)
        else 0,
        "submitter_invariant": "source rows only; existing live guard remains the sole submitter",
    }
    if not gate_passed:
        return [], {**base_report, "status": "PAPER_GATE_CLOSED", "eligible_rows": 0}

    members = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        for member in active_set_runtime.get("members") or []
        if isinstance(member, dict) and member.get("enabled") is not False
    }
    cursor_path = Path(cursor_state_path)
    cursor = load_json(cursor_path, default={}, cache_readonly=False)
    cursor = cursor if isinstance(cursor, dict) else {}
    source_path = Path(source_jsonl)
    try:
        source_size = source_path.stat().st_size
    except OSError:
        return [], {**base_report, "status": "SOURCE_MISSING", "eligible_rows": 0}
    previous_offset = int(cursor.get("next_byte_offset") or 0)
    cursor_reset = previous_offset < 0 or previous_offset > source_size
    start = previous_offset
    align_to_next_line = cursor_reset
    if start == 0 or cursor_reset:
        start = max(0, source_size - max(1, int(bootstrap_tail_bytes)))
        align_to_next_line = start > 0
    seen_order = [
        str(identity)
        for identity in cursor.get("seen_identities") or []
        if str(identity)
    ][-max(1, int(max_seen_identities)) :]
    seen = set(seen_order)
    accumulator = load_json(str(accumulator_path), default={}, cache_readonly=False)
    token_meta = (
        accumulator.get("token_metadata_cache")
        if isinstance(accumulator, dict)
        and isinstance(accumulator.get("token_metadata_cache"), dict)
        else {}
    )
    current_window_start = int(now_ts // 300) * 300
    rows: list[dict[str, Any]] = []
    rejected: Counter[str] = Counter()
    raw_rows = 0
    member_rows = 0
    with source_path.open("rb") as handle:
        handle.seek(start)
        if align_to_next_line:
            handle.readline()
        for raw_line in handle:
            try:
                source = json.loads(raw_line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                rejected["invalid_json"] += 1
                continue
            if not isinstance(source, dict) or source.get("event") != "polygon_orderfilled_log":
                continue
            raw_rows += 1
            source_wallets = [
                str(source.get(key) or "").strip().lower()
                for key in ("selected_wallet", "maker", "taker")
            ]
            wallet = next((wallet for wallet in source_wallets if wallet in members), "")
            if not wallet:
                rejected["wallet_not_enabled_runtime_member"] += 1
                continue
            member_rows += 1
            normalized_source = dict(source)
            normalized_source["selected_wallet"] = wallet
            normalized_source["decoded"] = decode_polygon_orderfilled_v2(normalized_source)
            event_source = normalize_polygon_orderfilled_row(normalized_source)
            if event_source is None or str(event_source.side or "").upper() != "BUY":
                rejected["not_buy"] += 1
                continue
            transaction_hash = str(event_source.transaction_hash or "").strip().lower()
            log_index = source.get("log_index")
            token_id = str(event_source.asset or "")
            meta = token_meta.get(token_id) if isinstance(token_meta.get(token_id), dict) else {}
            market_slug = str(meta.get("market_slug") or "")
            condition_id = str(meta.get("condition_id") or "")
            outcome = str(meta.get("outcome") or "")
            try:
                window_start = int(market_slug.rsplit("-", 1)[-1])
                event_ts = float(event_source.event_ts)
                observed_ts = float(event_source.received_at_s)
                price = float(event_source.price)
                size = float(event_source.size)
            except (TypeError, ValueError):
                rejected["token_mapping_or_required_field_invalid"] += 1
                continue
            if not transaction_hash or not isinstance(log_index, int):
                rejected["polygon_identity_invalid"] += 1
                continue
            identity = f"{transaction_hash}|{log_index}"
            if identity in seen:
                rejected["duplicate_polygon_log"] += 1
                continue
            seen.add(identity)
            seen_order.append(identity)
            if window_start not in {current_window_start, current_window_start + 300}:
                rejected["not_current_or_next_window"] += 1
                continue
            if max(0.0, now_ts - event_ts) > 30.0:
                rejected["source_event_age_gt_30s"] += 1
                continue
            event = WalletEvent(
                source_wallet=wallet,
                wallet_name=wallet,
                row_type="trade",
                action="BUY",
                condition_id=condition_id,
                market_slug=market_slug,
                outcome=outcome,
                price=price,
                size=size,
                usdc_size=round(price * size, 6),
                event_ts=event_ts,
                observed_ts=observed_ts,
                event_id=f"polygon_orderfilled:{identity}",
                source="polygon_orderfilled_hot_source",
                market_id=condition_id,
                event_slug=market_slug,
                asset="BTC",
                duration="5m",
                window_start_s=window_start,
                token_id=token_id,
                transaction_hash=transaction_hash,
                api_latency_s=max(0.0, observed_ts - event_ts),
                raw={
                    "_walletCopySource": "polygon_orderfilled_hot_source",
                    "transaction_hash": transaction_hash,
                    "log_index": log_index,
                },
            )
            rows.append(event.asdict())
    next_offset = handle.tell()
    consumed_at = time.time()
    atomic_write_json(
        cursor_path,
        {
            "schema_version": 1,
            "flow_stage": "LIVE/ROTATE/DEFEND",
            "generated_at": utc_now_iso(),
            "source_jsonl": str(source_path),
            "next_byte_offset": next_offset,
            "cursor_reset": cursor_reset,
            "seen_identities": seen_order[-max(1, int(max_seen_identities)) :],
            "raw_rows": raw_rows,
            "runtime_member_rows": member_rows,
            "eligible_rows": len(rows),
        },
    )
    return rows, {
        **base_report,
        "status": "PASS" if rows else "PASS_NO_CURRENT_EVENT",
        "eligible_rows": len(rows),
        "rejected": dict(sorted(rejected.items())),
        "transport": "direct_incremental_polygon_jsonl",
        "raw_rows": raw_rows,
        "runtime_member_rows": member_rows,
        "previous_byte_offset": previous_offset,
        "next_byte_offset": next_offset,
        "cursor_reset": cursor_reset,
        "guard_consumed_at_s": consumed_at,
        "max_source_age_s": max(
            [max(0.0, consumed_at - float(row.get("event_ts") or consumed_at)) for row in rows],
            default=None,
        ),
        "identity_rule": "Polygon primary identity is transaction_hash|log_index; bare tx hash is never a merge key",
    }


def _active_member_orderfilled_direct_delta(
    *,
    polygon_jsonl: str | Path,
    gate_state_path: str | Path,
    accumulator_state_path: str | Path,
    cursor_state_path: str | Path,
    active_set_runtime: dict[str, Any],
    now_ts: float,
    bootstrap_bytes: int,
    max_seen_identities: int = 5000,
    source_identity_router_state_path: str | Path = DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read fresh gate-qualified Polygon logs directly with restart-safe identity."""
    gate = load_json(str(gate_state_path), default={}, cache_readonly=False)
    gate_passed = bool(
        isinstance(gate, dict)
        and gate.get("live_source_wiring_gate_passed") is True
        and int(gate.get("unique_resolved_source_events") or 0)
        >= int(gate.get("required_unique_resolved_source_events") or 100)
        and int(gate.get("token_mapping_missing") or 0) == 0
        and int(gate.get("identity_market_outcome_parity_violations") or 0) == 0
    )
    report: dict[str, Any] = {
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "gate_passed": gate_passed,
        "source": str(polygon_jsonl),
        "cursor_state": str(cursor_state_path),
        "identity_rule": "transaction_hash|log_index",
        "submitter_invariant": "direct detection rows only; existing live guard remains the sole submitter",
    }
    if not gate_passed:
        return [], {**report, "status": "PAPER_GATE_CLOSED", "eligible_rows": 0}

    source_path = Path(polygon_jsonl)
    cursor_path = Path(cursor_state_path)
    cursor = load_json(str(cursor_path), default={}, cache_readonly=False)
    cursor = cursor if isinstance(cursor, dict) else {}
    byte_offset = int(cursor.get("next_byte_offset") or 0)
    seen_order = [str(value) for value in cursor.get("seen_identities") or [] if str(value)]
    seen = set(seen_order)
    if not source_path.exists():
        return [], {**report, "status": "SOURCE_MISSING", "eligible_rows": 0}
    size = source_path.stat().st_size
    cursor_reset = byte_offset < 0 or byte_offset > size
    start = byte_offset
    align = cursor_reset
    if start == 0 or cursor_reset:
        start = max(0, size - max(1, int(bootstrap_bytes)))
        align = start > 0

    raw_rows: list[dict[str, Any]] = []
    with source_path.open("rb") as handle:
        handle.seek(start)
        if align:
            handle.readline()
        for raw in handle:
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                raw_rows.append(row)
        next_offset = handle.tell()

    accumulator = load_json(str(accumulator_state_path), default={}, cache_readonly=False)
    token_meta = (
        accumulator.get("token_metadata_cache")
        if isinstance(accumulator, dict) and isinstance(accumulator.get("token_metadata_cache"), dict)
        else {}
    )
    members = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        for member in active_set_runtime.get("members") or []
        if isinstance(member, dict) and member.get("enabled") is not False
    }
    router_state = load_json(
        str(source_identity_router_state_path), default={}, cache_readonly=False
    )
    current_router = (
        router_state.get("current_cohort")
        if isinstance(router_state, dict)
        and isinstance(router_state.get("current_cohort"), dict)
        else {}
    )
    proxy_aliases = {
        str(proxy or "").lower(): str(wallet or "").lower()
        for proxy, wallet in (
            current_router.get("proxy_source_aliases")
            if isinstance(current_router.get("proxy_source_aliases"), dict)
            else {}
        ).items()
        if str(wallet or "").lower() in members
    }
    current_window = int(now_ts // 300) * 300
    rows: list[dict[str, Any]] = []
    rejected: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for raw in raw_rows:
        if raw.get("event") != "polygon_orderfilled_log":
            rejected["not_orderfilled"] += 1
            continue
        source_wallets = [
            str(raw.get(key) or "").strip().lower()
            for key in ("selected_wallet", "maker", "taker")
        ]
        wallet = next((candidate for candidate in source_wallets if candidate in members), "")
        member_attribution = "direct" if wallet else ""
        if not wallet:
            wallet = next(
                (
                    proxy_aliases[candidate]
                    for candidate in source_wallets
                    if candidate in proxy_aliases
                ),
                "",
            )
            if wallet:
                member_attribution = "proxy_alias"
        if not wallet:
            rejected["wallet_not_enabled_runtime_member"] += 1
            continue
        normalized_raw = dict(raw)
        normalized_raw["selected_wallet"] = wallet
        event = normalize_polygon_orderfilled_row(normalized_raw)
        if event is None:
            rejected["normalize_failed"] += 1
            continue
        transaction_hash = str(event.transaction_hash or "").strip().lower()
        log_index = raw.get("log_index")
        if not transaction_hash or not isinstance(log_index, int):
            rejected["polygon_identity_invalid"] += 1
            continue
        identity = f"{transaction_hash}|{log_index}"
        if identity in seen:
            rejected["duplicate_polygon_log"] += 1
            continue
        seen.add(identity)
        seen_order.append(identity)
        if str(event.side or "").upper() != "BUY":
            rejected["not_buy"] += 1
            continue
        meta = token_meta.get(str(event.asset or ""))
        if not isinstance(meta, dict):
            rejected["token_outside_btc5m_cache"] += 1
            continue
        market_slug = str(meta.get("market_slug") or "")
        try:
            window_start = int(market_slug.rsplit("-", 1)[-1])
            event_ts = float(event.event_ts)
            received_at_s = float(event.received_at_s)
            price = float(event.price)
            event_size = float(event.size)
        except (TypeError, ValueError):
            rejected["required_field_invalid"] += 1
            continue
        if window_start not in {current_window, current_window + 300}:
            rejected["not_current_or_next_window"] += 1
            continue
        event_age_s = max(0.0, now_ts - event_ts)
        receipt_to_guard_s = max(0.0, now_ts - received_at_s)
        if event_age_s > 30.0:
            rejected["source_event_age_gt_30s"] += 1
            continue
        wallet_event = WalletEvent(
            source_wallet=wallet,
            wallet_name=wallet,
            row_type="trade",
            action="BUY",
            condition_id=str(meta.get("condition_id") or ""),
            market_slug=market_slug,
            outcome=str(meta.get("outcome") or ""),
            price=price,
            size=event_size,
            usdc_size=round(price * event_size, 6),
            event_ts=event_ts,
            observed_ts=received_at_s,
            event_id=f"polygon_orderfilled:{identity}",
            source="polygon_orderfilled_direct",
            market_id=str(meta.get("condition_id") or ""),
            event_slug=market_slug,
            asset="BTC",
            duration="5m",
            window_start_s=window_start,
            token_id=str(event.asset or ""),
            transaction_hash=transaction_hash,
            api_latency_s=max(0.0, received_at_s - event_ts),
            raw={
                "_walletCopySource": "polygon_orderfilled_direct",
                "transaction_hash": transaction_hash,
                "log_index": log_index,
                "raw_received_at_s": received_at_s,
                "sidecar_appended_at_s": raw.get("sidecar_appended_at_s"),
                "guard_consumed_at_s": now_ts,
                "receipt_to_guard_s": receipt_to_guard_s,
                "member_attribution": member_attribution,
                "source_participants": source_wallets,
            },
        )
        rows.append(wallet_event.asdict())
        samples.append(
            {
                "identity": identity,
                "source_wallet": wallet,
                "member_attribution": member_attribution,
                "market_slug": market_slug,
                "price": price,
                "raw_received_at_s": received_at_s,
                "sidecar_appended_at_s": raw.get("sidecar_appended_at_s"),
                "guard_consumed_at_s": now_ts,
                "receipt_to_guard_s": round(receipt_to_guard_s, 6),
                "event_age_s": round(event_age_s, 6),
            }
        )

    atomic_write_json(
        cursor_path,
        {
            "schema_version": 1,
            "flow_stage": "LIVE/ROTATE/DEFEND",
            "generated_at": utc_now_iso(),
            "next_byte_offset": next_offset,
            "seen_identities": seen_order[-max(1, int(max_seen_identities)) :],
            "source_size_bytes": size,
            "identity_rule": "transaction_hash|log_index",
        },
    )
    return rows, {
        **report,
        "status": "PASS" if rows else "PASS_NO_CURRENT_EVENT",
        "raw_rows": len(raw_rows),
        "eligible_rows": len(rows),
        "next_byte_offset": next_offset,
        "cursor_reset": cursor_reset,
        "rejected": dict(sorted(rejected.items())),
        "samples": samples[-20:],
        "proxy_source_alias_count": len(proxy_aliases),
    }


def _orderfilled_live_source_selection(args: argparse.Namespace) -> dict[str, Any]:
    """Select the dedicated receiver stream only after its paper gate passes."""
    mixed_source = str(getattr(args, "polygon_ws_premerge_jsonl", "") or "")
    mixed_cursor = str(getattr(args, "active_member_orderfilled_direct_cursor_state", "") or "")
    sidecar_source = str(getattr(args, "orderfilled_sidecar_jsonl", "") or "")
    sidecar_cursor = str(getattr(args, "orderfilled_sidecar_live_cursor_state", "") or "")
    gate_path = str(getattr(args, "orderfilled_sidecar_gate_state", "") or "")
    gate = load_json(gate_path, default={}, cache_readonly=False)
    gate = gate if isinstance(gate, dict) else {}
    matched = int(gate.get("unique_exact_payload_matched_identities") or 0)
    required = int(gate.get("required_identities") or 100)
    p95_lead = float(gate.get("p95_sidecar_lead_vs_mixed_guard_s") or 0.0)
    required_lead = float(gate.get("required_p95_lead_s") or 5.0)
    rss_gib = float(gate.get("max_rss_gib") or 999.0)
    gate_passed_once = bool(gate.get("gate_passed_once"))
    gate_passed = bool(
        gate.get("paper_only") is True
        and gate.get("live_source_wiring_gate_passed") is True
        and (
            gate_passed_once
            or (
                matched >= required
                and int(gate.get("overdue_sidecar_only_payloads") or 0) == 0
                and int(gate.get("overdue_mixed_only_payloads") or 0) == 0
                and p95_lead > required_lead
                and rss_gib < float(gate.get("max_rss_gate_gib") or 1.0)
            )
        )
        and sidecar_source
        and Path(sidecar_source).exists()
    )
    return {
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "status": "SIDECAR_GATE_PASSED" if gate_passed else "MIXED_SOURCE_GATE_CLOSED",
        "gate_passed": gate_passed,
        "gate_passed_once": gate_passed_once,
        "source_jsonl": sidecar_source if gate_passed else mixed_source,
        "cursor_state": sidecar_cursor if gate_passed else mixed_cursor,
        "gate_state": gate_path,
        "unique_exact_payload_matched_identities": matched,
        "required_identities": required,
        "overdue_sidecar_only_payloads": int(gate.get("overdue_sidecar_only_payloads") or 0),
        "overdue_mixed_only_payloads": int(gate.get("overdue_mixed_only_payloads") or 0),
        "p95_sidecar_lead_vs_mixed_guard_s": p95_lead,
        "required_p95_lead_s": required_lead,
        "max_rss_gib": rss_gib,
        "reader_only_change": True,
    }


def _initialize_orderfilled_sidecar_cursor(
    *,
    source_jsonl: str | Path,
    old_cursor_state: str | Path,
    new_cursor_state: str | Path,
    bootstrap_bytes: int,
    max_seen_identities: int = 5_000,
) -> dict[str, Any]:
    """Initialize a sidecar cursor before a known mixed-stream boundary.

    The handoff intentionally replays a bounded overlap. Existing Polygon
    identity and live-ledger dedupe absorb that overlap; starting after an
    uncertain boundary could silently lose an event.
    """
    source_path = Path(source_jsonl)
    old_cursor_path = Path(old_cursor_state)
    new_cursor_path = Path(new_cursor_state)
    if new_cursor_path.exists():
        current = load_json(str(new_cursor_path), default={}, cache_readonly=False)
        return {
            "status": "EXISTING",
            "next_byte_offset": int(current.get("next_byte_offset") or 0)
            if isinstance(current, dict)
            else 0,
        }
    old_cursor = load_json(str(old_cursor_path), default={}, cache_readonly=False)
    old_cursor = old_cursor if isinstance(old_cursor, dict) else {}
    seen_order = [str(value) for value in old_cursor.get("seen_identities") or [] if str(value)]
    overlap_identities = set(seen_order[-100:])
    size = source_path.stat().st_size if source_path.exists() else 0
    fallback_start = max(0, size - max(1, int(bootstrap_bytes)))
    matched_offsets: list[int] = []
    if source_path.exists() and overlap_identities:
        with source_path.open("rb") as handle:
            while True:
                line_start = handle.tell()
                raw = handle.readline()
                if not raw:
                    break
                try:
                    row = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(row, dict):
                    continue
                tx_hash = str(row.get("transaction_hash") or "").strip().lower()
                log_index = row.get("log_index")
                identity = f"{tx_hash}|{log_index}" if tx_hash and isinstance(log_index, int) else ""
                if identity in overlap_identities:
                    matched_offsets.append(line_start)
    next_offset = min(matched_offsets) if matched_offsets else fallback_start
    atomic_write_json(
        new_cursor_path,
        {
            "schema_version": 1,
            "flow_stage": "LIVE/OBSERVE/SELF-DEV",
            "generated_at": utc_now_iso(),
            "next_byte_offset": next_offset,
            "seen_identities": seen_order[-max(1, int(max_seen_identities)) :],
            "source_size_bytes": size,
            "identity_rule": "transaction_hash|log_index",
            "handoff": {
                "status": "MATCHED_OVERLAP" if matched_offsets else "BOUNDED_TAIL_FALLBACK",
                "old_cursor_state": str(old_cursor_path),
                "matched_overlap_identities": len(matched_offsets),
                "replay_before_boundary": True,
            },
        },
    )
    return {
        "status": "INITIALIZED_MATCHED_OVERLAP" if matched_offsets else "INITIALIZED_BOUNDED_TAIL",
        "next_byte_offset": next_offset,
        "matched_overlap_identities": len(matched_offsets),
    }


def _history_event_identity_snapshot(history_state: str | Path) -> set[str]:
    payload = load_json(str(history_state), default={}, cache_readonly=True)
    events = payload.get("events") if isinstance(payload, dict) and isinstance(payload.get("events"), list) else []
    identities: set[str] = set()
    for row in events:
        if isinstance(row, dict):
            identities.update(_wallet_event_identity_keys(row))
    return identities


def _post_poll_history_event_delta(
    history_state: str | Path,
    *,
    before_identities: set[str],
) -> list[dict[str, Any]]:
    payload = load_json(str(history_state), default={}, cache_readonly=False)
    events = payload.get("events") if isinstance(payload, dict) and isinstance(payload.get("events"), list) else []
    seen = set(before_identities)
    delta: list[dict[str, Any]] = []
    for row in events:
        if not isinstance(row, dict):
            continue
        keys = _wallet_event_identity_keys(row)
        if not keys or any(key in seen for key in keys):
            continue
        seen.update(keys)
        delta.append(row)
    return delta


def _bridge_or_fused_live_execution(
    args: argparse.Namespace,
    *,
    candidate_id: str,
    bridge_report: dict[str, Any],
    bridge_live_result: dict[str, Any] | None,
) -> dict[str, Any]:
    if (
        isinstance(bridge_live_result, dict)
        and int(bridge_report.get("submit_stage_invocations") or 0) > 0
    ):
        return bridge_live_result
    return _run_fused_live_execution(args, candidate_id=candidate_id)


def _empty_alternate_transport_bridge_result(
    args: argparse.Namespace,
) -> tuple[argparse.Namespace, dict[str, Any]]:
    """Skip bridge setup when the transport union has no rows to route."""
    return args, {
        "status": "NO_QUALIFYING_CURRENT_EVENT",
        "flow_stage": "LIVE/ROTATE/SELF-DEV",
        "input_delta_rows": 0,
        "invalid_delta_rows": 0,
        "fast_lane_identity_exclusions": 0,
        "selected_wallet": None,
        "rows": [],
        "live_orders_allowed": False,
        "empty_delta_fast_path": True,
        "submitter_invariant": "bridge selects only; existing live guard remains sole submitter",
    }


def _alternate_transport_copyintent_bridge(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    active_set_rtds_premerge: dict[str, Any],
    now_ts: float | None = None,
    exclude_identity_keys: set[str] | None = None,
    generation_fence: Callable[[], bool] | None = None,
    allow_submit_stage: bool = True,
    submit_stage_authority: dict[str, Any] | None = None,
) -> tuple[argparse.Namespace, dict[str, Any]]:
    """Select a protected runtime member from the bounded RTDS/Polygon delta.

    This only changes which already-admitted member the existing live builder
    evaluates.  It does not create or submit an intent itself.
    """
    now_ts = float(time.time() if now_ts is None else now_ts)
    overlay = _load_auto_degrade_active_set_overlay()
    rotation = (
        overlay.get("alternate_source_rotation")
        if isinstance(overlay.get("alternate_source_rotation"), dict)
        else {}
    )
    excluded_wallets = {
        str(wallet or "").lower() for wallet in rotation.get("excluded_wallets") or [] if str(wallet or "")
    }
    members = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower(): member
        for member in active_set_runtime.get("members") or []
        if isinstance(member, dict) and member.get("enabled") is not False
    }
    policy_by_wallet = {
        str(wallet or "").strip().lower(): dict(policy)
        for wallet, policy in (
            active_set_runtime.get("policy_by_wallet")
            if isinstance(active_set_runtime.get("policy_by_wallet"), dict)
            else {}
        ).items()
        if isinstance(policy, dict)
    }
    seen: set[str] = set()
    excluded = set(exclude_identity_keys or set())
    events: list[WalletEvent] = []
    invalid_rows = 0
    for row in active_set_rtds_premerge.get("matching_events_delta") or []:
        if not isinstance(row, dict):
            continue
        try:
            event = WalletEvent.from_dict(row)
        except (TypeError, ValueError):
            invalid_rows += 1
            continue
        identity_keys = _wallet_event_identity_keys(event.asdict())
        if not identity_keys or any(key in seen or key in excluded for key in identity_keys):
            continue
        seen.update(identity_keys)
        events.append(event)
    events.sort(
        key=lambda event: (
            -float(event.event_ts or 0.0),
            str(event.event_id or ""),
            str(event.source_wallet or "").lower(),
        )
    )

    rows: list[dict[str, Any]] = []
    accepted_events: list[tuple[dict[str, Any], WalletEvent, dict[str, Any], int]] = []
    current_window_start = int(now_ts // 300) * 300
    primary_live_candidate = _primary_live_candidate_contract()
    for event in events:
        wallet = str(event.source_wallet or "").strip().lower()
        member = members.get(wallet)
        reason = ""
        policy_reason = ""
        candidate: dict[str, Any] = {}
        pass_gate: dict[str, Any] = {}
        event_window_start = int(float(event.window_start_s or 0.0)) if event.window_start_s is not None else 0
        event_age_s = max(0.0, now_ts - float(event.event_ts or 0.0))
        if member is None:
            reason = "wallet_not_enabled_runtime_member"
        elif wallet in excluded_wallets:
            reason = "mechanical_alternate_source_rotation_excluded"
        elif str(event.action or "").upper() != "BUY":
            reason = "not_buy"
        elif event_window_start != current_window_start and event_window_start != current_window_start + 300:
            reason = "not_current_btc5m_interval"
        elif event_age_s > 30.0:
            reason = "source_event_age_gt_30s"
        else:
            candidate_id, source_wallet, policy_id = _active_set_member_candidate_pins(member)
            policy = _live_execution_policy_from_mission_member(
                member,
                fallback_policy_id=policy_id,
            )
            # active_set_runtime.policy_by_wallet is the post-defense policy
            # exported by the guard.  Its caps must win over the raw member,
            # which intentionally remains a compact roster identity.
            policy.update(policy_by_wallet.get(wallet, {}))
            member_args = argparse.Namespace(**vars(args))
            member_args.active_set_selected_member_candidate_id = candidate_id
            member_args.active_set_selected_member_source_wallet = source_wallet
            member_args.active_set_selected_member_policy = dict(policy)
            member_args.active_set_selected_member_status = str(member.get("status") or "")
            candidate = _active_set_runtime_member_candidate(
                member_args,
                candidate_id_pin=candidate_id,
                source_wallet_pin=source_wallet,
                policy_id_pin=policy_id,
            )
            pass_gate = _candidate_pass_gate(
                candidate,
                source_wallet_pin=wallet,
                now=dt.datetime.fromtimestamp(now_ts, tz=dt.timezone.utc),
            )
            if not pass_gate.get("passed"):
                reason = "runtime_member_protection_gate"
            else:
                candidate_policy = CandidatePolicy(
                    policy_id=str(policy.get("policy_id") or policy_id),
                    min_price=_float_or_default(policy.get("min_price"), 0.01),
                    max_price=_float_or_default(policy.get("max_price"), 1.0),
                    min_wallet_usdc=_float_or_default(policy.get("min_wallet_usdc"), 0.0),
                    max_wallet_usdc=_float_or_default(policy.get("max_wallet_usdc"), 0.0),
                    min_seconds_from_open=policy.get("min_seconds_from_open"),
                    max_seconds_from_open=policy.get("max_seconds_from_open"),
                    move_slice_keys=tuple(
                        str(value)
                        for value in policy.get("move_slice_keys") or []
                        if str(value)
                    ),
                    wallet_fraction=_float_or_default(policy.get("wallet_fraction"), 0.1),
                    max_order_usd=_float_or_default(policy.get("max_order_usd"), 1.0),
                    min_order_usd=_float_or_default(policy.get("min_order_usd"), 1.0),
                    maker_min_share_funding_cap_usd=_float_or_default(
                        policy.get("maker_min_share_funding_cap_usd"), 0.0
                    ),
                    maker_min_share_original_policy_cap_usd=_float_or_default(
                        policy.get("maker_min_share_original_policy_cap_usd"), 0.0
                    ),
                    maker_min_share_base_request_cap_usd=_float_or_default(
                        policy.get("maker_min_share_base_request_cap_usd"), 0.0
                    ),
                )
                accepted, policy_reason = policy_accepts_event(candidate_policy, event)
                reason = "policy_accepted" if accepted else f"policy_rejected:{policy_reason}"
                if accepted:
                    accepted_events.append((member, event, policy, len(rows)))
        rows.append(
            {
                "source_wallet": wallet,
                "event_id": event.event_id,
                "source_fingerprint": event.source_fingerprint,
                "transaction_hash": event.transaction_hash,
                "market_slug": event.market_slug,
                "event_ts": event.event_ts,
                "observed_ts": event.observed_ts,
                "event_age_s": round(event_age_s, 6),
                "current_window_start_s": current_window_start,
                "event_window_start_s": event_window_start or None,
                "candidate_id": str(candidate.get("candidate_id") or ""),
                "protection_passed": bool(pass_gate.get("passed")),
                "policy_reason": policy_reason or None,
                "terminal_stage": reason,
            }
        )

    accepted_events.sort(
        key=lambda item: (
            0 if float(item[1].price or 0.0) < 0.50 else 1,
            -float(item[1].event_ts or 0.0),
            str(item[1].event_id or ""),
        )
    )
    if not accepted_events:
        return args, {
            "status": "NO_QUALIFYING_CURRENT_EVENT",
            "flow_stage": "LIVE/ROTATE/SELF-DEV",
            "input_delta_rows": len(events),
            "invalid_delta_rows": invalid_rows,
            "fast_lane_identity_exclusions": len(excluded),
            "selected_wallet": None,
            "rows": rows,
            "live_orders_allowed": False,
            "submitter_invariant": "bridge selects only; existing live guard remains sole submitter",
        }

    def configured_args(
        member: dict[str, Any],
        policy: dict[str, Any],
        *,
        override_suffix: str = "",
    ) -> argparse.Namespace:
        bridge_args = argparse.Namespace(**vars(args))
        candidate_id, source_wallet, policy_id = _active_set_member_candidate_pins(member)
        bridge_args.candidate_id = candidate_id
        bridge_args.source_wallet = source_wallet
        bridge_args.policy_id = policy_id
        bridge_args.active_set_selected_member_candidate_id = candidate_id
        bridge_args.active_set_selected_member_source_wallet = source_wallet
        bridge_args.active_set_selected_member_policy = dict(policy)
        bridge_args.active_set_selection_reason = "alternate_transport_current_event"
        suffix = f".{override_suffix}" if override_suffix else ""
        bridge_args.selected_candidate_override_state = _write_live_execution_probe_override(
            bridge_args,
            member,
            override_path=ROOT
            / f"data/research/wallet_copy_live_guard_alternate_transport_override{suffix}.json",
        )
        policy_max_price = _float_or_default(policy.get("max_price"), 0.0)
        if policy_max_price > 0:
            bridge_args.price_band_decision_max_price = min(
                _float_or_default(getattr(args, "price_band_decision_max_price", 0.5), 0.5),
                policy_max_price,
            )
        policy_min_price = _float_or_default(policy.get("min_price"), 0.0)
        if policy_min_price > 0:
            bridge_args.price_band_decision_min_price = max(
                _float_or_default(getattr(args, "price_band_decision_min_price", 0.0), 0.0),
                policy_min_price,
            )
        return bridge_args

    first_member, first_event, first_policy, _ = accepted_events[0]
    bridge_args = configured_args(first_member, first_policy)
    candidate_id, source_wallet, policy_id = _active_set_member_candidate_pins(first_member)
    planner_enabled = all(
        hasattr(args, name)
        for name in ("profit_state", "history_state", "live_arm_state", "live_ledger_state")
    )
    planner_rows: list[dict[str, Any]] = []
    selected_event = first_event
    preplanned_live_result: dict[str, Any] | None = None
    last_plan_result: dict[str, Any] | None = None
    plan_run_id = stable_id(
        "alternate_plan",
        [
            str(event.source_fingerprint or event.event_id or "")
            for _member, event, _policy, _row_index in accepted_events
        ],
        length=16,
    )
    stale_generation_discarded = False
    if planner_enabled:
        for member, event, policy, row_index in accepted_events:
            if generation_fence is not None and generation_fence() is not True:
                rows[row_index]["terminal_stage"] = "STALE_GENERATION_DISCARDED"
                stale_generation_discarded = True
                continue
            event_candidate_id, event_wallet, event_policy_id = _active_set_member_candidate_pins(member)
            plan_args = configured_args(
                member,
                policy,
                override_suffix=str(event.source_fingerprint or event.event_id)[-16:],
            )
            isolated_history_path = (
                ROOT
                / "data/research/alternate_transport_plans"
                / f"{plan_run_id}_slot_{len(planner_rows):02d}.json"
            )
            atomic_write_json(
                isolated_history_path,
                {
                    "schema_version": 1,
                    "kind": "wallet_copy_alternate_transport_isolated_plan_history",
                    "generated_at": utc_now_iso(),
                    "events": [event.asdict()],
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
            )
            plan_args.history_state = str(isolated_history_path)
            plan_args.history_window_index = str(isolated_history_path.with_suffix(".index.json"))
            plan_args.live_arm_state = str(isolated_history_path.with_suffix(".arm.json"))
            plan_args.live_ledger_event_log = str(isolated_history_path.with_suffix(".events.jsonl"))
            plan_args.suppress_submission = True
            plan_result = _run_fused_live_execution(plan_args, candidate_id=event_candidate_id)
            last_plan_result = plan_result
            plan_stdout = (
                plan_result.get("stdout_json")
                if isinstance(plan_result.get("stdout_json"), dict)
                else {}
            )
            survivor_count = len(plan_stdout.get("planned_intents") or [])
            planner_row = {
                "source_wallet": event_wallet,
                "candidate_id": event_candidate_id,
                "policy_id": event_policy_id,
                "event_id": event.event_id,
                "source_fingerprint": event.source_fingerprint,
                "plan_status": plan_stdout.get("status"),
                "planned_intents": survivor_count,
                "planned_intent_hash": plan_stdout.get("planned_intent_hash"),
                "intent_blockers": plan_stdout.get("intent_blockers") or [],
                "orders_submitted": int(plan_stdout.get("orders_submitted") or 0),
                "terminal_stage": (
                    "post_protection_survivor"
                    if survivor_count > 0 and not plan_stdout.get("blockers")
                    else "PROTECTED_MEASURED_SKIP"
                ),
            }
            planner_rows.append(planner_row)
            rows[row_index]["terminal_stage"] = planner_row["terminal_stage"]
            rows[row_index]["post_protection_plan"] = planner_row
            if planner_row["terminal_stage"] != "post_protection_survivor":
                continue
            if generation_fence is not None and generation_fence() is not True:
                planner_row["terminal_stage"] = "STALE_GENERATION_DISCARDED"
                rows[row_index]["terminal_stage"] = "STALE_GENERATION_DISCARDED"
                stale_generation_discarded = True
                continue
            if not allow_submit_stage:
                bridge_args = configured_args(member, policy)
                candidate_id, source_wallet, policy_id = (
                    event_candidate_id,
                    event_wallet,
                    event_policy_id,
                )
                selected_event = event
                break
            submit_args = configured_args(member, policy)
            submit_args.preplanned_plan = plan_stdout
            submit_args.suppress_submission = False
            preplanned_live_result = _run_fused_live_execution(
                submit_args,
                candidate_id=event_candidate_id,
            )
            bridge_args = submit_args
            candidate_id, source_wallet, policy_id = event_candidate_id, event_wallet, event_policy_id
            selected_event = event
            break

    status = "QUALIFYING_EVENT_ROUTED_TO_EXISTING_COPYINTENT_BUILDER"
    if planner_enabled and not allow_submit_stage and any(
        row.get("terminal_stage") == "post_protection_survivor"
        for row in planner_rows
    ):
        status = "PAPER_SURVIVOR_READY_ACTIVATION"
    elif planner_enabled and preplanned_live_result is None:
        status = "STALE_GENERATION_DISCARDED" if stale_generation_discarded else "PROTECTED_NO_SURVIVOR"
    elif preplanned_live_result is not None:
        status = "SURVIVOR_SUBMITTED_TO_EXISTING_GUARD"
    return bridge_args, {
        "status": status,
        "flow_stage": "LIVE/ROTATE/SELF-DEV",
        "input_delta_rows": len(events),
        "invalid_delta_rows": invalid_rows,
        "selected_wallet": source_wallet,
        "selected_candidate_id": candidate_id,
        "selected_policy_id": policy_id,
        "selected_event_id": selected_event.event_id,
        "selected_source_fingerprint": selected_event.source_fingerprint,
        "selected_transaction_hash": selected_event.transaction_hash,
        "selected_market_slug": selected_event.market_slug,
        "planner_rows": planner_rows,
        "post_protection_survivors": sum(
            1 for row in planner_rows if row.get("terminal_stage") == "post_protection_survivor"
        ),
        "submit_stage_invocations": 1 if preplanned_live_result is not None else 0,
        "submit_stage_allowed": bool(allow_submit_stage),
        "submit_stage_authority": submit_stage_authority or {
            "source": "caller_supplied_legacy_boolean",
            "operator_approval_id": str(
                getattr(args, "operator_approval_id", "") or ""
            )
            or None,
            "router_live_route_allowed": None,
            "activation_age_h": None,
        },
        "generation_fence_checked": generation_fence is not None,
        "stale_generation_discarded": stale_generation_discarded,
        "_live_result": preplanned_live_result,
        "rows": rows,
        "live_orders_allowed": bool(getattr(args, "live_orders_allowed", False)),
        "submitter_invariant": "bridge plans protected intents; existing live guard submit stage remains sole submitter",
        "primary_live_candidate_observed": primary_live_candidate.get("candidate_id"),
    }


def _copy_source_wake_activation_snapshot(
    *,
    activation_state_path: str | Path = DEFAULT_COPY_SOURCE_WAKE_ACTIVATION_STATE,
    router_state_path: str | Path = DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    activation = load_json(activation_state_path, default={})
    activation = activation if isinstance(activation, dict) else {}
    router = load_json(router_state_path, default={})
    router = router if isinstance(router, dict) else {}
    gates = router.get("gates") if isinstance(router.get("gates"), dict) else {}
    frozen = router.get("frozen_cohort") if isinstance(router.get("frozen_cohort"), dict) else {}
    current = router.get("current_cohort") if isinstance(router.get("current_cohort"), dict) else {}
    reconciliation_ready = bool(
        frozen.get("input_rows") == frozen.get("terminal_rows")
        and current.get("input_rows") == current.get("terminal_rows")
        and int(frozen.get("identity_market_outcome_parity_violations") or 0) == 0
        and int(current.get("identity_market_outcome_parity_violations") or 0) == 0
        and int(frozen.get("duplicate_routes") or 0) == 0
        and int(current.get("duplicate_routes") or 0) == 0
        and (
            gates.get("frozen_110_of_110") is True
            or gates.get("frozen_reconciliation_full") is True
        )
        and (
            gates.get("current_full_reconciliation") is True
            or gates.get("current_reconciliation_full") is True
        )
        and gates.get("zero_parity_violations") is True
        and gates.get("zero_duplicate_routes") is True
    )
    activated = bool(
        activation.get("activated") is True
        and activation.get("paper_survivor_identity")
        and reconciliation_ready
    )
    now = now or dt.datetime.now(dt.timezone.utc)
    activation_generated_at = _parse_iso_datetime(activation.get("generated_at"))
    activation_age_h = (
        round(max(0.0, (now - activation_generated_at).total_seconds()) / 3600.0, 6)
        if activation_generated_at is not None
        else None
    )
    return {
        "activated": activated,
        "activation_recorded": activation.get("activated") is True,
        "reconciliation_ready": reconciliation_ready,
        "paper_survivor_identity": activation.get("paper_survivor_identity"),
        "paper_only_proof": activation.get("paper_only_proof"),
        "live_orders_allowed": activation.get("live_orders_allowed"),
        "router_live_route_allowed": router.get("live_route_allowed"),
        "activation_age_h": activation_age_h,
        "activation": activation,
        "router_gates": gates,
    }


def _alternate_transport_submit_stage_authority(
    args: argparse.Namespace,
    *,
    activation_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind bridge arming to the same explicit operator authority as live."""
    approval_id = str(getattr(args, "operator_approval_id", "") or "")
    snapshot = activation_snapshot or {}
    allowed = bool(
        getattr(args, "live_orders_allowed", False)
        and getattr(args, "execute_live", False)
        and getattr(args, "explicit_live_operator_go", False)
        and approval_id == "OP-LIVE-20260703-BELA"
    )
    return {
        "allowed": allowed,
        "source": "explicit_operator_live_decision",
        "operator_approval_id": approval_id or None,
        "router_live_route_allowed": snapshot.get("router_live_route_allowed"),
        "activation_age_h": snapshot.get("activation_age_h"),
    }


def _persist_copy_source_wake_activation(
    bridge_report: dict[str, Any],
    *,
    runtime_generation: str,
    activation_state_path: str | Path = DEFAULT_COPY_SOURCE_WAKE_ACTIVATION_STATE,
    router_state_path: str | Path = DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
) -> dict[str, Any]:
    snapshot = _copy_source_wake_activation_snapshot(
        activation_state_path=activation_state_path,
        router_state_path=router_state_path,
    )
    if snapshot["activation_recorded"]:
        return snapshot
    survivor = next(
        (
            row
            for row in bridge_report.get("planner_rows") or []
            if isinstance(row, dict)
            and row.get("terminal_stage") == "post_protection_survivor"
        ),
        None,
    )
    if survivor is None or not snapshot["reconciliation_ready"]:
        return snapshot
    identity = str(
        survivor.get("event_id")
        or survivor.get("source_fingerprint")
        or ""
    )
    if not identity:
        return snapshot
    payload = {
        "schema_version": 1,
        "kind": "copy_source_wake_activation",
        "flow_stage": "LIVE/OBSERVE/DEFEND",
        "generated_at": utc_now_iso(),
        "activated": True,
        "paper_only_proof": True,
        "live_orders_allowed": False,
        "paper_survivor_identity": identity,
        "paper_survivor": survivor,
        "runtime_generation": runtime_generation,
        "identity_rule": "transaction_hash|log_index",
        "rule": (
            "the proof identity never replays; only a later fresh identity may "
            "reach the existing sole-guard submit stage"
        ),
    }
    atomic_write_json(activation_state_path, payload)
    return _copy_source_wake_activation_snapshot(
        activation_state_path=activation_state_path,
        router_state_path=router_state_path,
    )


def _copy_source_proof_identity_keys(activation: dict[str, Any]) -> set[str]:
    survivor = (
        activation.get("paper_survivor")
        if isinstance(activation.get("paper_survivor"), dict)
        else {}
    )
    identities = {
        str(value or "").strip().lower()
        for value in (
            activation.get("paper_survivor_identity"),
            survivor.get("event_id"),
            survivor.get("source_fingerprint"),
        )
        if str(value or "").strip()
    }
    keys: set[str] = set()
    for identity in identities:
        keys.update({f"fingerprint:{identity}", f"event:{identity}"})
        match = re.search(r"(0x[0-9a-f]+)\|([0-9]+)$", identity)
        if match:
            transaction_hash, log_index = match.group(1), int(match.group(2))
            keys.update(
                {
                    f"polygon_log:{transaction_hash}|{log_index}",
                    f"event:polygon_orderfilled:{transaction_hash}|{log_index}",
                    f"fingerprint:polygon_log:{transaction_hash}|{log_index}",
                }
            )
    return keys


def _orderfilled_fast_lane_external_generation() -> str:
    digest = hashlib.sha256()
    for path in (
        MISSION_CONTRACT_PATH,
        CONFIG_GENERATION_PATH,
        ROOT / "configs/wallet_copy/entry_price_band_gate.json",
    ):
        digest.update(str(path).encode("utf-8"))
        digest.update(b"\0")
        try:
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        except FileNotFoundError:
            digest.update(b"MISSING")
        digest.update(b"\0")
    return digest.hexdigest()


def _orderfilled_fast_lane_runtime_generation(
    args: argparse.Namespace,
    active_set_runtime: dict[str, Any],
) -> str:
    protected_args = {
        name: getattr(args, name, None)
        for name in (
            "execute_live",
            "explicit_live_operator_go",
            "live_orders_allowed",
            "operator_approval_id",
            "min_live_order_usd",
            "wallet_fraction",
            "max_order_usd",
            "max_event_age_s",
            "live_build_max_observed_age_s",
            "price_band_decision_min_price",
            "price_band_decision_max_price",
            "profit_latency_window_time_suppress_gte_s",
            "profit_latency_signal_age_suppress_gte_s",
            "per_window_fill_cap",
        )
    }
    normalized_members = []
    for member in active_set_runtime.get("members") or []:
        if not isinstance(member, dict):
            continue
        policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
        normalized_members.append(
            {
                "candidate_id": member.get("candidate_id"),
                "source_wallet": str(member.get("source_wallet") or member.get("wallet") or "").lower(),
                "policy_id": member.get("policy_id") or policy.get("policy_id"),
                "enabled": member.get("enabled") is not False,
                "admission_status": member.get("admission_status") or member.get("status"),
                "policy": {
                    key: policy.get(key)
                    for key in (
                        "min_price",
                        "max_price",
                        "max_order_usd",
                        "min_order_usd",
                        "wallet_fraction",
                        "late_window_stop_s",
                        "maker_min_share_base_request_cap_usd",
                        "maker_min_share_funding_cap_usd",
                        "maker_min_share_original_policy_cap_usd",
                    )
                },
            }
        )
    normalized_members.sort(key=lambda row: (str(row["source_wallet"]), str(row["candidate_id"])))
    payload = {
        "external_generation": _orderfilled_fast_lane_external_generation(),
        # normalized_members is the authoritative effective roster/policy
        # fingerprint.  Do not include the auto-degrade state's raw generation
        # or file bytes: that state is rewritten with observational counters
        # every cycle even when no execution-relevant member field changes.
        "members": normalized_members,
        "protected_args": protected_args,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _remember_fast_lane_identities(rows: list[dict[str, Any]]) -> None:
    keys: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            keys.update(_wallet_event_identity_keys(row))
    if not keys:
        return
    with _ORDERFILLED_FAST_LANE_CONSUMED_LOCK:
        for key in sorted(keys):
            if key in _ORDERFILLED_FAST_LANE_CONSUMED:
                continue
            if len(_ORDERFILLED_FAST_LANE_CONSUMED_ORDER) == _ORDERFILLED_FAST_LANE_CONSUMED_ORDER.maxlen:
                oldest = _ORDERFILLED_FAST_LANE_CONSUMED_ORDER.popleft()
                _ORDERFILLED_FAST_LANE_CONSUMED.discard(oldest)
            _ORDERFILLED_FAST_LANE_CONSUMED_ORDER.append(key)
            _ORDERFILLED_FAST_LANE_CONSUMED.add(key)


def _fast_lane_consumed_identity_snapshot() -> set[str]:
    with _ORDERFILLED_FAST_LANE_CONSUMED_LOCK:
        return set(_ORDERFILLED_FAST_LANE_CONSUMED)


class _OrderFilledFastLane:
    """Event-driven OrderFilled consumer inside the sole live-guard process."""

    def __init__(self, base_args: argparse.Namespace):
        self.base_args = base_args
        self.enabled = bool(getattr(base_args, "orderfilled_fast_lane", True))
        self.poll_s = min(0.1, max(0.01, float(getattr(base_args, "orderfilled_fast_lane_poll_s", 0.1))))
        self.socket_path = Path(
            str(getattr(base_args, "orderfilled_fast_lane_wake_socket", DEFAULT_ORDERFILLED_FAST_LANE_WAKE_SOCKET))
        )
        self.state_path = str(
            getattr(base_args, "orderfilled_fast_lane_state", DEFAULT_ORDERFILLED_FAST_LANE_STATE)
        )
        self.activation_state_path = str(
            getattr(
                base_args,
                "copy_source_wake_activation_state",
                DEFAULT_COPY_SOURCE_WAKE_ACTIVATION_STATE,
            )
        )
        self.router_state_path = str(
            getattr(
                base_args,
                "copy_source_identity_router_state",
                DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
            )
        )
        self._snapshot_lock = threading.Lock()
        self._snapshot: dict[str, Any] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._latencies: deque[float] = deque(maxlen=500)
        self._latency_generation = ""
        self._latency_generation_resets = 0
        self._stage_counters: Counter[str] = Counter()
        self._last_stage_timing: dict[str, Any] = {}
        self._bridged_identities: set[str] = set()
        self._bridged_identity_order: deque[str] = deque(maxlen=5000)
        self._duplicate_bridge_invocations = 0
        self._duplicate_bridge_rows_suppressed = 0
        self._last_source_size = -1
        previous_state = load_json(self.state_path, default={}, cache_readonly=False)
        previous_state = previous_state if isinstance(previous_state, dict) else {}
        self._terminal_ring: deque[dict[str, Any]] = deque(
            [
                dict(row)
                for row in previous_state.get("terminal_ring") or []
                if isinstance(row, dict)
            ][-200:],
            maxlen=200,
        )
        previous_report = previous_state.get("latest_nonempty_bridge_report")
        self._last_nonempty_bridge_report = (
            dict(previous_report) if isinstance(previous_report, dict) else {}
        )

    def update_snapshot(
        self,
        *,
        args: argparse.Namespace,
        active_set_runtime: dict[str, Any],
        cycle: int,
    ) -> None:
        runtime_copy = json.loads(json.dumps(active_set_runtime, default=str))
        snapshot = {
            "args": argparse.Namespace(**vars(args)),
            "active_set_runtime": runtime_copy,
            "cycle": int(cycle),
            "external_generation": _orderfilled_fast_lane_external_generation(),
            "runtime_generation": _orderfilled_fast_lane_runtime_generation(args, runtime_copy),
            "captured_at_s": time.time(),
        }
        with self._snapshot_lock:
            self._snapshot = snapshot

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="orderfilled_fast_lane_tick",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _snapshot_copy(self) -> dict[str, Any]:
        with self._snapshot_lock:
            return dict(self._snapshot)

    def _generation_current(self, snapshot: dict[str, Any]) -> bool:
        if str(snapshot.get("external_generation") or "") != _orderfilled_fast_lane_external_generation():
            return False
        latest = self._snapshot_copy()
        return bool(
            latest
            and str(latest.get("runtime_generation") or "")
            == str(snapshot.get("runtime_generation") or "")
        )

    def _set_latency_generation(self, runtime_generation: str) -> None:
        runtime_generation = str(runtime_generation or "")
        if runtime_generation == self._latency_generation:
            return
        if self._latency_generation:
            self._latencies.clear()
            self._latency_generation_resets += 1
        self._latency_generation = runtime_generation

    def _write_state(self, payload: dict[str, Any]) -> None:
        latencies = sorted(self._latencies)
        p95 = None
        if latencies:
            p95 = latencies[min(len(latencies) - 1, max(0, math.ceil(len(latencies) * 0.95) - 1))]
        atomic_write_json(
            self.state_path,
            {
                "schema_version": 1,
                "kind": "wallet_copy_orderfilled_fast_lane",
                "flow_stage": "LIVE/OBSERVE/SELF-DEV",
                "generated_at": utc_now_iso(),
                "pid": os.getpid(),
                "thread_name": "orderfilled_fast_lane_tick",
                "sole_submitter_process": "scripts/run_wallet_copy_live_guard.py",
                "wake_socket": str(self.socket_path),
                "stat_poll_fallback_s": self.poll_s,
                "receipt_to_guard_sample_count": len(latencies),
                "receipt_to_guard_p95_s": None if p95 is None else round(p95, 6),
                "latency_aggregation_generation": self._latency_generation,
                "latency_generation_resets": self._latency_generation_resets,
                "cursor_owner": "orderfilled_fast_lane_tick",
                "cursor_owner_count": 1,
                "duplicate_bridge_invocations": self._duplicate_bridge_invocations,
                "duplicate_bridge_rows_suppressed": self._duplicate_bridge_rows_suppressed,
                "stage_counters": dict(sorted(self._stage_counters.items())),
                "last_stage_timing": self._last_stage_timing,
                "terminal_ring": list(self._terminal_ring),
                "terminal_ring_count": len(self._terminal_ring),
                "latest_nonempty_bridge_report": self._last_nonempty_bridge_report,
                **payload,
            },
        )

    def _record_bridge_terminals(
        self,
        *,
        rows: list[dict[str, Any]],
        bridge_report: dict[str, Any],
        terminal_at_s: float,
    ) -> dict[str, Any]:
        planner_by_event = {
            str(row.get("event_id") or row.get("source_fingerprint") or ""): row
            for row in bridge_report.get("planner_rows") or []
            if isinstance(row, dict)
        }
        bridge_by_event = {
            str(row.get("event_id") or row.get("source_fingerprint") or ""): row
            for row in bridge_report.get("rows") or []
            if isinstance(row, dict)
        }
        live_result = (
            bridge_report.get("_live_result")
            if isinstance(bridge_report.get("_live_result"), dict)
            else {}
        )
        submit_stdout = (
            live_result.get("stdout_json")
            if isinstance(live_result.get("stdout_json"), dict)
            else {}
        )
        order_rows = next(
            (
                value
                for key in ("orders", "submitted_orders", "order_results")
                for value in [submit_stdout.get(key)]
                if isinstance(value, list)
            ),
            [],
        )
        first_order = next((row for row in order_rows if isinstance(row, dict)), {})
        submit_status = str(
            submit_stdout.get("status")
            or live_result.get("status")
            or bridge_report.get("status")
            or ""
        )
        serialized_submit = json.dumps(submit_stdout, sort_keys=True, default=str).lower()
        fak_no_match = "fak_no_match" in serialized_submit or "fak no match" in serialized_submit
        outcome = {
            "submit_attempted": bool(int(bridge_report.get("submit_stage_invocations") or 0)),
            "submit_status": submit_status or None,
            "orders_submitted": int(submit_stdout.get("orders_submitted") or 0),
            "order_id": (
                submit_stdout.get("order_id")
                or first_order.get("order_id")
                or first_order.get("id")
            ),
            "order_status": first_order.get("status") or submit_status or None,
            "fak_no_match": fak_no_match,
            "submitted_intent_hash": submit_stdout.get("planned_intent_hash"),
        }
        selected_identity = str(
            bridge_report.get("selected_event_id")
            or bridge_report.get("selected_source_fingerprint")
            or ""
        )
        for source_row in rows:
            event_id = str(source_row.get("event_id") or "")
            source_fingerprint = str(source_row.get("source_fingerprint") or "")
            identity = event_id or source_fingerprint
            bridge_row = bridge_by_event.get(event_id) or bridge_by_event.get(source_fingerprint) or {}
            planner_row = planner_by_event.get(event_id) or planner_by_event.get(source_fingerprint) or {}
            raw = source_row.get("raw") if isinstance(source_row.get("raw"), dict) else {}
            selected = identity == selected_identity or event_id == selected_identity
            terminal = {
                "identity": identity,
                "event_id": event_id or None,
                "source_fingerprint": source_fingerprint or None,
                "source_wallet": source_row.get("source_wallet"),
                "member_attribution": raw.get("member_attribution") or "direct",
                "member_policy_result": bridge_row.get("policy_reason")
                or bridge_row.get("terminal_stage"),
                "protection_passed": bridge_row.get("protection_passed"),
                "protection_terminal": planner_row.get("terminal_stage")
                or bridge_row.get("terminal_stage")
                or bridge_report.get("status"),
                "planned_intent_hash": planner_row.get("planned_intent_hash"),
                "submitted_intent_hash": (
                    outcome["submitted_intent_hash"] if selected else None
                ),
                "copyintent_parity": (
                    bool(
                        planner_row.get("planned_intent_hash")
                        and planner_row.get("planned_intent_hash")
                        == outcome["submitted_intent_hash"]
                    )
                    if selected and outcome["submit_attempted"]
                    else None
                ),
                "submit_attempted": outcome["submit_attempted"] if selected else False,
                "orders_submitted": outcome["orders_submitted"] if selected else 0,
                "order_id": outcome["order_id"] if selected else None,
                "order_status": outcome["order_status"] if selected else None,
                "fak_no_match": outcome["fak_no_match"] if selected else False,
                "event_ts": source_row.get("event_ts"),
                "observed_ts": source_row.get("observed_ts"),
                "raw_received_at_s": raw.get("raw_received_at_s"),
                "sidecar_appended_at_s": raw.get("sidecar_appended_at_s"),
                "terminal_at_s": terminal_at_s,
                "terminal_at": dt.datetime.fromtimestamp(
                    terminal_at_s, tz=dt.timezone.utc
                ).isoformat(),
            }
            self._terminal_ring.append(terminal)
        return outcome

    def _tick(self, *, wake_source: str, wake_received_at_s: float | None = None) -> None:
        tick_started_at_s = time.time()
        tick_started_perf = time.perf_counter()
        wake_received_at_s = float(wake_received_at_s or tick_started_at_s)
        snapshot = self._snapshot_copy()
        if not snapshot:
            self._write_state({"status": "WAITING_FOR_RUNTIME_SNAPSHOT", "wake_source": wake_source})
            return
        args = snapshot["args"]
        active_set_runtime = snapshot["active_set_runtime"]
        selection = _orderfilled_live_source_selection(args)
        if not selection.get("gate_passed"):
            self._write_state({"status": "PAPER_GATE_CLOSED", "wake_source": wake_source})
            return
        runtime_generation = str(snapshot.get("runtime_generation") or "")
        self._set_latency_generation(runtime_generation)
        self._stage_counters[f"wake_{wake_source}"] += 1
        lock_wait_started = time.perf_counter()
        with _ORDERFILLED_CURSOR_LOCK:
            lock_acquired_at_s = time.time()
            lock_wait_s = time.perf_counter() - lock_wait_started
            self._stage_counters["cursor_lock_acquisitions"] += 1
            handoff = _initialize_orderfilled_sidecar_cursor(
                source_jsonl=selection["source_jsonl"],
                old_cursor_state=getattr(args, "active_member_orderfilled_direct_cursor_state", ""),
                new_cursor_state=selection["cursor_state"],
                bootstrap_bytes=int(getattr(args, "polygon_ws_premerge_tail_bytes", 64 * 1024 * 1024)),
            )
            delta_parse_started_at_s = time.time()
            delta_parse_started_perf = time.perf_counter()
            rows, source_report = _active_member_orderfilled_direct_delta(
                polygon_jsonl=selection["source_jsonl"],
                gate_state_path=getattr(args, "active_member_orderfilled_hot_source_state", ""),
                accumulator_state_path=getattr(args, "active_member_orderfilled_accumulator_state", ""),
                cursor_state_path=selection["cursor_state"],
                active_set_runtime=active_set_runtime,
                now_ts=time.time(),
                bootstrap_bytes=int(
                    getattr(args, "active_member_orderfilled_direct_bootstrap_bytes", 8 * 1024 * 1024)
                    or 0
                ),
                source_identity_router_state_path=self.router_state_path,
            )
            delta_parse_completed_at_s = time.time()
            delta_parse_s = time.perf_counter() - delta_parse_started_perf
        unique_rows: list[dict[str, Any]] = []
        for row in rows:
            identity = str(row.get("event_id") or row.get("source_event_id") or "")
            if identity and identity in self._bridged_identities:
                self._duplicate_bridge_rows_suppressed += 1
                continue
            if identity:
                if len(self._bridged_identity_order) == self._bridged_identity_order.maxlen:
                    oldest = self._bridged_identity_order.popleft()
                    self._bridged_identities.discard(oldest)
                self._bridged_identity_order.append(identity)
                self._bridged_identities.add(identity)
            unique_rows.append(row)
        rows = unique_rows
        self._stage_counters["delta_parse_invocations"] += 1
        bridge_started_at_s = time.time()
        bridge_invoked = False
        terminal_at_s = bridge_started_at_s
        event_timings: list[dict[str, Any]] = []
        for row in rows:
            raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
            latency = raw.get("receipt_to_guard_s")
            if isinstance(latency, (int, float)):
                self._latencies.append(float(latency))
            raw_received_at_s = raw.get("raw_received_at_s")
            sidecar_appended_at_s = raw.get("sidecar_appended_at_s")
            event_timings.append(
                {
                    "event_id": row.get("event_id"),
                    "raw_received_at_s": raw_received_at_s,
                    "sidecar_appended_at_s": sidecar_appended_at_s,
                    "datagram_received_at_s": wake_received_at_s if wake_source == "writer_datagram" else None,
                    "cursor_lock_acquired_at_s": lock_acquired_at_s,
                    "delta_parse_completed_at_s": delta_parse_completed_at_s,
                    "bridge_started_at_s": bridge_started_at_s,
                    "receipt_to_append_s": (
                        round(float(sidecar_appended_at_s) - float(raw_received_at_s), 6)
                        if isinstance(raw_received_at_s, (int, float))
                        and isinstance(sidecar_appended_at_s, (int, float))
                        else None
                    ),
                    "append_to_wake_s": (
                        round(wake_received_at_s - float(sidecar_appended_at_s), 6)
                        if wake_source == "writer_datagram"
                        and isinstance(sidecar_appended_at_s, (int, float))
                        else None
                    ),
                }
            )
        self._last_stage_timing = {
            "wake_source": wake_source,
            "tick_started_at_s": tick_started_at_s,
            "datagram_received_at_s": wake_received_at_s if wake_source == "writer_datagram" else None,
            "cursor_lock_acquired_at_s": lock_acquired_at_s,
            "cursor_lock_wait_s": round(lock_wait_s, 6),
            "delta_parse_started_at_s": delta_parse_started_at_s,
            "delta_parse_completed_at_s": delta_parse_completed_at_s,
            "delta_parse_s": round(delta_parse_s, 6),
            "bridge_started_at_s": bridge_started_at_s,
            "terminal_at_s": terminal_at_s,
            "tick_elapsed_s": round(time.perf_counter() - tick_started_perf, 6),
            "events": event_timings[-20:],
            "bridge_invoked": False,
        }
        if not rows:
            self._write_state(
                {
                    "status": "PASS_NO_CURRENT_EVENT",
                    "wake_source": wake_source,
                    "runtime_generation": snapshot.get("runtime_generation"),
                    "source_report": source_report,
                    "cursor_handoff": handoff,
                }
            )
            return
        bridge_input = {"matching_events_delta": rows}
        bridge_invoked = True
        self._stage_counters["bridge_invocations"] += 1
        activation_before = _copy_source_wake_activation_snapshot(
            activation_state_path=self.activation_state_path,
            router_state_path=self.router_state_path,
        )
        submit_stage_authority = _alternate_transport_submit_stage_authority(
            args,
            activation_snapshot=activation_before,
        )
        proof_identity_keys = _copy_source_proof_identity_keys(
            activation_before.get("activation")
            if isinstance(activation_before.get("activation"), dict)
            else {}
        )
        _routed_args, bridge_report = _alternate_transport_copyintent_bridge(
            args,
            active_set_runtime=active_set_runtime,
            active_set_rtds_premerge=bridge_input,
            now_ts=time.time(),
            exclude_identity_keys=proof_identity_keys,
            generation_fence=lambda: self._generation_current(snapshot),
            allow_submit_stage=bool(submit_stage_authority["allowed"]),
            submit_stage_authority=submit_stage_authority,
        )
        activation_after = _persist_copy_source_wake_activation(
            bridge_report,
            runtime_generation=runtime_generation,
            activation_state_path=self.activation_state_path,
            router_state_path=self.router_state_path,
        )
        bridge_report["wake_activation_before"] = activation_before
        bridge_report["wake_activation_after"] = activation_after
        _remember_fast_lane_identities(rows)
        terminal_at_s = time.time()
        bridge_report["submit_outcome"] = self._record_bridge_terminals(
            rows=rows,
            bridge_report=bridge_report,
            terminal_at_s=terminal_at_s,
        )
        bridge_report.pop("_live_result", None)
        self._last_nonempty_bridge_report = json.loads(
            json.dumps(bridge_report, default=str)
        )
        self._last_stage_timing["terminal_at_s"] = terminal_at_s
        self._last_stage_timing["bridge_s"] = round(max(0.0, terminal_at_s - bridge_started_at_s), 6)
        self._last_stage_timing["tick_elapsed_s"] = round(time.perf_counter() - tick_started_perf, 6)
        self._last_stage_timing["bridge_invoked"] = bridge_invoked
        self._write_state(
            {
                "status": bridge_report.get("status"),
                "wake_source": wake_source,
                "runtime_generation": snapshot.get("runtime_generation"),
                "snapshot_age_s": round(max(0.0, time.time() - float(snapshot.get("captured_at_s") or 0.0)), 6),
                "source_report": source_report,
                "cursor_handoff": handoff,
                "bridge_report": bridge_report,
            }
        )

    def _run(self) -> None:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
            listener.bind(str(self.socket_path))
            listener.settimeout(self.poll_s)
            self._write_state({"status": "RESIDENT_WAITING", "wake_source": "startup"})
            while not self._stop.is_set():
                wake_source = "stat_poll"
                wake_received_at_s = time.time()
                try:
                    listener.recv(4096)
                    wake_source = "writer_datagram"
                    wake_received_at_s = time.time()
                except TimeoutError:
                    wake_received_at_s = time.time()
                except OSError as exc:
                    if self._stop.is_set():
                        break
                    self._write_state({"status": "WAKE_SOCKET_ERROR", "error": f"{type(exc).__name__}: {exc}"})
                source = Path(str(getattr(self.base_args, "orderfilled_sidecar_jsonl", "") or ""))
                try:
                    source_size = source.stat().st_size
                except OSError:
                    source_size = -1
                if wake_source == "stat_poll" and source_size == self._last_source_size:
                    continue
                self._last_source_size = source_size
                try:
                    self._tick(wake_source=wake_source, wake_received_at_s=wake_received_at_s)
                except Exception as exc:  # pragma: no cover - fast lane must not terminate the sole guard.
                    self._write_state(
                        {
                            "status": "FAST_TICK_ERROR",
                            "wake_source": wake_source,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
        finally:
            listener.close()
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass


def _main_cycle_orderfilled_delta(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    fast_lane_enabled: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Keep exactly one cursor owner; the main cycle is fallback-only."""
    selection = _orderfilled_live_source_selection(args)
    if fast_lane_enabled:
        delegated = {
            "flow_stage": "LIVE/OBSERVE/SELF-DEV",
            "status": "DELEGATED_TO_FAST_LANE",
            "eligible_rows": 0,
            "cursor_owner": "orderfilled_fast_lane_tick",
            "cursor_owner_count": 1,
            "source": selection.get("source_jsonl"),
            "cursor_state": selection.get("cursor_state"),
            "rule": "main cycle never initializes, locks, reads, or advances the OrderFilled cursor while the fast lane is enabled",
        }
        return [], delegated, {"status": "DELEGATED_TO_FAST_LANE"}, selection

    handoff = {"status": "NOT_REQUIRED"}
    with _ORDERFILLED_CURSOR_LOCK:
        if selection["gate_passed"]:
            handoff = _initialize_orderfilled_sidecar_cursor(
                source_jsonl=selection["source_jsonl"],
                old_cursor_state=getattr(args, "active_member_orderfilled_direct_cursor_state", ""),
                new_cursor_state=selection["cursor_state"],
                bootstrap_bytes=int(
                    getattr(args, "polygon_ws_premerge_tail_bytes", 64 * 1024 * 1024)
                ),
            )
        rows, report = _active_member_orderfilled_direct_delta(
            polygon_jsonl=selection["source_jsonl"],
            gate_state_path=getattr(args, "active_member_orderfilled_hot_source_state", ""),
            accumulator_state_path=getattr(args, "active_member_orderfilled_accumulator_state", ""),
            cursor_state_path=selection["cursor_state"],
            active_set_runtime=active_set_runtime,
            now_ts=time.time(),
            bootstrap_bytes=int(
                getattr(args, "active_member_orderfilled_direct_bootstrap_bytes", 8 * 1024 * 1024)
                or 0
            ),
            source_identity_router_state_path=getattr(
                args,
                "copy_source_identity_router_state",
                DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
            ),
        )
    report["cursor_owner"] = "main_cycle_fallback"
    report["cursor_owner_count"] = 1
    return rows, report, handoff, selection


def _own_impact_monitor(
    *,
    live_stdout: dict[str, Any],
    window_participation: dict[str, Any],
    dataapi_poll_result: dict[str, Any],
    active_set_rtds_premerge: dict[str, Any],
    live_ledger: dict[str, Any],
) -> dict[str, Any]:
    rows = [row for row in window_participation.get("rows") or [] if isinstance(row, dict)]
    reason_counts = window_participation.get("dominant_skip_reason_counts")
    reason_counts = reason_counts if isinstance(reason_counts, dict) else {}
    stale_rows = [row for row in rows if str(row.get("dominant_skip_reason") or "") == "inventory_window_state_stale"]
    eligible_rows = [row for row in rows if int(row.get("wallet_eligible_orders") or 0) > 0]
    poll_summary = dataapi_poll_result.get("summary") if isinstance(dataapi_poll_result.get("summary"), dict) else {}
    ledger_summary = live_ledger.get("summary") if isinstance(live_ledger.get("summary"), dict) else {}
    own_submits = sum(int(row.get("our_submits") or 0) for row in eligible_rows)
    own_fills = sum(int(row.get("our_fills") or 0) for row in eligible_rows)
    wallet_eligible_orders = sum(int(row.get("wallet_eligible_orders") or 0) for row in eligible_rows)
    status = "PASS" if not stale_rows else "STALE_INVENTORY_CAUSE_NAMED"
    return {
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN",
        "status": status,
        "named_cause": (
            "active_set_history_freshness_skew"
            if stale_rows
            else "no_stale_inventory_rows_in_current_retained_participation"
        ),
        "cause_detail": (
            "active-set member windows were retained as wallet-eligible, but selected-member-only history refresh left "
            "their latest inventory observations older than the live-build freshness gate; active-set RTDS premerge is "
            "the live-ready fix path."
            if stale_rows
            else "retained participation rows do not currently attribute misses to stale inventory."
        ),
        "window_rows": len(rows),
        "eligible_rows": len(eligible_rows),
        "stale_inventory_rows": len(stale_rows),
        "dominant_skip_reason_counts": dict(sorted((str(k), int(v)) for k, v in reason_counts.items())),
        "wallet_eligible_orders": wallet_eligible_orders,
        "our_submits": own_submits,
        "our_fills": own_fills,
        "orders_submitted_this_cycle": int(live_stdout.get("orders_submitted") or 0),
        "fresh_candidate_intents": int(live_stdout.get("fresh_candidate_intents") or 0),
        "new_live_candidate_intents": int(live_stdout.get("new_live_candidate_intents") or 0),
        "live_orders": int(ledger_summary.get("live_orders") or 0),
        "filled_orders": int(ledger_summary.get("filled_orders") or 0),
        "rejected_orders": int(ledger_summary.get("rejected_orders") or 0),
        "latest_order_ts": ledger_summary.get("latest_order_ts"),
        "active_set_rtds_premerge_status": active_set_rtds_premerge.get("status"),
        "active_set_rtds_wallets_refreshed": int(active_set_rtds_premerge.get("wallets_refreshed") or 0),
        "active_set_rtds_new_matching_events": int(active_set_rtds_premerge.get("new_matching_events") or 0),
        "active_set_poll_only_signals": int(poll_summary.get("poll_only_signals") or 0),
        "active_set_fresh_poll_only_signals": int(poll_summary.get("fresh_poll_only_signals") or 0),
        "next_action": (
            "run one guard cycle with active-set RTDS premerge and verify stale rows stop increasing or become "
            "explicit measured-skip rows"
            if stale_rows
            else "continue monitoring own submits/fills against active wallet windows"
        ),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _run_self_feed_ledger_diff(args: argparse.Namespace) -> dict[str, Any]:
    profile_started = time.perf_counter()
    if not bool(getattr(args, "self_feed_ledger_diff", True)):
        return {
            "status": "DISABLED",
            "summary": {"ledger_filled_tx_groups": 0, "matched_ledger_tx_groups": 0},
            "paper_only": True,
            "live_orders_allowed": False,
        }
    diff_args = argparse.Namespace(
        ledger=str(getattr(args, "live_ledger_state", "data/research/wallet_copy_live_execution_state.json")),
        scorecard="data/research/wallet_copy_daily_scorecard_current.json",
        resolutions="data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
        output=str(getattr(args, "self_feed_ledger_diff_state", DEFAULT_SELF_FEED_VS_LEDGER_STATE)),
        self_feed_log=str(getattr(args, "self_feed_log", DEFAULT_SELF_FEED_LOG)),
        user="",
        start_iso="",
        end_iso="",
        day="",
        limit=int(getattr(args, "self_feed_ledger_diff_limit", 200)),
        max_pages=int(getattr(args, "self_feed_ledger_diff_pages", 10)),
        timeout_s=float(getattr(args, "self_feed_ledger_diff_timeout_s", 2.0)),
        ledger_missing_grace_s=float(getattr(args, "self_feed_ledger_missing_grace_s", 300.0)),
        data_api_base_url="https://data-api.polymarket.com",
        polygon_rpc_url=str(os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")),
        polygon_lookback_blocks=int(getattr(args, "self_feed_polygon_lookback_blocks", 700)),
        enable_polygon=bool(getattr(args, "self_feed_polygon", True)),
    )
    reconcile_started = time.perf_counter()
    try:
        report = _run_self_feed_reconcile(diff_args)
    except Exception as exc:  # pragma: no cover - self-watch cannot stop live trading.
        return {
            "status": "ERROR",
            "error": f"{type(exc).__name__}: {exc}",
            "summary": {
                "ledger_filled_tx_groups": 0,
                "matched_ledger_tx_groups": 0,
                "ledger_missing_self_feed_critical": 0,
                "self_feed_missing_ledger_critical": 0,
            },
            "paper_only": True,
            "live_orders_allowed": False,
            "profile": {
                "total_s": round(time.perf_counter() - profile_started, 6),
                "reconcile_s": round(time.perf_counter() - reconcile_started, 6),
            },
        }
    reconcile_s = round(time.perf_counter() - reconcile_started, 6)
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    return {
        "status": report.get("status"),
        "generated_at": report.get("generated_at"),
        "state": str(diff_args.output),
        "self_feed_log": str(diff_args.self_feed_log),
        "summary": summary,
        "top_diffs": report.get("top_diffs") if isinstance(report.get("top_diffs"), list) else [],
        "paper_only": True,
        "live_orders_allowed": False,
        "profile": {
            "total_s": round(time.perf_counter() - profile_started, 6),
            "reconcile_s": reconcile_s,
            "reconcile_stages": (
                report.get("runtime_profile")
                if isinstance(report.get("runtime_profile"), dict)
                else {}
            ),
            "report_projection_s": round(
                max(
                    0.0,
                    time.perf_counter() - profile_started - reconcile_s,
                ),
                6,
            ),
        },
    }


def _rtds_offset_state(args: argparse.Namespace, *, source_wallet: str = "") -> str:
    configured = str(getattr(args, "rtds_offset_state", "") or "")
    if configured:
        active_set_enabled = bool(getattr(args, "active_set", False))
        wallet = str(source_wallet or "").lower()
        if active_set_enabled and wallet:
            base = Path(configured)
            return str(base.with_name(f"{base.name}.{wallet[-8:]}.json"))
        return configured
    rtds_jsonl = str(getattr(args, "rtds_jsonl", "") or "")
    history_state = str(getattr(args, "history_state", "") or "")
    suffix = stable_id(
        "live_guard_rtds_offset",
        {"history_state": history_state, "rtds_jsonl": rtds_jsonl, "source_wallet": str(source_wallet or "").lower()},
    )[-12:]
    base = Path(history_state)
    return str(base.with_name(f"{base.name}.{suffix}.rtds_offset.json"))


def _rtds_tail_backfill_bytes(args: argparse.Namespace) -> int:
    configured = int(getattr(args, "rtds_tail_bytes", 32 * 1024 * 1024) or 0)
    cold_backfill = int(getattr(args, "rtds_cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES) or 0)
    if configured <= 0:
        return cold_backfill
    return min(configured, cold_backfill)


def _rtds_catchup_lag_s(pipeline_result: dict[str, Any], *, now_ts: float) -> float | None:
    stdout = pipeline_result.get("stdout_json") if isinstance(pipeline_result, dict) else {}
    stdout = stdout if isinstance(stdout, dict) else {}
    try:
        direct_lag = stdout.get("rtds_catchup_lag_s")
        if direct_lag is not None:
            return round(max(0.0, float(direct_lag)), 6)
    except (TypeError, ValueError):
        pass
    try:
        processed_ts = float(stdout.get("latest_processed_captured_at_s") or 0.0)
    except (TypeError, ValueError):
        processed_ts = 0.0
    if processed_ts > 0:
        return round(max(0.0, float(now_ts) - processed_ts), 6)
    try:
        observed_ts = float(stdout.get("latest_observed_ts") or 0.0)
    except (TypeError, ValueError):
        observed_ts = 0.0
    if observed_ts <= 0:
        return None
    return round(max(0.0, float(now_ts) - observed_ts), 6)


def _run_fused_live_execution(args: argparse.Namespace, *, candidate_id: str) -> dict[str, Any]:
    argv = _live_command(args, candidate_id=candidate_id)
    if not bool(getattr(args, "fuse_hot_path", True)):
        return _run_command(argv, timeout_s=float(args.live_timeout_s))
    started = time.time()
    live_args = argparse.Namespace(
        profit_state=args.profit_state,
        promotion_rotation_state=str(getattr(args, "promotion_rotation_state", DEFAULT_PROMOTION_ROTATION_STATE)),
        history_state=args.history_state,
        history_window_index=str(
            getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")
        ),
        rtds_watermark_state=str(
            getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")
        ),
        rtds_signal_watermark_state=str(
            getattr(args, "rtds_signal_watermark_state", DEFAULT_RTDS_SIGNAL_WATERMARK_STATE)
        ),
        state=args.live_arm_state,
        live_ledger_state=args.live_ledger_state,
        live_ledger_event_log=args.live_ledger_event_log,
        operator_approval_id=args.operator_approval_id,
        execute_live=bool(args.execute_live),
        explicit_live_operator_go=bool(args.explicit_live_operator_go),
        live_orders_allowed=bool(args.live_orders_allowed),
        allow_no_fresh_intents=True,
        runtime_live_paused_flag="runtime_live_paused.flag",
        candidate_id=candidate_id,
        selected_candidate_override_state=str(getattr(args, "selected_candidate_override_state", "") or ""),
        max_intents=int(args.max_intents),
        max_event_age_s=float(args.max_event_age_s),
        live_build_max_observed_age_s=_live_build_max_observed_age_s(args),
        gamma_timeout_s=5.0,
        min_live_order_usd=float(args.min_live_order_usd),
        max_window_usd=10.0,
        max_per_wallet_usd=2.0,
        wallet_copy_min_buy_price=float(getattr(args, "price_band_decision_min_price", 0.25) or 0.0),
        wallet_copy_max_buy_price=float(getattr(args, "price_band_decision_max_price", 0.50) or 0.0),
        profit_latency_window_time_suppress_gte_s=float(
            getattr(
                args,
                "profit_latency_window_time_suppress_gte_s",
                PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S,
            )
        ),
        profit_latency_signal_age_suppress_gte_s=float(
            getattr(
                args,
                "profit_latency_signal_age_suppress_gte_s",
                PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S,
            )
        ),
        min_inventory_plan_usd=1.0,
        min_agreeing_wallets=2,
        max_price_spread=0.08,
        alpha_decay_report=str(args.alpha_decay_report),
        toxicity_denylist_config=str(
            getattr(args, "toxicity_denylist_config", "configs/wallet_copy/toxicity_denylist.json")
        ),
        enable_drift_buffer=bool(args.enable_drift_buffer),
        max_drift_buffer_price=float(args.max_drift_buffer_price),
        enable_maker_fallback=bool(args.enable_maker_fallback),
        copy_model=str(getattr(args, "copy_model", "inventory") or "inventory"),
        inventory_late_window_stop_s=float(getattr(args, "inventory_late_window_stop_s", 60.0)),
        inventory_max_converge_orders_per_window=int(
            getattr(args, "inventory_max_converge_orders_per_window", 6)
        ),
        inventory_best_ask_timeout_s=float(getattr(args, "inventory_best_ask_timeout_s", 1.0)),
        inventory_future_window_lookahead_s=float(
            getattr(args, "inventory_future_window_lookahead_s", INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S)
        ),
        drip_min_tranche_usd=float(getattr(args, "drip_min_tranche_usd", 1.0)),
        drip_max_tranche_usd=float(getattr(args, "drip_max_tranche_usd", 2.5)),
        drip_max_tranches_per_window=int(getattr(args, "drip_max_tranches_per_window", 12)),
        per_window_fill_cap=int(getattr(args, "per_window_fill_cap", 1)),
        suppress_submission=bool(getattr(args, "suppress_submission", False)),
        live_guard_generation_sha256=str(
            getattr(args, "live_guard_generation_sha256", "") or ""
        ),
    )
    try:
        preplanned_plan = getattr(args, "preplanned_plan", None)
        if isinstance(preplanned_plan, dict):
            rc, summary = _submit_preplanned_live_execution(live_args, plan=preplanned_plan)
        else:
            rc, summary = _run_live_execution(live_args)
        return _callable_result(
            argv=argv,
            started=started,
            returncode=rc,
            stdout_json=summary,
            mode="in_process",
        )
    except Exception as exc:  # pragma: no cover - defensive live evidence
        return _callable_result(
            argv=argv,
            started=started,
            returncode=1,
            stdout_json={},
            mode="in_process",
            stderr_tail=f"{type(exc).__name__}: {exc}",
        )


def _wallet_fresh_rows_from_meta(meta: dict[str, Any]) -> int:
    fresh_by_source = meta.get("fresh_buy_rows_le_10s_by_source")
    if isinstance(fresh_by_source, dict):
        return sum(int(value or 0) for value in fresh_by_source.values())
    return 0


def _wallet_best_lag_from_meta(meta: dict[str, Any]) -> float:
    lag_by_source = meta.get("freshest_buy_lag_s_by_source")
    lags: list[float] = []
    if isinstance(lag_by_source, dict):
        for value in lag_by_source.values():
            try:
                lags.append(float(value))
            except (TypeError, ValueError):
                continue
    return min(lags) if lags else float("inf")


def _probe_candidate_members(
    args: argparse.Namespace,
    active_set_runtime: dict[str, Any],
    dataapi_poll_result: dict[str, Any],
    *,
    max_members: int,
    force_wallets: set[str] | None = None,
    shadow_candidate_members: list[dict[str, Any]] | None = None,
    include_all_runtime_members: bool = False,
    round_robin_offset: int = 0,
) -> list[dict[str, Any]]:
    members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    selected = active_set_runtime.get("selected_member") if isinstance(active_set_runtime.get("selected_member"), dict) else {}
    selected_wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").strip().lower()
    force_wallets = {str(wallet or "").strip().lower() for wallet in (force_wallets or set()) if str(wallet or "").strip()}
    shadow_members: list[dict[str, Any]] = []
    seen_shadow_wallets: set[str] = set()
    runtime_wallets = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        for member in members
        if isinstance(member, dict)
    }
    for member in shadow_candidate_members or []:
        if not isinstance(member, dict):
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if not wallet or wallet in runtime_wallets or wallet in seen_shadow_wallets:
            continue
        shadow_members.append(member)
        seen_shadow_wallets.add(wallet)
    fetch_meta = (
        dataapi_poll_result.get("fetch_meta")
        if isinstance(dataapi_poll_result.get("fetch_meta"), dict)
        else {}
    )
    watermark_path = Path(str(getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")))
    if not watermark_path.is_absolute():
        watermark_path = ROOT / watermark_path
    watermarks = load_json(watermark_path, default={})
    watermark_rows = watermarks.get("wallets") if isinstance(watermarks, dict) and isinstance(watermarks.get("wallets"), dict) else {}
    now_ts = time.time()
    rows: list[tuple[int, int, int, float, int, dict[str, Any]]] = []
    for idx, member in enumerate(members):
        if not isinstance(member, dict):
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if not wallet:
            continue
        if wallet == selected_wallet and wallet not in force_wallets:
            continue
        if wallet in force_wallets:
            rows.append((-10_000_000, -10_000_000, 0, 0.0, idx, member))
            continue
        if include_all_runtime_members:
            rows.append((0, 0, 0, 0.0, idx, member))
            continue
        meta = fetch_meta.get(wallet) if isinstance(fetch_meta.get(wallet), dict) else {}
        policy_rows = _meta_first_number(
            meta,
            {
                "fresh_policy_compatible_buy_rows_le_30s",
                "policy_compatible_fresh_buy_rows_le_30s",
                "policy_compatible_fresh_le_30",
                "policy_compatible_fresh_le_30s",
                "recent_policy_compatible_fresh_buy_events_le_30s",
            },
        )
        fresh_rows = _wallet_fresh_rows_from_meta(meta)
        watermark = watermark_rows.get(wallet) if isinstance(watermark_rows.get(wallet), dict) else {}
        try:
            watermark_age_s = max(0.0, now_ts - float(watermark.get("latest_checked_ts") or 0.0))
        except (TypeError, ValueError):
            watermark_age_s = float("inf")
        watermark_fresh = bool(watermark and watermark_age_s <= 15.0 and int(watermark.get("retained_matching_rows") or 0) > 0)
        if int(policy_rows or 0) <= 0 and fresh_rows <= 0 and not watermark_fresh:
            continue
        watermark_rank = 0 if watermark_fresh else 1
        rows.append((-int(policy_rows or 0), -fresh_rows, watermark_rank, _wallet_best_lag_from_meta(meta), idx, member))
    sorted_rows = sorted(rows)
    force_rows = [row for row in sorted_rows if row[0] <= -10_000_000]
    normal_rows = [row for row in sorted_rows if row[0] > -10_000_000]
    if include_all_runtime_members and normal_rows:
        offset = max(0, int(round_robin_offset or 0)) % len(normal_rows)
        normal_rows = normal_rows[offset:] + normal_rows[:offset]
    selected_rows = normal_rows[: max(0, int(max_members))]
    return (
        shadow_members
        + [member for *_prefix, member in force_rows]
        + [member for *_prefix, member in selected_rows]
    )


def _active_set_live_execution_probe_member_cap(
    args: argparse.Namespace,
    *,
    runtime_member_count: int = 0,
) -> int:
    if bool(getattr(args, "active_set_evaluate_all_runtime_members_per_cycle", False)):
        return max(DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_SLOW_PATH_MEMBER_CAP, int(runtime_member_count or 0))
    return max(
        0,
        int(
            getattr(
                args,
                "active_set_live_execution_probe_max_members",
                DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_MAX_MEMBERS,
            )
            or 0
        ),
    )


def _active_set_live_execution_probe_round_robin_offset(
    cycle: int,
    *,
    every_n: int,
    member_cap: int,
) -> int:
    if int(member_cap) <= 0:
        return 0
    pass_index = max(0, (max(1, int(cycle)) - 1) // max(1, int(every_n)))
    return pass_index * int(member_cap)


def _run_active_set_live_execution_probes(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    dataapi_poll_result: dict[str, Any],
    force_wallets: set[str] | None = None,
    shadow_candidate_members: list[dict[str, Any]] | None = None,
    max_members_override: int | None = None,
    include_all_runtime_members_override: bool | None = None,
    round_robin_offset: int = 0,
    parallel: bool = False,
    max_workers: int | None = None,
) -> dict[str, Any]:
    if not bool(getattr(args, "active_set_live_execution_probes", True)):
        return {"enabled": False, "reason": "active_set_live_execution_probes_disabled"}
    all_runtime_members = (
        bool(getattr(args, "active_set_evaluate_all_runtime_members_per_cycle", False))
        if include_all_runtime_members_override is None
        else bool(include_all_runtime_members_override)
    )
    runtime_members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    max_members = (
        max(0, int(max_members_override))
        if max_members_override is not None
        else int(
            len(runtime_members)
            if all_runtime_members
            else getattr(
                args,
                "active_set_live_execution_probe_max_members",
                DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_MAX_MEMBERS,
            )
        )
    )
    members = _probe_candidate_members(
        args,
        active_set_runtime,
        dataapi_poll_result,
        max_members=max_members,
        force_wallets=force_wallets,
        shadow_candidate_members=shadow_candidate_members,
        include_all_runtime_members=all_runtime_members,
        round_robin_offset=round_robin_offset,
    )
    def run_probe_member(member: dict[str, Any]) -> dict[str, Any] | None:
        candidate_id = str(member.get("candidate_id") or "")
        if not candidate_id:
            return None
        probe_args = argparse.Namespace(**vars(args))
        probe_path = _live_execution_probe_path(member)
        probe_args.selected_candidate_override_state = _write_live_execution_probe_override(
            args,
            member,
            override_path=_live_execution_probe_override_path(probe_path),
        )
        probe_args.live_arm_state = str(probe_path)
        probe_args.live_ledger_event_log = str(probe_path.with_suffix(".events.jsonl"))
        probe_args.execute_live = False
        probe_args.explicit_live_operator_go = False
        probe_args.live_orders_allowed = False
        started = time.time()
        result = _run_fused_live_execution(probe_args, candidate_id=candidate_id)
        stdout = result.get("stdout_json") if isinstance(result.get("stdout_json"), dict) else {}
        summary = stdout.get("candidate_intent_summary") if isinstance(stdout.get("candidate_intent_summary"), dict) else {}
        return {
            "candidate_id": candidate_id,
            "source_wallet": str(member.get("source_wallet") or member.get("wallet") or "").strip().lower(),
            "path": str(probe_path),
            "returncode": result.get("returncode"),
            "duration_s": round(max(0.0, time.time() - started), 6),
            "status": stdout.get("status"),
            "fresh_candidate_intents": int(summary.get("fresh_candidate_intents") or 0),
            "fresh_candidate_intents_after_toxicity_protection": int(
                summary.get("fresh_candidate_intents_after_toxicity_protection") or 0
            ),
            "fresh_candidate_intents_after_expected_fee_gate": int(
                summary.get("fresh_candidate_intents_after_expected_fee_gate") or 0
            ),
            "paper_only": True,
            "live_orders_allowed": False,
        }

    worker_count = max(1, min(len(members), int(max_workers or DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_SLOW_PATH_MEMBER_CAP)))
    if parallel and len(members) > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            rows = [row for row in executor.map(run_probe_member, members) if row is not None]
    else:
        rows = [row for row in (run_probe_member(member) for member in members) if row is not None]
    return {
        "enabled": True,
        "flow_stage": "LIVE/SELF-DEV",
        "paper_only": True,
        "live_orders_allowed": False,
        "evaluation_mode": "all_runtime_members_per_cycle"
        if all_runtime_members
        else "fresh_runtime_member_sample",
        "all_runtime_members_per_cycle": all_runtime_members,
        "runtime_member_count": len(runtime_members),
        "member_cap": max_members,
        "round_robin_offset": max(0, int(round_robin_offset or 0)) if all_runtime_members else 0,
        "cached_results_used": False,
        "parallel_probe_execution": bool(parallel and len(members) > 1),
        "probe_worker_count": worker_count if parallel and len(members) > 1 else 1,
        "probed_members_this_cycle": len(rows),
        "probed_members": len(rows),
        "force_wallets": sorted(force_wallets or []),
        "shadow_candidate_seat_wallets": [
            str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
            for member in shadow_candidate_members or []
            if isinstance(member, dict)
        ],
        "rows": rows,
        "rule": "read-only member probes feed cadenced guarded promotion evidence; cached results are reused between slow-path passes",
    }


def _intent_time_copyability_record_from_decision(
    decision: dict[str, Any],
    *,
    candidate_id: str,
    generated_at: str,
) -> dict[str, Any] | None:
    if not isinstance(decision, dict):
        return None
    if str(decision.get("wallet_action") or "BUY").upper() != "BUY":
        return None
    intent_id = str(decision.get("intent_id") or "")
    if not intent_id:
        return None
    blockers: list[str] = []
    try:
        event_age_s = float(decision.get("event_age_s"))
    except (TypeError, ValueError):
        event_age_s = -1.0
        blockers.append("missing_event_age")
    if event_age_s >= 0 and event_age_s > 10.0:
        blockers.append("event_age_above_cap")
    best_ask = _float_or_default(decision.get("best_ask"), 0.0)
    source_price = _float_or_default(decision.get("source_price") or decision.get("limit_price"), 0.0)
    max_copy_price = _float_or_default(decision.get("max_copy_price"), 0.0)
    if max_copy_price <= 0 and source_price > 0:
        max_copy_price = min(0.99, source_price * 1.015)
    fill_ratio = _float_or_default(decision.get("fill_ratio"), 0.0)
    if best_ask <= 0:
        blockers.append("no_ask_liquidity")
    if max_copy_price > 0 and best_ask > max_copy_price:
        blockers.append("best_ask_above_slippage_cap")
    if fill_ratio < 0.999:
        blockers.append("depth_below_min_fill_ratio")
    route_status = str(decision.get("clob_route_status") or "")
    if route_status and route_status != "PASS":
        blockers.append("book_route_not_pass")
    accepted = not blockers
    details = {
        "blockers": blockers,
        "primary_blocker": blockers[0] if blockers else None,
        "event_age_s": None if event_age_s < 0 else round(event_age_s, 6),
        "max_event_age_s": 10.0,
        "clob_best_ask": round(best_ask, 6) if best_ask > 0 else None,
        "clob_max_copy_price": round(max_copy_price, 6) if max_copy_price > 0 else None,
        "clob_fill_ratio": round(fill_ratio, 6),
        "clob_fillable_usd": decision.get("fillable_usd"),
        "clob_book_status": "OK" if best_ask > 0 else "MISSING",
        "clob_instant_fill_status": decision.get("instant_fill_status"),
        "clob_blocking_reason": decision.get("blocking_reason"),
        "clob_book_timestamp": decision.get("book_timestamp"),
        "clob_book_hash": decision.get("book_hash"),
        "source_price": round(source_price, 6) if source_price > 0 else None,
        "max_book_slippage_bps": 150.0,
        "min_clob_fill_ratio": 0.999,
    }
    copy_size_usd = _float_or_default(decision.get("copy_size_usd"), 0.0)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_intent_time_copyability_proof_record",
        "flow_stage": "LIVE/DEFEND",
        "generated_at": generated_at,
        "source": "live_guard_intent_time_probe",
        "candidate_id": candidate_id,
        "intent_id": intent_id,
        "source_event_id": intent_id,
        "source_wallet": str(decision.get("source_wallet") or "").lower(),
        "market_slug": decision.get("market_slug"),
        "outcome": decision.get("outcome"),
        "token_id": decision.get("token_id"),
        "wallet_action": "BUY",
        "event_ts": decision.get("event_ts"),
        "source_event_ts": decision.get("event_ts"),
        "observed_ts": decision.get("observed_ts"),
        "event_age_s": None if event_age_s < 0 else round(event_age_s, 6),
        "api_latency_s": None if event_age_s < 0 else round(event_age_s, 6),
        "wallet_api_fetch_duration_s": decision.get("fetch_duration_s"),
        "copy_status": "COPIED_FILLED" if accepted else "FILTERED",
        "copyability_accepted": accepted,
        "copyability_reason": "accepted" if accepted else blockers[0],
        "copyability_policy_id": "wallet_copy_copyability_gate_v1",
        "copyability_details": details,
        "profit_policy_accepted": True,
        "profit_policy_reason": "accepted",
        "filter_policy": None if accepted else "copyability",
        "clob_book_status": details["clob_book_status"],
        "clob_instant_fill_status": details["clob_instant_fill_status"],
        "clob_best_ask": details["clob_best_ask"],
        "clob_max_copy_price": details["clob_max_copy_price"],
        "clob_fill_ratio": details["clob_fill_ratio"],
        "clob_fillable_usd": details["clob_fillable_usd"],
        "clob_blocking_reason": details["clob_blocking_reason"],
        "clob_book_admission_relevant": True,
        "fill_source": "clob_book_evidence" if accepted else None,
        "fill_ratio": round(fill_ratio, 6),
        "copy_size_usd": round(copy_size_usd, 6),
        "filled_size_usd": round(copy_size_usd, 6) if accepted else 0.0,
        "missed_copy_reason": None if accepted else blockers[0],
        "live_orders_allowed": False,
        "paper_only": True,
    }


def _write_intent_time_copyability_proof_state(
    args: argparse.Namespace,
    *,
    live_probe_result: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    output = Path(str(getattr(args, "intent_time_copyability_proof_state", DEFAULT_INTENT_TIME_COPYABILITY_PROOF_STATE)))
    if not output.is_absolute():
        output = ROOT / output
    previous = load_json(output, default={})
    previous_rows = previous.get("records") if isinstance(previous, dict) and isinstance(previous.get("records"), list) else []
    rows: list[dict[str, Any]] = [dict(row) for row in previous_rows if isinstance(row, dict)]
    probe_rows = [row for row in live_probe_result.get("rows") or [] if isinstance(row, dict)]
    fresh_rows: list[dict[str, Any]] = []
    for probe_row in probe_rows:
        if not isinstance(probe_row, dict):
            continue
        probe_path = Path(str(probe_row.get("path") or ""))
        if not probe_path.is_absolute():
            probe_path = ROOT / probe_path
        probe = load_json(probe_path, default={})
        if not isinstance(probe, dict):
            continue
        candidate_id = str(probe.get("candidate_id") or probe_row.get("candidate_id") or "")
        gate = probe.get("inventory_best_ask_gate")
        if not isinstance(gate, dict):
            gate = (probe.get("candidate_intent_summary") or {}).get("inventory_best_ask_gate") if isinstance(probe.get("candidate_intent_summary"), dict) else {}
        if not isinstance(gate, dict):
            continue
        for decision in gate.get("sample_decisions") or []:
            record = _intent_time_copyability_record_from_decision(
                decision,
                candidate_id=candidate_id,
                generated_at=generated_at,
            )
            if record is not None:
                fresh_rows.append(record)
    rows.extend(fresh_rows)

    def dedupe_key(row: dict[str, Any]) -> str:
        source_event_id = str(row.get("source_event_id") or "").strip()
        if not source_event_id:
            source_event_id = str(row.get("event_ts") or "").strip()
        return "|".join(
            [
                str(row.get("intent_id") or "").strip(),
                str(row.get("source_wallet") or "").strip().lower(),
                source_event_id,
            ]
        )

    def observation_at(row: dict[str, Any]) -> str:
        return str(row.get("generated_at") or row.get("observed_at") or row.get("last_observed_at") or "")

    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = dedupe_key(row)
        if key.strip("|"):
            observed_at = observation_at(row)
            existing = deduped.get(key)
            if existing is None:
                merged = dict(row)
                merged["first_observed_at"] = str(row.get("first_observed_at") or observed_at)
                merged["last_observed_at"] = str(row.get("last_observed_at") or observed_at)
                deduped[key] = merged
                continue
            first_values = [
                str(value)
                for value in (existing.get("first_observed_at"), row.get("first_observed_at"), observed_at)
                if str(value or "")
            ]
            last_values = [
                str(value)
                for value in (existing.get("last_observed_at"), row.get("last_observed_at"), observed_at)
                if str(value or "")
            ]
            merged = dict(existing)
            merged.update(row)
            if first_values:
                merged["first_observed_at"] = min(first_values)
            if last_values:
                merged["last_observed_at"] = max(last_values)
            deduped[key] = merged
    retain = max(1, int(getattr(args, "intent_time_copyability_proof_retain_rows", 5000) or 5000))
    retained = sorted(deduped.values(), key=lambda row: str(row.get("generated_at") or ""))[-retain:]
    fresh_distinct_keys = {dedupe_key(row) for row in fresh_rows if dedupe_key(row).strip("|")}
    accepted = [row for row in retained if row.get("copyability_accepted") is True]
    rejected = [row for row in retained if row.get("copyability_accepted") is not True]
    blocker_counts: Counter[str] = Counter(
        str((row.get("copyability_details") or {}).get("primary_blocker") or row.get("copyability_reason") or "unknown")
        for row in rejected
        if isinstance(row, dict)
    )
    covered_members = sorted(
        {
            str(row.get("source_wallet") or "").strip().lower()
            for row in retained
            if str(row.get("source_wallet") or "").strip()
        }
    )
    covered_members_this_cycle = sorted(
        {
            wallet
            for wallet in (
                [str(row.get("source_wallet") or "").strip().lower() for row in probe_rows]
                + [str(row.get("source_wallet") or "").strip().lower() for row in fresh_rows]
            )
            if wallet
        }
    )
    runtime_member_count = max(
        len(probe_rows),
        int(live_probe_result.get("runtime_member_count") or 0),
    )
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_intent_time_copyability_proof_state",
        "flow_stage": "LIVE/DEFEND",
        "generated_at": generated_at,
        "status": "PASS" if accepted else "WATCH",
        "paper_only": True,
        "live_orders_allowed": False,
        "source": "scripts/run_wallet_copy_live_guard.py active-set live execution probes",
        "policy": {
            "copyability_policy_id": "wallet_copy_copyability_gate_v1",
            "max_event_age_s": 10.0,
            "max_book_slippage_bps": 150.0,
            "min_clob_fill_ratio": 0.999,
        },
        "summary": {
            "records": len(retained),
            "distinct_intents": len(retained),
            "covered_members": covered_members,
            "covered_members_this_cycle": covered_members_this_cycle,
            "covered_member_count_this_cycle": len(covered_members_this_cycle),
            "runtime_member_count_this_cycle": runtime_member_count,
            "dropped_runtime_members_this_cycle": max(0, runtime_member_count - len(covered_members_this_cycle)),
            "fresh_records_this_cycle": len(fresh_rows),
            "fresh_distinct_records_this_cycle": len(fresh_distinct_keys),
            "sampled_intents_this_cycle": len(fresh_distinct_keys),
            "required_buy_copy_events": len(accepted),
            "clob_filled_buy_copy_events": len(accepted),
            "copyability_rejected_buy_events": len(rejected),
            "blocker_counts": dict(sorted(blocker_counts.items())),
        },
        "records": retained,
    }
    atomic_write_json(output, payload)
    return {
        "enabled": True,
        "state": _display_path(output),
        "status": payload["status"],
        "summary": payload["summary"],
    }


def _merge_active_set_live_execution_probe_results(
    cached_result: dict[str, Any],
    fresh_result: dict[str, Any],
    *,
    force_wallets: set[str],
) -> dict[str, Any]:
    cached = dict(cached_result) if isinstance(cached_result, dict) else {}
    fresh = fresh_result if isinstance(fresh_result, dict) else {}
    if not cached:
        cached = {
            "enabled": bool(fresh.get("enabled", True)),
            "flow_stage": "LIVE/SELF-DEV",
            "paper_only": True,
            "live_orders_allowed": False,
            "evaluation_mode": "cached_latest_probe_results",
            "all_runtime_members_per_cycle": bool(fresh.get("all_runtime_members_per_cycle", False)),
            "runtime_member_count": int(fresh.get("runtime_member_count") or 0),
            "rows": [],
            "rule": "read-only member probes feed cadenced guarded promotion evidence; cached results are reused between slow-path passes",
        }
    cached_rows = cached.get("rows") if isinstance(cached.get("rows"), list) else []
    fresh_rows = fresh.get("rows") if isinstance(fresh.get("rows"), list) else []
    merged_by_key: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    def row_key(row: dict[str, Any]) -> str:
        wallet = str(row.get("source_wallet") or "").strip().lower()
        candidate_id = str(row.get("candidate_id") or "").strip()
        return wallet or candidate_id

    for row in cached_rows:
        if not isinstance(row, dict):
            continue
        key = row_key(row)
        if not key:
            continue
        merged_by_key[key] = dict(row)
        order.append(key)
    for row in fresh_rows:
        if not isinstance(row, dict):
            continue
        key = row_key(row)
        if not key:
            continue
        if key not in merged_by_key:
            order.append(key)
        merged_by_key[key] = dict(row)
    merged = dict(cached)
    merged["rows"] = [merged_by_key[key] for key in order if key in merged_by_key]
    merged["cached_results_used"] = True
    merged["fresh_force_probe_executed_this_cycle"] = bool(fresh_rows)
    merged["force_wallets_this_cycle"] = sorted(
        str(wallet or "").strip().lower() for wallet in force_wallets if str(wallet or "").strip()
    )
    merged["probed_members_this_cycle"] = len(fresh_rows)
    merged["probed_members"] = len(merged["rows"])
    if fresh.get("runtime_member_count") is not None:
        merged["runtime_member_count"] = int(fresh.get("runtime_member_count") or 0)
    return merged


def _run_active_set_live_execution_probe_promotions(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    live_probe_result: dict[str, Any],
    max_promotions: int = 1,
) -> dict[str, Any]:
    rows = live_probe_result.get("rows") if isinstance(live_probe_result.get("rows"), list) else []
    members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    selected = active_set_runtime.get("selected_member") if isinstance(active_set_runtime.get("selected_member"), dict) else {}
    selected_wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").strip().lower()
    member_by_candidate = {
        str(member.get("candidate_id") or ""): member
        for member in members
        if isinstance(member, dict) and str(member.get("candidate_id") or "")
    }
    member_wallets = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        for member in members
        if isinstance(member, dict)
    }
    priority_freeze = _active_set_selection_priority_freeze(member_wallets)
    frozen_wallets = set(priority_freeze.get("wallets") or [])
    promotions: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        if len(promotions) >= max(0, int(max_promotions)):
            break
        if not isinstance(row, dict):
            continue
        candidate_id = str(row.get("candidate_id") or "")
        wallet = str(row.get("source_wallet") or "").strip().lower()
        if not candidate_id or candidate_id not in member_by_candidate:
            skipped.append({"candidate_id": candidate_id, "source_wallet": wallet, "reason": "unknown_probe_member"})
            continue
        if wallet == selected_wallet:
            skipped.append({"candidate_id": candidate_id, "source_wallet": wallet, "reason": "already_selected_member"})
            continue
        if wallet in frozen_wallets:
            skipped.append({"candidate_id": candidate_id, "source_wallet": wallet, "reason": "selection_priority_frozen"})
            continue
        after_fee = int(row.get("fresh_candidate_intents_after_expected_fee_gate") or 0)
        if after_fee <= 0:
            skipped.append({"candidate_id": candidate_id, "source_wallet": wallet, "reason": "probe_after_fee_zero"})
            continue
        result = _run_fused_live_execution(args, candidate_id=candidate_id)
        stdout = result.get("stdout_json") if isinstance(result.get("stdout_json"), dict) else {}
        summary = stdout.get("candidate_intent_summary") if isinstance(stdout.get("candidate_intent_summary"), dict) else {}
        promotions.append(
            {
                "candidate_id": candidate_id,
                "source_wallet": wallet,
                "returncode": result.get("returncode"),
                "status": stdout.get("status"),
                "orders_submitted": int(stdout.get("orders_submitted") or 0),
                "orders_accepted": int(stdout.get("orders_accepted") or 0),
                "orders_rejected": int(stdout.get("orders_rejected") or 0),
                "fresh_candidate_intents": int(summary.get("fresh_candidate_intents") or 0),
                "fresh_candidate_intents_after_toxicity_protection": int(
                    summary.get("fresh_candidate_intents_after_toxicity_protection") or 0
                ),
                "fresh_candidate_intents_after_expected_fee_gate": int(
                    summary.get("fresh_candidate_intents_after_expected_fee_gate") or 0
                ),
                "live_orders_allowed": bool(getattr(args, "live_orders_allowed", False)),
                "execute_live": bool(getattr(args, "execute_live", False)),
                "execution_mode": result.get("execution_mode"),
                "duration_s": result.get("duration_s"),
                "stderr_tail": result.get("stderr_tail") or "",
            }
        )
    return {
        "enabled": bool(live_probe_result.get("enabled", True)),
        "flow_stage": "LIVE/SELF-DEV",
        "rule": "same-cycle promote paper-only probes that already passed live execution gates; no selector pin or gate widening",
        "max_promotions": max(0, int(max_promotions)),
        "promoted_members": len(promotions),
        "rows": promotions,
        "skipped": skipped[:20],
        "selection_priority_freeze": priority_freeze or None,
    }


E6_SHADOW_PARK_PNL_FLOOR_USD = -30.0
E6_SHADOW_PARK_ROI_FLOOR_PCT = -5.0


def _e6_shadow_park_decision(args: argparse.Namespace) -> dict[str, Any]:
    state_path = str(getattr(args, "e6_shadow_lane_state", "data/research/e6_whale_net_flow_paper_lane_state.json"))
    state = load_json(state_path, default={})
    state = state if isinstance(state, dict) else {}
    gate = state.get("promotion_gate") if isinstance(state.get("promotion_gate"), dict) else {}
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    pnl = gate.get("resolved_paper_pnl_usd", summary.get("resolved_paper_pnl_usd"))
    roi = gate.get("resolved_paper_roi_pct", summary.get("resolved_paper_roi_pct"))
    try:
        pnl_value = float(pnl)
    except (TypeError, ValueError):
        pnl_value = None
    try:
        roi_value = float(roi)
    except (TypeError, ValueError):
        roi_value = None
    pnl_tripped = pnl_value is not None and pnl_value <= E6_SHADOW_PARK_PNL_FLOOR_USD
    roi_tripped = roi_value is not None and roi_value <= E6_SHADOW_PARK_ROI_FLOOR_PCT
    return {
        "lane": "e6_whale_net_flow_v1",
        "source_tag": "E6_WHALE_SIDE",
        "flow_stage": "ROTATE/PROMOTE",
        "state_path": state_path,
        "parked": bool(pnl_tripped or roi_tripped),
        "reason": "RULING_AQ_DECIDE_2_E6_FLOOR" if (pnl_tripped or roi_tripped) else "",
        "resolved_paper_pnl_usd": pnl_value,
        "resolved_paper_roi_pct": roi_value,
        "pnl_floor_usd": E6_SHADOW_PARK_PNL_FLOOR_USD,
        "roi_floor_pct": E6_SHADOW_PARK_ROI_FLOOR_PCT,
    }


def _shadow_lane_specs(args: argparse.Namespace, *, include_parked: bool = False) -> list[dict[str, Any]]:
    e6_decision = _e6_shadow_park_decision(args)
    specs: list[dict[str, Any]] = [
        {
            "lane": "e5_maker_first_btc5m_v1",
            "source_tag": "E5_MAKER_FIRST",
            "state_path": str(getattr(args, "e5_shadow_lane_state", "data/research/maker_first_btc5m_paper_state.json")),
            "paper_state_path": str(getattr(args, "e5_shadow_lane_state", "data/research/maker_first_btc5m_paper_state.json")),
        },
        {
            "lane": "e6_whale_net_flow_v1",
            "source_tag": "E6_WHALE_SIDE",
            "state_path": str(
                getattr(args, "e6_shadow_lane_state", "data/research/e6_whale_net_flow_paper_lane_state.json")
            ),
            "paper_state_path": str(
                getattr(args, "e6_shadow_paper_state", "data/research/e6_whale_net_flow_paper_state.json")
            ),
            "park_decision": e6_decision,
            "parked": bool(e6_decision.get("parked")),
        },
    ]
    structural_state_path = str(getattr(args, "structural_scalp_lane_state", "") or "")
    if structural_state_path:
        structural_state = load_json(structural_state_path, default={})
        structural_intents = (
            structural_state.get("current_intents")
            if isinstance(structural_state, dict) and isinstance(structural_state.get("current_intents"), list)
            else []
        )
        if structural_intents:
            specs.append(
                {
                    "lane": "btc5m_structural_scalp_v1",
                    "source_tag": "structural::intra-window-scalp",
                    "state_path": structural_state_path,
                    "paper_state_path": structural_state_path,
                }
            )
    if include_parked:
        return specs
    return [spec for spec in specs if not spec.get("parked")]


def _e5_live_route_candidates(
    args: argparse.Namespace,
    feed: dict[str, Any],
    *,
    now_ts: float | None = None,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    """Fail closed from the compact E5 paper feed to exact live-route candidates."""

    now_value = time.time() if now_ts is None else float(now_ts)
    regrade_path = str(
        getattr(args, "e5_5share_regrade_state", "data/research/e5_maker_first_5share_regrade_latest.json")
    )
    regrade = load_json(regrade_path, default={})
    regrade = regrade if isinstance(regrade, dict) else {}
    regrade_gate = regrade.get("gate") if isinstance(regrade.get("gate"), dict) else {}
    regrade_summary = regrade.get("summary") if isinstance(regrade.get("summary"), dict) else {}
    regrade_contract = regrade.get("contract") if isinstance(regrade.get("contract"), dict) else {}
    gate_checks = {
        "regrade_kind": str(regrade.get("kind") or "") == "e5_maker_first_5share_regrade",
        "gate_pass_auto_promote": regrade_gate.get("pass") is True
        and str(regrade_gate.get("decision") or "") == "PASS_AUTO_PROMOTE_FIXED_SHARES_5",
        "fixed_shares_contract": float(regrade_contract.get("size_shares") or 0.0) == 5.0
        and str(regrade_contract.get("sizing_policy_id") or "") == "fixed_shares_5",
        "copyintent_parity_clean": int(regrade_summary.get("copyintent_parity_violations") or 0) == 0,
        "resolved_gate_met": int(regrade_summary.get("resolved_distinct_executions") or 0)
        >= int(regrade_gate.get("resolved_executions_required") or 150),
        "post_fee_positive": float(regrade_summary.get("resolved_post_fee_pnl_usd") or 0.0) > 0.0
        and float(regrade_summary.get("resolved_post_fee_roi_pct") or 0.0) > 0.0,
        "maker_fill_rate_met": float(regrade_summary.get("terminal_maker_fill_rate_pct") or 0.0)
        >= float(regrade_gate.get("terminal_maker_fill_rate_required_pct") or 90.0),
        "zero_fallback": int(regrade_summary.get("fallback_violations") or 0) == 0,
        "max_notional": float(regrade_summary.get("max_notional_usd") or 0.0) <= 2.50,
    }
    gate_pass = all(gate_checks.values())
    reason_counts: Counter[str] = Counter()
    accepted: list[CopyIntent] = []
    max_event_age_s = float(getattr(args, "max_event_age_s", 30.0) or 30.0)
    max_observed_age_s = float(_live_build_max_observed_age_s(args))
    for row in feed.get("current_intents") or []:
        if not isinstance(row, dict):
            reason_counts["invalid_intent_row"] += 1
            continue
        try:
            intent = CopyIntent.from_dict(row)
        except (TypeError, ValueError):
            reason_counts["invalid_copyintent"] += 1
            continue
        metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
        e5 = metadata.get("e5_maker_first_btc5m_v1") if isinstance(
            metadata.get("e5_maker_first_btc5m_v1"), dict
        ) else {}
        top = e5.get("top_of_book") if isinstance(e5.get("top_of_book"), dict) else {}
        route = top.get("route_report") if isinstance(top.get("route_report"), dict) else {}
        fallback_markers = (
            route.get("fallback_source"),
            route.get("fallback_attempts"),
            route.get("primary_error"),
            route.get("primary_route_report"),
            route.get("suppressed_env_var"),
        )
        checks = {
            "gate_pass": gate_pass,
            "source_tag": intent.source_wallet == "E5_MAKER_FIRST",
            "lane": intent.strategy_family == "e5_maker_first_btc5m_v1"
            and intent.wallet_name == "e5_maker_first_btc5m_v1",
            "paper_source": intent.mode == "paper" and intent.live_orders_allowed is False,
            "buy_only": intent.action == "BUY",
            "fixed_shares_5": abs(float(intent.shares) - 5.0) <= 1e-9
            and str(intent.sizing_policy_id or "") == "fixed_shares_5",
            "share_native_notional": abs(float(intent.copy_size_usd) - round(5.0 * float(intent.limit_price), 6))
            <= 1e-9
            and float(intent.copy_size_usd) <= 2.50,
            "quote_price_match": abs(float(intent.limit_price) - float(e5.get("quote_price") or 0.0)) <= 1e-9,
            "token_match": bool(intent.token_id) and intent.token_id == str(e5.get("token_id") or ""),
            "side_match": intent.side == str(e5.get("side") or "")
            and intent.outcome == str(e5.get("outcome") or ""),
            "no_fallback_enforced": e5.get("enforced_no_fallback_book") is True
            and str(e5.get("book_evidence_mode") or "") == "enforced_no_fallback",
            "book_hash": str(top.get("status") or "") == "OK" and bool(str(top.get("book_hash") or "")),
            "route_pass": str(route.get("status") or "") == "PASS" and not any(bool(item) for item in fallback_markers),
            "event_fresh": intent.event_ts is not None
            and 0.0 <= now_value - float(intent.event_ts) <= max_event_age_s,
            "observed_fresh": intent.observed_ts is not None
            and 0.0 <= now_value - float(intent.observed_ts) <= max_observed_age_s,
        }
        diagnostics = _intent_runtime_diagnostics(
            intent,
            now_ts=now_value,
            max_event_age_s=max_event_age_s,
            live_build_max_observed_age_s=max_observed_age_s,
        )
        checks["live_window_open"] = bool(diagnostics.get("live_tradeable_window_open"))
        failures = [name for name, passed in checks.items() if not passed]
        if failures:
            reason_counts.update(failures)
            continue
        accepted.append(intent)
    max_intents = max(1, min(int(getattr(args, "max_intents", 6) or 6), 1))
    return accepted[:max_intents], {
        "gate_pass": gate_pass,
        "gate_checks": gate_checks,
        "raw_intents": len(feed.get("current_intents") or []),
        "strict_candidates": len(accepted),
        "selected_candidates": min(len(accepted), max_intents),
        "refusal_counts": dict(sorted(reason_counts.items())),
        "max_event_age_s": max_event_age_s,
        "max_observed_age_s": max_observed_age_s,
        "regrade_path": regrade_path,
        "regrade_cohort_sha256": str((regrade.get("source") or {}).get("cohort_sha256") or ""),
    }


def _e5_rtds_live_intents(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build fresh paper E5 intents in the sole guard; this function never submits."""

    now_ts = time.time()
    rtds_jsonl = str(getattr(args, "rtds_jsonl", "") or "")
    if not rtds_jsonl:
        return [], {"status": "NO_RTDS_SOURCE", "intents": 0, "orders_submitted": 0}
    try:
        events, feed_diagnostics = _e5_load_recent_events(
            rtds_jsonl,
            scan_limit=min(int(getattr(args, "rtds_scan_limit", 50_000) or 50_000), 10_000),
            scan_max_bytes=min(int(getattr(args, "rtds_tail_bytes", 5_242_880) or 5_242_880), 5_242_880),
            max_feed_events=2_000,
            max_event_age_s=float(getattr(args, "max_event_age_s", 30.0) or 30.0),
            now_ts=now_ts,
        )
        clob = CLOBMarketClient(
            "http://127.0.0.1:8787/clob",
            timeout_s=min(float(getattr(args, "inventory_best_ask_timeout_s", 1.0) or 1.0), 1.0),
            retries=1,
        )
        signals, signal_diagnostics = _e5_build_quote_signals(
            events,
            prior_quote_ids=set(),
            now_ts=now_ts,
            quote_lookback_s=float(getattr(args, "max_event_age_s", 30.0) or 30.0),
            max_quotes=1,
            order_usd=1.0,
            max_order_usd=1.0,
            max_price=float(getattr(args, "price_band_decision_max_price", 0.50) or 0.50),
            tick_size=0.01,
            quote_latency_s=0.25,
            cancel_before_close_s=30.0,
            fetch_clob_book=True,
            clob=clob,
            enforce_no_fallback_book=True,
            lane_id="e5_maker_first_btc5m_v1",
            copy_model="maker_first_btc5m",
            intent_source_wallet="E5_MAKER_FIRST",
            fixed_shares=5.0,
        )
        intents = [_e5_maker_signal_to_intent(signal).asdict() for signal in signals]
        return intents, {
            "status": "PASS",
            "source": rtds_jsonl,
            "built_at": utc_now_iso(),
            "feed": feed_diagnostics,
            "signals": signal_diagnostics,
            "intents": len(intents),
            "orders_submitted": 0,
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        }
    except Exception as exc:
        return [], {
            "status": "ERROR",
            "source": rtds_jsonl,
            "error": f"{type(exc).__name__}: {exc}",
            "intents": 0,
            "orders_submitted": 0,
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        }


async def _execute_e5_live_route_async(
    args: argparse.Namespace,
    intents: list[CopyIntent],
    *,
    token_map: dict[str, list[str]],
) -> dict[str, Any]:
    live_intents = [promote_intent_for_live(intent, operator_approval_id=args.operator_approval_id) for intent in intents]
    snapshot = LiveAdmissionSnapshot(
        decision_status="PASS",
        live_admission_status="PASS",
        live_orders_allowed=True,
        paper_only=False,
        live_tracker_truth_status="PASS",
        candidate_type="SIGNAL_ENGINE",
        candidate_policy_id=str(intents[0].policy_id),
        sizing_policy_id=str(intents[0].sizing_policy_id),
        candidate_source_wallet="e5_maker_first",
        operator_approval_id=str(args.operator_approval_id),
        runtime_live_paused=False,
        blockers=(),
    )
    executor = await _live_trade_executor(
        max_buy_price=float(getattr(args, "price_band_decision_max_price", 0.50) or 0.0),
        min_buy_price=float(getattr(args, "price_band_decision_min_price", 0.25) or 0.0),
    )
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
        enable_maker_fallback=False,
        per_window_fill_cap=int(getattr(args, "per_window_fill_cap", 1) or 1),
    )
    return await adapter.execute_async(live_intents, clob_token_ids_by_condition=token_map)


async def _cancel_due_e5_maker_orders_async(
    args: argparse.Namespace,
    *,
    force_all_on_demotion: bool = False,
) -> dict[str, Any]:
    lifecycle = LiveWalletCopyLifecycle(
        LiveExecutionLedgerConfig(state_path=args.live_ledger_state, event_log_path=args.live_ledger_event_log)
    )
    due = [
        row
        for row in lifecycle.due_maker_fallback_orders(force_e5_demotion=force_all_on_demotion)
        if str((row.get("trade_decision") or {}).get("execution_lane") or "") == "e5_maker_first_btc5m_v1"
    ]
    if not due:
        return {"status": "NO_DUE_E5_MAKER_ORDERS", "due_orders": 0, "canceled_orders": 0, "filled_orders": 0}
    executor = await _live_trade_executor(
        max_buy_price=float(getattr(args, "price_band_decision_max_price", 0.50) or 0.0),
        min_buy_price=float(getattr(args, "price_band_decision_min_price", 0.25) or 0.0),
    )
    return await lifecycle.cancel_due_maker_fallback_orders(
        executor,
        force_e5_demotion=force_all_on_demotion,
    )


def _drop_e5_already_routed_windows(
    intents: list[CopyIntent],
    *,
    live_ledger_state: str,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    ledger = load_json(live_ledger_state, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    routed_windows = {
        str(row.get("market_slug") or "")
        for row in ledger.get("orders") or []
        if isinstance(row, dict)
        and str((row.get("trade_decision") or {}).get("execution_lane") or "") == "e5_maker_first_btc5m_v1"
        and str(row.get("final_status") or "").upper() in {"SUBMITTED", "FILLED"}
        and str(row.get("market_slug") or "")
    }
    kept = [intent for intent in intents if intent.market_slug not in routed_windows]
    return kept, {
        "rule": "at_most_one_accepted_e5_gtc_per_btc5m_window",
        "routed_windows": sorted(routed_windows),
        "input_intents": len(intents),
        "new_window_intents": len(kept),
        "skipped_market_slugs": sorted({intent.market_slug for intent in intents if intent.market_slug in routed_windows}),
    }


def _run_e5_live_actuator(args: argparse.Namespace, *, generated_at: str) -> dict[str, Any]:
    state_path = str(
        getattr(args, "e5_live_actuator_state", "data/research/e5_maker_first_live_actuator_latest.json")
    )
    base = {
        "schema_version": 1,
        "kind": "e5_maker_first_live_actuator",
        "flow_stage": "LIVE/PROMOTE/ROTATE",
        "generated_at": generated_at,
        "lane": "e5_maker_first_btc5m_v1",
        "source_tag": "E5_MAKER_FIRST",
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "order_type": "GTC",
        "post_only_strict": True,
        "direct_fallback_allowed": False,
        "live_mutation": True,
    }
    if not bool(getattr(args, "e5_live_actuator", True)):
        previous = load_json(state_path, default={}, cache_readonly=True)
        previous = previous if isinstance(previous, dict) else {}
        if str(previous.get("status") or "") == "DISABLED" and isinstance(
            previous.get("demotion_force_cancel"), dict
        ):
            return {
                **previous,
                "generated_at": generated_at,
                "disabled_hot_path_noop": True,
                "orders_submitted": 0,
                "orders_accepted": 0,
                "orders_filled": 0,
            }
        maker_cancel = asyncio.run(
            _cancel_due_e5_maker_orders_async(args, force_all_on_demotion=True)
        )
        payload = {
            **base,
            "status": "DISABLED",
            "demotion_force_cancel": maker_cancel,
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
        atomic_write_json(state_path, payload)
        return payload
    if not (
        bool(getattr(args, "execute_live", False))
        and bool(getattr(args, "explicit_live_operator_go", False))
        and bool(getattr(args, "live_orders_allowed", False))
        and str(getattr(args, "operator_approval_id", "") or "")
    ):
        payload = {**base, "status": "LIVE_PERMISSION_BLOCKED", "orders_submitted": 0}
        atomic_write_json(state_path, payload)
        return payload
    maker_cancel = asyncio.run(_cancel_due_e5_maker_orders_async(args))
    feed_path = str(
        getattr(args, "e5_live_intents_state", "data/research/maker_first_btc5m_live_intents_latest.json")
    )
    feed = load_json(feed_path, default={})
    feed = feed if isinstance(feed, dict) else {}
    in_process_intents, in_process_builder = _e5_rtds_live_intents(args)
    if in_process_intents:
        feed = {**feed, "current_intents": in_process_intents}
    intents, selection = _e5_live_route_candidates(args, feed)
    if not intents:
        payload = {
            **base,
            "status": "ARMED_NO_QUALIFYING_INTENT" if selection.get("gate_pass") else "GATE_BLOCKED",
            "feed_path": feed_path,
            "feed_updated_at": feed.get("updated_at"),
            "maker_cancel": maker_cancel,
            "in_process_builder": in_process_builder,
            "selection": selection,
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
        atomic_write_json(state_path, payload)
        return payload
    intents, window_dedupe = _drop_e5_already_routed_windows(
        intents,
        live_ledger_state=str(args.live_ledger_state),
    )
    if not intents:
        payload = {
            **base,
            "status": "NO_NEW_WINDOWS",
            "feed_path": feed_path,
            "selection": selection,
            "in_process_builder": in_process_builder,
            "maker_cancel": maker_cancel,
            "window_dedupe": window_dedupe,
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
        atomic_write_json(state_path, payload)
        return payload
    deduped, dedupe = _drop_already_live_submitted_intents(
        intents,
        live_ledger_state=str(args.live_ledger_state),
    )
    if not deduped:
        payload = {
            **base,
            "status": "NO_NEW_INTENTS",
            "feed_path": feed_path,
            "selection": selection,
            "in_process_builder": in_process_builder,
            "maker_cancel": maker_cancel,
            "dedupe": dedupe,
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
        atomic_write_json(state_path, payload)
        return payload
    fallback_token_map = _intent_token_maps(deduped, allow_partial=True)
    capsules, token_map, parity_blockers = _parity_capsules(
        deduped,
        operator_approval_id=str(args.operator_approval_id),
        gamma_timeout_s=5.0,
        fallback_token_map=fallback_token_map,
    )
    parity_ok = len(capsules) == len(deduped) and not parity_blockers and all(
        str(row.get("status") or "") == "PASS" for row in capsules
    )
    if not parity_ok:
        payload = {
            **base,
            "status": "PARITY_BLOCKED",
            "feed_path": feed_path,
            "selection": selection,
            "in_process_builder": in_process_builder,
            "maker_cancel": maker_cancel,
            "parity_blockers": parity_blockers,
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
        atomic_write_json(state_path, payload)
        return payload
    try:
        execution = asyncio.run(_execute_e5_live_route_async(args, deduped, token_map=token_map))
    except Exception as exc:
        payload = {
            **base,
            "status": "EXECUTION_ERROR",
            "feed_path": feed_path,
            "selection": selection,
            "in_process_builder": in_process_builder,
            "maker_cancel": maker_cancel,
            "error": f"{type(exc).__name__}: {exc}",
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
        atomic_write_json(state_path, payload)
        return payload
    results = [row for row in execution.get("results") or [] if isinstance(row, dict)]
    accepted = [
        row
        for row in results
        if str(row.get("status") or "").lower() in {"submitted", "filled"}
        or str(row.get("post_status") or "").lower() in {"live", "matched", "filled"}
    ]
    filled = [
        row
        for row in results
        if str(row.get("status") or "").lower() == "filled"
        or str(row.get("post_status") or "").lower() in {"matched", "filled"}
        or float(row.get("filled_size_usd") or row.get("response_filled_size_usd") or 0.0) > 0
    ]
    payload = {
        **base,
        "status": "LIVE_SUBMITTED" if accepted else "LIVE_ATTEMPT_REJECTED",
        "feed_path": feed_path,
        "feed_updated_at": feed.get("updated_at"),
        "in_process_builder": in_process_builder,
        "maker_cancel": maker_cancel,
        "selection": selection,
        "dedupe": dedupe,
        "window_dedupe": window_dedupe,
        "parity_capsules": [
            {
                key: capsule.get(key)
                for key in ("status", "paper_intent_id", "live_intent_id", "parity_digest", "blockers")
            }
            for capsule in capsules
        ],
        "intent_ids": [intent.intent_id for intent in deduped],
        "book_hashes": [
            str(((intent.metadata.get("e5_maker_first_btc5m_v1") or {}).get("top_of_book") or {}).get("book_hash") or "")
            for intent in deduped
        ],
        "orders_submitted": len(results),
        "orders_accepted": len(accepted),
        "orders_filled": len(filled),
        "results": results,
    }
    atomic_write_json(state_path, payload)
    return payload


def _cross_exchange_identity_payload(intent: CopyIntent) -> dict[str, Any]:
    payload = intent.asdict()
    metadata = dict(payload.get("metadata") or {})
    metadata.pop("operator_approval_id", None)
    payload["metadata"] = metadata
    payload.pop("mode", None)
    payload.pop("live_orders_allowed", None)
    return payload


def _cross_exchange_payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _cross_exchange_resolution_patch(
    order: dict[str, Any],
    resolutions: dict[str, Any],
    *,
    generated_at: str,
) -> dict[str, Any] | None:
    normalized = dict(order)
    cost = _float_or_default(
        order.get("filled_size_usd")
        or order.get("response_filled_size_usd")
        or (order.get("expected_vs_realized_fee") or {}).get("response_cost_usd"),
        0.0,
    )
    shares = _float_or_default(
        order.get("filled_shares")
        or order.get("response_fill_size_shares")
        or (order.get("expected_vs_realized_fee") or {}).get("response_fill_size_shares"),
        0.0,
    )
    normalized["filled_size_usd"] = cost
    normalized["filled_shares"] = shares
    scored = score_order(normalized, resolutions)
    if not bool(scored.get("resolved")):
        return None
    resolution = scored.get("resolution") if isinstance(scored.get("resolution"), dict) else {}
    pnl = round(_float_or_default(scored.get("pnl_usd"), 0.0), 6)
    return {
        "filled_size_usd": round(cost, 6),
        "filled_shares": round(shares, 6),
        "pnl_usd": pnl,
        "resolved_post_fee_pnl_usd": pnl,
        "resolution_status": "RESOLVED",
        "resolved_at": generated_at,
        "resolution": {
            **resolution,
            "winner": scored.get("winner"),
            "win": scored.get("win"),
            "payout_usd": scored.get("payout_usd"),
            "pnl_usd": pnl,
        },
        "canonical_method_resolution": {
            "schema_version": 1,
            "flow_stage": "LIVE/LEARN/ROTATE",
            "status": "RESOLVED",
            "resolver_source": resolution.get("source"),
            "join_keys": {
                "market_slug": order.get("market_slug"),
                "condition_id": order.get("condition_id"),
            },
            "outcome": order.get("outcome"),
            "winner": scored.get("winner"),
            "win": scored.get("win"),
            "filled_cost_usd": round(cost, 6),
            "filled_shares": round(shares, 6),
            "fee_basis": "actual_response_cost_embeds_polymarket_buy_fee",
            "post_fee_pnl_usd": pnl,
            "resolved_at": generated_at,
        },
    }


def _resolve_cross_exchange_campaign_orders(
    live_ledger_state: str,
    resolutions_path: str,
    *,
    generated_at: str,
    source_tag: str = _CROSS_EXCHANGE_SOURCE,
    lane: str = _CROSS_EXCHANGE_LANE,
) -> dict[str, Any]:
    ledger = load_json(live_ledger_state, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    resolutions = load_resolutions(resolutions_path)
    changed = 0
    resolved = 0
    unresolved = 0
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
        wallet_copy = (
            decision.get("wallet_copy")
            if isinstance(decision.get("wallet_copy"), dict)
            else {}
        )
        source_wallet = str(row.get("source_wallet") or wallet_copy.get("source_wallet") or "")
        strategy_family = str(
            decision.get("strategy_family") or wallet_copy.get("strategy_family") or ""
        )
        if (
            source_wallet.lower() != source_tag.lower()
            and strategy_family != lane
        ):
            continue
        status = str(row.get("final_status") or row.get("status") or "").upper()
        if status not in {"FILLED", "MATCHED"}:
            continue
        patch = _cross_exchange_resolution_patch(
            row,
            resolutions,
            generated_at=generated_at,
        )
        if patch is None:
            unresolved += 1
            continue
        resolved += 1
        existing = row.get("canonical_method_resolution")
        stable_patch = dict(patch)
        if isinstance(existing, dict) and existing.get("status") == "RESOLVED":
            stable_patch["resolved_at"] = row.get("resolved_at") or existing.get("resolved_at")
            stable_patch["canonical_method_resolution"]["resolved_at"] = (
                existing.get("resolved_at") or generated_at
            )
        if any(row.get(key) != value for key, value in stable_patch.items()):
            row.update(stable_patch)
            changed += 1
    if changed:
        atomic_write_json(live_ledger_state, ledger)
    return {
        "status": "UPDATED" if changed else "NO_CHANGE",
        "resolved_fills": resolved,
        "unresolved_fills": unresolved,
        "changed_orders": changed,
        "resolution_source": "canonical_btc5m_resolution_index",
    }


def _cross_exchange_campaign_truth(
    live_ledger_state: str,
    *,
    source_tag: str = _CROSS_EXCHANGE_SOURCE,
    lane: str = _CROSS_EXCHANGE_LANE,
) -> dict[str, Any]:
    ledger = load_json(live_ledger_state, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    resolved: list[dict[str, Any]] = []
    method_orders: list[dict[str, Any]] = []
    seen_order_ids: set[str] = set()
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
        wallet_copy = decision.get("wallet_copy") if isinstance(decision.get("wallet_copy"), dict) else {}
        source_wallet = str(row.get("source_wallet") or wallet_copy.get("source_wallet") or "")
        strategy_family = str(decision.get("strategy_family") or wallet_copy.get("strategy_family") or "")
        if (
            source_wallet.lower() != source_tag.lower()
            and strategy_family != lane
        ):
            continue
        order_id = str(row.get("order_id") or "")
        identity = order_id or str(row.get("intent_id") or "")
        if identity and identity in seen_order_ids:
            continue
        if identity:
            seen_order_ids.add(identity)
        method_orders.append(row)
        pnl_raw = row.get("pnl_usd")
        if pnl_raw is None:
            pnl_raw = row.get("resolved_post_fee_pnl_usd")
        if pnl_raw is None and isinstance(row.get("resolution"), dict):
            pnl_raw = row["resolution"].get("pnl_usd")
        attribution = (
            row.get("alternate_transport_attribution")
            if isinstance(row.get("alternate_transport_attribution"), dict)
            else {}
        )
        if pnl_raw is None and str(attribution.get("resolution_status") or "").upper() == "RESOLVED":
            pnl_raw = attribution.get("resolved_post_fee_pnl_usd")
        if pnl_raw is None:
            continue
        try:
            pnl = float(pnl_raw)
        except (TypeError, ValueError):
            continue
        resolved.append(
            {
                "order_id": order_id,
                "accepted_at": row.get("accepted_at") or row.get("submitted_at"),
                "pnl_usd": round(pnl, 6),
            }
        )
    rolling = resolved[-20:]
    accepted_statuses = {"SUBMITTED", "FILLED", "LIVE", "MATCHED"}
    accepted = [
        row
        for row in method_orders
        if str(row.get("final_status") or row.get("status") or "").upper() in accepted_statuses
    ]
    filled = [
        row
        for row in accepted
        if str(row.get("final_status") or row.get("status") or "").upper() in {"FILLED", "MATCHED"}
        or float(row.get("filled_size_usd") or row.get("response_filled_size_usd") or 0.0) > 0.0
    ]
    last_accepted = accepted[-1] if accepted else {}
    method_pnl = {
        "flow_stage": "LIVE/ROTATE",
        "resolved_fills": len(resolved),
        "unresolved_fills": max(0, len(filled) - len(resolved)),
        "rolling_window_fills": 20,
        "rolling_resolved_fills": len(rolling),
        "rolling_realized_pnl_usd": round(sum(float(row["pnl_usd"]) for row in rolling), 6),
        "rows": rolling,
    }
    return {
        "campaign_count_source": "method_tagged_live_ledger",
        "orders_submitted": len(method_orders),
        "orders_accepted": len(accepted),
        "orders_filled": len(filled),
        "last_order_id": str(last_accepted.get("order_id") or "") or None,
        "last_order_status": str(
            last_accepted.get("final_status") or last_accepted.get("status") or ""
        )
        or None,
        "last_accepted_at": (
            last_accepted.get("accepted_at") or last_accepted.get("submitted_at")
        ),
        "method_pnl": method_pnl,
    }


def _cross_exchange_method_pnl(
    live_ledger_state: str,
    *,
    source_tag: str = _CROSS_EXCHANGE_SOURCE,
    lane: str = _CROSS_EXCHANGE_LANE,
) -> dict[str, Any]:
    return dict(
        _cross_exchange_campaign_truth(
            live_ledger_state,
            source_tag=source_tag,
            lane=lane,
        )["method_pnl"]
    )


def _promoted_paired_bundle_intents(
    terminal: dict[str, Any],
    *,
    cell_id: str,
    source_tag: str,
    lane: str,
    activation_id: str,
    record_checksum: str,
    evidence_snapshot_checksum: str,
    execution_mode: str = "paired_passive",
) -> list[CopyIntent]:
    rows = [row for row in terminal.get("paired_intents") or [] if isinstance(row, dict)]
    pair_id = str(terminal.get("terminal_id") or "")
    if len(rows) != 2 or {str(row.get("outcome") or "") for row in rows} != {"Up", "Down"} or not pair_id:
        raise ValueError("paired selector terminal requires exactly one immutable Up/Down pair")
    intents: list[CopyIntent] = []
    for leg_index, row in enumerate(sorted(rows, key=lambda value: str(value.get("outcome") or ""))):
        payload = dict(row)
        payload.update({
            "intent_id": stable_id("ci", {"cell_id": cell_id, "pair_id": pair_id, "outcome": row.get("outcome")}),
            "source_wallet": source_tag,
            "wallet_name": lane,
            "strategy_family": lane,
        })
        metadata = dict(payload.get("metadata") or {})
        metadata["promoted_cell"] = {
            "cell_id": cell_id,
            "record_checksum": record_checksum,
            "evidence_snapshot_checksum": evidence_snapshot_checksum,
            "activation_id": activation_id,
            "execution_mode": execution_mode,
        }
        metadata["paired_bundle"] = {
            "pair_id": pair_id,
            "leg_index": leg_index,
            "leg_count": 2,
            "cancel_survivor_on_asymmetric_rejection": True,
        }
        if execution_mode == "paired_passive":
            metadata["precision_requires_passive_source"] = {
                "status": "promoted_cell_post_only",
                "execution_path": "direct_post_only_gtc_at_source",
                "passive_price": float(row.get("limit_price") or 0.0),
                "original_source_price": float(row.get("limit_price") or 0.0),
                "buffered_limit_price": float(row.get("limit_price") or 0.0),
                "max_copy_price": _CROSS_EXCHANGE_MAX_PRICE,
                "best_ask": float(((terminal.get("signal") or {}).get("paired_legs") or [{}, {}])[leg_index].get("best_ask") or 0.0),
            }
        elif execution_mode == "paired_split_sell":
            metadata["complete_set_split_sell"] = {
                "ctf_split_confirmed_before_submit": True,
                "split_collateral_usd": 1.0,
                "split_shares_per_leg": 1.0,
                "condition_id": str(row.get("condition_id") or ""),
                "carry_unsold_leg_to_canonical_resolution": True,
            }
        payload["metadata"] = metadata
        intents.append(CopyIntent.from_dict(payload))
    if abs(sum(float(intent.copy_size_usd) for intent in intents) - 1.0) > 1e-9:
        raise ValueError("paired selector terminal total notional must equal $1")
    return intents


def _execute_complete_set_split(condition_id: str) -> dict[str, Any]:
    """Execute and confirm the exact $1 binary CTF split before either SELL."""
    from scripts.run_own_position_redeemer import (  # local import keeps inactive path cold
        _builder_config,
        _env,
        _load_relayer_symbols,
    )

    RelayClient, RelayerTxType, Transaction = _load_relayer_symbols()
    private_key = _env("PRIVATE_KEY")
    if not private_key:
        raise RuntimeError("PRIVATE_KEY missing for complete-set split")
    chain_id = int(_env("CHAIN_ID", "137"))
    relayer_url = _env("RELAYER_URL", "https://relayer-v2.polymarket.com/")
    rpc_url = _env("POLYGON_RPC_URL") or None
    client_kwargs: dict[str, Any] = {"builder_config": _builder_config()}
    params = inspect.signature(RelayClient).parameters
    if "relay_tx_type" in params and RelayerTxType is not None:
        client_kwargs["relay_tx_type"] = RelayerTxType.PROXY
    if "rpc_url" in params:
        client_kwargs["rpc_url"] = rpc_url
    client = RelayClient(relayer_url, chain_id, private_key, **client_kwargs)
    response = client.execute(
        [Transaction(to=POLYMARKET_CTF, data=split_position_calldata(condition_id), value="0")],
        metadata="wallet-copy complete-set split $1",
    )
    waited = response.wait()
    state = str((waited or {}).get("state") if isinstance(waited, dict) else "")
    if state.upper() not in {"STATE_CONFIRMED", "CONFIRMED", "SUCCESS", "MINED"}:
        raise RuntimeError(f"complete-set split did not confirm: {state or 'UNKNOWN'}")
    return {
        "status": "CONFIRMED",
        "state": state,
        "transaction_id": getattr(response, "transaction_id", None),
        "transaction_hash": getattr(response, "transaction_hash", None) or getattr(response, "hash", None),
        "condition_id": condition_id,
        "split_collateral_usd": 1.0,
    }


async def _execute_cross_exchange_live_route_async(
    args: argparse.Namespace,
    intents: list[CopyIntent],
    *,
    token_map: dict[str, list[str]],
    source_tag: str = _CROSS_EXCHANGE_SOURCE,
    lane: str = _CROSS_EXCHANGE_LANE,
    execution_mode: str = "taker",
    record_checksum: str = "",
    evidence_snapshot_checksum: str = "",
) -> dict[str, Any]:
    if not intents or any(
        intent.source_wallet.lower() != source_tag.lower()
        or intent.strategy_family != lane
        for intent in intents
    ):
        raise ValueError("cross-exchange admission source/lane identity mismatch")
    promoted = source_tag.upper().startswith("BTC5M_PROMOTED_CELL:")
    if promoted and (not record_checksum or not evidence_snapshot_checksum):
        raise ValueError("promoted cell execution requires immutable record/evidence checksums")
    if execution_mode not in {"taker", "passive", "paired_passive", "paired_split_sell"}:
        raise ValueError("cross-exchange execution mode must be taker, passive, paired_passive, or paired_split_sell")
    for intent in intents:
        cell = intent.metadata.get("promoted_cell") if isinstance(intent.metadata, dict) else {}
        if promoted and (
            not isinstance(cell, dict)
            or cell.get("record_checksum") != record_checksum
            or cell.get("evidence_snapshot_checksum") != evidence_snapshot_checksum
            or cell.get("execution_mode") != execution_mode
        ):
            raise ValueError("promoted cell intent checksum/execution identity mismatch")
    paired_bundle = execution_mode in {"paired_passive", "paired_split_sell"}
    if paired_bundle:
        pair_ids = {
            str((intent.metadata.get("paired_bundle") or {}).get("pair_id") or "")
            for intent in intents
        }
        if (
            len(intents) != 2
            or {intent.outcome for intent in intents} != {"Up", "Down"}
            or len(pair_ids) != 1
            or "" in pair_ids
            or abs(sum(float(intent.copy_size_usd) for intent in intents) - 1.0) > 1e-9
        ):
            raise ValueError("paired promoted cell requires one $1 two-leg bundle")
    live_intents = [
        promote_intent_for_live(intent, operator_approval_id=args.operator_approval_id)
        for intent in intents
    ]
    snapshot = LiveAdmissionSnapshot(
        decision_status="PASS",
        live_admission_status="PASS",
        live_orders_allowed=True,
        paper_only=False,
        live_tracker_truth_status="PASS",
        candidate_type="SIGNAL_ENGINE",
        candidate_policy_id=str(intents[0].policy_id),
        sizing_policy_id=str(intents[0].sizing_policy_id),
        candidate_source_wallet=source_tag.lower(),
        operator_approval_id=str(args.operator_approval_id),
        runtime_live_paused=False,
        blockers=(),
    )
    executor = await _live_trade_executor(
        max_buy_price=_CROSS_EXCHANGE_MAX_PRICE,
        min_buy_price=_CROSS_EXCHANGE_MIN_PRICE,
    )
    adapter = CopyExecutionAdapter(
        gate=ExecutionGate(
            mode="live",
            explicit_operator_go=bool(args.explicit_live_operator_go),
            live_orders_allowed=bool(args.live_orders_allowed),
            dry_run=False,
            admission_snapshot=snapshot,
        ),
        live_lifecycle=LiveWalletCopyLifecycle(
            LiveExecutionLedgerConfig(
                state_path=args.live_ledger_state,
                event_log_path=args.live_ledger_event_log,
            )
        ),
        trade_executor=executor,
        enable_maker_fallback=False,
        per_window_fill_cap=(2 if paired_bundle else int(getattr(args, "per_window_fill_cap", 1) or 1)),
    )
    split_result = None
    if execution_mode == "paired_split_sell":
        condition_ids = {intent.condition_id for intent in intents}
        if len(condition_ids) != 1 or any(intent.action != "SELL" or abs(float(intent.shares) - 1.0) > 1e-9 for intent in intents):
            raise ValueError("paired split-sell requires one condition and exact one-share SELL legs")
        split_result = await asyncio.to_thread(_execute_complete_set_split, next(iter(condition_ids)))
    result = await adapter.execute_async(live_intents, clob_token_ids_by_condition=token_map)
    if split_result:
        result["complete_set_split"] = split_result
    if paired_bundle:
        rows = [row for row in result.get("results") or [] if isinstance(row, dict)]
        accepted = [row for row in rows if str(row.get("status") or "").lower() in {"submitted", "filled"} or str(row.get("post_status") or "").lower() in {"live", "matched", "filled"}]
        result["paired_bundle_execution"] = {
            "pair_id": next(iter(pair_ids)),
            "accepted_legs": len(accepted),
            "asymmetric_rejection": len(accepted) == 1,
            "survivor_cancelled": False,
        }
        if len(accepted) == 1 and execution_mode == "paired_passive":
            order_id = str(accepted[0].get("order_id") or "")
            cancel = getattr(executor, "cancel_order", None)
            cancel_ok = bool(order_id and cancel and await cancel(order_id))
            result["paired_bundle_execution"].update({"survivor_order_id": order_id, "survivor_cancelled": cancel_ok})
            if not cancel_ok:
                raise RuntimeError("paired bundle asymmetric rejection survivor cancellation failed")
        elif len(accepted) == 1:
            result["paired_bundle_execution"]["orphan_inventory_action"] = "CARRY_UNSOLD_LEG_TO_CANONICAL_RESOLUTION"
    return result


def _cross_exchange_deadman_gate(deadman: dict[str, Any]) -> dict[str, Any]:
    policy_choke = deadman.get("policy_choke") if isinstance(deadman.get("policy_choke"), dict) else {}
    rung_a = (
        policy_choke.get("rung_a_seat_read")
        if isinstance(policy_choke.get("rung_a_seat_read"), dict)
        else {}
    )
    fire_drill = (
        deadman.get("policy_choke_fire_drill")
        if isinstance(deadman.get("policy_choke_fire_drill"), dict)
        else {}
    )
    rung_c = (
        fire_drill.get("rung_c_full_pool_liveness_drill")
        if isinstance(fire_drill.get("rung_c_full_pool_liveness_drill"), dict)
        else {}
    )
    selected_wallet = str(policy_choke.get("selected_wallet") or "")
    selected_fresh = int(policy_choke.get("selected_fresh_source_rows") or 0)
    idle_s = float(
        (deadman.get("raw_accepted_order_deadman") or {}).get("accepted_order_idle_s")
        or deadman.get("accepted_order_idle_s")
        or deadman.get("idle_s")
        or 0.0
    )
    checks = {
        "incident_order_flow_dead": str(deadman.get("status") or "") == "INCIDENT_ORDER_FLOW_DEAD",
        "can_trade": deadman.get("can_trade") is True,
        "accepted_order_idle_gte_3600s": idle_s >= _CROSS_EXCHANGE_MIN_DEADMAN_IDLE_S,
        "source_silent_32de": (
            selected_wallet.lower() == _CROSS_EXCHANGE_SILENT_WALLET
            and selected_fresh == 0
        ),
        "rung_a_dry": not str(rung_a.get("target_wallet") or ""),
        "rung_c_dry": int(rung_c.get("fresh_own_source_positive_rows") or 0) == 0,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "reason": "SOURCE_SILENT_32DE" if checks["source_silent_32de"] else "DEADMAN_PRECONDITION_FAILED",
        "checks": checks,
        "accepted_order_idle_s": round(idle_s, 6),
        "selected_wallet": selected_wallet,
        "selected_fresh_source_rows": selected_fresh,
        "rung_c_candidate_count": int(rung_c.get("rung_c_candidate_count") or 0),
        "rung_c_fresh_own_source_positive_rows": int(rung_c.get("fresh_own_source_positive_rows") or 0),
    }


def _drop_cross_exchange_already_routed_windows(
    intents: list[CopyIntent],
    *,
    live_ledger_state: str,
    source_tag: str = _CROSS_EXCHANGE_SOURCE,
    lane: str = _CROSS_EXCHANGE_LANE,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    ledger = load_json(live_ledger_state, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    routed_windows: set[str] = set()
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
        wallet_copy = decision.get("wallet_copy") if isinstance(decision.get("wallet_copy"), dict) else {}
        source_wallet = str(row.get("source_wallet") or wallet_copy.get("source_wallet") or "")
        strategy_family = str(decision.get("strategy_family") or wallet_copy.get("strategy_family") or "")
        accepted = str(row.get("final_status") or row.get("status") or "").upper() in {"SUBMITTED", "FILLED"}
        market_slug = str(row.get("market_slug") or wallet_copy.get("market_slug") or "")
        if (
            accepted
            and market_slug
            and (source_wallet == source_tag or strategy_family == lane)
        ):
            routed_windows.add(market_slug)
    kept = [intent for intent in intents if intent.market_slug not in routed_windows]
    return kept, {
        "rule": "at_most_one_accepted_cross_exchange_intent_per_btc5m_window",
        "routed_windows": sorted(routed_windows),
        "input_intents": len(intents),
        "new_window_intents": len(kept),
        "skipped_market_slugs": sorted(
            {intent.market_slug for intent in intents if intent.market_slug in routed_windows}
        ),
    }


def _wide_family_activation_validation(
    family: dict[str, Any],
    *,
    generated_at: str,
) -> tuple[CopyIntent | None, dict[str, bool], str]:
    activation = (
        family.get("activation") if isinstance(family.get("activation"), dict) else {}
    )
    packet = {key: value for key, value in activation.items() if key != "activation_checksum"}
    evidence = (
        activation.get("evidence_snapshot")
        if isinstance(activation.get("evidence_snapshot"), dict)
        else {}
    )
    source_order = (
        activation.get("source_order")
        if isinstance(activation.get("source_order"), dict)
        else {}
    )
    lineage = (
        activation.get("source_lineage")
        if isinstance(activation.get("source_lineage"), dict)
        else {}
    )
    lineage_body = {key: value for key, value in lineage.items() if key != "checksum"}
    paper_payload = (
        activation.get("paper_intent")
        if isinstance(activation.get("paper_intent"), dict)
        else {}
    )
    now_dt = _parse_iso_datetime(generated_at) or dt.datetime.now(tz=dt.timezone.utc)
    now_ts = now_dt.timestamp()
    observed_ts = _float_or_default(paper_payload.get("observed_ts"), 0.0)
    market_slug = str(paper_payload.get("market_slug") or "")
    current_window = int(now_ts // 300) * 300
    metadata = (
        paper_payload.get("metadata") if isinstance(paper_payload.get("metadata"), dict) else {}
    )
    family_meta = (
        metadata.get("wide_positive_slice_family")
        if isinstance(metadata.get("wide_positive_slice_family"), dict)
        else {}
    )
    source_book = (
        metadata.get("source_book") if isinstance(metadata.get("source_book"), dict) else {}
    )
    gates = evidence.get("gates") if isinstance(evidence.get("gates"), dict) else {}
    checks = {
        "family_gate_complete": (
            family.get("admission_ready") is True
            and family.get("status") == "PROMOTION_GATE_COMPLETE"
            and bool(gates)
            and all(gates.values())
        ),
        "activation_ready": (
            activation.get("status") == "ACTIVATION_READY"
            and activation.get("live_mutation_allowed") is True
        ),
        "activation_checksum_exact": (
            bool(activation.get("activation_checksum"))
            and activation.get("activation_checksum") == _cross_exchange_payload_hash(packet)
        ),
        "family_checksum_exact": (
            bool(family.get("family_checksum"))
            and activation.get("family_checksum") == family.get("family_checksum")
            and family_meta.get("family_checksum") == family.get("family_checksum")
        ),
        "evidence_checksum_exact": (
            bool(activation.get("evidence_checksum"))
            and activation.get("evidence_checksum") == _cross_exchange_payload_hash(evidence)
            and family_meta.get("evidence_checksum") == activation.get("evidence_checksum")
        ),
        "source_order_checksum_exact": (
            bool(activation.get("source_order_checksum"))
            and activation.get("source_order_checksum")
            == _cross_exchange_payload_hash(source_order)
        ),
        "lineage_checksum_exact": (
            bool(lineage.get("checksum"))
            and lineage.get("checksum") == _cross_exchange_payload_hash(lineage_body)
            and lineage.get("order_id") == source_order.get("order_id")
            and lineage.get("run_id") == source_order.get("run_id")
            and lineage.get("cohort_id") == source_order.get("cohort_id")
            and str(lineage.get("wallet") or "").lower()
            == str(source_order.get("wallet") or "").lower()
        ),
        "paper_intent_checksum_exact": (
            bool(activation.get("paper_intent_checksum"))
            and activation.get("paper_intent_checksum")
            == _cross_exchange_payload_hash(paper_payload)
        ),
        "family_source_lane_exact": (
            str(paper_payload.get("source_wallet") or "") == _WIDE_FAMILY_SOURCE
            and str(paper_payload.get("wallet_name") or "") == _WIDE_FAMILY_LANE
            and str(paper_payload.get("strategy_family") or "") == _WIDE_FAMILY_LANE
        ),
        "current_btc5m_window": market_slug == f"btc-updown-5m-{current_window}",
        "fresh_observation_lte_5s": (
            0.0 <= now_ts - observed_ts <= _WIDE_FAMILY_MAX_SIGNAL_AGE_S
        ),
        "receipt_lag_lte_5s": (
            0.0
            <= _float_or_default(source_order.get("receipt_to_book_fetch_lag_s"), 999.0)
            <= _WIDE_FAMILY_MAX_SIGNAL_AGE_S
        ),
        "hard_price_bounds": (
            _CROSS_EXCHANGE_MIN_PRICE
            <= _float_or_default(paper_payload.get("limit_price"), 0.0)
            <= _CROSS_EXCHANGE_MAX_PRICE
        ),
        "fixed_one_dollar": (
            abs(_float_or_default(paper_payload.get("copy_size_usd"), 0.0) - 1.0) <= 1e-9
            and str(paper_payload.get("sizing_policy_id") or "") == "fixed_usd_1"
        ),
        "executable_depth": source_book.get("executable_depth_pass") is True,
        "fees_and_net_edge_measured": (
            gates.get("fees_measured") is True
            and gates.get("post_fee_positive") is True
            and _float_or_default(evidence.get("post_fee_pnl_usd"), 0.0) > 0.0
        ),
        "paper_only_input": (
            paper_payload.get("mode") == "paper"
            and paper_payload.get("live_orders_allowed") is False
        ),
    }
    if not paper_payload:
        return None, checks, "ACTIVATION_PACKET_MISSING"
    try:
        intent = CopyIntent.from_dict(paper_payload)
    except Exception:
        return None, checks, "PAPER_INTENT_RECONSTRUCTION_FAILED"
    return (
        intent if all(checks.values()) else None,
        checks,
        "PASS" if all(checks.values()) else "ACTIVATION_PROTECTION_GATE_FAILED",
    )


def _run_wide_family_live_actuator(
    args: argparse.Namespace,
    *,
    generated_at: str,
) -> dict[str, Any]:
    state_path = str(
        getattr(
            args,
            "wide_family_live_actuator_state",
            "data/research/wide_positive_slice_family_live_actuator_latest.json",
        )
    )
    family_path = str(
        getattr(
            args,
            "wide_family_state",
            "data/research/wide_positive_slice_family_state.json",
        )
    )
    family = load_json(family_path, default={})
    family = family if isinstance(family, dict) else {}
    base = {
        "schema_version": 1,
        "kind": "wide_positive_slice_family_live_actuator",
        "flow_stage": "LIVE/ROTATE/PROMOTE/SELF-DEV",
        "generated_at": generated_at,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "family_state_path": family_path,
        "family_checksum": family.get("family_checksum"),
        "lane": _WIDE_FAMILY_LANE,
        "source_tag": _WIDE_FAMILY_SOURCE,
        "fixed_order_usd": 1.0,
    }

    def persist(status: str, **extra: Any) -> dict[str, Any]:
        payload = {**base, "status": status, "orders_submitted": 0, "orders_accepted": 0,
                   "orders_filled": 0, **extra}
        atomic_write_json(state_path, payload)
        return payload

    if not bool(getattr(args, "wide_family_live_actuator", True)):
        return persist("DISABLED", terminal_reason="ACTUATOR_FLAG_DISABLED")
    if not (
        bool(getattr(args, "execute_live", False))
        and bool(getattr(args, "explicit_live_operator_go", False))
        and bool(getattr(args, "live_orders_allowed", False))
        and str(getattr(args, "operator_approval_id", "") or "")
    ):
        return persist("LIVE_PERMISSION_BLOCKED", terminal_reason="LIVE_PERMISSION_BLOCKED")
    deadman = load_json(
        str(getattr(args, "cross_exchange_deadman_state", "data/research/order_flow_deadman_state.json")),
        default={},
    )
    idle_s = _float_or_default(
        ((deadman or {}).get("raw_accepted_order_deadman") or {}).get("accepted_order_idle_s"),
        0.0,
    )
    deadman_checks = {
        "incident_order_flow_dead": (deadman or {}).get("status") == "INCIDENT_ORDER_FLOW_DEAD",
        "can_trade": (deadman or {}).get("can_trade") is True,
        "idle_gte_1800s": idle_s >= 1800.0,
    }
    if not all(deadman_checks.values()):
        return persist(
            "DEADMAN_GATE_BLOCKED",
            terminal_reason="ORDER_FLOW_DEAD_PRECONDITION_FAILED",
            deadman_checks=deadman_checks,
        )
    intent, checks, reason = _wide_family_activation_validation(
        family,
        generated_at=generated_at,
    )
    if intent is None:
        return persist(
            "ARMED_WAITING_GATE_COMPLETE_FAMILY",
            terminal_reason=reason,
            protection_checks=checks,
        )
    intents, window_dedupe = _drop_cross_exchange_already_routed_windows(
        [intent],
        live_ledger_state=str(args.live_ledger_state),
        source_tag=_WIDE_FAMILY_SOURCE,
        lane=_WIDE_FAMILY_LANE,
    )
    if not intents:
        return persist(
            "NO_NEW_WINDOWS",
            terminal_reason="WINDOW_ALREADY_ACCEPTED",
            protection_checks=checks,
            window_dedupe=window_dedupe,
        )
    intents, dedupe = _drop_already_live_submitted_intents(
        intents,
        live_ledger_state=str(args.live_ledger_state),
    )
    if not intents:
        return persist(
            "NO_NEW_INTENTS",
            terminal_reason="INTENT_ALREADY_ACCEPTED",
            protection_checks=checks,
            dedupe=dedupe,
            window_dedupe=window_dedupe,
        )
    fallback_token_map = _intent_token_maps(intents, allow_partial=True)
    capsules, token_map, parity_blockers = _parity_capsules(
        intents,
        operator_approval_id=str(args.operator_approval_id),
        gamma_timeout_s=5.0,
        fallback_token_map=fallback_token_map,
    )
    parity_ok = len(capsules) == len(intents) and not parity_blockers and all(
        str(row.get("status") or "") == "PASS"
        and not list(row.get("mismatched_fields") or [])
        and row.get("decision_wallet_copy_matches_live_intent") is not False
        for row in capsules
    )
    if not parity_ok:
        return persist(
            "PARITY_BLOCKED",
            terminal_reason="COPYINTENT_PARITY_CAPSULE_FAILED",
            protection_checks=checks,
            parity_blockers=parity_blockers,
            parity_capsules=capsules,
        )
    execution = asyncio.run(
        _execute_cross_exchange_live_route_async(
            args,
            intents,
            token_map=token_map,
            source_tag=_WIDE_FAMILY_SOURCE,
            lane=_WIDE_FAMILY_LANE,
            execution_mode="taker",
        )
    )
    results = [row for row in execution.get("results") or [] if isinstance(row, dict)]
    accepted = [
        row
        for row in results
        if str(row.get("status") or "").lower() in {"submitted", "filled"}
        or str(row.get("post_status") or "").lower() in {"live", "matched", "filled"}
    ]
    filled = [
        row
        for row in results
        if str(row.get("status") or "").lower() == "filled"
        or str(row.get("post_status") or "").lower() in {"matched", "filled"}
    ]
    first = accepted[0] if accepted else {}
    return persist(
        "LIVE_SUBMITTED" if accepted else "LIVE_ATTEMPT_REJECTED",
        terminal_reason="ACCEPTED_ORDER_RESTORED_FLOW" if accepted else "EXCHANGE_ATTEMPT_NOT_ACCEPTED",
        protection_checks=checks,
        parity_digest=str(capsules[0].get("parity_digest") or "") if capsules else "",
        orders_submitted=len(results),
        orders_accepted=len(accepted),
        orders_filled=len(filled),
        last_order_id=first.get("order_id"),
        last_order_status=first.get("post_status") or first.get("status"),
        last_accepted_at=first.get("accepted_at"),
    )


def _run_cross_exchange_live_actuator(args: argparse.Namespace, *, generated_at: str) -> dict[str, Any]:
    state_path = str(
        getattr(
            args,
            "cross_exchange_live_actuator_state",
            "data/research/btc5m_cross_exchange_probability_edge_live_actuator_latest.json",
        )
    )
    previous = load_json(state_path, default={})
    previous = previous if isinstance(previous, dict) else {}
    if not bool(getattr(args, "cross_exchange_live_actuator", False)):
        return {
            "schema_version": 1,
            "kind": "btc5m_cross_exchange_probability_edge_live_actuator",
            "flow_stage": "LIVE/ROTATE/PROMOTE/SELF-DEV",
            "generated_at": generated_at,
            "status": "DISABLED",
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
            "terminal_reason": "ACTUATOR_FLAG_DISABLED",
            "disabled_hot_path_noop": True,
            "orders_submitted": 0,
            "orders_accepted": 0,
            "orders_filled": 0,
        }
    selector_path = str(
        getattr(
            args,
            "cross_exchange_promoted_cell_state",
            "data/research/btc5m_cross_exchange_promoted_cell_latest.json",
        )
    )
    arbiter_sources_raw = str(getattr(args, "cross_exchange_promoted_cell_sources", "") or "")
    arbiter_sources = [value.strip() for value in arbiter_sources_raw.split(",") if value.strip()]
    if arbiter_sources:
        _build_promoted_cell_arbiter(source_paths=arbiter_sources, output_path=selector_path)
    selector = load_json(selector_path, default={})
    selector = selector if isinstance(selector, dict) else {}
    selected = selector.get("selected") if isinstance(selector.get("selected"), dict) else {}
    promoted = bool(
        selector.get("status") == "PROMOTED_CELL_READY"
        and selected.get("gate_pass") is True
        and selected.get("activation_id")
        and selected.get("record_checksum")
    )
    cell_id = str(selected.get("cell_id") or "")
    activation_id = str(selected.get("activation_id") or _CROSS_EXCHANGE_ACTIVATION_ID)
    lane = f"btc5m_cross_exchange_promoted_cell:{cell_id}" if promoted else _CROSS_EXCHANGE_LANE
    source_tag = f"BTC5M_PROMOTED_CELL:{cell_id}".upper() if promoted else _CROSS_EXCHANGE_SOURCE
    model_checksum = str(selected.get("model_checksum") or _CROSS_EXCHANGE_MODEL_CHECKSUM)
    execution_mode = str(selected.get("execution_mode") or "taker") if promoted else "taker"
    promoted_state_path = str(selected.get("state_path") or "")
    base = {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_probability_edge_live_actuator",
        "flow_stage": "LIVE/ROTATE/PROMOTE/SELF-DEV",
        "generated_at": generated_at,
        "activation_id": activation_id,
        "lane": lane,
        "source_tag": source_tag,
        "execution_mode": execution_mode,
        "promoted_cell": selected if promoted else None,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "permanent_promotion_resolved_required": int(selector.get("permanent_promotion_resolved_required") or 200),
        "emergency_probe_does_not_waive_permanent_gate": True,
        "hard_entry_bounds": [_CROSS_EXCHANGE_MIN_PRICE, _CROSS_EXCHANGE_MAX_PRICE],
        "fixed_order_usd": _CROSS_EXCHANGE_ORDER_USD,
    }
    resolution_join = _resolve_cross_exchange_campaign_orders(
        str(args.live_ledger_state),
        str(
            getattr(
                args,
                "resolutions",
                "data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
            )
        ),
        generated_at=generated_at,
        source_tag=source_tag,
        lane=lane,
    )
    base["resolution_join"] = resolution_join

    def persist(status: str, **extra: Any) -> dict[str, Any]:
        campaign = _cross_exchange_campaign_truth(
            str(args.live_ledger_state),
            source_tag=source_tag,
            lane=lane,
        )
        payload = {**base, "status": status, **campaign, **extra}
        for key in (
            "activation_started_at",
            "activation_expires_at",
            "last_order_id",
            "last_order_status",
            "last_accepted_at",
            "paper_intent_hash",
            "live_intent_hash",
            "parity_digest",
            "source_event_hash",
        ):
            if key not in payload and previous.get(key) is not None:
                payload[key] = previous.get(key)
        atomic_write_json(state_path, payload)
        return payload

    if previous.get("activation_id") == activation_id and str(
        previous.get("status") or ""
    ) in {"EXPIRED_ZERO_CONVERSION", "DEMOTED_NEGATIVE_ROLLING_PNL"}:
        return persist(
            str(previous["status"]),
            terminal_reason=str(previous.get("terminal_reason") or previous["status"]),
        )
    if not (
        bool(getattr(args, "execute_live", False))
        and bool(getattr(args, "explicit_live_operator_go", False))
        and bool(getattr(args, "live_orders_allowed", False))
        and str(getattr(args, "operator_approval_id", "") or "")
    ):
        return persist("LIVE_PERMISSION_BLOCKED", terminal_reason="LIVE_PERMISSION_BLOCKED")

    campaign = _cross_exchange_campaign_truth(
        str(args.live_ledger_state),
        source_tag=source_tag,
        lane=lane,
    )
    method_pnl = campaign["method_pnl"]
    if (
        int(method_pnl.get("rolling_resolved_fills") or 0) > 0
        and float(method_pnl.get("rolling_realized_pnl_usd") or 0.0) < 0.0
    ):
        return persist(
            "DEMOTED_NEGATIVE_ROLLING_PNL",
            terminal_reason="NEGATIVE_ROLLING_METHOD_PNL",
        )

    deadman_path = str(
        getattr(args, "cross_exchange_deadman_state", "data/research/order_flow_deadman_state.json")
    )
    deadman = load_json(deadman_path, default={})
    deadman = deadman if isinstance(deadman, dict) else {}
    deadman_gate = _cross_exchange_deadman_gate(deadman)
    if deadman_gate["status"] != "PASS":
        return persist(
            "DEADMAN_GATE_BLOCKED",
            terminal_reason=str(deadman_gate.get("reason") or "DEADMAN_GATE_BLOCKED"),
            deadman_gate=deadman_gate,
        )

    now_dt = _parse_iso_datetime(generated_at) or dt.datetime.now(tz=dt.timezone.utc)
    now_ts = now_dt.timestamp()
    activation_started_at = (
        str(previous.get("activation_started_at") or "")
        if previous.get("activation_id") == activation_id
        else ""
    )
    if not activation_started_at:
        activation_started_at = now_dt.isoformat().replace("+00:00", "Z")
    activation_start_dt = _parse_iso_datetime(activation_started_at) or now_dt
    ttl_s = float(
        getattr(args, "cross_exchange_live_actuator_ttl_s", _CROSS_EXCHANGE_ACTIVATION_TTL_S)
        or _CROSS_EXCHANGE_ACTIVATION_TTL_S
    )
    activation_expires_dt = activation_start_dt + dt.timedelta(seconds=ttl_s)
    activation_expires_at = activation_expires_dt.isoformat().replace("+00:00", "Z")
    activation = {
        "activation_started_at": activation_started_at,
        "activation_expires_at": activation_expires_at,
        "activation_ttl_s": ttl_s,
        "activation_non_refreshing": True,
    }
    if now_dt >= activation_expires_dt:
        converted = int(campaign.get("orders_accepted") or 0) > 0
        return persist(
            "EXPIRED_AFTER_CONVERSION" if converted else "EXPIRED_ZERO_CONVERSION",
            **activation,
            terminal_reason=(
                "NON_REFRESHING_TTL_EXPIRED_AFTER_ACCEPTANCE"
                if converted
                else "NON_REFRESHING_TTL_EXPIRED_WITHOUT_ACCEPTANCE"
            ),
            deadman_gate=deadman_gate,
        )

    paper_path = promoted_state_path or str(
        getattr(
            args,
            "cross_exchange_paper_state",
            "data/research/btc5m_cross_exchange_probability_edge_paper_lane_state.json",
        )
    )
    paper = load_json(paper_path, default={})
    paper = paper if isinstance(paper, dict) else {}
    frozen = paper.get("frozen_model") if isinstance(paper.get("frozen_model"), dict) else {}
    walk_forward = paper.get("walk_forward") if isinstance(paper.get("walk_forward"), dict) else {}
    train = walk_forward.get("train") if isinstance(walk_forward.get("train"), dict) else {}
    holdout = (
        walk_forward.get("chronological_holdout")
        if isinstance(walk_forward.get("chronological_holdout"), dict)
        else {}
    )
    prospective = (
        paper.get("prospective_executable_book")
        if isinstance(paper.get("prospective_executable_book"), dict)
        else {}
    )
    selected_checks = (
        (selected.get("evidence_snapshot") or {}).get("checks")
        if isinstance(selected.get("evidence_snapshot"), dict)
        else {}
    )
    selected_checks = selected_checks if isinstance(selected_checks, dict) else {}
    evidence_checks = {
        "paper_only": paper.get("paper_only") is True and int(paper.get("orders_submitted") or 0) == 0,
        "frozen_checksum_exact": (
            str(frozen.get("checksum") or "") == model_checksum
            and str(frozen.get("status") or "") == "IMMUTABLE_CHECKSUM_VERIFIED"
        ),
        "train_positive": bool(train.get("positive")) and float(train.get("post_fee_pnl_usd") or 0.0) > 0.0,
        "holdout_positive": (
            bool(holdout.get("positive")) and float(holdout.get("post_fee_pnl_usd") or 0.0) > 0.0
        ),
        "prospective_positive": (
            bool(prospective.get("positive"))
            and float(prospective.get("post_fee_pnl_usd") or 0.0) > 0.0
            and int(prospective.get("resolved_signals") or 0) >= 10
        ),
    }
    if promoted:
        evidence_checks = {
            "selector_gate_pass": selected.get("gate_pass") is True,
            "selector_all_checks_pass": bool(selected_checks) and all(selected_checks.values()),
            "selector_record_checksum_present": bool(selected.get("record_checksum")),
            "selector_evidence_checksum_present": bool(selected.get("evidence_snapshot_checksum")),
            "paper_only": paper.get("paper_only") is True and paper.get("live_orders_allowed") is False,
            "model_checksum_exact": (
                str(
                    ((paper.get("frozen_model") or {}).get("checksum"))
                    or ((paper.get("activation_gate") or {}).get("model_checksum"))
                    or ""
                )
                == model_checksum
            ),
        }
    evidence = {
        "checks": evidence_checks,
        "model_checksum": frozen.get("checksum"),
        "train": train,
        "chronological_holdout": holdout,
        "prospective_executable_book": prospective,
        "permanent_promotion_gate": paper.get("promotion_gate"),
    }
    if not all(evidence_checks.values()):
        return persist(
            "EVIDENCE_GATE_BLOCKED",
            **activation,
            terminal_reason="FROZEN_EVIDENCE_REGRESSION",
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )

    terminal = paper.get("current_terminal") if isinstance(paper.get("current_terminal"), dict) else {}
    if promoted and execution_mode == "passive" and not terminal:
        decision = paper.get("current_decision") if isinstance(paper.get("current_decision"), dict) else {}
        quote_price = float(decision.get("quote_price") or 0.0)
        signal = {
            **decision,
            "signal_id": stable_id(
                "xepc",
                {"cell_id": cell_id, "market_slug": decision.get("market_slug")},
            ),
            "executable_price": quote_price,
            "observed_ts": (
                _parse_iso_datetime(paper.get("generated_at")).timestamp()
                if _parse_iso_datetime(paper.get("generated_at"))
                else 0.0
            ),
            "signal_ts": int(decision.get("window_start_s") or 0)
            + int(selected.get("signal_offset_s") or 0),
            "book": {
                "status": "OK",
                "best_bid": decision.get("best_bid_at_quote"),
                "best_ask": decision.get("best_ask_at_quote"),
                "fillable_usd": decision.get("fillable_usd"),
                "book_hash": stable_id("book", decision),
            },
            "passive_quote_evidence": decision.get("passive_quote_evidence"),
            "blockers": list(decision.get("reasons") or []),
        }
        terminal = {
            "terminal_status": "SIGNAL" if decision.get("eligible") else "PROTECTED_SKIP",
            "model_checksum": model_checksum,
            "window_start_s": decision.get("window_start_s"),
            "signal": signal,
        }
    terminal_status = str(terminal.get("terminal_status") or "")
    if terminal_status != "SIGNAL":
        return persist(
            "ARMED_WAITING_QUALIFYING_SIGNAL",
            **activation,
            terminal_reason=terminal_status or "NO_CURRENT_TERMINAL",
            terminal_blockers=list(terminal.get("blockers") or []),
            paper_state_path=paper_path,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    signal = terminal.get("signal") if isinstance(terminal.get("signal"), dict) else {}
    paired_bundle = promoted and execution_mode in {"paired_passive", "paired_split_sell"}
    window_start_s = int(signal.get("window_start_s") or terminal.get("window_start_s") or 0)
    current_window_start_s = int(now_ts // 300) * 300
    book = signal.get("book") if isinstance(signal.get("book"), dict) else {}
    executable_price = float(signal.get("executable_price") or 0.0)
    passive_quote_evidence = (
        signal.get("passive_quote_evidence")
        if isinstance(signal.get("passive_quote_evidence"), dict)
        else {}
    )
    passive_execution_proof = (
        execution_mode != "passive"
        or (
            bool(passive_quote_evidence.get("passive_fill_model_checksum"))
            and passive_quote_evidence.get("queue_ahead_shares_at_quote") is not None
            and float(passive_quote_evidence.get("book_sequence") or 0.0) > 0.0
        )
    )
    signal_checks = {
        "terminal_checksum_exact": str(terminal.get("model_checksum") or "") == model_checksum,
        "current_btc5m_window": (
            window_start_s == current_window_start_s
            and str(signal.get("market_slug") or "") == f"btc-updown-5m-{current_window_start_s}"
        ),
        "fresh_observation": 0.0 <= now_ts - float(signal.get("observed_ts") or 0.0) <= 300.0,
        "book_ok": str(book.get("status") or "") == "OK",
        "executable_depth": (
            all(
                float((row.get("book") or {}).get("fillable_sell_shares") or 0.0) + 1e-9 >= 1.0
                for row in signal.get("paired_legs") or []
            )
            if execution_mode == "paired_split_sell"
            else passive_execution_proof
            if execution_mode == "passive"
            else float(book.get("fillable_usd") or 0.0) + 1e-9
            >= _CROSS_EXCHANGE_ORDER_USD
        ),
        "passive_quote_execution_proof": passive_execution_proof,
        "hard_price_bounds": (
            all(
                (0.0 < float(row.get("limit_price") or 0.0) < 1.0)
                if execution_mode == "paired_split_sell"
                else (_CROSS_EXCHANGE_MIN_PRICE <= float(row.get("limit_price") or 0.0) <= _CROSS_EXCHANGE_MAX_PRICE)
                for row in terminal.get("paired_intents") or []
            )
            if paired_bundle
            else _CROSS_EXCHANGE_MIN_PRICE <= executable_price <= _CROSS_EXCHANGE_MAX_PRICE
        ),
        "positive_net_edge": float(signal.get("net_edge_per_share") or 0.0) > 0.0,
        "no_signal_blockers": not list(signal.get("blockers") or []),
    }
    if not all(signal_checks.values()):
        return persist(
            "PROTECTED_SKIP",
            **activation,
            terminal_reason="SIGNAL_PROTECTION_GATE_FAILED",
            signal_checks=signal_checks,
            signal_blockers=list(signal.get("blockers") or []),
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    try:
        paper_intent = _cross_exchange_probability_signal_to_intent(signal)
        bundle_intents: list[CopyIntent] = []
        if promoted:
            if paired_bundle:
                bundle_intents = _promoted_paired_bundle_intents(
                    terminal,
                    cell_id=cell_id,
                    source_tag=source_tag,
                    lane=lane,
                    activation_id=activation_id,
                    record_checksum=str(selected.get("record_checksum") or ""),
                    evidence_snapshot_checksum=str(selected.get("evidence_snapshot_checksum") or ""),
                    execution_mode=execution_mode,
                )
                paper_intent = bundle_intents[0]
            else:
                intent_payload = paper_intent.asdict()
                intent_payload.update(
                    {
                        "intent_id": stable_id(
                            "ci",
                            {"cell_id": cell_id, "signal_id": signal.get("signal_id")},
                        ),
                        "source_wallet": source_tag,
                        "wallet_name": lane,
                        "strategy_family": lane,
                    }
                )
                metadata = dict(intent_payload.get("metadata") or {})
                metadata["promoted_cell"] = {
                    "cell_id": cell_id,
                    "record_checksum": selected.get("record_checksum"),
                    "evidence_snapshot_checksum": selected.get("evidence_snapshot_checksum"),
                    "activation_id": activation_id,
                    "execution_mode": execution_mode,
                }
                if execution_mode == "passive":
                    metadata["precision_requires_passive_source"] = {
                        "status": "promoted_cell_post_only",
                        "execution_path": "direct_post_only_gtc_at_source",
                        "passive_price": float(signal.get("executable_price") or 0.0),
                        "original_source_price": float(signal.get("executable_price") or 0.0),
                        "buffered_limit_price": float(signal.get("executable_price") or 0.0),
                        "max_copy_price": _CROSS_EXCHANGE_MAX_PRICE,
                        "best_ask": float((signal.get("book") or {}).get("best_ask") or 0.0),
                    }
                intent_payload["metadata"] = metadata
                paper_intent = CopyIntent.from_dict(intent_payload)
    except Exception as exc:
        return persist(
            "INTENT_RECONSTRUCTION_BLOCKED",
            **activation,
            terminal_reason="PAPER_INTENT_RECONSTRUCTION_FAILED",
            error=f"{type(exc).__name__}: {exc}",
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    fixed_size_ok = (
        abs(sum(float(row.copy_size_usd) for row in bundle_intents) - _CROSS_EXCHANGE_ORDER_USD) <= 1e-9
        if paired_bundle
        else abs(float(paper_intent.copy_size_usd) - _CROSS_EXCHANGE_ORDER_USD) <= 1e-9
        and str(paper_intent.sizing_policy_id or "") == "fixed_usd_1"
    )
    terminal_intent = terminal.get("intent") if isinstance(terminal.get("intent"), dict) else {}
    if promoted:
        terminal_intent = paper_intent.asdict()
    reconstruction_ok = bool(bundle_intents) if paired_bundle else paper_intent.asdict() == terminal_intent
    if not fixed_size_ok or not reconstruction_ok:
        return persist(
            "INTENT_IDENTITY_BLOCKED",
            **activation,
            terminal_reason="FIXED_SIZE_OR_TERMINAL_INTENT_MISMATCH",
            fixed_size_ok=fixed_size_ok,
            terminal_intent_matches_reconstruction=reconstruction_ok,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    identity_intents = bundle_intents if paired_bundle else [paper_intent]
    live_identity_intents = [promote_intent_for_live(row, operator_approval_id=str(args.operator_approval_id)) for row in identity_intents]
    paper_identity = [_cross_exchange_identity_payload(row) for row in identity_intents]
    live_identity = [_cross_exchange_identity_payload(row) for row in live_identity_intents]
    paper_intent_hash = _cross_exchange_payload_hash(paper_identity)
    live_intent_hash = _cross_exchange_payload_hash(live_identity)
    source_event_hash = _cross_exchange_payload_hash(signal)
    if paper_identity != live_identity or paper_intent_hash != live_intent_hash:
        return persist(
            "PARITY_BLOCKED",
            **activation,
            terminal_reason="PAPER_LIVE_IDENTITY_HASH_MISMATCH",
            paper_intent_hash=paper_intent_hash,
            live_intent_hash=live_intent_hash,
            source_event_hash=source_event_hash,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    intents, window_dedupe = _drop_cross_exchange_already_routed_windows(
        identity_intents,
        live_ledger_state=str(args.live_ledger_state),
        source_tag=source_tag,
        lane=lane,
    )
    if not intents:
        return persist(
            "NO_NEW_WINDOWS",
            **activation,
            terminal_reason="WINDOW_ALREADY_ACCEPTED",
            window_dedupe=window_dedupe,
            paper_intent_hash=paper_intent_hash,
            live_intent_hash=live_intent_hash,
            source_event_hash=source_event_hash,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    intents, dedupe = _drop_already_live_submitted_intents(
        intents,
        live_ledger_state=str(args.live_ledger_state),
    )
    if not intents:
        return persist(
            "NO_NEW_INTENTS",
            **activation,
            terminal_reason="INTENT_ALREADY_ACCEPTED",
            dedupe=dedupe,
            window_dedupe=window_dedupe,
            paper_intent_hash=paper_intent_hash,
            live_intent_hash=live_intent_hash,
            source_event_hash=source_event_hash,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    fallback_token_map = _intent_token_maps(intents, allow_partial=True)
    capsules, token_map, parity_blockers = _parity_capsules(
        intents,
        operator_approval_id=str(args.operator_approval_id),
        gamma_timeout_s=5.0,
        fallback_token_map=fallback_token_map,
    )
    parity_ok = len(capsules) == len(intents) and not parity_blockers and all(
        str(row.get("status") or "") == "PASS"
        and not list(row.get("mismatched_fields") or [])
        and row.get("decision_wallet_copy_matches_live_intent") is not False
        for row in capsules
    )
    parity_digest = str(capsules[0].get("parity_digest") or "") if capsules else ""
    if not parity_ok:
        return persist(
            "PARITY_BLOCKED",
            **activation,
            terminal_reason="COPYINTENT_PARITY_CAPSULE_FAILED",
            parity_blockers=parity_blockers,
            parity_capsules=capsules,
            paper_intent_hash=paper_intent_hash,
            live_intent_hash=live_intent_hash,
            source_event_hash=source_event_hash,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    try:
        execution = asyncio.run(
            _execute_cross_exchange_live_route_async(
                args,
                intents,
                token_map=token_map,
                source_tag=source_tag,
                lane=lane,
                execution_mode=execution_mode,
                record_checksum=str(selected.get("record_checksum") or ""),
                evidence_snapshot_checksum=str(
                    selected.get("evidence_snapshot_checksum") or ""
                ),
            )
        )
    except Exception as exc:
        return persist(
            "EXECUTION_ERROR",
            **activation,
            terminal_reason="SOLE_GUARD_EXECUTION_ERROR",
            error=f"{type(exc).__name__}: {exc}",
            parity_digest=parity_digest,
            paper_intent_hash=paper_intent_hash,
            live_intent_hash=live_intent_hash,
            source_event_hash=source_event_hash,
            deadman_gate=deadman_gate,
            evidence=evidence,
            method_pnl=method_pnl,
        )
    results = [row for row in execution.get("results") or [] if isinstance(row, dict)]
    accepted = [
        row
        for row in results
        if str(row.get("status") or "").lower() in {"submitted", "filled"}
        or str(row.get("post_status") or "").lower() in {"live", "matched", "filled"}
    ]
    filled = [
        row
        for row in results
        if str(row.get("status") or "").lower() == "filled"
        or str(row.get("post_status") or "").lower() in {"matched", "filled"}
        or float(row.get("filled_size_usd") or row.get("response_filled_size_usd") or 0.0) > 0
    ]
    first_accepted = accepted[0] if accepted else {}
    return persist(
        "LIVE_SUBMITTED" if accepted else "LIVE_ATTEMPT_REJECTED",
        **activation,
        terminal_reason="ACCEPTED_ORDER_RESTORED_FLOW" if accepted else "EXCHANGE_ATTEMPT_NOT_ACCEPTED",
        paper_state_path=paper_path,
        deadman_gate=deadman_gate,
        evidence=evidence,
        signal_checks=signal_checks,
        dedupe=dedupe,
        window_dedupe=window_dedupe,
        paper_intent_hash=paper_intent_hash,
        live_intent_hash=live_intent_hash,
        source_event_hash=source_event_hash,
        parity_digest=parity_digest,
        parity_capsules=[
            {
                key: capsule.get(key)
                for key in (
                    "status",
                    "paper_intent_id",
                    "live_intent_id",
                    "parity_digest",
                    "blockers",
                    "mismatched_fields",
                    "decision_wallet_copy_matches_live_intent",
                )
            }
            for capsule in capsules
        ],
        intent_ids=[intent.intent_id for intent in intents],
        cycle_orders_submitted=len(results),
        cycle_orders_accepted=len(accepted),
        cycle_orders_filled=len(filled),
        cycle_last_order_id=str(first_accepted.get("order_id") or ""),
        cycle_last_order_status=str(
            first_accepted.get("final_status")
            or first_accepted.get("post_status")
            or first_accepted.get("status")
            or ""
        ),
        cycle_last_accepted_at=(
            first_accepted.get("accepted_at")
            or first_accepted.get("submitted_at")
            or generated_at
            if accepted
            else None
        ),
        results=results,
    )


def _shadow_intent_from_row(row: dict[str, Any], *, lane: str, source_tag: str) -> CopyIntent | None:
    if not isinstance(row, dict):
        return None
    try:
        intent = CopyIntent.from_dict(row)
    except TypeError:
        return None
    metadata = dict(intent.metadata or {})
    metadata["guard_shadow_route"] = {
        "flow_stage": "LIVE/PROMOTE/OBSERVE",
        "lane": lane,
        "source_tag": source_tag,
        "live_hard_off": True,
        "rule": "paper_lane_copyintent_through_single_live_guard_shadow_route",
    }
    return CopyIntent.from_dict(
        {
            **intent.asdict(),
            "source_wallet": source_tag,
            "wallet_name": lane,
            "mode": "paper",
            "live_orders_allowed": False,
            "metadata": metadata,
        }
    )


def _shadow_orders_by_intent(paper_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    orders = paper_state.get("orders") if isinstance(paper_state, dict) else []
    by_intent: dict[str, dict[str, Any]] = {}
    for order in orders if isinstance(orders, list) else []:
        if not isinstance(order, dict):
            continue
        intent_id = str(order.get("intent_id") or (order.get("source_intent") or {}).get("intent_id") or "")
        if not intent_id:
            continue
        current = by_intent.get(intent_id)
        if current is None or str(order.get("updated_at") or "") >= str(current.get("updated_at") or ""):
            by_intent[intent_id] = order
    return by_intent


def _shadow_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _shadow_e5_maker_quote(order: dict[str, Any]) -> dict[str, Any]:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    maker_quote = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
    if maker_quote:
        return maker_quote
    embedded = metadata.get("e5_maker_first_btc5m_v1")
    return embedded if isinstance(embedded, dict) else {}


def _shadow_book_aware_fill_test(order: dict[str, Any], *, lane: str) -> dict[str, Any]:
    if not isinstance(order, dict):
        return {"status": "NO_PAPER_ORDER", "lane": lane}
    if lane == "e5_maker_first_btc5m_v1":
        maker_quote = _shadow_e5_maker_quote(order)
        top_of_book = maker_quote.get("top_of_book") if isinstance(maker_quote.get("top_of_book"), dict) else {}
        route_report = top_of_book.get("route_report") if isinstance(top_of_book.get("route_report"), dict) else {}
        return {
            "status": top_of_book.get("status") or "MISSING_TOP_OF_BOOK",
            "lane": lane,
            "requested_size_usd": _shadow_float(order.get("requested_size_usd") or top_of_book.get("copy_size_usd")),
            "requested_shares": _shadow_float(order.get("requested_shares")),
            "best_bid": _shadow_float(top_of_book.get("best_bid")),
            "best_ask": _shadow_float(top_of_book.get("best_ask")),
            "fillable_usd": _shadow_float(top_of_book.get("fillable_usd")),
            "fillable_shares": _shadow_float(top_of_book.get("fillable_shares")),
            "fill_ratio": _shadow_float(top_of_book.get("fill_ratio")),
            "avg_fill_price": _shadow_float(top_of_book.get("avg_fill_price")),
            "instant_fill_status": top_of_book.get("instant_fill_status"),
            "blocking_reason": top_of_book.get("blocking_reason"),
            "levels_used": int(top_of_book.get("levels_used") or 0),
            "book_hash_present": bool(top_of_book.get("book_hash")),
            "route_status": route_report.get("status"),
            "route_class": route_report.get("route_class"),
            "fallback_source": route_report.get("fallback_source"),
        }

    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    fill_estimate = source_intent.get("fill_estimate") if isinstance(source_intent.get("fill_estimate"), dict) else {}
    if not fill_estimate:
        for event in order.get("lifecycle") or []:
            if isinstance(event, dict) and isinstance(event.get("payload"), dict):
                payload = event["payload"]
                if payload.get("fill_model") or payload.get("source"):
                    fill_estimate = payload
                    break
    reject_details = fill_estimate.get("reject_details") if isinstance(fill_estimate.get("reject_details"), dict) else {}
    return {
        "status": fill_estimate.get("status") or str(order.get("final_status") or order.get("status") or ""),
        "lane": lane,
        "requested_size_usd": _shadow_float(order.get("requested_size_usd") or fill_estimate.get("requested_size_usd")),
        "filled_size_usd": _shadow_float(order.get("filled_size_usd") or fill_estimate.get("filled_size_usd")),
        "fill_ratio": _shadow_float(fill_estimate.get("fill_ratio")),
        "effective_price": _shadow_float(fill_estimate.get("effective_price")),
        "source": fill_estimate.get("source"),
        "fill_model": fill_estimate.get("fill_model") or order.get("fill_model"),
        "book_status": reject_details.get("book_status"),
    }


def _pricing_provenance_for_shadow_order(order: dict[str, Any], *, lane: str) -> dict[str, Any]:
    if not isinstance(order, dict):
        return {"category": "no_paper_order", "lane": lane}
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    maker_quote = _shadow_e5_maker_quote(order)
    top_of_book = maker_quote.get("top_of_book") if isinstance(maker_quote.get("top_of_book"), dict) else {}
    route_report = top_of_book.get("route_report") if isinstance(top_of_book.get("route_report"), dict) else {}
    if lane == "e5_maker_first_btc5m_v1":
        fallback_markers = (
            route_report.get("fallback_source"),
            route_report.get("fallback_attempts"),
            route_report.get("primary_error"),
            route_report.get("primary_route_report"),
            route_report.get("suppressed_env_var"),
        )
        if any(bool(item) for item in fallback_markers):
            category = "direct_fallback"
        elif top_of_book.get("status") == "OK" and top_of_book.get("book_hash"):
            category = "genuine_book"
        elif not top_of_book or str(top_of_book.get("status") or "").lower().startswith("not_fetched"):
            category = "no_book_evidence"
        else:
            category = "book_unusable"
        return {
            "category": category,
            "lane": lane,
            "top_of_book_status": top_of_book.get("status"),
            "book_hash_present": bool(top_of_book.get("book_hash")),
            "route_status": route_report.get("status"),
            "route_class": route_report.get("route_class"),
            "fallback_source": route_report.get("fallback_source"),
            "primary_error_present": bool(route_report.get("primary_error")),
            "fallback_attempts": len(route_report.get("fallback_attempts") or [])
            if isinstance(route_report.get("fallback_attempts"), list)
            else 0,
        }

    fill_estimate = source_intent.get("fill_estimate") if isinstance(source_intent.get("fill_estimate"), dict) else {}
    if not fill_estimate:
        for event in order.get("lifecycle") or []:
            if isinstance(event, dict) and isinstance(event.get("payload"), dict):
                payload = event["payload"]
                if payload.get("fill_model") or payload.get("source"):
                    fill_estimate = payload
                    break
    source = str(fill_estimate.get("source") or "")
    if source:
        category = source
    elif str(order.get("final_status") or "").upper() == "FILLED":
        category = "paper_fill_without_source_detail"
    else:
        category = "no_fill_evidence"
    return {
        "category": category,
        "lane": lane,
        "fill_model": fill_estimate.get("fill_model") or order.get("fill_model"),
        "book_status": (fill_estimate.get("reject_details") or {}).get("book_status")
        if isinstance(fill_estimate.get("reject_details"), dict)
        else None,
    }


def _shadow_zero_submit_assertion(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    orders_submitted = int(summary.get("orders_submitted") or 0)
    if orders_submitted == 0:
        payload["zero_live_assertion"] = {
            "status": "PASS",
            "orders_submitted": 0,
            "rule": "guard_shadow_route_must_never_submit_orders",
        }
        return payload
    payload["status"] = "SHADOW_INCIDENT_NONZERO_SUBMIT"
    payload["enabled"] = False
    payload["incident_triggered"] = True
    payload["zero_live_assertion"] = {
        "status": "INCIDENT",
        "orders_submitted": orders_submitted,
        "rule": "guard_shadow_route_must_never_submit_orders",
        "next_action": "stop shadow route immediately and ask_fable",
    }
    return payload


def _shadow_filter_reason(diagnostics: dict[str, Any]) -> str:
    if not diagnostics:
        return "diagnostics_missing"
    if not diagnostics.get("btc_5m_scope_ok"):
        return "not_btc_5m"
    if diagnostics.get("market_closed_now"):
        return "market_closed_now"
    if diagnostics.get("observed_after_market_close"):
        return "observed_after_market_close"
    if not diagnostics.get("fresh_for_live_build"):
        return "stale_build_observed_age"
    if not (diagnostics.get("fresh_by_event_age") or diagnostics.get("fresh_by_observation_event_age")):
        return "stale_event_age"
    if not diagnostics.get("live_tradeable_window_open"):
        return "not_live_tradeable_window"
    return "guard_or_parity_filtered"


def _build_shadow_lane(
    args: argparse.Namespace,
    *,
    spec: dict[str, str],
    prior_rows: dict[tuple[str, str], dict[str, Any]],
    generated_at: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    profile_started = time.perf_counter()
    profile_checkpoint = profile_started
    profile_stages: list[dict[str, Any]] = []

    def mark_profile(name: str) -> None:
        nonlocal profile_checkpoint
        now = time.perf_counter()
        profile_stages.append({"name": name, "duration_s": round(now - profile_checkpoint, 6)})
        profile_checkpoint = now

    lane = spec["lane"]
    source_tag = spec["source_tag"]
    lane_state = load_json(spec["state_path"], default={})
    lane_state = lane_state if isinstance(lane_state, dict) else {}
    paper_state = load_json(spec["paper_state_path"], default={})
    paper_state = paper_state if isinstance(paper_state, dict) else {}
    orders_by_intent = _shadow_orders_by_intent(paper_state)
    raw_intents = [
        intent
        for row in lane_state.get("current_intents") or []
        if (intent := _shadow_intent_from_row(row, lane=lane, source_tag=source_tag)) is not None
    ]
    mark_profile("load_lane_and_paper_state")
    shadow_max_event_age_s = _shadow_max_event_age_s(args)
    shadow_live_build_max_observed_age_s = _shadow_live_build_max_observed_age_s(args)
    now_ts = time.time()
    freshness = _candidate_runtime_freshness_summary(
        raw_intents,
        now_ts=now_ts,
        max_event_age_s=shadow_max_event_age_s,
        live_build_max_observed_age_s=shadow_live_build_max_observed_age_s,
    )
    diagnostics_by_intent = {
        intent.intent_id: _intent_runtime_diagnostics(
            intent,
            now_ts=now_ts,
            max_event_age_s=shadow_max_event_age_s,
            live_build_max_observed_age_s=shadow_live_build_max_observed_age_s,
        )
        for intent in raw_intents
    }
    freshness_filter_reason_counts: dict[str, int] = {}
    for diagnostics in diagnostics_by_intent.values():
        reason = "pass" if diagnostics.get("live_tradeable_window_open") else _shadow_filter_reason(diagnostics)
        freshness_filter_reason_counts[reason] = freshness_filter_reason_counts.get(reason, 0) + 1
    freshness = {
        **freshness,
        "path": "shadow_only",
        "applied_max_event_age_s": round(float(shadow_max_event_age_s), 6),
        "applied_live_build_max_observed_age_s": round(float(shadow_live_build_max_observed_age_s), 6),
        "live_path_max_event_age_s": round(float(getattr(args, "max_event_age_s", 30.0)), 6),
        "live_path_live_build_max_observed_age_s": round(float(_live_build_max_observed_age_s(args)), 6),
        "filter_reason_counts": dict(sorted(freshness_filter_reason_counts.items())),
    }
    mark_profile("freshness_diagnostics")
    fresh = _fresh_intents(
        raw_intents,
        max_event_age_s=shadow_max_event_age_s,
        max_intents=int(getattr(args, "shadow_max_intents_per_lane", 6)),
        min_copy_size_usd=0.0,
        live_build_max_observed_age_s=shadow_live_build_max_observed_age_s,
    )
    after_cap, hard_cap = _apply_live_hard_buy_price_cap(
        fresh,
        max_buy_price=float(getattr(args, "price_band_decision_max_price", 0.50) or 0.0),
    )
    hard_cap_filtered_by_intent = {
        str(row.get("intent_id") or ""): row
        for row in hard_cap.get("sample_filtered_intents") or []
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    }
    after_band_floor, hard_floor = _apply_live_hard_buy_price_floor(
        after_cap,
        min_buy_price=float(getattr(args, "price_band_decision_min_price", 0.25) or 0.0),
    )
    after_floor, min_order = _floor_live_min_order_intents(
        after_band_floor,
        min_live_order_usd=float(getattr(args, "min_live_order_usd", 1.0)),
    )
    after_book, book_gate = _apply_inventory_best_ask_gate(
        after_floor,
        timeout_s=float(getattr(args, "inventory_best_ask_timeout_s", 1.0)),
        enable_maker_fallback=False,
        enforce_copy_model_coverage=False,
    )
    after_dedupe, dedupe = _drop_already_live_submitted_intents(
        after_book,
        live_ledger_state=str(getattr(args, "live_ledger_state", "data/research/wallet_copy_live_execution_state.json")),
    )
    mark_profile("price_book_and_dedupe_filters")
    fallback_token_map = _intent_token_maps(after_dedupe, allow_partial=True)
    parity_profile: dict[str, Any] = {}
    capsules, token_map, token_blockers = _parity_capsules(
        after_dedupe,
        operator_approval_id="SHADOW_ROUTE_PLACEHOLDER_NOT_OPERATOR_APPROVAL",
        gamma_timeout_s=5.0,
        fallback_token_map=fallback_token_map,
        profile_out=parity_profile,
    )
    mark_profile("parity_capsules")
    guard_pass_ids = {intent.intent_id for intent in after_dedupe}
    parity_pass_ids = {
        str(capsule.get("paper_intent_id") or "")
        for capsule in capsules
        if str(capsule.get("status") or "") == "PASS"
    }
    final_ids = guard_pass_ids & parity_pass_ids if capsules else guard_pass_ids
    resolutions = load_resolutions(str(getattr(args, "resolutions", "data/research/btc_resolutions_from_btcusdt_ticks.jsonl")))
    mark_profile("load_resolutions")
    rows: list[dict[str, Any]] = []
    for intent in raw_intents:
        order = orders_by_intent.get(intent.intent_id, {})
        final_status = str(order.get("final_status") or order.get("status") or "").upper()
        guard_filter_passed = intent.intent_id in guard_pass_ids
        parity_passed = intent.intent_id in parity_pass_ids if capsules else guard_filter_passed
        prior = prior_rows.get((lane, intent.intent_id), {})
        provenance = _pricing_provenance_for_shadow_order(order, lane=lane)
        book_aware_fill_test = _shadow_book_aware_fill_test(order, lane=lane)
        runtime_diagnostics = diagnostics_by_intent.get(intent.intent_id, {})
        hard_cap_filtered = hard_cap_filtered_by_intent.get(intent.intent_id, {})
        hard_cap_reason = str(hard_cap_filtered.get("taxonomy") or hard_cap_filtered.get("reject_reason") or "")
        shadow_filter_reason = (
            None
            if intent.intent_id in final_ids
            else hard_cap_reason
            if hard_cap_reason
            else _shadow_filter_reason(runtime_diagnostics)
        )
        price_cap_shadow: dict[str, Any] = {}
        if hard_cap_reason == "price_cap_045":
            scored = score_order(order, resolutions) if isinstance(order, dict) else {}
            price_cap_shadow = {
                "status": "BLOCKED_BY_PRICE_CAP_045",
                "flow_stage": "LIVE/LEARN",
                "rule_id": "fable-20260712T0044-price-cap-045",
                "limit_price": round(float(intent.limit_price), 6),
                "max_buy_price": hard_cap_filtered.get("max_buy_price"),
                "previous_max_buy_price": hard_cap_filtered.get("previous_max_buy_price", 0.50),
                "price_cap_band": hard_cap_filtered.get("price_cap_band", "45_50"),
                "paper_final_status": final_status,
                "window_outcome": {
                    "resolved": bool(scored.get("resolved")),
                    "winner": scored.get("winner", ""),
                    "win": scored.get("win"),
                    "pnl_usd": scored.get("pnl_usd"),
                    "roi_pct": scored.get("roi_pct"),
                    "skip_reason": scored.get("skip_reason", ""),
                    "resolution": scored.get("resolution"),
                },
            }
        row = {
            **prior,
            "schema_version": 1,
            "flow_stage": "LIVE/PROMOTE/OBSERVE",
            "lane": lane,
            "source_tag": source_tag,
            "intent_id": intent.intent_id,
            "market_slug": intent.market_slug,
            "condition_id": intent.condition_id,
            "outcome": intent.outcome,
            "side": intent.side,
            "limit_price": round(float(intent.limit_price), 6),
            "copy_size_usd": round(float(intent.copy_size_usd), 6),
            "observed_ts": intent.observed_ts,
            "event_ts": intent.event_ts,
            "paper_only": True,
            "live_orders_allowed": False,
            "orders_submitted": 0,
            "guard_filter_passed": guard_filter_passed,
            "parity_passed": parity_passed,
            "shadow_status": "SHADOW_READY" if intent.intent_id in final_ids else "SHADOW_FILTERED",
            "shadow_freshness": {
                "applied_max_event_age_s": round(float(shadow_max_event_age_s), 6),
                "applied_live_build_max_observed_age_s": round(float(shadow_live_build_max_observed_age_s), 6),
                "event_age_s": runtime_diagnostics.get("event_age_s"),
                "observed_age_s": runtime_diagnostics.get("observed_age_s"),
                "observation_event_age_s": runtime_diagnostics.get("observation_event_age_s"),
                "freshness_basis": runtime_diagnostics.get("freshness_basis"),
                "filter_reason": shadow_filter_reason,
            },
            "shadow_freshness_filter_reason": shadow_filter_reason,
            "policy_filter_reason": hard_cap_reason or None,
            "price_cap_045_shadow": price_cap_shadow,
            "shadow_event_age_s_at_cycle": runtime_diagnostics.get("event_age_s"),
            "shadow_observed_age_s_at_cycle": runtime_diagnostics.get("observed_age_s"),
            "shadow_observation_event_age_s_at_cycle": runtime_diagnostics.get("observation_event_age_s"),
            "shadow_max_event_age_s": round(float(shadow_max_event_age_s), 6),
            "shadow_live_build_max_observed_age_s": round(float(shadow_live_build_max_observed_age_s), 6),
            "paper_order_id": order.get("order_id"),
            "paper_final_status": final_status,
            "would_have_filled": bool(intent.intent_id in final_ids and final_status == "FILLED"),
            "filled_size_usd": order.get("filled_size_usd"),
            "pricing_provenance": provenance,
            "book_aware_fill_test": book_aware_fill_test,
            "updated_at": generated_at,
            "source_intent": intent.asdict(),
        }
        if not prior:
            row["first_seen_at"] = generated_at
        rows.append(row)
    mark_profile("score_and_serialize_rows")
    provenance_counts: dict[str, int] = {}
    for row in rows:
        if not row.get("would_have_filled"):
            continue
        category = str((row.get("pricing_provenance") or {}).get("category") or "unknown")
        provenance_counts[category] = provenance_counts.get(category, 0) + 1
    lane_summary = {
        "lane": lane,
        "source_tag": source_tag,
        "paper_only": True,
        "live_orders_allowed": False,
        "intents_built": len(raw_intents),
        "fresh_intents": len(fresh),
        "guard_filter_passed": len(guard_pass_ids),
        "parity_passed": len(parity_pass_ids) if capsules else len(guard_pass_ids),
        "would_have_filled": sum(1 for row in rows if row.get("would_have_filled")),
        "orders_submitted": 0,
        "pricing_provenance_counts": dict(sorted(provenance_counts.items())),
        "shadow_max_event_age_s": round(float(shadow_max_event_age_s), 6),
        "shadow_live_build_max_observed_age_s": round(float(shadow_live_build_max_observed_age_s), 6),
        "token_mapping_blockers": token_blockers,
        "filters": {
            "freshness": freshness,
            "hard_cap": hard_cap,
            "hard_floor": hard_floor,
            "min_order": min_order,
            "inventory_best_ask_gate": book_gate,
            "live_dedupe": dedupe,
            "parity_capsules_profile": parity_profile,
            "token_map_conditions": len(token_map),
        },
        "runtime_profile": {
            "stages": profile_stages,
            "total_s": round(time.perf_counter() - profile_started, 6),
        },
    }
    return lane_summary, rows


def _shadow_snapshot_signature(payload: dict[str, Any]) -> str:
    lanes = []
    for lane in payload.get("lanes") or []:
        if not isinstance(lane, dict):
            continue
        lanes.append(
            {
                "lane": lane.get("lane"),
                "intents_built": lane.get("intents_built"),
                "fresh_intents": lane.get("fresh_intents"),
                "guard_filter_passed": lane.get("guard_filter_passed"),
                "parity_passed": lane.get("parity_passed"),
                "would_have_filled": lane.get("would_have_filled"),
                "orders_submitted": lane.get("orders_submitted"),
                "pricing_provenance_counts": lane.get("pricing_provenance_counts"),
                "freshness_filter_reason_counts": (
                    ((lane.get("filters") or {}).get("freshness") or {}).get("filter_reason_counts")
                ),
            }
        )
    material = {
        "summary": payload.get("summary"),
        "lanes": lanes,
        "zero_live_assertion": (payload.get("zero_live_assertion") or {}).get("status"),
    }
    return stable_id("shadow_snapshot", material, length=16)


def _jsonl_size(path: str) -> int:
    target = ROOT / path if not Path(path).is_absolute() else Path(path)
    try:
        return int(target.stat().st_size)
    except FileNotFoundError:
        return 0


def _rotate_shadow_event_log(path: str, *, max_bytes: int = 5 * 1024 * 1024) -> None:
    target = ROOT / path if not Path(path).is_absolute() else Path(path)
    if not target.exists() or target.stat().st_size < max_bytes:
        return
    rotated = target.with_name(f"{target.name}.1")
    if rotated.exists():
        rotated.unlink()
    target.replace(rotated)


def _shadow_event_window_start_s() -> int:
    return int(time.time() // 300) * 300


def _append_guard_shadow_lanes_events(
    args: argparse.Namespace,
    *,
    payload: dict[str, Any],
    prior: dict[str, Any],
    prior_rows: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    event_log = getattr(args, "shadow_event_log", "data/research/wallet_copy_guard_shadow_lanes_events.jsonl")
    _rotate_shadow_event_log(event_log)
    snapshot_signature = _shadow_snapshot_signature(payload)
    snapshot_window_start_s = _shadow_event_window_start_s()
    prior_event_state = prior.get("event_emitter") if isinstance(prior.get("event_emitter"), dict) else {}
    should_snapshot = (
        snapshot_signature != prior_event_state.get("last_snapshot_signature")
        and int(prior_event_state.get("last_snapshot_window_start_s") or -1) != snapshot_window_start_s
    )
    events_written = 0
    if should_snapshot:
        _append_jsonl(
            event_log,
            {
                "event": "wallet_copy_guard_shadow_lanes_snapshot",
                **{key: value for key, value in payload.items() if key not in {"rows", "event_emitter"}},
                "snapshot_signature": snapshot_signature,
                "snapshot_window_start_s": snapshot_window_start_s,
            },
        )
        events_written += 1

    for row in payload.get("rows") or []:
        if not isinstance(row, dict):
            continue
        key = (str(row.get("lane") or ""), str(row.get("intent_id") or ""))
        if not key[0] or not key[1]:
            continue
        prior_row = prior_rows.get(key, {})
        base = {
            "generated_at": payload.get("generated_at"),
            "lane": row.get("lane"),
            "intent_id": row.get("intent_id"),
            "first_seen_at": row.get("first_seen_at"),
            "market_slug": row.get("market_slug"),
            "orders_submitted": int(row.get("orders_submitted") or 0),
        }
        if not prior_row:
            _append_jsonl(event_log, {"event": "shadow_intent_first_seen", **base})
            events_written += 1
        book_test = row.get("book_aware_fill_test") if isinstance(row.get("book_aware_fill_test"), dict) else {}
        if book_test and not isinstance(prior_row.get("book_aware_fill_test"), dict):
            provenance = row.get("pricing_provenance") if isinstance(row.get("pricing_provenance"), dict) else {}
            _append_jsonl(
                event_log,
                {
                    "event": "shadow_book_test",
                    **base,
                    "instant_fill_status": book_test.get("instant_fill_status"),
                    "blocking_reason": book_test.get("blocking_reason"),
                    "fillable_usd": book_test.get("fillable_usd"),
                    "fill_ratio": book_test.get("fill_ratio"),
                    "pricing_provenance_category": provenance.get("category"),
                    "pricing_fallback_source": provenance.get("fallback_source"),
                    "top_of_book_status": provenance.get("top_of_book_status"),
                },
            )
            events_written += 1
        if row.get("would_have_filled") and not prior_row.get("would_have_filled"):
            _append_jsonl(event_log, {"event": "shadow_would_have_filled", **base})
            events_written += 1

    if should_snapshot:
        last_snapshot_signature = snapshot_signature
        last_snapshot_window_start_s = snapshot_window_start_s
    else:
        last_snapshot_signature = prior_event_state.get("last_snapshot_signature")
        last_snapshot_window_start_s = prior_event_state.get("last_snapshot_window_start_s")
    return {
        "event_log": event_log,
        "events_written": events_written,
        "last_snapshot_signature": last_snapshot_signature,
        "last_snapshot_window_start_s": last_snapshot_window_start_s,
        "snapshot_written": should_snapshot,
        "current_snapshot_signature": snapshot_signature,
        "current_window_start_s": snapshot_window_start_s,
        "size_bytes": _jsonl_size(event_log),
    }


def _run_guard_shadow_lanes(args: argparse.Namespace, *, generated_at: str) -> dict[str, Any]:
    profile_started = time.perf_counter()
    if not bool(getattr(args, "shadow_lanes", True)):
        return {
            "schema_version": 1,
            "kind": "wallet_copy_guard_shadow_lanes_state",
            "flow_stage": "LIVE/PROMOTE/OBSERVE",
            "generated_at": generated_at,
            "enabled": False,
            "status": "DISABLED",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {"orders_submitted": 0},
        }
    prior = load_json(getattr(args, "shadow_state", "data/research/wallet_copy_guard_shadow_lanes_state.json"), default={})
    prior = prior if isinstance(prior, dict) else {}
    prior_rows = {
        (str(row.get("lane") or ""), str(row.get("intent_id") or "")): row
        for row in prior.get("rows") or []
        if isinstance(row, dict) and row.get("lane") and row.get("intent_id")
    }
    prior_load_s = round(time.perf_counter() - profile_started, 6)
    all_specs = _shadow_lane_specs(args, include_parked=True)
    active_specs = [spec for spec in all_specs if not spec.get("parked")]
    parked_lanes = [
        dict(spec.get("park_decision") or {})
        for spec in all_specs
        if spec.get("parked")
    ]
    lane_summaries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    lane_profiles: list[dict[str, Any]] = []
    for spec in active_specs:
        lane_started = time.perf_counter()
        lane_summary, lane_rows = _build_shadow_lane(args, spec=spec, prior_rows=prior_rows, generated_at=generated_at)
        lane_profiles.append(
            {
                "lane": spec.get("lane"),
                "duration_s": round(time.perf_counter() - lane_started, 6),
                "rows": len(lane_rows),
            }
        )
        lane_summaries.append(lane_summary)
        rows.extend(lane_rows)
    lanes_built_s = round(time.perf_counter() - profile_started - prior_load_s, 6)
    sort_started = time.perf_counter()
    retain = max(1, int(getattr(args, "shadow_retain_rows", 10_000) or 10_000))
    rows = sorted(rows, key=lambda row: (str(row.get("updated_at") or ""), str(row.get("lane") or ""), str(row.get("intent_id") or "")))[
        -retain:
    ]
    sort_retain_s = round(time.perf_counter() - sort_started, 6)
    summary = {
        "lane_count": len(lane_summaries),
        "intents_built": sum(int(row.get("intents_built") or 0) for row in lane_summaries),
        "fresh_intents": sum(int(row.get("fresh_intents") or 0) for row in lane_summaries),
        "guard_filter_passed": sum(int(row.get("guard_filter_passed") or 0) for row in lane_summaries),
        "parity_passed": sum(int(row.get("parity_passed") or 0) for row in lane_summaries),
        "would_have_filled": sum(int(row.get("would_have_filled") or 0) for row in lane_summaries),
        "orders_submitted": 0,
    }
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_guard_shadow_lanes_state",
        "flow_stage": "LIVE/PROMOTE/OBSERVE",
        "generated_at": generated_at,
        "status": "PASS",
        "enabled": True,
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py owns shadow evaluation and submits zero shadow orders",
        "route_order": [spec["lane"] for spec in active_specs],
        "parked_lanes": parked_lanes,
        "summary": summary,
        "lanes": lane_summaries,
        "rows": rows,
    }
    payload = _shadow_zero_submit_assertion(payload)
    event_emitter_started = time.perf_counter()
    event_emitter = _append_guard_shadow_lanes_events(
        args,
        payload=payload,
        prior=prior,
        prior_rows=prior_rows,
    )
    event_emitter_s = round(time.perf_counter() - event_emitter_started, 6)
    payload["event_emitter"] = event_emitter
    payload["profile"] = {
        "status": "MEASURED",
        "prior_load_s": prior_load_s,
        "lanes_built_s": lanes_built_s,
        "sort_retain_s": sort_retain_s,
        "event_emitter_s": event_emitter_s,
        "lane_profiles": lane_profiles,
        "total_before_persist_s": round(
            time.perf_counter() - profile_started,
            6,
        ),
    }
    payload["profile"]["total_s"] = payload["profile"]["total_before_persist_s"]
    atomic_write_json(getattr(args, "shadow_state", "data/research/wallet_copy_guard_shadow_lanes_state.json"), payload)
    return payload


def _run_routing_shadow_validation(
    args: argparse.Namespace,
    *,
    active_set_runtime: dict[str, Any],
    live_stdout: dict[str, Any],
    live_probe_result: dict[str, Any],
    live_probe_promotion_result: dict[str, Any],
    dataapi_poll_result: dict[str, Any],
    generated_at: str,
    shadow_candidate_members: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    mode = str(getattr(args, "routing_router_mode", "shadow") or "shadow").lower()
    if mode != "shadow":
        return {
            "schema_version": 1,
            "kind": "routing_shadow_validation",
            "flow_stage": "LIVE/LEARN/SELF-DEV",
            "generated_at": generated_at,
            "enabled": False,
            "routing_mode": mode,
            "paper_only": True,
            "live_orders_allowed": False,
            "status": "DISABLED",
            "reason": "routing_router_mode_not_shadow",
        }
    state_path = str(getattr(args, "routing_shadow_validation_state", DEFAULT_ROUTING_SHADOW_VALIDATION_STATE))
    output = Path(state_path)
    if not output.is_absolute():
        output = ROOT / output
    previous = load_json(output, default={})
    previous = previous if isinstance(previous, dict) else {}
    members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    member_wallets = {
        str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        for member in members
        if isinstance(member, dict)
    }
    last_success = _latest_successful_nondenied_wallet(args, member_wallets)
    guard_like = {
        "generated_at": generated_at,
        "active_set_runtime": active_set_runtime,
        "live_execution": live_stdout,
        "active_set_live_execution_probes": live_probe_result,
        "active_set_live_execution_probe_promotions": live_probe_promotion_result,
        "active_set_dataapi_poller": dataapi_poll_result,
        "routing_shadow_candidate_seats_state": str(
            getattr(args, "routing_shadow_candidate_seats", DEFAULT_ROUTING_SHADOW_CANDIDATE_SEATS)
        ),
    }
    payload = build_routing_shadow_validation_from_guard(
        guard_like,
        previous=previous,
        generated_at=generated_at,
        min_validation_hours=float(
            getattr(args, "routing_shadow_validation_min_hours", DEFAULT_ROUTING_SHADOW_MIN_VALIDATION_HOURS)
        ),
        retain_rows=int(getattr(args, "routing_shadow_validation_retain_rows", DEFAULT_ROUTING_SHADOW_RETAIN_ROWS)),
        last_successful_wallet=str(last_success.get("wallet") or ""),
        shadow_candidate_members=shadow_candidate_members,
    )
    atomic_write_json(output, payload)
    return _compact_routing_shadow_validation_for_guard(payload, state_path=state_path)


def _candidate_pool(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in (
        "runtime_admission_candidate",
        "forward_runtime_candidate",
        "best_runtime_candidate",
        "best_candidate",
        "forward_candidate",
    ):
        row = payload.get(key)
        if isinstance(row, dict):
            rows.append(row)
    for key in (
        "forward_queue_runtime_candidates",
        "pass_candidates",
        "ranked_candidates",
        "forward_tracking_queue",
    ):
        for row in payload.get(key) or []:
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _candidate_policy_id(candidate: dict[str, Any]) -> str:
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    admission_identity = (
        candidate.get("admission_identity")
        if isinstance(candidate.get("admission_identity"), dict)
        else {}
    )
    return str(policy.get("policy_id") or candidate.get("policy_id") or admission_identity.get("policy_id") or "")


def _find_mission_alias_candidate(
    payload: dict[str, Any],
    *,
    source_wallet_pin: str,
    policy_id_pin: str,
) -> dict[str, Any]:
    if not source_wallet_pin or not policy_id_pin:
        return {}
    for row in _candidate_pool(payload):
        if str(row.get("candidate_type") or "") != "SINGLE_WALLET":
            continue
        if _candidate_source_wallet(row) != source_wallet_pin:
            continue
        if _candidate_policy_id(row) != policy_id_pin:
            continue
        matched = dict(row)
        matched["mission_candidate_id_alias_matched"] = True
        matched["mission_matched_profit_candidate_id"] = row.get("candidate_id")
        return matched
    return {}


def _runtime_copy_contract() -> dict[str, Any]:
    runtime_phase = mission_contract().get("current_runtime_phase_contract")
    runtime_phase = runtime_phase if isinstance(runtime_phase, dict) else {}
    profitability_filter = runtime_phase.get("profitability_filter_contract")
    profitability_filter = profitability_filter if isinstance(profitability_filter, dict) else {}
    return {
        "copy_mode": runtime_phase.get("live_mode"),
        "copy_style": runtime_phase.get("copy_style"),
        "profitability_first": bool(runtime_phase.get("profitability_first")),
        "strict_source_order_1_to_1_required": bool(runtime_phase.get("strict_source_order_1_to_1_required")),
        "selected_intent_parity_required": bool(runtime_phase.get("selected_intent_parity_required")),
        "profitability_filter": profitability_filter,
    }


def _mission_runtime_candidate_policy(candidate: dict[str, Any]) -> dict[str, Any]:
    return _live_execution_mission_runtime_candidate_policy(candidate)


def _active_set_runtime_member_candidate(
    args: argparse.Namespace,
    *,
    candidate_id_pin: str,
    source_wallet_pin: str,
    policy_id_pin: str,
) -> dict[str, Any]:
    selected_candidate_id = str(getattr(args, "active_set_selected_member_candidate_id", "") or "")
    selected_wallet = str(getattr(args, "active_set_selected_member_source_wallet", "") or "").lower()
    selected_policy = getattr(args, "active_set_selected_member_policy", None)
    if not selected_candidate_id or not selected_wallet:
        return {}
    if candidate_id_pin and candidate_id_pin != selected_candidate_id:
        return {}
    if source_wallet_pin and source_wallet_pin != selected_wallet:
        return {}
    policy = dict(selected_policy) if isinstance(selected_policy, dict) else {}
    if policy_id_pin and not policy.get("policy_id"):
        policy["policy_id"] = policy_id_pin
    if not policy.get("policy_id"):
        policy["policy_id"] = selected_candidate_id
    return {
        "candidate_id": selected_candidate_id,
        "candidate_type": "SINGLE_WALLET",
        "status": "PASS",
        "source_wallet": selected_wallet,
        "policy_id": policy.get("policy_id"),
        "policy": policy,
        "metadata": {
            "source": "active_set_runtime_member",
            "flow_stage": "LIVE/PROMOTE",
            "synthetic_candidate_reason": "selected_active_set_member_missing_from_profit_state",
            "active_set_member_status": str(getattr(args, "active_set_selected_member_status", "") or ""),
            "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py",
        },
        "live_target_profile": {
            "status": "PASS",
            "blockers": [],
            "operator_approval_id": str(getattr(args, "operator_approval_id", "") or "OP-LIVE-20260703-BELA"),
            "source": "active_set_runtime_member",
        },
    }


def _apply_active_set_selected_member_policy(candidate: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    selected_policy = getattr(args, "active_set_selected_member_policy", None)
    if not candidate or not isinstance(selected_policy, dict) or not selected_policy:
        return candidate
    selected_candidate_id = str(getattr(args, "active_set_selected_member_candidate_id", "") or "")
    selected_wallet = str(getattr(args, "active_set_selected_member_source_wallet", "") or "").lower()
    if selected_candidate_id and str(candidate.get("candidate_id") or "") != selected_candidate_id:
        return candidate
    if selected_wallet and _candidate_source_wallet(candidate) != selected_wallet:
        return candidate

    policy = dict(candidate.get("policy")) if isinstance(candidate.get("policy"), dict) else {}
    changed = False
    for key, value in selected_policy.items():
        if value is not None and policy.get(key) != value:
            policy[key] = value
            changed = True
    updated = dict(candidate)
    metadata = dict(updated.get("metadata")) if isinstance(updated.get("metadata"), dict) else {}
    original_status = str(updated.get("status") or "")
    if original_status:
        metadata.setdefault("active_set_member_status", original_status)
    if metadata:
        updated["metadata"] = metadata
    if changed:
        updated["policy"] = policy
        if policy.get("policy_id"):
            updated["policy_id"] = policy.get("policy_id")
        updated["active_set_runtime_policy_override"] = True
    override = load_json(
        str(getattr(args, "selected_candidate_override_state", "") or ""),
        default={},
        cache_readonly=False,
    )
    override_candidate = override.get("candidate") if isinstance(override, dict) else {}
    override_candidate = override_candidate if isinstance(override_candidate, dict) else {}
    pass_override_matches = (
        str(override_candidate.get("status") or "") == "PASS"
        and str(override_candidate.get("candidate_id") or "") == str(updated.get("candidate_id") or "")
        and _candidate_source_wallet(override_candidate) == _candidate_source_wallet(updated)
    )
    if pass_override_matches and str(updated.get("status") or "") != "PASS":
        updated["status"] = "PASS"
        updated["active_set_runtime_status_override"] = True
    return updated


def _live_source_route_gate(args: argparse.Namespace) -> dict[str, Any]:
    state = load_json(args.source_route_state, default={})
    state = state if isinstance(state, dict) else {}
    status = source_route_status(state)
    blockers: list[str] = []
    if not status:
        blockers.append("live_source_route_state_missing")
    elif not source_route_allows_live_execution(state):
        blockers.append(f"live_source_route_{status.lower()}_not_live_admissible")
    approval_id = source_route_live_operator_approval(state)
    next_action = state.get("next_action")
    if blockers and not next_action:
        next_action = (
            "configure and measure a live-admissible POLYMARKET_SOURCE_PROXY_URL, "
            "POLYMARKET_HTTPS_PROXY, or Polymarket base override before guarded live copy can trade"
        )
    required_live_operator_inputs = []
    if blockers:
        required_live_operator_inputs = [
            "POLYMARKET_SOURCE_PROXY_URL",
            "POLYMARKET_HTTPS_PROXY",
            "POLYMARKET_DATA_API_BASE_URL",
            "POLYMARKET_GAMMA_API_BASE_URL",
            "POLYMARKET_CLOB_API_BASE_URL",
        ]
    return {
        "status": status or "MISSING",
        "blockers": blockers,
        "next_action": next_action,
        "required_live_operator_inputs": required_live_operator_inputs,
        "external_route_required": state.get("external_route_required"),
        "source_proxy_configured": state.get("source_proxy_configured"),
        "operator_live_source_route_approval_id": approval_id or None,
        "measured_base_or_proxy_route_pass": state.get("measured_base_or_proxy_route_pass"),
        "route_class_counts": state.get("route_class_counts") or {},
        "endpoint_statuses": state.get("endpoint_statuses") or {},
    }


def _active_set_dataapi_disable_source_base_overrides(args: argparse.Namespace) -> bool:
    requested_disable = bool(getattr(args, "active_set_dataapi_poller_disable_source_base_overrides", True))
    if not requested_disable:
        return False
    state = load_json(getattr(args, "source_route_state", ""), default={})
    state = state if isinstance(state, dict) else {}
    if (
        source_route_allows_live_execution(state)
        and source_route_live_operator_approval(state)
        and bool(state.get("measured_base_or_proxy_route_pass"))
    ):
        return False
    return True


def _live_price_band_decision_snapshot(
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    since: str,
    max_price: float,
    min_resolved: int,
    min_fill_rate_pct: float,
    target_wallet: str = "",
) -> dict[str, Any]:
    report = build_fill_quality_report(
        ledger if isinstance(ledger, dict) else {},
        resolutions if isinstance(resolutions, dict) else {},
        price_band_decision_since=str(since or ""),
        price_band_decision_max_price=float(max_price),
        price_band_decision_min_resolved=int(min_resolved),
        price_band_decision_min_fill_rate_pct=float(min_fill_rate_pct),
        price_band_decision_wallet=str(target_wallet or ""),
    )
    window = report.get("price_band_decision_window")
    window = window if isinstance(window, dict) else {}
    return {
        "flow_stage": "LIVE",
        "source": "scripts/report_fill_quality.py",
        "argv_equivalent": [
            "python3",
            "scripts/report_fill_quality.py",
            "--price-band-decision-since",
            str(since or ""),
            "--price-band-decision-max-price",
            str(float(max_price)),
            "--price-band-decision-min-resolved",
            str(int(min_resolved)),
            "--price-band-decision-min-fill-rate-pct",
            str(float(min_fill_rate_pct)),
            "--price-band-decision-wallet",
            str(target_wallet or ""),
        ],
        "updated_at": report.get("updated_at"),
        "ledger_orders": int(report.get("orders") or 0),
        "ledger_filled": int(report.get("filled") or 0),
        "ledger_rejected": int(report.get("rejected") or 0),
        "price_band_decision_window": window,
    }


def _load_live_price_band_decision(args: argparse.Namespace, *, target_wallet: str = "") -> dict[str, Any]:
    since = str(getattr(args, "price_band_decision_since", "") or "")
    if not since:
        return {
            "flow_stage": "LIVE",
            "source": "scripts/report_fill_quality.py",
            "enabled": False,
            "status": "SKIPPED",
            "next_action": "set --price-band-decision-since to persist the live price-band decision window",
        }
    return _live_price_band_decision_snapshot(
        load_json(args.live_ledger_state, default={}),
        load_resolutions(args.resolutions),
        since=since,
        max_price=float(getattr(args, "price_band_decision_max_price", 0.50)),
        min_resolved=int(getattr(args, "price_band_decision_min_resolved", 10)),
        min_fill_rate_pct=float(getattr(args, "price_band_decision_min_fill_rate_pct", 40.0)),
        target_wallet=str(target_wallet or ""),
    )


def _maybe_refresh_live_price_band_decision(
    args: argparse.Namespace,
    cache: dict[str, Any],
    *,
    now_ts: float,
    target_wallet: str = "",
) -> dict[str, Any]:
    refresh_s = max(1.0, float(getattr(args, "price_band_decision_refresh_s", 30.0)))
    last_checked = float(cache.get("checked_at_ts") or 0.0)
    if cache.get("snapshot") and now_ts - last_checked < refresh_s:
        return cache["snapshot"] if isinstance(cache.get("snapshot"), dict) else {}

    ledger_path = ROOT / args.live_ledger_state if not Path(args.live_ledger_state).is_absolute() else Path(args.live_ledger_state)
    try:
        ledger_mtime = ledger_path.stat().st_mtime
    except OSError as exc:
        snapshot = {
            "flow_stage": "LIVE",
            "source": "scripts/report_fill_quality.py",
            "status": "ERROR",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "ledger": str(ledger_path),
            "next_action": "repair live ledger path or permissions so guard can persist the price-band decision window",
        }
        cache.update({"checked_at_ts": now_ts, "ledger_mtime": None, "snapshot": snapshot})
        return snapshot

    if cache.get("snapshot") and cache.get("ledger_mtime") == ledger_mtime:
        cache["checked_at_ts"] = now_ts
        return cache["snapshot"] if isinstance(cache.get("snapshot"), dict) else {}

    try:
        snapshot = _load_live_price_band_decision(args, target_wallet=target_wallet)
    except Exception as exc:  # pragma: no cover - live evidence must survive report failures
        snapshot = {
            "flow_stage": "LIVE",
            "source": "scripts/report_fill_quality.py",
            "status": "ERROR",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "ledger": str(ledger_path),
            "next_action": "fix report_fill_quality inputs; live guard continues but price-band decision evidence is stale",
        }
    cache.update({"checked_at_ts": now_ts, "ledger_mtime": ledger_mtime, "snapshot": snapshot})
    return snapshot


def _is_live_primary_candidate(candidate: dict[str, Any], *, source_wallet_pin: str = "") -> bool:
    source_wallet = _candidate_source_wallet(candidate)
    if str(candidate.get("candidate_type") or "") != "SINGLE_WALLET":
        return False
    if str(candidate.get("status") or "") != "PASS":
        return False
    if not source_wallet:
        return False
    return not source_wallet_pin or source_wallet == source_wallet_pin


def _candidate_pass_gate(
    candidate: dict[str, Any],
    *,
    source_wallet_pin: str = "",
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    source_wallet = _candidate_source_wallet(candidate) if candidate else ""
    candidate_type = str(candidate.get("candidate_type") or "") if candidate else ""
    status = str(candidate.get("status") or "") if candidate else ""
    failed_checks: list[str] = []
    if not candidate:
        failed_checks.append("candidate_present")
    if candidate and candidate_type != "SINGLE_WALLET":
        failed_checks.append("candidate_type_single_wallet")
    if candidate and not _candidate_status_live_admissible(candidate):
        failed_checks.append("status_live_admissible")
    if candidate and not source_wallet:
        failed_checks.append("source_wallet_present")
    if candidate and source_wallet_pin and source_wallet != source_wallet_pin:
        failed_checks.append("source_wallet_pin_match")
    live_protection = (
        _candidate_live_protection_gate(candidate, now=now)
        if candidate
        and candidate_type == "SINGLE_WALLET"
        and source_wallet
        else {}
    )
    failed_checks.extend(str(item) for item in live_protection.get("failed_checks") or [] if str(item))
    return {
        "passed": not failed_checks,
        "failed_checks": failed_checks,
        "candidate_id": str(candidate.get("candidate_id") or "") if candidate else "",
        "candidate_type": candidate_type,
        "status": status,
        "source_wallet": source_wallet,
        "source_wallet_pin": str(source_wallet_pin or "").lower(),
        "live_protection_gate": live_protection,
    }


def _pass_gate_blockers(pass_gate: dict[str, Any]) -> list[str]:
    failed = {str(item) for item in pass_gate.get("failed_checks") or [] if str(item)}
    blockers: list[str] = []
    if "candidate_type_single_wallet" in failed:
        blockers.append("live_guard_candidate_not_single_wallet")
    if "status_live_admissible" in failed:
        blockers.append("live_guard_candidate_not_pass")
    if "source_wallet_present" in failed:
        blockers.append("runtime_admission_source_wallet_missing")
    if "source_wallet_pin_match" in failed:
        blockers.append("live_guard_source_wallet_pin_mismatch")
    if "shared_live_gate_disabled_or_demoted" in failed:
        blockers.append("live_guard_candidate_disabled_or_demoted")
    if "shared_live_gate_temporal_slice" in failed:
        blockers.append("live_guard_candidate_temporal_slice_excluded")
    if "shared_live_gate_external_liveness" in failed:
        blockers.append("live_guard_candidate_external_liveness_failed")
    return blockers


def _candidate_attempt_summary(
    candidate: dict[str, Any],
    *,
    source_wallet_pin: str,
    policy_id_pin: str,
    origin: str,
    blockers: list[str],
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    pass_gate = _candidate_pass_gate(candidate, source_wallet_pin=source_wallet_pin, now=now)
    return {
        "origin": origin,
        "candidate_id": str(candidate.get("candidate_id") or "") if candidate else "",
        "source_wallet": _candidate_source_wallet(candidate) if candidate else "",
        "policy_id": _candidate_policy_id(candidate) if candidate else str(policy_id_pin or ""),
        "blockers": sorted(set(blockers)),
        "pass_gate": pass_gate,
    }


def _active_set_member_candidate_pins(member: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(member.get("candidate_id") or ""),
        str(member.get("source_wallet") or member.get("wallet") or "").lower(),
        str(member.get("policy_id") or ""),
    )


def _resolve_candidate_for_pins(
    payload: dict[str, Any],
    args: argparse.Namespace,
    *,
    candidate_id_pin: str,
    source_wallet_pin: str,
    policy_id_pin: str,
    primary_live_candidate: dict[str, Any],
) -> tuple[dict[str, Any], str, list[str]]:
    selection_blockers: list[str] = []
    source_wallet_pin = str(source_wallet_pin or "").lower()
    if candidate_id_pin:
        selection_reason = str(getattr(args, "active_set_selection_reason", "") or "")
        prefer_runtime_selected_member = selection_reason in {
            "auto_degrade_latest_admission_priority",
            "runtime_member_selection_pin",
        }
        candidate = (
            _active_set_runtime_member_candidate(
                args,
                candidate_id_pin=candidate_id_pin,
                source_wallet_pin=source_wallet_pin
                or str(primary_live_candidate.get("source_wallet") or "").lower(),
                policy_id_pin=policy_id_pin,
            )
            if prefer_runtime_selected_member
            else {}
        )
        if not candidate:
            candidate = _selected_candidate(payload, override_id=candidate_id_pin)
        if str(candidate.get("candidate_id") or "") != candidate_id_pin:
            alias_candidate = _find_mission_alias_candidate(
                payload,
                source_wallet_pin=source_wallet_pin
                or str(primary_live_candidate.get("source_wallet") or "").lower(),
                policy_id_pin=policy_id_pin,
            )
            if alias_candidate:
                candidate = alias_candidate
            else:
                mission_candidate = _live_execution_mission_active_member_candidate(
                    candidate_id_pin=candidate_id_pin,
                    source_wallet_pin=source_wallet_pin
                    or str(primary_live_candidate.get("source_wallet") or "").lower(),
                    policy_id_pin=policy_id_pin,
                )
                if mission_candidate:
                    candidate = mission_candidate
                else:
                    rotation_candidate = _promotion_rotation_runtime_candidate(
                        str(getattr(args, "promotion_rotation_state", DEFAULT_PROMOTION_ROTATION_STATE) or ""),
                        candidate_id_pin=candidate_id_pin,
                        source_wallet_pin=source_wallet_pin
                        or str(primary_live_candidate.get("source_wallet") or "").lower(),
                        policy_id_pin=policy_id_pin,
                    )
                    if rotation_candidate:
                        candidate = rotation_candidate
                    else:
                        runtime_candidate = _active_set_runtime_member_candidate(
                            args,
                            candidate_id_pin=candidate_id_pin,
                            source_wallet_pin=source_wallet_pin
                            or str(primary_live_candidate.get("source_wallet") or "").lower(),
                            policy_id_pin=policy_id_pin,
                        )
                        if runtime_candidate:
                            candidate = runtime_candidate
                        else:
                            selection_blockers.append("live_guard_pinned_candidate_missing")
                            candidate = {
                                "candidate_id": candidate_id_pin,
                                "candidate_type": "SINGLE_WALLET",
                                "status": "PINNED_CANDIDATE_MISSING_FROM_PROFIT_STATE",
                                "metadata": {
                                    "source_wallet": source_wallet_pin
                                    or str(primary_live_candidate.get("source_wallet") or "").lower()
                                },
                                "source_wallet": source_wallet_pin
                                or str(primary_live_candidate.get("source_wallet") or "").lower(),
                                "policy": {"policy_id": policy_id_pin},
                            }
    else:
        candidate = next(
            (
                row
                for row in _candidate_pool(payload)
                if _is_live_primary_candidate(row, source_wallet_pin=source_wallet_pin)
            ),
            {},
        )
        if not candidate:
            selection_blockers.append("live_guard_pass_single_wallet_candidate_missing")
            candidate = _selected_candidate(payload)

    candidate = _mission_runtime_candidate_policy(candidate)
    candidate = _apply_active_set_selected_member_policy(candidate, args)
    source_wallet = _candidate_source_wallet(candidate)
    gate_now = _parse_iso_datetime(getattr(args, "active_set_temporal_now_iso", "") or "")
    pass_gate = _candidate_pass_gate(candidate, source_wallet_pin=source_wallet_pin, now=gate_now)
    selection_blockers.extend(_pass_gate_blockers(pass_gate))
    return candidate, source_wallet, sorted(set(selection_blockers))


def _attach_candidate_selection_diagnostics(
    candidate: dict[str, Any],
    *,
    attempts: list[dict[str, Any]],
    fallthrough: dict[str, Any],
    source_wallet_pin: str,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    if not candidate:
        return candidate
    enriched = dict(candidate)
    enriched["guard_pass_gate"] = _candidate_pass_gate(enriched, source_wallet_pin=source_wallet_pin, now=now)
    enriched["guard_selection_attempts"] = attempts
    enriched["guard_fallthrough"] = fallthrough
    return enriched


def _candidate_pass_gate_diagnostics(candidate: dict[str, Any], blockers: list[str]) -> dict[str, Any]:
    pass_gate = candidate.get("guard_pass_gate") if isinstance(candidate.get("guard_pass_gate"), dict) else {}
    attempts = (
        candidate.get("guard_selection_attempts")
        if isinstance(candidate.get("guard_selection_attempts"), list)
        else []
    )
    fallthrough = (
        candidate.get("guard_fallthrough")
        if isinstance(candidate.get("guard_fallthrough"), dict)
        else {}
    )
    failed_check_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    first_passing_attempt: dict[str, Any] = {}
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        attempt_pass_gate = attempt.get("pass_gate") if isinstance(attempt.get("pass_gate"), dict) else {}
        failed_checks = [str(item) for item in attempt_pass_gate.get("failed_checks") or [] if str(item)]
        for check in failed_checks:
            failed_check_counts[check] = failed_check_counts.get(check, 0) + 1
        for blocker in [str(item) for item in attempt.get("blockers") or [] if str(item)]:
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
        if not first_passing_attempt and attempt_pass_gate.get("passed"):
            first_passing_attempt = {
                "origin": attempt.get("origin"),
                "candidate_id": attempt.get("candidate_id"),
                "source_wallet": attempt.get("source_wallet"),
                "policy_id": attempt.get("policy_id"),
            }
    return {
        "flow_stage": "LIVE",
        "selected_candidate_id": str(candidate.get("candidate_id") or "") if candidate else "",
        "selected_source_wallet": _candidate_source_wallet(candidate) if candidate else "",
        "selected_policy_id": _candidate_policy_id(candidate) if candidate else "",
        "selected_passed": bool(pass_gate.get("passed")),
        "selected_failed_checks": [str(item) for item in pass_gate.get("failed_checks") or [] if str(item)],
        "blockers": sorted({str(blocker) for blocker in blockers if str(blocker)}),
        "attempt_count": len([attempt for attempt in attempts if isinstance(attempt, dict)]),
        "failed_check_counts": dict(sorted(failed_check_counts.items())),
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "first_passing_attempt": first_passing_attempt,
        "fallthrough": {
            "enabled": bool(fallthrough.get("enabled")),
            "applied": bool(fallthrough.get("applied")),
            "reason": fallthrough.get("reason"),
            "replacement_candidate_id": fallthrough.get("replacement_candidate_id"),
            "attempted_candidates": fallthrough.get("attempted_candidates"),
        },
    }


def _fallthrough_active_set_candidate(
    payload: dict[str, Any],
    args: argparse.Namespace,
    *,
    current_candidate: dict[str, Any],
    current_source_wallet: str,
    current_blockers: list[str],
    current_attempts: list[dict[str, Any]],
    primary_live_candidate: dict[str, Any],
) -> tuple[dict[str, Any], str, list[str], list[dict[str, Any]], dict[str, Any]]:
    gate_now = _parse_iso_datetime(getattr(args, "active_set_temporal_now_iso", "") or "")
    members = getattr(args, "active_set_fallthrough_members", [])
    if not isinstance(members, list) or not members:
        return current_candidate, current_source_wallet, current_blockers, current_attempts, {
            "enabled": False,
            "applied": False,
            "reason": "active_set_fallthrough_members_missing",
        }
    if not current_blockers:
        return current_candidate, current_source_wallet, current_blockers, current_attempts, {
            "enabled": True,
            "applied": False,
            "reason": "selected_candidate_passed",
        }
    attempted_keys = {
        (
            str(current_candidate.get("candidate_id") or ""),
            str(current_source_wallet or "").lower(),
            _candidate_policy_id(current_candidate),
        )
    }
    attempts = list(current_attempts)
    for member in members:
        if not isinstance(member, dict):
            continue
        candidate_id_pin, source_wallet_pin, policy_id_pin = _active_set_member_candidate_pins(member)
        key = (candidate_id_pin, source_wallet_pin, policy_id_pin)
        if key in attempted_keys:
            continue
        attempted_keys.add(key)
        candidate, source_wallet, blockers = _resolve_candidate_for_pins(
            payload,
            args,
            candidate_id_pin=candidate_id_pin,
            source_wallet_pin=source_wallet_pin,
            policy_id_pin=policy_id_pin,
            primary_live_candidate=primary_live_candidate,
        )
        attempts.append(
            _candidate_attempt_summary(
                candidate,
                source_wallet_pin=source_wallet_pin,
                policy_id_pin=policy_id_pin,
                origin="active_set_fallthrough",
                blockers=blockers,
                now=gate_now,
            )
        )
        if not blockers:
            return candidate, source_wallet, blockers, attempts, {
                "enabled": True,
                "applied": True,
                "reason": "selected_candidate_failed_pass_gate",
                "selected_candidate_id": str(current_candidate.get("candidate_id") or ""),
                "selected_source_wallet": str(current_source_wallet or "").lower(),
                "replacement_candidate_id": str(candidate.get("candidate_id") or ""),
                "replacement_source_wallet": str(source_wallet or "").lower(),
                "attempted_candidates": len(attempts),
            }
    return current_candidate, current_source_wallet, current_blockers, attempts, {
        "enabled": True,
        "applied": False,
        "reason": "no_active_set_member_passed",
        "attempted_candidates": len(attempts),
    }


def _load_candidate(args: argparse.Namespace) -> tuple[dict[str, Any], str, list[str]]:
    if bool(getattr(args, "active_set_empty_until_replacement", False)):
        return {}, "", ["active_set_empty_until_replacement"]
    if bool(getattr(args, "active_set_executable_roster_empty", False)):
        reason = str(getattr(args, "active_set_executable_roster_empty_reason", "") or "")
        return {}, "", [reason or "active_set_executable_roster_empty"]

    payload = load_json(args.profit_state, default={})
    payload = payload if isinstance(payload, dict) else {}
    candidate_id_pin = str(getattr(args, "candidate_id", "") or "")
    source_wallet_pin = str(getattr(args, "source_wallet", "") or "").lower()
    primary_live_candidate = _primary_live_candidate_contract()
    policy_id_pin = str(getattr(args, "policy_id", "") or primary_live_candidate.get("policy_id") or "")
    candidate, source_wallet, selection_blockers = _resolve_candidate_for_pins(
        payload,
        args,
        candidate_id_pin=candidate_id_pin,
        source_wallet_pin=source_wallet_pin,
        policy_id_pin=policy_id_pin,
        primary_live_candidate=primary_live_candidate,
    )
    attempts = [
        _candidate_attempt_summary(
            candidate,
            source_wallet_pin=source_wallet_pin,
            policy_id_pin=policy_id_pin,
            origin="selected_member",
            blockers=selection_blockers,
            now=_parse_iso_datetime(getattr(args, "active_set_temporal_now_iso", "") or ""),
        )
    ]
    candidate, source_wallet, selection_blockers, attempts, fallthrough = _fallthrough_active_set_candidate(
        payload,
        args,
        current_candidate=candidate,
        current_source_wallet=source_wallet,
        current_blockers=selection_blockers,
        current_attempts=attempts,
        primary_live_candidate=primary_live_candidate,
    )
    candidate = _attach_candidate_selection_diagnostics(
        candidate,
        attempts=attempts,
        fallthrough=fallthrough,
        source_wallet_pin=source_wallet,
        now=_parse_iso_datetime(getattr(args, "active_set_temporal_now_iso", "") or ""),
    )
    return candidate, source_wallet, sorted(set(selection_blockers))


def _pipeline_command(args: argparse.Namespace, *, source_wallet: str) -> list[str]:
    wallet_name = f"live_primary_{source_wallet[-8:]}" if source_wallet else "live_primary"
    if str(getattr(args, "rtds_jsonl", "") or ""):
        return [
            sys.executable,
            "scripts/merge_rtds_wallet_events.py",
            "--rtds-jsonl",
            str(args.rtds_jsonl),
            "--source-wallet",
            source_wallet,
            "--wallet-name",
            wallet_name,
            "--history-state",
            args.history_state,
            "--history-window-index",
            str(getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")),
            "--wallet-event-log",
            args.wallet_event_log,
            "--scan-limit",
            str(int(args.rtds_scan_limit)),
            "--max-new-events",
            str(int(args.rtds_max_new_events)),
            "--tail-bytes",
            str(int(getattr(args, "rtds_tail_bytes", 32 * 1024 * 1024))),
            "--cold-tail-bytes",
            str(_rtds_tail_backfill_bytes(args)),
            "--offset-state",
            _rtds_offset_state(args, source_wallet=source_wallet),
            "--watermark-state",
            str(getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")),
            "--history-retain-events",
            str(_history_retain_events(args)),
            "--history-retain-copy-intents",
            str(_history_retain_copy_intents(args)),
        ]
    return [
        sys.executable,
        "scripts/run_wallet_copy_pipeline.py",
        "--wallet",
        source_wallet,
        "--wallet-name",
        wallet_name,
        "--limit",
        str(int(args.pipeline_limit)),
        "--pages",
        str(int(args.pipeline_pages)),
        "--include-activity" if bool(args.pipeline_include_activity) else "--no-include-activity",
        "--parallel-data-api-sources",
        "--data-api-timeout-s",
        str(float(args.pipeline_data_api_timeout_s)),
        "--data-api-retries",
        str(int(args.pipeline_data_api_retries)),
        "--data-api-trade-query-keys",
        str(args.pipeline_data_api_trade_query_keys),
        "--merge-history-state",
        "--history-state",
        args.history_state,
        "--history-window-index",
        str(getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")),
        "--history-retain-events",
        str(_history_retain_events(args)),
        "--history-retain-copy-intents",
        str(_history_retain_copy_intents(args)),
        "--wallet-event-log",
        args.wallet_event_log,
        "--paper-state",
        args.paper_state,
        "--paper-event-log",
        args.paper_event_log,
        "--wallet-fraction",
        str(float(args.wallet_fraction)),
        "--max-order-usd",
        str(float(args.max_order_usd)),
        "--max-event-age-s",
        str(float(args.max_event_age_s)),
        "--policy-id",
        "live_guard_history_refresh",
    ]


def _live_command(args: argparse.Namespace, *, candidate_id: str) -> list[str]:
    argv = [
        sys.executable,
        "scripts/run_wallet_copy_live_execution.py",
        "--profit-state",
        args.profit_state,
        "--promotion-rotation-state",
        str(getattr(args, "promotion_rotation_state", DEFAULT_PROMOTION_ROTATION_STATE)),
        "--history-state",
        args.history_state,
        "--history-window-index",
        str(getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")),
        "--rtds-watermark-state",
        str(getattr(args, "rtds_watermark_state", "data/research/wallet_copy_rtds_observation_watermarks.json")),
        "--rtds-signal-watermark-state",
        str(getattr(args, "rtds_signal_watermark_state", DEFAULT_RTDS_SIGNAL_WATERMARK_STATE)),
        "--state",
        args.live_arm_state,
        "--live-ledger-state",
        args.live_ledger_state,
        "--live-ledger-event-log",
        args.live_ledger_event_log,
        "--operator-approval-id",
        args.operator_approval_id,
        "--candidate-id",
        candidate_id,
        "--selected-candidate-override-state",
        str(getattr(args, "selected_candidate_override_state", "") or ""),
        "--max-event-age-s",
        str(float(args.max_event_age_s)),
        "--live-build-max-observed-age-s",
        str(_live_build_max_observed_age_s(args)),
        "--max-intents",
        str(int(args.max_intents)),
        "--min-live-order-usd",
        str(float(args.min_live_order_usd)),
        "--alpha-decay-report",
        str(args.alpha_decay_report),
        "--toxicity-denylist-config",
        str(getattr(args, "toxicity_denylist_config", "configs/wallet_copy/toxicity_denylist.json")),
        "--max-drift-buffer-price",
        str(float(args.max_drift_buffer_price)),
        "--copy-model",
        str(getattr(args, "copy_model", "inventory") or "inventory"),
        "--inventory-late-window-stop-s",
        str(float(getattr(args, "inventory_late_window_stop_s", 60.0))),
        "--inventory-max-converge-orders-per-window",
        str(int(getattr(args, "inventory_max_converge_orders_per_window", 6))),
        "--inventory-best-ask-timeout-s",
        str(float(getattr(args, "inventory_best_ask_timeout_s", 1.0))),
        "--inventory-future-window-lookahead-s",
        str(float(getattr(args, "inventory_future_window_lookahead_s", INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S))),
        "--drip-min-tranche-usd",
        str(float(getattr(args, "drip_min_tranche_usd", 1.0))),
        "--drip-max-tranche-usd",
        str(float(getattr(args, "drip_max_tranche_usd", 2.5))),
        "--drip-max-tranches-per-window",
        str(int(getattr(args, "drip_max_tranches_per_window", 12))),
        "--per-window-fill-cap",
        str(int(getattr(args, "per_window_fill_cap", 1))),
        "--wallet-copy-max-buy-price",
        str(float(getattr(args, "price_band_decision_max_price", 0.50) or 0.0)),
        "--wallet-copy-min-buy-price",
        str(float(getattr(args, "price_band_decision_min_price", 0.25) or 0.0)),
        "--profit-latency-window-time-suppress-gte-s",
        str(
            float(
                getattr(
                    args,
                    "profit_latency_window_time_suppress_gte_s",
                    PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S,
                )
            )
        ),
        "--profit-latency-signal-age-suppress-gte-s",
        str(
            float(
                getattr(
                    args,
                    "profit_latency_signal_age_suppress_gte_s",
                    PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S,
                )
            )
        ),
        "--allow-no-fresh-intents",
    ]
    argv.append("--enable-drift-buffer" if bool(args.enable_drift_buffer) else "--no-enable-drift-buffer")
    argv.append("--enable-maker-fallback" if bool(args.enable_maker_fallback) else "--no-enable-maker-fallback")
    if args.execute_live:
        argv.append("--execute-live")
    if args.explicit_live_operator_go:
        argv.append("--explicit-live-operator-go")
    if args.live_orders_allowed:
        argv.append("--live-orders-allowed")
    return argv


def _subset(mapping: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: mapping.get(key) for key in keys if key in mapping}


def _operator_live_authority_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    """Return launch authority that candidate-cycle outcomes cannot revoke."""

    approval_id = str(getattr(args, "operator_approval_id", "") or "")
    active = bool(
        getattr(args, "execute_live", False)
        and getattr(args, "live_orders_allowed", False)
        and getattr(args, "explicit_live_operator_go", False)
        and approval_id
    )
    return {
        "active": active,
        "operator_approval_id": approval_id or None,
        "execute_live_launch_flag": bool(getattr(args, "execute_live", False)),
        "live_orders_allowed_launch_flag": bool(getattr(args, "live_orders_allowed", False)),
        "explicit_live_operator_go_launch_flag": bool(
            getattr(args, "explicit_live_operator_go", False)
        ),
        "cycle_invariant": True,
        "source": "guard_launch_identity",
    }


def _compact_window_participation_for_event(participation: dict[str, Any]) -> dict[str, Any]:
    rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    rollups = participation.get("window_rollups") if isinstance(participation.get("window_rollups"), list) else []
    recent_rollups = (
        participation.get("recent_window_rollups")
        if isinstance(participation.get("recent_window_rollups"), list)
        else rollups
    )
    out = _subset(
        participation,
        (
            "schema_version",
            "flow_stage",
            "set_generation_id",
            "active_windows",
            "missed_active_windows",
            "consecutive_missed_active_windows",
            "incident_triggered",
            "incident_threshold_windows",
            "raw_incident_triggered",
            "adjusted_participation",
            "dominant_skip_reason_counts",
            "current_cycle_rows",
            "retained_rows",
            "retention_target_hours",
            "retention_max_rows",
        ),
    )
    out["rows_count"] = len(rows) if rows else int(participation.get("rows_count") or 0)
    out["window_rollups_count"] = (
        len(rollups) if rollups else int(participation.get("window_rollups_count") or 0)
    )
    out["recent_window_rollups"] = recent_rollups[:PARTICIPATION_EVENT_ROLLUP_MAX_ROWS]
    out["recent_window_rollups_retention_max"] = PARTICIPATION_EVENT_ROLLUP_MAX_ROWS
    return out


def _compact_window_participation_for_guard_state(
    participation: dict[str, Any],
) -> dict[str, Any]:
    """Drop diagnostic-only row bulk before the hot guard-state rewrite.

    The retained rows are the rolling control input for MISS detection, so they
    stay in the guard state.  ``stale_drop_audit`` is different: its event and
    transaction samples are immutable diagnostics copied into every later
    cycle.  Keeping those samples on thousands of rows made the prior-state
    payload tens of MiB without changing any participation decision.  Preserve
    the scalar fields consumed by downstream reports and remove null fields;
    the current cycle's full audit remains available in live-execution output.
    """

    participation = participation if isinstance(participation, dict) else {}
    rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    compact_rows: list[dict[str, Any]] = []
    dropped_audit_bytes = 0
    dropped_null_fields = 0
    for source_row in rows:
        if not isinstance(source_row, dict):
            continue
        row: dict[str, Any] = {}
        for key, value in source_row.items():
            if value is None:
                dropped_null_fields += 1
                continue
            if key != "stale_drop_audit":
                row[key] = value
                continue
            audit = value if isinstance(value, dict) else {}
            if audit:
                try:
                    dropped_audit_bytes += len(
                        json.dumps(audit, sort_keys=True, separators=(",", ":"), default=str)
                    )
                except (TypeError, ValueError):
                    pass
            compact_audit = _subset(
                audit,
                (
                    "status",
                    "live_build_max_observed_age_s",
                    "stale_drop_events",
                    "total_events",
                ),
            )
            if compact_audit:
                row[key] = compact_audit
        compact_rows.append(row)

    out = dict(participation)
    out["rows"] = compact_rows
    out["guard_state_compaction"] = {
        "status": "COMPACT_DIAGNOSTIC_ONLY_ROW_BULK",
        "rule": (
            "retain every rolling participation control row and rollup; drop null row fields "
            "and stale-drop event/transaction samples from the hot prior-state rewrite"
        ),
        "rows_retained": len(compact_rows),
        "stale_drop_audit_serialized_bytes_before": dropped_audit_bytes,
        "null_fields_dropped": dropped_null_fields,
        "dropped_stale_drop_audit_keys": ["sample_events", "events_by_source_tx"],
    }
    return out


def _compact_routing_shadow_validation_for_guard(
    payload: dict[str, Any],
    *,
    state_path: str,
) -> dict[str, Any]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    fee_rows = (
        payload.get("fee_gated_measurement_rows")
        if isinstance(payload.get("fee_gated_measurement_rows"), list)
        else []
    )
    cycle_samples = payload.get("cycle_samples") if isinstance(payload.get("cycle_samples"), list) else []
    suppressed = (
        payload.get("routing_suppressed_rows")
        if isinstance(payload.get("routing_suppressed_rows"), list)
        else []
    )
    denied = payload.get("denied_signal_rows") if isinstance(payload.get("denied_signal_rows"), list) else []
    parity_conflicts = (
        payload.get("copyintent_parity_conflicts")
        if isinstance(payload.get("copyintent_parity_conflicts"), list)
        else []
    )
    out = _subset(
        payload,
        (
            "schema_version",
            "kind",
            "flow_stage",
            "generated_at",
            "status",
            "enabled",
            "routing_mode",
            "paper_only",
            "live_orders_allowed",
            "summary",
            "member_evidence",
            "rule",
            "single_submitter_invariant",
        ),
    )
    out.update(
        {
            "rows_count": len(rows),
            "fee_gated_measurement_rows_count": len(fee_rows),
            "cycle_samples_count": len(cycle_samples),
            "routing_suppressed_rows_count": len(suppressed),
            "denied_signal_rows_count": len(denied),
            "copyintent_parity_conflicts_count": len(parity_conflicts),
            "full_state_path": state_path,
            "guard_state_compaction": {
                "status": "COMPACT_FULL_STATE_IN_SEPARATE_JSON",
                "rule": (
                    "live guard state keeps routing shadow summary and counts only; "
                    "full validation rows stay in routing_shadow_validation_state"
                ),
                "dropped_lists": [
                    "rows",
                    "fee_gated_measurement_rows",
                    "cycle_samples",
                    "routing_suppressed_rows",
                    "denied_signal_rows",
                    "copyintent_parity_conflicts",
                ],
            },
        }
    )
    return out


def _compact_guard_shadow_lanes_for_guard(
    payload: dict[str, Any],
    *,
    state_path: str,
) -> dict[str, Any]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    lanes = payload.get("lanes") if isinstance(payload.get("lanes"), list) else []
    parked = payload.get("parked_lanes") if isinstance(payload.get("parked_lanes"), list) else []
    out = _subset(
        payload,
        (
            "schema_version",
            "kind",
            "flow_stage",
            "generated_at",
            "status",
            "enabled",
            "paper_only",
            "live_orders_allowed",
            "single_submitter_invariant",
            "route_order",
            "summary",
            "zero_live_assertion",
            "event_emitter",
            "profile",
            "cadence",
        ),
    )
    out.update(
        {
            "rows_count": len(rows) if rows else int(payload.get("rows_count") or 0),
            "lanes_count": len(lanes) if lanes else int(payload.get("lanes_count") or 0),
            "parked_lanes_count": len(parked) if parked else int(payload.get("parked_lanes_count") or 0),
            "full_state_path": state_path,
            "guard_state_compaction": {
                "status": "COMPACT_FULL_STATE_IN_SEPARATE_JSON",
                "rule": (
                    "live guard state keeps shadow lane summary and counts only; "
                    "full shadow rows stay in shadow_state"
                ),
                "dropped_lists": ["rows", "lanes", "parked_lanes"],
            },
        }
    )
    return out


def _shadow_lanes_row_profile(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    lanes = payload.get("lanes") if isinstance(payload.get("lanes"), list) else []
    parked = payload.get("parked_lanes") if isinstance(payload.get("parked_lanes"), list) else []
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return {
        "rows_count": len(rows) if rows else int(payload.get("rows_count") or 0),
        "lanes_count": len(lanes) if lanes else int(payload.get("lanes_count") or summary.get("lane_count") or 0),
        "parked_lanes_count": len(parked) if parked else int(payload.get("parked_lanes_count") or 0),
        "lane_intents_built": int(summary.get("intents_built") or 0),
        "lane_fresh_intents": int(summary.get("fresh_intents") or 0),
        "lane_guard_filter_passed": int(summary.get("guard_filter_passed") or 0),
        "lane_parity_passed": int(summary.get("parity_passed") or 0),
        "total_rows_held_across_shadow_lane_specs": len(rows)
        if rows
        else int(payload.get("rows_count") or summary.get("intents_built") or 0),
    }


def _compact_poller_for_event(poller: dict[str, Any]) -> dict[str, Any]:
    summary = poller.get("summary") if isinstance(poller.get("summary"), dict) else {}
    return {
        **_subset(
            poller,
            (
                "status",
                "kind",
                "flow_stage",
                "generated_at",
                "duration_s",
                "poller_label",
                "measure_only",
                "paper_only",
                "live_orders_allowed",
                "history_state",
                "watch_tier_wallets_config",
                "separate_file_contract",
            ),
        ),
        "source_wallet_count": len(poller.get("source_wallets") or []),
        "summary": _subset(
            summary,
            (
                "active_set_wallets",
                "watch_tier_wallets",
                "events_fetched",
                "poll_only_signals",
                "fresh_poll_only_signals",
                "duplicate_counts",
                "errored_wallets",
                "history_write_skipped",
                "source_base_overrides_disabled",
                "dataapi_first_seen_rows",
            ),
        ),
    }


def _compact_live_execution_for_event(live_execution: dict[str, Any]) -> dict[str, Any]:
    out = _subset(
        live_execution,
        (
            "schema_version",
            "kind",
            "flow_stage",
            "generated_at",
            "status",
            "candidate_id",
            "source_wallet",
            "policy_id",
            "orders_submitted",
            "fresh_candidate_intents",
            "new_live_candidate_intents",
            "dropped_already_live_submitted_intents",
            "paper_only",
            "live_orders_allowed",
            "copyintent_parity_violations",
            "selected_candidate",
            "profit_latency_suppression",
            "entry_price_band_gate",
            "toxicity_protection",
            "candidate_intent_summary",
            "drought_funnel",
            "guard_live_execution_runtime",
        ),
    )
    if isinstance(live_execution.get("window_participation"), dict):
        out["window_participation"] = _compact_window_participation_for_event(live_execution["window_participation"])
    return out


def _compact_guard_cycle_event(payload: dict[str, Any]) -> dict[str, Any]:
    active_set = payload.get("active_set") if isinstance(payload.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    guard_loop = payload.get("guard_loop_profile") if isinstance(payload.get("guard_loop_profile"), dict) else {}
    stage_timers = guard_loop.get("stage_timers") if isinstance(guard_loop.get("stage_timers"), list) else []
    live_execution = payload.get("live_execution") if isinstance(payload.get("live_execution"), dict) else {}
    participation = payload.get("window_participation") if isinstance(payload.get("window_participation"), dict) else {}
    return {
        "event": "wallet_copy_live_guard_cycle",
        **_subset(
            payload,
            (
                "schema_version",
                "kind",
                "generated_at",
                "status",
                "cycle_outcome",
                "cycle",
                "pid",
                "execute_live",
                "paper_only",
                "live_orders_allowed",
                "candidate_id",
                "source_wallet",
                "policy_id",
                "blockers",
                "participation_alerts",
                "recent_cycle_counts",
                "rtds_catchup_lag_s",
                "drought_funnel",
            ),
        ),
        "candidate": payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {},
        "active_set": {
            **_subset(
                active_set,
                (
                    "flow_stage",
                    "qualified_member_count",
                    "target_member_count_min",
                    "target_member_count_max",
                    "set_generation_id",
                    "status",
                    "refill_direction_id",
                ),
            ),
            "member_count": len(members),
            "current_member": next(
                (
                    {
                        "candidate_id": row.get("candidate_id"),
                        "source_wallet": row.get("source_wallet"),
                        "policy_id": row.get("policy_id"),
                    }
                    for row in members
                    if isinstance(row, dict) and row.get("is_current_cycle_member")
                ),
                {},
            ),
        },
        "pipeline": _subset(
            payload.get("pipeline") if isinstance(payload.get("pipeline"), dict) else {},
            ("returncode", "duration_s", "execution_mode", "stderr_tail"),
        ),
        "active_set_rtds_premerge": _subset(
            payload.get("active_set_rtds_premerge")
            if isinstance(payload.get("active_set_rtds_premerge"), dict)
            else {},
            (
                "status",
                "wallets_refreshed",
                "new_events",
                "new_matching_events",
                "retained_events",
                "retained_matching_rows",
                "max_lag_s",
                "max_rtds_catchup_lag_s",
                "paper_only",
                "live_orders_allowed",
            ),
        ),
        "event_triggered_cycle_scheduler": _subset(
            payload.get("event_triggered_cycle_scheduler")
            if isinstance(payload.get("event_triggered_cycle_scheduler"), dict)
            else {},
            (
                "flow_stage",
                "ruling_id",
                "enabled",
                "triggered",
                "status",
                "reason",
                "configured_sleep_s",
                "trigger_sleep_s",
                "sleep_s",
                "cycle",
                "cycle_started_at",
                "cycle_duration_s",
                "live_guard_can_trade",
                "scheduler_submits_orders",
                "single_submitter_change",
                "copyintent_parity_change",
                "cap_threshold_eligibility_change",
                "premerge_new_matching_events",
                "premerge_wallets",
                "source_event",
                "last_trigger",
            ),
        ),
        "mission_contract_hot_reload": _subset(
            payload.get("mission_contract_hot_reload")
            if isinstance(payload.get("mission_contract_hot_reload"), dict)
            else {},
            ("status", "changed", "reloaded", "reload_count", "schema_version", "mission_mtime_ns"),
        ),
        "active_set_dataapi_poller": _compact_poller_for_event(
            payload.get("active_set_dataapi_poller")
            if isinstance(payload.get("active_set_dataapi_poller"), dict)
            else {}
        ),
        "watch_tier_dataapi_poller": _compact_poller_for_event(
            payload.get("watch_tier_dataapi_poller")
            if isinstance(payload.get("watch_tier_dataapi_poller"), dict)
            else {}
        ),
        "live_execution": _compact_live_execution_for_event(live_execution),
        "window_participation": _compact_window_participation_for_event(participation),
        "guard_loop_profile": {
            **_subset(
                guard_loop,
                (
                    "flow_stage",
                    "status",
                    "target_median_iteration_lt_s",
                    "live_build_max_observed_age_s",
                    "cycle_started_wall_ts",
                    "cycle_started_at",
                    "slow_path_cadence",
                    "cycle_duration_s",
                    "total_s_before_state_write",
                    "rule",
                ),
            ),
            "stage_timers": stage_timers,
        },
        "guard_cycle_tail_attribution": _subset(
            payload.get("guard_cycle_tail_attribution")
            if isinstance(payload.get("guard_cycle_tail_attribution"), dict)
            else {},
            (
                "schema_version",
                "flow_stage",
                "status",
                "measurement_source",
                "measurement_basis",
                "supersedes_basis",
                "previous_write_state_file_bytes",
                "previous_write_state_file_mib",
                "measured_at",
                "measurement_lag_cycles",
                "cadence",
            ),
        ),
        "full_state_path": payload.get("_state_path"),
        "event_compaction": {
            "status": "COMPACT_FULL_STATE_IN_JSON",
            "rule": "cycle JSONL drops embedded rosters/state echoes; full guard state remains authoritative",
        },
    }


def _write_state(args: argparse.Namespace, payload: dict[str, Any]) -> dict[str, Any]:
    atomic_write_json(args.state, payload)
    try:
        state_size_bytes = Path(args.state).stat().st_size
    except OSError:
        state_size_bytes = 0
    _append_jsonl(args.event_log, _compact_guard_cycle_event(payload))
    return {
        "bytes": int(state_size_bytes),
        "mib": round(state_size_bytes / (1024 * 1024), 6),
        "measured_at": utc_now_iso(),
        "seeded": False,
    }


def _seed_previous_write_state_file_measurement(
    previous_state: dict[str, Any],
    state_path: str | Path,
) -> dict[str, Any]:
    previous_tail = (
        previous_state.get("guard_cycle_tail_attribution")
        if isinstance(previous_state.get("guard_cycle_tail_attribution"), dict)
        else {}
    )
    try:
        seeded_bytes = int(previous_tail.get("previous_write_state_file_bytes") or 0)
    except (TypeError, ValueError):
        seeded_bytes = 0
    if seeded_bytes > 0:
        return {
            "bytes": seeded_bytes,
            "mib": float(
                previous_tail.get("previous_write_state_file_mib")
                or round(seeded_bytes / (1024 * 1024), 6)
            ),
            "measured_at": str(previous_tail.get("measured_at") or utc_now_iso()),
            "seeded": True,
            "seed_status": "CARRIED_FORWARD_PREVIOUS_WRITE_STAT",
            "measurement_basis": str(
                previous_tail.get("measurement_basis") or "complete_state_file_after_write"
            ),
            "measurement_source": str(
                previous_tail.get("measurement_source") or "os.path.getsize_after_previous_write"
            ),
            "measurement_lag_cycles": previous_tail.get("measurement_lag_cycles"),
        }
    else:
        try:
            seeded_bytes = Path(state_path).stat().st_size
        except OSError:
            seeded_bytes = 0
    if seeded_bytes <= 0:
        return {}
    return {
        "bytes": seeded_bytes,
        "mib": round(seeded_bytes / (1024 * 1024), 6),
        "measured_at": utc_now_iso(),
        "seeded": True,
        "seed_status": "SEEDED_FROM_RESIDENT_STATE_FILE_STAT",
        "measurement_basis": "complete_state_file_at_process_start",
        "measurement_source": "resident_state_file_stat_at_process_start",
        "measurement_lag_cycles": None,
    }


def _guard_cycle_tail_attribution(
    previous_measurement: dict[str, Any],
    shadow_lanes_payload: dict[str, Any],
    cycle: int,
) -> dict[str, Any]:
    previous_write_bytes = int(previous_measurement.get("bytes") or 0)
    previous_write_mib = float(previous_measurement.get("mib") or 0.0)
    seeded = bool(previous_measurement.get("seeded")) and previous_write_bytes > 0
    status = "WAITING_FOR_PREVIOUS_WRITE_STAT"
    if previous_write_bytes > 0:
        status = (
            str(previous_measurement.get("seed_status"))
            if seeded and previous_measurement.get("seed_status")
            else "MEASURED_PREVIOUS_WRITE_STAT"
        )
    return {
        "schema_version": 1,
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "status": status,
        "rule": (
            "split guard_shadow_lanes from scheduler and state-payload work so memory spike "
            "context is not absorbed by the last shadow-lane mark"
        ),
        "shadow_lanes": _shadow_lanes_row_profile(shadow_lanes_payload),
        "previous_write_state_file_bytes": previous_write_bytes,
        "previous_write_state_file_mib": previous_write_mib,
        "measured_at": previous_measurement.get("measured_at"),
        "measurement_lag_cycles": (
            previous_measurement.get("measurement_lag_cycles")
            if seeded
            else 1
            if previous_write_bytes > 0
            else None
        ),
        "measurement_source": str(
            previous_measurement.get("measurement_source")
            or "os.path.getsize_after_previous_write"
        ),
        "measurement_basis": str(
            previous_measurement.get("measurement_basis")
            or "complete_state_file_after_write"
        ),
        "supersedes_basis": "partial_payload_serialization_pre_root_assignment",
        "cadence": {
            "name": "guard_state_persist_post_write_stat",
            "every_n_cycles": 1,
            "cycle_offset": 0,
            "executed_this_cycle": bool(previous_write_bytes > 0),
            "cycle": int(cycle),
            "reason": "post_write_stat_owned_by_guard_state_persist",
        },
    }


def _guard_code_identity(*, started_at_utc: str, pid: int | None = None, script_path: Path | None = None) -> dict[str, Any]:
    script_path = script_path or Path(__file__).resolve()
    git_head: str | None = None
    git_error: str | None = None
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            check=False,
            text=True,
            timeout=2.0,
        )
        if completed.returncode == 0:
            git_head = completed.stdout.strip() or None
        else:
            git_error = (completed.stderr or completed.stdout or "").strip() or f"returncode={completed.returncode}"
    except Exception as exc:  # pragma: no cover - identity must never disturb live guard.
        git_error = f"{type(exc).__name__}: {exc}"
    script_sha256: str | None = None
    script_error: str | None = None
    try:
        script_sha256 = hashlib.sha256(script_path.read_bytes()).hexdigest()
    except Exception as exc:  # pragma: no cover - identity must never disturb live guard.
        script_error = f"{type(exc).__name__}: {exc}"
    generation_files = []
    generation_digest = hashlib.sha256()
    generation_errors = []
    for generation_path in LIVE_GUARD_GENERATION_FILES:
        try:
            file_hash = hashlib.sha256(generation_path.read_bytes()).hexdigest()
            exists = True
        except FileNotFoundError:
            file_hash = None
            exists = False
        except Exception as exc:  # pragma: no cover - identity must never disturb live guard.
            file_hash = None
            exists = False
            generation_errors.append(f"{generation_path}: {type(exc).__name__}: {exc}")
        try:
            display_path = str(generation_path.relative_to(ROOT))
        except ValueError:
            display_path = str(generation_path)
        generation_files.append({"path": display_path, "exists": exists, "sha256": file_hash})
        generation_digest.update(display_path.encode("utf-8"))
        generation_digest.update(b"\0")
        generation_digest.update(str(file_hash or "MISSING").encode("utf-8"))
        generation_digest.update(b"\0")
    return {
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "status": "PASS" if git_head and script_sha256 else "PARTIAL",
        "audit_only": True,
        "control_flow_reads_allowed": False,
        "git_head_at_launch": git_head,
        "git_error": git_error,
        "script_path": str(script_path),
        "script_sha256": script_sha256,
        "script_error": script_error,
        "live_guard_generation_sha256": generation_digest.hexdigest(),
        "live_guard_generation_files": generation_files,
        "live_guard_generation_errors": generation_errors,
        "live_guard_generation_rule": "content hash over guard launcher, live guard, mission contract, and config generation",
        "started_at_utc": started_at_utc,
        "pid": int(pid if pid is not None else os.getpid()),
    }


def _recent_cycle_counts(args: argparse.Namespace, *, current_payload: dict[str, Any]) -> dict[str, Any]:
    path = ROOT / args.event_log if not Path(args.event_log).is_absolute() else Path(args.event_log)
    rows: deque[dict[str, Any]] = deque(maxlen=99)
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if "wallet_copy_live_guard_cycle" not in line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(row, dict) and row.get("event") == "wallet_copy_live_guard_cycle":
                        rows.append(row)
        except OSError:
            rows.clear()
    window = [*rows, current_payload][-100:]
    status_counts: dict[str, int] = {}
    outcome_counts: dict[str, int] = {}
    submitted_cycles = 0
    for row in window:
        status = str(row.get("status") or "unknown")
        outcome = str(row.get("cycle_outcome") or status or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        live_execution = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
        try:
            orders_submitted = int(live_execution.get("orders_submitted") or row.get("orders_submitted") or 0)
        except (TypeError, ValueError):
            orders_submitted = 0
        if orders_submitted > 0 or str(live_execution.get("status") or "") == "LIVE_EXECUTION_SUBMITTED":
            submitted_cycles += 1
    return {
        "flow_stage": "LIVE",
        "window_cycles": 100,
        "cycles_observed": len(window),
        "blocked_cycles": status_counts.get("LIVE_GUARD_BLOCKED", 0),
        "submitted_cycles": submitted_cycles,
        "running_cycles": status_counts.get("LIVE_GUARD_RUNNING", 0),
        "filtered_no_submit_cycles": outcome_counts.get("FILTERED_NO_SUBMIT", 0),
        "status_counts": dict(sorted(status_counts.items())),
        "cycle_outcome_counts": dict(sorted(outcome_counts.items())),
    }


def _stdout_cycle_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Compact stdout heartbeat; full machine state remains in the state file."""

    live_execution = payload.get("live_execution") if isinstance(payload.get("live_execution"), dict) else {}
    window_participation = (
        payload.get("window_participation") if isinstance(payload.get("window_participation"), dict) else {}
    )
    drought_funnel = payload.get("drought_funnel") if isinstance(payload.get("drought_funnel"), dict) else {}
    guard_loop = payload.get("guard_loop_profile") if isinstance(payload.get("guard_loop_profile"), dict) else {}

    def _stage_names(value: Any) -> list[str]:
        if isinstance(value, list):
            return [
                str(row.get("name") or row.get("stage") or "")
                for row in value
                if isinstance(row, dict) and (row.get("name") or row.get("stage"))
            ]
        if isinstance(value, dict):
            return [str(name) for name in value.keys()]
        return []

    stage_names = _stage_names(guard_loop.get("stage_timers"))
    after_persist_names = _stage_names(guard_loop.get("stage_timers_after_persist"))
    return {
        "schema_version": 1,
        "kind": "wallet_copy_live_guard_stdout_summary",
        "generated_at": payload.get("generated_at"),
        "status": payload.get("status"),
        "cycle_outcome": payload.get("cycle_outcome"),
        "cycle": payload.get("cycle"),
        "pid": payload.get("pid"),
        "candidate_id": payload.get("candidate_id"),
        "source_wallet": payload.get("source_wallet"),
        "policy_id": payload.get("policy_id"),
        "live_orders_allowed": bool(payload.get("live_orders_allowed")),
        "paper_only": bool(payload.get("paper_only")),
        "blockers": payload.get("blockers") or [],
        "participation_alerts": payload.get("participation_alerts") or [],
        "recent_cycle_counts": payload.get("recent_cycle_counts") or {},
        "window_participation": {
            "active_windows": int(window_participation.get("active_windows") or 0),
            "missed_active_windows": int(window_participation.get("missed_active_windows") or 0),
            "consecutive_missed_active_windows": int(
                window_participation.get("consecutive_missed_active_windows") or 0
            ),
            "incident_triggered": bool(window_participation.get("incident_triggered")),
            "dominant_skip_reason_counts": window_participation.get("dominant_skip_reason_counts") or {},
        },
        "live_execution": {
            "status": live_execution.get("status"),
            "orders_submitted": int(live_execution.get("orders_submitted") or 0),
            "fresh_candidate_intents": int(live_execution.get("fresh_candidate_intents") or 0),
            "new_live_candidate_intents": int(live_execution.get("new_live_candidate_intents") or 0),
        },
        "drought_funnel": {
            "diagnosis": drought_funnel.get("diagnosis"),
            "fresh_candidate_intents": int(drought_funnel.get("fresh_candidate_intents") or 0),
            "orders_submitted": int(drought_funnel.get("orders_submitted") or 0),
        },
        "guard_cycle_tail_attribution": payload.get("guard_cycle_tail_attribution") or {},
        "guard_loop_stage_markers": {
            "stage_timers_tail": stage_names[-8:],
            "stage_timers_after_persist_tail": after_persist_names[-8:],
            "has_guard_cycle_scheduler": "guard_cycle_scheduler" in stage_names,
            "has_guard_state_persist_post_write_stat": (
                payload.get("guard_cycle_tail_attribution", {}).get("status")
                in {"MEASURED_PREVIOUS_WRITE_STAT", "SEEDED_FROM_RESIDENT_STATE_FILE_STAT"}
            ),
            "has_guard_state_persist": "guard_state_persist" in after_persist_names,
        },
        "full_state_path": payload.get("_state_path"),
        "stdout_policy": "compact_summary_only_full_state_in_json",
    }


def _participation_row_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("source_wallet") or "").lower(),
        str(row.get("market_slug") or ""),
        str(row.get("condition_id") or ""),
        str(row.get("outcome") or ""),
    )


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


def _parse_iso_ts(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _iso_from_ts(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _event_triggered_cycle_window_start(row: dict[str, Any]) -> float | None:
    try:
        explicit = float(row.get("window_start_s") or 0.0)
    except (TypeError, ValueError):
        explicit = 0.0
    if explicit > 0:
        return explicit
    slug = str(row.get("market_slug") or row.get("event_slug") or "")
    match = re.search(r"btc-updown-5m-(\d+)$", slug)
    if not match:
        return None
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None


def _event_triggered_cycle_event_ts(row: dict[str, Any]) -> float | None:
    for key in ("observed_ts", "deduped_observed_ts", "event_ts", "timestamp"):
        try:
            value = float(row.get(key) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    for key in ("deduped_observed_ts", "timestamp"):
        try:
            value = float(raw.get(key) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return None


def _event_triggered_cycle_premerge_wallets(active_set_rtds_premerge: dict[str, Any]) -> list[str]:
    rows = active_set_rtds_premerge.get("rows") if isinstance(active_set_rtds_premerge.get("rows"), list) else []
    wallets: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            new_matching = int(row.get("new_matching_events") or 0)
        except (TypeError, ValueError):
            new_matching = 0
        if new_matching <= 0:
            continue
        wallet = str(row.get("source_wallet") or "").strip().lower()
        if wallet:
            wallets.append(wallet)
    selected_wallet = str(active_set_rtds_premerge.get("selected_wallet") or "").strip().lower()
    try:
        total_new = int(active_set_rtds_premerge.get("new_matching_events") or 0)
    except (TypeError, ValueError):
        total_new = 0
    if total_new > 0 and selected_wallet and not wallets:
        wallets.append(selected_wallet)
    return sorted(set(wallets))


def _event_triggered_cycle_is_btc5m_buy(row: dict[str, Any]) -> bool:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    action = str(row.get("action") or row.get("side") or raw.get("side") or "").strip().upper()
    asset = str(row.get("asset") or raw.get("asset") or "").strip().upper()
    duration = str(row.get("duration") or "").strip().lower()
    slug = str(row.get("market_slug") or row.get("event_slug") or raw.get("slug") or raw.get("eventSlug") or "")
    return action == "BUY" and asset == "BTC" and (duration == "5m" or slug.startswith("btc-updown-5m-"))


def _event_triggered_cycle_latest_event(
    history: dict[str, Any],
    *,
    source_wallets: set[str],
    now_ts: float,
    max_signal_age_s: float,
) -> dict[str, Any]:
    events = history.get("events") if isinstance(history.get("events"), list) else []
    best: dict[str, Any] = {}
    best_observed_ts = -1.0
    for row in events:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        if wallet not in source_wallets:
            continue
        if not _event_triggered_cycle_is_btc5m_buy(row):
            continue
        observed_ts = _event_triggered_cycle_event_ts(row)
        if observed_ts is None:
            continue
        signal_age_s = max(0.0, float(now_ts) - observed_ts)
        if max_signal_age_s > 0 and signal_age_s > max_signal_age_s:
            continue
        window_start_s = _event_triggered_cycle_window_start(row)
        if window_start_s is None:
            continue
        window_close_ts = window_start_s + 300.0
        if window_close_ts <= now_ts:
            continue
        if observed_ts > best_observed_ts:
            best_observed_ts = observed_ts
            best = {
                "event_id": row.get("event_id"),
                "source_wallet": wallet,
                "market_slug": row.get("market_slug") or row.get("event_slug"),
                "outcome": row.get("outcome"),
                "price": row.get("price"),
                "observed_ts": round(observed_ts, 6),
                "observed_at": _iso_from_ts(observed_ts),
                "event_ts": row.get("event_ts"),
                "window_start_s": round(window_start_s, 6),
                "window_close_ts": round(window_close_ts, 6),
                "window_close_at": _iso_from_ts(window_close_ts),
                "signal_age_s": round(signal_age_s, 6),
                "seconds_until_window_close": round(window_close_ts - now_ts, 6),
            }
    return best


def _event_triggered_cycle_scheduler_decision(
    args: argparse.Namespace,
    *,
    active_set_rtds_premerge: dict[str, Any],
    previous_state: dict[str, Any],
    blockers: list[str],
    cycle: int,
    cycle_started_wall_ts: float,
    cycle_duration_s: float,
    generated_at: str,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now_ts = float(time.time() if now_ts is None else now_ts)
    configured_sleep_s = max(0.0, float(getattr(args, "sleep_s", 0.0) or 0.0))
    trigger_sleep_s = max(0.0, float(getattr(args, "event_triggered_cycle_trigger_sleep_s", 0.0) or 0.0))
    previous_scheduler = (
        previous_state.get("event_triggered_cycle_scheduler")
        if isinstance(previous_state.get("event_triggered_cycle_scheduler"), dict)
        else {}
    )
    prior_last_trigger = (
        previous_scheduler.get("last_trigger") if isinstance(previous_scheduler.get("last_trigger"), dict) else {}
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "wallet_copy_event_triggered_cycle_scheduler",
        "flow_stage": "LIVE/PROMOTE",
        "ruling_id": "2026-07-17T12:30Z-fable-ruling21b-event-triggered-cycle-promotion",
        "generated_at": generated_at,
        "enabled": bool(getattr(args, "event_triggered_cycle_scheduler", True)),
        "preauthorized_by_ruling21b": True,
        "triggered": False,
        "status": "NO_TRIGGER",
        "reason": "",
        "configured_sleep_s": configured_sleep_s,
        "trigger_sleep_s": trigger_sleep_s,
        "sleep_s": configured_sleep_s,
        "cycle": int(cycle),
        "cycle_started_wall_ts": round(float(cycle_started_wall_ts), 6),
        "cycle_started_at": _iso_from_ts(float(cycle_started_wall_ts)),
        "cycle_duration_s": round(float(cycle_duration_s), 6),
        "evaluated_at_ts": round(now_ts, 6),
        "evaluated_at": _iso_from_ts(now_ts),
        "live_guard_can_trade": bool(getattr(args, "execute_live", False))
        and bool(getattr(args, "live_orders_allowed", False))
        and not blockers,
        "scheduler_submits_orders": False,
        "single_submitter_change": False,
        "copyintent_parity_change": False,
        "cap_threshold_eligibility_change": False,
        "submitter_invariant": (
            "scripts/run_wallet_copy_live_guard.py remains the sole live order submitter; "
            "scheduler only changes this same process sleep before the next guard cycle"
        ),
        "copyintent_parity_invariant": "CopyIntent construction remains owned by scripts/run_wallet_copy_live_execution.py",
        "premerge_new_matching_events": int(active_set_rtds_premerge.get("new_matching_events") or 0)
        if isinstance(active_set_rtds_premerge, dict)
        else 0,
        "premerge_status": active_set_rtds_premerge.get("status") if isinstance(active_set_rtds_premerge, dict) else None,
        "premerge_wallets": [],
        "source_event": {},
        "last_trigger": prior_last_trigger,
        "first_live_evidence_required": "event_id, cycle stamp, decide latency versus window close",
    }
    if not payload["enabled"]:
        payload["reason"] = "event_triggered_cycle_scheduler_disabled"
        return payload
    if not payload["live_guard_can_trade"]:
        payload["reason"] = "live_guard_not_tradeable" if not blockers else "guard_has_blockers"
        payload["blockers"] = list(blockers)
        return payload
    if int(payload["premerge_new_matching_events"] or 0) <= 0:
        payload["reason"] = "no_new_matching_events_from_premerge"
        return payload
    wallets = _event_triggered_cycle_premerge_wallets(active_set_rtds_premerge)
    payload["premerge_wallets"] = wallets
    if not wallets:
        payload["reason"] = "new_matching_events_without_wallet_attribution"
        return payload
    raw_max_signal_age = float(getattr(args, "event_triggered_cycle_max_signal_age_s", 0.0) or 0.0)
    max_signal_age_s = raw_max_signal_age if raw_max_signal_age > 0 else float(_live_build_max_observed_age_s(args))
    payload["max_signal_age_s"] = round(max_signal_age_s, 6)
    history = load_json(str(getattr(args, "history_state", "")), default={})
    history = history if isinstance(history, dict) else {}
    event = _event_triggered_cycle_latest_event(
        history,
        source_wallets=set(wallets),
        now_ts=now_ts,
        max_signal_age_s=max_signal_age_s,
    )
    if not event:
        payload["reason"] = "no_open_fresh_btc5m_buy_event_in_hot_history"
        return payload
    window_close_ts = float(event.get("window_close_ts") or 0.0)
    trigger = {
        "event_id": event.get("event_id"),
        "source_wallet": event.get("source_wallet"),
        "market_slug": event.get("market_slug"),
        "observed_at": event.get("observed_at"),
        "window_close_at": event.get("window_close_at"),
        "trigger_cycle": int(cycle),
        "trigger_cycle_started_at": payload["cycle_started_at"],
        "triggered_at": payload["evaluated_at"],
        "triggered_sleep_s": min(configured_sleep_s, trigger_sleep_s),
        "trigger_cycle_latency_vs_window_close_s": round(now_ts - window_close_ts, 6),
        "next_cycle_expected": int(cycle) + 1,
    }
    payload.update(
        {
            "triggered": True,
            "status": "TRIGGER_NEXT_GUARD_CYCLE",
            "reason": "fresh_in_window_btc5m_copy_event",
            "sleep_s": min(configured_sleep_s, trigger_sleep_s),
            "source_event": event,
            "last_trigger": trigger,
        }
    )
    return payload


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


def _participation_window_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("source_wallet") or "").lower(),
        str(row.get("market_slug") or row.get("condition_id") or ""),
    )


def _participation_window_rollups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rollups_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    child_rows_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = _participation_window_key(row)
        if not key[1]:
            continue
        child_rows_by_key.setdefault(key, []).append(row)
        rollup = rollups_by_key.setdefault(
            key,
            {
                "flow_stage": "LIVE",
                "source_wallet": key[0],
                "market_slug": key[1],
                "set_generation_ids": set(),
                "window_start_s": _participation_window_start(row),
                "condition_ids": set(),
                "outcomes": set(),
                "wallet_eligible_orders": 0,
                "our_submits": 0,
                "our_fills": 0,
                "our_attempts": 0,
                "first_seen_at": row.get("first_seen_at"),
                "last_seen_at": row.get("last_seen_at"),
                "dominant_skip_reason_counts": {},
            },
        )
        if row.get("condition_id"):
            rollup["condition_ids"].add(str(row.get("condition_id")))
        if row.get("set_generation_id"):
            rollup["set_generation_ids"].add(str(row.get("set_generation_id")))
        if row.get("outcome"):
            rollup["outcomes"].add(str(row.get("outcome")))
        rollup["wallet_eligible_orders"] += int(row.get("wallet_eligible_orders") or 0)
        rollup["our_submits"] += int(row.get("our_submits") or 0)
        rollup["our_fills"] += int(row.get("our_fills") or 0)
        rollup["our_attempts"] += int(row.get("our_attempts") or 0)
        if row.get("first_seen_at") and (
            not rollup.get("first_seen_at") or str(row.get("first_seen_at")) < str(rollup.get("first_seen_at"))
        ):
            rollup["first_seen_at"] = row.get("first_seen_at")
        if row.get("last_seen_at") and (
            not rollup.get("last_seen_at") or str(row.get("last_seen_at")) > str(rollup.get("last_seen_at"))
        ):
            rollup["last_seen_at"] = row.get("last_seen_at")
        reason = str(row.get("dominant_skip_reason") or "unknown")
        reason_counts = rollup["dominant_skip_reason_counts"]
        reason_counts[reason] = int(reason_counts.get(reason) or 0) + 1

    rollups: list[dict[str, Any]] = []
    for rollup in rollups_by_key.values():
        reason_counts = rollup["dominant_skip_reason_counts"]
        dominant_reason = max(reason_counts.items(), key=lambda item: (item[1], item[0]))[0] if reason_counts else "unknown"
        child_rows = child_rows_by_key.get(
            (str(rollup.get("source_wallet") or ""), str(rollup.get("market_slug") or "")),
            [],
        )
        pending_reasons: dict[str, int] = {}
        for row in child_rows:
            if row.get("miss_pending_market_lifecycle"):
                reason = str(row.get("miss_pending_market_lifecycle_reason") or "unknown")
                pending_reasons[reason] = pending_reasons.get(reason, 0) + 1
        out = {
            **rollup,
            "condition_ids": sorted(rollup["condition_ids"]),
            "set_generation_ids": sorted(rollup["set_generation_ids"]),
            "set_generation_id": sorted(rollup["set_generation_ids"])[-1]
            if rollup["set_generation_ids"]
            else "",
            "outcomes": sorted(rollup["outcomes"]),
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
        key=lambda row: (
            float(row.get("window_start_s") or 0.0),
            str(row.get("last_seen_at") or row.get("first_seen_at") or ""),
        ),
        reverse=True,
    )


def _merge_window_participation(
    previous: dict[str, Any] | None,
    current: dict[str, Any] | None,
    *,
    generated_at: str,
    set_generation_id: str,
    active_set_rtds_premerge: dict[str, Any] | None = None,
    max_rows: int = PARTICIPATION_RETENTION_MAX_ROWS,
) -> dict[str, Any]:
    profile_started = time.perf_counter()
    profile_checkpoint = profile_started
    profile_stages: list[dict[str, Any]] = []

    def mark_profile_stage(name: str) -> None:
        nonlocal profile_checkpoint
        now = time.perf_counter()
        profile_stages.append(
            {
                "name": name,
                "duration_s": round(now - profile_checkpoint, 6),
                "elapsed_s": round(now - profile_started, 6),
            }
        )
        profile_checkpoint = now

    previous = previous if isinstance(previous, dict) else {}
    current = current if isinstance(current, dict) else {}
    rows_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for row in previous.get("rows") or []:
        if not isinstance(row, dict):
            continue
        key = _participation_row_key(row)
        if not any(key):
            continue
        rows_by_key[key] = dict(row)
    mark_profile_stage("index_previous_rows")

    current_rows = [dict(row) for row in current.get("rows") or [] if isinstance(row, dict)]
    for row in current_rows:
        key = _participation_row_key(row)
        if not any(key):
            continue
        prior = rows_by_key.get(key, {})
        merged = {**prior, **row}
        merged.setdefault("flow_stage", "LIVE")
        merged["set_generation_id"] = set_generation_id
        merged["first_seen_at"] = prior.get("first_seen_at") or generated_at
        merged["last_seen_at"] = generated_at
        rows_by_key[key] = merged
    mark_profile_stage("merge_current_rows")

    rows = sorted(
        rows_by_key.values(),
        key=lambda row: (
            _participation_window_start(row),
            str(row.get("last_seen_at") or row.get("first_seen_at") or ""),
            str(row.get("outcome") or ""),
        ),
        reverse=True,
    )[: max(1, int(max_rows))]
    mark_profile_stage("sort_and_trim_rows")

    now_ts = _parse_iso_ts(generated_at) or time.time()
    premerge = active_set_rtds_premerge if isinstance(active_set_rtds_premerge, dict) else {}
    premerge_fresh_by_wallet: dict[str, float] = {}
    for row in premerge.get("rows") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").strip().lower()
        if not (wallet.startswith("0x") and len(wallet) == 42):
            continue
        try:
            watermark_ts = float(row.get("latest_observed_ts") or 0.0)
        except (TypeError, ValueError):
            watermark_ts = 0.0
        if watermark_ts <= 0:
            continue
        premerge_fresh_by_wallet[wallet] = max(watermark_ts, premerge_fresh_by_wallet.get(wallet, 0.0))
    mark_profile_stage("index_premerge_freshness")
    reason_counts: dict[str, int] = {}
    active_rows: list[dict[str, Any]] = []
    for row in rows:
        row_generation_id = str(row.get("set_generation_id") or "")
        row["current_set_generation"] = bool(row_generation_id == set_generation_id)
        eligible = int(row.get("wallet_eligible_orders") or 0)
        window_start_s = _participation_window_start(row)
        latest_observed_ts = row.get("latest_observed_ts")
        if window_start_s > 0 and latest_observed_ts is not None:
            try:
                observed_delta_s = round(float(latest_observed_ts) - window_start_s, 6)
                row["slug_observed_delta_s"] = observed_delta_s
                if row.get("observed_slug_epoch_delta_s") is None:
                    row["observed_slug_epoch_delta_s"] = observed_delta_s
            except (TypeError, ValueError):
                pass
        reason = str(row.get("dominant_skip_reason") or "unknown")
        if reason == "inventory_window_state_stale":
            wallet = str(row.get("source_wallet") or "").strip().lower()
            watermark_ts = premerge_fresh_by_wallet.get(wallet, 0.0)
            if watermark_ts > 0:
                try:
                    source_observed_ts = float(row.get("latest_observed_ts") or 0.0)
                except (TypeError, ValueError):
                    source_observed_ts = 0.0
                watermark_age_s = max(0.0, now_ts - watermark_ts)
                if watermark_age_s <= 30.0 and watermark_ts > source_observed_ts:
                    row["source_latest_observed_ts"] = row.get("latest_observed_ts")
                    row["source_latest_observed_age_s"] = (
                        round(max(0.0, now_ts - source_observed_ts), 6) if source_observed_ts > 0 else None
                    )
                    row["latest_observed_ts"] = round(watermark_ts, 6)
                    row["effective_latest_observed_ts"] = round(watermark_ts, 6)
                    row["latest_observed_age_s"] = round(watermark_age_s, 6)
                    row["freshness_watermark_ts"] = round(watermark_ts, 6)
                    row["freshness_confirmed_unchanged"] = True
                    row["dominant_skip_reason"] = "inventory_confirmed_unchanged_no_edge"
                    row["measured_skip_from_premerge_watermark"] = True
                    reason = "inventory_confirmed_unchanged_no_edge"
        row["missed_active_window"] = _participation_missed_active_window(row, now_ts=now_ts)
        annotate_participation_item(row)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if eligible > 0:
            active_rows.append(row)
    mark_profile_stage("annotate_retained_rows")

    window_rollups = _participation_window_rollups(rows)
    mark_profile_stage("build_window_rollups")
    pending_market_lifecycle_windows = sum(
        1
        for row in window_rollups
        if int(row.get("wallet_eligible_orders") or 0) > 0 and row.get("miss_pending_market_lifecycle")
    )
    active_windows = [
        row
        for row in window_rollups
        if int(row.get("wallet_eligible_orders") or 0) > 0
        and not row.get("miss_pending_market_lifecycle")
        and row.get("set_generation_id") == set_generation_id
    ]
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
    mark_profile_stage("summarize_adjusted_participation")
    raw_incident_triggered = consecutive_missed >= PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS
    total_duration_s = round(time.perf_counter() - profile_started, 6)
    top_stage = max(
        profile_stages,
        key=lambda row: float(row.get("duration_s") or 0.0),
        default={},
    )

    return {
        "enabled": bool(rows),
        "flow_stage": "LIVE",
        "schema_version": 2,
        "set_generation_id": set_generation_id,
        "rows": rows,
        "window_rollups": window_rollups[: max(1, int(max_rows))],
        "current_cycle_rows": len(current_rows),
        "retained_rows": len(rows),
        "retention_target_hours": PARTICIPATION_RETENTION_TARGET_HOURS,
        "retention_max_rows": int(max_rows),
        "active_windows": len(active_windows),
        "missed_active_windows": sum(1 for row in active_windows if row.get("missed_active_window")),
        "consecutive_missed_active_windows": consecutive_missed,
        "pending_market_lifecycle_windows": pending_market_lifecycle_windows,
        "incident_threshold_windows": PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
        "raw_incident_triggered": raw_incident_triggered,
        "incident_triggered": bool(adjusted_participation.get("adjusted_incident_triggered")),
        "adjusted_participation": adjusted_participation,
        "dominant_skip_reason_counts": dict(sorted(reason_counts.items())),
        "merge_profile": {
            "status": "MEASURED",
            "total_duration_s": total_duration_s,
            "retained_rows": len(rows),
            "window_rollups": len(window_rollups),
            "current_cycle_rows": len(current_rows),
            "top_stage": top_stage.get("name"),
            "top_stage_duration_s": top_stage.get("duration_s"),
            "stages": profile_stages,
        },
        "rule": (
            "raw_rolling_wallet_eligible_orders_gt_0_and_our_submits_eq_0_is_raw_miss;"
            " adjusted_skip_taxonomy_excludes_no_signal_and_counts_correct_skip_as_participation_equivalent;"
            " current_generation_incident_threshold_6"
        ),
    }


def _current_process_rss_gib(pid: int | None = None) -> float | None:
    target_pid = int(pid or os.getpid())
    if target_pid == os.getpid() and sys.platform == "darwin":
        class _TimeValue(ctypes.Structure):
            _fields_ = [
                ("seconds", ctypes.c_int),
                ("microseconds", ctypes.c_int),
            ]

        class _MachTaskBasicInfo(ctypes.Structure):
            _fields_ = [
                ("virtual_size", ctypes.c_uint64),
                ("resident_size", ctypes.c_uint64),
                ("resident_size_max", ctypes.c_uint64),
                ("user_time", _TimeValue),
                ("system_time", _TimeValue),
                ("policy", ctypes.c_int),
                ("suspend_count", ctypes.c_int),
            ]

        try:
            libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            libc.mach_task_self.restype = ctypes.c_uint
            libc.task_info.argtypes = [
                ctypes.c_uint,
                ctypes.c_int,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_uint),
            ]
            info = _MachTaskBasicInfo()
            count = ctypes.c_uint(
                ctypes.sizeof(info) // ctypes.sizeof(ctypes.c_uint)
            )
            return_code = libc.task_info(
                libc.mach_task_self(),
                20,  # MACH_TASK_BASIC_INFO
                ctypes.byref(info),
                ctypes.byref(count),
            )
            if return_code == 0:
                return round(float(info.resident_size) / float(1024**3), 6)
        except (AttributeError, OSError, TypeError, ValueError):
            pass
    if target_pid == os.getpid():
        statm = Path("/proc/self/statm")
        try:
            resident_pages = int(statm.read_text(encoding="utf-8").split()[1])
            return round(
                float(resident_pages * os.sysconf("SC_PAGE_SIZE"))
                / float(1024**3),
                6,
            )
        except (IndexError, OSError, TypeError, ValueError):
            pass
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(target_pid)],
            check=True,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
        return round(float(result.stdout.strip()) / (1024 * 1024), 6)
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def _active_set_snapshot(
    args: argparse.Namespace,
    *,
    candidate: dict[str, Any],
    source_wallet: str,
    generated_at: str,
    active_set_admissible_backfill: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    contract = _active_live_set_contract()
    members: list[dict[str, Any]] = []
    for raw_member in _active_live_set_members_contract():
        member_wallet = str(raw_member.get("source_wallet") or "").lower()
        member_policy_id = str(raw_member.get("policy_id") or "")
        members.append(
            {
                "candidate_id": str(raw_member.get("candidate_id") or ""),
                "candidate_type": str(raw_member.get("candidate_type") or "SINGLE_WALLET"),
                "source_wallet": member_wallet,
                "policy_id": member_policy_id,
                "wallet_fraction": float(raw_member.get("wallet_fraction") or getattr(args, "wallet_fraction", 0.0) or 0.0),
                "max_order_usd": float(raw_member.get("max_order_usd") or getattr(args, "max_order_usd", 0.0) or 0.0),
                "max_price": float(raw_member.get("max_price") or getattr(args, "price_band_decision_max_price", 0.0) or 0.0),
                "rolling_loss_trigger_usd": float(raw_member.get("rolling_loss_trigger_usd") or -16.0),
                "status": str(raw_member.get("status") or ""),
                "is_current_cycle_member": bool(
                    member_wallet == str(source_wallet or "").lower()
                    and (
                        not member_policy_id
                        or member_policy_id == str(policy.get("policy_id") or candidate.get("policy_id") or "")
                    )
                ),
            }
        )
    if not members and not _active_live_set_is_empty(contract):
        members.append(
            {
                "candidate_id": str(candidate.get("candidate_id") or ""),
                "candidate_type": str(candidate.get("candidate_type") or ""),
                "source_wallet": str(source_wallet or ""),
                "policy_id": str(policy.get("policy_id") or candidate.get("policy_id") or ""),
                "wallet_fraction": float(policy.get("wallet_fraction") or getattr(args, "wallet_fraction", 0.0) or 0.0),
                "max_order_usd": float(policy.get("max_order_usd") or getattr(args, "max_order_usd", 0.0) or 0.0),
                "max_price": float(policy.get("max_price") or getattr(args, "price_band_decision_max_price", 0.0) or 0.0),
                "rolling_loss_trigger_usd": -16.0,
                "status": str(candidate.get("status") or ""),
                "is_current_cycle_member": True,
            }
        )
    return {
        "flow_stage": "LIVE/PROMOTE/ROTATE",
        "schema_version": 1,
        "generated_at": generated_at,
        "mode": str(contract.get("mode") or "active_set_single_guard"),
        "target_member_count_min": int(contract.get("target_member_count_min") or 3),
        "target_member_count_max": int(contract.get("target_member_count_max") or 5),
        "qualified_member_count": len(
            [member for member in members if member.get("candidate_id") and member.get("source_wallet")]
        ),
        "active_set_admissible_backfill": (
            active_set_admissible_backfill
            if isinstance(active_set_admissible_backfill, dict)
            else contract.get("active_set_admissible_backfill")
        ),
        "members": members,
        "submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
        "next_action": (
            "fast-track qualify 2-4 currently active positive-profile wallets and add them "
            "as independent active-set members only after positive replayed band PnL"
        ),
    }


def _coverage_kpi_snapshot(
    args: argparse.Namespace,
    *,
    active_set: dict[str, Any],
    ledger: dict[str, Any],
    window_participation: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    summary = ledger.get("summary") if isinstance(ledger.get("summary"), dict) else {}
    latest_order_ts = summary.get("latest_order_ts") or ledger.get("latest_order_ts")
    generated_ts = _parse_iso_ts(generated_at) or time.time()
    latest_order_epoch = _parse_iso_ts(latest_order_ts)
    armed_idle_s = None if latest_order_epoch is None else max(0.0, generated_ts - latest_order_epoch)
    qualified_member_count = int(active_set.get("qualified_member_count") or 0)
    below_active_set_min = qualified_member_count < int(active_set.get("target_member_count_min") or 3)
    armed_live = bool(getattr(args, "execute_live", False) and getattr(args, "live_orders_allowed", False))
    active_member_windows = int(window_participation.get("active_windows") or 0)
    incident = bool(armed_live and armed_idle_s is not None and armed_idle_s > 3600.0 and below_active_set_min)
    return {
        "flow_stage": "LIVE/PROMOTE/ROTATE",
        "schema_version": 1,
        "generated_at": generated_at,
        "metric": "armed_idle_hours_while_active_set_underfilled",
        "armed_live": armed_live,
        "latest_live_order_ts": latest_order_ts,
        "armed_idle_s": None if armed_idle_s is None else round(armed_idle_s, 6),
        "armed_idle_hours": None if armed_idle_s is None else round(armed_idle_s / 3600.0, 6),
        "qualified_member_count": qualified_member_count,
        "target_member_count_min": int(active_set.get("target_member_count_min") or 3),
        "target_member_count_max": int(active_set.get("target_member_count_max") or 5),
        "below_active_set_min": below_active_set_min,
        "active_member_windows_retained": active_member_windows,
        "coverage_incident": incident,
        "incident_rule": (
            "armed idle >1h with fewer than 3 qualified active-set members; "
            "qualify or backfill members instead of treating no alternate as a hold"
        ),
        "next_action": active_set.get("next_action") if incident or below_active_set_min else "maintain active-set coverage",
    }


def _sync_live_ledger_runtime_permission(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    live_allowed = bool(payload.get("live_orders_allowed"))
    blockers = sorted({str(blocker) for blocker in payload.get("blockers") or [] if str(blocker)})
    desired_signature = {
        "paper_only": not live_allowed,
        "live_orders_allowed": live_allowed,
        "can_trade": live_allowed,
        "status": str(payload.get("status") or ""),
        "blockers": blockers,
    }
    current_state = load_json(args.live_ledger_state, default={})
    current_permission = (
        current_state.get("runtime_permission") if isinstance(current_state.get("runtime_permission"), dict) else {}
    )
    current_signature = {
        "paper_only": bool(current_permission.get("paper_only")),
        "live_orders_allowed": bool(current_permission.get("live_orders_allowed")),
        "can_trade": bool(current_permission.get("can_trade")),
        "status": str(current_permission.get("status") or ""),
        "blockers": sorted({str(blocker) for blocker in current_permission.get("blockers") or [] if str(blocker)}),
    }
    if desired_signature == current_signature:
        return
    lifecycle = LiveWalletCopyLifecycle(
        LiveExecutionLedgerConfig(
            state_path=args.live_ledger_state,
            event_log_path=args.live_ledger_event_log,
        )
    )
    lifecycle.set_runtime_permission(
        paper_only=not live_allowed,
        live_orders_allowed=live_allowed,
        can_trade=live_allowed,
        status=str(payload.get("status") or ""),
        blockers=blockers,
        owner="wallet_copy_live_guard",
        details={
            "guard_state": args.state,
            "candidate_id": (payload.get("candidate") or {}).get("candidate_id"),
            "mission_candidate_id": payload.get("mission_candidate_id"),
            "mission_matched_profit_candidate_id": payload.get("mission_matched_profit_candidate_id"),
            "source_wallet": (payload.get("candidate") or {}).get("source_wallet"),
            "cycle": payload.get("cycle"),
            "active_set": payload.get("active_set") if isinstance(payload.get("active_set"), dict) else {},
        },
    )


def _sync_blocked_live_arm_state(
    args: argparse.Namespace,
    *,
    payload: dict[str, Any],
    source_route_gate: dict[str, Any],
) -> None:
    """Keep arm state current when guard blocks before live execution runs."""

    if payload.get("status") != "LIVE_GUARD_BLOCKED":
        return
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    live_execution = payload.get("live_execution") if isinstance(payload.get("live_execution"), dict) else {}
    guard_live_execution_runtime = (
        live_execution.get("guard_live_execution_runtime")
        if isinstance(live_execution.get("guard_live_execution_runtime"), dict)
        else payload.get("live_execution_runtime")
    )
    state = {
        "schema_version": 1,
        "kind": "wallet_copy_live_execution_arm_state",
        "generated_at": payload.get("generated_at") or utc_now_iso(),
        "status": "LIVE_ARMED_BLOCKED",
        "execution_result": {"status": "LIVE_GUARD_BLOCKED", "results": []},
        "orders_submitted": 0,
        "live_orders_allowed": False,
        "paper_only": True,
        "candidate_id": candidate.get("candidate_id") or payload.get("candidate_id"),
        "mission_candidate_id": payload.get("mission_candidate_id"),
        "mission_candidate_id_alias_matched": payload.get("mission_candidate_id_alias_matched"),
        "mission_matched_profit_candidate_id": payload.get("mission_matched_profit_candidate_id"),
        "source_wallet": candidate.get("source_wallet") or payload.get("source_wallet"),
        "policy": candidate.get("policy") or {},
        "copy_mode": payload.get("copy_mode"),
        "copy_style": payload.get("copy_style"),
        "strict_source_order_1_to_1_required": payload.get("strict_source_order_1_to_1_required"),
        "selected_intent_parity_required": payload.get("selected_intent_parity_required"),
        "blockers": payload.get("blockers") or [],
        "guard_status": payload.get("status"),
        "guard_live_execution_runtime": guard_live_execution_runtime or {},
        "guard_live_execution": live_execution,
        "live_source_route_gate": source_route_gate,
        "candidate_intent_summary": {
            "status": "LIVE_GUARD_BLOCKED",
            "candidate_id": candidate.get("candidate_id") or payload.get("candidate_id"),
            "mission_candidate_id": payload.get("mission_candidate_id"),
            "mission_candidate_id_alias_matched": payload.get("mission_candidate_id_alias_matched"),
            "mission_matched_profit_candidate_id": payload.get("mission_matched_profit_candidate_id"),
            "source_wallet": candidate.get("source_wallet") or payload.get("source_wallet"),
            "policy": candidate.get("policy") or {},
            "fresh_candidate_intents": 0,
            "candidate_intents_live_tradeable_window_open": 0,
            "blockers": payload.get("blockers") or [],
        },
        "live_dedupe_summary": {
            "input_intents": 0,
            "new_intents": 0,
            "already_submitted_intents": 0,
            "skipped_intent_ids": [],
            "submitted_intent_ids_seen": None,
            "live_ledger_state": args.live_ledger_state,
        },
    }
    atomic_write_json(args.live_arm_state, state)


def _acquire_lock(args: argparse.Namespace):
    path = ROOT / args.lock_file if not Path(args.lock_file).is_absolute() else Path(args.lock_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.seek(0)
        holder = handle.read().strip()
        state = {
            "schema_version": 1,
            "kind": "wallet_copy_live_guard_state",
            "generated_at": utc_now_iso(),
            "status": "LIVE_GUARD_ALREADY_RUNNING",
            "lock_file": str(path),
            "holder": holder,
            "paper_only": True,
            "live_orders_allowed": False,
            "state_write_skipped": True,
            "rule": "lock losers must not overwrite the canonical full guard state",
        }
        print(json.dumps(state, indent=2, sort_keys=True))
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps({"pid": os.getpid(), "started_at": utc_now_iso(), "state": args.state}) + "\n")
    handle.flush()
    return handle


def _total_loss_member_auto_disable(
    members: list[dict[str, Any]],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if bool(getattr(args, "disable_total_loss_member_auto_disable", False)):
        return members, {"enabled": False, "reason": "disabled_by_arg"}
    if not hasattr(args, "live_ledger_state") or not hasattr(args, "resolutions"):
        return members, {"enabled": True, "reason": "runtime_paths_missing", "disabled_members": []}
    min_resolved = max(
        1,
        int(
            getattr(
                args,
                "total_loss_auto_disable_min_resolved_fills",
                TOTAL_LOSS_AUTO_DISABLE_MIN_RESOLVED_FILLS,
            )
            or TOTAL_LOSS_AUTO_DISABLE_MIN_RESOLVED_FILLS
        ),
    )
    wallet_to_member = {
        str(member.get("source_wallet") or member.get("wallet") or "").lower(): member
        for member in members
        if isinstance(member, dict) and str(member.get("source_wallet") or member.get("wallet") or "").strip()
    }
    if not wallet_to_member:
        return members, {"enabled": True, "min_resolved_fills": min_resolved, "disabled_members": []}
    ledger = load_json(str(getattr(args, "live_ledger_state", "data/research/wallet_copy_live_execution_state.json")), default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) and isinstance(ledger.get("orders"), list) else []
    resolutions = load_resolutions(str(getattr(args, "resolutions", "data/research/btc_resolutions_from_btcusdt_ticks.jsonl")))
    stats: dict[str, dict[str, Any]] = {
        wallet: {"resolved_fills": 0, "total_loss_fills": 0, "cost_usd": 0.0, "pnl_usd": 0.0}
        for wallet in wallet_to_member
    }
    for order in orders:
        if not isinstance(order, dict):
            continue
        wallet = str(order.get("source_wallet") or "").lower()
        if wallet not in stats:
            continue
        event = score_order(order, resolutions)
        event_status = str(
            event.get("status")
            or event.get("final_status")
            or order.get("final_status")
            or order.get("status")
            or ""
        ).upper()
        if event_status != "FILLED" or not bool(event.get("resolved")):
            continue
        cost = float(event.get("cost_usd") or 0.0)
        shares = float(event.get("shares") or 0.0)
        if cost <= 0:
            cost = float(order.get("requested_size_usd") or order.get("size_usd") or 0.0)
        if shares <= 0:
            shares = float(order.get("requested_shares") or order.get("shares") or 0.0)
        pnl = float(event.get("pnl_usd") or 0.0)
        if float(event.get("cost_usd") or 0.0) <= 0 and cost > 0:
            pnl = (shares if bool(event.get("win")) else 0.0) - cost
        if cost <= 0:
            continue
        row = stats[wallet]
        row["resolved_fills"] += 1
        row["cost_usd"] = round(float(row["cost_usd"]) + cost, 6)
        row["pnl_usd"] = round(float(row["pnl_usd"]) + pnl, 6)
        if pnl <= -cost + 1e-9:
            row["total_loss_fills"] += 1

    disabled_reasons: dict[str, str] = {}
    for wallet, row in stats.items():
        if (
            int(row["resolved_fills"]) >= min_resolved
            and int(row["total_loss_fills"]) == int(row["resolved_fills"])
        ):
            disabled_reasons[wallet] = "resolved_fills_all_total_losses"
            continue
        member = wallet_to_member[wallet]
        band_admission = (
            member.get("band_scoped_admission")
            if isinstance(member.get("band_scoped_admission"), dict)
            else {}
        )
        cell_admission = (
            member.get("cell_scoped_admission")
            if isinstance(member.get("cell_scoped_admission"), dict)
            else {}
        )
        admission = cell_admission or band_admission
        loss_line = _float_or_default(
            admission.get("per_cell_loss_line_usd")
            if cell_admission
            else admission.get("per_member_loss_line_usd"),
            0.0,
        )
        first_slice = (
            cell_admission.get("first_slice_kill")
            if isinstance(cell_admission.get("first_slice_kill"), dict)
            else {}
        )
        first_slice_min_resolved = max(
            1, int(first_slice.get("min_resolved_fills") or min_resolved)
        )
        if (
            str(cell_admission.get("status") or "").upper() == "ACTIVE"
            and int(row["resolved_fills"]) >= first_slice_min_resolved
            and float(row["pnl_usd"])
            <= _float_or_default(first_slice.get("pnl_lte_usd"), 0.0)
        ):
            disabled_reasons[wallet] = "cell_scoped_first_slice_breach"
            continue
        if (
            str(admission.get("status") or "").upper() == "ACTIVE"
            and loss_line < 0.0
            and int(row["resolved_fills"]) >= min_resolved
            and float(row["pnl_usd"]) <= loss_line
        ):
            disabled_reasons[wallet] = "band_scoped_per_member_loss_line"
    disabled_wallets = set(disabled_reasons)
    filtered: list[dict[str, Any]] = []
    disabled_rows: list[dict[str, Any]] = []
    for member in members:
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
        if wallet in disabled_wallets:
            disabled = dict(member)
            disabled["enabled"] = False
            reason = disabled_reasons[wallet]
            disabled["status"] = (
                "AUTO_DISABLED_CELL_SCOPED_FIRST_SLICE_BREACH"
                if reason == "cell_scoped_first_slice_breach"
                else
                "AUTO_DISABLED_BAND_SCOPED_LOSS_LINE"
                if reason == "band_scoped_per_member_loss_line"
                else "AUTO_DISABLED_100PCT_TOTAL_LOSS_RATE"
            )
            disabled["auto_disable_reason"] = reason
            disabled_row = {
                "candidate_id": disabled.get("candidate_id"),
                "source_wallet": wallet,
                **stats.get(wallet, {}),
            }
            if reason in {
                "band_scoped_per_member_loss_line",
                "cell_scoped_first_slice_breach",
            }:
                disabled_row["auto_disable_reason"] = reason
                disabled_row["loss_line_usd"] = _float_or_default(
                    (
                        member.get("cell_scoped_admission")
                        if isinstance(member.get("cell_scoped_admission"), dict)
                        else member.get("band_scoped_admission")
                        if isinstance(member.get("band_scoped_admission"), dict)
                        else {}
                    ).get(
                        "per_cell_loss_line_usd",
                        (
                            member.get("band_scoped_admission")
                            if isinstance(member.get("band_scoped_admission"), dict)
                            else {}
                        ).get("per_member_loss_line_usd"),
                    ),
                    0.0,
                )
            disabled_rows.append(disabled_row)
            continue
        filtered.append(member)
    return filtered, {
        "enabled": True,
        "flow_stage": "LIVE/ROTATE",
        "rule": (
            "auto-disable active-set members with 3+ resolved fills and 100% "
            "total-loss rate, or OP-TOMORROW band-scoped members at their "
            "explicit cumulative loss line"
        ),
        "min_resolved_fills": min_resolved,
        "disabled_members": disabled_rows,
        "member_stats": stats,
    }


def _cap_step_resolved_rows(
    *,
    orders: list[Any],
    resolutions: dict[str, Any],
    source_wallet: str,
    since: dt.datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        wallet = _order_source_wallet(order)
        if wallet != source_wallet:
            continue
        submitted_at = _parse_iso_datetime(order.get("submitted_at") or order.get("created_at") or order.get("updated_at"))
        if submitted_at is None or submitted_at < since:
            continue
        event = score_order(order, resolutions)
        event_status = str(
            event.get("status")
            or event.get("final_status")
            or order.get("final_status")
            or order.get("status")
            or ""
        ).upper()
        if event_status != "FILLED" or not bool(event.get("resolved")):
            continue
        cost = _float_or_default(event.get("cost_usd"), 0.0)
        shares = _float_or_default(event.get("shares"), 0.0)
        if cost <= 0:
            cost = _float_or_default(order.get("requested_size_usd") or order.get("size_usd"), 0.0)
        if shares <= 0:
            shares = _float_or_default(order.get("requested_shares") or order.get("shares"), 0.0)
        pnl = _float_or_default(event.get("pnl_usd"), 0.0)
        if _float_or_default(event.get("cost_usd"), 0.0) <= 0 and cost > 0:
            pnl = (shares if bool(event.get("win")) else 0.0) - cost
        rows.append(
            {
                "submitted_at": submitted_at.isoformat(),
                "market_slug": order.get("market_slug") or event.get("market_slug"),
                "order_id": order.get("order_id") or event.get("order_id"),
                "pnl_usd": round(float(pnl), 6),
                "cost_usd": round(float(cost), 6),
                "payout_usd": round(float(event.get("payout_usd") or 0.0), 6),
            }
        )
    rows.sort(key=lambda row: str(row.get("submitted_at") or ""))
    return rows


def _cap_step_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cum_pnl = round(sum(float(row.get("pnl_usd") or 0.0) for row in rows), 6)
    consecutive_losses = 0
    for row in reversed(rows):
        if float(row.get("pnl_usd") or 0.0) < 0:
            consecutive_losses += 1
        else:
            break
    return {
        "resolved_fills": len(rows),
        "cum_resolved_pnl_usd": cum_pnl,
        "consecutive_losing_resolutions": consecutive_losses,
        "rows": rows[-10:],
    }


def _maybe_execute_cap_step_revert(args: argparse.Namespace, *, generated_at: str) -> dict[str, Any]:
    overlay = _load_auto_degrade_active_set_overlay()
    step = overlay.get("latest_e6db_cap_step") if isinstance(overlay.get("latest_e6db_cap_step"), dict) else {}
    if not step:
        return {"enabled": False, "flow_stage": "LIVE/ROTATE", "status": "NO_CAP_STEP_STATE"}
    if step.get("reverted_at") or str(step.get("status") or "").upper() == "CAP_6_REVERTED":
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE",
            "status": "ALREADY_REVERTED",
            "reverted_at": step.get("reverted_at"),
        }
    source_wallet = str(step.get("source_wallet") or "").strip().lower()
    since = _parse_iso_datetime(step.get("applied_at"))
    revert = step.get("mechanical_revert") if isinstance(step.get("mechanical_revert"), dict) else {}
    if not source_wallet or since is None or not revert:
        return {"enabled": True, "flow_stage": "LIVE/ROTATE", "status": "CAP_STEP_STATE_INCOMPLETE"}
    ledger = load_json(str(getattr(args, "live_ledger_state", "data/research/wallet_copy_live_execution_state.json")), default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) and isinstance(ledger.get("orders"), list) else []
    resolutions = load_resolutions(str(getattr(args, "resolutions", "data/research/btc_resolutions_from_btcusdt_ticks.jsonl")))
    rows = _cap_step_resolved_rows(orders=orders, resolutions=resolutions, source_wallet=source_wallet, since=since)
    attribution = _cap_step_attribution(rows)
    cum_threshold = _float_or_default(revert.get("cum_resolved_pnl_from_step_lte_usd"), -5.0)
    loss_threshold = max(1, int(revert.get("consecutive_losing_resolutions") or 4))
    fired = False
    reason = ""
    if float(attribution["cum_resolved_pnl_usd"]) <= cum_threshold:
        fired = True
        reason = "cum_resolved_pnl_lte_threshold"
    elif int(attribution["consecutive_losing_resolutions"]) >= loss_threshold:
        fired = True
        reason = "consecutive_losing_resolutions_gte_threshold"
    decision = {
        "enabled": True,
        "flow_stage": "LIVE/ROTATE",
        "status": "WATCH",
        "rule": "Fable 2026-07-10T02:54Z: event-driven cap_6 revert check each guard cycle",
        "source_wallet": source_wallet,
        "candidate_id": step.get("candidate_id"),
        "applied_at": step.get("applied_at"),
        "thresholds": {
            "cum_resolved_pnl_from_step_lte_usd": cum_threshold,
            "consecutive_losing_resolutions": loss_threshold,
        },
        "attribution": attribution,
    }
    if not fired:
        return decision

    revert_policy_id = str(revert.get("revert_policy_id") or step.get("from_policy_id") or "")
    revert_max_order_usd = _float_or_default(revert.get("revert_max_order_usd"), _float_or_default(step.get("from_max_order_usd"), 0.0))
    members = overlay.get("members") if isinstance(overlay.get("members"), list) else []
    updated_members: list[Any] = []
    touched = False
    final_packet = {
        **decision,
        "status": "CAP_6_REVERTED",
        "verdict": "CAP_6_REVERTED",
        "reverted_at": generated_at,
        "trigger_reason": reason,
        "revert_policy_id": revert_policy_id,
        "revert_max_order_usd": revert_max_order_usd,
        "requires_fable_ping": False,
    }
    for member in members:
        if not isinstance(member, dict):
            updated_members.append(member)
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if wallet != source_wallet:
            updated_members.append(member)
            continue
        updated = _set_gate_recognized_member_status(
            member,
            gate_status="PASS",
            provenance_status="CAP_6_REVERTED_TO_CAP_4",
            direction_id="2026-07-10T02:54Z-fable-event-driven-cap6-revert",
            reason="Mechanical cap-step revert changed policy/size; member status remains gate-recognized.",
        )
        updated["policy_id"] = revert_policy_id
        updated["max_order_usd"] = revert_max_order_usd
        updated["mechanical_revert_status"] = "CAP_6_REVERTED_TO_CAP_4"
        policy = dict(updated.get("policy")) if isinstance(updated.get("policy"), dict) else {}
        policy["policy_id"] = revert_policy_id
        policy["max_order_usd"] = revert_max_order_usd
        updated["policy"] = policy
        summary = dict(updated.get("summary")) if isinstance(updated.get("summary"), dict) else {}
        summary["cap6_revert"] = final_packet
        updated["summary"] = summary
        updated_members.append(updated)
        touched = True
    if not touched:
        return {**decision, "status": "REVERT_NOT_APPLIED_MEMBER_NOT_FOUND", "trigger_reason": reason}
    payload = dict(overlay)
    payload["members"] = updated_members
    payload["updated_at"] = generated_at
    payload["latest_e6db_cap_step"] = {
        **step,
        "status": "CAP_6_REVERTED",
        "reverted_at": generated_at,
        "final_attribution": attribution,
        "trigger_reason": reason,
        "revert_direction_id": "2026-07-10T02:54Z-fable-event-driven-cap6-revert",
    }
    payload["latest_e6db_cap_revert"] = final_packet
    _atomic_write_auto_degrade_overlay(payload)
    return final_packet


def _rank_postfilter_active_set_members(
    members: list[dict[str, Any]],
    *,
    limit: int,
    target_member_count_min: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Apply capacity only after fail-closed filters, preferring admissible survivors."""
    admissible_members = [member for member in members if _candidate_status_live_admissible(member)]
    selected_members = members[:limit]
    if admissible_members and not any(
        _candidate_status_live_admissible(member) for member in selected_members
    ):
        required_admissible = min(limit, target_member_count_min, len(admissible_members))
        replacements = admissible_members[:required_admissible]
        replacement_wallets = {_member_wallet(member) for member in replacements}
        retained = [
            member for member in selected_members if _member_wallet(member) not in replacement_wallets
        ][: max(0, limit - len(replacements))]
        selected_members = replacements + retained
    selected_wallets = {_member_wallet(member) for member in selected_members}
    return selected_members, {
        "enabled": True,
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "target_member_count_min": target_member_count_min,
        "target_member_count_max": limit,
        "admissible_available": len(admissible_members),
        "members_added": [
            {
                "candidate_id": str(member.get("candidate_id") or ""),
                "source_wallet": _member_wallet(member),
                "status": str(member.get("status") or ""),
                "reason": "post_filter_live_admissible_backfill",
            }
            for member in selected_members
            if _candidate_status_live_admissible(member)
        ],
        "members_rejected": [
            {
                "candidate_id": str(member.get("candidate_id") or ""),
                "source_wallet": _member_wallet(member),
                "status": str(member.get("status") or ""),
                "reason": "post_filter_target_capacity",
            }
            for member in members
            if _member_wallet(member) not in selected_wallets
        ],
        "rule": (
            "apply disabled, loss, temporal, and liveness filters before the member limit; "
            "rank live-admissible survivors first without widening status admission"
        ),
    }


def _active_set_runtime_args(args: argparse.Namespace, *, cycle: int) -> tuple[argparse.Namespace, dict[str, Any]]:
    temporal_now = _parse_iso_datetime(getattr(args, "active_set_temporal_now_iso", "") or "") or dt.datetime.now(dt.timezone.utc)
    try:
        active_set = _active_live_set_contract(now=temporal_now)
    except TypeError as exc:
        if "unexpected keyword argument 'now'" not in str(exc):
            raise
        active_set = _active_live_set_contract()
    live_status_registration = _live_status_registration_audit(active_set)
    members = (
        [
            dict(row)
            for row in active_set.get("members", [])
            if isinstance(row, dict) and not _active_set_member_is_disabled(row, now=temporal_now)
        ]
        if bool(getattr(args, "active_set", True))
        else []
    )
    requested_limit = int(getattr(args, "active_set_member_limit", 0) or 0)
    set_generation_id = _active_set_generation_id(active_set)
    contract_limit = int((active_set.get("target_member_count_max") or 5))
    limit = max(1, requested_limit if requested_limit > 0 else contract_limit)
    members, auto_disable = _total_loss_member_auto_disable(members, args)
    temporal_profiles = _temporal_profiles_by_wallet()
    members, temporal_exclusion = _apply_temporal_slice_exclusions(
        members,
        profiles=temporal_profiles,
        now=temporal_now,
    )
    overflow_members = active_set.get("auto_degrade_overflow_members")
    if len(members) < limit and isinstance(overflow_members, list):
        existing_wallets = {
            str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
            for member in members
            if isinstance(member, dict)
        }
        disabled_wallets = {
            str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
            for row in auto_disable.get("disabled_members", [])
            if isinstance(row, dict)
        }
        disabled_wallets.update(str(row.get("source_wallet") or "") for row in temporal_exclusion.get("excluded_members", []))
        refill_candidates = []
        for row in overflow_members:
            if not isinstance(row, dict) or _active_set_member_is_disabled(row, now=temporal_now):
                continue
            wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
            if not wallet or wallet in existing_wallets or wallet in disabled_wallets:
                continue
            refill_candidates.append(dict(row))
        if refill_candidates:
            refill_members, refill_auto_disable = _total_loss_member_auto_disable(refill_candidates, args)
            refill_members, refill_temporal_exclusion = _apply_temporal_slice_exclusions(
                refill_members,
                profiles=temporal_profiles,
                now=temporal_now,
            )
            temporal_exclusion["excluded_count"] = int(temporal_exclusion.get("excluded_count") or 0) + int(
                refill_temporal_exclusion.get("excluded_count") or 0
            )
            temporal_exclusion["excluded_members"] = list(temporal_exclusion.get("excluded_members") or []) + list(
                refill_temporal_exclusion.get("excluded_members") or []
            )
            temporal_exclusion["excluded_wallets"] = [
                row.get("source_wallet")
                for row in temporal_exclusion.get("excluded_members", [])
                if isinstance(row, dict) and row.get("source_wallet")
            ]
            refill_disabled = refill_auto_disable.get("disabled_members")
            if isinstance(refill_disabled, list) and refill_disabled:
                auto_disable.setdefault("disabled_members", []).extend(refill_disabled)
            needed = max(0, limit - len(members))
            members.extend(refill_members[:needed])
            auto_disable["overflow_refill"] = {
                "enabled": True,
                "available_candidates": len(refill_candidates),
                "admitted": len(refill_members[:needed]),
                "target_limit": limit,
                "admitted_queue_positions": [
                    _active_set_member_queue_position(member) for member in refill_members[:needed]
                ],
                "rule": "refill active-set slots after total-loss auto-disable using contract overflow order",
            }
    members, active_set_admissible_backfill = _rank_postfilter_active_set_members(
        members,
        limit=limit,
        target_member_count_min=int(active_set.get("target_member_count_min") or 3),
    )
    filtered_rejections: list[dict[str, Any]] = []
    for row in auto_disable.get("disabled_members") or []:
        if isinstance(row, dict):
            filtered_rejections.append(
                {
                    "candidate_id": str(row.get("candidate_id") or ""),
                    "source_wallet": _member_wallet(row),
                    "status": str(row.get("status") or ""),
                    "reason": "total_loss_auto_disable",
                }
            )
    for row in temporal_exclusion.get("excluded_members") or []:
        if isinstance(row, dict):
            filtered_rejections.append(
                {
                    "candidate_id": str(row.get("candidate_id") or ""),
                    "source_wallet": _member_wallet(row),
                    "status": str(row.get("status") or ""),
                    "reason": str(row.get("reason") or "temporal_slice_exclusion"),
                }
            )
    for report in (active_set.get("external_liveness_sweep") or {}).get("reports") or []:
        if not isinstance(report, dict):
            continue
        for row in report.get("disabled_members") or []:
            if isinstance(row, dict):
                filtered_rejections.append(
                    {
                        "candidate_id": str(row.get("candidate_id") or ""),
                        "source_wallet": _member_wallet(row),
                        "status": str(row.get("status") or ""),
                        "reason": str(row.get("reason") or "external_liveness_sweep"),
                    }
                )
    seen_rejections = {
        (row.get("candidate_id"), row.get("source_wallet"), row.get("reason"))
        for row in active_set_admissible_backfill["members_rejected"]
    }
    active_set_admissible_backfill["members_rejected"].extend(
        row
        for row in filtered_rejections
        if (row.get("candidate_id"), row.get("source_wallet"), row.get("reason"))
        not in seen_rejections
    )
    active_set["active_set_admissible_backfill"] = active_set_admissible_backfill
    members, weekend_proven_admission = _apply_weekend_proven_seat_admission(
        members,
        now=temporal_now,
    )
    temporal_exclusion["included_count"] = len(members)
    active_set["weekend_proven_seat_admission"] = weekend_proven_admission
    temporal_exclusion["excluded_wallets"] = list(dict.fromkeys(temporal_exclusion.get("excluded_wallets") or []))
    if not members:
        run_args = argparse.Namespace(**vars(args))
        run_args.candidate_id = ""
        run_args.source_wallet = ""
        run_args.policy_id = ""
        active_set_empty = _active_live_set_is_empty(active_set)
        run_args.active_set_empty_until_replacement = active_set_empty
        run_args.active_set_executable_roster_empty = True
        run_args.active_set_executable_roster_empty_reason = (
            "active_set_empty_until_replacement" if active_set_empty else "no_enabled_active_set_members"
        )
        run_args.active_set_temporal_now_iso = temporal_now.isoformat()
        return run_args, {
            "enabled": False,
            "flow_stage": "LIVE",
            "mode": active_set.get("mode") or "active_set_single_guard",
            "selection_mode": "active_set_empty_until_replacement"
            if _active_live_set_is_empty(active_set)
            else "no_enabled_active_set_members",
            "contract_status": active_set.get("status") or "EMPTY",
            "set_generation_id": set_generation_id,
            "qualified_member_count": 0,
            "member_count": 0,
            "selected_member_index": None,
            "total_loss_auto_disable": auto_disable,
            "temporal_slice_exclusion": temporal_exclusion,
            "external_liveness_sweep": active_set.get("external_liveness_sweep"),
            "live_status_registration": live_status_registration,
            "active_set_admissible_backfill": active_set_admissible_backfill,
        }
    roster_size_defense = _apply_probe_cap_size_defense_to_runtime_members(members, args)
    round_robin_index = (max(1, int(cycle)) - 1) % len(members)
    fresh_index, fresh_selection = _fresh_runtime_member_selection_index(members, args)
    priority_freeze = fresh_selection.get("selection_priority_freeze") if isinstance(fresh_selection, dict) else {}
    priority_frozen_wallets = set(priority_freeze.get("wallets") or []) if isinstance(priority_freeze, dict) else set()
    reserved_seat_hold = _reserved_5960_seat_hold(priority_freeze, members=members, now=temporal_now)
    if reserved_seat_hold.get("active") is True:
        hold_reason = (
            "temporal_slice_active_unproven_basis"
            if reserved_seat_hold.get("unproven_members")
            else "reserved_live_probe_seat_pending_activation"
        )
        run_args = argparse.Namespace(**vars(args))
        run_args.candidate_id = ""
        run_args.source_wallet = ""
        run_args.policy_id = ""
        run_args.active_set_empty_until_replacement = _active_live_set_is_empty(active_set)
        run_args.active_set_executable_roster_empty = True
        run_args.active_set_executable_roster_empty_reason = hold_reason
        run_args.active_set_temporal_now_iso = temporal_now.isoformat()
        return run_args, {
            "enabled": False,
            "flow_stage": "LIVE",
            "mode": active_set.get("mode") or "active_set_single_guard",
            "selection_mode": hold_reason,
            "contract_status": active_set.get("status") or "ACTIVE",
            "set_generation_id": set_generation_id,
            "qualified_member_count": len(members),
            "member_count": len(members),
            "selected_member_index": None,
            "round_robin_selected_member_index": round_robin_index,
            "fresh_runtime_member_selection": fresh_selection,
            "total_loss_auto_disable": auto_disable,
            "temporal_slice_exclusion": temporal_exclusion,
            "external_liveness_sweep": active_set.get("external_liveness_sweep"),
            "reserved_seat_hold": reserved_seat_hold,
            "roster_size_defense": roster_size_defense,
            "live_status_registration": live_status_registration,
            "active_set_admissible_backfill": active_set_admissible_backfill,
        }
    active_unproven_members = _active_unproven_temporal_members(members)
    active_unproven_wallets = {
        str(row.get("source_wallet") or "").strip().lower()
        for row in active_unproven_members
        if str(row.get("source_wallet") or "").strip()
    }
    selection_pin_wallet = ""
    selection_pin_temporal_override: dict[str, Any] | None = None
    if isinstance(fresh_selection, dict) and fresh_selection.get("reason") == "runtime_member_selection_pin":
        selection_pin_wallet = str(fresh_selection.get("selected_wallet") or "").strip().lower()
        if selection_pin_wallet in active_unproven_wallets:
            selection_pin_temporal_override = {
                "enabled": True,
                "flow_stage": "LIVE/ROTATE",
                "source_wallet": selection_pin_wallet,
                "selection_pin_id": fresh_selection.get("selection_pin_id"),
                "reason": "explicit Fable selection pin overrides unproven temporal selection hold; proven-negative temporal exclusion still applies earlier",
            }
            fresh_selection = dict(fresh_selection)
            fresh_selection["temporal_unproven_override"] = selection_pin_temporal_override
    selectable_indices = [
        idx
        for idx, member in enumerate(members)
        for wallet in [str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()]
        if (
            wallet not in priority_frozen_wallets
            and (wallet not in active_unproven_wallets or wallet == selection_pin_wallet)
        )
    ]
    runtime_executable_filter = _active_set_member_executable_filter(members, args)
    executable_indices = set(runtime_executable_filter.get("executable_indices") or [])
    if runtime_executable_filter.get("applied"):
        executable_selectable_indices = [idx for idx in selectable_indices if idx in executable_indices]
        if executable_selectable_indices:
            selectable_indices = executable_selectable_indices
        else:
            runtime_executable_filter = dict(runtime_executable_filter)
            runtime_executable_filter["applied"] = False
            runtime_executable_filter["reason"] = "no_executable_member_after_selection_freeze_filters"
    if not selectable_indices:
        run_args = argparse.Namespace(**vars(args))
        run_args.candidate_id = ""
        run_args.source_wallet = ""
        run_args.policy_id = ""
        run_args.active_set_empty_until_replacement = _active_live_set_is_empty(active_set)
        run_args.active_set_executable_roster_empty = True
        empty_reason = (
            "temporal_slice_active_unproven_basis"
            if active_unproven_members
            else "all_active_set_members_selection_priority_frozen"
        )
        run_args.active_set_executable_roster_empty_reason = empty_reason
        run_args.active_set_temporal_now_iso = temporal_now.isoformat()
        return run_args, {
            "enabled": False,
            "flow_stage": "LIVE",
            "mode": active_set.get("mode") or "active_set_single_guard",
            "selection_mode": empty_reason,
            "contract_status": active_set.get("status") or "ACTIVE",
            "set_generation_id": set_generation_id,
            "qualified_member_count": len(members),
            "member_count": len(members),
            "selected_member_index": None,
            "round_robin_selected_member_index": round_robin_index,
            "fresh_runtime_member_selection": fresh_selection,
            "runtime_member_executable_filter": runtime_executable_filter,
            "total_loss_auto_disable": auto_disable,
            "temporal_slice_exclusion": temporal_exclusion,
            "external_liveness_sweep": active_set.get("external_liveness_sweep"),
            "temporal_slice_active_unproven": {
                "blocker": "temporal_slice_active_unproven_basis",
                "inadmissible_members": active_unproven_members,
            },
            "roster_size_defense": roster_size_defense,
            "live_status_registration": live_status_registration,
            "active_set_admissible_backfill": active_set_admissible_backfill,
        }
    if fresh_index is not None and fresh_index not in selectable_indices:
        fresh_index = None
    selected_index = fresh_index
    if selected_index is None:
        selectable_set = set(selectable_indices)
        selected_index = next(
            (idx for offset in range(len(members)) if (idx := (round_robin_index + offset) % len(members)) in selectable_set),
            selectable_indices[0],
        )
    selected = dict(members[selected_index])
    selected_policy = _live_execution_policy_from_mission_member(
        selected,
        fallback_policy_id=str(selected.get("policy_id") or getattr(args, "policy_id", "") or ""),
    )
    try:
        selected_late_window_stop_s = float(selected_policy.get("late_window_stop_s"))
    except (TypeError, ValueError):
        selected_late_window_stop_s = float(getattr(args, "inventory_late_window_stop_s", 60.0))
    run_args = argparse.Namespace(**vars(args))
    run_args.configured_min_live_order_usd = float(getattr(args, "min_live_order_usd", 1.0) or 1.0)
    run_args.candidate_id = str(selected.get("candidate_id") or getattr(args, "candidate_id", "") or "")
    run_args.source_wallet = str(
        selected.get("source_wallet") or selected.get("wallet") or getattr(args, "source_wallet", "") or ""
    ).lower()
    run_args.policy_id = str(selected.get("policy_id") or getattr(args, "policy_id", "") or "")
    run_args.inventory_late_window_stop_s = selected_late_window_stop_s
    selected_runtime_policy = dict(selected_policy)
    for key in ("policy_id", "max_order_usd", "max_price", "wallet_fraction", "min_live_order_usd"):
        if selected.get(key) is not None:
            selected_runtime_policy[key] = selected.get(key)
    if "min_live_order_usd" in selected_runtime_policy and "min_order_usd" not in selected_runtime_policy:
        selected_runtime_policy["min_order_usd"] = selected_runtime_policy.get("min_live_order_usd")
    max_order = _float_or_default(selected_runtime_policy.get("max_order_usd"), 0.0)
    member_max_order = _float_or_default(selected.get("max_order_usd"), max_order)
    cap_order = min(value for value in (max_order, member_max_order) if value and value > 0) if (max_order > 0 or member_max_order > 0) else 0.0
    if cap_order > 0:
        selected_runtime_policy["max_order_usd"] = min(max_order if max_order > 0 else cap_order, cap_order)
        raw_min_order = _float_or_default(selected_runtime_policy.get("min_order_usd"), cap_order)
        selected_runtime_policy["min_order_usd"] = min(raw_min_order, cap_order)
        if "min_live_order_usd" in selected_runtime_policy:
            selected_runtime_policy["min_live_order_usd"] = min(
                _float_or_default(selected_runtime_policy.get("min_live_order_usd"), cap_order),
                cap_order,
            )
    hour_band_size_clamp = _apply_hour_band_size_clamp(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        now=temporal_now,
    )
    size_defense = _apply_probe_cap_size_defense(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        args=args,
    )
    structural_min_notional_repair = _apply_selection_pin_min_notional_repair(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
    )
    live_min_order_cap_floor = _apply_live_min_order_cap_floor(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        process_min_live_order_usd=run_args.configured_min_live_order_usd,
        source="active_set_selected_runtime_policy",
    )
    selected_roster_floor = (
        selected.get("live_min_order_cap_floor_clamp")
        if isinstance(selected.get("live_min_order_cap_floor_clamp"), dict)
        else {}
    )
    if live_min_order_cap_floor.get("active") is not True and selected_roster_floor.get("active") is True:
        live_min_order_cap_floor = dict(selected_roster_floor)
    min_order_override = selected_runtime_policy.get("min_live_order_usd", selected_runtime_policy.get("min_order_usd"))
    if min_order_override is not None:
        try:
            run_args.min_live_order_usd = float(min_order_override)
        except (TypeError, ValueError):
            pass
    policy_max_price = _float_or_default(selected_runtime_policy.get("max_price"), 0.0)
    if policy_max_price > 0:
        # Member policies may tighten the guard cap; the weekday golden ceiling
        # is 0.50 unless an explicit Fable-ruled carve-out widens it.
        guard_cap = _float_or_default(getattr(args, "price_band_decision_max_price", 0.50), 0.50)
        price_carveout_active = False
        if hasattr(args, "state_digest_json"):
            digest = load_json(_state_digest_path(args), default={})
            tripwires = digest.get("defense_tripwires") if isinstance(digest.get("defense_tripwires"), dict) else {}
            price_carveout_active = _is_a689_canary_carveout_active(
                selected=selected,
                selected_runtime_policy=selected_runtime_policy,
                tripwires=tripwires,
            )
        if price_carveout_active:
            guard_cap = max(guard_cap, _A689_CANARY_MAX_PRICE)
        run_args.price_band_decision_max_price = min(policy_max_price, guard_cap)
        selected_runtime_policy["max_price"] = run_args.price_band_decision_max_price
        if price_carveout_active:
            selected_runtime_policy["a689_canary_price_carveout"] = {
                "flow_stage": "LIVE/DEFEND",
                "status": "A689_CANARY_PRICE_BAND_CARVEOUT",
                "policy_max_price": round(policy_max_price, 6),
                "effective_guard_cap": round(guard_cap, 6),
                "max_price": selected_runtime_policy["max_price"],
                "rule": "Fable-ruled a689 canary may use the 0.70 price band while the day-PnL carve-out is valid",
            }
    policy_min_price = _float_or_default(selected_runtime_policy.get("min_price"), 0.0)
    if policy_min_price > 0:
        run_args.price_band_decision_min_price = policy_min_price
    run_args.active_set_selected_member_candidate_id = run_args.candidate_id
    run_args.active_set_selected_member_source_wallet = run_args.source_wallet
    run_args.active_set_selected_member_policy = selected_runtime_policy
    run_args.active_set_selected_member_status = str(selected.get("status") or "")
    selected_candidate_override_state = _selected_candidate_override_state_path(
        getattr(args, "selected_candidate_override_state", "")
        or "data/research/wallet_copy_live_guard_selected_candidate_override.json",
        run_args.candidate_id,
    )
    run_args.selected_candidate_override_state = selected_candidate_override_state
    selected_candidate_override = {
        "schema_version": 1,
        "kind": "wallet_copy_live_guard_selected_candidate_override",
        "flow_stage": "LIVE/DEFEND",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "candidate": {
            "candidate_id": run_args.candidate_id,
            "candidate_type": str(selected.get("candidate_type") or "SINGLE_WALLET"),
            "status": "PASS",
            "source_wallet": run_args.source_wallet,
            "policy_id": run_args.policy_id,
            "policy": selected_runtime_policy,
            "metadata": {
                "source": "active_set_runtime_selected_member",
                "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py",
                "active_set_selected_member_index": selected_index,
                "active_set_member_status": str(selected.get("status") or ""),
            },
            "live_target_profile": {
                "status": "PASS",
                "blockers": [],
                "operator_approval_id": str(getattr(args, "operator_approval_id", "") or "OP-LIVE-20260703-BELA"),
                "source": "active_set_runtime_selected_member",
            },
        },
        "rule": "child live execution must use the already-clamped runtime selected-member policy",
    }
    atomic_write_json(selected_candidate_override_state, selected_candidate_override)
    run_args.active_set_selection_reason = str(fresh_selection.get("reason") or "")
    for key in ("drip_min_tranche_usd", "drip_max_tranche_usd"):
        raw = selected_runtime_policy.get(key)
        if raw is not None:
            try:
                setattr(run_args, key, float(raw))
            except (TypeError, ValueError):
                pass
    raw_tranche_count = selected_runtime_policy.get("drip_max_tranches_per_window")
    if raw_tranche_count is not None:
        try:
            run_args.drip_max_tranches_per_window = int(raw_tranche_count)
        except (TypeError, ValueError):
            pass
    run_args.active_set_empty_until_replacement = _active_live_set_is_empty(active_set)
    run_args.active_set_executable_roster_empty = False
    run_args.active_set_executable_roster_empty_reason = ""
    run_args.active_set_temporal_now_iso = temporal_now.isoformat()
    run_args.active_set_fallthrough_members = []
    for member in members:
        member_wallet = str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()
        if member_wallet in priority_frozen_wallets or member_wallet in active_unproven_wallets:
            continue
        fallthrough_member = {
            "candidate_id": str(member.get("candidate_id") or ""),
            "source_wallet": member_wallet,
            "policy_id": str(member.get("policy_id") or ""),
        }
        if member.get("queue_position") is not None:
            fallthrough_member["queue_position"] = member.get("queue_position")
        run_args.active_set_fallthrough_members.append(fallthrough_member)
    run_args.active_set_selected_member_index = selected_index
    return run_args, {
        "enabled": True,
        "flow_stage": "LIVE",
        "mode": active_set.get("mode") or "active_set_single_guard",
        "selection_mode": (
            "auto_degrade_latest_admission_priority_single_guard"
            if fresh_selection.get("reason") == "auto_degrade_latest_admission_priority"
            else "selection_pin_single_guard"
            if fresh_selection.get("reason") == "runtime_member_selection_pin"
            else "last_successful_nondenied_member_priority_single_guard"
            if fresh_selection.get("reason") == "runtime_member_last_successful_nondenied_priority"
            else "policy_compatible_fresh_after_toxicity_le_30_single_guard"
            if fresh_selection.get("reason") == "runtime_member_policy_compatible_fresh_after_toxicity_le_30"
            else "policy_compatible_fresh_le_30_single_guard"
            if fresh_selection.get("reason") == "runtime_member_policy_compatible_fresh_le_30"
            else "freshest_runtime_member_lag_fallback_single_guard"
            if fresh_selection.get("reason") == "runtime_member_freshest_buy_lag_fallback"
            else "freshest_runtime_member_single_guard"
            if fresh_selection.get("applied")
            else "round_robin_single_guard"
        ),
        "contract_status": active_set.get("status") or "ACTIVE",
        "set_generation_id": set_generation_id,
        "target_member_count_min": active_set.get("target_member_count_min"),
        "target_member_count_max": active_set.get("target_member_count_max"),
        "target_member_count": active_set.get("target_member_count"),
        "qualified_member_count": len(members),
        "member_count": len(members),
        "total_loss_auto_disable": auto_disable,
        "external_liveness_sweep": active_set.get("external_liveness_sweep"),
        "live_status_registration": live_status_registration,
        "active_set_admissible_backfill": active_set_admissible_backfill,
        "selected_member_index": selected_index,
        "round_robin_selected_member_index": round_robin_index,
        "fresh_runtime_member_selection": fresh_selection,
        "runtime_member_executable_filter": runtime_executable_filter,
        "size_defense": size_defense,
        "live_min_order_cap_floor": live_min_order_cap_floor,
        "roster_size_defense": roster_size_defense,
        "structural_min_notional_repair": structural_min_notional_repair,
        "hour_band_size_clamp": hour_band_size_clamp,
        "temporal_slice_active_unproven": {
            "blocker": "temporal_slice_active_unproven_basis",
            "inadmissible_members": active_unproven_members,
            "selection_pin_override": selection_pin_temporal_override,
        },
        "temporal_slice_exclusion": temporal_exclusion,
        "weekend_proven_seat_admission": weekend_proven_admission,
        "policy_by_wallet": {
            str(member.get("source_wallet") or member.get("wallet") or "").lower(): _live_execution_policy_from_mission_member(
                member,
                fallback_policy_id=str(member.get("policy_id") or ""),
            )
            for member in members
            if str(member.get("source_wallet") or member.get("wallet") or "").strip()
        },
        "selected_member": {
            "candidate_id": run_args.candidate_id,
            "source_wallet": run_args.source_wallet,
            "policy_id": run_args.policy_id,
            "selected_candidate_override_state": selected_candidate_override_state,
            "role": selected.get("role") or "member",
            "member_id": selected.get("member_id") or run_args.candidate_id,
            "late_window_stop_s": selected_late_window_stop_s,
            "max_order_usd": selected_runtime_policy.get("max_order_usd"),
            "max_price": selected_runtime_policy.get("max_price"),
            "drip_max_tranche_usd": selected_runtime_policy.get("drip_max_tranche_usd"),
            "size_defense": selected_runtime_policy.get("size_defense"),
            "hour_band_size_clamp": selected_runtime_policy.get("hour_band_size_clamp"),
            "queue_position": selected.get("queue_position"),
            "status": selected.get("status"),
        },
        "members": [
            {
                "candidate_id": str(member.get("candidate_id") or ""),
                "source_wallet": str(member.get("source_wallet") or member.get("wallet") or "").lower(),
                "policy_id": str(member.get("policy_id") or ""),
                "queue_position": member.get("queue_position"),
                "role": member.get("role") or "member",
                "enabled": member.get("enabled") is not False,
                "max_order_usd": member.get("max_order_usd"),
                "max_price": member.get("max_price"),
                "drip_max_tranche_usd": (
                    member.get("policy") if isinstance(member.get("policy"), dict) else {}
                ).get("drip_max_tranche_usd"),
                "size_defense": (
                    member.get("policy") if isinstance(member.get("policy"), dict) else {}
                ).get("size_defense"),
                "rolling_loss_trigger_usd": member.get("rolling_loss_trigger_usd"),
                "late_window_stop_s": (member.get("policy") if isinstance(member.get("policy"), dict) else {}).get(
                    "late_window_stop_s"
                ),
                "temporal_slice_evaluation": member.get("temporal_slice_evaluation"),
                "status": member.get("status"),
            }
            for member in members
        ],
        "rule": "one live guard process rotates active-set members; no second submitter",
    }


RULED_FLAT_5960_ACTIVATES_AT = "2026-07-13T00:00:00Z"
RULED_FLAT_5960_DIRECTION_ID = "2026-07-11T20:04Z-fable-5960-readmission"
_GENERIC_NO_CANDIDATE_BLOCKERS = {
    "all_active_set_members_selection_priority_frozen",
    "runtime_admission_candidate_missing",
    "runtime_admission_source_wallet_missing",
}


def _reserved_5960_seat_hold(
    priority_freeze: dict[str, Any] | None,
    *,
    members: list[dict[str, Any]] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    freeze = priority_freeze if isinstance(priority_freeze, dict) else {}
    rows = [row for row in freeze.get("rows") or [] if isinstance(row, dict)]
    matched = [
        row
        for row in rows
        if str(row.get("direction_id") or "") == RULED_FLAT_5960_DIRECTION_ID
        or "5960 re-admission" in str(row.get("reason") or "")
    ]
    if not matched:
        return {"active": False}
    activation = _parse_iso_datetime(RULED_FLAT_5960_ACTIVATES_AT)
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    if activation is None or now_dt >= activation:
        return {"active": False}
    freeze_wallets = [str(wallet).lower() for wallet in freeze.get("wallets") or [] if str(wallet)]
    seat = {
        "direction_id": RULED_FLAT_5960_DIRECTION_ID,
        "reserved_wallets": freeze_wallets,
        "activates_at": RULED_FLAT_5960_ACTIVATES_AT,
        "reason": next((str(row.get("reason") or "") for row in matched if row.get("reason")), ""),
    }
    unproven_members: list[dict[str, Any]] = []
    for member in members or []:
        temporal = member.get("temporal_slice_evaluation") if isinstance(member, dict) else {}
        temporal = temporal if isinstance(temporal, dict) else {}
        evaluated = temporal.get("evaluated_slices") if isinstance(temporal.get("evaluated_slices"), list) else []
        unproven = [
            row
            for row in evaluated
            if isinstance(row, dict) and str(row.get("label") or "").upper() == "UNPROVEN"
        ]
        if unproven or str(temporal.get("reason") or "") == "temporal_profile_missing_or_unproven":
            unproven_members.append(
                {
                    "candidate_id": str(member.get("candidate_id") or ""),
                    "source_wallet": _member_wallet(member),
                    "temporal_reason": temporal.get("reason"),
                    "evaluated_slices": evaluated,
                }
            )
    blockers = [
        "temporal_slice_exclusion_all_measured_members",
        f"seat_reserved(direction_id={RULED_FLAT_5960_DIRECTION_ID}, activates_at={RULED_FLAT_5960_ACTIVATES_AT})",
    ]
    if unproven_members:
        blockers.insert(0, "temporal_slice_active_unproven_basis")
    return {
        "active": True,
        "flow_stage": "LIVE/DEFEND/ROTATE",
        "status": "RULED_FLAT_WEEKEND",
        "blockers": blockers,
        "seat_reserved": seat,
        "unproven_members": unproven_members,
        "next_action": "hold flat until the reserved 5960 probe seat reaches its Monday activation time",
    }


def _member_active_unproven_temporal_basis(member: dict[str, Any]) -> dict[str, Any] | None:
    temporal = member.get("temporal_slice_evaluation") if isinstance(member, dict) else {}
    temporal = temporal if isinstance(temporal, dict) else {}
    evaluated = temporal.get("evaluated_slices") if isinstance(temporal.get("evaluated_slices"), list) else []
    reason = str(temporal.get("reason") or "")
    unproven = [
        row
        for row in evaluated
        if isinstance(row, dict) and str(row.get("label") or "").upper() == "UNPROVEN"
    ]
    if not unproven and reason != "temporal_profile_missing_or_unproven":
        return None
    return {
        "candidate_id": str(member.get("candidate_id") or ""),
        "source_wallet": _member_wallet(member),
        "temporal_reason": reason,
        "evaluated_slices": evaluated,
    }


def _active_unproven_temporal_members(members: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for member in members or []:
        if not isinstance(member, dict):
            continue
        row = _member_active_unproven_temporal_basis(member)
        if row is not None:
            rows.append(row)
    return rows


def _ruled_flat_active_set_state(active_set_runtime: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(active_set_runtime, dict):
        return {"active": False}
    reserved_hold = active_set_runtime.get("reserved_seat_hold")
    if isinstance(reserved_hold, dict) and reserved_hold.get("active") is True:
        temporal = active_set_runtime.get("temporal_slice_exclusion")
        temporal = temporal if isinstance(temporal, dict) else {}
        return {
            "active": True,
            "flow_stage": "LIVE/DEFEND/ROTATE",
            "status": "RULED_FLAT_WEEKEND",
            "blockers": [str(blocker) for blocker in reserved_hold.get("blockers") or [] if str(blocker)],
            "active_slices": temporal.get("active_slices") or _temporal_active_slice_names(),
            "temporal_excluded_wallets": temporal.get("excluded_wallets") or [],
            "seat_reserved": reserved_hold.get("seat_reserved") or {},
            "operator_notify_suppression": {
                "enabled": True,
                "reason": "ruled_flat_weekend_state_matches_fable_20260711T2138",
                "suppress_repeat_notify_while_blocker_set_unchanged": True,
                "notify_on_blocker_set_change": True,
            },
            "next_action": reserved_hold.get("next_action") or "hold flat until temporal eligibility lifts or Fable changes the seat",
        }
    if active_set_runtime.get("enabled") is True:
        return {"active": False}
    if str(active_set_runtime.get("selection_mode") or "") != "all_active_set_members_selection_priority_frozen":
        return {"active": False}
    temporal = active_set_runtime.get("temporal_slice_exclusion")
    temporal = temporal if isinstance(temporal, dict) else {}
    fresh_selection = active_set_runtime.get("fresh_runtime_member_selection")
    fresh_selection = fresh_selection if isinstance(fresh_selection, dict) else {}
    freeze = fresh_selection.get("selection_priority_freeze")
    freeze = freeze if isinstance(freeze, dict) else {}
    freeze_rows = [row for row in freeze.get("rows") or [] if isinstance(row, dict)]
    freeze_wallets = [str(wallet).lower() for wallet in freeze.get("wallets") or [] if str(wallet)]
    excluded_members = [row for row in temporal.get("excluded_members") or [] if isinstance(row, dict)]
    if not freeze_wallets or not excluded_members:
        return {"active": False}
    direction_id = next((str(row.get("direction_id") or "") for row in freeze_rows if row.get("direction_id")), "")
    seat = {
        "direction_id": direction_id or "2026-07-11T20:04Z-fable-5960-readmission",
        "reserved_wallets": freeze_wallets,
        "activates_at": RULED_FLAT_5960_ACTIVATES_AT,
        "reason": next((str(row.get("reason") or "") for row in freeze_rows if row.get("reason")), ""),
    }
    return {
        "active": True,
        "flow_stage": "LIVE/DEFEND/ROTATE",
        "status": "RULED_FLAT_WEEKEND",
        "blockers": [
            "temporal_slice_exclusion_all_measured_members",
            f"seat_reserved(direction_id={seat['direction_id']}, activates_at={seat['activates_at']})",
        ],
        "active_slices": temporal.get("active_slices") or [],
        "temporal_excluded_wallets": temporal.get("excluded_wallets") or [
            row.get("source_wallet") for row in excluded_members if row.get("source_wallet")
        ],
        "seat_reserved": seat,
        "operator_notify_suppression": {
            "enabled": True,
            "reason": "ruled_flat_weekend_state_matches_fable_20260711T2105",
            "suppress_repeat_notify_while_blocker_set_unchanged": True,
            "notify_on_blocker_set_change": True,
        },
        "next_action": "hold flat until temporal eligibility lifts or Fable changes the seat",
    }


def _operator_notify_transition(
    previous_state: dict[str, Any],
    *,
    current_blockers: list[str],
    suppression: dict[str, Any] | None = None,
    now_iso: str | None = None,
) -> dict[str, Any]:
    previous_blockers = sorted({str(item) for item in (previous_state.get("blockers") or []) if str(item)})
    current = sorted({str(item) for item in current_blockers if str(item)})
    changed = previous_blockers != current
    suppression = suppression if isinstance(suppression, dict) else {}
    previous_transition = previous_state.get("operator_notify_transition")
    previous_transition = previous_transition if isinstance(previous_transition, dict) else {}
    previous_last_notify_at = str(previous_transition.get("last_notify_at") or previous_state.get("last_notify_at") or "")
    if changed:
        reason = "guard_blocker_set_changed"
        last_notify_at = str(now_iso or utc_now_iso())
    elif suppression.get("enabled") and suppression.get("suppress_repeat_notify_while_blocker_set_unchanged"):
        reason = "repeat_ruled_state_suppressed"
        last_notify_at = previous_last_notify_at
    else:
        reason = "guard_blocker_set_unchanged"
        last_notify_at = previous_last_notify_at
    return {
        "enabled": True,
        "notify": changed,
        "notify_on_blocker_set_change": True,
        "last_notify_at": last_notify_at,
        "previous_blockers": previous_blockers,
        "current_blockers": current,
        "blocker_set_changed": changed,
        "suppression_enabled": bool(suppression.get("enabled")),
        "reason": reason,
    }


def _latest_auto_degrade_band_filter_artifact() -> Path | None:
    matches = sorted(ROOT.glob(AUTO_DEGRADE_BAND_FILTER_GLOB), key=lambda path: path.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _auto_degrade_candidate_rows() -> list[dict[str, Any]]:
    artifact = _latest_auto_degrade_band_filter_artifact()
    if artifact is None:
        return []
    payload = load_json(artifact, default={})
    payload = payload if isinstance(payload, dict) else {}
    rows = payload.get("next5")
    if not isinstance(rows, list):
        rows = payload.get("selected") if isinstance(payload.get("selected"), list) else []
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or row.get("source_wallet") or "").lower()
        if not wallet:
            continue
        candidates.append({**row, "wallet": wallet, "artifact": str(artifact.relative_to(ROOT))})
    return candidates


def _auto_degrade_member_from_candidate(row: dict[str, Any], *, generated_at: str) -> dict[str, Any]:
    wallet = str(row.get("wallet") or row.get("source_wallet") or "").lower()
    suffix = wallet[2:12] if wallet.startswith("0x") else wallet[:10]
    policy_id = "protection_refill_0.10_cap_8_auto_degrade_le_25"
    return {
        "candidate_id": f"auto_degrade_{suffix}",
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": wallet,
        "policy_id": policy_id,
        "wallet_fraction": 0.10,
        "max_order_usd": 8.0,
        "max_price": 0.25,
        "rolling_loss_trigger_usd": -16.0,
        "enabled": True,
        "status": "AUTO_DEGRADE_PROTECTION_BOUNDED",
        "policy": {
            "policy_id": policy_id,
            "min_price": 0.0,
            "max_price": 0.25,
            "wallet_fraction": 0.10,
            "max_order_usd": 8.0,
            "min_order_usd": 1.0,
        },
        "summary": {
            "direction_id": "2026-07-05T20:40:00Z-fable-auto-degrade-mechanization",
            "promotion_basis": "active_set_expansion_next5_band_filtered",
            "artifact": row.get("artifact"),
            "resolved_pnl": row.get("resolved_pnl"),
            "copyable_rate_pct": row.get("copyable_rate_pct"),
            "le25_buy_fraction_pct": row.get("le25_buy_fraction_pct"),
            "admitted_at": generated_at,
        },
    }


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _utc_hour_in_band(now: dt.datetime, *, start_hour: int, end_hour: int) -> bool:
    start = start_hour % 24
    end = end_hour % 24
    hour = now.astimezone(dt.timezone.utc).hour
    if start == end:
        return True
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _apply_live_min_order_cap_floor(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    process_min_live_order_usd: float,
    source: str,
) -> dict[str, Any]:
    floor = max(0.0, _float_or_default(process_min_live_order_usd, 0.0))
    if floor <= 0:
        return {"active": False, "status": "CLEAR", "source": source, "reason": "floor_not_positive"}
    old_max_order = _float_or_default(
        selected_runtime_policy.get("max_order_usd", selected.get("max_order_usd")),
        0.0,
    )
    maker_funding_cap = _float_or_default(
        selected_runtime_policy.get("maker_min_share_funding_cap_usd"),
        0.0,
    )
    if old_max_order > 0 and maker_funding_cap > 0:
        effective_request_cap = max(old_max_order, floor)
        original_policy_cap = max(
            maker_funding_cap,
            _float_or_default(
                selected_runtime_policy.get(
                    "maker_min_share_original_policy_cap_usd"
                ),
                4.0,
            ),
        )
        funding_fields = {
            "maker_min_share_base_request_cap_usd": round(
                effective_request_cap, 6
            ),
            "maker_min_share_funding_cap_usd": round(maker_funding_cap, 6),
            "maker_min_share_original_policy_cap_usd": round(
                original_policy_cap, 6
            ),
        }
        selected_runtime_policy.update(funding_fields)
        selected.update(funding_fields)
        selected_policy = (
            dict(selected.get("policy"))
            if isinstance(selected.get("policy"), dict)
            else {}
        )
        selected_policy.update(funding_fields)
        selected["policy"] = selected_policy
    if old_max_order <= 0 or old_max_order >= floor:
        return {
            "active": False,
            "status": "CLEAR",
            "source": source,
            "effective_max_order_usd": round(old_max_order, 9),
            "process_min_live_order_usd": round(floor, 9),
        }
    old_min_order = _float_or_default(selected_runtime_policy.get("min_order_usd"), old_max_order)
    old_min_live = _float_or_default(selected_runtime_policy.get("min_live_order_usd"), old_min_order)
    old_tranche = _float_or_default(selected_runtime_policy.get("drip_max_tranche_usd"), old_max_order)
    clamp = {
        "flow_stage": "LIVE/DEFEND",
        "active": True,
        "status": "LIVE_MIN_ORDER_CAP_FLOOR_CLAMPED",
        "source": source,
        "direction_id": "2026-07-15T13:18Z-fable-c50d-cap-floor-1",
        "candidate_id": str(selected.get("candidate_id") or ""),
        "source_wallet": str(selected.get("source_wallet") or selected.get("wallet") or "").lower(),
        "policy_id": str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or ""),
        "old_max_order_usd": round(old_max_order, 9),
        "new_max_order_usd": round(floor, 9),
        "old_min_order_usd": round(old_min_order, 9),
        "old_min_live_order_usd": round(old_min_live, 9),
        "old_drip_max_tranche_usd": round(old_tranche, 9),
        "process_min_live_order_usd": round(floor, 9),
        "reason": "live-enabled runtime member cap cannot sit below the process min-live order",
        "rule": "live-enabled runtime max_order_usd is clamped to process_min_live_order_usd; below-floor live requires explicit PAPER_ONLY demotion",
    }
    selected_runtime_policy["max_order_usd"] = round(floor, 6)
    selected_runtime_policy["min_order_usd"] = round(floor, 6)
    selected_runtime_policy["min_live_order_usd"] = round(floor, 6)
    selected_runtime_policy["drip_max_tranche_usd"] = round(max(old_tranche, floor), 6)
    selected_runtime_policy["live_min_order_cap_floor_clamp"] = clamp
    selected["max_order_usd"] = selected_runtime_policy["max_order_usd"]
    selected["drip_max_tranche_usd"] = selected_runtime_policy["drip_max_tranche_usd"]
    policy = dict(selected.get("policy")) if isinstance(selected.get("policy"), dict) else {}
    policy.update(
        {
            "max_order_usd": selected_runtime_policy["max_order_usd"],
            "min_order_usd": selected_runtime_policy["min_order_usd"],
            "min_live_order_usd": selected_runtime_policy["min_live_order_usd"],
            "drip_max_tranche_usd": selected_runtime_policy["drip_max_tranche_usd"],
            "live_min_order_cap_floor_clamp": clamp,
        }
    )
    selected["policy"] = policy
    selected["live_min_order_cap_floor_clamp"] = clamp
    return clamp


def _apply_hour_band_size_clamp(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    bench = selected.get("hour_band_bench") if isinstance(selected.get("hour_band_bench"), dict) else {}
    policy = selected.get("policy") if isinstance(selected.get("policy"), dict) else {}
    raw_cap = (
        bench.get("morning_min_size_probe_usd")
        or policy.get("morning_min_size_probe_usd")
        or selected.get("morning_min_size_probe_usd")
    )
    cap_usd = _float_or_default(raw_cap, 0.0)
    if cap_usd <= 0:
        return {
            "active": False,
            "status": "CLEAR",
            "source": "member.hour_band_bench.morning_min_size_probe_usd",
            "reason": "morning_min_size_probe_usd_missing",
        }
    start_hour = _int_or_default(
        bench.get("morning_min_size_start_hour_utc") or policy.get("morning_min_size_start_hour_utc"),
        0,
    )
    end_hour = _int_or_default(
        bench.get("morning_min_size_end_hour_utc") or policy.get("morning_min_size_end_hour_utc"),
        13,
    )
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
    now_dt = now_dt.astimezone(dt.timezone.utc)
    if not _utc_hour_in_band(now_dt, start_hour=start_hour, end_hour=end_hour):
        return {
            "active": False,
            "status": "CLEAR",
            "source": "member.hour_band_bench.morning_min_size_probe_usd",
            "reason": "outside_morning_min_size_hour_band",
            "source_wallet": _member_wallet(selected),
            "start_hour_utc": start_hour % 24,
            "end_hour_utc": end_hour % 24,
            "as_of": now_dt.isoformat(),
        }

    old_max_order = _float_or_default(
        selected_runtime_policy.get("max_order_usd", selected.get("max_order_usd")),
        cap_usd,
    )
    old_min_order = _float_or_default(selected_runtime_policy.get("min_order_usd"), cap_usd)
    old_min_live = _float_or_default(selected_runtime_policy.get("min_live_order_usd"), old_min_order)
    old_tranche = _float_or_default(selected_runtime_policy.get("drip_max_tranche_usd"), old_max_order)

    new_max_order = min(old_max_order if old_max_order > 0 else cap_usd, cap_usd)
    new_min_order = min(old_min_order if old_min_order > 0 else new_max_order, new_max_order)
    new_min_live = min(old_min_live if old_min_live > 0 else new_min_order, new_max_order)
    new_tranche = min(old_tranche if old_tranche > 0 else new_max_order, new_max_order)
    clamp = {
        "flow_stage": "LIVE/DEFEND",
        "active": True,
        "status": "MORNING_MIN_SIZE_PROBE",
        "source": "member.hour_band_bench.morning_min_size_probe_usd",
        "source_wallet": _member_wallet(selected),
        "policy_id": str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or ""),
        "cap_usd": round(cap_usd, 6),
        "start_hour_utc": start_hour % 24,
        "end_hour_utc": end_hour % 24,
        "as_of": now_dt.isoformat(),
        "old_max_order_usd": round(old_max_order, 6),
        "new_max_order_usd": round(new_max_order, 6),
        "old_drip_max_tranche_usd": round(old_tranche, 6),
        "new_drip_max_tranche_usd": round(new_tranche, 6),
        "classification": bench.get("classification"),
        "fable_direction_id": bench.get("fable_direction_id"),
        "rule": "unproven morning hours use capped probe size; later size defenses may clamp smaller",
    }
    selected_runtime_policy["max_order_usd"] = round(new_max_order, 6)
    selected_runtime_policy["min_order_usd"] = round(new_min_order, 6)
    selected_runtime_policy["min_live_order_usd"] = round(new_min_live, 6)
    selected_runtime_policy["drip_max_tranche_usd"] = round(new_tranche, 6)
    selected_runtime_policy["hour_band_size_clamp"] = clamp
    selected["max_order_usd"] = selected_runtime_policy["max_order_usd"]
    policy_out = dict(policy)
    policy_out.update(
        {
            "max_order_usd": selected_runtime_policy["max_order_usd"],
            "min_order_usd": selected_runtime_policy["min_order_usd"],
            "min_live_order_usd": selected_runtime_policy["min_live_order_usd"],
            "drip_max_tranche_usd": selected_runtime_policy["drip_max_tranche_usd"],
            "hour_band_size_clamp": clamp,
        }
    )
    selected["policy"] = policy_out
    return clamp


def _state_digest_path(args: argparse.Namespace) -> Path:
    raw = getattr(args, "state_digest_json", STATE_DIGEST_JSON)
    path = raw if isinstance(raw, Path) else Path(str(raw))
    return path if path.is_absolute() else ROOT / path


def _policy_cap_from_policy_id(policy_id: str) -> float | None:
    match = re.search(r"(?:^|_)cap_(\d+(?:\.\d+)?)(?:_|$)", str(policy_id or ""))
    if not match:
        return None
    return _float_or_default(match.group(1), 0.0) or None


_A689_CANARY_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
_A689_CANARY_POLICY_ID = "a6896d11_price_reject_canary_0.10_cap_2_le_70"
_A689_CANARY_CAP_USD = 2.0
# 2026-07-20T07:31Z fable PROBE_SIZE ruling: hard cap 1.0 while in force. Restore to
# _A689_CANARY_CAP_USD only by fable direction after two consecutive resolved-basis
# since-topup reads >= +21 (see HANDOFF 07:31Z/08:15Z rungs). The cap must not depend
# on fable_cap_max_order_usd surviving member rebuilds: missing key fails safe to this.
_A689_FABLE_PROBE_CAP_USD = 1.0
_A689_CANARY_MAX_PRICE = 0.7
_A689_CANARY_DAY_PNL_VALID_ABOVE_USD = -6.0
_F418_PROBE_WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
_F418_PROBE_POLICY_ID = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
_F418_PROBE_CAP_USD = 1.0
_F418_PROBE_MAX_PRICE = 0.5
_WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID = "2026-07-18T07:10Z-fable-f418-seat-loss-rotation-rider"
_F418_TENURE_PIN_DIRECTION_ID = "2026-07-18T05:56Z-fable-qc1-f418-selected-rotation"
_WEEKEND_PROBE_CAP_LATCH_DIRECTION_ID = "2026-07-18T08:14Z-fable-weekend-latch"
_WEEKEND_PROBE_CAP_USD = 1.0
_WEEKEND_PROBE_CAP_LATCH_EXPIRES_AT = "2026-07-19T00:00:00Z"


def _utc_iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _path_from_arg(raw: Any, default: str | Path) -> Path:
    path = raw if isinstance(raw, Path) else Path(str(raw or default))
    return path if path.is_absolute() else ROOT / path


def _selection_pin_active(pin: dict[str, Any], *, now: dt.datetime, wallet: str | None = None) -> bool:
    if not isinstance(pin, dict) or pin.get("enabled") is False:
        return False
    pin_wallet = _member_wallet(pin)
    if wallet is not None and pin_wallet != wallet:
        return False
    quiet_clock = pin.get("quiet_clock") if isinstance(pin.get("quiet_clock"), dict) else {}
    quiet_fire = _parse_iso_datetime(quiet_clock.get("earliest_fire_iso"))
    expires_at = _parse_iso_datetime(pin.get("expires_at"))
    if (
        str(quiet_clock.get("reset_predicate") or "") == "selected_submit_eligible_copyintent"
        and quiet_fire is not None
        and (expires_at is None or quiet_fire > expires_at)
    ):
        expires_at = quiet_fire
    if expires_at is not None and now > expires_at:
        return False
    created_at = _parse_iso_datetime(pin.get("created_at") or pin.get("updated_at"))
    if (
        created_at is not None
        and (now - created_at.astimezone(dt.timezone.utc)).total_seconds() > ACTIVE_SET_SELECTION_PIN_MAX_AGE_S
        and not (
            str(quiet_clock.get("reset_predicate") or "") == "selected_submit_eligible_copyintent"
            and quiet_fire is not None
            and now <= quiet_fire
        )
    ):
        return False
    return bool(pin_wallet)


def _overlay_member_for_wallet(overlay: dict[str, Any], wallet: str) -> dict[str, Any]:
    wallet = str(wallet or "").strip().lower()
    latest = overlay.get("latest_admission") if isinstance(overlay.get("latest_admission"), dict) else {}
    if _member_wallet(latest) == wallet:
        return latest
    for row in overlay.get("members") if isinstance(overlay.get("members"), list) else []:
        if isinstance(row, dict) and _member_wallet(row) == wallet:
            return row
    return {}


def _pin_active_set_member(
    *,
    overlay: dict[str, Any],
    member: dict[str, Any],
    pin_id: str,
    direction_id: str,
    reason: str,
    now: dt.datetime,
    expires_at: dt.datetime,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    original_pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    pin = {
        "enabled": True,
        "pin_id": pin_id,
        "direction_id": direction_id,
        "created_at": _utc_iso(now),
        "expires_at": _utc_iso(expires_at),
        "candidate_id": member.get("candidate_id"),
        "source_wallet": _member_wallet(member),
        "reason": reason,
    }
    if original_pin.get("created_at"):
        pin["original_created_at"] = original_pin.get("original_created_at") or original_pin.get("created_at")
    if extra:
        pin.update(extra)
    updated = dict(overlay)
    updated["selection_pin"] = pin
    updated["updated_at"] = _utc_iso(now)
    _atomic_write_auto_degrade_overlay(updated)
    return pin


def _weekend_day_probe(args: argparse.Namespace) -> dict[str, Any]:
    if not hasattr(args, "state_digest_json"):
        return {}
    digest = load_json(_state_digest_path(args), default={})
    if not isinstance(digest, dict):
        return {}
    probe = digest.get("weekend_day_probe")
    return probe if isinstance(probe, dict) else {}


def _weekend_probe_fired(probe: dict[str, Any]) -> bool:
    rider = probe.get("seat_loss_rotation_rider") if isinstance(probe.get("seat_loss_rotation_rider"), dict) else {}
    return bool(
        probe.get("triggered") is True
        and str(probe.get("status") or "").upper() == "TRIGGERED"
        and (not rider or str(rider.get("action") or "ROTATE_F418_TO_A689") == "ROTATE_F418_TO_A689")
    )


def _weekend_probe_cap_latch_active(latch: dict[str, Any], *, now: dt.datetime) -> bool:
    if not isinstance(latch, dict) or not latch or latch.get("enabled") is False or latch.get("active") is False:
        return False
    expires_at = _parse_iso_datetime(latch.get("expires_at"))
    if expires_at is not None and now >= expires_at.astimezone(dt.timezone.utc):
        return False
    return str(latch.get("status") or "ACTIVE").upper() == "ACTIVE"


def _weekend_probe_cap_latch_payload(
    *,
    now: dt.datetime,
    reason: str,
) -> dict[str, Any]:
    return {
        "enabled": True,
        "active": True,
        "status": "ACTIVE",
        "flow_stage": "LIVE/DEFEND/WEEKEND",
        "direction_id": _WEEKEND_PROBE_CAP_LATCH_DIRECTION_ID,
        "created_at": _utc_iso(now),
        "updated_at": _utc_iso(now),
        "expires_at": _WEEKEND_PROBE_CAP_LATCH_EXPIRES_AT,
        "max_order_usd": _WEEKEND_PROBE_CAP_USD,
        "drip_max_tranche_usd": _WEEKEND_PROBE_CAP_USD,
        "caps": {"max_order_usd": _WEEKEND_PROBE_CAP_USD, "drip_max_tranche_usd": _WEEKEND_PROBE_CAP_USD},
        "restore_caps": {"max_order_usd": 8.0, "drip_max_tranche_usd": 2.5},
        "restore_rule": "clear latch on first hourly UTC-day check of 2026-07-19 while applying standard caps",
        "reason": reason,
    }


def _weekend_probe_cap_latch(
    *,
    overlay: dict[str, Any],
    now: dt.datetime,
    should_persist: bool,
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    latch = overlay.get("weekend_probe_cap_latch") if isinstance(overlay.get("weekend_probe_cap_latch"), dict) else {}
    expires_at = _parse_iso_datetime(latch.get("expires_at"))
    if latch and expires_at is not None and now >= expires_at.astimezone(dt.timezone.utc):
        updated = dict(overlay)
        cleared = dict(latch)
        cleared.update(
            {
                "enabled": False,
                "active": False,
                "status": "EXPIRED",
                "cleared_at": _utc_iso(now),
                "updated_at": _utc_iso(now),
                "next_action": "restore standard caps 8.0/2.5 in the same UTC-day restore runbook step",
            }
        )
        updated["weekend_probe_cap_latch"] = cleared
        updated["updated_at"] = _utc_iso(now)
        _atomic_write_auto_degrade_overlay(updated)
        return cleared, updated
    if _weekend_probe_cap_latch_active(latch, now=now):
        return latch, overlay
    if not should_persist:
        return latch, overlay

    updated = dict(overlay)
    active = _weekend_probe_cap_latch_payload(now=now, reason=reason)
    updated["weekend_probe_cap_latch"] = active
    updated["updated_at"] = _utc_iso(now)
    _atomic_write_auto_degrade_overlay(updated)
    return active, updated


def _weekend_probe_cap_guard_flag_check(
    args: argparse.Namespace,
    probe: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc)
    overlay = _load_auto_degrade_active_set_overlay()
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    a689_weekend_pin_active = (
        _selection_pin_active(pin, now=now, wallet=_A689_CANARY_WALLET)
        and str(pin.get("direction_id") or "") == _WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID
    )
    selected_snapshot = _latest_runtime_selected_wallet(args)
    a689_runtime_selected = _member_wallet(selected_snapshot) == _A689_CANARY_WALLET
    latch_expires_at = _parse_iso_datetime(_WEEKEND_PROBE_CAP_LATCH_EXPIRES_AT)
    fable_latch_window_active = latch_expires_at is not None and now < latch_expires_at.astimezone(dt.timezone.utc)
    probe_fired = _weekend_probe_fired(probe)
    latch, overlay = _weekend_probe_cap_latch(
        overlay=overlay,
        now=now,
        should_persist=probe_fired or a689_weekend_pin_active or a689_runtime_selected or fable_latch_window_active,
        reason=(
            "weekend probe fired"
            if probe_fired
            else "a689 weekend rider pin active"
            if a689_weekend_pin_active
            else "fable weekend cap latch active until next UTC-day restore"
            if fable_latch_window_active
            else "a689 runtime selected under weekend rider"
        ),
    )
    latch_active = _weekend_probe_cap_latch_active(latch, now=now)
    if not probe_fired and not latch_active:
        return {
            "enabled": True,
            "flow_stage": "LIVE/DEFEND/WEEKEND",
            "status": "CLEAR",
            "reason": "weekend_probe_not_triggered",
            "weekend_probe_status": probe.get("status"),
            "weekend_probe_triggered": bool(probe.get("triggered")),
            "weekend_probe_cap_latch": latch if isinstance(latch, dict) else {},
        }
    cap_usd = _float_or_default(
        latch.get("max_order_usd") if latch_active else probe.get("probe_cap_usd"),
        _WEEKEND_PROBE_CAP_USD,
    )
    if cap_usd <= 0:
        cap_usd = _WEEKEND_PROBE_CAP_USD
    max_order_usd = _float_or_default(getattr(args, "max_order_usd", None), 0.0)
    drip_max_tranche_usd = _float_or_default(getattr(args, "drip_max_tranche_usd", None), 0.0)
    guard_state = load_json(_path_from_arg(getattr(args, "state", ""), "data/research/wallet_copy_live_guard_state.json"), default={})
    runtime = guard_state.get("active_set_runtime") if isinstance(guard_state, dict) else {}
    runtime = runtime if isinstance(runtime, dict) else {}
    selected_member = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    selected_policy = selected_member.get("policy") if isinstance(selected_member.get("policy"), dict) else {}
    selected_wallet = _member_wallet(selected_member) or _member_wallet(selected_snapshot)
    selected_max_order_usd = _float_or_default(
        selected_member.get("max_order_usd", selected_policy.get("max_order_usd")),
        0.0,
    )
    selected_drip_max_tranche_usd = _float_or_default(
        selected_member.get("drip_max_tranche_usd", selected_policy.get("drip_max_tranche_usd")),
        0.0,
    )
    use_effective_selected_cap = (
        selected_wallet == _A689_CANARY_WALLET
        and selected_max_order_usd > 0
        and selected_drip_max_tranche_usd > 0
    )
    checked_max_order_usd = selected_max_order_usd if use_effective_selected_cap else max_order_usd
    checked_drip_max_tranche_usd = (
        selected_drip_max_tranche_usd if use_effective_selected_cap else drip_max_tranche_usd
    )
    exceeds = checked_max_order_usd > cap_usd + 1e-9 or checked_drip_max_tranche_usd > cap_usd + 1e-9
    return {
        "enabled": True,
        "flow_stage": "LIVE/DEFEND/WEEKEND",
        "status": "FAIL_PROBE_CAP_FLAGS_EXCEED_RUNBOOK" if exceeds else "PASS",
        "passed": not exceeds,
        "probe_cap_usd": round(cap_usd, 6),
        "guard_max_order_usd": round(max_order_usd, 6),
        "guard_drip_max_tranche_usd": round(drip_max_tranche_usd, 6),
        "selected_wallet": selected_wallet,
        "selected_effective_max_order_usd": round(selected_max_order_usd, 6),
        "selected_effective_drip_max_tranche_usd": round(selected_drip_max_tranche_usd, 6),
        "check_surface": (
            "normalized_selected_a689_plus_trade_executor_proof_v3"
            if use_effective_selected_cap
            else "process_flags_non_a689_or_missing_effective_cap"
        ),
        "proof_standard_v3": "accepted a689 BUY actual filled cost <= effective cap + 0.10 USD",
        "weekend_probe_status": probe.get("status"),
        "weekend_probe_triggered": bool(probe.get("triggered")),
        "weekend_probe_cap_latch": latch if isinstance(latch, dict) else {},
        "runbook": "floor_breach_probe_cap_runbook.json",
        "direction_id": _WEEKEND_PROBE_CAP_LATCH_DIRECTION_ID if latch_active else "2026-07-18T07:55Z-fable-weekend-probe-cap-reload",
        "next_action": (
            "apply probe process flags before any non-a689 member's first order, or restore selected-member effective cap evidence"
            if exceeds
            else "keep effective selected-member probe cap plus proof-v3 for rest of UTC day or until restore rule fires"
        ),
    }


def _weekend_rider_result(
    result: dict[str, Any],
    *,
    args: argparse.Namespace,
    probe: dict[str, Any],
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    cap_check = _weekend_probe_cap_guard_flag_check(args, probe, now=now)
    result["probe_caps_guard_flag_check"] = cap_check
    if cap_check.get("passed") is False:
        result["status_without_probe_cap_check"] = result.get("status")
        result["status"] = "PROBE_CAPS_GUARD_FLAGS_EXCEED_RUNBOOK"
        result["next_action"] = cap_check.get("next_action")
    return result


def _runtime_selected_wallet_from_payload(payload: dict[str, Any]) -> tuple[str, str, dt.datetime | None, str]:
    if not isinstance(payload, dict):
        return "", "", None, ""
    runtime = payload.get("active_set_runtime") if isinstance(payload.get("active_set_runtime"), dict) else {}
    selected = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    if selected:
        return (
            _member_wallet(selected),
            str(selected.get("candidate_id") or ""),
            _parse_iso_datetime(payload.get("generated_at")),
            "live_guard_state.active_set_runtime.selected_member",
        )
    permission = payload.get("runtime_permission") if isinstance(payload.get("runtime_permission"), dict) else {}
    details = permission.get("details") if isinstance(permission.get("details"), dict) else {}
    wallet = str(details.get("source_wallet") or "").strip().lower()
    candidate_id = str(details.get("candidate_id") or "")
    if wallet:
        return wallet, candidate_id, _parse_iso_datetime(permission.get("updated_at")), "live_execution.runtime_permission.details"
    active_set = details.get("active_set") if isinstance(details.get("active_set"), dict) else {}
    for row in active_set.get("members") if isinstance(active_set.get("members"), list) else []:
        if isinstance(row, dict) and row.get("is_current_cycle_member"):
            return (
                _member_wallet(row),
                str(row.get("candidate_id") or ""),
                _parse_iso_datetime(permission.get("updated_at")),
                "live_execution.runtime_permission.active_set.current_member",
            )
    return "", "", None, ""


def _latest_runtime_selected_wallet(args: argparse.Namespace) -> dict[str, Any]:
    snapshots: list[dict[str, Any]] = []
    for raw_path, source_name in (
        (getattr(args, "state", "data/research/wallet_copy_live_guard_state.json"), "live_guard_state"),
        (getattr(args, "live_ledger_state", "data/research/wallet_copy_live_execution_state.json"), "live_execution_state"),
    ):
        path = _path_from_arg(raw_path, "")
        payload = load_json(path, default={})
        wallet, candidate_id, ts, source = _runtime_selected_wallet_from_payload(payload if isinstance(payload, dict) else {})
        if wallet:
            snapshots.append(
                {
                    "source_wallet": wallet,
                    "candidate_id": candidate_id,
                    "ts": ts,
                    "source": source or source_name,
                    "state": str(path),
                }
            )
    if not snapshots:
        return {}
    snapshots.sort(key=lambda row: row.get("ts") or dt.datetime.min.replace(tzinfo=dt.timezone.utc), reverse=True)
    best = dict(snapshots[0])
    ts = best.pop("ts", None)
    best["as_of"] = None if ts is None else _utc_iso(ts)
    return best


def _maybe_refresh_f418_quiet_clock_selection_pin(
    args: argparse.Namespace,
    *,
    generated_at: str,
) -> dict[str, Any]:
    now = _parse_iso_datetime(generated_at) or dt.datetime.now(dt.timezone.utc)
    overlay = _load_auto_degrade_active_set_overlay()
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    if (
        _selection_pin_active(pin, now=now, wallet=_A689_CANARY_WALLET)
        and (
            str(pin.get("direction_id") or "") == _WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID
            or str(pin.get("pin_id") or "") == "weekend-seat-loss-f418-to-a689"
        )
    ):
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
            "status": "SUPPRESSED_BY_WEEKEND_SEAT_LOSS_RIDER",
            "source_wallet": _F418_PROBE_WALLET,
            "current_seat_wallet": _A689_CANARY_WALLET,
            "pin_id": pin.get("pin_id"),
            "expires_at": pin.get("expires_at"),
        }
    packet = load_json(ACTIVE_SET_ROTATION_PACKET_STATE, default={})
    if not isinstance(packet, dict):
        return {"enabled": False, "flow_stage": "LIVE/ROTATE", "status": "NO_ROTATION_PACKET"}
    quiet = packet.get("quiet_clock") if isinstance(packet.get("quiet_clock"), dict) else {}
    selected_wallet = str(packet.get("selected_wallet") or packet.get("presumptive_target") or "").strip().lower()
    if selected_wallet != _F418_PROBE_WALLET:
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE",
            "status": "CLEAR_SELECTED_WALLET_NOT_F418",
            "selected_wallet": selected_wallet,
        }
    if quiet.get("fires_now") is True:
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE",
            "status": "QUIET_CLOCK_FIRES_NOW",
            "selected_wallet": selected_wallet,
            "earliest_fire_iso": quiet.get("earliest_fire_iso"),
        }
    earliest_fire = _parse_iso_datetime(quiet.get("earliest_fire_iso"))
    if earliest_fire is None or now >= earliest_fire:
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE",
            "status": "QUIET_CLOCK_NOT_HOLDABLE",
            "selected_wallet": selected_wallet,
            "earliest_fire_iso": quiet.get("earliest_fire_iso"),
        }
    member = _overlay_member_for_wallet(overlay, _F418_PROBE_WALLET)
    if not member:
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE",
            "status": "F418_MEMBER_MISSING",
            "selected_wallet": selected_wallet,
            "earliest_fire_iso": quiet.get("earliest_fire_iso"),
        }
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    created_at = _parse_iso_datetime(pin.get("created_at") or pin.get("updated_at"))
    age_s = None if created_at is None else max(0.0, (now - created_at.astimezone(dt.timezone.utc)).total_seconds())
    if (
        _selection_pin_active(pin, now=now, wallet=_F418_PROBE_WALLET)
        and age_s is not None
        and age_s < ACTIVE_SET_SELECTION_PIN_REFRESH_AFTER_S
    ):
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE",
            "status": "F418_PIN_ALREADY_ACTIVE",
            "source_wallet": _F418_PROBE_WALLET,
            "candidate_id": pin.get("candidate_id"),
            "pin_id": pin.get("pin_id"),
            "pin_age_s": round(age_s, 6),
            "earliest_fire_iso": quiet.get("earliest_fire_iso"),
        }
    refreshed_pin = _pin_active_set_member(
        overlay=overlay,
        member=member,
        pin_id="qc1-f418-tenure-selection-hold",
        direction_id=_F418_TENURE_PIN_DIRECTION_ID,
        reason="QC-1 tenure hold: f418 quiet clock has not fired, so selected seat remains f418",
        now=now,
        expires_at=earliest_fire,
        extra={
            "quiet_clock": {
                "anchor_iso": quiet.get("anchor_iso"),
                "earliest_fire_iso": quiet.get("earliest_fire_iso"),
                "reset_predicate": quiet.get("reset_predicate"),
            },
        },
    )
    return {
        "enabled": True,
        "flow_stage": "LIVE/ROTATE",
        "status": "F418_PIN_REFRESHED",
        "source_wallet": _F418_PROBE_WALLET,
        "candidate_id": refreshed_pin.get("candidate_id"),
        "pin_id": refreshed_pin.get("pin_id"),
        "earliest_fire_iso": quiet.get("earliest_fire_iso"),
    }


def _maybe_execute_weekend_seat_loss_rotation_rider(
    args: argparse.Namespace,
    *,
    generated_at: str,
) -> dict[str, Any]:
    now = _parse_iso_datetime(generated_at) or dt.datetime.now(dt.timezone.utc)
    if now.weekday() < 5:
        return {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
            "status": "SKIPPED_CURRENT_DAY_NOT_WEEKEND",
            "current_is_weekend": False,
            "weekday_utc": now.strftime("%A"),
            "submitter_verdict": "NO_SUBMIT_PATH_EVALUATED",
            "rule": "the weekend seat-loss rider performs no state reads or writes on UTC weekdays",
        }
    probe = _weekend_day_probe(args)
    if not _weekend_probe_fired(probe):
        return _weekend_rider_result(
            {
                "enabled": True,
                "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
                "status": "CLEAR",
                "weekend_day_probe_status": probe.get("status"),
                "day_pnl_usd": probe.get("day_pnl_usd"),
                "weekend_day_probe_trigger_usd": probe.get("weekend_day_probe_trigger_usd"),
                "distance_to_weekend_probe_usd": probe.get("distance_to_weekend_probe_usd"),
            },
            args=args,
            probe=probe,
            now=now,
        )
    overlay = _load_auto_degrade_active_set_overlay()
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    if _selection_pin_active(pin, now=now, wallet=_A689_CANARY_WALLET):
        return _weekend_rider_result(
            {
                "enabled": True,
                "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
                "status": "A689_PIN_ALREADY_ACTIVE",
                "target_wallet": _A689_CANARY_WALLET,
                "pin_id": pin.get("pin_id"),
                "expires_at": pin.get("expires_at"),
                "day_pnl_usd": probe.get("day_pnl_usd"),
                "weekend_day_probe_trigger_usd": probe.get("weekend_day_probe_trigger_usd"),
            },
            args=args,
            probe=probe,
            now=now,
        )
    selected_snapshot = _latest_runtime_selected_wallet(args)
    seat_wallet = _member_wallet(pin) if _selection_pin_active(pin, now=now) else str(selected_snapshot.get("source_wallet") or "")
    if seat_wallet != _F418_PROBE_WALLET:
        return _weekend_rider_result(
            {
                "enabled": True,
                "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
                "status": "NOT_APPLIED_CURRENT_SEAT_NOT_F418",
                "current_seat_wallet": seat_wallet,
                "current_seat_source": "active_set_overlay.selection_pin"
                if _selection_pin_active(pin, now=now)
                else selected_snapshot.get("source"),
                "day_pnl_usd": probe.get("day_pnl_usd"),
                "weekend_day_probe_trigger_usd": probe.get("weekend_day_probe_trigger_usd"),
            },
            args=args,
            probe=probe,
            now=now,
        )
    target = _overlay_member_for_wallet(overlay, _A689_CANARY_WALLET)
    if not target:
        return _weekend_rider_result(
            {
                "enabled": True,
                "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
                "status": "TARGET_A689_MEMBER_MISSING",
                "current_seat_wallet": seat_wallet,
                "day_pnl_usd": probe.get("day_pnl_usd"),
                "weekend_day_probe_trigger_usd": probe.get("weekend_day_probe_trigger_usd"),
                "next_action": "restore a689 active-set member before the weekend probe can rotate the seat",
            },
            args=args,
            probe=probe,
            now=now,
        )
    expires_at = now + dt.timedelta(seconds=ACTIVE_SET_SELECTION_PIN_MAX_AGE_S)
    rider_pin = _pin_active_set_member(
        overlay=overlay,
        member=target,
        pin_id="weekend-seat-loss-f418-to-a689",
        direction_id=_WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID,
        reason="Fable weekend -8.0 seat-loss rider fired; rotate selected member from f418 to a689",
        now=now,
        expires_at=expires_at,
        extra={
            "weekend_day_probe": {
                "day_pnl_usd": probe.get("day_pnl_usd"),
                "weekend_day_probe_trigger_usd": probe.get("weekend_day_probe_trigger_usd"),
                "distance_to_weekend_probe_usd": probe.get("distance_to_weekend_probe_usd"),
            },
        },
    )
    updated = _load_auto_degrade_active_set_overlay()
    updated["latest_weekend_seat_loss_rotation_rider"] = {
        "enabled": True,
        "status": "SEAT_LOSS_ROTATION_PIN_WRITTEN",
        "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
        "direction_id": _WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID,
        "trigger_wallet": _F418_PROBE_WALLET,
        "target_wallet": _A689_CANARY_WALLET,
        "target_candidate_id": rider_pin.get("candidate_id"),
        "generated_at": _utc_iso(now),
        "weekend_day_probe": rider_pin.get("weekend_day_probe"),
    }
    _atomic_write_auto_degrade_overlay(updated)
    return _weekend_rider_result(
        {
            "enabled": True,
            "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
            "status": "SEAT_LOSS_ROTATION_PIN_WRITTEN",
            "trigger_wallet": _F418_PROBE_WALLET,
            "target_wallet": _A689_CANARY_WALLET,
            "target_candidate_id": rider_pin.get("candidate_id"),
            "pin_id": rider_pin.get("pin_id"),
            "expires_at": rider_pin.get("expires_at"),
            "day_pnl_usd": probe.get("day_pnl_usd"),
            "weekend_day_probe_trigger_usd": probe.get("weekend_day_probe_trigger_usd"),
            "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the only live order submitter",
        },
        args=args,
        probe=probe,
        now=now,
    )


def _is_a689_canary_carveout_active(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    tripwires: dict[str, Any],
) -> bool:
    wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").lower()
    policy_id = str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or "")
    day_pnl = _float_or_default(tripwires.get("t1_day_pnl_usd"), -999999.0)
    return (
        wallet == _A689_CANARY_WALLET
        and policy_id == _A689_CANARY_POLICY_ID
        and day_pnl > _A689_CANARY_DAY_PNL_VALID_ABOVE_USD
    )


def _is_f418_probe_cap_preservation_active(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    tripwires: dict[str, Any],
) -> bool:
    wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").lower()
    policy_id = str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or "")
    day_pnl = _float_or_default(tripwires.get("t1_day_pnl_usd"), -999999.0)
    return (
        wallet == _F418_PROBE_WALLET
        and policy_id == _F418_PROBE_POLICY_ID
        and day_pnl > _A689_CANARY_DAY_PNL_VALID_ABOVE_USD
    )


_FABLE_01A_SIZE_CAP_DIRECTION_ID = "2026-08-02T17:00Z-fable-01a-only-size-cap-4"
_FABLE_01A_SIZE_CAP_POLICY_ID = "wide_fp_538b6b5a3fe49bd6d82b6eb3"
_FABLE_01A_SIZE_CAP_WALLET = "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b"
_FABLE_01A_SIZE_CAP_USD = 4.0
_FABLE_0850_01A_PRECISION_D2_DIRECTION_ID = "2026-08-03T08:50Z-fable-01a-precision-d2"


def _is_fable_01a_size_cap_requested(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
) -> bool:
    """Recognize the ruled 01a stake request before bankroll admission."""

    wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").lower()
    policy_id = str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or "")
    ruled_cap = _float_or_default(
        selected.get("fable_cap_max_order_usd", selected_runtime_policy.get("max_order_usd")),
        0.0,
    )
    return (
        wallet == _FABLE_01A_SIZE_CAP_WALLET
        and policy_id == _FABLE_01A_SIZE_CAP_POLICY_ID
        and ruled_cap >= _FABLE_01A_SIZE_CAP_USD
    )


def _is_fable_01a_size_cap_active(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    stake_admissible_max_usd: float,
) -> bool:
    """Activate the ruled stake only when the loss ladder can contain it."""

    return _is_fable_01a_size_cap_requested(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
    ) and _FABLE_01A_SIZE_CAP_USD <= stake_admissible_max_usd


def _apply_unconditional_bankroll_stake_admission(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    digest: dict[str, Any],
) -> dict[str, Any]:
    """Clamp the ruled 01a stake on every runtime path, including clean days."""

    requested = _is_fable_01a_size_cap_requested(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
    )
    tripwires = digest.get("defense_tripwires") if isinstance(digest.get("defense_tripwires"), dict) else {}
    ceiling = _float_or_default(tripwires.get("stake_admissible_max_usd"), 0.0)
    if not requested:
        return {
            "active": False,
            "status": "CLEAR",
            "requested_stake_usd": None,
            "stake_admissible_max_usd": ceiling or None,
        }
    if ceiling > 0 and _FABLE_01A_SIZE_CAP_USD <= ceiling:
        return {
            "active": False,
            "status": "ADMISSIBLE",
            "requested_stake_usd": _FABLE_01A_SIZE_CAP_USD,
            "stake_admissible_max_usd": ceiling,
        }

    probe_cap = _float_or_default(tripwires.get("probe_caps_cap_usd"), 1.0)
    if probe_cap <= 0:
        probe_cap = 1.0
    safe_cap = min(probe_cap, ceiling) if ceiling > 0 else probe_cap
    old_cap = _float_or_default(selected_runtime_policy.get("max_order_usd"), _FABLE_01A_SIZE_CAP_USD)
    for target in (selected_runtime_policy, selected):
        target["max_order_usd"] = round(min(old_cap, safe_cap), 6)
        for key in ("min_order_usd", "min_live_order_usd", "drip_min_tranche_usd", "drip_max_tranche_usd"):
            if target.get(key) is not None:
                target[key] = round(min(_float_or_default(target.get(key), safe_cap), safe_cap), 6)
        for key in (
            "maker_min_share_funding_cap_usd",
            "maker_min_share_base_request_cap_usd",
            "maker_fallback_defense_cap_usd",
        ):
            target[key] = round(safe_cap, 6)
    policy = dict(selected.get("policy")) if isinstance(selected.get("policy"), dict) else {}
    policy.update(selected_runtime_policy)
    selected["policy"] = policy
    result = {
        "active": True,
        "flow_stage": "LIVE/DEFEND",
        "status": "REFUSED_BANKROLL_LADDER",
        "direction_id": _FABLE_01A_SIZE_CAP_DIRECTION_ID,
        "requested_stake_usd": _FABLE_01A_SIZE_CAP_USD,
        "stake_admissible_max_usd": ceiling or None,
        "stake_exceeds_bankroll_admissible": True,
        "first_trigger_clamp_stake_usd": tripwires.get("first_trigger_clamp_stake_usd"),
        "armed_clear_posture_collapse_stake_usd": tripwires.get(
            "armed_clear_posture_collapse_stake_usd"
        ),
        "ladder_degenerate": bool(tripwires.get("ladder_degenerate")),
        "effective_cap_usd": round(safe_cap, 6),
        "reason": "requested stake exceeds bankroll-admissible ceiling"
        if ceiling > 0
        else "bankroll-admissible ceiling missing; fail closed to probe cap",
    }
    selected_runtime_policy["bankroll_stake_admission"] = result
    selected["bankroll_stake_admission"] = result
    selected["policy"]["bankroll_stake_admission"] = result
    return result


def _apply_probe_cap_size_defense(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    if not hasattr(args, "state_digest_json"):
        return {
            "active": False,
            "status": "CLEAR",
            "source": "state_digest.defense_tripwires",
            "reason": "state_digest_json_arg_not_configured",
        }
    digest = load_json(_state_digest_path(args), default={})
    bankroll_admission = _apply_unconditional_bankroll_stake_admission(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        digest=digest,
    )
    tripwires = _latched_intraday_probe_tripwires(args=args, digest=digest)
    if str(tripwires.get("size_defense_action") or "") != "PROBE_CAPS_REST_OF_UTC_DAY":
        if bankroll_admission.get("active") is True:
            return bankroll_admission
        return {
            "active": False,
            "status": "CLEAR",
            "source": "state_digest.defense_tripwires",
        }
    cap_usd = _float_or_default(tripwires.get("probe_caps_cap_usd"), 1.0)
    weight = _float_or_default(tripwires.get("probe_caps_weight"), 0.10)
    if cap_usd <= 0:
        cap_usd = 1.0
    if weight <= 0:
        weight = 0.10
    policy_id = str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or "")
    old_max_order = _float_or_default(
        selected_runtime_policy.get("max_order_usd", selected.get("max_order_usd")),
        cap_usd,
    )
    old_fraction = _float_or_default(
        selected_runtime_policy.get("wallet_fraction", selected.get("wallet_fraction")),
        weight,
    )
    old_min_order = _float_or_default(selected_runtime_policy.get("min_order_usd"), cap_usd)
    old_min_live = _float_or_default(selected_runtime_policy.get("min_live_order_usd"), old_min_order)
    old_tranche = _float_or_default(selected_runtime_policy.get("drip_max_tranche_usd"), cap_usd)

    policy_cap = _policy_cap_from_policy_id(policy_id)
    a689_canary_carveout = _is_a689_canary_carveout_active(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        tripwires=tripwires,
    )
    f418_probe_carveout = _is_f418_probe_cap_preservation_active(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        tripwires=tripwires,
    )
    fable_01a_size_cap_requested = _is_fable_01a_size_cap_requested(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
    )
    fable_01a_size_cap = _is_fable_01a_size_cap_active(
        selected=selected,
        selected_runtime_policy=selected_runtime_policy,
        stake_admissible_max_usd=_float_or_default(
            bankroll_admission.get("stake_admissible_max_usd"),
            0.0,
        ),
    )
    if a689_canary_carveout:
        effective_cap_usd = _A689_FABLE_PROBE_CAP_USD
        size_defense_status = "A689_CANARY_PROBE_CAP_CARVEOUT"
    elif f418_probe_carveout:
        effective_cap_usd = _F418_PROBE_CAP_USD
        size_defense_status = "F418_100USD_SOURCE_DROUGHT_READMISSION_CAP"
    elif fable_01a_size_cap:
        effective_cap_usd = _FABLE_01A_SIZE_CAP_USD
        size_defense_status = "FABLE_01A_ONLY_SIZE_CAP_4"
    elif fable_01a_size_cap_requested:
        effective_cap_usd = cap_usd
        size_defense_status = "REFUSED_BANKROLL_LADDER"
    else:
        effective_cap_usd = cap_usd
        size_defense_status = "PROBE_CAPS_REST_OF_UTC_DAY"
    baseline_max_order = max(old_max_order, policy_cap or 0.0)
    new_max_order = min(baseline_max_order if baseline_max_order > 0 else effective_cap_usd, effective_cap_usd)
    new_fraction = min(old_fraction if old_fraction > 0 else weight, weight)
    repair_decayed_cap = bool(policy_cap and old_max_order < new_max_order)
    repaired_min_order = max(old_min_order, new_max_order) if repair_decayed_cap else old_min_order
    repaired_min_live = max(old_min_live, new_max_order) if repair_decayed_cap else old_min_live
    repaired_tranche = max(old_tranche, new_max_order) if repair_decayed_cap else old_tranche
    new_min_order = min(repaired_min_order if repaired_min_order > 0 else effective_cap_usd, new_max_order)
    new_min_live = min(repaired_min_live if repaired_min_live > 0 else new_min_order, new_max_order)
    new_tranche = min(repaired_tranche if repaired_tranche > 0 else new_max_order, new_max_order)
    new_min_tranche = min(
        _float_or_default(selected_runtime_policy.get("drip_min_tranche_usd"), new_min_order),
        new_max_order,
    )
    if f418_probe_carveout:
        new_min_tranche = min(new_min_order, new_max_order)
    if fable_01a_size_cap:
        # Out-of-band intents terminate before submit, so this tranche applies
        # only to the ruled [0.25, 0.32) entry band.
        new_tranche = new_max_order

    selected_runtime_policy["wallet_fraction"] = round(new_fraction, 6)
    selected_runtime_policy["max_order_usd"] = round(new_max_order, 6)
    selected_runtime_policy["min_order_usd"] = round(new_min_order, 6)
    selected_runtime_policy["min_live_order_usd"] = round(new_min_live, 6)
    selected_runtime_policy["drip_min_tranche_usd"] = round(new_min_tranche, 6)
    selected_runtime_policy["drip_max_tranche_usd"] = round(new_tranche, 6)
    # The intraday probe cap is a strict defense. Do not let the normal
    # five-share venue-minimum allowance expand it after the tripwire fires.
    selected_runtime_policy["maker_min_share_funding_cap_usd"] = round(
        new_max_order, 6
    )
    selected_runtime_policy["maker_min_share_original_policy_cap_usd"] = round(
        policy_cap or 4.0, 6
    )
    selected_runtime_policy["maker_min_share_base_request_cap_usd"] = round(
        new_max_order, 6
    )
    selected_runtime_policy["maker_fallback_defense_cap_usd"] = round(
        new_max_order, 6
    )
    selected_runtime_policy["size_defense"] = {
        "flow_stage": "LIVE/DEFEND",
        "status": size_defense_status,
        "source": "state_digest.defense_tripwires",
        "day_utc": (
            (digest.get("pnl") or {}).get("day_utc")
            if isinstance(digest.get("pnl"), dict)
            else None
        ),
        "probe_caps_cap_usd": round(cap_usd, 6),
        "effective_cap_usd": round(effective_cap_usd, 6),
        "a689_canary_carveout": a689_canary_carveout,
        "f418_probe_carveout": f418_probe_carveout,
        "fable_01a_size_cap": fable_01a_size_cap,
        "fable_01a_size_cap_requested": fable_01a_size_cap_requested,
        "stake_admissible_max_usd": bankroll_admission.get("stake_admissible_max_usd"),
        "stake_exceeds_bankroll_admissible": bool(
            bankroll_admission.get("stake_exceeds_bankroll_admissible")
        ),
        "ladder_degenerate": bool(tripwires.get("ladder_degenerate")),
        "bankroll_stake_admission": bankroll_admission,
        "fable_01a_size_cap_direction_id": (
            _FABLE_01A_SIZE_CAP_DIRECTION_ID if fable_01a_size_cap else None
        ),
        "probe_caps_weight": round(weight, 6),
        "policy_cap_usd": None if policy_cap is None else round(policy_cap, 6),
        "baseline_max_order_usd": round(baseline_max_order, 6),
        "repair_decayed_cap": repair_decayed_cap,
        "day_pnl_usd": tripwires.get("t1_day_pnl_usd"),
        "since_topup_actual_usd": tripwires.get("t1_since_topup_actual_usd"),
        "single_fill_probe_triggered": bool(tripwires.get("single_fill_probe_triggered")),
        "intraday_probe_triggered": bool(tripwires.get("intraday_probe_triggered")),
        "intraday_probe_latched": bool(tripwires.get("intraday_probe_latched")),
        "old_max_order_usd": round(old_max_order, 6),
        "new_max_order_usd": round(new_max_order, 6),
        "old_wallet_fraction": round(old_fraction, 6),
        "new_wallet_fraction": round(new_fraction, 6),
        "new_drip_min_tranche_usd": round(new_min_tranche, 6),
        "new_drip_max_tranche_usd": round(new_tranche, 6),
        "drip_clamp_rule": "min(standing_effective_drip, probe_effective_cap)",
    }
    selected["wallet_fraction"] = selected_runtime_policy["wallet_fraction"]
    selected["max_order_usd"] = selected_runtime_policy["max_order_usd"]
    selected["drip_min_tranche_usd"] = selected_runtime_policy["drip_min_tranche_usd"]
    selected["drip_max_tranche_usd"] = selected_runtime_policy["drip_max_tranche_usd"]
    selected["maker_min_share_funding_cap_usd"] = selected_runtime_policy[
        "maker_min_share_funding_cap_usd"
    ]
    selected["maker_min_share_original_policy_cap_usd"] = selected_runtime_policy[
        "maker_min_share_original_policy_cap_usd"
    ]
    selected["maker_min_share_base_request_cap_usd"] = selected_runtime_policy[
        "maker_min_share_base_request_cap_usd"
    ]
    selected["maker_fallback_defense_cap_usd"] = selected_runtime_policy[
        "maker_fallback_defense_cap_usd"
    ]
    if a689_canary_carveout:
        selected["fable_cap_max_order_usd"] = round(effective_cap_usd, 6)
    policy = dict(selected.get("policy")) if isinstance(selected.get("policy"), dict) else {}
    policy.update(
        {
            "wallet_fraction": selected_runtime_policy["wallet_fraction"],
            "max_order_usd": selected_runtime_policy["max_order_usd"],
            "min_order_usd": selected_runtime_policy["min_order_usd"],
            "min_live_order_usd": selected_runtime_policy["min_live_order_usd"],
            "drip_min_tranche_usd": selected_runtime_policy["drip_min_tranche_usd"],
            "drip_max_tranche_usd": selected_runtime_policy["drip_max_tranche_usd"],
            "maker_min_share_funding_cap_usd": selected_runtime_policy[
                "maker_min_share_funding_cap_usd"
            ],
            "maker_min_share_original_policy_cap_usd": selected_runtime_policy[
                "maker_min_share_original_policy_cap_usd"
            ],
            "maker_min_share_base_request_cap_usd": selected_runtime_policy[
                "maker_min_share_base_request_cap_usd"
            ],
            "maker_fallback_defense_cap_usd": selected_runtime_policy[
                "maker_fallback_defense_cap_usd"
            ],
            "size_defense": selected_runtime_policy["size_defense"],
        }
    )
    selected["policy"] = policy
    return {
        "active": True,
        "status": (
            "REFUSED_BANKROLL_LADDER"
            if fable_01a_size_cap_requested and not fable_01a_size_cap
            else "PROBE_CAPS_REST_OF_UTC_DAY"
        ),
        "source": "state_digest.defense_tripwires",
        "candidate_id": str(selected.get("candidate_id") or ""),
        "source_wallet": str(selected.get("source_wallet") or selected.get("wallet") or "").lower(),
        "policy_id": policy_id,
        "old_max_order_usd": round(old_max_order, 6),
        "policy_cap_usd": None if policy_cap is None else round(policy_cap, 6),
        "baseline_max_order_usd": round(baseline_max_order, 6),
        "effective_cap_usd": round(effective_cap_usd, 6),
        "a689_canary_carveout": a689_canary_carveout,
        "f418_probe_carveout": f418_probe_carveout,
        "fable_01a_size_cap": fable_01a_size_cap,
        "fable_01a_size_cap_requested": fable_01a_size_cap_requested,
        "stake_admissible_max_usd": bankroll_admission.get("stake_admissible_max_usd"),
        "stake_exceeds_bankroll_admissible": bool(
            bankroll_admission.get("stake_exceeds_bankroll_admissible")
        ),
        "ladder_degenerate": bool(tripwires.get("ladder_degenerate")),
        "bankroll_stake_admission": bankroll_admission,
        "fable_01a_size_cap_direction_id": (
            _FABLE_01A_SIZE_CAP_DIRECTION_ID if fable_01a_size_cap else None
        ),
        "repair_decayed_cap": repair_decayed_cap,
        "new_max_order_usd": selected_runtime_policy["max_order_usd"],
        "old_wallet_fraction": round(old_fraction, 6),
        "new_wallet_fraction": selected_runtime_policy["wallet_fraction"],
        "new_drip_min_tranche_usd": selected_runtime_policy["drip_min_tranche_usd"],
        "new_drip_max_tranche_usd": selected_runtime_policy["drip_max_tranche_usd"],
        "maker_min_share_funding_cap_usd": selected_runtime_policy[
            "maker_min_share_funding_cap_usd"
        ],
        "maker_min_share_original_policy_cap_usd": selected_runtime_policy[
            "maker_min_share_original_policy_cap_usd"
        ],
        "maker_min_share_base_request_cap_usd": selected_runtime_policy[
            "maker_min_share_base_request_cap_usd"
        ],
        "intraday_probe_latched": bool(tripwires.get("intraday_probe_latched")),
        "next_action": (
            "keep a689 at Fable probe cap until the restore rung explicitly releases it; all other probe caps stand"
            if a689_canary_carveout
            else (
                "keep f418 at the Fable-ruled $1.00 (process min-live floor) source-drought readmission cap while day PnL remains above -6"
                if f418_probe_carveout
                else (
                    "hold effective $4 only for 01a until 40 new fills are re-measured"
                    if fable_01a_size_cap
                    else (
                        "refuse $4 as REFUSED_BANKROLL_LADDER; keep $1 until the stake ceiling rises above $4"
                        if fable_01a_size_cap_requested
                        else "keep probe caps for rest of UTC day; restore only by existing UTC rollover/defense rule"
                    )
                )
            )
        ),
    }


def _intraday_probe_latch_path(args: argparse.Namespace) -> Path:
    return _state_digest_path(args).with_name("wallet_copy_intraday_probe_cap_latch.json")


def _latched_intraday_probe_tripwires(
    *,
    args: argparse.Namespace,
    digest: dict[str, Any],
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Persist REST_OF_UTC_DAY probe semantics across level-read recovery."""
    tripwires = digest.get("defense_tripwires") if isinstance(digest.get("defense_tripwires"), dict) else {}
    effective = dict(tripwires)
    now_utc = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    pnl = digest.get("pnl") if isinstance(digest.get("pnl"), dict) else {}
    day_utc = str(pnl.get("day_utc") or now_utc.date().isoformat())
    expires_at = dt.datetime.combine(
        now_utc.date() + dt.timedelta(days=1),
        dt.time.min,
        tzinfo=dt.timezone.utc,
    )
    path = _intraday_probe_latch_path(args)
    latch = load_json(path, default={})
    latch = latch if isinstance(latch, dict) else {}
    latch_active = bool(
        latch.get("active") is True
        and str(latch.get("day_utc") or "") == day_utc
        and now_utc < (_parse_iso_datetime(latch.get("expires_at")) or expires_at)
    )
    raw_fired = str(tripwires.get("size_defense_action") or "") == "PROBE_CAPS_REST_OF_UTC_DAY"
    if raw_fired:
        raw_cap = _float_or_default(tripwires.get("probe_caps_cap_usd"), 1.0)
        raw_weight = _float_or_default(tripwires.get("probe_caps_weight"), 0.10)
        cap = min(_float_or_default(latch.get("probe_caps_cap_usd"), raw_cap), raw_cap) if latch_active else raw_cap
        weight = min(_float_or_default(latch.get("probe_caps_weight"), raw_weight), raw_weight) if latch_active else raw_weight
        created_at = latch.get("created_at") if latch_active else _utc_iso(now_utc)
        latch = {
            "active": True,
            "status": "ACTIVE",
            "flow_stage": "LIVE/DEFEND",
            "day_utc": day_utc,
            "created_at": created_at,
            "updated_at": _utc_iso(now_utc),
            "expires_at": _utc_iso(expires_at),
            "probe_caps_cap_usd": round(cap, 6),
            "probe_caps_weight": round(weight, 6),
            "source": "state_digest.defense_tripwires",
            "rule": "once fired, probe caps remain active until the next UTC midnight",
        }
        atomic_write_json(path, latch)
        latch_active = True
    elif latch and not latch_active and latch.get("status") != "EXPIRED":
        expired = dict(latch)
        expired.update({"active": False, "status": "EXPIRED", "updated_at": _utc_iso(now_utc)})
        atomic_write_json(path, expired)
    if latch_active:
        effective.update(
            {
                "size_defense_action": "PROBE_CAPS_REST_OF_UTC_DAY",
                "probe_caps_cap_usd": latch.get("probe_caps_cap_usd"),
                "probe_caps_weight": latch.get("probe_caps_weight"),
                "intraday_probe_latched": True,
                "intraday_probe_latch_day_utc": day_utc,
                "intraday_probe_latch_expires_at": latch.get("expires_at"),
            }
        )
    return effective


def _apply_probe_cap_size_defense_to_runtime_members(
    members: list[dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    clamped: list[dict[str, Any]] = []
    clear: dict[str, Any] | None = None
    for idx, member in enumerate(members):
        runtime_policy = _live_execution_policy_from_mission_member(
            member,
            fallback_policy_id=str(member.get("policy_id") or getattr(args, "policy_id", "") or ""),
        )
        for key in (
            "policy_id",
            "max_order_usd",
            "max_price",
            "wallet_fraction",
            "min_live_order_usd",
            "maker_min_share_funding_cap_usd",
            "maker_min_share_original_policy_cap_usd",
            "maker_min_share_base_request_cap_usd",
            "maker_fallback_defense_cap_usd",
        ):
            if member.get(key) is not None:
                runtime_policy[key] = member.get(key)
        if "min_live_order_usd" in runtime_policy and "min_order_usd" not in runtime_policy:
            runtime_policy["min_order_usd"] = runtime_policy.get("min_live_order_usd")
        result = _apply_probe_cap_size_defense(
            selected=member,
            selected_runtime_policy=runtime_policy,
            args=args,
        )
        if result.get("active") is True:
            clamped.append(
                {
                    "selected_member_index": idx,
                    "candidate_id": str(member.get("candidate_id") or ""),
                    "source_wallet": str(member.get("source_wallet") or member.get("wallet") or "").lower(),
                    "policy_id": str(member.get("policy_id") or runtime_policy.get("policy_id") or ""),
                    "new_max_order_usd": runtime_policy.get("max_order_usd"),
                    "new_drip_max_tranche_usd": runtime_policy.get("drip_max_tranche_usd"),
                }
            )
        elif clear is None:
            clear = result
    if not clamped:
        return clear or {
            "active": False,
            "status": "CLEAR",
            "source": "state_digest.defense_tripwires",
        }
    return {
        "active": True,
        "status": "PROBE_CAPS_REST_OF_UTC_DAY",
        "source": "state_digest.defense_tripwires",
        "clamped_member_count": len(clamped),
        "clamped_members": clamped,
        "rule": "probe caps are applied to every runtime member before selection, probes, and policy_by_wallet export",
        "next_action": "keep probe caps for rest of UTC day; restore only by existing UTC rollover/defense rule",
    }


def _runtime_member_submittability(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
    configured_min_live_order_usd: float,
    generated_at: str,
) -> dict[str, Any]:
    policy_max_order_usd = _float_or_default(
        selected_runtime_policy.get("max_order_usd"), 0.0
    )
    member_max_order_usd = _float_or_default(selected.get("max_order_usd"), 0.0)
    positive_cap_inputs = [
        cap for cap in (policy_max_order_usd, member_max_order_usd) if cap > 0
    ]
    effective_max_order_usd = min(positive_cap_inputs) if positive_cap_inputs else 0.0
    if policy_max_order_usd > 0 and member_max_order_usd > 0:
        if policy_max_order_usd < member_max_order_usd:
            binding_cap_source = "selected_runtime_policy.max_order_usd"
        elif member_max_order_usd < policy_max_order_usd:
            binding_cap_source = "selected.max_order_usd"
        else:
            binding_cap_source = "selected_runtime_policy.max_order_usd=selected.max_order_usd"
    elif policy_max_order_usd > 0:
        binding_cap_source = "selected_runtime_policy.max_order_usd"
    elif member_max_order_usd > 0:
        binding_cap_source = "selected.max_order_usd"
    else:
        binding_cap_source = "missing_or_zero_cap"
    process_min_live_order_usd = max(0.0, float(configured_min_live_order_usd))
    policy_min_live_order_usd = _float_or_default(selected_runtime_policy.get("min_live_order_usd"), 0.0)
    policy_min_order_usd = _float_or_default(selected_runtime_policy.get("min_order_usd"), 0.0)
    if policy_min_live_order_usd > 0:
        configured_min_live_order_usd = max(process_min_live_order_usd, policy_min_live_order_usd)
        min_order_source = (
            "process_min_live_order_usd_floor_over_selected_runtime_policy.min_live_order_usd"
            if process_min_live_order_usd > policy_min_live_order_usd
            else "selected_runtime_policy.min_live_order_usd"
        )
    elif policy_min_order_usd > 0:
        configured_min_live_order_usd = max(process_min_live_order_usd, policy_min_order_usd)
        min_order_source = (
            "process_min_live_order_usd_floor_over_selected_runtime_policy.min_order_usd"
            if process_min_live_order_usd > policy_min_order_usd
            else "selected_runtime_policy.min_order_usd"
        )
    else:
        configured_min_live_order_usd = process_min_live_order_usd
        min_order_source = "process_min_live_order_usd"
    status = "PASS"
    active = False
    next_action = "none"
    if effective_max_order_usd <= 0:
        status = "INCIDENT_MEMBER_UNSUBMITTABLE"
        active = True
        next_action = "repair missing_or_zero_cap before claiming this runtime member can submit live orders"
    elif configured_min_live_order_usd > 0 and effective_max_order_usd < configured_min_live_order_usd:
        status = "INCIDENT_MEMBER_UNSUBMITTABLE"
        active = True
        next_action = "fall through to an executable active-set member or restore this member cap before claiming live readiness"
    return {
        "flow_stage": "LIVE/DEFEND",
        "status": status,
        "active": active,
        "generated_at": generated_at,
        "candidate_id": str(selected.get("candidate_id") or ""),
        "source_wallet": str(selected.get("source_wallet") or selected.get("wallet") or "").lower(),
        "policy_id": str(selected_runtime_policy.get("policy_id") or selected.get("policy_id") or ""),
        "effective_max_order_usd": round(effective_max_order_usd, 9),
        "cap_inputs": {
            "policy_max_order_usd": round(policy_max_order_usd, 9),
            "member_max_order_usd": round(member_max_order_usd, 9),
            "binding_source": binding_cap_source,
        },
        "configured_min_live_order_usd": round(configured_min_live_order_usd, 9),
        "process_min_live_order_usd": round(process_min_live_order_usd, 9),
        "min_live_order_source": min_order_source,
        "reason": "missing_or_zero_cap" if effective_max_order_usd <= 0 else "max_below_live_minimum" if active else "submittable",
        "rule": "effective max_order_usd is the minimum positive policy/member cap and must be >= configured min_live_order_usd before a runtime member can be submittable",
        "next_action": next_action,
    }


def _apply_selection_pin_min_notional_repair(
    *,
    selected: dict[str, Any],
    selected_runtime_policy: dict[str, Any],
) -> dict[str, Any]:
    """Apply only the Fable-authorized 01a CLOB-minimum sizing repair."""
    overlay = _load_auto_degrade_active_set_overlay()
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").lower()
    pin_wallet = str(pin.get("source_wallet") or pin.get("wallet") or "").lower()
    direction_id = str(pin.get("structural_min_notional_direction_id") or "")
    requested_cap = _float_or_default(pin.get("structural_min_notional_cap_usd"), 0.0)
    authorized_direction_ids = {
        "2026-08-03T06:00Z-fable-flow-restore-sizing",
        _FABLE_0850_01A_PRECISION_D2_DIRECTION_ID,
    }
    maker_funding_cap = _float_or_default(
        pin.get("structural_min_notional_maker_funding_cap_usd"),
        2.5,
    )
    maker_funding_cap = min(max(maker_funding_cap, requested_cap), 2.5) if requested_cap > 0 else 2.5
    if (
        pin.get("enabled") is False
        or wallet != pin_wallet
        or direction_id not in authorized_direction_ids
        or requested_cap <= 0.0
    ):
        return {"active": False, "status": "CLEAR"}
    old_cap = _float_or_default(selected_runtime_policy.get("max_order_usd"), 0.0)
    new_cap = max(old_cap, requested_cap)
    table = [
        {"price": price, "clob_min_shares": 5.0, "min_notional_usd": round(5.0 * price, 6)}
        for price in (0.25, 0.28, 0.32, 0.40, 0.50)
    ]
    selected_runtime_policy["max_order_usd"] = round(new_cap, 6)
    selected_runtime_policy["drip_max_tranche_usd"] = round(
        max(_float_or_default(selected_runtime_policy.get("drip_max_tranche_usd"), 0.0), new_cap),
        6,
    )
    # Keep the ordinary request at the ruled $1.60 01a ceiling.  If that
    # request survives every other protection and a maker order needs the
    # venue's exact five-share minimum, the executor may fund precisely
    # 5 * limit_price, never more than $2.50.  This is the selected-seat-only
    # D2 exception; it does not widen the entry band or general order cap.
    selected_runtime_policy["maker_min_share_base_request_cap_usd"] = round(new_cap, 6)
    selected_runtime_policy["maker_min_share_funding_cap_usd"] = maker_funding_cap
    selected_runtime_policy["maker_min_share_original_policy_cap_usd"] = maker_funding_cap
    selected_runtime_policy["maker_fallback_defense_cap_usd"] = maker_funding_cap
    selected_runtime_policy["selected_only_size_exception"] = {
        "authority": "fable_2026-08-03T08:50Z_R1_precision_d2"
        if direction_id == _FABLE_0850_01A_PRECISION_D2_DIRECTION_ID
        else "fable_2026-08-03T06:00Z_R1b_sizing_d2",
        "reason": "market_buy_precision_infeasible_at_1usd"
        if direction_id == _FABLE_0850_01A_PRECISION_D2_DIRECTION_ID
        else "clob_min_notional_geometry",
        "max_order_usd": round(new_cap, 6),
        "maker_funding_cap_usd": round(maker_funding_cap, 6),
        "scope": "selected_only + 01a_only + sole_guard",
    }
    selected["max_order_usd"] = selected_runtime_policy["max_order_usd"]
    selected["drip_max_tranche_usd"] = selected_runtime_policy["drip_max_tranche_usd"]
    selected["selected_only_size_exception"] = dict(
        selected_runtime_policy["selected_only_size_exception"]
    )
    for key in (
        "maker_min_share_base_request_cap_usd",
        "maker_min_share_funding_cap_usd",
        "maker_min_share_original_policy_cap_usd",
        "maker_fallback_defense_cap_usd",
    ):
        selected[key] = selected_runtime_policy[key]
    policy = dict(selected.get("policy")) if isinstance(selected.get("policy"), dict) else {}
    policy.update(
        {
            "max_order_usd": selected_runtime_policy["max_order_usd"],
            "drip_max_tranche_usd": selected_runtime_policy["drip_max_tranche_usd"],
            "maker_min_share_base_request_cap_usd": selected_runtime_policy[
                "maker_min_share_base_request_cap_usd"
            ],
            "maker_min_share_funding_cap_usd": selected_runtime_policy[
                "maker_min_share_funding_cap_usd"
            ],
            "maker_min_share_original_policy_cap_usd": selected_runtime_policy[
                "maker_min_share_original_policy_cap_usd"
            ],
            "maker_fallback_defense_cap_usd": selected_runtime_policy[
                "maker_fallback_defense_cap_usd"
            ],
        }
    )
    selected["policy"] = policy
    report = {
        "active": True,
        "status": (
            "FABLE_0850_01A_PRECISION_D2_REPAIR"
            if direction_id == _FABLE_0850_01A_PRECISION_D2_DIRECTION_ID
            else "FABLE_0600_01A_MIN_NOTIONAL_REPAIR"
        ),
        "flow_stage": "LIVE/MONEY/DEFEND",
        "direction_id": direction_id,
        "source_wallet": wallet,
        "old_max_order_usd": round(old_cap, 6),
        "new_max_order_usd": round(new_cap, 6),
        "min_order_usd": selected_runtime_policy.get("min_order_usd"),
        "process_min_live_order_usd": 1.0,
        "maker_min_share_base_request_cap_usd": round(new_cap, 6),
        "maker_min_share_funding_cap_usd": round(maker_funding_cap, 6),
        "maker_min_share_rule": "exact 5 * limit_price after all other protections; hard ceiling $2.50",
        "selected_only_size_exception": selected_runtime_policy["selected_only_size_exception"],
        "clob_min_notional_table": table,
        "scope": "selected member only; 01a ceiling; no band widening",
    }
    selected_runtime_policy["structural_min_notional_repair"] = report
    selected["structural_min_notional_repair"] = report
    return report


def _auto_degrade_member_from_runtime_member(member: dict[str, Any], *, generated_at: str) -> dict[str, Any]:
    wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
    suffix = wallet[2:12] if wallet.startswith("0x") else wallet[:10]
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    policy_id = str(policy.get("policy_id") or member.get("policy_id") or "runtime_roster_auto_degrade_existing_bounds")
    max_price = min(_float_or_default(policy.get("max_price", member.get("max_price")), 0.50), 0.50)
    min_price = max(0.0, _float_or_default(policy.get("min_price"), 0.0))
    wallet_fraction = min(_float_or_default(policy.get("wallet_fraction", member.get("wallet_fraction")), 0.10), 0.10)
    policy_max_order_usd = _float_or_default(policy.get("max_order_usd", member.get("max_order_usd")), 2.0)
    member_max_order_usd = _float_or_default(member.get("max_order_usd"), policy_max_order_usd)
    positive_order_caps = [value for value in (policy_max_order_usd, member_max_order_usd) if value > 0]
    max_order_usd = min(positive_order_caps) if positive_order_caps else 1.0
    min_order_usd = min(_float_or_default(policy.get("min_order_usd"), 1.0), max_order_usd, member_max_order_usd)
    a689_canary_carveout = wallet == _A689_CANARY_WALLET and policy_id == _A689_CANARY_POLICY_ID
    if a689_canary_carveout:
        # Fable-ruled carve-out bounds ($2 cap / 0.70 max price) must survive the snapshot:
        # the source member may carry defensively-clamped runtime values, and because this
        # member replaces the wallet, clamped bounds would otherwise ratchet down permanently.
        max_price = _A689_CANARY_MAX_PRICE
        explicit_fable_cap = _float_or_default(
            member.get("fable_cap_max_order_usd"), _A689_FABLE_PROBE_CAP_USD
        )
        max_order_usd = min(
            _A689_CANARY_CAP_USD,
            explicit_fable_cap if explicit_fable_cap > 0 else _A689_FABLE_PROBE_CAP_USD,
            _A689_FABLE_PROBE_CAP_USD,
        )
        min_order_usd = min(_float_or_default(policy.get("min_order_usd"), 1.0), max_order_usd)
    f418_readmission_carveout = wallet == _F418_PROBE_WALLET and policy_id == _F418_PROBE_POLICY_ID
    if f418_readmission_carveout:
        max_price = _F418_PROBE_MAX_PRICE
        max_order_usd = _F418_PROBE_CAP_USD
        min_order_usd = min(_float_or_default(policy.get("min_order_usd"), 1.0), max_order_usd)
    runtime_policy = dict(policy)
    runtime_policy.update(
        {
            "policy_id": policy_id,
            "min_price": min_price,
            "max_price": max_price,
            "wallet_fraction": wallet_fraction,
            "max_order_usd": max_order_usd,
            "min_order_usd": min_order_usd,
        }
    )
    return {
        "candidate_id": f"runtime_auto_degrade_{suffix}",
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": wallet,
        "policy_id": policy_id,
        "wallet_fraction": wallet_fraction,
        "max_order_usd": max_order_usd,
        "max_price": max_price,
        "rolling_loss_trigger_usd": -4.0
        if f418_readmission_carveout
        else _float_or_default(member.get("rolling_loss_trigger_usd"), -16.0),
        "enabled": True,
        "status": "AUTO_DEGRADE_RUNTIME_ROSTER_PROTECTION_BOUNDED",
        "policy": runtime_policy,
        "auto_degrade_replaces_existing_wallet": True,
        **({"fable_cap_max_order_usd": max_order_usd} if a689_canary_carveout else {}),
        **({"fable_cap_max_order_usd": _F418_PROBE_CAP_USD} if f418_readmission_carveout else {}),
        **(
            {
                "f418_readmission_activation": {
                    "direction_id": "2026-07-17T07:24Z-fable-live-restore-rotate",
                    "packet_status": "PRE_RULED_ADMIT_F418_ACTIVATION",
                    "counterfactual_basis": "routing_shadow_member_attribution",
                    "measurable_resolved_intents": 15,
                    "post_fee_pnl_usd": 4.544406,
                    "basis_packet": "data/research/f418_readmission_packet_latest.json",
                    "auto_demote_rule": "RULING7 from reselection: demote if rolling live PnL <= -4.0 USD after readmission",
                }
            }
            if f418_readmission_carveout
            else {}
        ),
        "summary": {
            "direction_id": "2026-07-17T07:24Z-fable-live-restore-rotate"
            if f418_readmission_carveout
            else "2026-07-09T12:57Z-fable-runtime-roster-auto-degrade",
            "promotion_basis": (
                "mechanical source-drought rotation; f418 fresh direct Data API readmission"
                if f418_readmission_carveout
                else "freshest_runtime_roster_member_backstop_after_zero_acceptance"
            ),
            "source_candidate_id": member.get("candidate_id"),
            "source_policy_id": member.get("policy_id"),
            "admitted_at": generated_at,
            "bounds": (
                f"a689 canary carve-out bounds enforced (cap {max_order_usd:.1f} / max_price 0.70)"
                if a689_canary_carveout
                else (
                    "f418 source-drought readmission cap enforced (cap 1.0 = process min-live floor / max_price 0.50)"
                    if f418_readmission_carveout
                    else "existing runtime price/size bounds preserved; no gate loosened"
                )
            ),
            "a689_canary_carveout": a689_canary_carveout,
            "f418_readmission_carveout": f418_readmission_carveout,
        },
    }


def _auto_degrade_runtime_roster_member(
    active_set_runtime: dict[str, Any],
    *,
    generated_at: str,
    excluded_wallets: set[str] | None = None,
) -> dict[str, Any] | None:
    members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    excluded = {
        str(wallet or "").strip().lower()
        for wallet in (excluded_wallets or set())
        if str(wallet or "").strip()
    }
    enabled_members = [
        row
        for row in members
        if isinstance(row, dict)
        and not _active_set_member_is_disabled(row)
        and (
            wallet := str(
                row.get("source_wallet") or row.get("wallet") or ""
            ).strip().lower()
        )
        and wallet not in excluded
    ]
    if not enabled_members:
        return None
    selection = active_set_runtime.get("fresh_runtime_member_selection")
    selected_wallet = ""
    if isinstance(selection, dict):
        selected_wallet = str(selection.get("selected_wallet") or "").lower()
    if not selected_wallet:
        selected = active_set_runtime.get("selected_member")
        if isinstance(selected, dict):
            selected_wallet = str(selected.get("source_wallet") or selected.get("wallet") or "").lower()
    selected_member = None
    if selected_wallet:
        selected_member = next(
            (
                row
                for row in enabled_members
                if str(row.get("source_wallet") or row.get("wallet") or "").lower() == selected_wallet
            ),
            None,
        )
    if selected_member is None:
        selected_member = enabled_members[0]
    return _auto_degrade_member_from_runtime_member(selected_member, generated_at=generated_at)


def _maybe_write_auto_degrade_admission(
    *,
    active_set_runtime: dict[str, Any],
    coverage_kpi: dict[str, Any],
    live_stdout: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    threshold = float(coverage_kpi.get("coverage_incident_threshold_idle_hours") or 1.0)
    idle_hours = coverage_kpi.get("armed_idle_hours_since_latest_live_order")
    try:
        idle_value = float(idle_hours)
    except (TypeError, ValueError):
        idle_value = 0.0
    fresh_intents = int(live_stdout.get("fresh_candidate_intents") or live_stdout.get("new_live_candidate_intents") or 0)
    orders_submitted = int(live_stdout.get("orders_submitted") or 0)
    decision: dict[str, Any] = {
        "enabled": True,
        "flow_stage": "LIVE/PROMOTE",
        "status": "WATCH",
        "threshold_hours": threshold,
        "fresh_candidate_intents": fresh_intents,
        "orders_submitted": orders_submitted,
        "state_path": str(AUTO_DEGRADE_ACTIVE_SET_STATE.relative_to(ROOT)),
    }
    overlay = _load_auto_degrade_active_set_overlay()
    if overlay.get("auto_degrade_admissions_enabled") is False:
        return {
            **decision,
            "status": "NO_ACTION",
            "reason": "auto_degrade_admissions_disabled_by_runtime_overlay",
        }
    if not active_set_runtime.get("enabled") or int(active_set_runtime.get("qualified_member_count") or 0) <= 0:
        return {**decision, "status": "NO_ACTION", "reason": "active_set_not_armed_nonempty"}
    if idle_value < threshold:
        return {**decision, "status": "NO_ACTION", "reason": "live_order_idle_below_threshold"}
    if fresh_intents > 0 or orders_submitted > 0:
        return {**decision, "status": "NO_ACTION", "reason": "fresh_intent_or_order_observed"}

    active_set = _active_live_set_contract()
    active_set_members = [row for row in active_set.get("members", []) if isinstance(row, dict)]
    overlay_members = [row for row in overlay.get("members", []) if isinstance(row, dict)]
    retired_overlay_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower()
        for row in overlay_members
        if _active_set_member_is_disabled(row)
    }
    enabled_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower()
        for row in active_set_members
        if not _active_set_member_is_disabled(row)
    }
    existing_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower()
        for row in active_set_members
    } | retired_overlay_wallets
    if len(enabled_wallets) >= AUTO_DEGRADE_MAX_ACTIVE_MEMBERS:
        return {**decision, "status": "NO_ACTION", "reason": "active_set_cap_reached"}
    selected: dict[str, Any] | None = None
    selected_source = "band_filter"
    for row in _auto_degrade_candidate_rows():
        if str(row.get("wallet") or "").lower() not in existing_wallets:
            selected = row
            break
    if selected is None:
        member = _auto_degrade_runtime_roster_member(
            active_set_runtime,
            generated_at=generated_at,
            excluded_wallets=retired_overlay_wallets,
        )
        if member is None:
            return {**decision, "status": "NO_ACTION", "reason": "no_runtime_roster_member_available"}
        selected_source = "runtime_roster"
    else:
        member = _auto_degrade_member_from_candidate(selected, generated_at=generated_at)

    member_wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
    if member_wallet == FABLE_1413_A95B_WALLET and _fable_1413_e4_ratified_applies(overlay):
        member = _apply_fable_1413_a95b_cap_provenance(member)
    members = [
        dict(row)
        for row in overlay_members
        if str(row.get("source_wallet") or row.get("wallet") or "").lower() != member_wallet
    ]
    members.insert(0, member)
    payload = dict(overlay)
    payload.update(
        {
            "schema_version": 1,
            "kind": "wallet_copy_active_set_auto_degrade_state",
            "updated_at": generated_at,
            "direction_id": member.get("summary", {}).get("direction_id")
            or "2026-07-05T20:40:00Z-fable-auto-degrade-mechanization",
            "members": members,
            "latest_admission": member,
        }
    )
    if _fable_1413_e4_ratified_applies(payload):
        payload = _normalize_ratified_e4_active_set_overlay(payload)
    _atomic_write_auto_degrade_overlay(payload)
    return {
        **decision,
        "status": "AUTO_DEGRADE_ADMITTED",
        "admitted_candidate_id": member["candidate_id"],
        "admitted_wallet": member["source_wallet"],
        "policy_id": member["policy_id"],
        "source": selected_source,
    }


def _order135_journal_tail(
    path: Path,
    *,
    cutoff: dt.datetime,
    max_bytes: int = 4 * 1024 * 1024,
    chunk_bytes: int = 64 * 1024,
) -> tuple[list[dict[str, Any]], bool, int]:
    """Read a bounded JSONL tail until it covers the direct lookback window."""

    try:
        file_size = path.stat().st_size
    except OSError:
        return [], False, 0
    position = file_size
    payload = b""
    bytes_read = 0
    window_covered = False
    while position > 0 and bytes_read < max_bytes:
        take = min(chunk_bytes, position, max_bytes - bytes_read)
        position -= take
        with path.open("rb") as handle:
            handle.seek(position)
            payload = handle.read(take) + payload
        bytes_read += take
        lines = payload.splitlines()
        complete = lines if position == 0 else lines[1:]
        captured: list[float | None] = []
        for line in complete:
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(value, dict):
                captured.append(_parse_iso_ts(value.get("captured_at")))
        if any(value is not None and value < cutoff.timestamp() for value in captured):
            window_covered = True
            break
    lines = payload.splitlines()
    complete = lines if position == 0 else lines[1:]
    rows: list[dict[str, Any]] = []
    for line in complete:
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows, window_covered, bytes_read


def _order135_direct_projection(
    *,
    now: dt.datetime,
    packet_path: Path,
    journal_path: Path,
) -> dict[str, Any]:
    """Project current direct F2/F4 supply from bounded inputs, cached by packet."""

    from scripts.order_flow_deadman import (
        POLICY_CHOKE_LOOKBACK_S,
        _wide_direct_source_snapshot,
    )

    try:
        stat = packet_path.stat()
        signature = (str(packet_path), int(stat.st_mtime_ns), int(stat.st_size))
    except OSError:
        return {
            "status": "REFUSED_DIRECT_PACKET_MISSING",
            "ready": False,
            "journal_window_covered": False,
        }
    cached_signature = _ORDER135_DIRECT_PROJECTION_CACHE.get("signature")
    if cached_signature == signature:
        projection = dict(_ORDER135_DIRECT_PROJECTION_CACHE["projection"])
        updated_at = _parse_iso_ts(projection.get("updated_at"))
        packet_age_s = (
            max(0.0, now.timestamp() - updated_at)
            if updated_at is not None
            else None
        )
        projection["packet_age_s"] = packet_age_s
        projection["effective_packet_age_s"] = packet_age_s
        projection_ready = bool(
            packet_age_s is not None
            and packet_age_s <= ORDER135_PACKET_MAX_AGE_S
            and projection.get("direct_event_handoff") is True
            and projection.get("input_equals_terminal") is True
            and int(projection.get("current_attempted_buy_rows") or 0) > 0
            and projection.get("copyable_parity") is True
        )
        projection["ready"] = projection_ready
        projection["status"] = (
            "PASS"
            if projection_ready
            else "FAIL_COPYABLE_PARITY"
            if projection.get("copyable_parity") is not True
            else "FAIL"
        )
        projection["projection_cache_hit"] = True
        return projection

    packet = load_json(packet_path, default={})
    cutoff = now - dt.timedelta(seconds=POLICY_CHOKE_LOOKBACK_S + 60.0)
    journal, covered, bytes_read = _order135_journal_tail(
        journal_path,
        cutoff=cutoff,
    )
    projection = _wide_direct_source_snapshot(
        packet if isinstance(packet, dict) else {},
        now=now,
        journal=journal,
    )
    projection.update(
        {
            "journal_window_covered": covered,
            "journal_tail_bytes_read": bytes_read,
            "projection_cache_hit": False,
        }
    )
    _ORDER135_DIRECT_PROJECTION_CACHE.clear()
    _ORDER135_DIRECT_PROJECTION_CACHE.update(
        {"signature": signature, "projection": dict(projection)}
    )
    return projection


def _order135_direct_gate_pass(
    *,
    now: dt.datetime | None = None,
    fingerprint_evidence_path: Path = ORDER135_FINGERPRINT_EVIDENCE,
    deadman_path: Path = ORDER135_DEADMAN_STATE,
    freeze_shadow_path: Path = ORDER135_FREEZE_SHADOW,
    sidecar_path: Path = ORDER135_SIDECAR_STATE,
    overlay_path: Path = AUTO_DEGRADE_ACTIVE_SET_STATE,
    gate_log_path: str = ORDER135_GATE_LOG,
    direct_journal_path: Path = ORDER135_DIRECT_JOURNAL,
    direct_packet_path: Path = ORDER135_DIRECT_PACKET,
    actuator: Callable[..., tuple[dict[str, Any], dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Read, gate, and invoke ORDER135 inside one sole-guard process pass."""

    from scripts.build_copy_freeze_near_bar_allpass_dryrun_sidecar import (
        _checksum,
        _wallet_authority,
        build_sidecar,
    )
    from scripts.order_flow_deadman import _execute_policy_choke_rung_b

    evaluated_at = now or dt.datetime.now(dt.timezone.utc)
    if evaluated_at.tzinfo is None:
        evaluated_at = evaluated_at.replace(tzinfo=dt.timezone.utc)
    if evaluated_at >= ORDER135_EXPIRES_AT:
        return {
            "status": "ORDER135_UTC_DAY_EXPIRED",
            "direction_id": ORDER135_DIRECTION_ID,
            "expires_at": ORDER135_EXPIRES_AT.isoformat(),
            "invoked": False,
        }

    fingerprint_evidence = load_json(fingerprint_evidence_path, default={})
    deadman = load_json(deadman_path, default={})
    freeze_shadow = load_json(freeze_shadow_path, default={})
    deadman_checked_at = _parse_iso_ts(deadman.get("checked_at"))
    deadman_evidence_age_s = (
        max(0.0, evaluated_at.timestamp() - deadman_checked_at)
        if deadman_checked_at is not None
        else None
    )
    if (
        deadman_evidence_age_s is None
        or deadman_evidence_age_s > ORDER135_DEADMAN_EVIDENCE_MAX_AGE_S
    ):
        frozen = (
            freeze_shadow.get("primary")
            if isinstance(freeze_shadow.get("primary"), dict)
            else {}
        )
        audit = {
            "schema_version": 1,
            "kind": "order135_direct_gate_pass",
            "flow_stage": "LIVE/PROMOTE/DEFEND/LEARN",
            "evaluated_at": evaluated_at.isoformat(),
            "direction_id": ORDER135_DIRECTION_ID,
            "wallet": frozen.get("wallet"),
            "wide_policy_fingerprint": frozen.get("wide_policy_fingerprint"),
            "status": "REFUSED_STALE_DEADMAN_EVIDENCE",
            "deadman_checksum": _checksum(deadman),
            "deadman_evidence_age_s": (
                round(deadman_evidence_age_s, 6)
                if deadman_evidence_age_s is not None
                else None
            ),
            "history_halves": None,
            "history_halves_pass": None,
            "all_pass": False,
            "sidecar_all_pass": None,
            "invoked": False,
        }
        _append_jsonl(gate_log_path, audit)
        return audit
    projection = _order135_direct_projection(
        now=evaluated_at,
        packet_path=direct_packet_path,
        journal_path=direct_journal_path,
    )
    packet = build_sidecar(
        fingerprint_evidence if isinstance(fingerprint_evidence, dict) else {},
        deadman if isinstance(deadman, dict) else {},
        freeze_shadow if isinstance(freeze_shadow, dict) else {},
    )
    atomic_write_json(sidecar_path, packet)

    packet_age_s = projection.get("packet_age_s")
    within_packet_budget = bool(
        packet_age_s is not None
        and packet_age_s <= ORDER135_PACKET_MAX_AGE_S
    )
    primary = packet.get("primary") if isinstance(packet.get("primary"), dict) else {}
    checks = packet.get("checks") if isinstance(packet.get("checks"), dict) else {}
    pinned_wallet = str(primary.get("wallet") or "").lower()
    pinned_supply = (projection.get("per_wallet") or {}).get(pinned_wallet) or {}
    authority, _minimum, pin_diagnostic = _wallet_authority(
        deadman,
        pinned_wallet,
        str(primary.get("wide_policy_fingerprint") or ""),
    )
    authority_checks = (
        authority.get("checks") if isinstance(authority.get("checks"), dict) else {}
    )
    history_halves = {
        "f1_measured_positive_regime_cell": (
            checks.get("f1_measured_positive_regime_cell") is True
        ),
        "f3_not_enabled_or_cooloff_or_fading": (
            checks.get("f3_not_enabled_or_cooloff_or_fading") is True
        ),
        "active_temporal_not_proven_negative": (
            checks.get("active_temporal_not_proven_negative") is True
        ),
        "own_evidenced_policy_available": (
            authority_checks.get("own_evidenced_policy_available") is True
        ),
    }
    history_halves_pass = all(history_halves.values())
    f2_minimum = int(checks.get("f2_minimum") or 10)
    direct_f2 = {
        "attempts": int(pinned_supply.get("attempts") or 0),
        "copyable": int(pinned_supply.get("copyable") or 0),
        "minimum_attempts": f2_minimum,
        "minimum_copyable": 1,
    }
    direct_f2_pass = bool(
        direct_f2["attempts"] >= f2_minimum and direct_f2["copyable"] >= 1
    )
    direct_f4_pass = bool(
        projection.get("ready") is True
        and int(projection.get("current_attempted_buy_rows") or 0) > 0
    )
    effective_all_pass = bool(
        history_halves_pass and direct_f2_pass and direct_f4_pass
    )
    deadman_checksum = str(
        (packet.get("generation_fence") or {}).get("deadman_sha256") or ""
    )
    direct_packet_checksum = str(projection.get("checksum") or "")
    audit = {
        "schema_version": 1,
        "kind": "order135_direct_gate_pass",
        "flow_stage": "LIVE/PROMOTE/DEFEND/LEARN",
        "evaluated_at": evaluated_at.isoformat(),
        "direction_id": ORDER135_DIRECTION_ID,
        "wallet": primary.get("wallet"),
        "wide_policy_fingerprint": primary.get("wide_policy_fingerprint"),
        "sidecar_status": packet.get("status"),
        "all_pass": effective_all_pass,
        "sidecar_all_pass": checks.get("all_pass") is True,
        "history_halves_pass": history_halves_pass,
        "history_halves": history_halves,
        "packet_checksum": direct_packet_checksum,
        "direct_packet_checksum": direct_packet_checksum,
        "deadman_checksum": deadman_checksum,
        "packet_age_s": round(packet_age_s, 6) if packet_age_s is not None else None,
        "packet_max_age_s": ORDER135_PACKET_MAX_AGE_S,
        "within_packet_budget": within_packet_budget,
        "deadman_evidence_age_s": (
            round(deadman_evidence_age_s, 6)
            if deadman_evidence_age_s is not None
            else None
        ),
        "journal_window_covered": projection.get("journal_window_covered") is True,
        "projection_cache_hit": projection.get("projection_cache_hit") is True,
        "journal_tail_bytes_read": projection.get("journal_tail_bytes_read"),
        "projection_ready": projection.get("ready") is True,
        "projection_direct_event_handoff": (
            projection.get("direct_event_handoff") is True
        ),
        "projection_input_equals_terminal": (
            projection.get("input_equals_terminal") is True
        ),
        "projection_current_attempted_buy_rows": int(
            projection.get("current_attempted_buy_rows") or 0
        ),
        "projection_copyable_parity": projection.get("copyable_parity") is True,
        "projection_pinned_attempts": direct_f2["attempts"],
        "projection_pinned_copyable": direct_f2["copyable"],
        "direct_f2": direct_f2,
        "direct_f4_pass": direct_f4_pass,
        "false_checks": sorted(
            key
            for key, value in checks.items()
            if key.startswith(("f1_", "f2_", "f3_", "f4_", "active_temporal"))
            and value is False
        ),
        "invoked": False,
    }
    if (
        not authority
        or packet.get("status") == "WAIT_PIN_UNRESOLVED"
        or pin_diagnostic["pin_fingerprint_unmatched_in_supply"]
    ):
        supply_fingerprints = pin_diagnostic["supply_fingerprints_for_pinned_wallet"]
        unmatched = pin_diagnostic["pin_fingerprint_unmatched_in_supply"]
        if unmatched:
            # wallet is on the frontier but under a different policy fingerprint
            unresolved_basis = "pin_fingerprint_rotated_in_supply"
        elif not authority and not supply_fingerprints:
            # wallet has no frontier row at all: it left the candidate roster
            unresolved_basis = "pinned_wallet_absent_from_frontier"
        elif packet.get("status") == "WAIT_PIN_UNRESOLVED":
            unresolved_basis = "upstream_packet_pin_unresolved"
        else:
            unresolved_basis = "pin_authority_empty"
        audit.update(
            {
                "status": "WAIT_PIN_UNRESOLVED",
                "pin_unresolved_basis": unresolved_basis,
                "pin_fingerprint_unmatched_in_supply": unmatched,
                "pinned_wallet_on_frontier": bool(supply_fingerprints),
                "supply_fingerprints_for_pinned_wallet": supply_fingerprints,
            }
        )
        _append_jsonl(gate_log_path, audit)
        return audit
    if projection.get("journal_window_covered") is not True:
        audit["status"] = "REFUSED_DIRECT_PROJECTION_INCOMPLETE"
        _append_jsonl(gate_log_path, audit)
        return audit
    if not within_packet_budget:
        audit["status"] = "REFUSED_STALE_DIRECT_PACKET"
        _append_jsonl(gate_log_path, audit)
        return audit
    if projection.get("copyable_parity") is not True:
        audit["status"] = "REFUSED_DIRECT_COPYABLE_PARITY"
        _append_jsonl(gate_log_path, audit)
        return audit

    if not direct_f2_pass:
        audit.update(
            {
                "status": "REFUSED_DIRECT_F2_ARITHMETIC",
            }
        )
        _append_jsonl(gate_log_path, audit)
        return audit
    if not direct_f4_pass:
        audit["status"] = "REFUSED_DIRECT_F4_LIVENESS"
        _append_jsonl(gate_log_path, audit)
        return audit
    if not history_halves_pass:
        audit["status"] = "WAIT_OTHER_GATE"
        _append_jsonl(gate_log_path, audit)
        return audit

    overlay = load_json(overlay_path, default={})
    invoke = actuator or _execute_policy_choke_rung_b
    # ORDER135 seam repair: this line is reachable only after every gate check
    # above has passed against a packet proved fresh THIS cycle
    # (packet_age_s <= ORDER135_PACKET_MAX_AGE_S).  The deadman artefact the
    # authority was read from is written on its own ~2-3min cadence, so its
    # f4_external_liveness re-derives the same freshness question from a stale
    # snapshot and discards the pass.  Carry the proof forward instead of
    # letting it be re-derived; no bar moves value, and order_flow_deadman's
    # own rung-B derivation is untouched for every other caller.
    eligible_basis = "order135_direct_gate_pass"
    gate_proved_candidate = {
        **authority,
        "eligible": True,
        "eligible_basis": eligible_basis,
        "direct_liveness": {
            "packet_age_s": packet_age_s,
            "packet_max_age_s": ORDER135_PACKET_MAX_AGE_S,
            "current_attempted_buy_rows": projection.get(
                "current_attempted_buy_rows"
            ),
            "direct_f2": dict(direct_f2),
            "direct_f4_pass": direct_f4_pass,
            "history_halves_pass": history_halves_pass,
            "evaluated_at": evaluated_at.isoformat(),
        },
    }
    updated_overlay, actuator_result = invoke(
        overlay=overlay if isinstance(overlay, dict) else {},
        candidate=gate_proved_candidate,
        now=evaluated_at,
        probe_cap_usd=1.0,
        supply_rung="DIRECT",
    )
    actuator_status = str(actuator_result.get("status") or "")
    if actuator_status in {
        "DIRECT_SOURCE_SELECTION_PIN_WRITTEN",
        "DIRECT_SOURCE_SELECTION_POLICY_REPAIRED",
    }:
        if overlay_path == AUTO_DEGRADE_ACTIVE_SET_STATE:
            _atomic_write_auto_degrade_overlay(updated_overlay)
        else:
            atomic_write_json(overlay_path, updated_overlay)
    audit.update(
        {
            "status": actuator_status or "ACTUATOR_NO_STATUS",
            "invoked": True,
            "actuator": actuator_result,
            "eligible_basis": eligible_basis,
            "authority_eligible": authority.get("eligible") is True,
            "authority_false_checks": sorted(
                key
                for key, value in authority_checks.items()
                if value is False
            ),
        }
    )
    _append_jsonl(gate_log_path, audit)
    return audit


def _coverage_kpi(
    active_set_runtime: dict[str, Any],
    *,
    latest_live_order_ts: float | None,
    previous_state: dict[str, Any],
    generated_at: str,
    set_generation_id: str,
) -> dict[str, Any]:
    active_set = _active_live_set_contract()
    min_members = int(active_set.get("target_member_count_min") or 3)
    qualified = int(active_set_runtime.get("qualified_member_count") or active_set_runtime.get("member_count") or 0)
    idle_hours = None
    if latest_live_order_ts and latest_live_order_ts > 0:
        idle_hours = round(max(0.0, time.time() - float(latest_live_order_ts)) / 3600.0, 6)
    generated_ts = _parse_iso_ts(generated_at) or time.time()
    previous_kpi = previous_state.get("coverage_kpi") if isinstance(previous_state.get("coverage_kpi"), dict) else {}
    armed_nonempty = bool(active_set_runtime.get("enabled") and qualified > 0)
    since_ts: float | None = None
    if armed_nonempty:
        if str(previous_kpi.get("active_set_generation_id") or "") == set_generation_id:
            try:
                since_ts = float(previous_kpi.get("nonempty_set_armed_since_ts") or 0.0) or None
            except (TypeError, ValueError):
                since_ts = None
        if since_ts is None:
            since_ts = _parse_iso_ts(previous_state.get("generated_at")) or generated_ts
    since_hours = None if since_ts is None else round(max(0.0, generated_ts - since_ts) / 3600.0, 6)
    return {
        "enabled": bool(active_set_runtime.get("enabled")),
        "flow_stage": "LIVE/PROMOTE",
        "active_set_generation_id": set_generation_id,
        "qualified_member_count": qualified,
        "target_member_count_min": min_members,
        "target_member_count_max": int(active_set.get("target_member_count_max") or 5),
        "below_active_set_min": qualified < min_members,
        "armed_idle_hours_since_latest_live_order": idle_hours,
        "nonempty_set_armed_since_ts": since_ts,
        "armed_idle_since_nonempty_set_hours": since_hours,
        "coverage_incident_threshold_idle_hours": float(active_set.get("coverage_incident_threshold_idle_hours") or 1.0),
        "rule": "idle armed non-empty active set > threshold with zero fresh intents/orders triggers protection-bounded auto-degrade admission",
    }


def main() -> int:
    base_args = parse_args()
    lock = _acquire_lock(base_args)
    if lock is None:
        return 2
    started = time.time()
    started_at_utc = dt.datetime.fromtimestamp(started, dt.timezone.utc).isoformat().replace("+00:00", "Z")
    guard_code_identity = _guard_code_identity(started_at_utc=started_at_utc, pid=os.getpid())
    base_args.guard_code_identity = guard_code_identity
    base_args.live_guard_generation_sha256 = str(
        guard_code_identity.get("live_guard_generation_sha256") or ""
    )
    cycle = 0
    last_payload: dict[str, Any] = {}
    previous_write_state_file_measurement: dict[str, Any] = {}
    price_band_decision_cache: dict[str, Any] = {}
    active_set_runtime_cache: tuple[argparse.Namespace, dict[str, Any]] | None = None
    post_live_reporting_cache: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None = None
    orderfilled_fast_lane = _OrderFilledFastLane(base_args)
    orderfilled_fast_lane.start()
    try:
        while int(base_args.iterations) <= 0 or cycle < int(base_args.iterations):
            if float(base_args.max_runtime_s) > 0 and time.time() - started >= float(base_args.max_runtime_s):
                break
            cycle += 1
            guard_cycle_started_wall = time.time()
            guard_cycle_started = time.perf_counter()
            guard_stage_started = guard_cycle_started
            guard_stage_timers: list[dict[str, Any]] = []
            guard_cycle_rss_gib = _current_process_rss_gib()

            def mark_guard_stage(name: str) -> None:
                nonlocal guard_stage_started
                now = time.perf_counter()
                guard_stage_timers.append(
                    {
                        "name": name,
                        "duration_s": round(now - guard_stage_started, 6),
                        "elapsed_s": round(now - guard_cycle_started, 6),
                        "rss_gib": guard_cycle_rss_gib,
                        "rss_sample_offset_s": 0.0,
                        "rss_sample_authority": "once_per_cycle_" + (
                            "in_process_stage_boundary_mach_task_basic_info"
                            if sys.platform == "darwin"
                            else "in_process_stage_boundary_proc_statm"
                        ),
                    }
                )
                guard_stage_started = time.perf_counter()

            mission_hot_reload = _mission_contract_hot_reload_check(cycle=cycle)
            mark_guard_stage("mission_contract_hot_reload")
            cap_step_revert_every_n = _cadence_every_n(
                base_args,
                "cap_step_revert_every_n_cycles",
            )
            cap_step_revert_due = _cadence_due(cycle, cap_step_revert_every_n)
            pre_cycle_cap_step_revert = (
                _maybe_execute_cap_step_revert(base_args, generated_at=utc_now_iso())
                if cap_step_revert_due
                else {
                    "enabled": True,
                    "flow_stage": "LIVE/ROTATE/SELF-DEV",
                    "status": "SKIPPED_CADENCE",
                    "every_n_cycles": cap_step_revert_every_n,
                    "cycle": cycle,
                }
            )
            mark_guard_stage("active_set_cap_step_revert_precheck")
            pre_cycle_f418_tenure_pin = _maybe_refresh_f418_quiet_clock_selection_pin(
                base_args,
                generated_at=utc_now_iso(),
            )
            mark_guard_stage("active_set_f418_tenure_pin_precheck")
            current_utc_weekday = dt.datetime.now(dt.timezone.utc).weekday()
            if current_utc_weekday >= 5:
                pre_cycle_weekend_seat_loss_rotation = _maybe_execute_weekend_seat_loss_rotation_rider(
                    base_args,
                    generated_at=utc_now_iso(),
                )
            else:
                pre_cycle_weekend_seat_loss_rotation = {
                    "flow_stage": "LIVE/DEFEND",
                    "status": "SKIPPED_WEEKDAY",
                    "current_is_weekend": False,
                    "weekday": current_utc_weekday,
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "rule": "weekend seat-loss rider runs only on Saturday/Sunday UTC",
                }
            mark_guard_stage("active_set_weekend_seat_loss_rotation_precheck")
            try:
                pre_cycle_order135_direct_gate = (
                    {
                        "status": "SKIPPED_TEST_ENV",
                        "direction_id": ORDER135_DIRECTION_ID,
                        "invoked": False,
                    }
                    if os.environ.get("PYTEST_CURRENT_TEST")
                    else _order135_direct_gate_pass()
                )
            except Exception as exc:  # fail closed without disturbing the sole guard.
                pre_cycle_order135_direct_gate = {
                    "status": "ORDER135_GATE_PASS_ERROR",
                    "direction_id": ORDER135_DIRECTION_ID,
                    "error": f"{type(exc).__name__}: {exc}",
                    "invoked": False,
                }
                _append_jsonl(ORDER135_GATE_LOG, pre_cycle_order135_direct_gate)
            mark_guard_stage("order135_direct_gate_pass")
            active_set_runtime_every_n = _cadence_every_n(
                base_args,
                "active_set_runtime_refresh_every_n_cycles",
            )
            active_set_runtime_due = _cadence_due(cycle, active_set_runtime_every_n)
            if active_set_runtime_due or active_set_runtime_cache is None:
                args, active_set_runtime = _active_set_runtime_args(base_args, cycle=cycle)
                active_set_runtime_cache = (
                    argparse.Namespace(**vars(args)),
                    copy.deepcopy(active_set_runtime),
                )
            else:
                cached_args, cached_runtime = active_set_runtime_cache
                args = argparse.Namespace(**vars(cached_args))
                active_set_runtime = copy.deepcopy(cached_runtime)
                active_set_runtime["runtime_refresh_cadence"] = {
                    "status": "CARRIED_FORWARD",
                    "cycle": cycle,
                    "every_n_cycles": active_set_runtime_every_n,
                    "executed_this_cycle": False,
                }
            active_set_runtime["selection_pin"] = _active_set_selection_pin_view()
            args = _with_live_guard_runtime_history(args)
            orderfilled_fast_lane.update_snapshot(
                args=args,
                active_set_runtime=active_set_runtime,
                cycle=cycle,
            )
            mark_guard_stage("select_active_set_runtime")
            candidate, source_wallet, selection_blockers = _load_candidate(args)
            mark_guard_stage("load_candidate")
            candidate_id = str(candidate.get("candidate_id") or "")
            blockers: list[str] = list(selection_blockers)
            source_route_gate = _live_source_route_gate(args)
            mark_guard_stage("source_route_gate")
            blockers.extend(str(blocker) for blocker in source_route_gate.get("blockers") or [])
            if not candidate_id:
                blockers.append("runtime_admission_candidate_missing")
            if not source_wallet:
                blockers.append("runtime_admission_source_wallet_missing")
            pipeline_result = {}
            active_set_rtds_premerge_result = {}
            alternate_transport_copyintent_bridge_result = {}
            dataapi_poll_result = {}
            watch_tier_poll_result = {}
            live_result = {}
            live_probe_result = {}
            live_probe_promotion_result = {}
            e5_live_actuator_result = {}
            cross_exchange_live_actuator_result = {}
            wide_family_live_actuator_result = {}
            routing_shadow_validation_result = {}
            self_feed_result = {}
            previous_state = load_json(args.state, default={}, cache_readonly=True)
            previous_state = previous_state if isinstance(previous_state, dict) else {}
            if not previous_write_state_file_measurement:
                previous_write_state_file_measurement = (
                    _seed_previous_write_state_file_measurement(previous_state, args.state)
                )
            watch_tier_every_n = _cadence_every_n(args, "watch_tier_dataapi_poller_every_n_cycles")
            watch_tier_offset = _cadence_offset(
                args,
                "watch_tier_dataapi_poller_cycle_offset",
                every_n=watch_tier_every_n,
            )
            watch_tier_due = _cadence_due(cycle, watch_tier_every_n, watch_tier_offset)
            watch_tier_wallets = _watch_tier_poll_wallets(
                str(getattr(args, "watch_tier_wallets_config", DEFAULT_WATCH_TIER_WALLETS_CONFIG))
            )
            watch_tier_member_cap = _watch_tier_dataapi_poller_member_cap(args, len(watch_tier_wallets))
            watch_tier_round_robin_offset = _active_set_live_execution_probe_round_robin_offset(
                cycle,
                every_n=watch_tier_every_n,
                member_cap=watch_tier_member_cap,
            )
            active_set_dataapi_every_n = _cadence_every_n(args, "active_set_dataapi_poller_every_n_cycles")
            active_set_dataapi_offset = _cadence_offset(
                args,
                "active_set_dataapi_poller_cycle_offset",
                every_n=active_set_dataapi_every_n,
            )
            active_set_dataapi_due = _cadence_due(cycle, active_set_dataapi_every_n, active_set_dataapi_offset)
            active_set_probe_every_n = _cadence_every_n(args, "active_set_live_execution_probes_every_n_cycles")
            active_set_probe_offset = min(3, max(0, active_set_probe_every_n - 1))
            active_set_probe_due = _cadence_due(cycle, active_set_probe_every_n, active_set_probe_offset)
            active_set_probe_member_cap = _active_set_live_execution_probe_member_cap(
                args,
                runtime_member_count=int(active_set_runtime.get("member_count") or 0),
            )
            active_set_probe_round_robin_offset = _active_set_live_execution_probe_round_robin_offset(
                cycle,
                every_n=active_set_probe_every_n,
                member_cap=active_set_probe_member_cap,
            )
            self_feed_every_n = _cadence_every_n(args, "self_feed_ledger_diff_every_n_cycles")
            shadow_lanes_every_n = _cadence_every_n(args, "shadow_lanes_every_n_cycles")
            self_feed_offset = _cadence_offset(
                args,
                "self_feed_ledger_diff_cycle_offset",
                every_n=self_feed_every_n,
            )
            shadow_lanes_offset = _cadence_offset(
                args,
                "shadow_lanes_cycle_offset",
                every_n=shadow_lanes_every_n,
            )
            self_feed_due = _cadence_due(cycle, self_feed_every_n, self_feed_offset)
            shadow_lanes_due = _cadence_due(cycle, shadow_lanes_every_n, shadow_lanes_offset)
            if not blockers:
                runtime_selected_member = (
                    active_set_runtime.get("selected_member")
                    if isinstance(active_set_runtime.get("selected_member"), dict)
                    else {}
                )
                runtime_selected_wallet = str(
                    runtime_selected_member.get("source_wallet")
                    or runtime_selected_member.get("wallet")
                    or source_wallet
                    or ""
                ).lower()
                pipeline_result, active_set_rtds_premerge_result = _run_active_set_rtds_premerge(
                    args,
                    active_set_runtime=active_set_runtime,
                    selected_wallet=runtime_selected_wallet,
                )
                mark_guard_stage("active_set_rtds_premerge")
                if int(pipeline_result.get("returncode") or 0) != 0:
                    blockers.append("history_refresh_failed")
                if active_set_dataapi_due:
                    pre_dataapi_history_identities = _history_event_identity_snapshot(args.history_state)
                    dataapi_poll_result = _cadence_payload(
                        _run_active_set_dataapi_poller(
                            args,
                            active_set_runtime=active_set_runtime,
                            fallback_wallet=runtime_selected_wallet,
                        ),
                        name="active_set_dataapi_poller",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=active_set_dataapi_every_n,
                        offset=active_set_dataapi_offset,
                        executed=True,
                    )
                else:
                    pre_dataapi_history_identities = set()
                    dataapi_poll_result = _cadence_payload(
                        previous_state.get("active_set_dataapi_poller")
                        if isinstance(previous_state.get("active_set_dataapi_poller"), dict)
                        else {},
                        name="active_set_dataapi_poller",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=active_set_dataapi_every_n,
                        offset=active_set_dataapi_offset,
                        executed=False,
                    )
                if active_set_dataapi_due and isinstance(dataapi_poll_result, dict):
                    dataapi_poll_result["freshness_acceptance_discriminator"] = (
                        _active_set_detection_acceptance_discriminator(
                            dataapi_poll_result=dataapi_poll_result,
                            active_set_rtds_premerge=active_set_rtds_premerge_result,
                            previous_state=previous_state,
                            selected_wallet=runtime_selected_wallet,
                        )
                    )
                mark_guard_stage("active_set_dataapi_poller")
                dataapi_history_delta = (
                    _post_poll_history_event_delta(
                        args.history_state,
                        before_identities=pre_dataapi_history_identities,
                    )
                    if active_set_dataapi_due
                    else []
                )
                bridge_event_union = dict(active_set_rtds_premerge_result)
                (
                    orderfilled_rows,
                    orderfilled_hot_source_report,
                    orderfilled_cursor_handoff,
                    orderfilled_source_selection,
                ) = _main_cycle_orderfilled_delta(
                    args,
                    active_set_runtime=active_set_runtime,
                    fast_lane_enabled=orderfilled_fast_lane.enabled,
                )
                bridge_delta_rows = [
                    *(
                        active_set_rtds_premerge_result.get("matching_events_delta")
                        if isinstance(active_set_rtds_premerge_result.get("matching_events_delta"), list)
                        else []
                    ),
                    *dataapi_history_delta,
                    *orderfilled_rows,
                ]
                bridge_event_union["matching_events_delta"] = bridge_delta_rows
                bridge_event_union["event_union"] = {
                    "flow_stage": "LIVE/ROTATE/SELF-DEV",
                    "rtds_rows": len(active_set_rtds_premerge_result.get("matching_events_delta") or []),
                    "dataapi_post_poll_rows": len(dataapi_history_delta),
                    "orderfilled_hot_source_rows": len(orderfilled_rows),
                    "orderfilled_hot_source": orderfilled_hot_source_report,
                    "orderfilled_source_selection": orderfilled_source_selection,
                    "orderfilled_cursor_handoff": orderfilled_cursor_handoff,
                    "dedupe_rule": "polygon tx_hash|log_index; cross-transport source_fingerprint/event_id",
                    "dataapi_polled_before_bridge": bool(active_set_dataapi_due),
                }
                if bridge_delta_rows:
                    args, alternate_transport_copyintent_bridge_result = _alternate_transport_copyintent_bridge(
                        args,
                        active_set_runtime=active_set_runtime,
                        active_set_rtds_premerge=bridge_event_union,
                        exclude_identity_keys=_fast_lane_consumed_identity_snapshot(),
                    )
                else:
                    args, alternate_transport_copyintent_bridge_result = (
                        _empty_alternate_transport_bridge_result(args)
                    )
                bridge_live_result = alternate_transport_copyintent_bridge_result.pop("_live_result", None)
                alternate_transport_copyintent_bridge_result["event_union"] = bridge_event_union["event_union"]
                if (
                    alternate_transport_copyintent_bridge_result.get("status")
                    in {
                        "QUALIFYING_EVENT_ROUTED_TO_EXISTING_COPYINTENT_BUILDER",
                        "SURVIVOR_SUBMITTED_TO_EXISTING_GUARD",
                    }
                ):
                    candidate_id = str(args.candidate_id or candidate_id)
                    source_wallet = str(args.source_wallet or source_wallet).lower()
                mark_guard_stage("alternate_transport_copyintent_bridge")
                live_result = _bridge_or_fused_live_execution(
                    args,
                    candidate_id=candidate_id,
                    bridge_report=alternate_transport_copyintent_bridge_result,
                    bridge_live_result=bridge_live_result,
                )
                mark_guard_stage("live_execution")
                live_stdout = live_result.get("stdout_json") if isinstance(live_result, dict) else {}
                live_stdout = live_stdout if isinstance(live_stdout, dict) else {}
                live_stdout["hot_history_copy_intent_merge"] = _persist_built_copy_intents(
                    args,
                    live_stdout,
                )
                actual_live_wallet = str(live_stdout.get("source_wallet") or source_wallet or "").strip().lower()
                force_probe_wallets: set[str] = set()
                if runtime_selected_wallet and actual_live_wallet and runtime_selected_wallet != actual_live_wallet:
                    force_probe_wallets.add(runtime_selected_wallet)
                shadow_candidate_members = load_shadow_candidate_seats(
                    getattr(args, "routing_shadow_candidate_seats", DEFAULT_ROUTING_SHADOW_CANDIDATE_SEATS)
                )
                probe_generated_at = utc_now_iso()
                probes_enabled = bool(getattr(args, "active_set_live_execution_probes", True))
                prior_live_probe_result = (
                    previous_state.get("active_set_live_execution_probes")
                    if isinstance(previous_state.get("active_set_live_execution_probes"), dict)
                    else {}
                )
                if (not probes_enabled) or active_set_probe_due or force_probe_wallets:
                    fresh_live_probe_result = _run_active_set_live_execution_probes(
                        args,
                        active_set_runtime=active_set_runtime,
                        dataapi_poll_result=dataapi_poll_result,
                        force_wallets=force_probe_wallets,
                        shadow_candidate_members=shadow_candidate_members if active_set_probe_due else [],
                        max_members_override=active_set_probe_member_cap if active_set_probe_due else 0,
                        include_all_runtime_members_override=(
                            bool(getattr(args, "active_set_evaluate_all_runtime_members_per_cycle", False))
                            if active_set_probe_due
                            else False
                        ),
                        round_robin_offset=active_set_probe_round_robin_offset if active_set_probe_due else 0,
                        parallel=active_set_probe_due,
                        max_workers=active_set_probe_member_cap,
                    )
                    if active_set_probe_due or not probes_enabled:
                        live_probe_result = _cadence_payload(
                            fresh_live_probe_result,
                            name="active_set_live_execution_probes",
                            generated_at=probe_generated_at,
                            cycle=cycle,
                            every_n=active_set_probe_every_n,
                            offset=active_set_probe_offset,
                            executed=True,
                        )
                    else:
                        live_probe_result = _cadence_payload(
                            _merge_active_set_live_execution_probe_results(
                                prior_live_probe_result,
                                fresh_live_probe_result,
                                force_wallets=force_probe_wallets,
                            ),
                            name="active_set_live_execution_probes",
                            generated_at=probe_generated_at,
                            cycle=cycle,
                            every_n=active_set_probe_every_n,
                            offset=active_set_probe_offset,
                            executed=False,
                        )
                else:
                    live_probe_result = _cadence_payload(
                        prior_live_probe_result,
                        name="active_set_live_execution_probes",
                        generated_at=probe_generated_at,
                        cycle=cycle,
                        every_n=active_set_probe_every_n,
                        offset=active_set_probe_offset,
                        executed=False,
                    )
                    live_probe_result["cached_results_used"] = True
                    live_probe_result["probed_members_this_cycle"] = 0
                mark_guard_stage("active_set_live_execution_probes")
                if probes_enabled and active_set_probe_due:
                    live_probe_promotion_result = _cadence_payload(
                        _run_active_set_live_execution_probe_promotions(
                            args,
                            active_set_runtime=active_set_runtime,
                            live_probe_result=live_probe_result,
                        ),
                        name="active_set_live_execution_probe_promotions",
                        generated_at=probe_generated_at,
                        cycle=cycle,
                        every_n=active_set_probe_every_n,
                        offset=active_set_probe_offset,
                        executed=True,
                    )
                else:
                    prior_live_probe_promotion_result = (
                        previous_state.get("active_set_live_execution_probe_promotions")
                        if isinstance(previous_state.get("active_set_live_execution_probe_promotions"), dict)
                        else {}
                    )
                    live_probe_promotion_result = _cadence_payload(
                        prior_live_probe_promotion_result,
                        name="active_set_live_execution_probe_promotions",
                        generated_at=probe_generated_at,
                        cycle=cycle,
                        every_n=active_set_probe_every_n,
                        offset=active_set_probe_offset,
                        executed=False,
                    )
                mark_guard_stage("active_set_live_execution_probe_promotions")
                if shadow_lanes_due:
                    try:
                        routing_shadow_validation_result = _run_routing_shadow_validation(
                            args,
                            active_set_runtime=active_set_runtime,
                            live_stdout=live_stdout,
                            live_probe_result=live_probe_result,
                            live_probe_promotion_result=live_probe_promotion_result,
                            dataapi_poll_result=dataapi_poll_result,
                            generated_at=utc_now_iso(),
                            shadow_candidate_members=shadow_candidate_members,
                        )
                    except Exception as exc:  # pragma: no cover - shadow validation cannot disturb live order flow.
                        routing_shadow_validation_result = {
                            "schema_version": 1,
                            "kind": "routing_shadow_validation",
                            "flow_stage": "LIVE/LEARN/SELF-DEV",
                            "generated_at": utc_now_iso(),
                            "enabled": True,
                            "routing_mode": str(getattr(args, "routing_router_mode", "shadow") or "shadow"),
                            "paper_only": True,
                            "live_orders_allowed": False,
                            "status": "SHADOW_ERROR",
                            "error": f"{type(exc).__name__}: {exc}",
                            "next_action": "repair routing shadow validation while live guard continues unchanged",
                        }
                    routing_shadow_validation_result = _cadence_payload(
                        routing_shadow_validation_result,
                        name="routing_shadow_validation",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=shadow_lanes_every_n,
                        offset=shadow_lanes_offset,
                        executed=True,
                    )
                else:
                    prior_routing_shadow_validation = (
                        previous_state.get("routing_shadow_validation")
                        if isinstance(previous_state.get("routing_shadow_validation"), dict)
                        else {}
                    )
                    routing_shadow_validation_result = _cadence_payload(
                        prior_routing_shadow_validation,
                        name="routing_shadow_validation",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=shadow_lanes_every_n,
                        offset=shadow_lanes_offset,
                        executed=False,
                    )
                mark_guard_stage("routing_shadow_validation")
                if watch_tier_due:
                    watch_tier_poll_result = _cadence_payload(
                        _run_watch_tier_dataapi_poller(
                            args,
                            round_robin_offset=watch_tier_round_robin_offset,
                        ),
                        name="watch_tier_dataapi_poller",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=watch_tier_every_n,
                        offset=watch_tier_offset,
                        executed=True,
                    )
                else:
                    prior_watch_tier_poll = (
                        previous_state.get("watch_tier_dataapi_poller")
                        if isinstance(previous_state.get("watch_tier_dataapi_poller"), dict)
                        else {}
                    )
                    watch_tier_poll_result = _cadence_payload(
                        prior_watch_tier_poll,
                        name="watch_tier_dataapi_poller",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=watch_tier_every_n,
                        offset=watch_tier_offset,
                        executed=False,
                    )
                mark_guard_stage("watch_tier_dataapi_poller")
                live_stdout = live_result.get("stdout_json") if isinstance(live_result, dict) else {}
                live_stdout = live_stdout if isinstance(live_stdout, dict) else {}
                live_status = str(live_stdout.get("status") or "")
                live_filtered_no_submit = _live_execution_filtered_no_submit(live_stdout)
                if int(live_result.get("returncode") or 0) != 0:
                    if not live_filtered_no_submit:
                        blockers.append("live_execution_arm_failed")
                elif live_status == "LIVE_EXECUTION_SUBMITTED":
                    pass
                elif live_status not in {
                    "LIVE_ARMED_NO_FRESH_INTENTS",
                    "LIVE_ARMED_NO_NEW_INTENTS",
                    "LIVE_ARMED_DRY_RUN",
                    "LIVE_READY_BEHIND_OPERATOR_GATE",
                    "LIVE_PLAN_PROTECTED_NO_SURVIVOR",
                }:
                    if not live_filtered_no_submit:
                        blockers.append("unexpected_live_execution_status")
            else:
                mark_guard_stage("hot_path_skipped_for_blockers")
                if bool(getattr(args, "active_set_evaluate_all_runtime_members_per_cycle", False)):
                    probe_generated_at = utc_now_iso()
                    prior_live_probe_result = (
                        previous_state.get("active_set_live_execution_probes")
                        if isinstance(previous_state.get("active_set_live_execution_probes"), dict)
                        else {}
                    )
                    if active_set_probe_due:
                        live_probe_result = _cadence_payload(
                            _run_active_set_live_execution_probes(
                                args,
                                active_set_runtime=active_set_runtime,
                                dataapi_poll_result=dataapi_poll_result,
                                force_wallets=set(),
                                shadow_candidate_members=[],
                                max_members_override=active_set_probe_member_cap,
                                include_all_runtime_members_override=True,
                                round_robin_offset=active_set_probe_round_robin_offset,
                                parallel=True,
                                max_workers=active_set_probe_member_cap,
                            ),
                            name="active_set_live_execution_probes",
                            generated_at=probe_generated_at,
                            cycle=cycle,
                            every_n=active_set_probe_every_n,
                            offset=active_set_probe_offset,
                            executed=True,
                        )
                    else:
                        live_probe_result = _cadence_payload(
                            prior_live_probe_result,
                            name="active_set_live_execution_probes",
                            generated_at=probe_generated_at,
                            cycle=cycle,
                            every_n=active_set_probe_every_n,
                            offset=active_set_probe_offset,
                            executed=False,
                        )
                        live_probe_result["cached_results_used"] = True
                        live_probe_result["probed_members_this_cycle"] = 0
                    mark_guard_stage("active_set_all_runtime_member_evaluation")
                if watch_tier_due:
                    watch_tier_poll_result = _cadence_payload(
                        _run_watch_tier_dataapi_poller(
                            args,
                            round_robin_offset=watch_tier_round_robin_offset,
                        ),
                        name="watch_tier_dataapi_poller",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=watch_tier_every_n,
                        offset=watch_tier_offset,
                        executed=True,
                    )
                else:
                    prior_watch_tier_poll = (
                        previous_state.get("watch_tier_dataapi_poller")
                        if isinstance(previous_state.get("watch_tier_dataapi_poller"), dict)
                        else {}
                    )
                    watch_tier_poll_result = _cadence_payload(
                        prior_watch_tier_poll,
                        name="watch_tier_dataapi_poller",
                        generated_at=utc_now_iso(),
                        cycle=cycle,
                        every_n=watch_tier_every_n,
                        offset=watch_tier_offset,
                        executed=False,
                    )
                mark_guard_stage("watch_tier_dataapi_poller")
            try:
                e5_live_actuator_result = _run_e5_live_actuator(args, generated_at=utc_now_iso())
            except Exception as exc:  # pragma: no cover - wallet live path must continue on E5 actuator defect.
                e5_live_actuator_result = {
                    "schema_version": 1,
                    "kind": "e5_maker_first_live_actuator",
                    "flow_stage": "LIVE/PROMOTE/ROTATE",
                    "generated_at": utc_now_iso(),
                    "status": "ACTUATOR_ERROR",
                    "single_submitter": "scripts/run_wallet_copy_live_guard.py",
                    "orders_submitted": 0,
                    "orders_accepted": 0,
                    "orders_filled": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            mark_guard_stage("e5_maker_first_live_actuator")
            try:
                cross_exchange_live_actuator_result = _run_cross_exchange_live_actuator(
                    args,
                    generated_at=utc_now_iso(),
                )
            except Exception as exc:  # pragma: no cover - wallet live path must continue on actuator defect.
                cross_exchange_live_actuator_result = {
                    "schema_version": 1,
                    "kind": "btc5m_cross_exchange_probability_edge_live_actuator",
                    "flow_stage": "LIVE/ROTATE/PROMOTE/SELF-DEV",
                    "generated_at": utc_now_iso(),
                    "status": "ACTUATOR_ERROR",
                    "single_submitter": "scripts/run_wallet_copy_live_guard.py",
                    "orders_submitted": 0,
                    "orders_accepted": 0,
                    "orders_filled": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            mark_guard_stage("cross_exchange_probability_live_actuator")
            try:
                wide_family_live_actuator_result = _run_wide_family_live_actuator(
                    args,
                    generated_at=utc_now_iso(),
                )
            except Exception as exc:  # pragma: no cover - wallet live path must continue.
                wide_family_live_actuator_result = {
                    "schema_version": 1,
                    "kind": "wide_positive_slice_family_live_actuator",
                    "flow_stage": "LIVE/ROTATE/PROMOTE/SELF-DEV",
                    "generated_at": utc_now_iso(),
                    "status": "ACTUATOR_ERROR",
                    "single_submitter": "scripts/run_wallet_copy_live_guard.py",
                    "orders_submitted": 0,
                    "orders_accepted": 0,
                    "orders_filled": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            mark_guard_stage("wide_positive_slice_family_live_actuator")
            blockers = sorted(set(blockers))
            ruled_flat_active_set = _ruled_flat_active_set_state(active_set_runtime)
            if ruled_flat_active_set.get("active") and not candidate_id and not source_wallet:
                blockers = sorted(
                    {
                        *[blocker for blocker in blockers if blocker not in _GENERIC_NO_CANDIDATE_BLOCKERS],
                        *[str(blocker) for blocker in ruled_flat_active_set.get("blockers") or [] if str(blocker)],
                    }
                )
            live_stdout = live_result.get("stdout_json") if isinstance(live_result, dict) else {}
            live_stdout = live_stdout if isinstance(live_stdout, dict) else {}
            live_filtered_no_submit = _live_execution_filtered_no_submit(live_stdout)
            policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
            try:
                intent_time_copyability_proof = _write_intent_time_copyability_proof_state(
                    args,
                    live_probe_result=live_probe_result if isinstance(live_probe_result, dict) else {},
                    generated_at=utc_now_iso(),
                )
            except Exception as exc:  # pragma: no cover - proof capture must not disturb submitter loop.
                intent_time_copyability_proof = {
                    "enabled": True,
                    "status": "ERROR",
                    "error": f"{type(exc).__name__}: {exc}",
                    "next_action": "repair intent-time proof capture while live guard continues unchanged",
                }
            copy_contract = _runtime_copy_contract()
            mission_candidate_id = str(_primary_live_candidate_contract().get("candidate_id") or args.candidate_id or "")
            generated_at = utc_now_iso()
            operator_notify_suppression = (
                ruled_flat_active_set.get("operator_notify_suppression")
                if isinstance(ruled_flat_active_set.get("operator_notify_suppression"), dict)
                else {}
            )
            operator_notify_transition = _operator_notify_transition(
                previous_state,
                current_blockers=blockers,
                suppression=operator_notify_suppression,
                now_iso=generated_at,
            )
            slow_path_cadence = {
                "flow_stage": "LIVE/LEARN/SELF-DEV",
                "rule": "detect_build_gate_submit_hot_path_runs_every_cycle; reporting and shadow work are cadenced",
                "guard_slow_path_every_n_cycles": int(getattr(args, "guard_slow_path_every_n_cycles", 4) or 4),
                "watch_tier_dataapi_poller_every_n_cycles": watch_tier_every_n,
                "watch_tier_dataapi_poller_cycle_offset": watch_tier_offset,
                "watch_tier_dataapi_poller_due": watch_tier_due,
                "watch_tier_dataapi_poller_member_cap": watch_tier_member_cap,
                "watch_tier_dataapi_poller_round_robin_offset": watch_tier_round_robin_offset,
                "active_set_dataapi_poller_every_n_cycles": active_set_dataapi_every_n,
                "active_set_dataapi_poller_cycle_offset": active_set_dataapi_offset,
                "active_set_dataapi_poller_due": active_set_dataapi_due,
                "active_set_runtime_refresh_every_n_cycles": active_set_runtime_every_n,
                "active_set_runtime_refresh_due": active_set_runtime_due,
                "cap_step_revert_every_n_cycles": cap_step_revert_every_n,
                "cap_step_revert_due": cap_step_revert_due,
                "active_set_live_execution_probes_every_n_cycles": active_set_probe_every_n,
                "active_set_live_execution_probes_cycle_offset": active_set_probe_offset,
                "active_set_live_execution_probes_due": active_set_probe_due,
                "active_set_live_execution_probe_member_cap": active_set_probe_member_cap,
                "active_set_live_execution_probe_round_robin_offset": active_set_probe_round_robin_offset,
                "self_feed_ledger_diff_every_n_cycles": self_feed_every_n,
                "self_feed_ledger_diff_cycle_offset": self_feed_offset,
                "self_feed_ledger_diff_due": self_feed_due,
                "shadow_lanes_every_n_cycles": shadow_lanes_every_n,
                "shadow_lanes_cycle_offset": shadow_lanes_offset,
                "shadow_lanes_due": shadow_lanes_due,
            }
            set_generation_id = _active_set_generation_id()
            live_price_band_decision = _maybe_refresh_live_price_band_decision(
                args,
                price_band_decision_cache,
                now_ts=time.time(),
                target_wallet=source_wallet,
            )
            mark_guard_stage("live_price_band_decision")
            window_participation = _merge_window_participation(
                previous_state.get("window_participation") if isinstance(previous_state.get("window_participation"), dict) else {},
                live_stdout.get("window_participation") if isinstance(live_stdout.get("window_participation"), dict) else {},
                generated_at=generated_at,
                set_generation_id=set_generation_id,
                active_set_rtds_premerge=active_set_rtds_premerge_result,
            )
            mark_guard_stage("window_participation_merge")
            active_set = _active_set_snapshot(
                args,
                candidate=candidate,
                source_wallet=source_wallet,
                generated_at=generated_at,
                active_set_admissible_backfill=active_set_runtime.get(
                    "active_set_admissible_backfill"
                ),
            )
            mark_guard_stage("active_set_snapshot")
            live_ledger = load_json(args.live_ledger_state, default={}, cache_readonly=True)
            live_ledger = live_ledger if isinstance(live_ledger, dict) else {}
            coverage_kpi = _coverage_kpi_snapshot(
                args,
                active_set=active_set,
                ledger=live_ledger,
                window_participation=window_participation,
                generated_at=generated_at,
            )
            orders_submitted = int(live_stdout.get("orders_submitted") or 0) + int(
                e5_live_actuator_result.get("orders_submitted") or 0
            ) + int(
                cross_exchange_live_actuator_result.get("orders_submitted") or 0
            ) + int(
                wide_family_live_actuator_result.get("orders_submitted") or 0
            )
            live_status = str(live_stdout.get("status") or "")
            participation_incident = bool(window_participation.get("incident_triggered"))
            participation_missed_windows = int(window_participation.get("missed_active_windows") or 0)
            participation_alerts: list[str] = []
            if participation_incident and orders_submitted <= 0:
                participation_alerts.append("participation_miss_incident")
            elif (
                participation_missed_windows > 0
                and orders_submitted <= 0
                and live_status in {"LIVE_ARMED_NO_FRESH_INTENTS", "LIVE_ARMED_NO_NEW_INTENTS"}
            ):
                participation_alerts.append("no_fresh_live_tradeable_intents")
            blockers = sorted(set(blockers))
            status = "LIVE_GUARD_RUNNING" if not blockers else "LIVE_GUARD_BLOCKED"
            cycle_outcome = "FILTERED_NO_SUBMIT" if live_filtered_no_submit and not blockers else status
            rtds_catchup_lag_s = _rtds_catchup_lag_s(pipeline_result, now_ts=time.time())
            if self_feed_due:
                self_feed_result = _cadence_payload(
                    _run_self_feed_ledger_diff(args),
                    name="self_feed_ledger_diff",
                    generated_at=generated_at,
                    cycle=cycle,
                    every_n=self_feed_every_n,
                    offset=self_feed_offset,
                    executed=True,
                )
            else:
                self_feed_result = _cadence_payload(
                    previous_state.get("self_feed_vs_ledger")
                    if isinstance(previous_state.get("self_feed_vs_ledger"), dict)
                    else {},
                    name="self_feed_ledger_diff",
                    generated_at=generated_at,
                    cycle=cycle,
                    every_n=self_feed_every_n,
                    offset=self_feed_offset,
                    executed=False,
                )
            mark_guard_stage("self_feed_ledger_diff")
            live_execution_payload = dict(live_stdout)
            live_execution_payload["guard_live_execution_runtime"] = {
                "argv": live_result.get("argv") if isinstance(live_result, dict) else None,
                "duration_s": live_result.get("duration_s") if isinstance(live_result, dict) else None,
                "execution_mode": live_result.get("execution_mode") if isinstance(live_result, dict) else None,
                "returncode": live_result.get("returncode") if isinstance(live_result, dict) else None,
                "stderr_tail": live_result.get("stderr_tail") if isinstance(live_result, dict) else "",
            }
            live_execution_payload["window_participation"] = {
                **_compact_window_participation_for_event(window_participation),
                "guard_state_compaction": {
                    "status": "COMPACT_DUPLICATE_TOP_LEVEL_STATE",
                    "rule": (
                        "full window participation rows remain at guard_state.window_participation; "
                        "live_execution carries summary only to avoid retaining duplicate multi-MB rows"
                    ),
                },
            }
            drought_funnel = _live_drought_funnel(
                live_stdout=live_stdout,
                dataapi_poll_result=dataapi_poll_result,
                active_set_rtds_premerge=active_set_rtds_premerge_result,
                window_participation=window_participation,
            )
            live_execution_payload["drought_funnel"] = drought_funnel
            post_live_reporting_every_n = _cadence_every_n(
                args,
                "post_live_reporting_every_n_cycles",
            )
            post_live_reporting_due = _cadence_due(cycle, post_live_reporting_every_n)
            ledger_state = load_json(args.live_ledger_state, default={}, cache_readonly=True)
            ledger_summary = ledger_state.get("summary") if isinstance(ledger_state, dict) else {}
            latest_live_order_ts = _parse_iso_ts(
                ledger_summary.get("latest_order_ts") if isinstance(ledger_summary, dict) else None
            )
            post_live_cap_step_revert = dict(pre_cycle_cap_step_revert)
            mark_guard_stage("active_set_cap_step_revert_post_live")
            if post_live_reporting_due or post_live_reporting_cache is None:
                alternate_source_rotation = _reconcile_alternate_source_rotation(
                    args,
                    active_set_runtime=active_set_runtime,
                )
                own_impact_monitor = _own_impact_monitor(
                    live_stdout=live_stdout,
                    window_participation=window_participation,
                    dataapi_poll_result=dataapi_poll_result,
                    active_set_rtds_premerge=active_set_rtds_premerge_result,
                    live_ledger=ledger_state if isinstance(ledger_state, dict) else {},
                )
                coverage_kpi = _coverage_kpi(
                    active_set_runtime,
                    latest_live_order_ts=latest_live_order_ts,
                    previous_state=previous_state,
                    generated_at=generated_at,
                    set_generation_id=set_generation_id,
                )
                auto_degrade_admission = _maybe_write_auto_degrade_admission(
                    active_set_runtime=active_set_runtime,
                    coverage_kpi=coverage_kpi,
                    live_stdout=live_stdout,
                    generated_at=generated_at,
                )
                coverage_kpi["auto_degrade_admission"] = auto_degrade_admission
                post_live_reporting_cache = (
                    copy.deepcopy(alternate_source_rotation),
                    copy.deepcopy(own_impact_monitor),
                    copy.deepcopy(coverage_kpi),
                )
            else:
                alternate_source_rotation, own_impact_monitor, coverage_kpi = (
                    copy.deepcopy(value) for value in post_live_reporting_cache
                )
                for carried in (alternate_source_rotation, own_impact_monitor, coverage_kpi):
                    carried["hot_path_cadence"] = {
                        "status": "CARRIED_FORWARD",
                        "cycle": cycle,
                        "every_n_cycles": post_live_reporting_every_n,
                        "executed_this_cycle": False,
                    }
            shadow_state_path = str(getattr(args, "shadow_state", "data/research/wallet_copy_guard_shadow_lanes_state.json"))
            if shadow_lanes_due:
                try:
                    full_shadow_lanes_payload = _run_guard_shadow_lanes(args, generated_at=generated_at)
                    shadow_lanes_payload = _cadence_payload(
                        _compact_guard_shadow_lanes_for_guard(
                            full_shadow_lanes_payload,
                            state_path=shadow_state_path,
                        ),
                        name="shadow_lanes",
                        generated_at=generated_at,
                        cycle=cycle,
                        every_n=shadow_lanes_every_n,
                        offset=shadow_lanes_offset,
                        executed=True,
                    )
                except Exception as exc:  # pragma: no cover - shadow route must not disturb live guard.
                    shadow_lanes_payload = _cadence_payload(
                        {
                            "schema_version": 1,
                            "kind": "wallet_copy_guard_shadow_lanes_state",
                            "flow_stage": "LIVE/PROMOTE/OBSERVE",
                            "generated_at": generated_at,
                            "enabled": bool(getattr(args, "shadow_lanes", True)),
                            "status": "SHADOW_ERROR",
                            "paper_only": True,
                            "live_orders_allowed": False,
                            "summary": {"orders_submitted": 0},
                            "error": f"{type(exc).__name__}: {exc}",
                            "next_action": "repair shadow route while live guard continues unchanged",
                        },
                        name="shadow_lanes",
                        generated_at=generated_at,
                        cycle=cycle,
                        every_n=shadow_lanes_every_n,
                        offset=shadow_lanes_offset,
                        executed=True,
                    )
            else:
                prior_shadow_lanes_payload = (
                    previous_state.get("shadow_lanes") if isinstance(previous_state.get("shadow_lanes"), dict) else {}
                )
                if prior_shadow_lanes_payload:
                    prior_shadow_lanes_payload = _compact_guard_shadow_lanes_for_guard(
                        prior_shadow_lanes_payload,
                        state_path=shadow_state_path,
                    )
                shadow_lanes_payload = _cadence_payload(
                    prior_shadow_lanes_payload,
                    name="shadow_lanes",
                    generated_at=generated_at,
                    cycle=cycle,
                    every_n=shadow_lanes_every_n,
                    offset=shadow_lanes_offset,
                    executed=False,
                )
            mark_guard_stage("guard_shadow_lanes")
            cycle_duration_s = round(time.perf_counter() - guard_cycle_started, 6)
            event_triggered_cycle_scheduler = _event_triggered_cycle_scheduler_decision(
                args,
                active_set_rtds_premerge=active_set_rtds_premerge_result,
                previous_state=previous_state,
                blockers=blockers,
                cycle=cycle,
                cycle_started_wall_ts=guard_cycle_started_wall,
                cycle_duration_s=cycle_duration_s,
                generated_at=generated_at,
                now_ts=time.time(),
            )
            slow_path_cadence["event_triggered_cycle_scheduler"] = {
                "enabled": bool(event_triggered_cycle_scheduler.get("enabled")),
                "triggered": bool(event_triggered_cycle_scheduler.get("triggered")),
                "status": event_triggered_cycle_scheduler.get("status"),
                "reason": event_triggered_cycle_scheduler.get("reason"),
                "sleep_s": event_triggered_cycle_scheduler.get("sleep_s"),
                "rule": "fresh copy events shorten only this same guard process sleep before the next decide cycle",
            }
            mark_guard_stage("guard_cycle_scheduler")
            prior_guard_profile = (
                previous_state.get("guard_loop_profile")
                if isinstance(previous_state.get("guard_loop_profile"), dict)
                else {}
            )
            prior_cycle_series = [
                row
                for row in prior_guard_profile.get("cycle_duration_series") or []
                if isinstance(row, dict) and row.get("cycle_started_at")
            ]
            current_cycle_row = {
                "cycle_started_at": dt.datetime.fromtimestamp(
                    guard_cycle_started_wall, dt.timezone.utc
                )
                .isoformat()
                .replace("+00:00", "Z"),
                "cycle_duration_s": cycle_duration_s,
                "active_set_dataapi_poller_every_n_cycles": active_set_dataapi_every_n,
            }
            cycle_series_by_started_at = {
                str(row["cycle_started_at"]): row
                for row in [*prior_cycle_series, current_cycle_row]
            }
            cycle_duration_series = list(cycle_series_by_started_at.values())[-12:]
            freshness_budget_s = round(float(_live_build_max_observed_age_s(args)), 6)
            cycle_series_values = [
                float(row["cycle_duration_s"])
                for row in cycle_duration_series
                if isinstance(row.get("cycle_duration_s"), (int, float))
            ]
            detect_interval_values = _detect_interval_values(
                cycle_duration_series,
                default_every_n=active_set_dataapi_every_n,
            )
            guard_loop_profile = {
                "flow_stage": "LIVE/LEARN/SELF-DEV",
                "status": "MEASURING_GUARD_CYCLE_CADENCE",
                "target_median_iteration_lt_s": 15.0,
                "live_build_max_observed_age_s": freshness_budget_s,
                "cycle_started_wall_ts": round(guard_cycle_started_wall, 6),
                "cycle_started_at": dt.datetime.fromtimestamp(guard_cycle_started_wall, dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "slow_path_cadence": slow_path_cadence,
                "cycle_duration_s": cycle_duration_s,
                "cycle_duration_series": cycle_duration_series,
                "effective_detect_interval_s": round(
                    cycle_duration_s,
                    6,
                ),
                "cycle_duration_health": {
                    "status": (
                        "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"
                        if cycle_series_values
                        and max(cycle_series_values) > freshness_budget_s
                        else "OK"
                    ),
                    "sample_count": len(cycle_series_values),
                    "required_sample_count": 12,
                    "min_s": round(min(cycle_series_values), 6)
                    if cycle_series_values
                    else None,
                    "max_s": round(max(cycle_series_values), 6)
                    if cycle_series_values
                    else None,
                    "freshness_budget_s": freshness_budget_s,
                    "rule": "deduped per-cycle series keyed by cycle_started_at; depth 12",
                },
                "effective_detect_interval_health": _interval_health(
                    detect_interval_values,
                    freshness_budget_s=freshness_budget_s,
                ),
                "total_s_before_state_write": cycle_duration_s,
                "stage_timers": guard_stage_timers,
                "rule": "guard iteration must stay below half the observed-age budget before threshold changes are considered",
            }
            selected_runtime_policy_for_submittability = (
                getattr(args, "active_set_selected_member_policy", {})
                if isinstance(getattr(args, "active_set_selected_member_policy", {}), dict)
                else {}
            )
            runtime_member_submittability = _runtime_member_submittability(
                selected={
                    "candidate_id": candidate_id,
                    "source_wallet": source_wallet,
                    "policy_id": policy.get("policy_id"),
                    "max_order_usd": selected_runtime_policy_for_submittability.get(
                        "max_order_usd",
                        getattr(args, "max_order_usd", None),
                    ),
                },
                selected_runtime_policy={
                    **selected_runtime_policy_for_submittability,
                    "policy_id": policy.get("policy_id"),
                },
                configured_min_live_order_usd=float(
                    getattr(
                        args,
                        "configured_min_live_order_usd",
                        getattr(args, "min_live_order_usd", 1.0),
                    )
                ),
                generated_at=generated_at,
            )
            last_payload = {
                "schema_version": 1,
                "kind": "wallet_copy_live_guard_state",
                "generated_at": generated_at,
                "status": status,
                "cycle_outcome": cycle_outcome,
                "cycle": cycle,
                "pid": os.getpid(),
                "guard_code_identity": guard_code_identity,
                "mission_contract_hot_reload": mission_hot_reload,
                "execute_live": bool(args.execute_live),
                "paper_only": bool(blockers) or not bool(args.execute_live),
                "live_orders_allowed": bool(args.execute_live and args.live_orders_allowed and not blockers),
                "operator_live_authority": _operator_live_authority_snapshot(args),
                "candidate_id": candidate_id,
                "mission_candidate_id": mission_candidate_id,
                "mission_candidate_id_alias_matched": bool(candidate.get("mission_candidate_id_alias_matched")),
                "mission_matched_profit_candidate_id": candidate.get("mission_matched_profit_candidate_id"),
                "candidate_type": candidate.get("candidate_type"),
                "source_wallet": source_wallet,
                "policy_id": policy.get("policy_id"),
                "runtime_member_submittability": runtime_member_submittability,
                "intent_time_copyability_proof": intent_time_copyability_proof,
                **copy_contract,
                "guard_runtime_filter": {
                    "max_event_age_s": float(args.max_event_age_s),
                    "live_build_max_observed_age_s": _live_build_max_observed_age_s(args),
                    "max_intents": int(args.max_intents),
                    "min_live_order_usd": float(args.min_live_order_usd),
                    "wallet_fraction": float(args.wallet_fraction),
                    "max_order_usd": float(args.max_order_usd),
                    "alpha_decay_report": str(args.alpha_decay_report),
                    "runtime_history": _live_guard_runtime_history(args),
                    "history_state": str(getattr(args, "history_state", "")),
                    "history_window_index": str(getattr(args, "history_window_index", "")),
                    "enable_drift_buffer": bool(args.enable_drift_buffer),
                    "max_drift_buffer_price": float(args.max_drift_buffer_price),
                    "enable_maker_fallback": bool(args.enable_maker_fallback),
                    "copy_model": str(getattr(args, "copy_model", "inventory") or "inventory"),
                    "inventory_late_window_stop_s": float(getattr(args, "inventory_late_window_stop_s", 60.0)),
                    "inventory_max_converge_orders_per_window": int(
                        getattr(args, "inventory_max_converge_orders_per_window", 6)
                    ),
                    "inventory_best_ask_timeout_s": float(getattr(args, "inventory_best_ask_timeout_s", 1.0)),
                    "inventory_future_window_lookahead_s": float(
                        getattr(args, "inventory_future_window_lookahead_s", INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S)
                    ),
                    "drip_min_tranche_usd": float(getattr(args, "drip_min_tranche_usd", 1.0)),
                    "drip_max_tranche_usd": float(getattr(args, "drip_max_tranche_usd", 2.5)),
                    "drip_max_tranches_per_window": int(getattr(args, "drip_max_tranches_per_window", 12)),
                    "per_window_fill_cap": int(getattr(args, "per_window_fill_cap", 1)),
                    "total_loss_member_auto_disable_enabled": not bool(
                        getattr(args, "disable_total_loss_member_auto_disable", False)
                    ),
                    "total_loss_auto_disable_min_resolved_fills": int(
                        getattr(
                            args,
                            "total_loss_auto_disable_min_resolved_fills",
                            TOTAL_LOSS_AUTO_DISABLE_MIN_RESOLVED_FILLS,
                        )
                    ),
                    "price_band_decision_since": str(getattr(args, "price_band_decision_since", "") or ""),
                    "price_band_decision_max_price": float(getattr(args, "price_band_decision_max_price", 0.50)),
                    "price_band_decision_min_price": float(getattr(args, "price_band_decision_min_price", 0.25)),
                    "price_band_decision_min_resolved": int(getattr(args, "price_band_decision_min_resolved", 10)),
                    "price_band_decision_min_fill_rate_pct": float(
                        getattr(args, "price_band_decision_min_fill_rate_pct", 40.0)
                    ),
                    "price_band_decision_refresh_s": float(getattr(args, "price_band_decision_refresh_s", 30.0)),
                    "pipeline_limit": int(args.pipeline_limit),
                    "pipeline_pages": int(args.pipeline_pages),
                    "pipeline_include_activity": bool(args.pipeline_include_activity),
                    "pipeline_parallel_data_api_sources": True,
                    "pipeline_data_api_timeout_s": float(args.pipeline_data_api_timeout_s),
                    "pipeline_data_api_retries": int(args.pipeline_data_api_retries),
                    "pipeline_data_api_trade_query_keys": str(args.pipeline_data_api_trade_query_keys),
                    "rtds_jsonl": str(getattr(args, "rtds_jsonl", "") or ""),
                    "rtds_scan_limit": int(getattr(args, "rtds_scan_limit", 0) or 0),
                    "rtds_max_new_events": int(getattr(args, "rtds_max_new_events", 0) or 0),
                    "rtds_tail_bytes": int(getattr(args, "rtds_tail_bytes", 0) or 0),
                    "rtds_signal_watermark_state": str(
                        getattr(args, "rtds_signal_watermark_state", DEFAULT_RTDS_SIGNAL_WATERMARK_STATE)
                    ),
                    "rtds_tail_backfill_bytes": _rtds_tail_backfill_bytes(args),
                    "active_set_rtds_premerge": bool(getattr(args, "active_set_rtds_premerge", True)),
                    "active_set_rtds_premerge_member_limit": int(
                        getattr(args, "active_set_rtds_premerge_member_limit", 0) or 0
                    ),
                    "rtds_offset_state": (
                        _rtds_offset_state(args, source_wallet=source_wallet)
                        if str(getattr(args, "rtds_jsonl", "") or "")
                        else ""
                    ),
                    "rtds_catchup_lag_s": rtds_catchup_lag_s,
                    "active_set_dataapi_poller": bool(getattr(args, "active_set_dataapi_poller", True)),
                    "active_set_dataapi_poller_interval_s": float(
                        getattr(args, "active_set_dataapi_poller_interval_s", 1.0)
                    ),
                    "active_set_dataapi_poller_state": str(
                        getattr(args, "active_set_dataapi_poller_state", DEFAULT_ACTIVE_SET_DATAAPI_POLLER_STATE)
                    ),
                    "active_set_dataapi_poller_limit": int(getattr(args, "active_set_dataapi_poller_limit", 500)),
                    "active_set_dataapi_poller_pages": int(getattr(args, "active_set_dataapi_poller_pages", 2)),
                    "active_set_dataapi_poller_every_n_cycles": active_set_dataapi_every_n,
                    "active_set_dataapi_poller_cycle_offset": active_set_dataapi_offset,
                    "active_set_dataapi_poller_trade_query_keys": str(
                        getattr(args, "active_set_dataapi_poller_trade_query_keys", "")
                        or getattr(args, "pipeline_data_api_trade_query_keys", "user")
                        or "user"
                    ),
                    "active_set_dataapi_poller_disable_source_base_overrides": _active_set_dataapi_disable_source_base_overrides(args),
                    "active_set_live_execution_probes": bool(
                        getattr(args, "active_set_live_execution_probes", True)
                    ),
                    "active_set_live_execution_probe_max_members": int(
                        getattr(
                            args,
                            "active_set_live_execution_probe_max_members",
                            DEFAULT_ACTIVE_SET_LIVE_EXECUTION_PROBE_MAX_MEMBERS,
                        )
                    ),
                    "active_set_live_execution_probes_every_n_cycles": active_set_probe_every_n,
                    "active_set_live_execution_probes_cycle_offset": active_set_probe_offset,
                    "active_set_live_execution_probe_slow_path_member_cap": active_set_probe_member_cap,
                    "active_set_evaluate_all_runtime_members_per_cycle": bool(
                        getattr(args, "active_set_evaluate_all_runtime_members_per_cycle", False)
                    ),
                    "watch_tier_dataapi_poller": bool(getattr(args, "watch_tier_dataapi_poller", True)),
                    "watch_tier_wallets_config": str(
                        getattr(args, "watch_tier_wallets_config", DEFAULT_WATCH_TIER_WALLETS_CONFIG)
                    ),
                    "watch_tier_dataapi_poller_state": str(
                        getattr(args, "watch_tier_dataapi_poller_state", DEFAULT_WATCH_TIER_POLLER_STATE)
                    ),
                    "watch_tier_dataapi_poller_every_n_cycles": watch_tier_every_n,
                    "watch_tier_dataapi_poller_cycle_offset": watch_tier_offset,
                    "watch_tier_dataapi_poller_max_wallets_per_cycle": int(
                        getattr(args, "watch_tier_dataapi_poller_max_wallets_per_cycle", 4) or 0
                    ),
                    "watch_tier_history_state": str(
                        getattr(args, "watch_tier_history_state", DEFAULT_WATCH_TIER_HISTORY_STATE)
                    ),
                    "watch_tier_wallet_event_log": str(
                        getattr(args, "watch_tier_wallet_event_log", DEFAULT_WATCH_TIER_WALLET_EVENT_LOG)
                    ),
                    "self_feed_ledger_diff": bool(getattr(args, "self_feed_ledger_diff", True)),
                    "self_feed_ledger_diff_state": str(
                        getattr(args, "self_feed_ledger_diff_state", DEFAULT_SELF_FEED_VS_LEDGER_STATE)
                    ),
                    "self_feed_log": str(getattr(args, "self_feed_log", DEFAULT_SELF_FEED_LOG)),
                    "self_feed_ledger_missing_grace_s": float(
                        getattr(args, "self_feed_ledger_missing_grace_s", 300.0)
                    ),
                    "self_feed_polygon": bool(getattr(args, "self_feed_polygon", True)),
                    "fuse_hot_path": bool(getattr(args, "fuse_hot_path", True)),
                    "event_triggered_cycle_scheduler": bool(
                        getattr(args, "event_triggered_cycle_scheduler", True)
                    ),
                    "event_triggered_cycle_trigger_sleep_s": float(
                        getattr(args, "event_triggered_cycle_trigger_sleep_s", 0.0) or 0.0
                    ),
                    "event_triggered_cycle_max_signal_age_s": float(
                        getattr(args, "event_triggered_cycle_max_signal_age_s", 0.0) or 0.0
                    ),
                    "guard_slow_path_every_n_cycles": int(
                        getattr(args, "guard_slow_path_every_n_cycles", 4) or 4
                    ),
                    "self_feed_ledger_diff_every_n_cycles": self_feed_every_n,
                    "self_feed_ledger_diff_cycle_offset": self_feed_offset,
                    "shadow_lanes": bool(getattr(args, "shadow_lanes", True)),
                    "e5_live_actuator": bool(getattr(args, "e5_live_actuator", True)),
                    "e5_live_intents_state": str(getattr(args, "e5_live_intents_state", "")),
                    "e5_live_actuator_state": str(getattr(args, "e5_live_actuator_state", "")),
                    "cross_exchange_live_actuator": bool(
                        getattr(args, "cross_exchange_live_actuator", False)
                    ),
                    "cross_exchange_paper_state": str(
                        getattr(args, "cross_exchange_paper_state", "")
                    ),
                    "cross_exchange_live_actuator_state": str(
                        getattr(args, "cross_exchange_live_actuator_state", "")
                    ),
                    "cross_exchange_live_actuator_ttl_s": float(
                        getattr(
                            args,
                            "cross_exchange_live_actuator_ttl_s",
                            _CROSS_EXCHANGE_ACTIVATION_TTL_S,
                        )
                    ),
                    "wide_family_live_actuator": bool(
                        getattr(args, "wide_family_live_actuator", True)
                    ),
                    "wide_family_state": str(getattr(args, "wide_family_state", "")),
                    "wide_family_live_actuator_state": str(
                        getattr(args, "wide_family_live_actuator_state", "")
                    ),
                    "shadow_lanes_every_n_cycles": shadow_lanes_every_n,
                    "shadow_lanes_cycle_offset": shadow_lanes_offset,
                    "shadow_state": str(getattr(args, "shadow_state", "")),
                    "shadow_max_intents_per_lane": int(getattr(args, "shadow_max_intents_per_lane", 0) or 0),
                    "routing_router_mode": str(getattr(args, "routing_router_mode", "shadow") or "shadow"),
                    "routing_shadow_validation_state": str(
                        getattr(args, "routing_shadow_validation_state", DEFAULT_ROUTING_SHADOW_VALIDATION_STATE)
                    ),
                    "routing_shadow_candidate_seats": str(
                        getattr(args, "routing_shadow_candidate_seats", DEFAULT_ROUTING_SHADOW_CANDIDATE_SEATS)
                    ),
                    "routing_shadow_validation_min_hours": float(
                        getattr(args, "routing_shadow_validation_min_hours", DEFAULT_ROUTING_SHADOW_MIN_VALIDATION_HOURS)
                    ),
                },
                "active_set": active_set_runtime,
                "active_set_runtime": active_set_runtime,
                "coverage_kpi": coverage_kpi,
                "live_source_route_gate": source_route_gate,
                "candidate": {
                    "candidate_id": candidate_id,
                    "mission_candidate_id": mission_candidate_id,
                    "mission_candidate_id_alias_matched": bool(candidate.get("mission_candidate_id_alias_matched")),
                    "mission_matched_profit_candidate_id": candidate.get("mission_matched_profit_candidate_id"),
                    "candidate_type": candidate.get("candidate_type"),
                    "source_wallet": source_wallet,
                    "policy": policy,
                    "pass_gate": candidate.get("guard_pass_gate")
                    if isinstance(candidate.get("guard_pass_gate"), dict)
                    else {},
                    "selection_attempts": candidate.get("guard_selection_attempts")
                    if isinstance(candidate.get("guard_selection_attempts"), list)
                    else [],
                    "fallthrough": candidate.get("guard_fallthrough")
                    if isinstance(candidate.get("guard_fallthrough"), dict)
                    else {},
                },
                "candidate_pass_gate_diagnostics": _candidate_pass_gate_diagnostics(candidate, blockers),
                "blockers": blockers,
                "ruled_flat_active_set": ruled_flat_active_set,
                "operator_notify_suppression": operator_notify_suppression,
                "operator_notify_transition": operator_notify_transition,
                "participation_alerts": sorted(set(participation_alerts)),
                "pipeline": pipeline_result,
                "active_set_rtds_premerge": active_set_rtds_premerge_result,
                "alternate_transport_copyintent_bridge": alternate_transport_copyintent_bridge_result,
                "alternate_source_rotation": alternate_source_rotation,
                "active_set_dataapi_poller": dataapi_poll_result,
                "active_set_cap_step_revert": {
                    "pre_cycle": pre_cycle_cap_step_revert,
                    "post_live": post_live_cap_step_revert,
                },
                "active_set_f418_tenure_pin": pre_cycle_f418_tenure_pin,
                "active_set_weekend_seat_loss_rotation": pre_cycle_weekend_seat_loss_rotation,
                "order135_direct_gate_pass": pre_cycle_order135_direct_gate,
                "active_set_live_execution_probes": live_probe_result,
                "active_set_live_execution_probe_promotions": live_probe_promotion_result,
                "routing_shadow_validation": routing_shadow_validation_result,
                "watch_tier_dataapi_poller": watch_tier_poll_result,
                "self_feed_vs_ledger": self_feed_result,
                "own_impact_monitor": own_impact_monitor,
                "rtds_catchup_lag_s": rtds_catchup_lag_s,
                "drought_funnel": drought_funnel,
                "live_execution": live_execution_payload,
                "e5_live_actuator": e5_live_actuator_result,
                "cross_exchange_live_actuator": cross_exchange_live_actuator_result,
                "wide_family_live_actuator": wide_family_live_actuator_result,
                "event_triggered_cycle_scheduler": event_triggered_cycle_scheduler,
                "shadow_lanes": shadow_lanes_payload,
                "window_participation": _compact_window_participation_for_guard_state(
                    window_participation
                ),
                "active_set": active_set,
                "coverage_kpi": coverage_kpi,
                "auto_degrade_admission": auto_degrade_admission,
                "live_price_band_decision": live_price_band_decision,
                "live_execution_runtime": {
                    "argv": live_result.get("argv") if isinstance(live_result, dict) else None,
                    "duration_s": live_result.get("duration_s") if isinstance(live_result, dict) else None,
                    "execution_mode": live_result.get("execution_mode") if isinstance(live_result, dict) else None,
                    "returncode": live_result.get("returncode") if isinstance(live_result, dict) else None,
                    "stderr_tail": live_result.get("stderr_tail") if isinstance(live_result, dict) else "",
                },
                "guard_loop_profile": guard_loop_profile,
                "live_execution_returncode": live_result.get("returncode") if isinstance(live_result, dict) else None,
            }
            last_payload["recent_cycle_counts"] = _recent_cycle_counts(args, current_payload=last_payload)
            last_payload["_state_path"] = str(args.state)
            _sync_live_ledger_runtime_permission(args, last_payload)
            _sync_blocked_live_arm_state(args, payload=last_payload, source_route_gate=source_route_gate)
            guard_cycle_tail_attribution = _guard_cycle_tail_attribution(
                previous_write_state_file_measurement,
                shadow_lanes_payload,
                cycle,
            )
            last_payload["guard_cycle_tail_attribution"] = guard_cycle_tail_attribution
            guard_loop_profile["guard_cycle_tail_attribution"] = guard_cycle_tail_attribution
            guard_loop_profile["stage_timers"] = guard_stage_timers
            guard_loop_profile["total_s_before_state_write"] = round(time.perf_counter() - guard_cycle_started, 6)
            guard_loop_profile["cycle_duration_s"] = guard_loop_profile["total_s_before_state_write"]
            final_cycle_series = guard_loop_profile.get("cycle_duration_series") or []
            if final_cycle_series:
                final_cycle_series[-1]["cycle_duration_s"] = guard_loop_profile["cycle_duration_s"]
                final_cycle_series[-1]["active_set_dataapi_poller_every_n_cycles"] = active_set_dataapi_every_n
                final_cycle_values = [
                    float(row["cycle_duration_s"])
                    for row in final_cycle_series
                    if isinstance(row, dict)
                    and isinstance(row.get("cycle_duration_s"), (int, float))
                ]
                final_detect_values = _detect_interval_values(
                    final_cycle_series,
                    default_every_n=active_set_dataapi_every_n,
                )
                guard_loop_profile["effective_detect_interval_s"] = round(
                    guard_loop_profile["cycle_duration_s"],
                    6,
                )
                guard_loop_profile["cycle_duration_health"].update(
                    {
                        "status": (
                            "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"
                            if final_cycle_values
                            and max(final_cycle_values) > freshness_budget_s
                            else "OK"
                        ),
                        "sample_count": len(final_cycle_values),
                        "min_s": round(min(final_cycle_values), 6),
                        "max_s": round(max(final_cycle_values), 6),
                    }
                )
                guard_loop_profile["effective_detect_interval_health"] = _interval_health(
                    final_detect_values,
                    freshness_budget_s=freshness_budget_s,
                )
            last_payload["effective_detect_interval_health"] = guard_loop_profile.get(
                "effective_detect_interval_health", {}
            )
            dataapi_executed = bool(
                (dataapi_poll_result.get("cadence") or {}).get("executed_this_cycle")
            )
            current_active_set_dataapi = {}
            if dataapi_executed and dataapi_poll_result.get("status"):
                current_active_set_dataapi = {
                    "status": dataapi_poll_result.get("status"),
                    "cadence": dataapi_poll_result.get("cadence") or {},
                }
            last_payload["active_set_dataapi"] = _cadenced_root_mirror(
                current_active_set_dataapi,
                previous_state.get("active_set_dataapi")
                if isinstance(previous_state.get("active_set_dataapi"), dict)
                else {},
                generated_at=generated_at,
                executed_this_cycle=dataapi_executed,
            )
            last_payload["live_status_registration"] = (
                active_set_runtime.get("live_status_registration")
                if isinstance(active_set_runtime.get("live_status_registration"), dict)
                else {}
            )
            current_freshness_discriminator = (
                dataapi_poll_result.get("freshness_acceptance_discriminator")
                if dataapi_executed
                and isinstance(dataapi_poll_result.get("freshness_acceptance_discriminator"), dict)
                else {}
            )
            last_payload["freshness_discriminator"] = _cadenced_root_mirror(
                current_freshness_discriminator,
                previous_state.get("freshness_discriminator")
                if isinstance(previous_state.get("freshness_discriminator"), dict)
                else {},
                generated_at=generated_at,
                executed_this_cycle=dataapi_executed,
            )
            previous_write_state_file_measurement = _write_state(args, last_payload)
            mark_guard_stage("guard_state_persist")
            guard_loop_profile["stage_timers_after_persist"] = guard_stage_timers
            guard_loop_profile["persist_marker_note"] = (
                "guard_state_persist includes the authoritative state write plus post-write file stat; "
                "the next cycle state carries that stat with measurement_lag_cycles=1"
            )
            print(json.dumps(_stdout_cycle_summary(last_payload), sort_keys=True, default=str), flush=True)
            if int(base_args.iterations) > 0 and cycle >= int(base_args.iterations):
                break
            time.sleep(max(0.0, float(event_triggered_cycle_scheduler.get("sleep_s", base_args.sleep_s))))
    finally:
        orderfilled_fast_lane.stop()
        lock.close()
    return 0 if not last_payload.get("blockers") else 2


if __name__ == "__main__":
    raise SystemExit(main())
