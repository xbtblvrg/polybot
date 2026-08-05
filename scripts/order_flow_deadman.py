#!/usr/bin/env python3
"""Order-flow dead-man switch (zero-AI, mechanical).

Operator order 2026-07-07 (verbatim anger): "ez hiba nagy nagy hiba hogy
nem mennek a live orderek!!!!!!!!!!!" — the system must NEVER again fail
to notice a live-order drought on its own. This check needs no AI, no
interpretation, no denominator that can go quietly empty: if the guard
says it can trade and no live order has been accepted for
--max-idle-s seconds, that IS an incident. Runs from brainless_ops
every 10 minutes.

Fires: incident JSON + HANDOFF evidence; the shared operator-notification
discipline pushes only a new incident class or an actionable transition and
aggregates recurring same-class incidents by UTC day.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import os
import re
import shlex
import sys
import statistics
from collections import Counter, defaultdict, deque
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.participation import floor_blocked_miss
from src.wallet_copy.gate_registry import (
    GUARD_AUTHORED_GATE_CLASSES,
    PRE_SUBMIT_REFUSAL_CLASSES,
)
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.venue_executability import venue_gate_summary
from scripts.operator_notification_discipline import record_operator_event
from scripts.brainless_live_guard_restart import disk_generation
from scripts.report_regime_seat_selection import acceptance_share_rows, policy_choke_rung_a
from scripts.reconcile_wide_exact_policy_paper import manifest_wallet_policy_identities
from scripts.wide_direct_handoff_journal import (
    DEFAULT_JOURNAL as DEFAULT_WIDE_DIRECT_JOURNAL,
    DEFAULT_TERMINAL_LOG as DEFAULT_WIDE_TERMINAL_LOG,
    append_envelopes,
    envelope_from_packet,
    envelopes_from_incidents,
    load_jsonl,
)
from scripts.order_flow_incident_archive import (  # noqa: E402
    append_incident_row,
    load_incident_rows,
)

DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_EVENT_LOG = "data/research/wallet_copy_live_execution_events.jsonl"
DEFAULT_GUARD_EVENT_LOG = "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_ACTIVE_SET_STATE = "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_STATE = "data/research/order_flow_deadman_state.json"
DEFAULT_LOCK = "data/research/order_flow_deadman.lock"
DEFAULT_LOCK_SKIP_LOG = "data/research/order_flow_deadman_lock_skips.jsonl"
DEFAULT_ADMISSION_INTERVAL_LOG = (
    "data/research/order_flow_deadman_admission_intervals.jsonl"
)
DEFAULT_DEADMAN_CYCLE_LOG = "data/research/order_flow_deadman_cycles.jsonl"
DEFAULT_HANDOFF = "docs/agents/HANDOFF.md"
DEFAULT_TEMPORAL = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_HOT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_COPY_INTENTS_STATE = "data/research/wallet_copy_live_guard_copy_intents_state.json"
DEFAULT_QUALIFIED_POOL_STAKEOUT = (
    "data/research/copy_qualified_pool_orderfilled_resident_stakeout_state.json"
)
DEFAULT_ROUTING_SHADOW = "data/research/routing_shadow_validation_latest.json"
DEFAULT_READY_SHADOW = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_COHORT_ADMISSION = "data/research/cohort_alive_admission_packets_latest.json"
DEFAULT_FULL_POOL_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_WIDE_DIRECT_STATE = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_RTDS_CAPTURE = (
    "data/research/polymarket_activity_ws_capture_vpn_burnin_20260703T180934Z.jsonl"
)
DEFAULT_RTDS_LIVENESS_STATE = "data/research/order140_rtds_liveness_state.json"
STICKY_PAPER_ACCRUAL_FOCUS: tuple[tuple[str, str], ...] = ()


def _update_rtds_observed_liveness(
    capture_path: Path,
    state_path: Path,
    *,
    now: datetime,
    max_bytes: int = 64 * 1024 * 1024,
) -> dict[str, Any]:
    """Incrementally measure per-wallet RTDS trade liveness from a bounded tail."""

    state = load_json(state_path, default={})
    state = state if isinstance(state, dict) else {}
    per_wallet = state.get("per_wallet") if isinstance(state.get("per_wallet"), dict) else {}
    try:
        stat = capture_path.stat()
    except OSError as exc:
        return {
            "status": "CAPTURE_UNAVAILABLE",
            "error": str(exc),
            "bytes_read": 0,
            "per_wallet": per_wallet,
        }
    prior_inode = int(state.get("inode") or 0)
    prior_offset = int(state.get("byte_offset") or 0)
    reset = prior_inode != int(stat.st_ino) or prior_offset < 0 or prior_offset > stat.st_size
    start = max(0, stat.st_size - max_bytes) if reset or prior_offset == 0 else prior_offset
    if stat.st_size - start > max_bytes:
        start = max(0, stat.st_size - max_bytes)
        reset = True
    rows_read = 0
    observed_rows = 0
    with capture_path.open("rb") as handle:
        handle.seek(start)
        if start and reset:
            handle.readline()
        actual_start = handle.tell()
        while handle.tell() < stat.st_size:
            raw = handle.readline()
            if not raw:
                break
            rows_read += 1
            try:
                row = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                continue
            wallet = _normalize_wallet(row.get("source_wallet"))
            if not wallet or str(row.get("event") or "") != "rtds_trade_event":
                continue
            event_ts = _as_float(row.get("event_ts"))
            if event_ts is None:
                continue
            observed_rows += 1
            current = per_wallet.get(wallet) if isinstance(per_wallet.get(wallet), dict) else {}
            if event_ts >= float(current.get("latest_event_ts") or 0.0):
                per_wallet[wallet] = {
                    "latest_event_ts": event_ts,
                    "latest_received_at_s": _as_float(row.get("received_at_s")),
                    "latest_event_id": row.get("event_id"),
                    "latest_market_slug": row.get("market_slug"),
                }
        end = handle.tell()
    now_s = now.timestamp()
    published_wallets = {
        wallet: {
            **row,
            "observed_age_s": round(max(0.0, now_s - float(row["latest_event_ts"])), 6),
        }
        for wallet, row in per_wallet.items()
        if isinstance(row, dict) and _as_float(row.get("latest_event_ts")) is not None
    }
    next_state = {
        "flow_stage": "LIVE/DEFEND/MEASURE",
        "status": "PASS",
        "checked_at": now.isoformat(),
        "capture_path": str(capture_path),
        "inode": int(stat.st_ino),
        "byte_offset": end,
        "capture_size_bytes": int(stat.st_size),
        "bytes_read": max(0, end - actual_start),
        "rows_read": rows_read,
        "observed_trade_rows": observed_rows,
        "offset_reset_to_bounded_tail": reset,
        "max_bytes_per_cycle": max_bytes,
        "per_wallet": published_wallets,
    }
    atomic_write_json(state_path, next_state)
    return next_state


def _gap_closing_coverage(candidates: dict[str, Any]) -> list[str]:
    """Return bounded frontier coverage plus the directed paper-only focus."""

    covered = [
        str(row.get("wallet") or "").lower()
        for row in (candidates.get("nearest_frontier") or [])[:20]
        if isinstance(row, dict) and row.get("wallet")
    ]
    for wallet, _fingerprint in STICKY_PAPER_ACCRUAL_FOCUS:
        if wallet not in covered:
            covered.append(wallet)
    return covered


def _brain_candidate_evidence(value: Any) -> dict[str, Any]:
    """Bound prompt output while retaining full rows in the state artifact."""

    evidence = value if isinstance(value, dict) else {}
    return {
        "candidate_count": evidence.get("candidate_count"),
        "eligible_count": evidence.get("eligible_count"),
        "status": evidence.get("status"),
        "refusal_counts": evidence.get("refusal_counts") or {},
        "nearest_top3": [
            {
                "wallet": row.get("wallet"),
                "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
                "evidence_deficits": row.get("evidence_deficits") or [],
                "direct_attempts": (row.get("direct_source") or {}).get("attempts"),
                "direct_copyable": (row.get("direct_source") or {}).get("copyable"),
            }
            for row in (evidence.get("nearest_frontier") or [])[:3]
            if isinstance(row, dict)
        ],
        "full_artifact": DEFAULT_STATE,
    }


def _brain_policy_choke(value: Any) -> dict[str, Any]:
    choke = value if isinstance(value, dict) else {}
    actuator = choke.get("actuator") if isinstance(choke.get("actuator"), dict) else {}
    drought = choke.get("source_roster_drought") if isinstance(choke.get("source_roster_drought"), dict) else {}
    direct = drought.get("direct_source") if isinstance(drought.get("direct_source"), dict) else {}
    return {
        key: choke.get(key)
        for key in (
            "status", "firing", "selected_wallet", "selected_fresh_source_rows",
            "whole_runtime_fresh_source_rows", "selected_eligible_intents",
            "whole_runtime_eligible_intents", "selected_accepted_orders",
            "whole_runtime_accepted_orders", "method_accepted_orders",
            "wallet_policy_diagnostic", "mechanical_escalation",
        )
    } | {
        "actuator": {
            "status": actuator.get("status"),
            "action": actuator.get("action"),
            "selected_wallet": actuator.get("selected_wallet"),
            "candidate_evidence": _brain_candidate_evidence(actuator.get("candidate_evidence")),
        },
        "source_roster_drought": {
            "status": drought.get("status"),
            "firing": drought.get("firing"),
            "direct_source": {
                key: direct.get(key)
                for key in (
                    "status", "checksum", "fresh_rows", "copyable",
                    "latest_receipt_at", "latest_non_empty_generation_index",
                    "latest_non_empty_generation_receipt_at",
                )
            },
            "candidate_evidence": _brain_candidate_evidence(drought.get("candidate_evidence")),
        },
        "full_artifact": DEFAULT_STATE,
    }


def _utc_day_money(ledger: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Measure today's filled notional and resolved PnL from canonical orders."""

    realized_pnl_usd = 0.0
    filled_size_usd = 0.0
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        submitted_at = _parse_ts(order.get("submitted_at") or order.get("updated_at"))
        if submitted_at is None or submitted_at.date() != now.date():
            continue
        trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        filled_size_usd += float(
            order.get("response_filled_size_usd")
            or order.get("filled_size_usd")
            or trade_result.get("response_filled_size_usd")
            or trade_result.get("filled_size_usd")
            or 0.0
        )
        attribution = (
            order.get("alternate_transport_attribution")
            if isinstance(order.get("alternate_transport_attribution"), dict)
            else {}
        )
        if str(attribution.get("resolution_status") or "").upper() == "RESOLVED":
            realized_pnl_usd += float(attribution.get("resolved_post_fee_pnl_usd") or 0.0)
    return {
        "utc_day": now.date().isoformat(),
        "realized_pnl_usd": round(realized_pnl_usd, 6),
        "filled_size_usd": round(filled_size_usd, 6),
    }


def _money_anchored_status(*, accepted_order_idle_s: float | None, money: dict[str, Any]) -> str:
    return (
        "FLOW_DEAD_MONEY_ANCHORED"
        if accepted_order_idle_s is not None
        and accepted_order_idle_s > 21600.0
        and float(money.get("realized_pnl_usd") or 0.0) == 0.0
        and float(money.get("filled_size_usd") or 0.0) == 0.0
        else "OK"
    )


DEFAULT_WIDE_FRONTIER = "data/research/wide_direct_admissible_frontier_latest.json"
DEFAULT_WIDE_FINGERPRINT_EVIDENCE = (
    "data/research/wide_policy_fingerprint_evidence_latest.json"
)
DEFAULT_BAC25_FORWARD_LANE = "data/research/bac25_forward_only_lane_latest.json"
DEFAULT_BAC25_FORWARD_MANIFEST = (
    "data/research/wide_exact_policy_manifest_bac25_forward_only.json"
)
DEFAULT_BAC25_FORWARD_EVIDENCE = (
    "data/research/bac25_forward_only_evidence_latest.json"
)
DEFAULT_FREEZE_ALLPASS_SIDECAR = (
    "data/research/copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json"
)
DEFAULT_RECOVERY_TOKEN_STATE = "data/research/wide_token_map_recovery_state.json"
DEFAULT_RECOVERY_TOKEN_PREREG = "data/research/wide_token_map_recovery_preregistration.json"
DEFAULT_RECOVERY_ALPHA_STATE = "data/research/wide_alpha_counterfactual_state.json"
DEFAULT_RECOVERY_ALPHA_PREREG = "data/research/wide_alpha_counterfactual_preregistration.json"
DEFAULT_RECOVERY_DECISION = "data/research/wide_recovery_method_switch_decision.json"
DEFAULT_RECOVERY_PASSIVE_RESIDUAL_STATE = (
    "data/research/btc5m_multivenue_ttl_passive_residual_state.json"
)
DEFAULT_RECOVERY_MAKER_FIRST_RESIDUAL_STATE = (
    "data/research/btc5m_multivenue_ttl_maker_first_residual_state.json"
)
DEFAULT_STANDBY_READINESS = "data/research/pipeline_slo_and_standby_readiness_latest.json"
DEFAULT_GUARD_MEMORY_RESTART_SCRIPT = "scripts/brainless_live_guard_restart.py"
INCIDENT_EVIDENCE_JSONL = Path("data/research/order_flow_deadman_incidents.jsonl")
EPISODE_EVIDENCE_JSONL = Path("data/research/order_flow_deadman_episodes.jsonl")
EPISODE_EVIDENCE_MANIFEST = Path("data/research/order_flow_deadman_episodes_manifest.json")
BRAINLESS_RESTART_EVENTS_JSONL = Path(
    "data/research/brainless_live_guard_restart_events.jsonl"
)
EPISODE_EVIDENCE_FIRST_ARMED_COMMIT = "09176b85"
EPISODE_EVIDENCE_PRIOR_INCIDENT_ROWS = 12
EPISODE_EVIDENCE_PRIOR_RECORDED_CLEARS = 0
POLICY_CHOKE_FIRE_DRILL = Path("data/research/order_flow_deadman_policy_choke_fire_drill_latest.json")
LOCAL_REFUSAL_REJECT_CLASSES = set(PRE_SUBMIT_REFUSAL_CLASSES)
SOURCE_ROSTER_DROUGHT_FIRE_DRILL = Path(
    "data/research/order_flow_deadman_source_roster_drought_fire_drill_latest.json"
)
DEFAULT_STANDBY_PARK_REGISTRY = (
    "data/research/wallet_copy_standby_park_exclusions.json"
)
APPROVED_SUPPRESSION_TAGS = {
    "entry_price_band_closed_negative_holdout",
    "entry_price_band_gate",
    "market_buy_precision_infeasible",
    "window_fill_cap",
    "window_time_gte_180s",
    "signal_age_gte_60s",
}
PIPE_QUIET_GUARD_EVENT_MAX_AGE_S = 120.0
PIPE_QUIET_MAX_CLOCK_SKEW_S = 300.0
PIPE_QUIET_POLLER_MAX_AGE_S = 900.0
GATED_QUIET_HARD_BACKSTOP_S = 3600.0
SOURCE_QUIET_HARD_BACKSTOP_S = 4 * 3600.0
GUARD_SIDE_HALT_BACKSTOP_S = 90 * 60.0
FLOOR_DEADLOCK_MIN_LOOKBACK_S = 6 * 3600.0
GUARD_MEMORY_WARN_GIB = 5.0
GUARD_MEMORY_RESTART_GIB = 6.0
GUARD_MEMORY_AUTO_RESTART_COOLDOWN_S = 3600.0
FLOOR_DEADLOCK_EXACT_REASONS = {
    "drip_min_tranche_exceeds_window_budget",
    "drip_residual_gap_below_min_tranche",
    "entry_price_band_gate",
    "hard_entry_floor_skip",
    "inventory_residual_gap_below_min_order",
}
TIMING_SKIP_EXACT_REASONS = {
    "filled",
    "inventory_late_window_guard",
    "inventory_target_already_met",
    "inventory_window_state_stale",
    "submitted",
    "window_time_gte_180s",
}
APPROVED_GATED_QUIET_EXACT = GUARD_AUTHORED_GATE_CLASSES | {
    "best_ask_missing",
    "below_ruled_entry_floor",
    "drip_min_tranche_exceeds_window_budget",
    "drip_residual_gap_below_min_tranche",
    "entry_price_band_gate",
    "entry_price_band_closed_negative_holdout",
    "filled",
    "filtered_after_inventory_build",
    "fak_no_match",
    "hard_entry_cap_skip",
    "hard_entry_floor_skip",
    "inventory_above_vwap_plus_buffer",
    "inventory_best_ask_above_vwap_plus_buffer",
    "inventory_best_ask_above_limit_passive_lane_sealed",
    "inventory_best_ask_below_ruled_entry_floor",
    "inventory_best_ask_gate",
    "inventory_best_ask_missing",
    "inventory_confirmed_unchanged_no_edge",
    "inventory_late_window_guard",
    "inventory_residual_gap_below_min_order",
    "inventory_target_already_met",
    "inventory_window_state_stale",
    "late_window_guard",
    "market_closed_now",
    "market_buy_precision_infeasible",
    "price_band_skip",
    "price_outside_policy",
    "profit_latency_suppression",
    "signal_age_gte_60s",
    "submitted",
    "toxicity_protection",
    "window_fill_cap",
    "window_time_gte_180s",
}
APPROVED_GATED_QUIET_PREFIXES = ("prefilter:", "window:", "toxicity:", "policy:", "policy_")
# Never approved-quiet, and never approvable by prefix: these reasons mean the gate
# lacked fresh evidence, not that a ruled gate refused a price. Approving them would
# make the deadman quieter exactly as the book feed degrades.
GATE_EVIDENCE_STALE_EXACT = {"stale_book_at_gate"}
ARMED_PROBE_GATED_QUIET_EXACT = {"probe_cap_blocked", "policy_cap_maker_fallback"}
WINDOW_TIME_NEAR_MISS_MIN_S = 180.0
WINDOW_TIME_NEAR_MISS_MAX_S = 200.0
POLICY_CHOKE_LOOKBACK_S = 30 * 60.0
POLICY_CHOKE_MIN_FRESH_ROWS = 50
SOURCE_ROW_IDENTITY_COVERAGE_FLOOR = 0.99
INTENT_CHANNEL_MAX_AGE_S = POLICY_CHOKE_LOOKBACK_S
REQUIRED_SOURCE_DROUGHT_CHECKS = (
    "f1_measured_positive_regime_cell",
    "f1_venue_reachable_admissible",
    "f1_walk_forward_admissible",
    "f2_fresh_rows_and_own_policy_copyable",
    "f3_not_enabled_or_cooloff_or_fading",
    "f4_external_liveness",
    "own_evidenced_policy_available",
    "active_temporal_not_proven_negative",
    "active_temporal_regime_cell_measured",
    "not_terminal_park_red_clock_or_measured_loser",
    "both_resolved_halves_positive",
    "f1_concentration_admissible",
    "exact_policy_chronological_holdout_pass",
)
POLICY_CHOKE_RUNG_B_TTL_S = 3600
POLICY_CHOKE_RUNG_B_COOLOFF_S = 24 * 3600
POLICY_CHOKE_RUNG_B_PIN_ID = "policy-choke-rung-b"
POLICY_CHOKE_RUNG_B_DIRECTION_ID = "2026-07-20T15:32Z-fable-rung-b-closure"
DIRECT_ADMISSION_EVENT_SOURCES = frozenset(
    {"rtds_activity", "polygon_orderfilled_ws_premerge", "polygon_ws"}
)
POST_BOOT_ACCEPTANCE_PATH_UNCERTIFIED_S = 2 * 3600
ROSTER_COLLAPSE_ALERT_S = 1800.0


def _operator_live_authority(
    guard: dict[str, Any], *, process_command: str | None = None
) -> dict[str, Any]:
    """Read standing authority separately from a candidate-cycle permission."""

    published = guard.get("operator_live_authority")
    if isinstance(published, dict):
        return {**published, "active": published.get("active") is True}
    if published is True:
        return {"active": True, "source": "guard_state_boolean", "cycle_invariant": True}
    if guard.get("live_orders_allowed") is True:
        return {"active": True, "source": "legacy_guard_cycle_true", "cycle_invariant": False}
    command = process_command
    pid = (guard.get("guard_code_identity") or {}).get("pid")
    if command is None and pid is not None:
        try:
            completed = subprocess.run(
                ["ps", "-p", str(int(pid)), "-o", "command="],
                check=False,
                capture_output=True,
                text=True,
                timeout=2.0,
            )
            command = completed.stdout.strip() if completed.returncode == 0 else ""
        except (OSError, subprocess.SubprocessError, TypeError, ValueError):
            command = ""
    try:
        argv = shlex.split(command or "")
    except ValueError:
        argv = []
    approval_id = ""
    if "--operator-approval-id" in argv:
        index = argv.index("--operator-approval-id")
        if index + 1 < len(argv):
            approval_id = argv[index + 1]
    active = bool(
        "--execute-live" in argv
        and "--live-orders-allowed" in argv
        and "--explicit-live-operator-go" in argv
        and approval_id
    )
    return {
        "active": active,
        "operator_approval_id": approval_id or None,
        "source": "resident_guard_process_launch_identity",
        "cycle_invariant": True,
        "guard_pid": pid,
    }


def _money_and_tripwires_clear(
    *,
    operator_live_authority: dict[str, Any],
    weekend_rotation: dict[str, Any],
    total_loss_auto_disable: dict[str, Any],
) -> bool:
    disabled_members = total_loss_auto_disable.get("disabled_members") or []
    # A populated disabled-members ledger proves the loss protection acted; it
    # must not permanently freeze observation/admission for every unrelated
    # candidate.  Fail closed only when losses are reported without evidence
    # that the mechanical auto-disable is enabled.  Candidate-specific loss
    # and park exclusions remain enforced by the normal-gate row checks.
    total_loss_enforced = not disabled_members or total_loss_auto_disable.get("enabled") is True
    return bool(
        operator_live_authority.get("active") is True
        and str(weekend_rotation.get("status") or "CLEAR").upper()
        not in {"STOP", "HALT", "TRIGGERED", "ROTATED", "DEMOTED"}
        and total_loss_enforced
    )


def _direct_total_loss_fenced_wallets(
    *,
    overlay: dict[str, Any],
    total_loss_auto_disable: dict[str, Any],
) -> set[str]:
    """Return wallets that may never re-enter through the DIRECT frontier."""

    fenced: set[str] = set()
    for row in total_loss_auto_disable.get("disabled_members") or []:
        wallet = _normalize_wallet(
            (row.get("source_wallet") or row.get("wallet"))
            if isinstance(row, dict)
            else row
        )
        if wallet:
            fenced.add(wallet)
    pin_rows = list(overlay.get("previous_selection_pins") or [])
    current_pin = overlay.get("selection_pin")
    if isinstance(current_pin, dict) and current_pin.get("enabled") is False:
        pin_rows.append(current_pin)
    for pin in pin_rows:
        if not isinstance(pin, dict) or pin.get("enabled") is not False:
            continue
        reason = " ".join(
            str(pin.get(key) or "")
            for key in ("release_reason", "disabled_reason", "reason")
        ).upper()
        if "TOTAL_LOSS" not in reason:
            continue
        wallet = _normalize_wallet(pin.get("source_wallet") or pin.get("wallet"))
        if wallet:
            fenced.add(wallet)
    return fenced


def _normal_gate_f2_gap_row(
    *,
    rows: list[dict[str, Any]],
    observation: dict[str, Any],
    frontier_key: str,
    money_and_tripwires_clear: bool,
) -> dict[str, Any] | None:
    if not money_and_tripwires_clear:
        return None
    wallet = _normalize_wallet(observation.get("wallet"))
    fingerprint = str(observation.get("wide_policy_fingerprint") or "")
    if (
        not wallet
        or not fingerprint
        or str(observation.get("frontier_key") or "") != str(frontier_key or "")
    ):
        return None
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and _normalize_wallet(row.get("wallet")) == wallet
        and str(row.get("wide_policy_fingerprint") or "") == fingerprint
    ]
    for row in matches:
        deficits = [str(item) for item in row.get("evidence_deficits") or []]
        if deficits != ["f2_fresh_rows_and_own_policy_copyable"]:
            continue
        checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
        non_f2_failed = [
            key
            for key, value in checks.items()
            if key != "f2_fresh_rows_and_own_policy_copyable" and value is not True
        ]
        if non_f2_failed:
            continue
        if int(row.get("fresh_own_source_buy_rows_30m") or 0) < 10:
            continue
        return row
    return None


def _roster_collapse_limb(
    *, guard: dict[str, Any], previous: dict[str, Any], now: datetime,
    threshold_s: float = ROSTER_COLLAPSE_ALERT_S,
) -> dict[str, Any]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    member_count = int(runtime.get("member_count") if runtime.get("member_count") is not None else len(members))
    diagnostics = guard.get("candidate_pass_gate_diagnostics") if isinstance(guard.get("candidate_pass_gate_diagnostics"), dict) else {}
    fallthrough = diagnostics.get("fallthrough") if isinstance(diagnostics.get("fallthrough"), dict) else {}
    no_member_passed = bool(
        fallthrough.get("reason") == "no_active_set_member_passed"
        and diagnostics.get("selected_passed") is False
    )
    pair_blocked = str(guard.get("status") or "") == "LIVE_GUARD_BLOCKED" and (member_count == 0 or no_member_passed)
    prior = previous.get("roster_collapse_limb") if isinstance(previous.get("roster_collapse_limb"), dict) else {}
    started = _parse_ts(prior.get("started_at")) if pair_blocked else None
    if pair_blocked and started is None:
        started = now
    held_s = max(0.0, (now - started).total_seconds()) if started is not None else 0.0
    return {
        "firing": bool(pair_blocked and held_s >= float(threshold_s)),
        "pair_blocked": pair_blocked,
        "guard_status": guard.get("status"),
        "member_count": member_count,
        "started_at": started.isoformat() if started is not None else None,
        "held_s": round(held_s, 6),
        "threshold_s": float(threshold_s),
        "no_member_passed": no_member_passed,
        "incident_class": (
            "INCIDENT_ROSTER_COLLAPSE" if member_count == 0 else "INCIDENT_ROSTER_INADMISSIBLE"
        ),
        "restart_authorized": False,
        "mechanical_escalation": "NONE",
    }


def _zero_supply_seat_limb(
    *, guard: dict[str, Any], policy_choke: dict[str, Any], idle_s: float | None,
    threshold_s: float, post_selection_floor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_fresh = int(policy_choke.get("selected_fresh_source_rows") or 0)
    whole_runtime_fresh = int(
        policy_choke.get("whole_runtime_fresh_source_rows") or 0
    )
    floor = post_selection_floor or {}
    adopted_supply_clock_expired = bool(
        floor.get("matching_adopted_runtime")
        and floor.get("post_selection_idle_s") is not None
        and float(floor["post_selection_idle_s"]) >= float(threshold_s)
        and whole_runtime_fresh == 0
    )
    firing = bool(
        (
            str(guard.get("status") or "") == "LIVE_GUARD_BLOCKED"
            or adopted_supply_clock_expired
        )
        and idle_s is not None
        and float(idle_s) >= float(threshold_s)
        and selected_fresh == 0
    )
    return {
        "firing": firing,
        "incident_class": "INCIDENT_ZERO_SUPPLY_SEAT",
        "accepted_order_idle_s": idle_s,
        "threshold_s": float(threshold_s),
        "selected_fresh_source_rows": selected_fresh,
        "whole_runtime_fresh_source_rows": whole_runtime_fresh,
        "adopted_supply_clock_expired": adopted_supply_clock_expired,
        "selected_seat_epoch_at": floor.get("selected_seat_epoch_at"),
        "post_selection_idle_s": floor.get("post_selection_idle_s"),
        "rung_a_seat_read_authorized": firing,
        "member_enable_authorized": False,
        "restart_authorized": False,
        "mechanical_escalation": "NONE",
    }


def _enforce_policy_choke_zero_supply_status(
    policy_choke: dict[str, Any], *, idle_s: float | None, threshold_s: float
) -> bool:
    """A measured zero-supply runtime past budget may never report CLEAR."""

    measured_whole_runtime_fresh = policy_choke.get(
        "whole_runtime_fresh_source_rows"
    )
    past_budget = bool(
        measured_whole_runtime_fresh is not None
        and int(measured_whole_runtime_fresh) == 0
        and idle_s is not None
        and float(idle_s) >= float(threshold_s)
    )
    if past_budget and str(policy_choke.get("status") or "") == "CLEAR":
        policy_choke["status"] = "ZERO_RUNTIME_SUPPLY_PAST_IDLE_BUDGET"
        policy_choke["clear_refusal"] = {
            "reason": "whole_runtime_fresh_source_rows_zero_past_idle_budget",
            "whole_runtime_fresh_source_rows": 0,
            "accepted_order_idle_s": idle_s,
            "threshold_s": float(threshold_s),
        }
    return past_budget


def _post_selection_floor_can_downgrade(
    *, post_selection_floor: dict[str, Any], zero_supply_seat: dict[str, Any],
    roster_collapse: dict[str, Any]
) -> bool:
    return bool(
        post_selection_floor.get("active")
        and not zero_supply_seat.get("firing")
        and not roster_collapse.get("firing")
    )


def _is_window_time_taxonomy(reason: str) -> bool:
    return bool(re.fullmatch(r"window_time_gte_\d+s", str(reason or "")))


def _approved_suppression_hits(tags: set[str], approved: set[str]) -> set[str]:
    return {tag for tag in tags if tag in approved or _is_window_time_taxonomy(tag)}


def _notify(
    message: str,
    *,
    incident_class: str,
    root_path: Path = ROOT,
    self_healed: bool = False,
) -> dict[str, Any]:
    """macOS notification — suppressed when POLYMARKET_DEADMAN_NOTIFY=0
    (tests and manual/tmp runs must never pop real notifications)."""
    return record_operator_event(
        incident_class=incident_class,
        message=message,
        title="polymarket-agent DEADMAN",
        actionable=False,
        self_healed=self_healed,
        state_path=root_path / "data/research/operator_notification_discipline_state.json",
        events_path=root_path / "data/research/operator_notification_discipline_events.jsonl",
    )


def _incident_notify_allowed(*, status: str, can_trade: bool) -> bool:
    """Keep empty-seat policy diagnostics out of the dead-order alert channel."""
    return status != "INCIDENT_POLICY_CHOKE" or can_trade


def _failed_gates(gated_quiet: dict[str, Any]) -> list[str]:
    checks = gated_quiet.get("checks")
    if not isinstance(checks, dict):
        return []
    return [f"{key}:{value}" for key, value in sorted(checks.items()) if value is not True]


def _append_incident_evidence(root: Path, result: dict[str, Any]) -> None:
    path = root / INCIDENT_EVIDENCE_JSONL
    append_incident_row(path, result)


def _episode_fire_fields(prev: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    """Carry the first fire across repeated incident checks until the clear."""
    restart_event = _episode_restart_event(result)
    prior_restart_events = (
        prev.get("episode_restart_events")
        if isinstance(prev.get("episode_restart_events"), list)
        else []
    )
    restart_events = [row for row in prior_restart_events if isinstance(row, dict)]
    if restart_event is not None and restart_event not in restart_events:
        restart_events.append(restart_event)
    prev_is_incident = str(prev.get("status") or "").startswith("INCIDENT_")
    if prev_is_incident:
        policy_choke = prev.get("policy_choke")
        if not isinstance(policy_choke, dict):
            policy_choke = {}
        carried = {
            "episode_fire_at": prev.get("episode_fire_at") or prev.get("checked_at"),
            "episode_fire_idle_s": (
                prev.get("episode_fire_idle_s")
                if prev.get("episode_fire_idle_s") is not None
                else prev.get("idle_s")
            ),
            "episode_fire_status": prev.get("episode_fire_status") or prev.get("status"),
            "episode_fire_class": (
                prev.get("episode_fire_class")
                or prev.get("deadman_class")
                or prev.get("status")
            ),
            "episode_fire_mechanical_escalation": (
                prev.get("episode_fire_mechanical_escalation")
                or prev.get("mechanical_escalation")
            ),
            "episode_fire_wallet_policy_diagnostic": (
                prev.get("episode_fire_wallet_policy_diagnostic")
                or policy_choke.get("wallet_policy_diagnostic")
            ),
            "episode_fire_guard_generation_started_at": (
                prev.get("episode_fire_guard_generation_started_at")
                or prev.get("guard_loaded_generation_started_at")
            ),
            "episode_fire_liveness_ts": (
                prev.get("episode_fire_liveness_ts") or prev.get("liveness_ts")
            ),
        }
        previous_restart_performed = prev.get("episode_restart_performed")
        carried["episode_restart_performed"] = (
            True
            if restart_event is not None or previous_restart_performed is True
            else False
            if previous_restart_performed is False
            else None
        )
        carried["episode_restart_events"] = restart_events
        return carried
    policy_choke = result.get("policy_choke")
    if not isinstance(policy_choke, dict):
        policy_choke = {}
    return {
        "episode_fire_at": result.get("checked_at"),
        "episode_fire_idle_s": result.get("idle_s"),
        "episode_fire_status": result.get("status"),
        "episode_fire_class": result.get("deadman_class") or result.get("status"),
        "episode_fire_mechanical_escalation": result.get("mechanical_escalation"),
        "episode_fire_wallet_policy_diagnostic": policy_choke.get("wallet_policy_diagnostic"),
        "episode_fire_guard_generation_started_at": result.get(
            "guard_loaded_generation_started_at"
        ),
        "episode_fire_liveness_ts": result.get("liveness_ts"),
        "episode_restart_performed": restart_event is not None,
        "episode_restart_events": restart_events,
    }


def _episode_restart_event(result: dict[str, Any]) -> dict[str, Any] | None:
    guard_memory = result.get("guard_memory") if isinstance(result.get("guard_memory"), dict) else {}
    auto_restart = (
        guard_memory.get("auto_restart")
        if isinstance(guard_memory.get("auto_restart"), dict)
        else {}
    )
    if auto_restart.get("status") != "RESTART_EXECUTED":
        return None
    return {
        "at": guard_memory.get("checked_at") or result.get("checked_at"),
        "source": "guard_memory_auto_restart",
        "status": auto_restart.get("status"),
        "guard_memory_auto_restart_status": auto_restart.get("status"),
        "rss_gib": guard_memory.get("rss_gib"),
    }


def _event_in_episode(
    event_at: Any,
    fire_at: Any,
    cleared_at: Any,
) -> bool:
    event_ts = _parse_ts(event_at)
    fire_ts = _parse_ts(fire_at)
    clear_ts = _parse_ts(cleared_at)
    return bool(
        event_ts is not None
        and fire_ts is not None
        and clear_ts is not None
        and fire_ts <= event_ts <= clear_ts
    )


def _external_episode_restart_events(
    root: Path,
    prev: dict[str, Any],
    result: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Prove restarts from the independent actuator ledger and loaded generation."""
    fire_at = prev.get("episode_fire_at") or prev.get("checked_at")
    cleared_at = result.get("checked_at")
    events: list[dict[str, Any]] = []
    sources_checked: list[str] = []
    actuator_path = root / BRAINLESS_RESTART_EVENTS_JSONL
    actuator_rows = load_incident_rows(actuator_path)
    if actuator_path.is_file():
        sources_checked.append("brainless_actuator")
    for row in actuator_rows:
        if row.get("status") != "RESTART_EXECUTED":
            continue
        event_at = row.get("generated_at") or row.get("checked_at") or row.get("at")
        if not _event_in_episode(event_at, fire_at, cleared_at):
            continue
        execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
        events.append(
            {
                "at": event_at,
                "source": "brainless_actuator",
                "status": "RESTART_EXECUTED",
                "reason": row.get("reason"),
                "started_pid": execution.get("actual_pid"),
            }
        )

    fire_generation_at = prev.get("episode_fire_guard_generation_started_at")
    clear_generation_at = result.get("guard_loaded_generation_started_at")
    if fire_generation_at and clear_generation_at:
        sources_checked.append("guard_loaded_generation")
    if (
        clear_generation_at
        and clear_generation_at != fire_generation_at
        and _event_in_episode(clear_generation_at, fire_at, cleared_at)
    ):
        events.append(
            {
                "at": clear_generation_at,
                "source": "guard_loaded_generation",
                "status": "RESTART_CORROBORATED",
                "previous_started_at": fire_generation_at,
            }
        )
    return events, sources_checked


def _episode_closeout_row(prev: dict[str, Any], result: dict[str, Any]) -> dict[str, Any] | None:
    prev_status = str(prev.get("status") or "")
    current_status = str(result.get("status") or "")
    if not prev_status.startswith("INCIDENT_") or current_status.startswith("INCIDENT_"):
        return None
    if int(prev.get("consecutive_incidents") or 0) <= 0:
        return None

    fire_at = str(prev.get("episode_fire_at") or prev.get("checked_at") or "")
    cleared_at = str(result.get("checked_at") or "")
    fire_ts = _parse_ts(fire_at)
    clear_ts = _parse_ts(cleared_at)
    duration_s = (
        round(max(0.0, (clear_ts - fire_ts).total_seconds()), 6)
        if fire_ts is not None and clear_ts is not None
        else None
    )
    policy_choke = prev.get("policy_choke") if isinstance(prev.get("policy_choke"), dict) else {}
    restart_event = _episode_restart_event(result)
    prior_restart_events = (
        prev.get("episode_restart_events")
        if isinstance(prev.get("episode_restart_events"), list)
        else []
    )
    restart_events = [
        *[row for row in prior_restart_events if isinstance(row, dict)],
        *([restart_event] if restart_event is not None else []),
    ]
    if restart_event is not None:
        restart_performed = True
        restart_provenance = (
            "CARRIED_EPISODE_FIELD"
            if prev.get("episode_restart_performed") is True
            else "CLEAR_CHECK_GUARD_MEMORY"
        )
    elif prev.get("episode_restart_performed") is True:
        restart_performed = True
        restart_provenance = "CARRIED_EPISODE_FIELD"
    elif prev.get("episode_restart_performed") is False:
        restart_performed = False
        restart_provenance = "CARRIED_EPISODE_FIELD"
    else:
        restart_performed = None
        restart_provenance = "UNKNOWN_PRE_FIELD_EPISODE"
    episode_id = hashlib.sha256(
        f"{fire_at}|{prev.get('episode_fire_status') or prev_status}".encode("utf-8")
    ).hexdigest()[:24]
    fire_liveness_ts = prev.get("episode_fire_liveness_ts") or prev.get("liveness_ts")
    clear_liveness_ts = result.get("liveness_ts")
    parsed_fire_liveness = _parse_ts(fire_liveness_ts)
    parsed_clear_liveness = _parse_ts(clear_liveness_ts)
    stale_clear = bool(
        parsed_fire_liveness is not None
        and parsed_clear_liveness is not None
        and parsed_fire_liveness == parsed_clear_liveness
    )
    return {
        "schema_version": 1,
        "kind": "order_flow_deadman_episode_closeout",
        "episode_id": episode_id,
        "episode_fire_at": fire_at or None,
        "episode_fire_idle_s": (
            prev.get("episode_fire_idle_s")
            if prev.get("episode_fire_idle_s") is not None
            else prev.get("idle_s")
        ),
        "cleared_at": cleared_at or None,
        "fire_liveness_ts": fire_liveness_ts,
        "clear_liveness_ts": clear_liveness_ts,
        "clear_liveness_source": result.get("liveness_source"),
        "clear_class": "RECLASSIFIED_NOT_RECOVERED" if stale_clear else "RECOVERED",
        "recovered": not stale_clear,
        "idle_s_at_clear": result.get("idle_s"),
        "episode_duration_s": duration_s,
        "deadman_class": prev.get("episode_fire_class") or prev.get("deadman_class") or prev_status,
        "restart_performed": restart_performed,
        "restart_provenance": restart_provenance,
        "restart_events": restart_events,
        "wallet_policy_diagnostic": (
            prev.get("episode_fire_wallet_policy_diagnostic")
            or policy_choke.get("wallet_policy_diagnostic")
        ),
        "fire_status": prev.get("episode_fire_status") or prev_status,
        "clear_status": current_status,
        "fire_mechanical_escalation": (
            prev.get("episode_fire_mechanical_escalation")
            or prev.get("mechanical_escalation")
        ),
    }


def _append_episode_closeout(root: Path, prev: dict[str, Any], result: dict[str, Any]) -> dict[str, Any] | None:
    row = _episode_closeout_row(prev, result)
    if row is None:
        return None
    external_events, sources_checked = _external_episode_restart_events(root, prev, result)
    row["restart_sources_checked"] = sources_checked
    if external_events:
        restart_events = row.get("restart_events")
        restart_events = restart_events if isinstance(restart_events, list) else []
        for event in external_events:
            if event not in restart_events:
                restart_events.append(event)
        row["restart_events"] = restart_events
        row["restart_performed"] = True
        sources = {str(event.get("source") or "") for event in external_events}
        row["restart_provenance"] = (
            "BRAINLESS_ACTUATOR_EVENT_LEDGER+GUARD_LOADED_GENERATION"
            if {"brainless_actuator", "guard_loaded_generation"} <= sources
            else "BRAINLESS_ACTUATOR_EVENT_LEDGER"
            if "brainless_actuator" in sources
            else "GUARD_LOADED_GENERATION"
        )
    elif (
        row.get("restart_performed") is False
        and set(sources_checked)
        == {"brainless_actuator", "guard_loaded_generation"}
    ):
        row["restart_provenance"] = "DUAL_SOURCE_ABSENCE_PROVEN"
    episode_path = root / EPISODE_EVIDENCE_JSONL
    if any(existing.get("episode_id") == row["episode_id"] for existing in load_incident_rows(episode_path)):
        return None
    _ensure_episode_manifest_scope(root, row)
    append_incident_row(
        episode_path,
        row,
        manifest_path=root / EPISODE_EVIDENCE_MANIFEST,
    )
    return row


def _ensure_episode_manifest_scope(root: Path, first_row: dict[str, Any]) -> None:
    manifest_path = root / EPISODE_EVIDENCE_MANIFEST
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            manifest = {}
    except (OSError, json.JSONDecodeError):
        manifest = {}
    if isinstance(manifest.get("ledger_scope"), dict):
        return
    manifest.setdefault("schema_version", 1)
    manifest.setdefault("kind", "order_flow_incident_archive_manifest")
    manifest.setdefault("archives", [])
    manifest["ledger_scope"] = {
        "first_armed_commit": EPISODE_EVIDENCE_FIRST_ARMED_COMMIT,
        "first_armed_at": first_row.get("cleared_at"),
        "prior_episodes_unrecorded": True,
        "prior_incident_rows_at_arming": EPISODE_EVIDENCE_PRIOR_INCIDENT_ROWS,
        "prior_recorded_clears": EPISODE_EVIDENCE_PRIOR_RECORDED_CLEARS,
        "rule": (
            "episodes closing before first_armed_at are permanently unrecorded; "
            "an empty or short ledger is NOT evidence of zero episodes"
        ),
    }
    atomic_write_json(manifest_path, manifest)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _host_boot_time() -> datetime | None:
    env_value = os.getenv("WALLET_COPY_HOST_BOOT_TIME_UTC", "").strip()
    if env_value:
        return _parse_ts(env_value)
    if os.getenv("POLYMARKET_DEADMAN_NOTIFY") == "0":
        return None
    try:
        completed = subprocess.run(
            ["sysctl", "-n", "kern.boottime"],
            timeout=2,
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    text = str(completed.stdout or completed.stderr or "")
    match = re.search(r"sec\s*=\s*(\d+)", text)
    if not match:
        return None
    try:
        return datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _parse_ts(value):
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _latest_accepted_order(ledger: dict) -> dict[str, Any]:
    """Newest accepted live order plus its actuator identity."""

    newest: datetime | None = None
    newest_row: dict[str, Any] = {}
    accepted_statuses = {
        "FILLED",
        "LIVE_FILLED",
        "LIVE_MAKER_FILLED",
        "LIVE_SUBMITTED",
        "MATCHED",
        "SUBMITTED",
    }
    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    for row in orders[-500:]:
        if not isinstance(row, dict):
            continue
        statuses = {
            str(row.get("status") or "").upper(),
            str(row.get("final_status") or "").upper(),
        }
        if not (statuses & accepted_statuses):
            continue
        row_newest = None
        for key in ("updated_at", "submitted_at", "accepted_at", "timestamp", "ts", "created_at"):
            parsed = _parse_ts(row.get(key))
            if parsed and (row_newest is None or parsed > row_newest):
                row_newest = parsed
        if row_newest and (newest is None or row_newest > newest):
            newest = row_newest
            newest_row = row
    if newest is None:
        newest = _parse_ts((ledger.get("summary") or {}).get("latest_order_ts"))
    attribution = (
        newest_row.get("trade_decision")
        if isinstance(newest_row.get("trade_decision"), dict)
        else {}
    )
    return {
        "timestamp": newest,
        "source_wallet": _normalize_wallet(
            newest_row.get("source_wallet")
            or attribution.get("source_wallet")
            or attribution.get("wallet")
        )
        or None,
        "candidate_id": newest_row.get("candidate_id") or attribution.get("candidate_id"),
        "policy_id": newest_row.get("policy_id") or attribution.get("policy_id"),
    }


def _latest_order_ts(ledger: dict):
    """Newest accepted live order timestamp.

    Rejected rows and nested diagnostics are intentionally excluded. The
    deadman must stay red until an accepted submit/fill advances the ledger.
    """
    return _latest_accepted_order(ledger)["timestamp"]


def _tail_jsonl(path: Path, *, max_lines: int = 5000, tail_bytes: int = 8 * 1024 * 1024) -> list[dict]:
    rows: deque[dict] = deque(maxlen=max_lines)
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            start = max(0, size - max(1, int(tail_bytes)))
            handle.seek(start)
            chunk = handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return []
    lines = chunk.splitlines()
    if start > 0 and lines:
        lines = lines[1:]
    for line in lines[-max_lines:]:
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return list(rows)


def _as_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], pct: float):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * pct
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    weight = rank - lo
    return round(ordered[lo] * (1 - weight) + ordered[hi] * weight, 6)


def _profit_latency_summary(row: dict) -> dict:
    live_execution = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
    summary = live_execution.get("profit_latency_suppression")
    if isinstance(summary, dict):
        return summary
    candidate_summary = live_execution.get("candidate_intent_summary")
    if isinstance(candidate_summary, dict) and isinstance(candidate_summary.get("profit_latency_suppression"), dict):
        return candidate_summary["profit_latency_suppression"]
    if isinstance(row.get("profit_latency_suppression"), dict):
        return row["profit_latency_suppression"]
    return {}


def _row_ts(row: dict):
    for key in ("generated_at", "checked_at", "ts", "updated_at"):
        dt = _parse_ts(row.get(key))
        if dt:
            return dt
    return None


def _tags_from_summary(summary: dict) -> set[str]:
    tags: set[str] = set()
    counts = summary.get("taxonomy_counts") if isinstance(summary.get("taxonomy_counts"), dict) else {}
    for key, count in counts.items():
        try:
            if int(count or 0) > 0:
                tags.add(str(key))
        except (TypeError, ValueError):
            continue
    for sample in summary.get("sample_filtered_intents") or []:
        if not isinstance(sample, dict):
            continue
        for tag in sample.get("taxonomy_tags") or []:
            tags.add(str(tag))
        if sample.get("taxonomy"):
            tags.add(str(sample.get("taxonomy")))
    return tags


def _tags_from_event(row: dict) -> set[str]:
    tags: set[str] = set()
    for tag in row.get("taxonomy_tags") or []:
        tags.add(str(tag))
    for key in ("taxonomy", "reject_reason"):
        value = row.get(key)
        if not value:
            continue
        tags.update(part for part in str(value).split("+") if part)
    return tags


def _wallet_stats_payload(
    member_signal_ages: dict[str, list[float]],
    counts: dict[str, dict[str, int]],
    taxonomy: dict[str, dict[str, int]] | None = None,
) -> dict:
    out: dict[str, dict] = {}
    for wallet in sorted(set(member_signal_ages) | set(counts)):
        values = member_signal_ages.get(wallet, [])
        wallet_counts = counts.get(wallet, {})
        out[wallet] = {
            "signal_age_count": len(values),
            "signal_age_p50_s": _percentile(values, 0.50),
            "signal_age_p90_s": _percentile(values, 0.90),
            "eligible_intents": int(wallet_counts.get("eligible", 0)),
            "suppressed_intents": int(wallet_counts.get("suppressed", 0)),
            "suppression_taxonomy": dict(sorted((taxonomy or {}).get(wallet, {}).items())),
        }
    return out


def _scan_liveness_events(
    path: Path,
    *,
    approved_tags: set[str] | None = None,
    since: datetime | None = None,
) -> dict:
    approved = approved_tags or APPROVED_SUPPRESSION_TAGS
    latest_suppression = None
    latest_profit_pass = None
    suppression_tags: set[str] = set()
    suppressed_count = 0
    eligible_count = 0
    member_signal_ages: dict[str, list[float]] = defaultdict(list)
    member_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"eligible": 0, "suppressed": 0})
    member_taxonomy: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    fresh_stale_signal_rows = 0
    source_row_attribution: dict[str, str] = {}

    for row in _tail_jsonl(path):
        row_time = _row_ts(row)
        if since is not None and (row_time is None or row_time < since):
            continue
        direct_tags = _tags_from_event(row)
        direct_approved = (
            row.get("approved_suppression") is True
            or str(row.get("event_type") or "") == "FABLE_APPROVED_SUPPRESSION_REJECT"
            or str(row.get("event") or "") == "wallet_copy_live_profit_latency_suppression_reject"
        )
        direct_hits = _approved_suppression_hits(direct_tags, approved)
        source_row_id = str(row.get("source_event_id") or row.get("event_id") or "")
        if source_row_id and direct_tags:
            source_row_attribution[source_row_id] = sorted(direct_tags)[0]
        if direct_approved and direct_hits:
            suppression_tags.update(direct_hits)
            if row_time and (latest_suppression is None or row_time > latest_suppression):
                latest_suppression = row_time
            suppressed_count += 1
            fresh_stale_signal_rows += 1
            wallet = str(row.get("source_wallet") or "").lower()
            if wallet:
                member_counts[wallet]["suppressed"] += 1
                for tag in sorted(direct_tags):
                    member_taxonomy[wallet][tag] += 1
                signal_age = _as_float(row.get("signal_age_s"))
                if signal_age is not None:
                    member_signal_ages[wallet].append(signal_age)
            continue

        summary = _profit_latency_summary(row)
        if not summary:
            continue
        tags = _tags_from_summary(summary)
        approved_hits = _approved_suppression_hits(tags, approved)
        if approved_hits:
            suppression_tags.update(approved_hits)
            if row_time and (latest_suppression is None or row_time > latest_suppression):
                latest_suppression = row_time
            try:
                suppressed_count += int(summary.get("blocked_intents") or summary.get("filtered_intents") or 0)
            except (TypeError, ValueError):
                pass
            fresh_stale_signal_rows += 1
        try:
            output_intents = int(summary.get("output_intents") or 0)
        except (TypeError, ValueError):
            output_intents = 0
        if output_intents > 0:
            eligible_count += output_intents
            if row_time and (latest_profit_pass is None or row_time > latest_profit_pass):
                latest_profit_pass = row_time
        for key, bucket in (("sample_filtered_intents", "suppressed"), ("sample_passed_intents", "eligible")):
            for sample in summary.get(key) or []:
                if not isinstance(sample, dict):
                    continue
                wallet = str(sample.get("source_wallet") or "").lower()
                if not wallet:
                    continue
                member_counts[wallet][bucket] += 1
                if bucket == "suppressed":
                    sample_tags = _tags_from_event(sample)
                    sample_source_row_id = str(
                        sample.get("source_event_id") or sample.get("event_id") or ""
                    )
                    if sample_source_row_id and (sample_tags or tags):
                        source_row_attribution[sample_source_row_id] = sorted(sample_tags or tags)[0]
                    for tag in sorted(sample_tags or tags):
                        member_taxonomy[wallet][tag] += 1
                signal_age = _as_float(sample.get("signal_age_s"))
                if signal_age is not None:
                    member_signal_ages[wallet].append(signal_age)

    return {
        "latest_approved_suppression_ts": latest_suppression,
        "latest_profit_filter_pass_ts": latest_profit_pass,
        "approved_suppression_tags": sorted(suppression_tags),
        "approved_suppression_events": suppressed_count,
        "eligible_profit_filter_pass_intents": eligible_count,
        "fresh_stale_signal_rows": fresh_stale_signal_rows,
        "member_signal_age": _wallet_stats_payload(member_signal_ages, member_counts, member_taxonomy),
        "source_row_attribution": dict(sorted(source_row_attribution.items())),
        "lookback_since": since,
    }


def _latest_jsonl_ts(path: Path) -> datetime | None:
    latest = None
    for row in _tail_jsonl(path, max_lines=2000, tail_bytes=4 * 1024 * 1024):
        row_time = _row_ts(row)
        if row_time and (latest is None or row_time > latest):
            latest = row_time
    return latest


def _fetch_api_error_details(api_errors: object) -> list[dict[str, str]]:
    errors = api_errors if isinstance(api_errors, list) else [api_errors]
    details: list[dict[str, str]] = []
    for error in errors:
        if isinstance(error, dict):
            route_report = error.get("route_report") if isinstance(error.get("route_report"), dict) else {}
            attempts = route_report.get("attempts") if isinstance(route_report.get("attempts"), list) else []
            attempt = next((item for item in attempts if isinstance(item, dict)), {})
            exception = (
                error.get("type")
                or error.get("exception")
                or attempt.get("exception")
                or route_report.get("status")
                or "UnknownFetchApiError"
            )
            message = error.get("error") or error.get("message") or attempt.get("error") or ""
            source = error.get("source") or error.get("endpoint") or ""
        else:
            exception = type(error).__name__
            message = str(error)
            source = ""
        details.append(
            {
                "exception": str(exception),
                "source": str(source),
                "message": str(message)[:240],
            }
        )
    return details


def _pipe_verified_source_quiet(guard: dict, guard_event_log: Path, now: datetime) -> dict:
    """Return affirmative liveness evidence when the pipe is alive but sources are quiet."""
    latest_guard_event = _latest_jsonl_ts(guard_event_log)
    guard_state_ts = _parse_ts(guard.get("generated_at"))
    event_candidates = [dt for dt in (latest_guard_event, guard_state_ts) if dt is not None]
    latest_pipe_event = max(event_candidates) if event_candidates else None
    guard_event_age_s = (now - latest_guard_event).total_seconds() if latest_guard_event else None
    guard_state_age_s = (now - guard_state_ts).total_seconds() if guard_state_ts else None
    pipe_event_age_s = (now - latest_pipe_event).total_seconds() if latest_pipe_event else None
    poller = guard.get("active_set_dataapi_poller")
    if not isinstance(poller, dict):
        poller = {}
    poller_ts = _parse_ts(poller.get("generated_at"))
    poller_age_s = (now - poller_ts).total_seconds() if poller_ts else None
    fetch_meta = poller.get("fetch_meta") if isinstance(poller.get("fetch_meta"), dict) else {}
    fetch_meta_present = bool(fetch_meta)
    fetch_api_error_wallets: list[str] = []
    fetch_api_error_details: dict[str, list[dict[str, str]]] = {}
    demand_split = _active_set_fresh_demand_split(guard)
    fresh_buy_rows_le_10s_total = 0
    for wallet, meta in fetch_meta.items():
        wallet_key = str(wallet)
        if not isinstance(meta, dict):
            fetch_api_error_wallets.append(wallet_key)
            fetch_api_error_details[wallet_key] = [
                {
                    "exception": type(meta).__name__,
                    "source": "fetch_meta",
                    "message": "fetch_meta entry was not a dict",
                }
            ]
            continue
        api_errors = meta.get("api_errors")
        if api_errors:
            fetch_api_error_wallets.append(wallet_key)
            fetch_api_error_details[wallet_key] = _fetch_api_error_details(api_errors)
        by_source = (
            meta.get("fresh_buy_rows_le_10s_by_source")
            if isinstance(meta.get("fresh_buy_rows_le_10s_by_source"), dict)
            else {}
            )
        for value in by_source.values():
            try:
                fresh_buy_rows_le_10s_total += int(value or 0)
            except (TypeError, ValueError):
                continue
    drought_funnel = guard.get("drought_funnel") if isinstance(guard.get("drought_funnel"), dict) else {}
    try:
        active_set_fresh_signal_rows_total = int(drought_funnel.get("active_set_fresh_signal_rows") or 0)
    except (TypeError, ValueError):
        active_set_fresh_signal_rows_total = 0
    selected_wallet = str(demand_split.get("selected_wallet") or "")
    active_set_fresh_signal_rows = int(
        demand_split.get("active_set_selected_fresh_signal_rows")
        if selected_wallet
        else active_set_fresh_signal_rows_total
    )
    fresh_buy_rows_le_10s = int(
        demand_split.get("selected_fresh_buy_rows_le_10s")
        if selected_wallet
        else fresh_buy_rows_le_10s_total
    )
    floor_blocked = _active_set_floor_blocked_fresh_rows(
        guard,
        source_wallet=selected_wallet or None,
    )
    active_set_floor_blocked_signal_rows = min(
        active_set_fresh_signal_rows,
        int(floor_blocked.get("count") or 0),
    )
    active_set_actionable_signal_rows = max(
        0,
        active_set_fresh_signal_rows - active_set_floor_blocked_signal_rows,
    )
    pipe_event_fresh = (
        pipe_event_age_s is not None
        and 0 <= pipe_event_age_s <= PIPE_QUIET_GUARD_EVENT_MAX_AGE_S
    )
    poller_fresh = (
        poller_age_s is not None
        and 0 <= poller_age_s <= PIPE_QUIET_POLLER_MAX_AGE_S
    )
    api_clean = fetch_meta_present and not fetch_api_error_wallets
    source_quiet = fresh_buy_rows_le_10s == 0 and active_set_actionable_signal_rows == 0
    verified = pipe_event_fresh and poller_fresh and api_clean and source_quiet
    latest = max([dt for dt in (latest_pipe_event, poller_ts) if dt is not None], default=None)
    return {
        "verified": verified,
        "latest_ts": latest if verified else None,
        "latest_guard_event_ts": latest_guard_event,
        "guard_state_generated_at": guard_state_ts,
        "guard_event_age_s": guard_event_age_s,
        "guard_state_age_s": guard_state_age_s,
        "pipe_event_age_s": pipe_event_age_s,
        "pipe_event_clock_skew_s": (
            abs(min(0.0, pipe_event_age_s)) if pipe_event_age_s is not None else None
        ),
        "pipe_event_future_dated": bool(
            pipe_event_age_s is not None and pipe_event_age_s < 0
        ),
        "guard_event_future_dated": bool(
            guard_event_age_s is not None and guard_event_age_s < 0
        ),
        "guard_state_future_dated": bool(
            guard_state_age_s is not None and guard_state_age_s < 0
        ),
        "max_clock_skew_s": PIPE_QUIET_MAX_CLOCK_SKEW_S,
        "poller_generated_at": poller_ts,
        "poller_age_s": poller_age_s,
        "fetch_meta_wallets": len(fetch_meta),
        "fetch_api_error_wallets": fetch_api_error_wallets,
        "fetch_api_error_details": fetch_api_error_details,
        "fresh_buy_rows_le_10s": fresh_buy_rows_le_10s,
        "active_set_fresh_signal_rows": active_set_fresh_signal_rows,
        "active_set_actionable_signal_rows": active_set_actionable_signal_rows,
        "active_set_floor_blocked_signal_rows": active_set_floor_blocked_signal_rows,
        "active_set_floor_blocked_samples": floor_blocked.get("samples", []),
        "guard_event_fresh": pipe_event_fresh,
        "guard_event_log_fresh": (
            guard_event_age_s is not None
            and 0 <= guard_event_age_s <= PIPE_QUIET_GUARD_EVENT_MAX_AGE_S
        ),
        "guard_state_fresh": (
            guard_state_age_s is not None
            and 0 <= guard_state_age_s <= PIPE_QUIET_GUARD_EVENT_MAX_AGE_S
        ),
        "poller_fresh": poller_fresh,
        "api_clean": api_clean,
        "source_quiet": source_quiet,
        "active_set_fresh_demand_split": demand_split,
    }


def _merge_liveness_scans(scans: list[dict]) -> dict:
    latest_suppression = None
    latest_profit_pass = None
    suppression_tags: set[str] = set()
    suppressed_count = 0
    eligible_count = 0
    fresh_stale_signal_rows = 0
    member_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"eligible": 0, "suppressed": 0})
    member_signal_ages: dict[str, list[float]] = defaultdict(list)
    member_taxonomy: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    source_row_attribution: dict[str, str] = {}
    for scan in scans:
        source_row_attribution.update(
            {
                str(source_row_id): str(reason)
                for source_row_id, reason in (scan.get("source_row_attribution") or {}).items()
                if source_row_id and reason
            }
        )
        suppression = scan.get("latest_approved_suppression_ts")
        profit_pass = scan.get("latest_profit_filter_pass_ts")
        if suppression is not None and (latest_suppression is None or suppression > latest_suppression):
            latest_suppression = suppression
        if profit_pass is not None and (latest_profit_pass is None or profit_pass > latest_profit_pass):
            latest_profit_pass = profit_pass
        suppression_tags.update(str(tag) for tag in scan.get("approved_suppression_tags") or [])
        suppressed_count += int(scan.get("approved_suppression_events") or 0)
        eligible_count += int(scan.get("eligible_profit_filter_pass_intents") or 0)
        fresh_stale_signal_rows += int(scan.get("fresh_stale_signal_rows") or 0)
        for wallet, row in (scan.get("member_signal_age") or {}).items():
            if not isinstance(row, dict):
                continue
            wallet_key = str(wallet).lower()
            member_counts[wallet_key]["eligible"] += int(row.get("eligible_intents") or 0)
            member_counts[wallet_key]["suppressed"] += int(row.get("suppressed_intents") or 0)
            for reason, count_value in (row.get("suppression_taxonomy") or {}).items():
                member_taxonomy[wallet_key][str(reason)] += int(count_value or 0)
            p50 = _as_float(row.get("signal_age_p50_s"))
            count = int(row.get("signal_age_count") or 0)
            if p50 is not None and count > 0:
                member_signal_ages[wallet_key].extend([p50] * count)
    return {
        "latest_approved_suppression_ts": latest_suppression,
        "latest_profit_filter_pass_ts": latest_profit_pass,
        "approved_suppression_tags": sorted(suppression_tags),
        "approved_suppression_events": suppressed_count,
        "eligible_profit_filter_pass_intents": eligible_count,
        "fresh_stale_signal_rows": fresh_stale_signal_rows,
        "member_signal_age": _wallet_stats_payload(member_signal_ages, member_counts, member_taxonomy),
        "source_row_attribution": dict(sorted(source_row_attribution.items())),
    }


def _policy_choke_incident(
    *,
    scan: dict[str, Any],
    selected_wallet: str,
    can_trade: bool,
    rung_a: dict[str, Any],
    lookback_s: float = POLICY_CHOKE_LOOKBACK_S,
    min_fresh_rows: int = POLICY_CHOKE_MIN_FRESH_ROWS,
    submit_outcomes: dict[str, Any] | None = None,
    method_acceptance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    members = scan.get("member_signal_age") if isinstance(scan.get("member_signal_age"), dict) else {}
    selected = members.get(selected_wallet) if isinstance(members.get(selected_wallet), dict) else {}
    selected_suppressed = int(selected.get("suppressed_intents") or 0)
    selected_fresh = int(selected.get("fresh_source_rows") or selected_suppressed)
    selected_eligible = int(selected.get("eligible_intents") or 0)
    selected_accepted = int(selected.get("accepted_orders") or 0)
    selected_attribution_status = str(selected.get("attribution_status") or "")
    selected_source_row_identity_coverage = float(
        selected.get("source_row_identity_coverage") or 0.0
    )
    whole_suppressed = sum(int(row.get("suppressed_intents") or 0) for row in members.values() if isinstance(row, dict))
    whole_fresh = sum(int(row.get("fresh_source_rows") or row.get("suppressed_intents") or 0) for row in members.values() if isinstance(row, dict))
    whole_eligible = sum(int(row.get("eligible_intents") or 0) for row in members.values() if isinstance(row, dict))
    whole_accepted = sum(int(row.get("accepted_orders") or 0) for row in members.values() if isinstance(row, dict))
    selected_trigger = bool(selected_wallet and selected_fresh >= min_fresh_rows and selected_accepted == 0)
    whole_trigger = whole_fresh >= min_fresh_rows and whole_accepted == 0
    raw_firing = bool(can_trade and (selected_trigger or whole_trigger))
    method_acceptance = method_acceptance if isinstance(method_acceptance, dict) else {}
    method_accepted_orders = int(method_acceptance.get("accepted_orders") or 0)
    order_flow_clear_method_accepted = method_accepted_orders > 0
    submit_outcomes = submit_outcomes if isinstance(submit_outcomes, dict) else {}
    per_wallet_outcomes = (
        submit_outcomes.get("per_wallet")
        if isinstance(submit_outcomes.get("per_wallet"), dict)
        else {}
    )
    selected_outcomes = (
        per_wallet_outcomes.get(selected_wallet)
        if isinstance(per_wallet_outcomes.get(selected_wallet), dict)
        else {}
    )
    guard_submit_attempts = int(submit_outcomes.get("guard_submit_attempts") or 0)
    fak_no_match_outcomes = int(submit_outcomes.get("fak_no_match_outcomes") or 0)
    selected_guard_submit_attempts = int(selected_outcomes.get("guard_submit_attempts") or 0)
    selected_fak_no_match_outcomes = int(selected_outcomes.get("fak_no_match_outcomes") or 0)
    reject_taxonomy = (
        submit_outcomes.get("reject_taxonomy")
        if isinstance(submit_outcomes.get("reject_taxonomy"), dict)
        else {}
    )
    selected_reject_taxonomy = (
        selected_outcomes.get("reject_taxonomy")
        if isinstance(selected_outcomes.get("reject_taxonomy"), dict)
        else {}
    )
    liquidity_drought = bool(
        raw_firing
        and not order_flow_clear_method_accepted
        and guard_submit_attempts > 0
        and fak_no_match_outcomes > 0
    )
    source_wiring_attrition = bool(
        can_trade
        and selected_fresh > 0
        and selected_eligible == 0
        and selected_accepted == 0
        and not order_flow_clear_method_accepted
        and selected_attribution_status == "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
        and selected_source_row_identity_coverage >= SOURCE_ROW_IDENTITY_COVERAGE_FLOOR
    )
    firing = bool(
        (raw_firing and not order_flow_clear_method_accepted and not liquidity_drought)
        or source_wiring_attrition
    )
    target = rung_a.get("target_wallet") if isinstance(rung_a, dict) else None
    escalation = (
        "NONE_ATTRIBUTION_REQUIRED" if source_wiring_attrition
        else "RUNG_A_RESELECT" if firing and target
        else "RUNG_B_EMERGENCY_ADMISSION_DUE" if firing
        else "NONE"
    )
    return {
        "status": (
            "ORDER_FLOW_CLEAR_METHOD_ACCEPTED"
            if order_flow_clear_method_accepted
            else "LIQUIDITY_DROUGHT"
            if liquidity_drought
            else "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
            if source_wiring_attrition
            else "INCIDENT_POLICY_CHOKE"
            if firing
            else "CLEAR"
        ),
        "firing": firing,
        "raw_policy_choke_trigger": raw_firing,
        "source_wiring_attrition": source_wiring_attrition,
        "wallet_policy_diagnostic": (
            "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
            if raw_firing
            else "WALLET_POLICY_FLOW_CLEAR"
        ),
        "order_flow_clear_method_accepted": order_flow_clear_method_accepted,
        "method_accepted_orders": method_accepted_orders,
        "method_acceptance": method_acceptance,
        "liquidity_drought": liquidity_drought,
        "can_trade": can_trade,
        "lookback_s": lookback_s,
        "minimum_fresh_rows": min_fresh_rows,
        "selected_wallet": selected_wallet or None,
        "selected_suppressed_rows": selected_suppressed,
        "selected_fresh_source_rows": selected_fresh,
        "selected_eligible_intents": selected_eligible,
        "selected_accepted_orders": selected_accepted,
        "selected_suppression_taxonomy": selected.get("suppression_taxonomy", {}),
        "selected_attribution_status": selected_attribution_status or None,
        "selected_source_row_identity_coverage": selected_source_row_identity_coverage,
        "selected_intent_channel_populated": selected.get("intent_channel_populated"),
        "selected_intent_axis_coverage_residual_sampled": selected.get(
            "intent_axis_coverage_residual_sampled"
        ),
        "intent_channel": scan.get("intent_channel") or {},
        "whole_runtime_suppressed_rows": whole_suppressed,
        "whole_runtime_fresh_source_rows": whole_fresh,
        "whole_runtime_eligible_intents": whole_eligible,
        "whole_runtime_accepted_orders": whole_accepted,
        "guard_submit_attempts": guard_submit_attempts,
        "fak_no_match_outcomes": fak_no_match_outcomes,
        "selected_guard_submit_attempts": selected_guard_submit_attempts,
        "selected_fak_no_match_outcomes": selected_fak_no_match_outcomes,
        "reject_taxonomy": reject_taxonomy,
        "selected_reject_taxonomy": selected_reject_taxonomy,
        "submit_outcomes": submit_outcomes,
        "trigger_scope": "selected_member" if selected_trigger else "whole_runtime" if whole_trigger else None,
        "mechanical_escalation": escalation,
        "rung_a_seat_read": rung_a,
        "rule": (
            "wallet F1-F4 diagnostics remain wallet-scoped; any accepted currently armed sole-guard method order clears "
            "global order flow without being misattributed to a wallet; otherwise can_trade and >=50 "
            "fresh wallet rows with zero wallet accepts is POLICY_CHOKE unless FAK-no-match proves "
            "LIQUIDITY_DROUGHT"
        ),
    }


def _enforce_unattributed_selected_seat_attrition(
    policy_choke: dict[str, Any],
) -> bool:
    """Refuse source-quiet truth when selected supply disappears without a reason."""
    taxonomy = policy_choke.get("selected_suppression_taxonomy")
    taxonomy = taxonomy if isinstance(taxonomy, dict) else {}
    firing = bool(
        policy_choke.get("can_trade") is True
        and int(policy_choke.get("selected_fresh_source_rows") or 0) > 0
        and int(policy_choke.get("selected_eligible_intents") or 0) == 0
        and int(policy_choke.get("selected_accepted_orders") or 0) == 0
        and int(policy_choke.get("method_accepted_orders") or 0) == 0
        and not taxonomy
    )
    policy_choke["unattributed_selected_seat_attrition"] = firing
    if not firing:
        return False
    policy_choke["status_before_unattributed_selected_seat_attrition"] = (
        policy_choke.get("status")
    )
    policy_choke["status"] = "UNATTRIBUTED_SELECTED_SEAT_ATTRITION"
    policy_choke["firing"] = True
    policy_choke["raw_policy_choke_trigger"] = True
    policy_choke["mechanical_escalation"] = "NONE_ATTRIBUTION_REQUIRED"
    policy_choke["unattributed_selected_seat_attrition_rule"] = (
        "selected_fresh_source_rows>0 AND selected_eligible_intents==0 requires "
        "a non-empty selected_suppression_taxonomy; source quiet is forbidden"
    )
    return True


def _accepted_method_orders(
    ledger: dict[str, Any],
    *,
    since: datetime,
    until: datetime,
    effective_lanes: set[str] | None = None,
) -> dict[str, Any]:
    """Count accepted sole-guard orders without attributing them to wallet F1-F4."""
    accepted_statuses = {
        "FILLED",
        "LIVE_FILLED",
        "LIVE_MAKER_FILLED",
        "LIVE_SUBMITTED",
        "MATCHED",
        "SUBMITTED",
    }
    seen: set[str] = set()
    lane_counts: Counter[str] = Counter()
    excluded_lane_counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        row_ts = _order_event_ts(row)
        if row_ts is None or row_ts < since or row_ts > until:
            continue
        statuses = {
            str(row.get("status") or "").upper(),
            str(row.get("final_status") or "").upper(),
        }
        if not statuses.intersection(accepted_statuses):
            continue
        order_id = str(row.get("order_id") or row.get("intent_id") or "")
        if order_id and order_id in seen:
            continue
        if order_id:
            seen.add(order_id)
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
        lane = str(decision.get("execution_lane") or row.get("execution_lane") or "live_guard")
        if effective_lanes is not None and lane not in effective_lanes:
            excluded_lane_counts[lane] += 1
            continue
        lane_counts[lane] += 1
        if len(samples) < 10:
            samples.append(
                {
                    "order_id": order_id,
                    "intent_id": str(row.get("intent_id") or ""),
                    "accepted_at": row_ts.isoformat(),
                    "execution_lane": lane,
                    "statuses": sorted(statuses - {""}),
                }
            )
    return {
        "accepted_orders": sum(lane_counts.values()),
        "lane_counts": dict(sorted(lane_counts.items())),
        "effective_lanes": sorted(effective_lanes) if effective_lanes is not None else None,
        "excluded_demoted_rows": sum(excluded_lane_counts.values()),
        "excluded_lane_counts": dict(sorted(excluded_lane_counts.items())),
        "samples": samples,
        "lookback_start": since.isoformat(),
        "lookback_end": until.isoformat(),
        "rule": "accepted statuses from currently armed sole-guard execution lanes, deduped by order id",
    }


def _effective_execution_lanes(
    actuator: dict[str, Any],
    *,
    now: datetime,
    freshness_s: float = 180.0,
) -> set[str]:
    lanes = {"live_guard"}
    generated_at = _parse_ts(actuator.get("generated_at"))
    fresh = generated_at is not None and 0 <= (now - generated_at).total_seconds() <= freshness_s
    if fresh and str(actuator.get("status") or "").upper() not in {"", "DISABLED"}:
        lanes.add("e5_maker_first_btc5m_v1")
    return lanes


def _policy_choke_submit_outcomes(
    ledger: dict[str, Any],
    *,
    since: datetime,
    until: datetime,
    effective_lanes: set[str] | None = None,
) -> dict[str, Any]:
    """Count guard submit attempts and named exchange rejects by wallet."""
    per_wallet: dict[str, dict[str, int]] = defaultdict(
        lambda: {"guard_submit_attempts": 0, "fak_no_match_outcomes": 0}
    )
    per_wallet_reject_taxonomy: dict[str, Counter[str]] = defaultdict(Counter)
    seen: set[tuple[str, str]] = set()
    excluded_lane_counts: Counter[str] = Counter()
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("source_wallet") or row.get("wallet"))
        row_ts = _order_event_ts(row)
        if not wallet or row_ts is None or row_ts < since or row_ts > until:
            continue
        identity = str(row.get("order_id") or row.get("intent_id") or "")
        execution_role = str(row.get("execution_role") or "").lower()
        dedupe_key = (identity, execution_role)
        if identity and dedupe_key in seen:
            continue
        if identity:
            seen.add(dedupe_key)
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
        lane = str(decision.get("execution_lane") or row.get("execution_lane") or "live_guard")
        reject_reason = _classified_reject_reason(row)
        pre_submit_refusal = reject_reason in PRE_SUBMIT_REFUSAL_CLASSES
        if (
            effective_lanes is not None
            and lane not in effective_lanes
            and not pre_submit_refusal
        ):
            excluded_lane_counts[lane] += 1
            continue
        lifecycle = row.get("lifecycle") if isinstance(row.get("lifecycle"), list) else []
        submitted = any(
            isinstance(event, dict) and str(event.get("status") or "").upper() == "LIVE_SUBMITTED"
            for event in lifecycle
        )
        if pre_submit_refusal:
            per_wallet_reject_taxonomy[wallet][reject_reason] += 1
        if not submitted:
            continue
        per_wallet[wallet]["guard_submit_attempts"] += 1
        error_classes: set[str] = set()
        for event in lifecycle:
            if not isinstance(event, dict) or not isinstance(event.get("payload"), dict):
                continue
            error_class = str(event["payload"].get("error_class") or "").lower()
            if error_class:
                error_classes.add(error_class)
        if "fak_no_match" in error_classes:
            per_wallet[wallet]["fak_no_match_outcomes"] += 1
        if reject_reason and reject_reason not in PRE_SUBMIT_REFUSAL_CLASSES:
            per_wallet_reject_taxonomy[wallet][reject_reason] += 1
    wallets = set(per_wallet) | set(per_wallet_reject_taxonomy)
    rows = {
        wallet: {
            **dict(per_wallet[wallet]),
            "reject_taxonomy": dict(sorted(per_wallet_reject_taxonomy[wallet].items())),
        }
        for wallet in sorted(wallets)
    }
    reject_taxonomy: Counter[str] = Counter()
    for counts in per_wallet_reject_taxonomy.values():
        reject_taxonomy.update(counts)
    return {
        "lookback_start": since.isoformat(),
        "lookback_end": until.isoformat(),
        "guard_submit_attempts": sum(row["guard_submit_attempts"] for row in rows.values()),
        "fak_no_match_outcomes": sum(row["fak_no_match_outcomes"] for row in rows.values()),
        "reject_taxonomy": dict(sorted(reject_taxonomy.items())),
        "effective_lanes": sorted(effective_lanes) if effective_lanes is not None else None,
        "excluded_lane_counts": dict(sorted(excluded_lane_counts.items())),
        "per_wallet": rows,
    }


def _paper_routing_shadow_intent_trace(
    *,
    routing_shadow: dict[str, Any],
    ledger: dict[str, Any],
    selected_wallet: str,
    since: datetime,
    until: datetime,
    selected_seat_epoch_at: datetime | None,
    authority_counter: int,
) -> dict[str, Any]:
    """Trace paper-only routing-shadow eligible rows to observable ledger rows.

    This is reporting-only and intentionally not the live guard authority.  The
    routing-shadow rows declare paper-only execution, so missing live ledger rows
    are expected unless intent-id parity with the live guard population is proven.
    """
    selected_wallet = _normalize_wallet(selected_wallet)
    rows_dropped_missing_intent_id = 0
    rows_dropped_missing_observed_ts = 0
    ledger_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in ledger.get("orders") or []:
        if not isinstance(raw, dict):
            continue
        intent_id = str(raw.get("intent_id") or "")
        if intent_id:
            ledger_by_intent[intent_id].append(raw)

    traces: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in routing_shadow.get("fee_gated_measurement_rows") or []:
        if not isinstance(raw, dict):
            continue
        wallet = _normalize_wallet(
            raw.get("source_wallet") or raw.get("copyintent_source_wallet")
        )
        if wallet != selected_wallet:
            continue
        if str(raw.get("dominant_skip_reason") or "").lower() != "eligible":
            continue
        observed_s = _as_float(
            raw.get("observed_ts") or raw.get("source_detection_observed_ts")
        )
        if observed_s is None:
            rows_dropped_missing_observed_ts += 1
            continue
        observed_at = datetime.fromtimestamp(observed_s, tz=timezone.utc)
        if observed_at < since or observed_at > until:
            continue
        intent_id = str(raw.get("intent_id") or "")
        if not intent_id:
            rows_dropped_missing_intent_id += 1
            continue
        if intent_id in seen:
            continue
        seen.add(intent_id)
        ledger_rows = ledger_by_intent.get(intent_id, [])
        reject_reasons = sorted(
            {
                reason
                for row in ledger_rows
                for reason in [_classified_reject_reason(row)]
                if reason
            }
        )
        submitted = any(
            any(
                isinstance(event, dict)
                and str(event.get("status") or "").upper() == "LIVE_SUBMITTED"
                for event in (
                    row.get("lifecycle")
                    if isinstance(row.get("lifecycle"), list)
                    else []
                )
            )
            for row in ledger_rows
        )
        if submitted:
            last_stage = "LIVE_SUBMITTED"
            drop_predicate = None
        elif reject_reasons:
            last_stage = "PRE_SUBMIT_REFUSAL"
            drop_predicate = None
            explained_predicate = reject_reasons[0]
        elif ledger_rows:
            last_stage = "LEDGER_ROW_WITHOUT_SUBMIT_OR_NAMED_REFUSAL"
            drop_predicate = "UNEXPLAINED_LEDGER_TERMINAL"
            explained_predicate = None
        else:
            last_stage = "POLICY_ELIGIBLE_ROUTING_SHADOW"
            drop_predicate = "UNEXPLAINED_NO_LEDGER_ROW"
            explained_predicate = None
        if submitted:
            explained_predicate = None
        traces.append(
            {
                "intent_id": intent_id,
                "source_wallet": wallet,
                "candidate_id": raw.get("candidate_id"),
                "policy_id": raw.get("policy_id"),
                "market_slug": raw.get("market_slug"),
                "observed_at": observed_at.isoformat(),
                "last_stage": last_stage,
                "drop_predicate": drop_predicate,
                "explained_predicate": explained_predicate,
                "post_selected_seat_epoch": bool(
                    selected_seat_epoch_at is not None
                    and observed_at >= selected_seat_epoch_at
                ),
            }
        )

    unexplained = [row for row in traces if row.get("drop_predicate")]
    if selected_seat_epoch_at is None:
        alignment_status = "NOT_COMPARABLE_MISSING_SEAT_EPOCH"
    elif since < selected_seat_epoch_at:
        alignment_status = "NOT_COMPARABLE_LOOKBACK_PREDATES_SEAT_EPOCH"
    else:
        alignment_status = "COMPARABLE_LOOKBACK_WITHIN_SEAT_EPOCH"
    comparable = alignment_status == "COMPARABLE_LOOKBACK_WITHIN_SEAT_EPOCH"
    population_parity = "NOT_COMPARABLE"
    if comparable and len(traces) != int(authority_counter or 0):
        population_parity = "DIVERGENT"
    unexplained_count: int | None = len(unexplained) if comparable else None
    post_seat_unexplained_count: int | None = (
        sum(1 for row in unexplained if row["post_selected_seat_epoch"])
        if comparable
        else None
    )
    return {
        "measurement_only": True,
        "live_mutation": False,
        "traced_population_source": "routing_shadow.fee_gated_measurement_rows",
        "traced_population_lane": "paper_only",
        "traced_population_live_orders_allowed": False,
        "authority_counter_source": "policy_choke.selected_eligible_intents",
        "authority_counter": int(authority_counter or 0),
        "population_parity": population_parity,
        "rows_dropped_missing_intent_id": rows_dropped_missing_intent_id,
        "rows_dropped_missing_observed_ts": rows_dropped_missing_observed_ts,
        "selected_wallet": selected_wallet or None,
        "window_alignment": {
            "status": alignment_status,
            "submit_lookback_start": since.isoformat(),
            "submit_lookback_end": until.isoformat(),
            "selected_seat_epoch_at": (
                selected_seat_epoch_at.isoformat()
                if selected_seat_epoch_at is not None
                else None
            ),
        },
        "eligible_intent_traces": traces,
        "traced_eligible_intents": len(traces),
        "unexplained_eligible_intents": unexplained_count,
        "post_seat_unexplained_eligible_intents": post_seat_unexplained_count,
        "unexplained_status": (
            "COUNTED_COMPARABLE_WINDOW"
            if comparable
            else alignment_status
        ),
        "rule": (
            "paper-only routing-shadow rows are not live guard authority; cite only "
            "when window_alignment is comparable and population_parity is proven"
        ),
    }


def _live_intent_to_submit_reconciliation(
    *, selected_eligible_intents: int, selected_guard_submit_attempts: int
) -> dict[str, Any]:
    return {
        "measurement_only": True,
        "live_mutation": False,
        "status": "NO_PER_INTENT_GUARD_RECORD",
        "eligible": int(selected_eligible_intents or 0),
        "submit_attempts": int(selected_guard_submit_attempts or 0),
        "explained": 0,
        "unexplained": "UNMEASURABLE_NO_PER_INTENT_RECORD",
        "authority": "guard scan member_signal_age",
    }


def _policy_choke_scan_from_acceptance(
    rows: list[dict[str, Any]],
    event_scan: dict[str, Any],
    copy_intent_state: dict[str, Any] | None = None,
    *,
    now_s: float | None = None,
    guard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_members = event_scan.get("member_signal_age") if isinstance(event_scan.get("member_signal_age"), dict) else {}
    guard_attribution = (
        event_scan.get("source_row_attribution")
        if isinstance(event_scan.get("source_row_attribution"), dict)
        else {}
    )
    copy_intent_state = copy_intent_state if isinstance(copy_intent_state, dict) else {}
    copy_intents = (
        copy_intent_state.get("copy_intents")
        if isinstance(copy_intent_state.get("copy_intents"), list)
        else []
    )
    intent_merge_error = str(copy_intent_state.get("status") or "") == "COPY_INTENT_HISTORY_MERGE_ERROR"
    reference_now_s = float(now_s if now_s is not None else time.time())
    generated_at = _parse_ts(copy_intent_state.get("generated_at"))
    generated_at_s = generated_at.timestamp() if generated_at is not None else None
    intent_channel_age_s = (
        max(0.0, reference_now_s - generated_at_s)
        if generated_at_s is not None
        else None
    )
    guard = guard if isinstance(guard, dict) else {}
    guard_code_identity = (
        guard.get("guard_code_identity")
        if isinstance(guard.get("guard_code_identity"), dict)
        else {}
    )
    guard_pid = _guard_pid_from_state(guard)
    guard_started_at = str(guard_code_identity.get("started_at_utc") or "")
    writer_pid = copy_intent_state.get("writer_pid")
    try:
        writer_pid = int(writer_pid)
    except (TypeError, ValueError):
        writer_pid = None
    writer_guard_started_at = str(copy_intent_state.get("writer_guard_started_at") or "")
    writer_generation = str(
        copy_intent_state.get("writer_live_guard_generation_sha256") or ""
    )
    guard_generation = str(
        guard_code_identity.get("live_guard_generation_sha256") or ""
    )
    writer_adopted = bool(
        copy_intent_state
        and str(copy_intent_state.get("single_submitter") or "")
        == "scripts/run_wallet_copy_live_guard.py"
        and guard_pid is not None
        and writer_pid == guard_pid
        and writer_guard_started_at
        and writer_guard_started_at == guard_started_at
        and writer_generation
        and writer_generation == guard_generation
    )
    intent_channel_fresh = bool(
        intent_channel_age_s is not None
        and 0.0 <= intent_channel_age_s <= INTENT_CHANNEL_MAX_AGE_S
    )
    intent_channel_populated = bool(
        copy_intent_state
        and not intent_merge_error
        and intent_channel_fresh
        and writer_adopted
    )
    intent_channel_status = (
        "ABSENT"
        if not copy_intent_state
        else "COPY_INTENT_HISTORY_MERGE_ERROR"
        if intent_merge_error
        else "INTENT_CHANNEL_WRITER_UNADOPTED"
        if not writer_adopted
        else "INTENT_CHANNEL_STALE"
        if not intent_channel_fresh
        else str(copy_intent_state.get("status") or "PRESENT")
    )
    intents_by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for outer in copy_intents:
        if not isinstance(outer, dict):
            continue
        intent = outer.get("intent") if isinstance(outer.get("intent"), dict) else outer
        wallet = _normalize_wallet(intent.get("source_wallet"))
        observed_value = intent.get("observed_ts") or intent.get("event_ts")
        try:
            observed_s = float(observed_value)
        except (TypeError, ValueError):
            observed_at = _parse_ts(observed_value)
            observed_s = observed_at.timestamp() if observed_at is not None else None
        if (
            wallet
            and observed_s is not None
            and reference_now_s - POLICY_CHOKE_LOOKBACK_S <= observed_s <= reference_now_s
        ):
            intents_by_wallet[wallet].append(intent)
    members: dict[str, dict[str, Any]] = {}
    for row in rows:
        wallet = _normalize_wallet(row.get("wallet"))
        if not wallet:
            continue
        fresh = int(row.get("raw_own_source_buy_rows") or 0)
        eligible = int(row.get("policy_accepted_intents") or 0)
        if "policy_eligible_intents" in row:
            eligible = int(row.get("policy_eligible_intents") or 0)
        accepted = int(row.get("accepted_live_orders") or 0)
        event_row = event_members.get(wallet) if isinstance(event_members.get(wallet), dict) else {}
        unmatched_ids = [str(value) for value in row.get("unmatched_source_row_ids") or [] if value]
        identity_coverage = float(row.get("source_row_identity_coverage") or 0.0)
        unmatched_count = int(row.get("unmatched_source_row_count") or 0)
        taxonomy_covers_rows = len(unmatched_ids)
        residual_ids = [
            source_row_id
            for source_row_id in unmatched_ids
            if source_row_id not in guard_attribution
        ]
        wallet_intents = intents_by_wallet.get(wallet, [])
        intent_source_ids = {
            str(intent.get("source_row_event_id") or "")
            for intent in wallet_intents
            if str(intent.get("source_row_event_id") or "")
        }
        intent_axis_evidenced_ids = {
            source_row_id
            for source_row_id in residual_ids
            if source_row_id in intent_source_ids
        }
        intent_axis_coverage_residual_sampled = (
            len(intent_axis_evidenced_ids) / len(residual_ids)
            if residual_ids
            else 0.0
        )
        attribution = Counter()
        if identity_coverage >= SOURCE_ROW_IDENTITY_COVERAGE_FLOOR:
            for source_row_id in unmatched_ids:
                if source_row_id in guard_attribution:
                    attribution[str(guard_attribution[source_row_id])] += 1
                elif not intent_channel_populated:
                    attribution[intent_channel_status] += 1
                elif source_row_id in intent_source_ids:
                    attribution["INTENT_BUILT_NEVER_SUBMITTED"] += 1
                else:
                    attribution["NO_INTENT_RECORD_FOR_SOURCE_ROW"] += 1
        taxonomy = dict(sorted(attribution.items()))
        dominant_reason = (
            sorted(taxonomy.items(), key=lambda item: (-item[1], item[0]))[0][0]
            if taxonomy
            else None
        )
        members[wallet] = {
            "fresh_source_rows": fresh,
            "eligible_intents": eligible,
            "accepted_orders": accepted,
            "suppressed_intents": max(0, fresh - eligible),
            "observed_suppression_events": int(event_row.get("suppressed_intents") or 0),
            "suppression_taxonomy": taxonomy,
            "unmatched_source_row_ids": unmatched_ids,
            "unmatched_source_row_ids_truncated": bool(row.get("unmatched_source_row_ids_truncated")),
            "source_row_identity_coverage": identity_coverage,
            "source_row_identity_coverage_floor": SOURCE_ROW_IDENTITY_COVERAGE_FLOOR,
            "intent_record_axis_available": intent_channel_populated,
            "intent_channel_populated": intent_channel_populated,
            "intent_channel_wallet_records_30m": len(wallet_intents),
            "intent_channel_merge_error": intent_merge_error,
            "intent_channel_writer_adopted": writer_adopted,
            "intent_channel_status": intent_channel_status,
            "guard_attributed_rows": max(0, len(unmatched_ids) - len(residual_ids)),
            "intent_axis_evidenced_rows": len(intent_axis_evidenced_ids),
            "intent_axis_residual_sampled_rows": len(residual_ids),
            "intent_axis_coverage_residual_sampled": intent_axis_coverage_residual_sampled,
            "taxonomy_covers_rows": taxonomy_covers_rows,
            "taxonomy_total_rows": unmatched_count,
            "taxonomy_coverage_fraction": (
                round(taxonomy_covers_rows / unmatched_count, 6)
                if unmatched_count
                else 0.0
            ),
            "attribution_status": (
                "NO_UNMATCHED_SOURCE_ROWS"
                if unmatched_count <= 0
                else "UNMEASURABLE_SOURCE_ROW_IDENTITY"
                if identity_coverage <= 0.0
                else "PARTIAL_SAMPLE_ATTRIBUTION"
                if (
                    identity_coverage < SOURCE_ROW_IDENTITY_COVERAGE_FLOOR
                    or taxonomy_covers_rows < unmatched_count
                )
                else intent_channel_status
                if residual_ids and not intent_channel_populated
                else "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
                if dominant_reason == "NO_INTENT_RECORD_FOR_SOURCE_ROW"
                else "ATTRIBUTED_SOURCE_ROW_ATTRITION"
                if dominant_reason
                else "UNMEASURED"
            ),
        }
    return {
        "member_signal_age": members,
        "intent_channel": {
            "status": intent_channel_status,
            "sidecar_status": str(copy_intent_state.get("status") or "ABSENT"),
            "generated_at": copy_intent_state.get("generated_at"),
            "age_s": round(intent_channel_age_s, 6) if intent_channel_age_s is not None else None,
            "max_age_s": INTENT_CHANNEL_MAX_AGE_S,
            "writer_pid": writer_pid,
            "writer_guard_started_at": writer_guard_started_at or None,
            "writer_live_guard_generation_sha256": writer_generation or None,
            "guard_pid": guard_pid,
            "guard_started_at": guard_started_at or None,
            "guard_live_guard_generation_sha256": guard_generation or None,
            "writer_adopted": writer_adopted,
            "writer_adoption_basis": (
                "single_submitter + writer_pid + started_at_utc + live_guard_generation_sha256 exact match"
            ),
            "record_count_30m": sum(len(intent_rows) for intent_rows in intents_by_wallet.values()),
            "merge_error": intent_merge_error,
        },
    }


def _policy_choke_terminal_reconciliation(
    *,
    guard: dict[str, Any],
    hot_history: dict[str, Any],
    routing_shadow: dict[str, Any],
    ledger: dict[str, Any],
    now: datetime,
    lookback_s: float = POLICY_CHOKE_LOOKBACK_S,
) -> dict[str, Any]:
    """Assign every policy-choke source row to one terminal funnel stage.

    The 30-minute policy-choke numerator deliberately includes recently
    observed source BUYs from closed windows. Those rows prove roster
    liveness, but they are not current order supply. Reconcile them here
    and expose a separate live-feedstock count so closed/stale rows can
    never satisfy F2 or the policy-eligible numerator.
    """
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    enabled_wallets = {
        _normalize_wallet(member.get("source_wallet") or member.get("wallet"))
        for member in runtime.get("members") or []
        if isinstance(member, dict) and member.get("enabled") is not False
    }
    since_s = now.timestamp() - lookback_s
    source_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in hot_history.get("events") or []:
        if not isinstance(row, dict) or str(row.get("action") or "").upper() != "BUY":
            continue
        wallet = _normalize_wallet(row.get("source_wallet"))
        observed_s = _as_float(row.get("observed_ts") or row.get("event_ts"))
        event_id = str(row.get("event_id") or row.get("source_fingerprint") or "")
        if (
            wallet not in enabled_wallets
            or observed_s is None
            or not since_s <= observed_s <= now.timestamp()
            or not event_id
        ):
            continue
        source_rows.setdefault((wallet, event_id), row)

    routing_by_market: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in routing_shadow.get("fee_gated_measurement_rows") or []:
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("source_wallet") or row.get("copyintent_source_wallet"))
        observed_s = _as_float(row.get("observed_ts") or row.get("source_detection_observed_ts"))
        if wallet not in enabled_wallets or observed_s is None or not since_s <= observed_s <= now.timestamp():
            continue
        key = (
            wallet,
            str(row.get("market_slug") or ""),
            str(row.get("outcome") or row.get("side") or "").upper(),
        )
        routing_by_market[key].append(row)

    participation_by_market: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    for row in participation.get("rows") or []:
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("source_wallet"))
        key = (
            wallet,
            str(row.get("market_slug") or ""),
            str(row.get("outcome") or "").upper(),
        )
        participation_by_market[key].append(row)

    accepted_statuses = {
        "FILLED", "LIVE_FILLED", "LIVE_MAKER_FILLED",
        "LIVE_SUBMITTED", "MATCHED", "SUBMITTED",
    }
    accepted_markets: set[tuple[str, str, str]] = set()
    attempted_markets: set[tuple[str, str, str]] = set()
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        row_ts = _order_event_ts(row)
        wallet = _normalize_wallet(row.get("source_wallet") or row.get("wallet"))
        if wallet not in enabled_wallets or row_ts is None or row_ts.timestamp() < since_s:
            continue
        key = (
            wallet,
            str(row.get("market_slug") or row.get("market") or ""),
            str(row.get("outcome") or row.get("side") or "").upper(),
        )
        attempted_markets.add(key)
        statuses = {
            str(row.get("status") or "").upper(),
            str(row.get("final_status") or "").upper(),
        }
        if statuses.intersection(accepted_statuses):
            accepted_markets.add(key)

    def classified_reason(reason: str) -> str:
        value = reason.lower()
        if any(token in value for token in ("price", "entry_cap", "entry_floor", "band")):
            return "price_or_band_gate"
        if "precision" in value or "min_order" in value or "tranche" in value:
            return "precision_or_order_floor"
        if any(token in value for token in ("inventory", "window_fill_cap", "filled")):
            return "inventory_gate"
        if value in {"submitted", "ready_to_submit", "live_order_rejected", "fak_no_match"}:
            return "submit_outcome"
        return f"copyintent_policy:{value or 'unspecified'}"

    terminal_counts: Counter[str] = Counter()
    wallet_counts: dict[str, Counter[str]] = defaultdict(Counter)
    phase_cross_tab: Counter[tuple[str, str, str, str]] = Counter()
    deferred_re_evaluation_by_wallet: Counter[str] = Counter()
    deferred_re_evaluated_by_wallet: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for (wallet, event_id), row in sorted(
        source_rows.items(),
        key=lambda item: float(item[1].get("observed_ts") or item[1].get("event_ts") or 0),
    ):
        market_slug = str(row.get("market_slug") or row.get("market") or "")
        outcome = str(row.get("outcome") or row.get("side") or "").upper()
        key = (wallet, market_slug, outcome)
        market_match = re.search(r"(?:btc-updown-5m-|btc-5m-)(\d{10})$", market_slug)
        market_start_s = int(market_match.group(1)) if market_match else None
        observed_s = _as_float(row.get("observed_ts"))
        event_s = _as_float(row.get("event_ts"))
        api_latency_s = _as_float(row.get("api_latency_s"))
        if api_latency_s is None and observed_s is not None and event_s is not None:
            api_latency_s = max(0.0, observed_s - event_s)
        current_window_start_s = int(now.timestamp() // 300) * 300
        guard_next_window_preopen = bool(
            market_start_s is not None
            and observed_s is not None
            and observed_s < market_start_s
            and market_start_s == current_window_start_s + 300
        )
        pre_open_observation_ready = bool(
            market_start_s is not None
            and observed_s is not None
            and observed_s < market_start_s
            and now.timestamp() >= market_start_s
        )
        if market_start_s is None:
            terminal = "freshness_market_identity_missing"
        elif observed_s is None:
            terminal = "freshness_observed_ts_missing"
        elif observed_s < market_start_s and not pre_open_observation_ready and not guard_next_window_preopen:
            terminal = "freshness_market_not_open_yet"
        elif observed_s >= market_start_s + 300:
            terminal = "freshness_market_closed_before_evaluation"
        elif api_latency_s is not None and api_latency_s > 30.0:
            terminal = "freshness_source_too_stale"
        elif key in accepted_markets:
            terminal = "submit_outcome:accepted"
        elif key in attempted_markets:
            terminal = "submit_outcome:rejected_or_unaccepted"
        else:
            participation_rows = participation_by_market.get(key, [])
            routing_rows = routing_by_market.get(key, [])
            if participation_rows:
                terminal = classified_reason(
                    str(participation_rows[-1].get("dominant_skip_reason") or "")
                )
            elif routing_rows:
                reason = str(routing_rows[-1].get("dominant_skip_reason") or "")
                terminal = (
                    "copyintent_policy:eligible_no_guard_candidate"
                    if reason.lower() == "eligible"
                    else classified_reason(reason)
                )
            elif now.timestamp() >= market_start_s + 300 and not pre_open_observation_ready:
                terminal = "freshness_market_closed_before_evaluation"
            else:
                terminal = "candidate_not_built"
        if pre_open_observation_ready:
            deferred_re_evaluated_by_wallet[wallet] += 1
        terminal_counts[terminal] += 1
        wallet_counts[wallet][terminal] += 1
        source_inside_window = (
            "unknown"
            if market_start_s is None or event_s is None
            else "yes"
            if market_start_s <= event_s < market_start_s + 300
            else "no"
        )
        latency_within_budget = (
            "unknown"
            if api_latency_s is None
            else "yes"
            if api_latency_s <= 30.0
            else "no"
        )
        phase_cross_tab[
            (wallet, terminal, source_inside_window, latency_within_budget)
        ] += 1
        if terminal == "freshness_market_not_open_yet":
            deferred_re_evaluation_by_wallet[wallet] += 1
        if len(samples) < 40:
            samples.append(
                {
                    "source_wallet": wallet,
                    "event_id": event_id,
                    "market_slug": market_slug,
                    "outcome": outcome,
                    "observed_ts": row.get("observed_ts") or row.get("event_ts"),
                    "api_latency_s": api_latency_s,
                    "terminal_stage": terminal,
                }
            )

    input_rows = len(source_rows)
    terminal_rows = sum(terminal_counts.values())
    source_coverage_full = input_rows == terminal_rows
    liveness_only_terminals = {
        "freshness_market_identity_missing",
        "freshness_observed_ts_missing",
        "freshness_market_not_open_yet",
        "freshness_market_closed_before_evaluation",
        "freshness_source_too_stale",
    }
    liveness_only_rows = sum(
        count for terminal, count in terminal_counts.items()
        if terminal in liveness_only_terminals
    )
    per_wallet_live_feedstock_rows = {
        wallet: sum(
            count for terminal, count in counts.items()
            if terminal not in liveness_only_terminals
        )
        for wallet, counts in sorted(wallet_counts.items())
    }
    return {
        "status": "PASS" if source_coverage_full else "FAIL",
        "lookback_s": lookback_s,
        "input_fresh_source_rows": input_rows,
        "terminal_rows": terminal_rows,
        "source_coverage_full": source_coverage_full,
        "input_equals_terminal_rows": input_rows == terminal_rows,
        "live_feedstock_rows": max(0, terminal_rows - liveness_only_rows),
        "liveness_only_rows": liveness_only_rows,
        "per_wallet_live_feedstock_rows": per_wallet_live_feedstock_rows,
        "terminal_stage_counts": dict(sorted(terminal_counts.items())),
        "per_wallet_terminal_stage_counts": {
            wallet: dict(sorted(counts.items()))
            for wallet, counts in sorted(wallet_counts.items())
        },
        "terminal_phase_cross_tab": {
            "schema_version": 1,
            "rows": [
                {
                    "source_wallet": wallet,
                    "terminal_stage": terminal,
                    "source_event_inside_market_window": source_inside_window,
                    "api_latency_lte_30s": latency_within_budget,
                    "rows": count,
                }
                for (
                    wallet,
                    terminal,
                    source_inside_window,
                    latency_within_budget,
                ), count in sorted(phase_cross_tab.items())
            ],
            "classification": {
                "cycle_rescuable": (
                    "freshness_market_closed_before_evaluation with "
                    "source_event_inside_market_window=yes"
                ),
                "cycle_unrescuable": (
                    "freshness_market_closed_before_evaluation with "
                    "source_event_inside_market_window=no"
                ),
                "deferred_open_opportunity": "freshness_market_not_open_yet",
            },
            "rule": (
                "aggregate all terminal rows by wallet, terminal stage, source-event "
                "market-window membership, and 30s API-latency budget; never pool "
                "closed-before-evaluation with pre-open rows"
            ),
        },
        "deferred_re_evaluation_at_open": {
            "total_rows": sum(deferred_re_evaluation_by_wallet.values()),
            "per_wallet_rows": dict(sorted(deferred_re_evaluation_by_wallet.items())),
            "c539_rows": deferred_re_evaluation_by_wallet.get(
                "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
                0,
            ),
            "re_evaluated_rows": sum(deferred_re_evaluated_by_wallet.values()),
            "re_evaluated_per_wallet_rows": dict(
                sorted(deferred_re_evaluated_by_wallet.items())
            ),
            "cycle_rescuable": False,
            "next_mechanism": "retain pre-open rows and re-evaluate when now >= market_start",
            "rule": (
                "freshness_market_not_open_yet is retained only until market open; "
                "the first cut at now >= market_start re-runs normal accepted, attempted, "
                "participation, routing, and freshness terminal attribution"
            ),
        },
        "samples": samples,
        "acceptance_gate": (
            "input_fresh_source_rows == terminal_rows and source_coverage_full; "
            "rows observed while their market is open with api_latency_s<=30, plus "
            "guard-admissible next-window pre-open rows matching run_wallet_copy_live_guard.py:5196, "
            "may satisfy live feedstock/F2/eligible counts; closed or stale rows are liveness-only"
        ),
    }


def _rung_a_candidate_reconciliation(
    *,
    rung_a: dict[str, Any],
    candidates: dict[str, Any],
    terminal_reconciliation: dict[str, Any],
) -> dict[str, Any]:
    candidate_wallets = {
        str(row.get("wallet") or "").lower()
        for row in (candidates.get("nearest_frontier") or [])
        if isinstance(row, dict) and str(row.get("wallet") or "")
    }
    cross_tab = (
        terminal_reconciliation.get("terminal_phase_cross_tab")
        if isinstance(
            terminal_reconciliation.get("terminal_phase_cross_tab"), dict
        )
        else {}
    )
    phase_rows = [
        row for row in (cross_tab.get("rows") or []) if isinstance(row, dict)
    ]
    live_feedstock_by_wallet = (
        terminal_reconciliation.get("per_wallet_live_feedstock_rows")
        if isinstance(
            terminal_reconciliation.get("per_wallet_live_feedstock_rows"), dict
        )
        else {}
    )
    reconciled_rows: list[dict[str, Any]] = []
    for raw in rung_a.get("rows") or []:
        if not isinstance(raw, dict):
            continue
        wallet = str(raw.get("wallet") or "").lower()
        wallet_phase_rows = [
            row
            for row in phase_rows
            if str(row.get("source_wallet") or "").lower() == wallet
        ]
        terminal_rows = sum(int(row.get("rows") or 0) for row in wallet_phase_rows)
        pre_open_rows = sum(
            int(row.get("rows") or 0)
            for row in wallet_phase_rows
            if row.get("terminal_stage") == "freshness_market_not_open_yet"
        )
        cycle_rescuable_closed_rows = sum(
            int(row.get("rows") or 0)
            for row in wallet_phase_rows
            if row.get("terminal_stage")
            == "freshness_market_closed_before_evaluation"
            and row.get("source_event_inside_market_window") == "yes"
        )
        live_feedstock_rows = int(live_feedstock_by_wallet.get(wallet) or 0)
        candidate_present = wallet in candidate_wallets
        proven_positive = str(raw.get("regime_slice_label") or "") == "PROVEN-POSITIVE"
        no_evidenced_policy_for_incumbent = bool(
            proven_positive
            and not candidate_present
            and int(raw.get("policy_eligible_intents") or 0) == 0
        )
        excluded_by_liveness_only_feedstock = bool(
            proven_positive
            and not candidate_present
            and terminal_rows > 0
            and live_feedstock_rows == 0
            and not no_evidenced_policy_for_incumbent
        )
        exclusion_rescuable_at_cut = bool(
            excluded_by_liveness_only_feedstock
            and (pre_open_rows > 0 or cycle_rescuable_closed_rows > 0)
        )
        reconciled_rows.append(
            {
                "wallet": wallet,
                "regime_slice_label": raw.get("regime_slice_label"),
                "policy_eligible_intents": raw.get("policy_eligible_intents"),
                "candidate_present": candidate_present,
                "terminal_rows": terminal_rows,
                "pre_open_rows": pre_open_rows,
                "cycle_rescuable_closed_rows": cycle_rescuable_closed_rows,
                "live_feedstock_rows": live_feedstock_rows,
                "no_evidenced_policy_for_incumbent": no_evidenced_policy_for_incumbent,
                "excluded_by_liveness_only_feedstock_gate": (
                    excluded_by_liveness_only_feedstock
                ),
                "classification": (
                    "NO_EVIDENCED_POLICY_FOR_INCUMBENT"
                    if no_evidenced_policy_for_incumbent
                    else "PROVEN_POSITIVE_EXCLUDED_RESCUABLE_AT_CUT"
                    if exclusion_rescuable_at_cut
                    else "PROVEN_POSITIVE_EXCLUDED_UNRESCUABLE_AT_CUT"
                    if excluded_by_liveness_only_feedstock
                    else "PRESENT_IN_CANDIDATE_EVIDENCE"
                    if candidate_present
                    else "ABSENT_OTHER_OR_UNMEASURED_CAUSE"
                ),
            }
        )
    excluded_positive = [
        row
        for row in reconciled_rows
        if row["excluded_by_liveness_only_feedstock_gate"]
    ]
    rescuable_excluded = [
        row
        for row in excluded_positive
        if row["pre_open_rows"] > 0 or row["cycle_rescuable_closed_rows"] > 0
    ]
    unrescuable_excluded = [
        row
        for row in excluded_positive
        if row["pre_open_rows"] == 0 and row["cycle_rescuable_closed_rows"] == 0
    ]
    no_policy_incumbents = [
        row for row in reconciled_rows if row["no_evidenced_policy_for_incumbent"]
    ]
    action = str(rung_a.get("action") or "")
    return {
        "status": (
            "UNRECONCILED_NO_RUNG_A_TARGET_POSITIVE_LIVENESS_ONLY_EXCLUSION"
            if action == "NO_RUNG_A_TARGET" and rescuable_excluded
            else "RECONCILED_WITH_NO_EVIDENCED_POLICY_FOR_INCUMBENT"
            if action == "NO_RUNG_A_TARGET" and no_policy_incumbents
            else "RECONCILED_WITH_NAMED_UNRESCUABLE_EXCLUSION"
            if action == "NO_RUNG_A_TARGET" and unrescuable_excluded
            else "RECONCILED"
        ),
        "rung_a_action": action,
        "candidate_count": candidates.get("candidate_count"),
        "eligible_count": candidates.get("eligible_count"),
        "rows": reconciled_rows,
        "positive_liveness_only_excluded_wallets": [
            row["wallet"] for row in excluded_positive
        ],
        "rescuable_excluded_wallets": [
            row["wallet"] for row in rescuable_excluded
        ],
        "unrescuable_excluded_wallets": [
            row["wallet"] for row in unrescuable_excluded
        ],
        "no_evidenced_policy_incumbent_wallets": [
            row["wallet"] for row in no_policy_incumbents
        ],
        "finding": (
            "NO_RUNG_A_TARGET is an unreconciled read: a PROVEN-POSITIVE "
            "rung-A wallet is absent from candidate evidence because its current "
            "pre-open or cycle-rescuable rows have not reached live feedstock"
            if rescuable_excluded
            else "NO_RUNG_A_TARGET is reconciled with a PROVEN-POSITIVE incumbent "
            "that has no evidenced policy candidate to build"
            if no_policy_incumbents
            else "NO_RUNG_A_TARGET is reconciled with a named PROVEN-POSITIVE "
            "wallet whose current exclusion is unrescuable at this cut"
            if unrescuable_excluded
            else "rung-A and candidate evidence reconcile at this cut"
        ),
        "next_mechanism": (
            "re-evaluate pre-open rows at market open and separately optimize "
            "cycle-rescuable closed-before-evaluation rows"
            if rescuable_excluded
            else "NO_EVIDENCED_POLICY_FOR_INCUMBENT"
            if no_policy_incumbents
            else "raise source-side row supply or cut source latency for the named wallet"
            if unrescuable_excluded
            else "none"
        ),
        "rule": (
            "compare rung_a_seat_read.rows and candidate_evidence.nearest_frontier "
            "from the same deadman cut; reserve UNRECONCILED for exclusions with "
            "pre-open or cycle-rescuable rows, and name zero-rescuable-row exclusions "
            "without suppressing NO_RUNG_A_TARGET"
        ),
    }


def _apply_policy_choke_feedstock_gate(
    policy_choke: dict[str, Any],
    reconciliation: dict[str, Any],
) -> None:
    """Exclude stale/closed source rows from eligible intent counters in-place."""
    by_wallet = (
        reconciliation.get("per_wallet_live_feedstock_rows")
        if isinstance(reconciliation.get("per_wallet_live_feedstock_rows"), dict)
        else {}
    )
    selected_wallet = _normalize_wallet(policy_choke.get("selected_wallet"))
    selected_live_feedstock = int(by_wallet.get(selected_wallet) or 0)
    whole_live_feedstock = sum(int(value or 0) for value in by_wallet.values())
    selected_before = int(policy_choke.get("selected_eligible_intents") or 0)
    whole_before = int(policy_choke.get("whole_runtime_eligible_intents") or 0)
    policy_choke["selected_eligible_intents_pre_feedstock_gate"] = selected_before
    policy_choke["whole_runtime_eligible_intents_pre_feedstock_gate"] = whole_before
    policy_choke["selected_live_feedstock_rows"] = selected_live_feedstock
    policy_choke["whole_runtime_live_feedstock_rows"] = whole_live_feedstock
    policy_choke["selected_eligible_intents"] = min(
        selected_before,
        selected_live_feedstock,
    )
    policy_choke["whole_runtime_eligible_intents"] = min(
        whole_before,
        whole_live_feedstock,
    )
    policy_choke["feedstock_gate"] = {
        "status": "PASS",
        "max_api_latency_s": 30.0,
        "closed_or_stale_rows_are_liveness_only": True,
        "rule": (
            "policy eligibility is capped by rows observed while the BTC-5m market "
            "was open with api_latency_s<=30, plus guard-admissible next-window pre-open "
            "rows matching run_wallet_copy_live_guard.py:5196; roster-liveness counts "
            "remain unchanged"
        ),
    }


def _classify_policy_choke_local_skip(
    *,
    policy_choke: dict[str, Any],
    reconciliation: dict[str, Any],
    can_trade: bool,
    guard_status: str,
) -> dict[str, Any]:
    reject_taxonomy = (
        policy_choke.get("reject_taxonomy")
        if isinstance(policy_choke.get("reject_taxonomy"), dict)
        else {}
    )
    terminal_counts = (
        reconciliation.get("terminal_stage_counts")
        if isinstance(reconciliation.get("terminal_stage_counts"), dict)
        else {}
    )
    live_feedstock_rows = int(reconciliation.get("live_feedstock_rows") or 0)
    local_reject_rows = sum(
        int(count or 0)
        for name, count in reject_taxonomy.items()
        if str(name) in LOCAL_REFUSAL_REJECT_CLASSES
    )
    local_terminal_rows = (
        int(terminal_counts.get("inventory_gate") or 0)
        + int(terminal_counts.get("price_or_band_gate") or 0)
        + sum(
            int(count or 0)
            for name, count in terminal_counts.items()
            if str(name).startswith("copyintent_policy:")
        )
        + local_reject_rows
    )
    local_terminal_share = (
        local_terminal_rows / live_feedstock_rows if live_feedstock_rows > 0 else 0.0
    )
    reject_classes = {str(name) for name in reject_taxonomy}
    explicit_local_refusal_evidence = bool(reject_classes)
    local_only_rejects = bool(
        explicit_local_refusal_evidence
        and reject_classes.issubset(LOCAL_REFUSAL_REJECT_CLASSES)
    )
    sizing_only_rejects = bool(
        reject_classes
        and reject_classes.issubset({"maker_min_share_bump_exceeds_policy_cap"})
    )
    whole_runtime_accepted_orders = int(
        policy_choke.get("whole_runtime_accepted_orders") or 0
    )
    method_accepted_orders = int(policy_choke.get("method_accepted_orders") or 0)
    selected_accepted_orders = int(policy_choke.get("selected_accepted_orders") or 0)
    accepted_orders = max(
        whole_runtime_accepted_orders,
        method_accepted_orders,
        selected_accepted_orders,
    )
    pipe_healthy_terms = bool(
        can_trade
        and guard_status == "LIVE_GUARD_RUNNING"
        and int(policy_choke.get("fak_no_match_outcomes") or 0) == 0
    )
    local_refusal_terms = bool(
        local_only_rejects and local_terminal_share >= 0.60
    )
    qualifies = bool(pipe_healthy_terms and local_refusal_terms and accepted_orders == 0)
    flow_alive_accepted_orders = bool(
        pipe_healthy_terms and accepted_orders > 0
    )
    return {
        "qualifies": qualifies,
        "deadman_class": (
            "FLOW_ALIVE_ACCEPTED_ORDERS"
            if flow_alive_accepted_orders
            else "POLICY_CHOKE_LOCAL_SKIP"
            if qualifies and sizing_only_rejects
            else "MEASURED_SKIP_GATED_QUIET"
            if qualifies
            else "ORDER_FLOW_DEAD"
        ),
        "pipe_healthy_terms": pipe_healthy_terms,
        "local_refusal_terms": local_refusal_terms,
        "local_refusal_reject_classes": sorted(LOCAL_REFUSAL_REJECT_CLASSES),
        "observed_reject_classes": sorted(reject_classes),
        "explicit_local_refusal_evidence": explicit_local_refusal_evidence,
        "local_only_rejects": local_only_rejects,
        "sizing_only_rejects": sizing_only_rejects,
        "mechanical_escalation": (
            "LOCAL_POLICY_SIZING_CHOKE_DUE"
            if qualifies and sizing_only_rejects
            else "NONE"
        ),
        "local_reject_rows": local_reject_rows,
        "local_terminal_rows": local_terminal_rows,
        "live_feedstock_rows": live_feedstock_rows,
        "local_terminal_share": round(local_terminal_share, 6),
        "minimum_local_terminal_share": 0.60,
        "fak_no_match_outcomes": int(
            policy_choke.get("fak_no_match_outcomes") or 0
        ),
        "accepted_orders": accepted_orders,
        "whole_runtime_accepted_orders": whole_runtime_accepted_orders,
        "method_accepted_orders": method_accepted_orders,
        "selected_accepted_orders": selected_accepted_orders,
        "accepted_order_counters": {
            "whole_runtime_accepted_orders": whole_runtime_accepted_orders,
            "method_accepted_orders": method_accepted_orders,
            "selected_accepted_orders": selected_accepted_orders,
            "chosen_accepted_orders": accepted_orders,
            "chosen_rule": "max(whole_runtime, method, selected)",
        },
    }


def _accepted_order_deadman_disposition(
    *,
    raw_firing: bool,
    local_skip_qualifies: bool,
    ruled_posture_exemption: bool,
    accepted_orders: int = 0,
    local_terminal_share: float = 0.0,
    fak_no_match_outcomes: int = 0,
    liquidity_drought: bool = False,
    generation_mismatch: bool = True,
    guard_status: str = "",
    selection_pending_adoption: bool = False,
    wallet_policy_diagnostic: str = "",
    measured_empty_seat_no_target: bool = False,
    selected_eligible_intents: int = 0,
    selected_guard_submit_attempts: int = 0,
    local_skip_deadman_class: str = "POLICY_CHOKE_LOCAL_SKIP",
    local_skip_mechanical_escalation: str = "LOCAL_POLICY_SIZING_CHOKE_DUE",
) -> dict[str, str] | None:
    if not raw_firing:
        return None
    if local_skip_qualifies and not ruled_posture_exemption:
        if local_skip_mechanical_escalation == "NONE":
            return {
                "status": "INCIDENT_MEASURED_SKIP_GATED_QUIET",
                "deadman_class": local_skip_deadman_class,
                "mechanical_escalation": "NONE",
            }
        return {
            "status": "INCIDENT_LOCAL_POLICY_SKIP",
            "deadman_class": "POLICY_CHOKE_LOCAL_SKIP",
            "mechanical_escalation": "LOCAL_POLICY_SIZING_CHOKE_DUE",
        }
    if (
        wallet_policy_diagnostic == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
        and measured_empty_seat_no_target
        and selected_eligible_intents == 0
        and selected_guard_submit_attempts == 0
    ):
        return {
            "status": "MEASURED_SKIP_CORRECTLY_IDLE",
            "deadman_class": "MEASURED_NO_ADMISSIBLE_TARGET",
            "mechanical_escalation": "NONE",
        }
    if (
        accepted_orders == 0
        and local_terminal_share >= 0.60
        and (fak_no_match_outcomes > 0 or liquidity_drought)
        and not generation_mismatch
        and guard_status == "LIVE_GUARD_RUNNING"
    ):
        return {
            "status": "INCIDENT_MEASURED_NO_FLOW",
            "deadman_class": "MEASURED_ORDER_FLOW_DROUGHT",
            "mechanical_escalation": "NONE",
        }
    return {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "deadman_class": "ORDER_FLOW_DEAD",
        "mechanical_escalation": (
            "MANAGED_RESTART_SELECTION_PENDING_ADOPTION"
            if selection_pending_adoption
            else "NO_LAWFUL_ACTUATION"
        ),
    }


def _policy_choke_rung_b_refusal_record(
    *,
    selection_pin: dict[str, Any],
    candidate_evidence: dict[str, Any],
    overlay: dict[str, Any],
    now: datetime,
    supply_rung: str,
) -> dict[str, Any]:
    candidate_count_raw = candidate_evidence.get("candidate_count")
    eligible_count_raw = candidate_evidence.get("eligible_count")
    try:
        candidate_count = (
            int(candidate_count_raw) if candidate_count_raw is not None else None
        )
    except (TypeError, ValueError):
        candidate_count = None
    try:
        eligible_count = (
            int(eligible_count_raw) if eligible_count_raw is not None else None
        )
    except (TypeError, ValueError):
        eligible_count = None

    arm_events: dict[str, str] = {}
    renew_events: dict[str, str] = {}

    def add_pin_events(pin: dict[str, Any], source: str) -> None:
        if not isinstance(pin, dict) or pin.get("pin_id") != POLICY_CHOKE_RUNG_B_PIN_ID:
            return
        created = _parse_ts(pin.get("created_at"))
        if created is not None and created.date() == now.date():
            arm_events[created.isoformat()] = f"{source}.created_at"
        renewed = _parse_ts(pin.get("last_renewed_at"))
        if renewed is not None and renewed.date() == now.date():
            renew_events[renewed.isoformat()] = f"{source}.last_renewed_at"

    add_pin_events(selection_pin, "active_selection_pin")
    latest_admission = (
        overlay.get("latest_policy_choke_rung_b_admission")
        if isinstance(overlay.get("latest_policy_choke_rung_b_admission"), dict)
        else {}
    )
    add_pin_events(
        latest_admission.get("selection_pin")
        if isinstance(latest_admission.get("selection_pin"), dict)
        else {},
        "latest_policy_choke_rung_b_admission.selection_pin",
    )
    for idx, prior_pin in enumerate(overlay.get("previous_selection_pins") or []):
        if isinstance(prior_pin, dict):
            add_pin_events(prior_pin, f"previous_selection_pins[{idx}]")

    armed_at = selection_pin.get("last_renewed_at") or selection_pin.get("created_at")
    return {
        "armed_at": armed_at,
        "expires_at": selection_pin.get("expires_at"),
        "refused_by": "fable-D26-3/D27-3",
        "supply_rung": supply_rung,
        "record_key_is_legacy_alias": True,
        "record_key_scope": "all_policy_choke_supply_rungs",
        "frontier_candidate_count": candidate_count,
        "frontier_eligible_count": eligible_count,
        "refusal_basis": (
            f"frontier eligible_count = {eligible_count}"
            if eligible_count is not None
            else "UNMEASURED_NO_FRONTIER_EVIDENCE"
        ),
        "refusal_basis_source": (
            "policy_choke.actuator.rung_b_candidate_evidence.eligible_count"
        ),
        "arm_count_utc_day": (
            len(arm_events)
            if arm_events
            else "UNMEASURED_NO_ARM_EVENT_LOG"
        ),
        "arm_count_utc_day_source": (
            "same_utc_day_unique_policy_choke_rung_b_pin_timestamps"
            if arm_events
            else "UNMEASURED_NO_ARM_EVENT_LOG"
        ),
        "arm_events_utc_day": [
            {"armed_at": ts, "event_kind": "ARM", "source": source}
            for ts, source in sorted(arm_events.items())
        ],
        "renew_count_utc_day": (
            len(renew_events)
            if renew_events
            else "UNMEASURED_NO_RENEW_EVENT_LOG"
        ),
        "renew_count_utc_day_source": (
            "same_utc_day_unique_policy_choke_rung_b_last_renewed_timestamps"
            if renew_events
            else "UNMEASURED_NO_RENEW_EVENT_LOG"
        ),
        "renew_events_utc_day": [
            {"renewed_at": ts, "event_kind": "RENEW", "source": source}
            for ts, source in sorted(renew_events.items())
        ],
        "measurement_only": True,
        "live_mutation": False,
}


def _accepted_disposition_outranks_policy_choke(
    disposition: dict[str, Any] | None,
    policy_choke: dict[str, Any],
) -> bool:
    if disposition is None:
        return False
    if disposition.get("status") != "INCIDENT_ORDER_FLOW_DEAD":
        return True
    return bool(
        int(policy_choke.get("selected_eligible_intents") or 0) > 0
        and int(policy_choke.get("selected_guard_submit_attempts") or 0) == 0
    )


def _wallet_policy_disposition_diagnostic(
    *,
    policy_choke: dict[str, Any],
    previous: dict[str, Any],
    selected_candidate: dict[str, Any] | None,
) -> str:
    current = str(policy_choke.get("wallet_policy_diagnostic") or "")
    if (
        selected_candidate is None
        and str(previous.get("status") or "").startswith("INCIDENT_")
        and previous.get("episode_fire_wallet_policy_diagnostic")
        == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
    ):
        return "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
    return current


def _sync_terminal_source_coverage(
    result: dict[str, Any],
    gated_quiet: dict[str, Any],
    reconciliation: dict[str, Any],
) -> None:
    """Make the outer deadman coverage gate use the terminal reconciler."""
    if int(reconciliation.get("input_fresh_source_rows") or 0) <= 0:
        return
    reconciled_full = bool(
        reconciliation.get("source_coverage_full")
        and reconciliation.get("input_equals_terminal_rows")
    )
    checks = gated_quiet.get("checks") if isinstance(gated_quiet.get("checks"), dict) else {}
    checks["source_coverage_full"] = reconciled_full
    gated_quiet["checks"] = checks
    measured_checks = (
        gated_quiet.get("measured_skip_checks")
        if isinstance(gated_quiet.get("measured_skip_checks"), dict)
        else {}
    )
    if measured_checks:
        measured_checks["source_coverage_full"] = reconciled_full
        gated_quiet["measured_skip_checks"] = measured_checks
    gated_quiet["source_coverage"] = {
        "source": "policy_choke.terminal_reconciliation",
        "input_fresh_source_rows": int(reconciliation.get("input_fresh_source_rows") or 0),
        "terminal_rows": int(reconciliation.get("terminal_rows") or 0),
        "source_coverage_full": reconciled_full,
    }
    result["source_coverage_full"] = reconciled_full


def _execute_policy_choke_rung_a(
    *,
    overlay: dict[str, Any],
    target_wallet: str,
    now: datetime,
    ttl_s: int = 1800,
) -> tuple[dict[str, Any], dict[str, Any]]:
    target_wallet = _normalize_wallet(target_wallet)
    member = next(
        (
            row for row in overlay.get("members") or []
            if isinstance(row, dict)
            and _normalize_wallet(row.get("source_wallet") or row.get("wallet")) == target_wallet
        ),
        None,
    )
    if not target_wallet or member is None:
        return overlay, {
            "status": "RUNG_A_NOT_EXECUTED_TARGET_ABSENT_FROM_OVERLAY",
            "target_wallet": target_wallet or None,
        }
    expires = now + timedelta(seconds=max(1, int(ttl_s)))
    pin = {
        "enabled": True,
        "pin_id": "policy-choke-acceptance-share-rung-a",
        "direction_id": "2026-07-20T15:02Z-operator-throughput-deadman",
        "created_at": now.isoformat(),
        "expires_at": expires.isoformat(),
        "candidate_id": member.get("candidate_id"),
        "source_wallet": target_wallet,
        "reason": "POLICY_CHOKE rung A: nonzero weekday own-source acceptance outranks zero incumbent",
    }
    updated = dict(overlay)
    updated["selection_pin"] = pin
    updated["updated_at"] = now.isoformat()
    return updated, {
        "status": "RUNG_A_SELECTION_PIN_WRITTEN",
        "target_wallet": target_wallet,
        "selection_pin": pin,
        "single_submitter_invariant": "run_wallet_copy_live_guard.py remains the only order submitter",
    }


def _fresh_buy_counts(hot_history: dict[str, Any], *, now: datetime, lookback_s: float) -> dict[str, int]:
    since_s = now.timestamp() - lookback_s
    ids: dict[str, set[str]] = defaultdict(set)
    for row in hot_history.get("events") or []:
        if not isinstance(row, dict) or str(row.get("action") or "").upper() != "BUY":
            continue
        wallet = _normalize_wallet(row.get("source_wallet"))
        observed_s = _as_float(row.get("observed_ts") or row.get("event_ts"))
        if not wallet or observed_s is None or not since_s <= observed_s <= now.timestamp():
            continue
        market_slug = str(row.get("market_slug") or row.get("market") or "")
        market_match = re.search(r"(?:btc-updown-5m-|btc-5m-)(\d{10})$", market_slug)
        if market_match is None:
            continue
        market_start_s = int(market_match.group(1))
        if not market_start_s <= now.timestamp() < market_start_s + 300:
            continue
        event_id = str(row.get("event_id") or row.get("source_fingerprint") or "")
        if event_id:
            ids[wallet].add(event_id)
    return {wallet: len(values) for wallet, values in ids.items()}


def _recent_buy_counts(
    hot_history: dict[str, Any], *, now: datetime, lookback_s: float
) -> dict[str, int]:
    """Count identity-clean BTC-5m BUY flow across the full rolling lookback.

    F2 admission intentionally requires the currently tradeable market.  The
    Rung-B early terminal is different: it may fire only when the source has
    gone quiet across the complete lookback, not merely during the instant
    between two active-window observations.
    """
    since_s = now.timestamp() - lookback_s
    ids: dict[str, set[str]] = defaultdict(set)
    for row in hot_history.get("events") or []:
        if not isinstance(row, dict) or str(row.get("action") or "").upper() != "BUY":
            continue
        wallet = _normalize_wallet(row.get("source_wallet"))
        observed_s = _as_float(row.get("observed_ts") or row.get("event_ts"))
        if not wallet or observed_s is None or not since_s <= observed_s <= now.timestamp():
            continue
        market_slug = str(row.get("market_slug") or row.get("market") or "")
        if re.search(r"(?:btc-updown-5m-|btc-5m-)\d{10}$", market_slug) is None:
            continue
        event_id = str(row.get("event_id") or row.get("source_fingerprint") or "")
        if event_id:
            ids[wallet].add(event_id)
    return {wallet: len(values) for wallet, values in ids.items()}


def _union_qualified_pool_stakeout(
    hot_history: dict[str, Any],
    stakeout: dict[str, Any],
    *,
    candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Union identity-clean paper stakeout events into the canonical F2 read surface."""
    prospective = (
        stakeout.get("prospective_current_market")
        if isinstance(stakeout.get("prospective_current_market"), dict)
        else {}
    )
    consumption_gate = (
        prospective.get("actuator_consumption_gate")
        if isinstance(prospective.get("actuator_consumption_gate"), dict)
        else {}
    )
    gate_passed = consumption_gate.get("passed") is True
    candidate_wallet = _normalize_wallet((candidate or {}).get("wallet"))
    candidate_policy = (
        (candidate or {}).get("policy")
        if isinstance((candidate or {}).get("policy"), dict)
        else {}
    )
    candidate_fingerprint = str(
        (candidate or {}).get("wide_policy_fingerprint")
        or candidate_policy.get("wide_policy_fingerprint")
        or ""
    )
    holdouts = (
        consumption_gate.get("exact_policy_chronological_holdout_by_wallet")
        if isinstance(
            consumption_gate.get("exact_policy_chronological_holdout_by_wallet"),
            dict,
        )
        else {}
    )
    wallet_holdouts = (
        holdouts.get(candidate_wallet)
        if isinstance(holdouts.get(candidate_wallet), dict)
        else {}
    )
    candidate_holdout = (
        wallet_holdouts.get(candidate_fingerprint)
        if isinstance(wallet_holdouts.get(candidate_fingerprint), dict)
        else {}
    )
    wallet_counts = (
        consumption_gate.get("wallet_current_market_buy_counts")
        if isinstance(consumption_gate.get("wallet_current_market_buy_counts"), dict)
        else {}
    )
    admission_checks = {
        "identity_market_outcome_parity_violations_zero": (
            int(consumption_gate.get("identity_market_outcome_parity_violations") or 0)
            == 0
        ),
        "unique_fills_gte_100": int(consumption_gate.get("unique_fills") or 0) >= 100,
        "candidate_current_market_buys_gte_10": int(
            wallet_counts.get(candidate_wallet) or 0
        )
        >= 10,
        "candidate_exact_policy_holdout_pass": bool(
            candidate_wallet
            and candidate_fingerprint
            and candidate_holdout.get("passed") is True
            and str(candidate_holdout.get("wide_policy_fingerprint") or "")
            == candidate_fingerprint
        ),
    }
    exact_policy_evidence_status = (
        "NO_EXACT_POLICY_EVIDENCE_CELL"
        if not candidate_holdout
        else "PASS"
        if admission_checks["candidate_exact_policy_holdout_pass"]
        else "EVIDENCE_CELL_FAILED"
    )
    admission_gate_passed = bool(candidate_wallet and all(admission_checks.values()))
    read_gate_passed = admission_gate_passed if candidate is not None else gate_passed
    stakeout_events = [
        row
        for row in prospective.get("identity_clean_events") or []
        if read_gate_passed
        and isinstance(row, dict)
        and row.get("paper_only") is True
        and str(row.get("action") or "").upper() == "BUY"
        and str(row.get("event_id") or "")
        and _normalize_wallet(row.get("source_wallet"))
        and (
            candidate is None
            or _normalize_wallet(row.get("source_wallet")) == candidate_wallet
        )
    ]
    existing = [
        row for row in hot_history.get("events") or [] if isinstance(row, dict)
    ]
    known_ids = {
        str(row.get("event_id") or row.get("source_fingerprint") or "")
        for row in existing
        if str(row.get("event_id") or row.get("source_fingerprint") or "")
    }
    inserted = 0
    for row in stakeout_events:
        event_id = str(row["event_id"])
        if event_id in known_ids:
            continue
        known_ids.add(event_id)
        existing.append(row)
        inserted += 1
    return {
        **hot_history,
        "events": existing,
        "qualified_pool_orderfilled_union": {
            "status": "APPLIED",
            "paper_only_source": True,
            "source_generated_at": stakeout.get("generated_at"),
            "source_pid": stakeout.get("pid"),
            "actuator_consumption_gate_passed": gate_passed,
            "actuator_consumption_gate": consumption_gate,
            "consumer": "admission_read" if candidate is not None else "execution",
            "candidate_wallet": candidate_wallet or None,
            "admission_read_gate": {
                "passed": admission_gate_passed,
                "checks": admission_checks,
                "candidate_wide_policy_fingerprint": candidate_fingerprint or None,
                "candidate_holdout": candidate_holdout,
                "candidate_exact_policy_evidence_status": exact_policy_evidence_status,
                "execution_consumption_gate_unchanged": True,
            },
            "candidate_events": len(stakeout_events),
            "inserted_events": inserted,
            "deduped_events": len(stakeout_events) - inserted,
            "identity_rule": "transaction_hash|log_index",
        },
    }


def _rung_b_regime_evidence(row: dict[str, Any], regime: str) -> dict[str, Any]:
    slice_row = {}
    for container_key in ("regime_slices", "slice_labels", "temporal_slices"):
        container = row.get(container_key) if isinstance(row.get(container_key), dict) else {}
        if isinstance(container.get(regime), dict):
            slice_row = container[regime]
            break
    temporal = row.get("temporal_evidence") if isinstance(row.get("temporal_evidence"), dict) else {}
    matched_slice = temporal.get("matched_slice") if isinstance(temporal.get("matched_slice"), dict) else {}
    if not slice_row and str(matched_slice.get("slice") or "") == regime:
        slice_row = matched_slice
    pnl = _as_float(slice_row.get("gross_pnl_usd") or slice_row.get("pnl_usd"))
    roi = _as_float(slice_row.get("gross_roi_pct") or slice_row.get("roi_pct"))
    resolved = int(slice_row.get("resolved_signals") or slice_row.get("resolved_trades") or 0)
    source = f"explicit_{regime}_slice"
    if not slice_row:
        pnl = _as_float(row.get("retrospective_gross_pnl_usd") or row.get("feed_baseline_gross_pnl_usd"))
        roi = _as_float(row.get("retrospective_gross_roi_pct"))
        resolved = int(row.get("retrospective_resolved_signals") or row.get("feed_baseline_resolved_signals") or 0)
        source = "retrospective_regime_cell"
    return {"pnl_usd": pnl, "roi_pct": roi, "resolved_signals": resolved, "source": source}


def _temporal_active_slice(
    temporal_registry: dict[str, Any] | None,
    *,
    wallet: str,
    regime: str,
    slice_name: str | None = None,
) -> dict[str, Any]:
    active_slice = str(slice_name or regime)
    raw_wallets = (temporal_registry or {}).get("wallets") or []
    if isinstance(raw_wallets, dict):
        temporal_rows = [
            {**row, "wallet": row.get("wallet") or wallet}
            for wallet, row in raw_wallets.items()
            if isinstance(row, dict)
        ]
    else:
        temporal_rows = raw_wallets
    for row in temporal_rows:
        if not isinstance(row, dict):
            continue
        if _normalize_wallet(row.get("wallet") or row.get("source_wallet")) != wallet:
            continue
        # Match the live guard: venue-executable evidence is authoritative.
        slices = (
            row.get("venue_slice_labels")
            if isinstance(row.get("venue_slice_labels"), dict)
            else row.get("slice_labels")
            if isinstance(row.get("slice_labels"), dict)
            else {}
        )
        active = (
            slices.get(active_slice)
            if isinstance(slices.get(active_slice), dict)
            else {}
        )
        return {
            "wallet": wallet,
            "regime": regime,
            "slice": active_slice,
            "classification": row.get("classification"),
            "label": str(active.get("label") or "").upper(),
            "resolved_trades": int(active.get("resolved_trades") or 0),
            "roi_pct": _as_float(active.get("roi_pct")),
            "pnl_usd": _as_float(active.get("pnl_usd")),
            "reason": active.get("reason"),
            "source": DEFAULT_TEMPORAL,
        }
    return {
        "wallet": wallet,
        "regime": regime,
        "slice": active_slice,
        "label": "MISSING",
        "source": DEFAULT_TEMPORAL,
    }


def _temporal_active_slices(
    temporal_registry: dict[str, Any] | None,
    *,
    wallet: str,
    regime: str,
    now: datetime,
) -> list[dict[str, Any]]:
    slice_names = [regime]
    now_utc = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    if 18 <= now_utc.astimezone(timezone.utc).hour < 22:
        slice_names.append("dead_band_18_22_utc")
    return [
        _temporal_active_slice(
            temporal_registry,
            wallet=wallet,
            regime=regime,
            slice_name=slice_name,
        )
        for slice_name in slice_names
    ]


def _temporal_min_trades(temporal_registry: dict[str, Any] | None) -> int:
    criteria = (
        temporal_registry.get("criteria")
        if isinstance(temporal_registry, dict)
        and isinstance(temporal_registry.get("criteria"), dict)
        else {}
    )
    return max(1, int(criteria.get("min_trades") or 5))


def _f1_slice_disclosure(
    *,
    active_slices: list[dict[str, Any]],
    regime_evidence: dict[str, Any],
    checks: dict[str, bool],
) -> dict[str, Any]:
    """Disclose F1's actual basis and independently admissible active slices."""

    basis = {
        "slice": "full_stream",
        "source": regime_evidence.get("source"),
        "regime_sliced": regime_evidence.get("regime_sliced") is True,
        "resolved_signals": int(regime_evidence.get("resolved_signals") or 0),
    }
    admissible: list[dict[str, Any]] = []
    for row in active_slices:
        slice_checks = {
            "resolved_trades_gte_200": int(row.get("resolved_trades") or 0) >= 200,
            "positive_pnl_and_roi": bool(
                _as_float(row.get("pnl_usd")) is not None
                and float(row["pnl_usd"]) > 0
                and _as_float(row.get("roi_pct")) is not None
                and float(row["roi_pct"]) > 0
            ),
            "both_resolved_halves_positive": (
                checks.get("both_resolved_halves_positive") is True
            ),
            "venue_reachable_admissible": (
                checks.get("f1_venue_reachable_admissible") is True
            ),
            "concentration_admissible": (
                checks.get("f1_concentration_admissible") is True
            ),
        }
        if all(slice_checks.values()):
            admissible.append(
                {
                    "slice": row.get("slice"),
                    "resolved_trades": int(row.get("resolved_trades") or 0),
                    "pnl_usd": _as_float(row.get("pnl_usd")),
                    "roi_pct": _as_float(row.get("roi_pct")),
                    "checks": slice_checks,
                }
            )
    return {
        "f1_slice_basis": basis,
        "admissible_slices": admissible,
    }


def _stable_checksum(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _wide_direct_generation_snapshot(
    wide: dict[str, Any],
    *,
    now: datetime,
    lookback_s: float = POLICY_CHOKE_LOOKBACK_S,
    max_packet_age_s: float = 30.0,
) -> dict[str, Any]:
    """Build an identity-clean current source table from the direct WIDE handoff."""
    updated_at = _parse_ts(wide.get("updated_at") or wide.get("generated_at"))
    packet_age_s = (now - updated_at).total_seconds() if updated_at else None
    reconciliation = (
        wide.get("terminal_reconciliation")
        if isinstance(wide.get("terminal_reconciliation"), dict)
        else {}
    )
    input_rows = int(reconciliation.get("input_rows") or 0)
    terminal_rows = int(reconciliation.get("terminal_rows") or 0)
    input_equals_terminal = bool(
        reconciliation.get("input_equals_terminal") is True
        and input_rows == terminal_rows
    )
    direct_event_handoff = reconciliation.get("direct_event_handoff") is True
    since = now - timedelta(seconds=lookback_s)
    per_wallet: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "attempts": 0,
            "copyable": 0,
            "policy_depth_pass": 0,
            "latest_receipt_at": None,
            "attempt_ids": [],
            "copyable_order_ids": [],
        }
    )
    seen_attempts: set[str] = set()
    for row in wide.get("attempt_terminals") or []:
        if not isinstance(row, dict):
            continue
        recorded_at = _parse_ts(row.get("recorded_at"))
        wallet = _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
        attempt_id = str(row.get("attempt_id") or row.get("source_event_id") or "")
        if (
            not wallet
            or not attempt_id
            or attempt_id in seen_attempts
            or recorded_at is None
            or recorded_at < since
            or recorded_at > now
        ):
            continue
        seen_attempts.add(attempt_id)
        target = per_wallet[wallet]
        target["attempts"] += 1
        if len(target["attempt_ids"]) < 20:
            target["attempt_ids"].append(attempt_id)
        previous = _parse_ts(target.get("latest_receipt_at"))
        if previous is None or recorded_at > previous:
            target["latest_receipt_at"] = recorded_at.isoformat()

    seen_orders: set[str] = set()
    for row in wide.get("orders") or []:
        if not isinstance(row, dict):
            continue
        recorded_at = _parse_ts(row.get("recorded_at"))
        wallet = _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
        order_id = str(row.get("order_id") or "")
        terminal = (
            row.get("f1_f4_terminal")
            if isinstance(row.get("f1_f4_terminal"), dict)
            else {}
        )
        if (
            not wallet
            or not order_id
            or order_id in seen_orders
            or recorded_at is None
            or recorded_at < since
            or recorded_at > now
            or terminal.get("terminal") != "COPYABLE_EXACT_POLICY_PAPER_FILL"
        ):
            continue
        seen_orders.add(order_id)
        target = per_wallet[wallet]
        target["copyable"] += 1
        if terminal.get("F4_executable_book") == "PASS":
            target["policy_depth_pass"] += 1
        if len(target["copyable_order_ids"]) < 20:
            target["copyable_order_ids"].append(order_id)

    manifest = wide.get("manifest") if isinstance(wide.get("manifest"), dict) else {}
    source_alpha_path = str(manifest.get("source_alpha_report") or "")
    source_alpha = load_json(source_alpha_path, default={}) if source_alpha_path else {}
    source_alpha_updated_at = _parse_ts(source_alpha.get("updated_at"))
    source_alpha_age_h = (
        max(0.0, (now - source_alpha_updated_at).total_seconds()) / 3600.0
        if source_alpha_updated_at is not None
        else None
    )
    alpha_binding = {
        "source_alpha_report": source_alpha_path or None,
        "source_alpha_updated_at": (
            source_alpha_updated_at.isoformat()
            if source_alpha_updated_at is not None
            else None
        ),
        "source_alpha_age_h": (
            round(source_alpha_age_h, 6)
            if source_alpha_age_h is not None
            else None
        ),
        "max_age_h": 24.0,
        "status": (
            "PASS"
            if source_alpha_age_h is not None and source_alpha_age_h <= 24.0
            else "STALE_ALPHA_REPORT_REFUSED"
            if source_alpha_age_h is not None
            else "ALPHA_REPORT_AGE_MISSING_REFUSED"
        ),
    }
    cohort = wide.get("cohort") if isinstance(wide.get("cohort"), dict) else {}
    identity = {
        "manifest_id": manifest.get("manifest_id"),
        "cohort_id": cohort.get("cohort_id"),
        "run_id": cohort.get("run_id"),
        "policy_id": wide.get("policy_id"),
        "wallets": sorted(per_wallet),
    }
    # The direct writer can atomically replace the packet between this process's
    # `now` sample and file read. Tolerate only that bounded write-race skew.
    fresh = bool(
        packet_age_s is not None and -5.0 <= packet_age_s <= max_packet_age_s
    )
    current_attempts = sum(int(row["attempts"]) for row in per_wallet.values())
    current_copyables = sum(int(row["copyable"]) for row in per_wallet.values())
    ready = bool(
        fresh
        and direct_event_handoff
        and input_equals_terminal
    )
    return {
        "status": "PASS" if ready else "FAIL",
        "ready": ready,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "packet_age_s": packet_age_s,
        "effective_packet_age_s": max(0.0, packet_age_s) if packet_age_s is not None else None,
        "max_packet_age_s": max_packet_age_s,
        "lookback_s": lookback_s,
        "direct_event_handoff": direct_event_handoff,
        "input_rows": input_rows,
        "terminal_rows": terminal_rows,
        "input_equals_terminal": input_equals_terminal,
        "current_attempted_buy_rows": current_attempts,
        "current_copyable_rows": current_copyables,
        "per_wallet": dict(sorted(per_wallet.items())),
        "identity": identity,
        "alpha_binding": alpha_binding,
        "checksum": _stable_checksum(identity),
        "rule": (
            "packet age <=30s, direct_event_handoff, and exact input=terminal; "
            "the journal-inclusive outer snapshot enforces nonzero attempts"
        ),
    }


def _wide_direct_source_snapshot(
    wide: dict[str, Any],
    *,
    now: datetime,
    lookback_s: float = POLICY_CHOKE_LOOKBACK_S,
    max_packet_age_s: float = 30.0,
    journal: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read exact generations, dedupe row identities, and expire by row receipt."""
    latest = _wide_direct_generation_snapshot(
        wide, now=now, lookback_s=lookback_s, max_packet_age_s=max_packet_age_s
    )
    envelopes = list(journal or [])
    current = envelope_from_packet(wide)
    if current:
        envelopes.append(current)
    supply_continuity = _direct_supply_continuity(
        envelopes,
        now=now,
        window_s=lookback_s,
        window_count=4,
        required_windows=3,
    )
    generation_flow_summary = _direct_generation_flow_summary(envelopes)
    since = now - timedelta(seconds=lookback_s)
    generations: dict[str, dict[str, Any]] = {}
    seen_rows: set[str] = set()
    seen_copyable_orders: set[str] = set()
    seen_envelope_copyable_orders: set[str] = set()
    envelope_copyable_orders_in_window = 0
    terminal_taxonomy: Counter[str] = Counter()
    for envelope in envelopes:
        if not isinstance(envelope, dict) or envelope.get("input_equals_terminal") is not True:
            continue
        rows = [row for row in envelope.get("rows") or [] if isinstance(row, dict)]
        if int(envelope.get("input_rows") or 0) != int(envelope.get("terminal_rows") or 0):
            continue
        delta_encoding = (
            envelope.get("delta_encoding")
            if isinstance(envelope.get("delta_encoding"), dict)
            else {}
        )
        delta_encoded = delta_encoding.get("cumulative_snapshot") is True
        expected_physical_rows = (
            int(delta_encoding.get("delta_rows") or 0)
            if delta_encoded
            else int(envelope.get("input_rows") or 0)
        )
        if len(rows) != expected_physical_rows:
            continue
        generation = str(envelope.get("source_generation") or "")
        identity = envelope.get("identity") if isinstance(envelope.get("identity"), dict) else {}
        if not generation:
            continue
        target = generations.setdefault(
            generation,
            {
                "source_generation": generation,
                "identity": identity,
                "input_rows": int(envelope.get("input_rows") or 0),
                "terminal_rows": int(envelope.get("terminal_rows") or 0),
                "per_wallet": defaultdict(
                    lambda: {
                        "attempts": 0,
                        "copyable": 0,
                        "policy_depth_pass": 0,
                        "latest_receipt_at": None,
                        "attempt_ids": [],
                        "copyable_order_ids": [],
                        "terminal_taxonomy": Counter(),
                    }
                ),
                "terminal_taxonomy": Counter(),
            },
        )
        target["input_rows"] = max(
            int(target.get("input_rows") or 0),
            int(envelope.get("input_rows") or 0),
        )
        target["terminal_rows"] = max(
            int(target.get("terminal_rows") or 0),
            int(envelope.get("terminal_rows") or 0),
        )
        for row in rows:
            recorded_at = _parse_ts(row.get("recorded_at"))
            wallet = _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
            terminal_for_identity = row.get("f1_f4_terminal") if isinstance(row.get("f1_f4_terminal"), dict) else {}
            tx_hash = str(row.get("transaction_hash") or "")
            log_index = str(row.get("log_index") or row.get("source_event_id") or "")
            event_identity = (
                {"transaction_hash": tx_hash, "log_index": log_index, "wallet": wallet}
                if tx_hash and log_index
                else {"attempt_id": row.get("attempt_id"), "order_id": row.get("order_id"), "source_event_id": row.get("source_event_id"), "wallet": wallet}
            )
            row_identity = _stable_checksum({"generation": generation, "event_identity": event_identity, "terminal": terminal_for_identity.get("terminal")})
            if not wallet or recorded_at is None or recorded_at < since or recorded_at > now or row_identity in seen_rows:
                continue
            seen_rows.add(row_identity)
            wallet_row = target["per_wallet"][wallet]
            wallet_row["attempts"] += 1
            attempt_id = str(row.get("attempt_id") or row.get("order_id") or row.get("source_event_id") or "")
            if attempt_id and len(wallet_row["attempt_ids"]) < 20:
                wallet_row["attempt_ids"].append(attempt_id)
            previous = _parse_ts(wallet_row.get("latest_receipt_at"))
            if previous is None or recorded_at > previous:
                wallet_row["latest_receipt_at"] = recorded_at.isoformat()
            terminal = row.get("f1_f4_terminal") if isinstance(row.get("f1_f4_terminal"), dict) else {}
            terminal_name = str(terminal.get("terminal") or "MISSING_TERMINAL")
            wallet_row["terminal_taxonomy"][terminal_name] += 1
            target["terminal_taxonomy"][terminal_name] += 1
            terminal_taxonomy[terminal_name] += 1
            if terminal_name == "COPYABLE_EXACT_POLICY_PAPER_FILL":
                order_identity = f"{generation}|{str(row.get('order_id') or row_identity)}"
                if order_identity in seen_copyable_orders:
                    continue
                seen_copyable_orders.add(order_identity)
                wallet_row["copyable"] += 1
                if terminal.get("F4_executable_book") == "PASS":
                    wallet_row["policy_depth_pass"] += 1
                order_id = str(row.get("order_id") or "")
                if order_id and len(wallet_row["copyable_order_ids"]) < 20:
                    wallet_row["copyable_order_ids"].append(order_id)
        for order in envelope.get("copyable_orders") or []:
            if not isinstance(order, dict):
                continue
            recorded_at = _parse_ts(order.get("recorded_at"))
            terminal = (
                order.get("f1_f4_terminal")
                if isinstance(order.get("f1_f4_terminal"), dict)
                else {}
            )
            if (
                recorded_at is None
                or recorded_at < since
                or recorded_at > now
                or terminal.get("terminal")
                != "COPYABLE_EXACT_POLICY_PAPER_FILL"
            ):
                continue
            order_id = str(order.get("order_id") or "")
            witness_identity = (
                f"{generation}|{order_id}"
                if order_id
                else _stable_checksum(
                    {
                        "generation": generation,
                        "wallet": _normalize_wallet(
                            order.get("wallet") or order.get("source_wallet")
                        ),
                        "transaction_hash": order.get("transaction_hash"),
                        "log_index": order.get("log_index"),
                        "token_id": order.get("token_id"),
                    }
                )
            )
            if witness_identity in seen_envelope_copyable_orders:
                continue
            seen_envelope_copyable_orders.add(witness_identity)
            envelope_copyable_orders_in_window += 1
    per_wallet_generation: dict[str, dict[str, Any]] = {}
    per_wallet: dict[str, dict[str, Any]] = {}
    generation_rows = []
    for generation, row in sorted(generations.items()):
        for wallet_row in row["per_wallet"].values():
            wallet_row["terminal_taxonomy"] = dict(
                sorted(wallet_row["terminal_taxonomy"].items())
            )
        row["per_wallet"] = dict(sorted(row["per_wallet"].items()))
        row["terminal_taxonomy"] = dict(sorted(row["terminal_taxonomy"].items()))
        row["current_attempted_buy_rows"] = sum(value["attempts"] for value in row["per_wallet"].values())
        row["current_copyable_rows"] = sum(value["copyable"] for value in row["per_wallet"].values())
        receipts = [
            parsed
            for value in row["per_wallet"].values()
            if (parsed := _parse_ts(value.get("latest_receipt_at"))) is not None
        ]
        row["latest_receipt_at"] = max(receipts).isoformat() if receipts else None
        generation_rows.append(row)
        policy_id = str(row["identity"].get("policy_id") or "")
        for wallet, wallet_row in row["per_wallet"].items():
            key = f"{wallet}|{policy_id}|{generation}"
            enriched = {**wallet_row, "wallet": wallet, "policy_id": policy_id, "source_generation": generation, "generation_identity": row["identity"]}
            per_wallet_generation[key] = enriched
            incumbent = per_wallet.get(wallet)
            if incumbent is None or (enriched["copyable"], enriched["attempts"], generation) > (incumbent["copyable"], incumbent["attempts"], incumbent["source_generation"]):
                per_wallet[wallet] = enriched
    attempts = sum(row["current_attempted_buy_rows"] for row in generation_rows)
    copyables = sum(row["current_copyable_rows"] for row in generation_rows)
    copyable_terminal_rows = int(
        terminal_taxonomy.get("COPYABLE_EXACT_POLICY_PAPER_FILL") or 0
    )
    copyable_parity = copyables == copyable_terminal_rows
    copyable_cross_source_parity = (
        copyables == envelope_copyable_orders_in_window
    )
    ready = bool(latest.get("ready") and attempts > 0 and copyable_parity)
    non_empty_generations = [
        (index, row)
        for index, row in enumerate(generation_rows)
        if row["per_wallet"] and _parse_ts(row.get("latest_receipt_at")) is not None
    ]
    latest_non_empty = max(
        non_empty_generations,
        key=lambda item: _parse_ts(item[1]["latest_receipt_at"]),
        default=None,
    )
    latest_non_empty_receipt_at = (
        latest_non_empty[1]["latest_receipt_at"]
        if latest_non_empty is not None
        else None
    )
    return {
        **latest,
        "status": (
            "PASS"
            if ready
            else "FAIL_COPYABLE_PARITY"
            if not copyable_parity
            else "FAIL"
        ),
        "ready": ready,
        "current_attempted_buy_rows": attempts,
        "current_copyable_rows": copyables,
        "fresh_rows": attempts,
        "copyable": copyables,
        "latest_receipt_at": latest_non_empty_receipt_at,
        "direct_ready": ready,
        "copyable_terminal_rows": copyable_terminal_rows,
        "copyable_parity": copyable_parity,
        "envelope_copyable_orders_in_window": (
            envelope_copyable_orders_in_window
        ),
        "copyable_cross_source_parity": copyable_cross_source_parity,
        "per_wallet": dict(sorted(per_wallet.items())),
        "per_wallet_generation": dict(sorted(per_wallet_generation.items())),
        "generations": generation_rows,
        "generation_count": len(generation_rows),
        "latest_non_empty_generation_index": (
            latest_non_empty[0] if latest_non_empty is not None else None
        ),
        "latest_non_empty_generation_receipt_at": (
            latest_non_empty_receipt_at
        ),
        "terminal_taxonomy": dict(sorted(terminal_taxonomy.items())),
        "supply_continuity": supply_continuity,
        "generation_flow_summary": generation_flow_summary,
        "checksum": _stable_checksum([{"source_generation": row["source_generation"], "identity": row["identity"]} for row in generation_rows]),
        "rule": (
            "latest packet <=30s and exact; journal generations reconcile "
            "independently; copyables come only from reconciled terminal rows; "
            "sum(per_wallet.copyable) must equal terminal taxonomy COPYABLE; "
            "rows dedupe by immutable identity and expire only by recorded_at at 1800s"
        ),
    }


def _direct_supply_continuity(
    envelopes: list[dict[str, Any]],
    *,
    now: datetime,
    window_s: float,
    window_count: int = 4,
    required_windows: int = 3,
) -> dict[str, Any]:
    """Measure wallet supply presence across consecutive, non-overlapping windows."""

    per_wallet: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    seen_copyables: set[str] = set()
    for envelope in envelopes:
        if not isinstance(envelope, dict) or envelope.get("input_equals_terminal") is not True:
            continue
        generation = str(envelope.get("source_generation") or "")
        for row in envelope.get("rows") or []:
            if not isinstance(row, dict):
                continue
            wallet = _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
            recorded_at = _parse_ts(row.get("recorded_at"))
            if not wallet or recorded_at is None or recorded_at > now:
                continue
            age_s = (now - recorded_at).total_seconds()
            window_index = int(age_s // window_s) if window_s > 0 else window_count
            if window_index < 0 or window_index >= window_count:
                continue
            identity = _stable_checksum(
                {
                    "generation": generation,
                    "transaction_hash": row.get("transaction_hash"),
                    "log_index": row.get("log_index"),
                    "attempt_id": row.get("attempt_id"),
                    "order_id": row.get("order_id"),
                    "source_event_id": row.get("source_event_id"),
                    "wallet": wallet,
                }
            )
            if identity in seen:
                continue
            seen.add(identity)
            target = per_wallet.setdefault(
                wallet,
                {
                    "attempts_by_window": [0] * window_count,
                    "copyables_by_window": [0] * window_count,
                },
            )
            target["attempts_by_window"][window_index] += 1
        for order in envelope.get("copyable_orders") or []:
            if not isinstance(order, dict):
                continue
            wallet = _normalize_wallet(
                order.get("wallet") or order.get("source_wallet")
            )
            recorded_at = _parse_ts(order.get("recorded_at"))
            if not wallet or recorded_at is None or recorded_at > now:
                continue
            age_s = (now - recorded_at).total_seconds()
            window_index = int(age_s // window_s) if window_s > 0 else window_count
            if window_index < 0 or window_index >= window_count:
                continue
            identity = _stable_checksum(
                {
                    "generation": generation,
                    "order_id": order.get("order_id"),
                    "transaction_hash": order.get("transaction_hash"),
                    "log_index": order.get("log_index"),
                    "wallet": wallet,
                }
            )
            if identity in seen_copyables:
                continue
            seen_copyables.add(identity)
            target = per_wallet.setdefault(
                wallet,
                {
                    "attempts_by_window": [0] * window_count,
                    "copyables_by_window": [0] * window_count,
                },
            )
            target["copyables_by_window"][window_index] += 1
    for row in per_wallet.values():
        attempt_windows_present = sum(
            1 for value in row["attempts_by_window"] if value > 0
        )
        copyable_windows_present = sum(
            1 for value in row["copyables_by_window"] if value > 0
        )
        row["attempt_windows_present"] = attempt_windows_present
        row["copyable_windows_present"] = copyable_windows_present
        row["required_windows"] = required_windows
        row["window_count"] = window_count
        row["attempt_pass"] = attempt_windows_present >= required_windows
        row["copyable_pass"] = copyable_windows_present >= required_windows
    return {
        "window_s": window_s,
        "window_count": window_count,
        "required_windows": required_windows,
        "per_wallet": dict(sorted(per_wallet.items())),
        "attempt_passing_wallet_count": sum(
            1 for row in per_wallet.values() if row["attempt_pass"]
        ),
        "copyable_passing_wallet_count": sum(
            1 for row in per_wallet.values() if row["copyable_pass"]
        ),
        "measured_wallet_count": len(per_wallet),
        "rule": (
            "copyable_orders presence in >=3/4 windows is the primary ranking "
            "counter; attempt presence is secondary; neither counter changes F1-F4"
        ),
    }


def _direct_generation_flow_summary(
    envelopes: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare legacy and instrumented WIDE generation yield without changing bars."""

    def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
        captured = [
            parsed
            for row in rows
            if (parsed := _parse_ts(row.get("captured_at"))) is not None
        ]
        span_h = (
            max(0.0, (max(captured) - min(captured)).total_seconds()) / 3600.0
            if len(captured) >= 2
            else None
        )
        copyable_ids = {
            _stable_checksum(
                {
                    "source_generation": row.get("source_generation"),
                    "order_id": order.get("order_id"),
                    "transaction_hash": order.get("transaction_hash"),
                    "log_index": order.get("log_index"),
                    "wallet": order.get("wallet") or order.get("source_wallet"),
                }
            )
            for row in rows
            for order in row.get("copyable_orders") or []
            if isinstance(order, dict)
        }
        copyables = len(copyable_ids)
        empty = sum(int(row.get("input_rows") or 0) == 0 for row in rows)
        return {
            "generations": len(rows),
            "empty_generations": empty,
            "empty_generation_rate_pct": (
                round(100.0 * empty / len(rows), 6) if rows else None
            ),
            "copyable_orders": copyables,
            "observed_span_h": round(span_h, 6) if span_h is not None else None,
            "copyables_per_hour": (
                round(copyables / span_h, 6)
                if span_h is not None and span_h > 0
                else None
            ),
        }

    by_generation_cut: dict[tuple[str, int], dict[str, Any]] = {}
    for row in envelopes:
        if not isinstance(row, dict) or row.get("kind") != "wide_direct_handoff_generation":
            continue
        by_generation_cut[
            (str(row.get("source_generation") or ""), int(row.get("input_rows") or 0))
        ] = row
    rows = list(by_generation_cut.values())
    latest_captured = max(
        (
            parsed
            for row in rows
            if (parsed := _parse_ts(row.get("captured_at"))) is not None
        ),
        default=None,
    )
    comparison_lookback_h = 6.0
    if latest_captured is not None:
        comparison_floor = latest_captured - timedelta(hours=comparison_lookback_h)
        rows = [
            row
            for row in rows
            if (captured := _parse_ts(row.get("captured_at"))) is not None
            and captured >= comparison_floor
        ]
    legacy = [
        row for row in rows if not isinstance(row.get("generation_flow"), dict)
    ]
    instrumented = [
        row for row in rows if isinstance(row.get("generation_flow"), dict)
    ]
    empty_stage_counts: Counter[str] = Counter(
        str((row.get("generation_flow") or {}).get("empty_stage") or "NONEMPTY")
        for row in instrumented
    )
    return {
        "status": (
            "MEASURING_AFTER_INSTRUMENTATION"
            if instrumented
            else "WAIT_FIRST_INSTRUMENTED_GENERATION"
        ),
        "pre_instrumentation": summarize(legacy),
        "instrumented_after": summarize(instrumented),
        "instrumented_empty_stage_counts": dict(sorted(empty_stage_counts.items())),
        "comparison_lookback_h": comparison_lookback_h,
        "quality_bars_unchanged": True,
        "rule": (
            "compare empty-generation rate and copyables/hour before and after "
            "fetch/filter/dedupe/terminal-join instrumentation"
        ),
    }


def _evidenced_park_exclusions(*, now: datetime) -> dict[str, dict[str, Any]]:
    registry = load_json(DEFAULT_STANDBY_PARK_REGISTRY, default={})
    rows = registry.get("exclusions") if isinstance(registry, dict) else []
    active: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("wallet"))
        expires_at = _parse_ts(row.get("expires_at"))
        permanent = row.get("permanent") is True
        if not wallet or (not permanent and (expires_at is None or expires_at <= now)):
            continue
        basis = row.get("basis") if isinstance(row.get("basis"), dict) else {}
        basis_path = str(basis.get("artifact_path") or "")
        basis_checksum = str(basis.get("sha256") or "")
        observed_checksum = ""
        if basis_path and Path(basis_path).exists():
            observed_checksum = hashlib.sha256(Path(basis_path).read_bytes()).hexdigest()
        active[wallet] = {
            "status": "EVIDENCED_PERMANENT_PARK" if permanent else "EVIDENCED_EXPIRING_PARK",
            "reason": "permanent_park" if permanent else "expiring_park",
            "permanent_park": permanent,
            "evidenced_park": True,
            "decided_at": row.get("decided_at"),
            "decided_by": row.get("decided_by"),
            "expires_at": row.get("expires_at"),
            "supersedes": row.get("supersedes"),
            "park_basis": basis,
            "park_basis_status": (
                "PASS" if basis_checksum and observed_checksum == basis_checksum
                else "BASIS_CHECKSUM_MISMATCH_FAIL_CLOSED"
            ),
        }
    return active


def _standby_wallet_exclusions(readiness: dict[str, Any], *, now: datetime) -> dict[str, dict[str, Any]]:
    standby = readiness.get("standby_ready") if isinstance(readiness.get("standby_ready"), dict) else {}
    exclusions: dict[str, dict[str, Any]] = {}
    generated_at = _parse_ts(readiness.get("generated_at"))
    source_age_s = max(0.0, (now - generated_at).total_seconds()) if generated_at else None
    source_stale = source_age_s is None or source_age_s > 1800.0
    for lane_name, row in standby.items():
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
        if not wallet:
            continue
        status = str(row.get("status") or "").upper()
        terminal = str(row.get("terminal_decision") or "")
        binding = row.get("binding") if isinstance(row.get("binding"), dict) else {}
        stop_writer = bool(
            row.get("stop_writer")
            or binding.get("stop_writer")
            or (binding.get("terminal_outcome_on_deadline") or {}).get("stop_writer")
        )
        status_excluded = any(token in status for token in ("CANNOT_MATURE", "PARK", "RED", "DIVERGENT"))
        recognized_clear = status in {"PASS", "READY", "ACCRUING", "GREEN", ""}
        default_denied = bool(status and not status_excluded and not recognized_clear)
        red_clock = "RED" in status or "DIVERGENT" in status
        terminal_park = bool(terminal or "PARK" in status or "CANNOT_MATURE" in status or stop_writer)
        if terminal_park or red_clock or default_denied or source_stale:
            exclusions[wallet] = {
                "lane": lane_name,
                "status": status,
                "terminal_decision": terminal or None,
                "red_clock": red_clock,
                "stop_writer": stop_writer,
                "standby_exclusion_source_age_s": source_age_s,
                "standby_exclusion_source_status": (
                    "UNRECONCILED_STALE_EXCLUSION_SOURCE" if source_stale else "FRESH"
                ),
                "reason": (
                    "terminal_park"
                    if terminal_park
                    else "immutable_red_clock"
                    if red_clock
                    else "unrecognised_status_default_deny"
                    if default_denied
                    else "stale_exclusion_source"
                ),
            }
    for wallet, park in _evidenced_park_exclusions(now=now).items():
        exclusions[wallet] = {
            **exclusions.get(wallet, {}),
            **park,
            "standby_exclusion_source_age_s": source_age_s,
            "standby_exclusion_source_status": (
                "UNRECONCILED_STALE_EXCLUSION_SOURCE" if source_stale else "FRESH"
            ),
        }
    return exclusions


def _park_provenance(exclusion: dict[str, Any] | None) -> dict[str, Any] | None:
    """Name why a park-like refusal exists without changing its grade."""

    if not exclusion:
        return None
    status = str(exclusion.get("status") or "")
    terminal = str(exclusion.get("terminal_decision") or "")
    basis = exclusion.get("park_basis") if isinstance(exclusion.get("park_basis"), dict) else {}
    basis_path = str(basis.get("artifact_path") or "")
    source_payload = load_json(basis_path, default={}) if basis_path else {}
    source_payload = source_payload if isinstance(source_payload, dict) else {}
    upper = "|".join(
        (
            status,
            terminal,
            str(exclusion.get("reason") or ""),
            str(basis.get("replacement_reason") or ""),
            str(source_payload.get("vacated_reason") or ""),
            str(source_payload.get("resolved_at_deadline_reason") or ""),
        )
    ).upper()
    measurement = basis.get("observed_marginal_post_fee_usd_per_fill")
    if measurement is None:
        measurement = source_payload.get("observed_marginal_post_fee_usd_per_fill")
    if measurement is None:
        measurement = exclusion.get("post_fee_pnl_usd")
    try:
        measurement = float(measurement) if measurement is not None else None
    except (TypeError, ValueError):
        measurement = None
    if "OPERATOR" in str(exclusion.get("decided_by") or "").upper():
        park_basis = "OPERATOR"
        reason_string = terminal or status or str(exclusion.get("reason") or "operator_park")
        source_record = str(basis_path or DEFAULT_STANDBY_PARK_REGISTRY)
        measurement = None
    elif measurement is not None and measurement < 0.0:
        park_basis = "MEASURED_NEGATIVE"
        reason_string = f"OBSERVED_MARGINAL_POST_FEE_USD_PER_FILL={measurement:.6f}"
        source_record = str(
            basis_path
            or f"{DEFAULT_STANDBY_READINESS}#standby_ready.{exclusion.get('lane') or 'unknown'}"
        )
    elif "UNFED_CLOCK" in upper or "CANNOT_MATURE" in upper:
        park_basis = "EVIDENCE_ABSENCE"
        reason_string = "UNFED_CLOCK_CANNOT_MATURE"
        source_record = (
            "data/research/82c8_wide_standby_binding_latest.json"
            "#binding.terminal_outcome_on_deadline"
        )
        measurement = None
    else:
        park_basis = "UNCLASSIFIED"
        reason_string = (
            terminal
            or str(basis.get("replacement_reason") or "")
            or status
            or str(exclusion.get("reason") or "unclassified_park")
        )
        source_record = str(
            basis.get("artifact_path")
            or f"{DEFAULT_STANDBY_READINESS}#standby_ready.{exclusion.get('lane') or 'unknown'}"
        )
    return {
        "park_basis": park_basis,
        "source_record": source_record,
        "reason_string": reason_string,
        "measurement": measurement,
    }


def _fingerprint_direct_supersedes_volume_park(
    *,
    wallet: str,
    lane: dict[str, Any],
    evidence: dict[str, Any],
    exclusion: dict[str, Any] | None,
) -> bool:
    """Keep a volume-seat park from leaking into an exact WIDE DIRECT lane."""
    evidenced_park = bool(
        (exclusion or {}).get("evidenced_park") is True
        or wallet in _evidenced_park_exclusions(now=datetime.now(timezone.utc))
    )
    return bool(
        exclusion
        and not evidenced_park
        and str(exclusion.get("lane") or "").lower() == "volume"
        and exclusion.get("red_clock") is not True
        and str(exclusion.get("terminal_decision") or "")
        == "PARK_VOLUME_STANDBY_PAPER_ONLY"
        and lane.get("wide_policy_fingerprint")
        and evidence.get("resolved_signals", 0) >= 200
        and evidence.get("pnl_usd") is not None
        and evidence["pnl_usd"] > 0
        and evidence.get("roi_pct") is not None
        and evidence["roi_pct"] > 0
    )


def _source_roster_drought_incident(
    *,
    can_trade: bool,
    accepted_order_idle_s: float | None,
    runtime_fresh_rows: int,
    direct_source: dict[str, Any],
    candidates: dict[str, Any],
    max_idle_s: float = 1800.0,
    live_build_authorized: bool = False,
) -> dict[str, Any]:
    firing = bool(
        (can_trade or live_build_authorized)
        and accepted_order_idle_s is not None
        and accepted_order_idle_s >= max_idle_s
        and runtime_fresh_rows == 0
        and direct_source.get("ready") is True
    )
    selected = candidates.get("selected") if isinstance(candidates, dict) else None
    return {
        "status": "INCIDENT_SOURCE_ROSTER_DROUGHT" if firing else "CLEAR",
        "firing": firing,
        "can_trade": can_trade,
        "live_build_authorized": live_build_authorized,
        "accepted_order_idle_s": accepted_order_idle_s,
        "max_idle_s": max_idle_s,
        "runtime_fresh_rows": runtime_fresh_rows,
        "direct_source": direct_source,
        "candidate_evidence": candidates,
        "unmodeled_check_candidates": int(candidates.get("unmodeled_check_candidates") or 0),
        "unmodeled_check_names": candidates.get("unmodeled_check_names") or [],
        "mechanical_escalation": (
            "DIRECT_SOURCE_SELECTION_PIN" if firing and selected else
            "RUNG_C_METHOD_SWITCH_DUE" if firing else "NONE"
        ),
        "rule": (
            "can_trade-or-live-build-authorized + accepted idle >=1800s + zero runtime source rows + fresh "
            "fully reconciled nonzero direct WIDE BUY supply"
        ),
    }


def _source_roster_drought_fire_drill(*, now: datetime) -> dict[str, Any]:
    wallet = "0x" + "d" * 40
    direct = {
        "ready": True,
        "status": "PASS",
        "packet_age_s": 1.0,
        "input_equals_terminal": True,
        "current_attempted_buy_rows": 12,
        "per_wallet": {wallet: {"attempts": 12, "copyable": 8}},
        "checksum": "synthetic-direct",
    }
    selected = {"selected": {"wallet": wallet, "eligible": True}}
    no_target = {"selected": None, "status": "NO_ADMISSIBLE_TARGET"}
    cases = {
        "zero_runtime_positive_wide": _source_roster_drought_incident(
            can_trade=True,
            accepted_order_idle_s=1801.0,
            runtime_fresh_rows=0,
            direct_source=direct,
            candidates=selected,
        ),
        "stale_wide": _source_roster_drought_incident(
            can_trade=True,
            accepted_order_idle_s=1801.0,
            runtime_fresh_rows=0,
            direct_source={**direct, "ready": False, "packet_age_s": 31.0},
            candidates=selected,
        ),
        "incomplete_terminal_coverage": _source_roster_drought_incident(
            can_trade=True,
            accepted_order_idle_s=1801.0,
            runtime_fresh_rows=0,
            direct_source={**direct, "ready": False, "input_equals_terminal": False},
            candidates=selected,
        ),
        "terminal_park_exclusion": {
            "status": "PASS",
            "excluded": True,
            "reason": "terminal_park",
        },
        "no_eligible_target": _source_roster_drought_incident(
            can_trade=True,
            accepted_order_idle_s=1801.0,
            runtime_fresh_rows=0,
            direct_source=direct,
            candidates=no_target,
        ),
    }
    overlay = {
        "members": [
            {
                "candidate_id": "incumbent",
                "source_wallet": "0x" + "c" * 40,
                "enabled": True,
                "policy_id": "p",
                "policy": {"policy_id": "p", "max_order_usd": 1.0},
            }
        ]
    }
    candidate = {
        "wallet": wallet,
        "eligible": True,
        "paper_policy_id": "p",
        "policy": {"policy_id": "p", "max_order_usd": 1.0},
    }
    _unused, consumed = _execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=candidate,
        now=now,
        supply_rung="DIRECT",
        dry_run=True,
    )
    cases["successful_pin_consumption"] = consumed
    def packet(run_id: str, terminal: str) -> dict[str, Any]:
        row = {
            "attempt_id": f"{run_id}-attempt",
            "order_id": f"{run_id}-order",
            "wallet": wallet,
            "recorded_at": (now - timedelta(seconds=10)).isoformat(),
            "f1_f4_terminal": {
                "terminal": terminal,
                "F4_executable_book": "PASS" if terminal == "COPYABLE_EXACT_POLICY_PAPER_FILL" else "NOT_EVALUATED",
            },
        }
        return {
            "updated_at": now.isoformat(),
            "policy_id": "p",
            "manifest": {"manifest_id": f"manifest-{run_id}"},
            "cohort": {"run_id": run_id, "cohort_id": f"cohort-{run_id}"},
            "terminal_reconciliation": {"direct_event_handoff": True, "input_equals_terminal": True, "input_rows": 1, "terminal_rows": 1},
            "attempt_terminals": [row],
            "orders": [row] if terminal == "COPYABLE_EXACT_POLICY_PAPER_FILL" else [],
        }
    generation_a = envelope_from_packet(packet("generation-a", "COPYABLE_EXACT_POLICY_PAPER_FILL"))
    generation_b_packet = packet("generation-b", "REFUSED_METADATA_MISSING")
    incomplete = {**generation_a, "terminal_rows": 0} if generation_a else {}
    rollover = _wide_direct_source_snapshot(
        generation_b_packet,
        now=now,
        journal=[generation_a, generation_a, incomplete] if generation_a else [incomplete],
    )
    expiry_boundary = _wide_direct_source_snapshot(
        generation_b_packet,
        now=now + timedelta(seconds=1790),
        max_packet_age_s=3600,
        journal=[generation_a] if generation_a else [],
    )
    expired = _wide_direct_source_snapshot(
        generation_b_packet,
        now=now + timedelta(seconds=1790, microseconds=1),
        max_packet_age_s=3600,
        journal=[generation_a] if generation_a else [],
    )
    cases["generation_rollover"] = {
        "status": "PASS" if (
            rollover["generation_count"] == 2
            and rollover["current_copyable_rows"] == 1
            and rollover["terminal_taxonomy"].get("REFUSED_METADATA_MISSING") == 1
            and expiry_boundary["current_copyable_rows"] == 1
            and expired["current_copyable_rows"] == 0
        ) else "FAIL",
        "positive_a_survives_b": rollover["current_copyable_rows"],
        "b_terminal_taxonomy": rollover["terminal_taxonomy"],
        "identity_deduped_generation_count": rollover["generation_count"],
        "incomplete_generation_excluded": rollover["generation_count"] == 2,
        "copyables_at_exact_ttl": expiry_boundary["current_copyable_rows"],
        "copyables_after_ttl": expired["current_copyable_rows"],
    }
    verdict = bool(
        cases["zero_runtime_positive_wide"]["status"] == "INCIDENT_SOURCE_ROSTER_DROUGHT"
        and cases["zero_runtime_positive_wide"]["mechanical_escalation"] == "DIRECT_SOURCE_SELECTION_PIN"
        and cases["stale_wide"]["status"] == "CLEAR"
        and cases["incomplete_terminal_coverage"]["status"] == "CLEAR"
        and cases["terminal_park_exclusion"]["excluded"] is True
        and cases["no_eligible_target"]["mechanical_escalation"] == "RUNG_C_METHOD_SWITCH_DUE"
        and cases["successful_pin_consumption"]["status"] == "RUNG_DIRECT_DRY_RUN_PASS"
        and cases["generation_rollover"]["status"] == "PASS"
    )
    return {
        "kind": "source_roster_drought_fire_drill",
        "checked_at": now.isoformat(),
        "flow_stage": "LIVE/ROTATE/SELF-DEV",
        "verdict": "PASS" if verdict else "FAIL",
        "cases": cases,
        "quality_bars_unchanged": True,
    }


def _select_policy_choke_candidate_pool(
    *,
    pool: list[dict[str, Any]],
    overlay: dict[str, Any],
    hot_history: dict[str, Any],
    now: datetime,
    regime: str,
    temporal_registry: dict[str, Any],
    cooloffs: dict[str, Any] | None = None,
    direct_source: dict[str, Any] | None = None,
    standby_readiness: dict[str, Any] | None = None,
    rtds_liveness: dict[str, Any] | None = None,
    require_measured_temporal: bool = False,
    total_loss_disabled_wallets: set[str] | None = None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> dict[str, Any]:
    enabled_wallets = {
        _normalize_wallet(row.get("source_wallet") or row.get("wallet"))
        for row in overlay.get("members") or []
        if isinstance(row, dict) and row.get("enabled") is not False
    }
    total_loss_disabled_wallets = {
        _normalize_wallet(wallet)
        for wallet in (total_loss_disabled_wallets or set())
        if _normalize_wallet(wallet)
    }
    fresh_counts = _fresh_buy_counts(hot_history, now=now, lookback_s=POLICY_CHOKE_LOOKBACK_S)
    direct_source = direct_source if isinstance(direct_source, dict) else {}
    direct_summary_unmeasured = bool(
        str(direct_source.get("status") or "") == "PASS"
        and any(
            direct_source.get(key) is None
            for key in ("fresh_rows", "copyable", "latest_receipt_at")
        )
    )
    if direct_summary_unmeasured:
        direct_source = {
            **direct_source,
            "status": "UNMEASURED",
            "ready": False,
            "direct_ready": False,
        }
    direct_ready = direct_source.get("ready") is True
    direct_wallets = direct_source.get("per_wallet") if isinstance(direct_source.get("per_wallet"), dict) else {}
    rtds_liveness = rtds_liveness if isinstance(rtds_liveness, dict) else {}
    rtds_wallets = (
        rtds_liveness.get("per_wallet")
        if isinstance(rtds_liveness.get("per_wallet"), dict)
        else {}
    )
    continuity = (
        direct_source.get("supply_continuity")
        if isinstance(direct_source.get("supply_continuity"), dict)
        else {}
    )
    continuity_wallets = (
        continuity.get("per_wallet")
        if isinstance(continuity.get("per_wallet"), dict)
        else {}
    )
    standby_exclusions = _standby_wallet_exclusions(
        standby_readiness if isinstance(standby_readiness, dict) else {},
        now=now,
    )
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    temporal_min_trades = _temporal_min_trades(temporal_registry)
    for lane in pool:
        if heartbeat_callback is not None:
            heartbeat_callback()
        wallet = _normalize_wallet(lane.get("wallet") or lane.get("source_wallet"))
        if not wallet or wallet in seen:
            continue
        seen.add(wallet)
        evidence = _rung_b_regime_evidence(lane, regime)
        source_liveness = lane.get("source_liveness") if isinstance(lane.get("source_liveness"), dict) else {}
        external_liveness = lane.get("external_liveness") if isinstance(lane.get("external_liveness"), dict) else {}
        bench_liveness = lane.get("bench_liveness") if isinstance(lane.get("bench_liveness"), dict) else {}
        liveness_status = str(
            lane.get("external_liveness_status")
            or source_liveness.get("status")
            or external_liveness.get("status")
            or bench_liveness.get("status")
            or ""
        )
        age_h = _as_float(
            lane.get("external_latest_trade_age_h")
            or lane.get("latest_trade_age_h")
            or source_liveness.get("last_trade_age_h")
            or bench_liveness.get("last_real_trade_age_h")
        )
        max_age_h = _as_float(lane.get("external_liveness_max_age_h") or source_liveness.get("max_age_h")) or 24.0
        cooloff_until = _parse_ts((cooloffs or {}).get(wallet))
        lane_cooloff_candidates = [
            _parse_ts(lane.get(key))
            for key in ("cooloff_until", "demotion_cooloff_until", "toxicity_cooloff_until")
        ]
        lane_cooloff_until = max((value for value in lane_cooloff_candidates if value is not None), default=None)
        in_cooloff = bool(
            (cooloff_until is not None and cooloff_until > now)
            or (lane_cooloff_until is not None and lane_cooloff_until > now)
            or "COOLOFF" in str(lane.get("status") or lane.get("shadow_status") or "").upper()
        )
        replay = lane.get("replay") if isinstance(lane.get("replay"), dict) else {}
        policy_id = str(
            lane.get("paper_policy_id")
            or lane.get("copy_policy_family")
            or lane.get("policy_id")
            or replay.get("policy_id")
            or ""
        )
        policy_template = next(
            (
                member.get("policy") for member in overlay.get("members") or []
                if isinstance(member, dict)
                and str(member.get("policy_id") or (member.get("policy") or {}).get("policy_id") or "") == policy_id
                and isinstance(member.get("policy"), dict)
            ),
            None,
        )
        fresh_own_source_buy_rows = int(fresh_counts.get(wallet, 0))
        direct_row = direct_wallets.get(wallet) if isinstance(direct_wallets.get(wallet), dict) else {}
        direct_attempts = int(direct_row.get("attempts") or 0)
        direct_copyables = int(direct_row.get("copyable") or 0)
        direct_policy_depth_pass = int(direct_row.get("policy_depth_pass") or 0)
        continuity_measured = wallet in continuity_wallets
        continuity_row = (
            continuity_wallets.get(wallet)
            if isinstance(continuity_wallets.get(wallet), dict)
            else {}
        )
        continuity_attempt_windows_present = int(
            continuity_row.get("attempt_windows_present") or 0
        )
        continuity_copyable_windows_present = int(
            continuity_row.get("copyable_windows_present") or 0
        )
        continuity_required_windows = int(
            continuity_row.get("required_windows")
            or continuity.get("required_windows")
            or 3
        )
        if direct_ready and direct_attempts > 0:
            fresh_own_source_buy_rows = direct_attempts
        claimed_fresh_own_source_buy_rows = int(
            lane.get("fresh_own_source_buy_rows_30m") or 0
        )
        temporal = lane.get("temporal_evidence") if isinstance(lane.get("temporal_evidence"), dict) else {}
        temporal_classification = str(
            lane.get("temporal_classification") or temporal.get("classification") or ""
        ).upper()
        active_temporal_slices = _temporal_active_slices(
            temporal_registry,
            wallet=wallet,
            regime=regime,
            now=now,
        )
        active_temporal = next(
            (
                row
                for row in active_temporal_slices
                if row["label"] == "PROVEN-NEGATIVE"
            ),
            active_temporal_slices[0],
        )
        active_temporal_fading = any(
            str(row.get("classification") or "").upper() == "FADING"
            for row in active_temporal_slices
        )
        fading_clear = lane.get("fading_clear")
        if active_temporal_fading:
            # Fail closed on the canonical temporal registry.  A synthesized
            # generation-pool lane must never green-paint a FADING wallet,
            # even if an older lane snapshot carried fading_clear=true.
            fading_clear = False
        elif fading_clear is None:
            fading_clear = "FADING" not in temporal_classification
        direct_liveness_pass = bool(direct_ready and direct_attempts > 0)
        snapshot_liveness_pass = bool(
            liveness_status == "PASS" and age_h is not None and age_h <= max_age_h
        )
        rtds_row = rtds_wallets.get(wallet) if isinstance(rtds_wallets.get(wallet), dict) else {}
        rtds_observed_age_s = _as_float(rtds_row.get("observed_age_s"))
        rtds_liveness_pass = bool(
            rtds_observed_age_s is not None
            and rtds_observed_age_s <= max_age_h * 3600.0
        )
        f4_basis = (
            "direct"
            if direct_liveness_pass
            else "snapshot"
            if snapshot_liveness_pass
            else "rtds_observed"
        )
        exclusion = standby_exclusions.get(wallet)
        volume_park_superseded = _fingerprint_direct_supersedes_volume_park(
            wallet=wallet,
            lane=lane,
            evidence=evidence,
            exclusion=exclusion,
        )
        effective_exclusion = None if volume_park_superseded else exclusion
        checks = {
            "f1_measured_positive_regime_cell": bool(
                evidence["pnl_usd"] is not None and evidence["pnl_usd"] > 0
                and evidence["roi_pct"] is not None and evidence["roi_pct"] > 0
                and evidence["resolved_signals"] >= 200
            ),
            "f2_fresh_rows_and_own_policy_copyable": bool(
                fresh_own_source_buy_rows >= 10
                and (not direct_ready or direct_copyables > 0)
            ),
            "f3_not_enabled_or_cooloff_or_fading": bool(
                wallet not in enabled_wallets
                and wallet not in total_loss_disabled_wallets
                and not in_cooloff
                and fading_clear is not False
            ),
            "f4_external_liveness": bool(
                direct_liveness_pass
                or snapshot_liveness_pass
                or rtds_liveness_pass
            ),
            "own_evidenced_policy_available": bool(policy_id and policy_template),
            "active_temporal_not_proven_negative": (
                all(
                    row["label"] != "PROVEN-NEGATIVE"
                    for row in active_temporal_slices
                )
            ),
            "active_temporal_regime_cell_measured": (
                not require_measured_temporal
                or all(
                    int(row.get("resolved_trades") or 0) >= temporal_min_trades
                    for row in active_temporal_slices
                )
            ),
            "not_terminal_park_red_clock_or_measured_loser": (
                effective_exclusion is None
            ),
        }
        f1_slice_disclosure = _f1_slice_disclosure(
            active_slices=active_temporal_slices,
            regime_evidence=evidence,
            checks=checks,
        )
        rows.append(
            {
                "wallet": wallet,
                "candidate_id": lane.get("candidate_id"),
                "supply_source": lane.get("supply_source") or "ready_shadow",
                "paper_policy_id": policy_id,
                "policy": dict(policy_template) if policy_template else None,
                "regime": regime,
                "regime_evidence": evidence,
                "fresh_own_source_buy_rows_30m": fresh_own_source_buy_rows,
                "claimed_fresh_own_source_buy_rows_30m": claimed_fresh_own_source_buy_rows,
                "external_latest_trade_age_h": age_h,
                "external_liveness_max_age_h": max_age_h,
                "f4_basis": f4_basis,
                "f4_rtds_observed": {
                    **rtds_row,
                    "pass": rtds_liveness_pass,
                    "max_age_s": max_age_h * 3600.0,
                },
                "old_remote_liveness": {
                    "status": liveness_status or None,
                    "latest_trade_age_h": age_h,
                    "max_age_h": max_age_h,
                },
                "chosen_liveness_authority": (
                    "direct_polygon_wide"
                    if direct_liveness_pass
                    else "remote_probe"
                    if snapshot_liveness_pass
                    else "rtds_observed"
                ),
                "direct_source": {
                    "attempts": direct_attempts,
                    "copyable": direct_copyables,
                    "policy_depth_pass": direct_policy_depth_pass,
                    "latest_receipt_at": direct_row.get("latest_receipt_at"),
                    "packet_checksum": direct_source.get("checksum"),
                    "continuity_attempt_windows_present": (
                        continuity_attempt_windows_present
                    ),
                    "continuity_copyable_windows_present": (
                        continuity_copyable_windows_present
                    ),
                    "continuity_required_windows": continuity_required_windows,
                    "continuity_window_count": int(continuity.get("window_count") or 4),
                    "attempt_continuity_pass": bool(
                        continuity_measured
                        and continuity_attempt_windows_present
                        >= continuity_required_windows
                    ),
                    "copyable_continuity_pass": bool(
                        continuity_measured
                        and continuity_copyable_windows_present
                        >= continuity_required_windows
                    ),
                    "attempts_by_continuity_window": continuity_row.get(
                        "attempts_by_window"
                    ),
                    "copyables_by_continuity_window": continuity_row.get(
                        "copyables_by_window"
                    ),
                },
                "f2_copyable_policy_id": (
                    (direct_source.get("identity") or {}).get("policy_id")
                    if isinstance(direct_source.get("identity"), dict)
                    else None
                ),
                "f2_copyable_is_base_cohort_policy": True,
                "standby_exclusion": exclusion,
                "park_provenance": (
                    _park_provenance(effective_exclusion)
                    if checks["not_terminal_park_red_clock_or_measured_loser"] is False
                    else None
                ),
                "fading_clear": fading_clear is not False,
                "wallet_cooloff_active": in_cooloff,
                "total_loss_direct_fenced": wallet in total_loss_disabled_wallets,
                "standby_exclusion_superseded": (
                    {
                        "status": "SUPERSEDED_FOR_FINGERPRINT_STRICT_WIDE_DIRECT",
                        "scope": "volume_lane_only",
                        "wide_policy_fingerprint": lane.get("wide_policy_fingerprint"),
                    }
                    if volume_park_superseded
                    else None
                ),
                "cooloff_until": cooloff_until.isoformat() if cooloff_until else None,
                "active_temporal": active_temporal,
                "active_temporal_slices": active_temporal_slices,
                **f1_slice_disclosure,
                "checks": checks,
                "eligible": all(checks.values()),
            }
        )
    eligible = [row for row in rows if row["eligible"]]
    eligible.sort(
        key=lambda row: (
            -int(
                row.get("direct_source", {}).get(
                    "continuity_copyable_windows_present"
                )
                or 0
            ),
            -int(
                row.get("direct_source", {}).get(
                    "continuity_attempt_windows_present"
                )
                or 0
            ),
            -int(row.get("direct_source", {}).get("copyable") or 0),
            -float(row["regime_evidence"]["pnl_usd"]),
            -float(row["regime_evidence"]["roi_pct"]),
            row["wallet"],
        )
    )
    return {
        "regime": regime,
        "candidate_count": len(rows),
        "eligible_count": len(eligible),
        "selected": eligible[0] if eligible else None,
        "rows": rows,
        "rtds_observed_liveness": {
            key: rtds_liveness.get(key)
            for key in (
                "status", "checked_at", "bytes_read", "rows_read",
                "observed_trade_rows", "byte_offset", "capture_size_bytes",
                "offset_reset_to_bounded_tail", "max_bytes_per_cycle",
            )
        },
        "f4_root_defect": {
            "status": (
                "GLOBAL_DIRECT_READY_NULL_DISABLES_PER_WALLET_F4"
                if not direct_ready
                and direct_source.get("fresh_rows") is None
                and direct_source.get("copyable") is None
                and direct_source.get("latest_receipt_at") is None
                else "NOT_OBSERVED"
            ),
            "direct_ready": direct_ready,
            "direct_status": direct_source.get("status"),
            "fresh_rows": direct_source.get("fresh_rows"),
            "copyable": direct_source.get("copyable"),
            "latest_receipt_at": direct_source.get("latest_receipt_at"),
            "checksum": direct_source.get("checksum"),
            "next_action": "repair the global direct probe independently; RTDS liveness is a per-wallet measured route, not a direct-probe health claim",
        },
        "per_wallet_gate_global_scalar_audit": {
            "f1": {"status": "PER_WALLET", "basis": "lane regime evidence"},
            "f2": {
                "status": "GLOBAL_SCALAR_PRESENT_BASE_POOL_EXACT_FRONTIER_OVERRIDES",
                "global_scalar": "direct_ready",
                "per_wallet_inputs": ["fresh_counts", "direct_attempts", "direct_copyables"],
                "live_direct_frontier_rule": "exact generation attempts>=10 and copyable>0",
                "next_action": "remove the base-pool direct_ready conditional in a separately bounded cleanup; never count RTDS toward F2",
            },
            "f3": {"status": "PER_WALLET", "basis": "enabled/cooloff/fading"},
            "f4": {"status": "PER_WALLET_REPAIRED_ORDER140", "basis": "direct/snapshot/rtds_observed"},
            "policy": {"status": "PER_IDENTITY", "basis": "wallet policy join"},
            "temporal": {"status": "PER_WALLET", "basis": "active temporal slices"},
            "park": {"status": "PER_WALLET", "basis": "standby exclusion"},
        },
        "rule": (
            "F1-F4 plus exact evidenced policy and every simultaneously active "
            "temporal slice both measured and not PROVEN-NEGATIVE; F2 requires >=10 observed-current BTC-5m "
            "rows AND >=1 base-cohort-policy copyable; fresh reconciled direct WIDE liveness supersedes stale or "
            "missing remote liveness only; terminal parks/red clocks remain excluded; "
            "copyable continuity ranks first and attempt continuity second without "
            "gating F1-F4; then rank direct copyables, pnl, roi desc, wallet asc"
        ),
        "gate_digits": {
            "f1_min_resolved_signals": 200,
            "f1_pnl_usd_gt": 0,
            "f1_roi_pct_gt": 0,
            "f2_min_fresh_own_source_buy_rows_30m": 10,
            "f2_min_policy_depth_pass_copyables": 1,
            "f4_external_liveness_max_age_h": 24.0,
            "f5_min_regime_slice_resolved_trades": temporal_min_trades,
        },
    }


def _member_wide_fingerprint(member: dict[str, Any] | None) -> str:
    if not isinstance(member, dict):
        return ""
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    return str(
        member.get("wide_policy_fingerprint")
        or policy.get("wide_policy_fingerprint")
        or ""
    )


def _cooloff_record(
    key: str,
    value: Any,
) -> tuple[str, datetime | None, str]:
    wallet_key, separator, key_fingerprint = str(key).partition("|")
    if isinstance(value, dict):
        expires = _parse_ts(value.get("expires_at") or value.get("cooloff_until"))
        fingerprint = str(
            value.get("wide_policy_fingerprint") or key_fingerprint or ""
        )
    else:
        expires = _parse_ts(value)
        fingerprint = key_fingerprint if separator else ""
    return _normalize_wallet(wallet_key), expires, fingerprint


def _cooloff_scope_for_identity(
    *,
    cooloffs: dict[str, Any] | None,
    overlay: dict[str, Any],
    wallet: str,
    fingerprint: str,
    now: datetime,
    bar_wallet_on_any_fingerprint: bool = False,
) -> dict[str, Any]:
    """Resolve exact cooloff provenance, optionally barring the whole wallet."""

    provenance_fingerprints = {
        _member_wide_fingerprint(member)
        for member in overlay.get("members") or []
        if isinstance(member, dict)
        and _normalize_wallet(member.get("source_wallet") or member.get("wallet"))
        == wallet
        and member.get("enabled") is False
        and (
            str(member.get("candidate_id") or "").startswith("policy_choke_rung_")
            or "DEMOT" in str(member.get("status") or "").upper()
            or "DISABLED" in str(member.get("status") or "").upper()
        )
    }
    provenance_fingerprints.discard("")
    applied: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    for key, value in (cooloffs or {}).items():
        record_wallet, expires, scoped_fingerprint = _cooloff_record(str(key), value)
        if record_wallet != wallet or expires is None or expires <= now:
            continue
        if scoped_fingerprint:
            matches = scoped_fingerprint == fingerprint
        elif not fingerprint or not provenance_fingerprints:
            matches = True
        else:
            matches = fingerprint in provenance_fingerprints
        record = {
            "key": str(key),
            "expires_at": expires.isoformat(),
            "wide_policy_fingerprint": scoped_fingerprint or None,
        }
        (applied if matches else ignored).append(record)
    effective = [*applied, *ignored] if bar_wallet_on_any_fingerprint else applied
    return {
        "active": bool(effective),
        "cooloff_until": max(
            (row["expires_at"] for row in effective),
            default=None,
        ),
        "applied": applied,
        "cooloff_scope_mismatch_ignored": ignored,
        "known_demotion_fingerprints": sorted(provenance_fingerprints),
        "wallet_wide_frontier_bar": bool(
            bar_wallet_on_any_fingerprint and effective
        ),
    }


def _set_identity_cooloff(
    cooloffs: dict[str, Any],
    *,
    wallet: str,
    fingerprint: str,
    expires_at: datetime,
    reason: str,
) -> str:
    if fingerprint:
        key = f"{wallet}|{fingerprint}"
        cooloffs[key] = {
            "expires_at": expires_at.isoformat(),
            "wide_policy_fingerprint": fingerprint,
            "reason": reason,
        }
        return key
    cooloffs[wallet] = expires_at.isoformat()
    return wallet


def _select_policy_choke_rung_b_candidate(
    *,
    ready_shadow: dict[str, Any],
    overlay: dict[str, Any],
    hot_history: dict[str, Any],
    now: datetime,
    regime: str,
    temporal_registry: dict[str, Any],
    cooloffs: dict[str, Any] | None = None,
    require_measured_temporal: bool = False,
) -> dict[str, Any]:
    pool = [
        {**row, "supply_source": "ready_shadow"}
        for key in ("lanes", "hot_standby_ranked_candidates")
        for row in ready_shadow.get(key) or [] if isinstance(row, dict)
    ]
    return _select_policy_choke_candidate_pool(
        pool=pool,
        overlay=overlay,
        hot_history=hot_history,
        now=now,
        regime=regime,
        cooloffs=cooloffs,
        temporal_registry=temporal_registry,
        require_measured_temporal=require_measured_temporal,
    )


def _select_policy_choke_rung_c_candidate(
    *,
    cohort_admission: dict[str, Any],
    full_pool_queue: dict[str, Any],
    overlay: dict[str, Any],
    hot_history: dict[str, Any],
    now: datetime,
    regime: str,
    cooloffs: dict[str, Any] | None = None,
    temporal_registry: dict[str, Any],
) -> dict[str, Any]:
    pool = [
        {**row, "supply_source": "cohort_alive_admission_packets"}
        for row in cohort_admission.get("packets") or [] if isinstance(row, dict)
    ]
    pool.extend(
        {**row, "supply_source": "full_pool_member_queue"}
        for row in full_pool_queue.get("ranked_members") or [] if isinstance(row, dict)
    )
    evidence = _select_policy_choke_candidate_pool(
        pool=pool,
        overlay=overlay,
        hot_history=hot_history,
        now=now,
        regime=regime,
        cooloffs=cooloffs,
        temporal_registry=temporal_registry,
        require_measured_temporal=True,
    )
    evidence["status"] = "RUNG_C_FULL_POOL_SWEEP"
    check_names = (
        "f1_measured_positive_regime_cell",
        "f1_venue_reachable_admissible",
        "f1_walk_forward_admissible",
        "f2_fresh_rows_and_own_policy_copyable",
        "f3_not_enabled_or_cooloff_or_fading",
        "f4_external_liveness",
        "own_evidenced_policy_available",
        "active_temporal_not_proven_negative",
        "active_temporal_regime_cell_measured",
    )
    evidence["refusal_counts"] = {
        name: sum(1 for row in evidence["rows"] if not row["checks"].get(name))
        for name in check_names
    }
    evidence["supply_counts"] = {
        "cohort_alive_admission_packets": sum(
            1 for row in evidence["rows"] if row.get("supply_source") == "cohort_alive_admission_packets"
        ),
        "full_pool_member_queue": sum(
            1 for row in evidence["rows"] if row.get("supply_source") == "full_pool_member_queue"
        ),
    }
    return evidence


def _admit_forward_only_supply(
    *,
    direct_source: dict[str, Any],
    fingerprint_evidence: dict[str, Any],
    lane: dict[str, Any],
    manifest: dict[str, Any],
    forward_evidence: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Expose a preregistered lane only after its forward-only gate passes."""
    if lane.get("admission_eligible") is not True:
        return direct_source, fingerprint_evidence
    wallet = _normalize_wallet(lane.get("wallet"))
    fingerprint = str(lane.get("wide_policy_fingerprint") or "")
    manifest_id = str(manifest.get("manifest_id") or "")
    if (
        not wallet
        or not fingerprint
        or not manifest_id
        or lane.get("manifest_id") != manifest_id
        or lane.get("retrospective_is_admission_input") is not False
        or lane.get("retrospective_and_forward_may_be_summed") is not False
        or int(lane.get("forward_n") or 0) < 200
    ):
        return direct_source, fingerprint_evidence
    cell = next(
        (
            row
            for row in forward_evidence.get("cells") or []
            if isinstance(row, dict)
            and str(row.get("wide_policy_fingerprint") or "") == fingerprint
            and _normalize_wallet((row.get("identity") or {}).get("wallet")) == wallet
        ),
        {},
    )
    venue = venue_gate_summary(cell)
    if (
        not cell
        or venue.get("f1_walk_forward_admissible") is not True
        or float(_as_float(venue.get("venue_reachable_share_pct")) or 0.0) < 40.0
    ):
        return direct_source, fingerprint_evidence
    identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
    policy_id = str(identity.get("policy_id") or "")
    if not policy_id:
        return direct_source, fingerprint_evidence

    merged_direct = copy.deepcopy(direct_source)
    merged_evidence = copy.deepcopy(fingerprint_evidence)
    generation_key = f"bac25_forward_only|{manifest_id}|{wallet}"
    generation = {
        "wallet": wallet,
        "policy_id": policy_id,
        "attempts": int(lane.get("forward_n") or 0),
        "copyable": int(lane.get("forward_n") or 0),
        "policy_depth_pass": int(lane.get("forward_n") or 0),
        "source_generation": generation_key,
        "generation_identity": {
            "manifest_id": manifest_id,
            "run_id": "bac25_forward_only",
            "policy_id": policy_id,
        },
        "forward_only": True,
        "retrospective_credit": 0,
    }
    merged_direct["ready"] = True
    merged_direct.setdefault("per_wallet_generation", {})[generation_key] = generation
    merged_direct.setdefault("per_wallet", {})[wallet] = {
        "attempts": generation["attempts"],
        "copyable": generation["copyable"],
    }
    merged_evidence.setdefault("manifest_wallet_fingerprints", {})[
        f"{manifest_id}|{wallet}"
    ] = identity
    cells = [
        row
        for row in merged_evidence.get("cells") or []
        if not (
            isinstance(row, dict)
            and str(row.get("wide_policy_fingerprint") or "") == fingerprint
        )
    ]
    merged_evidence["cells"] = [*cells, cell]
    return merged_direct, merged_evidence


def _reread_direct_snapshot_packet(
    original: dict[str, Any],
    *,
    load_packet: Any,
    fingerprint_evidence: dict[str, Any],
    lane: dict[str, Any],
    manifest: dict[str, Any],
    forward_evidence: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    """Re-read direct supply at consumption time without accepting older data."""

    try:
        reread = load_packet()
    except (OSError, json.JSONDecodeError):
        return original, fingerprint_evidence, False
    if not isinstance(reread, dict) or not reread:
        return original, fingerprint_evidence, False
    original_at = _parse_ts(original.get("updated_at") or original.get("generated_at"))
    reread_at = _parse_ts(reread.get("updated_at") or reread.get("generated_at"))
    if reread_at is None or (original_at is not None and reread_at < original_at):
        return original, fingerprint_evidence, False
    admitted, admitted_evidence = _admit_forward_only_supply(
        direct_source=reread,
        fingerprint_evidence=fingerprint_evidence,
        lane=lane,
        manifest=manifest,
        forward_evidence=forward_evidence,
    )
    return admitted, admitted_evidence, True


def _source_drought_check_deficits(checks: dict[str, Any]) -> list[str]:
    return [
        name for name in REQUIRED_SOURCE_DROUGHT_CHECKS
        if checks.get(name) is not True
    ]


def _source_drought_unmodeled_checks(checks: dict[str, Any]) -> list[str]:
    return sorted(set(checks) - set(REQUIRED_SOURCE_DROUGHT_CHECKS))


def _source_drought_checks_pass(checks: dict[str, Any]) -> bool:
    return not _source_drought_check_deficits(checks) and not _source_drought_unmodeled_checks(checks)


def _select_source_drought_candidate(
    *,
    ready_shadow: dict[str, Any],
    cohort_admission: dict[str, Any],
    full_pool_queue: dict[str, Any],
    overlay: dict[str, Any],
    hot_history: dict[str, Any],
    direct_source: dict[str, Any],
    standby_readiness: dict[str, Any],
    now: datetime,
    regime: str,
    temporal_registry: dict[str, Any],
    cooloffs: dict[str, Any] | None = None,
    fingerprint_evidence: dict[str, Any] | None = None,
    manifest_identity_fallback: dict[str, dict[str, Any]] | None = None,
    rtds_liveness: dict[str, Any] | None = None,
    total_loss_auto_disable: dict[str, Any] | None = None,
    heartbeat_callback: Callable[[], None] | None = None,
    exact_policy_holdouts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Rank only fully evidenced targets that have current direct WIDE supply."""
    pool = [
        {**row, "supply_source": "ready_shadow"}
        for key in ("lanes", "hot_standby_ranked_candidates")
        for row in ready_shadow.get(key) or []
        if isinstance(row, dict)
    ]
    pool.extend(
        {**row, "supply_source": "cohort_alive_admission_packets"}
        for row in cohort_admission.get("packets") or []
        if isinstance(row, dict)
    )
    pool.extend(
        {**row, "supply_source": "full_pool_member_queue"}
        for row in full_pool_queue.get("ranked_members") or []
        if isinstance(row, dict)
    )
    generation_supply = (
        direct_source.get("per_wallet_generation")
        if isinstance(direct_source.get("per_wallet_generation"), dict)
        else {}
    )
    fingerprint_evidence = (
        fingerprint_evidence if isinstance(fingerprint_evidence, dict) else {}
    )
    holdout_gate_supplied = isinstance(exact_policy_holdouts, dict)
    exact_policy_holdouts = exact_policy_holdouts if holdout_gate_supplied else {}
    manifest_fingerprints = dict(
        fingerprint_evidence.get("manifest_wallet_fingerprints")
        if isinstance(fingerprint_evidence.get("manifest_wallet_fingerprints"), dict)
        else {}
    )
    manifest_fingerprints.update(
        manifest_identity_fallback
        if isinstance(manifest_identity_fallback, dict)
        else {}
    )
    pooled_wallets = {
        _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
        for row in pool
        if isinstance(row, dict)
    }
    # Reconciled journal generations remain valid evidence even while the latest
    # cumulative packet is between atomic terminal reconciliations.  Surface
    # their exact manifest identity in the diagnostic frontier; the separate
    # direct_source.ready gate still prevents any actuator from consuming a
    # stale/incomplete latest packet.
    # A wallet that is present in the reconciled direct supply but cannot be
    # admitted here leaves no trace anywhere downstream: it is absent from the
    # frontier, and supply_dropouts only tracks wallets that left the supply.
    # Record the exact refusal so a manifest rotation cannot silently blind the
    # frontier to a live, copyable, pinned wallet.  Diagnostic only.
    pool_admission_dropouts: list[dict[str, Any]] = []
    for supply in generation_supply.values():
        if not isinstance(supply, dict) or int(supply.get("attempts") or 0) <= 0:
            continue
        wallet = _normalize_wallet(supply.get("wallet"))
        policy_id = str(supply.get("policy_id") or "")
        identity = (
            supply.get("generation_identity")
            if isinstance(supply.get("generation_identity"), dict)
            else {}
        )
        manifest_id = str(identity.get("manifest_id") or "")
        manifest_identity = manifest_fingerprints.get(f"{manifest_id}|{wallet}")
        if wallet and wallet in pooled_wallets:
            continue
        admission_refusal = None
        if not wallet:
            admission_refusal = "supply_wallet_missing"
        elif not policy_id:
            admission_refusal = "supply_policy_id_missing"
        elif not manifest_id:
            admission_refusal = "supply_manifest_id_missing"
        elif not isinstance(manifest_identity, dict):
            admission_refusal = "manifest_wallet_fingerprint_absent"
        elif not str(manifest_identity.get("wide_policy_fingerprint") or ""):
            admission_refusal = "manifest_wallet_fingerprint_empty"
        elif _normalize_wallet(manifest_identity.get("wallet")) != wallet:
            admission_refusal = "manifest_identity_wallet_mismatch"
        elif str(manifest_identity.get("policy_id") or "") != policy_id:
            admission_refusal = "manifest_identity_policy_id_mismatch"
        if admission_refusal is not None:
            pool_admission_dropouts.append(
                {
                    "wallet": wallet,
                    "policy_id": policy_id,
                    "manifest_id": manifest_id,
                    "manifest_wallet_key": f"{manifest_id}|{wallet}",
                    "source_generation": str(supply.get("source_generation") or ""),
                    "run_id": str(identity.get("run_id") or ""),
                    "attempts": int(supply.get("attempts") or 0),
                    "copyable": int(supply.get("copyable") or 0),
                    "latest_receipt_at": supply.get("latest_receipt_at"),
                    "admission_refusal": admission_refusal,
                }
            )
            continue
        pool.append(
            {
                "wallet": wallet,
                "paper_policy_id": policy_id,
                "policy_id": policy_id,
                "wide_policy_fingerprint": str(
                    manifest_identity["wide_policy_fingerprint"]
                ),
                "supply_source": "direct_capture_watch_generation",
            }
        )
        pooled_wallets.add(wallet)
    total_loss_disabled_wallets = _direct_total_loss_fenced_wallets(
        overlay=overlay,
        total_loss_auto_disable=(
            total_loss_auto_disable
            if isinstance(total_loss_auto_disable, dict)
            else {}
        ),
    )
    evidence = _select_policy_choke_candidate_pool(
        pool=pool,
        overlay=overlay,
        hot_history=hot_history,
        now=now,
        regime=regime,
        cooloffs=cooloffs,
        temporal_registry=temporal_registry,
        direct_source=direct_source,
        standby_readiness=standby_readiness,
        rtds_liveness=rtds_liveness,
        require_measured_temporal=True,
        total_loss_disabled_wallets=total_loss_disabled_wallets,
        heartbeat_callback=heartbeat_callback,
    )
    cells_by_fingerprint = {
        str(row.get("wide_policy_fingerprint") or ""): row
        for row in fingerprint_evidence.get("cells") or []
        if isinstance(row, dict) and row.get("wide_policy_fingerprint")
    }
    strict_cells_by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cell in cells_by_fingerprint.values():
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        fixed = venue_gate_summary(cell)
        first = fixed.get("first_half") if isinstance(fixed.get("first_half"), dict) else {}
        second = fixed.get("second_half") if isinstance(fixed.get("second_half"), dict) else {}
        if (
            fixed.get("f1_pass") is True
            and fixed.get("concentration_admissible") is True
            and first.get("f1_pass") is True
            and first.get("concentration_admissible") is True
            and float(first.get("post_fee_pnl_usd") or 0.0) > 0.0
            and second.get("f1_pass") is True
            and second.get("concentration_admissible") is True
            and float(second.get("post_fee_pnl_usd") or 0.0) > 0.0
            and _normalize_wallet(identity.get("wallet"))
        ):
            strict_cells_by_wallet[_normalize_wallet(identity["wallet"])].append(cell)
    base_by_wallet_policy = {
        (row["wallet"], str(row.get("paper_policy_id") or "")): row
        for row in evidence["rows"]
    }
    base_by_wallet = {
        row["wallet"]: row
        for row in evidence["rows"]
    }
    frontier: list[dict[str, Any]] = []
    frontier_supplies: list[dict[str, Any]] = []
    for raw_supply in generation_supply.values():
        if not isinstance(raw_supply, dict):
            continue
        frontier_supplies.append(raw_supply)
        wallet = _normalize_wallet(raw_supply.get("wallet"))
        manifest_id = str(
            (raw_supply.get("generation_identity") or {}).get("manifest_id") or ""
        )
        current_identity = manifest_fingerprints.get(f"{manifest_id}|{wallet}") or {}
        current_fingerprint = str(current_identity.get("wide_policy_fingerprint") or "")
        for cell in strict_cells_by_wallet.get(wallet, []):
            identity = cell.get("identity") or {}
            fingerprint = str(cell.get("wide_policy_fingerprint") or "")
            if (
                not fingerprint
                or fingerprint == current_fingerprint
                or str(identity.get("policy_id") or "") != str(raw_supply.get("policy_id") or "")
            ):
                continue
            frontier_supplies.append(
                {**raw_supply, "_frontier_fingerprint_identity": identity}
            )
    for supply in frontier_supplies:
        wallet = str(supply.get("wallet") or "")
        policy_id = str(supply.get("policy_id") or "")
        base = base_by_wallet_policy.get((wallet, policy_id)) or base_by_wallet.get(wallet)
        if base is None:
            # Supplied but unrepresented in the candidate pool: the wallet
            # cannot appear on the frontier at all, so _wallet_authority()
            # returns {} for it and any pin on it dead-ends upstream of every
            # F1-F4 bar.  Name it instead of dropping it silently.
            pool_admission_dropouts.append(
                {
                    "wallet": _normalize_wallet(wallet),
                    "policy_id": policy_id,
                    "manifest_id": str(
                        (supply.get("generation_identity") or {}).get("manifest_id") or ""
                    ),
                    "manifest_wallet_key": (
                        f"{(supply.get('generation_identity') or {}).get('manifest_id') or ''}"
                        f"|{_normalize_wallet(wallet)}"
                    ),
                    "source_generation": str(supply.get("source_generation") or ""),
                    "run_id": str(
                        (supply.get("generation_identity") or {}).get("run_id") or ""
                    ),
                    "attempts": int(supply.get("attempts") or 0),
                    "copyable": int(supply.get("copyable") or 0),
                    "latest_receipt_at": supply.get("latest_receipt_at"),
                    "admission_refusal": "no_candidate_pool_base_row",
                }
            )
            continue
        checks = dict(base["checks"])
        generation_identity = (
            supply.get("generation_identity")
            if isinstance(supply.get("generation_identity"), dict)
            else {}
        )
        manifest_id = str(generation_identity.get("manifest_id") or "")
        fingerprint_identity = supply.get("_frontier_fingerprint_identity")
        if not isinstance(fingerprint_identity, dict):
            fingerprint_identity = manifest_fingerprints.get(f"{manifest_id}|{wallet}")
        fingerprint_identity = (
            fingerprint_identity if isinstance(fingerprint_identity, dict) else {}
        )
        fingerprint = str(fingerprint_identity.get("wide_policy_fingerprint") or "")
        fingerprint_cell = cells_by_fingerprint.get(fingerprint)
        exact_policy_join = bool(
            fingerprint_cell
            and str(fingerprint_identity.get("policy_id") or "") == policy_id
            and str(fingerprint_identity.get("wallet") or "") == wallet
        )
        fixed_evidence = (
            venue_gate_summary(fingerprint_cell) if exact_policy_join else {}
        )
        policy_template = (
            {
                "policy_id": f"wide_fp_{fingerprint[:24]}",
                "base_policy_id": policy_id,
                "wide_policy_fingerprint": fingerprint,
                "move_slice_keys": list(fingerprint_identity.get("move_slice_keys") or []),
                "min_price": 0.0,
                "max_price": 1.0,
                "wallet_fraction": float(
                    fingerprint_identity.get("wallet_fraction") or 0.1
                ),
                "max_order_usd": min(
                    1.0, float(fingerprint_identity.get("max_order_usd") or 1.0)
                ),
                "min_order_usd": min(
                    1.0, float(fingerprint_identity.get("min_order_usd") or 1.0)
                ),
                "max_fill_lag_s": float(
                    fingerprint_identity.get("max_fill_lag_s") or 5.0
                ),
                "fee_model_id": fingerprint_identity.get("fee_model_id"),
                "selection_rule_id": fingerprint_identity.get("selection_rule_id"),
            }
            if exact_policy_join
            else None
        )
        fingerprint_volume_park_superseded = (
            _fingerprint_direct_supersedes_volume_park(
                wallet=wallet,
                lane={"wide_policy_fingerprint": fingerprint},
                evidence={
                    "resolved_signals": int(fixed_evidence.get("resolved") or 0),
                    "pnl_usd": fixed_evidence.get("post_fee_pnl_usd"),
                    "roi_pct": fixed_evidence.get("roi_pct"),
                },
                exclusion=base.get("standby_exclusion"),
            )
            if exact_policy_join
            else False
        )
        if exact_policy_join:
            checks["f1_measured_positive_regime_cell"] = bool(
                int(fixed_evidence.get("resolved") or 0) >= 200
                and float(fixed_evidence.get("post_fee_pnl_usd") or 0.0) > 0
                and float(fixed_evidence.get("roi_pct") or 0.0) > 0
            )
            checks["f1_concentration_admissible"] = bool(
                fixed_evidence.get("concentration_admissible") is True
            )
            checks["f1_venue_reachable_admissible"] = bool(
                fixed_evidence.get("f1_venue_reachable_admissible") is True
            )
            checks["f1_walk_forward_admissible"] = bool(
                fixed_evidence.get("f1_walk_forward_admissible") is True
            )
            checks["both_resolved_halves_positive"] = bool(
                float(fixed_evidence.get("first_half_post_fee_pnl_usd") or 0.0)
                > 0
                and float(
                    fixed_evidence.get("second_half_post_fee_pnl_usd") or 0.0
                )
                > 0
            )
            checks["own_evidenced_policy_available"] = bool(policy_template)
            if fingerprint_volume_park_superseded:
                checks["not_terminal_park_red_clock_or_measured_loser"] = (
                    not bool((base.get("standby_exclusion") or {}).get("evidenced_park"))
                )
            cooloff_scope = _cooloff_scope_for_identity(
                cooloffs=cooloffs,
                overlay=overlay,
                wallet=wallet,
                fingerprint=fingerprint,
                now=now,
                bar_wallet_on_any_fingerprint=True,
            )
            enabled_wallets = {
                _normalize_wallet(
                    member.get("source_wallet") or member.get("wallet")
                )
                for member in overlay.get("members") or []
                if isinstance(member, dict) and member.get("enabled") is not False
            }
            checks["f3_not_enabled_or_cooloff_or_fading"] = bool(
                wallet not in enabled_wallets
                and wallet not in total_loss_disabled_wallets
                and not cooloff_scope["active"]
                and base.get("fading_clear") is not False
            )
        else:
            cooloff_scope = {
                "active": False,
                "cooloff_until": base.get("cooloff_until"),
                "applied": [],
                "cooloff_scope_mismatch_ignored": [],
                "known_demotion_fingerprints": [],
            }
            checks["f1_measured_positive_regime_cell"] = False
            checks["f1_concentration_admissible"] = False
            checks["f1_venue_reachable_admissible"] = False
            checks["f1_walk_forward_admissible"] = False
            checks["own_evidenced_policy_available"] = False
        checks["f2_fresh_rows_and_own_policy_copyable"] = bool(
            int(supply.get("attempts") or 0) >= 10
            and int(supply.get("copyable") or 0) > 0
        )
        wallet_holdouts = (
            exact_policy_holdouts.get(wallet)
            if isinstance(exact_policy_holdouts.get(wallet), dict)
            else {}
        )
        exact_holdout = (
            wallet_holdouts.get(fingerprint)
            if isinstance(wallet_holdouts.get(fingerprint), dict)
            else {}
        )
        checks["exact_policy_chronological_holdout_pass"] = bool(
            holdout_gate_supplied
            and exact_holdout.get("passed") is True
            and str(exact_holdout.get("wide_policy_fingerprint") or "")
            == fingerprint
        )
        exact_holdout_admission_refusal = (
            "EXACT_POLICY_HOLDOUT_COHORT_ABSENT"
            if not holdout_gate_supplied
            else "NO_EXACT_POLICY_EVIDENCE_CELL"
            if not exact_holdout
            else "EVIDENCE_CELL_FAILED"
            if not checks["exact_policy_chronological_holdout_pass"]
            else None
        )
        required_deficits = _source_drought_check_deficits(checks)
        unmodeled_checks = _source_drought_unmodeled_checks(checks)
        admission_refusal = (
            exact_holdout_admission_refusal
            if required_deficits
            and required_deficits[0] == "exact_policy_chronological_holdout_pass"
            else required_deficits[0]
            if required_deficits
            else "UNMODELED_CHECK_PRESENT"
            if unmodeled_checks
            else None
        )
        exact_regime_evidence = {
            "pnl_usd": fixed_evidence.get("post_fee_pnl_usd"),
            "roi_pct": fixed_evidence.get("roi_pct"),
            "resolved_signals": int(fixed_evidence.get("resolved") or 0),
            "first_half_post_fee_pnl_usd": fixed_evidence.get(
                "first_half_post_fee_pnl_usd"
            ),
            "second_half_post_fee_pnl_usd": fixed_evidence.get(
                "second_half_post_fee_pnl_usd"
            ),
            "pnl_excluding_top_1_market": fixed_evidence.get(
                "pnl_excluding_top_1_market"
            ),
            "top_1_market_share_pct": fixed_evidence.get(
                "top_1_market_share_pct"
            ),
            "venue_reachable_share_pct": fixed_evidence.get(
                "venue_reachable_share_pct"
            ),
            "venue_reachable_share_min_pct": fixed_evidence.get(
                "venue_reachable_share_min_pct"
            ),
            "win_rate_pct": fixed_evidence.get("win_rate_pct"),
            "concentration_admissible": fixed_evidence.get(
                "concentration_admissible"
            ),
            "genuine_concentration_edge": fixed_evidence.get(
                "genuine_concentration_edge"
            ),
            "source": "venue_executable_full_stream_rescore",
            "regime_sliced": False,
            "regime_basis": "full_stream_not_regime_sliced",
        }
        f1_slice_disclosure = _f1_slice_disclosure(
            active_slices=base.get("active_temporal_slices") or [],
            regime_evidence=exact_regime_evidence,
            checks=checks,
        )
        volume_park_superseded = _fingerprint_direct_supersedes_volume_park(
            wallet=wallet,
            lane={"wide_policy_fingerprint": fingerprint},
            evidence=exact_regime_evidence,
            exclusion=base.get("standby_exclusion"),
        )
        if volume_park_superseded:
            checks["not_terminal_park_red_clock_or_measured_loser"] = (
                not bool((base.get("standby_exclusion") or {}).get("evidenced_park"))
            )
        frontier.append(
            {
                **base,
                "paper_policy_id": (
                    policy_template.get("policy_id") if policy_template else policy_id
                ),
                "base_policy_id": policy_id,
                "wide_policy_fingerprint": fingerprint or None,
                "standby_exclusion_superseded": (
                    {
                        "status": "SUPERSEDED_FOR_FINGERPRINT_STRICT_WIDE_DIRECT",
                        "scope": "volume_lane_only",
                        "wide_policy_fingerprint": fingerprint,
                    }
                    if fingerprint_volume_park_superseded
                    else base.get("standby_exclusion_superseded")
                ),
                "policy": policy_template,
                "regime_evidence": exact_regime_evidence if exact_policy_join else {
                    "pnl_usd": None,
                    "roi_pct": None,
                    "resolved_signals": 0,
                    "source": "MISSING_EXACT_FINGERPRINT_EVIDENCE",
                    "regime_sliced": False,
                    "regime_basis": "full_stream_not_regime_sliced",
                },
                "standby_exclusion_superseded": (
                    {
                        "status": "SUPERSEDED_FOR_FINGERPRINT_STRICT_WIDE_DIRECT",
                        "scope": "volume_lane_only",
                        "wide_policy_fingerprint": fingerprint,
                    }
                    if volume_park_superseded
                    else base.get("standby_exclusion_superseded")
                ),
                "source_generation": supply.get("source_generation"),
                "source_identity": generation_identity,
                "claimed_fresh_own_source_buy_rows_30m": base.get(
                    "claimed_fresh_own_source_buy_rows_30m"
                ),
                "fresh_own_source_buy_rows_30m": (
                    int(supply.get("attempts") or 0)
                    if int(supply.get("attempts") or 0) >= 10
                    and int(supply.get("copyable") or 0) > 0
                    else int(base.get("fresh_own_source_buy_rows_30m") or 0)
                ),
                "f2_evaluated_copyable": int(supply.get("copyable") or 0),
                "f2_copyable_policy_id": generation_identity.get("policy_id"),
                "f2_copyable_is_base_cohort_policy": True,
                "cooloff_until": cooloff_scope.get("cooloff_until"),
                "total_loss_direct_fenced": wallet in total_loss_disabled_wallets,
                "cooloff_scope": cooloff_scope,
                "cooloff_scope_mismatch_ignored": cooloff_scope.get(
                    "cooloff_scope_mismatch_ignored"
                )
                or [],
                "direct_source": {
                    **base["direct_source"],
                    "attempts": int(supply.get("attempts") or 0),
                    "copyable": int(supply.get("copyable") or 0),
                    "policy_depth_pass": int(supply.get("policy_depth_pass") or 0),
                    "latest_receipt_at": supply.get("latest_receipt_at"),
                },
                **f1_slice_disclosure,
                "checks": checks,
                "admission_refusal": admission_refusal,
                "eligible": _source_drought_checks_pass(checks),
                "evidence_deficits": sorted(
                    required_deficits
                    + [f"unmodeled_check:{name}" for name in unmodeled_checks]
                    + (
                        [
                            "standby_exclusion_source_status:"
                            + str(
                                (base.get("standby_exclusion") or {}).get(
                                    "standby_exclusion_source_status"
                                )
                            )
                        ]
                        if (base.get("standby_exclusion") or {}).get(
                            "standby_exclusion_source_status"
                        )
                        == "UNRECONCILED_STALE_EXCLUSION_SOURCE"
                        else []
                    )
                ),
            }
        )
    frontier.sort(
        key=lambda row: (
            len(row["evidence_deficits"]),
            -float(row["regime_evidence"].get("second_half_post_fee_pnl_usd") or 0.0),
            -int(row["regime_evidence"].get("resolved_signals") or 0),
            -int(row["direct_source"]["copyable"]),
            row["wallet"],
            str(row.get("wide_policy_fingerprint") or ""),
            str(row["source_generation"]),
        )
    )
    if frontier:
        evidence["rows"] = frontier
        evidence["candidate_count"] = len(frontier)
        eligible = [row for row in frontier if row["eligible"]]
        evidence["eligible_count"] = len(eligible)
        evidence["selected"] = eligible[0] if eligible else None
        evidence["nearest_frontier"] = frontier[:20]
    pool_admission_dropouts.sort(
        key=lambda row: (
            str(row.get("admission_refusal") or ""),
            str(row.get("wallet") or ""),
            str(row.get("source_generation") or ""),
        )
    )
    evidence["pool_admission_dropouts"] = pool_admission_dropouts
    evidence["pool_admission_dropout_counts"] = dict(
        sorted(
            Counter(
                str(row.get("admission_refusal") or "") for row in pool_admission_dropouts
            ).items()
        )
    )
    check_names = REQUIRED_SOURCE_DROUGHT_CHECKS
    evidence.update(
        {
            "status": (
                "DIRECT_SOURCE_TARGET_SELECTED"
                if evidence.get("selected")
                else "NO_ADMISSIBLE_TARGET"
            ),
            "source_checksum": direct_source.get("checksum"),
            "frontier_checksum": _stable_checksum(frontier),
            "frontier_key": "wallet|wide_policy_fingerprint|source_generation",
            "prior_live_acceptance_required": False,
            "refusal_counts": {
                name: sum(
                    1 for row in evidence["rows"] if not row["checks"].get(name)
                )
                for name in check_names
            },
            "unmodeled_check_candidates": sum(
                1 for row in evidence["rows"]
                if _source_drought_unmodeled_checks(row.get("checks") or {})
            ),
            "unmodeled_check_names": sorted({
                name
                for row in evidence["rows"]
                for name in _source_drought_unmodeled_checks(row.get("checks") or {})
            }),
        }
    )
    evidence["gate_digits"]["f1_regime_basis"] = (
        "full_stream_not_regime_sliced"
    )
    evidence["rule"] = (
        f"{evidence.get('rule')}; F1 measures the full stream and is not "
        "conditioned on the row regime"
    )
    order140_wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    order140_row = next(
        (row for row in evidence["rows"] if row.get("wallet") == order140_wallet),
        None,
    )
    order140_regime = (
        order140_row.get("regime_evidence")
        if isinstance(order140_row, dict)
        and isinstance(order140_row.get("regime_evidence"), dict)
        else {}
    )
    order140_checks = (
        order140_row.get("checks")
        if isinstance(order140_row, dict)
        and isinstance(order140_row.get("checks"), dict)
        else {}
    )
    evidence["order140_f1_rederivation"] = {
        "wallet": order140_wallet,
        "row_present": order140_row is not None,
        "f1_measured_positive_regime_cell": order140_checks.get(
            "f1_measured_positive_regime_cell"
        ),
        "both_resolved_halves_positive": order140_checks.get(
            "both_resolved_halves_positive"
        ),
        "resolved_signals": order140_regime.get("resolved_signals"),
        "pnl_usd": order140_regime.get("pnl_usd"),
        "roi_pct": order140_regime.get("roi_pct"),
        "f4_external_liveness": order140_checks.get("f4_external_liveness"),
        "f4_basis": order140_row.get("f4_basis") if order140_row else None,
        "eligible": order140_row.get("eligible") if order140_row else None,
        "evidence_deficits": order140_row.get("evidence_deficits") if order140_row else None,
    }
    return evidence


def _candidate_supply_dropouts(
    *,
    previous: dict[str, Any],
    current: dict[str, Any],
    ready_shadow: dict[str, Any],
    cohort_admission: dict[str, Any],
    full_pool_queue: dict[str, Any],
    direct_source: dict[str, Any],
    overlay: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    previous_rows = previous.get("rows") if isinstance(previous.get("rows"), list) else []
    current_wallets = {
        _normalize_wallet(row.get("wallet"))
        for row in current.get("rows") or []
        if isinstance(row, dict)
    }
    pooled_wallets = {
        _normalize_wallet(row.get("wallet") or row.get("source_wallet"))
        for rows in (
            ready_shadow.get("lanes") or [],
            ready_shadow.get("hot_standby_ranked_candidates") or [],
            cohort_admission.get("packets") or [],
            full_pool_queue.get("ranked_members") or [],
        )
        for row in rows
        if isinstance(row, dict)
    }
    direct_wallets = {
        _normalize_wallet(row.get("wallet"))
        for row in (
            direct_source.get("per_wallet_generation") or {}
        ).values()
        if isinstance(row, dict) and int(row.get("attempts") or 0) > 0
    }
    dropouts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in previous_rows:
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("wallet"))
        if not wallet or wallet in current_wallets or wallet in seen:
            continue
        seen.add(wallet)
        if wallet not in pooled_wallets and wallet not in direct_wallets:
            reason = "absent_from_all_candidate_supply_inputs"
        elif wallet not in direct_wallets:
            reason = "absent_from_current_reconciled_direct_generation_supply"
        else:
            reason = "no_exact_generation_policy_fingerprint_join"
        dropouts.append(
            {
                "wallet": wallet,
                "previous_supply_source": row.get("supply_source"),
                "absent_reason": reason,
            }
        )

    overlay = overlay if isinstance(overlay, dict) else {}
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    pin_wallet = _normalize_wallet(pin.get("source_wallet") or pin.get("wallet"))
    pin_candidate_id = str(pin.get("candidate_id") or "")
    pin_member = next(
        (
            member
            for member in overlay.get("members") or []
            if isinstance(member, dict)
            and _normalize_wallet(member.get("source_wallet") or member.get("wallet"))
            == pin_wallet
            and (
                not pin_candidate_id
                or str(member.get("candidate_id") or "") == pin_candidate_id
            )
        ),
        {},
    )
    pin_policy = pin_member.get("policy") if isinstance(pin_member.get("policy"), dict) else {}
    pin_fingerprint = str(
        pin.get("wide_policy_fingerprint")
        or pin_policy.get("wide_policy_fingerprint")
        or pin_member.get("wide_policy_fingerprint")
        or ""
    )
    pin_rows = [
        row
        for row in current.get("rows") or []
        if isinstance(row, dict) and _normalize_wallet(row.get("wallet")) == pin_wallet
    ]
    pin_supply_fingerprints = sorted(
        {
            str(row.get("wide_policy_fingerprint") or "")
            for row in pin_rows
            if str(row.get("wide_policy_fingerprint") or "")
        }
    )
    direct_pin_supply = next(
        (
            row
            for row in (direct_source.get("per_wallet_generation") or {}).values()
            if isinstance(row, dict)
            and _normalize_wallet(row.get("wallet")) == pin_wallet
        ),
        {},
    )
    pin_absent_reason = None
    if pin.get("enabled") is not False and pin_wallet:
        if not pin_rows:
            pin_absent_reason = "pinned_wallet_absent_from_frontier"
        elif pin_fingerprint and pin_fingerprint not in pin_supply_fingerprints:
            pin_absent_reason = "pinned_fingerprint_absent_from_supply"
    if pin_absent_reason:
        dropouts = [row for row in dropouts if row.get("wallet") != pin_wallet]
        dropouts.append(
            {
                "wallet": pin_wallet,
                "wide_policy_fingerprint": pin_fingerprint or None,
                "candidate_id": pin_candidate_id or None,
                "pin_id": pin.get("pin_id"),
                "absent_reason": pin_absent_reason,
                "pinned_wallet_on_frontier": bool(pin_rows),
                "supply_fingerprints_for_pinned_wallet": pin_supply_fingerprints,
                "direct_attempts": int(direct_pin_supply.get("attempts") or 0),
                "direct_copyable": int(direct_pin_supply.get("copyable") or 0),
                "next_action": "escalate_pin_absent_and_retain_live_seat_until_rotation_authority",
            }
        )
    return sorted(dropouts, key=lambda row: row["wallet"])


def _policy_choke_rung_c_terminal(
    rung_c_candidates: dict[str, Any],
    rung_b_candidates: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": "RUNG_C_NO_ADMISSIBLE_TARGET",
        "reason": "full_evidenced_candidate_supply_dry_at_unchanged_F1_F4_bars",
        "candidate_evidence": rung_c_candidates,
        "rung_b_candidate_evidence": rung_b_candidates,
        "refusal_counts": rung_c_candidates.get("refusal_counts", {}),
        "terminal_outcome": True,
        "re_evaluate_each_heartbeat": True,
        "quality_bars_unchanged": True,
    }


def _normal_gate_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    direct_source = (
        row.get("direct_source")
        if isinstance(row.get("direct_source"), dict)
        else {}
    )
    regime_evidence = (
        row.get("regime_evidence")
        if isinstance(row.get("regime_evidence"), dict)
        else {}
    )
    return (
        -int(direct_source.get("copyable_continuity_pass") is True),
        -int(direct_source.get("attempt_continuity_pass") is True),
        -int(direct_source.get("copyable") or 0),
        -float(regime_evidence.get("pnl_usd") or 0.0),
        -float(regime_evidence.get("roi_pct") or 0.0),
        _normalize_wallet(row.get("wallet")),
        str(row.get("wide_policy_fingerprint") or ""),
        str(row.get("source_generation") or ""),
    )


def _source_generation_order(identity: Any) -> datetime | None:
    """Return a comparable WIDE generation clock without ordering hashes."""

    identity = identity if isinstance(identity, dict) else {}
    for key in ("generated_at", "updated_at"):
        parsed = _parse_ts(identity.get(key))
        if parsed is not None:
            return parsed
    run_id = str(identity.get("run_id") or "")
    match = re.search(r"(?:^|_)(\d{8}T\d{6}Z)(?:$|_)", run_id)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def _source_generation_transition(
    previous_observation: dict[str, Any],
    row: dict[str, Any],
) -> str:
    previous_generation = str(previous_observation.get("source_generation") or "")
    current_generation = str(row.get("source_generation") or "")
    if previous_generation and previous_generation == current_generation:
        return "SAME"
    previous_order = _source_generation_order(
        previous_observation.get("source_identity")
    )
    current_order = _source_generation_order(row.get("source_identity"))
    if previous_order is None or current_order is None or previous_order == current_order:
        return "UNKNOWN"
    return "FORWARD" if current_order > previous_order else "BACKWARD"


def _wide_generation_churn(
    journal: list[dict[str, Any]],
    *,
    required_stability_s: float = 1920.0,
) -> dict[str, Any]:
    generations: dict[str, dict[str, Any]] = {}
    for envelope in journal:
        if not isinstance(envelope, dict):
            continue
        generation = str(envelope.get("source_generation") or "")
        identity = envelope.get("identity") if isinstance(envelope.get("identity"), dict) else {}
        order = _source_generation_order(identity)
        if not generation or order is None:
            continue
        generations[generation] = {
            "source_generation": generation,
            "run_id": identity.get("run_id"),
            "generation_at": order.isoformat(),
            "order": order,
        }
    ordered = sorted(generations.values(), key=lambda item: item["order"])
    intervals = [
        round((right["order"] - left["order"]).total_seconds(), 6)
        for left, right in zip(ordered, ordered[1:])
    ]
    rows = [
        {key: value for key, value in row.items() if key != "order"}
        for row in ordered
    ]
    return {
        "status": "MEASURED" if intervals else "INSUFFICIENT_GENERATIONS",
        "generation_count": len(rows),
        "generations": rows,
        "intervals_s": intervals,
        "min_interval_s": min(intervals) if intervals else None,
        "median_interval_s": statistics.median(intervals) if intervals else None,
        "max_interval_s": max(intervals) if intervals else None,
        "required_stability_s": required_stability_s,
        "below_required_count": sum(
            interval < required_stability_s for interval in intervals
        ),
        "d3_required": bool(
            intervals and min(intervals) < required_stability_s
        ),
        "rule": (
            "generation hashes are never ordered; compare the timestamp encoded "
            "by the generation identity run_id"
        ),
    }


def _incumbent_allpass_row(
    *,
    candidates: dict[str, Any],
    overlay: dict[str, Any],
    source_generation: str = "",
) -> dict[str, Any] | None:
    """Rebuild the seated DIRECT row while waiving only F3 incumbency."""

    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    if pin.get("pin_id") != POLICY_CHOKE_RUNG_B_PIN_ID or pin.get("enabled") is False:
        return None
    wallet = _normalize_wallet(pin.get("source_wallet") or pin.get("wallet"))
    candidate_id = str(pin.get("candidate_id") or "")
    member = next(
        (
            row
            for row in reversed(overlay.get("members") or [])
            if isinstance(row, dict)
            and row.get("enabled") is not False
            and _normalize_wallet(row.get("source_wallet") or row.get("wallet")) == wallet
            and (not candidate_id or str(row.get("candidate_id") or "") == candidate_id)
        ),
        {},
    )
    fingerprint = _member_wide_fingerprint(member)
    matches: list[dict[str, Any]] = []
    for row in candidates.get("rows") or []:
        if not isinstance(row, dict) or _normalize_wallet(row.get("wallet")) != wallet:
            continue
        if source_generation and str(row.get("source_generation") or "") != source_generation:
            continue
        if fingerprint and str(row.get("wide_policy_fingerprint") or "") != fingerprint:
            continue
        checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
        other_checks = {
            key: value
            for key, value in checks.items()
            if key != "f3_not_enabled_or_cooloff_or_fading"
        }
        if (
            checks.get("f3_not_enabled_or_cooloff_or_fading") is False
            and other_checks
            and all(value is True for value in other_checks.values())
            and row.get("wallet_cooloff_active") is False
            and row.get("fading_clear") is True
        ):
            matches.append(
                {
                    **row,
                    "eligible": True,
                    "checks": {
                        **checks,
                        "f3_not_enabled_or_cooloff_or_fading": True,
                    },
                    "incumbency_f3_waived": True,
                }
            )
    return min(matches, key=_normal_gate_rank) if matches else None


def _freeze_refusal_with_grace(
    result: dict[str, Any],
    *,
    status: str,
    previous_candidates: dict[str, Any],
    freeze_wallet: str,
    freeze_fingerprint: str,
    now: datetime | None,
) -> tuple[dict[str, Any], bool]:
    """Carry one same-freeze heartbeat without manufacturing strict evidence."""

    result["selected"] = None
    result["status"] = status
    result["selection_authority"] = "freeze_allpass_sidecar_exact_identity"
    previous = (
        previous_candidates.get("normal_gate_unique_allpass_observation")
        if isinstance(
            previous_candidates.get("normal_gate_unique_allpass_observation"), dict
        )
        else {}
    )
    same_freeze = bool(
        _normalize_wallet(previous.get("wallet")) == freeze_wallet
        and str(previous.get("wide_policy_fingerprint") or "")
        == freeze_fingerprint
    )
    prior_gap = int(previous.get("f2_gap_heartbeats") or 0)
    if same_freeze and prior_gap < 1 and now is not None:
        first_observed_at = _parse_ts(previous.get("first_observed_at")) or now
        result["normal_gate_unique_allpass_observation"] = {
            **previous,
            "last_observed_at": now.isoformat(),
            "elapsed_s": round((now - first_observed_at).total_seconds(), 6),
            "f2_gap_heartbeats": prior_gap + 1,
            "f2_gap_last_at": now.isoformat(),
            "freeze_refusal_grace": {
                "status": status,
                "bounded_heartbeats": 1,
                "consecutive_heartbeats_incremented": False,
            },
        }
    else:
        result["normal_gate_unique_allpass_observation"] = None
    return result, True


def _enforce_freeze_only_direct_authority(
    *,
    candidates: dict[str, Any],
    freeze_allpass_sidecar: dict[str, Any],
    previous_candidates: dict[str, Any] | None = None,
    active_set_overlay: dict[str, Any] | None = None,
    now: datetime | None = None,
    money_and_tripwires_clear: bool = True,
) -> tuple[dict[str, Any], bool]:
    """Fail closed unless the exact freeze primary has cleared every gate.

    Generic WIDE frontier winners are paper evidence, not autonomous live-pin
    authority.  The current Exit-A contract grants the DIRECT actuator only to
    the exact freeze identity once its sidecar is non-refused ALL_PASS_READY.
    """

    result = dict(candidates)
    previous_candidates = (
        previous_candidates if isinstance(previous_candidates, dict) else {}
    )
    primary = (
        freeze_allpass_sidecar.get("primary")
        if isinstance(freeze_allpass_sidecar.get("primary"), dict)
        else {}
    )
    freeze_wallet = _normalize_wallet(primary.get("wallet"))
    freeze_fingerprint = str(primary.get("wide_policy_fingerprint") or "")
    freeze_direct_eligible = bool(
        freeze_allpass_sidecar.get("status") == "ALL_PASS_READY"
        and (freeze_allpass_sidecar.get("checks") or {}).get("all_pass") is True
        and (freeze_allpass_sidecar.get("actuator_contract") or {}).get(
            "eligible_to_invoke"
        )
        is True
        and freeze_wallet
        and freeze_fingerprint
    )
    freeze_matches = [
        row
        for row in result.get("rows") or []
        if isinstance(row, dict)
        and row.get("eligible") is True
        and _normalize_wallet(row.get("wallet")) == freeze_wallet
        and str(row.get("wide_policy_fingerprint") or "") == freeze_fingerprint
    ]
    if freeze_direct_eligible:
        if not freeze_matches:
            return _freeze_refusal_with_grace(
                result,
                status="FREEZE_ALLPASS_IDENTITY_ABSENT_FROM_FRONTIER",
                previous_candidates=previous_candidates,
                freeze_wallet=freeze_wallet,
                freeze_fingerprint=freeze_fingerprint,
                now=now,
            )
        if len(freeze_matches) > 1:
            result["freeze_duplicate_key_rows"] = len(freeze_matches)
            return _freeze_refusal_with_grace(
                result,
                status="FREEZE_ALLPASS_IDENTITY_LOST_DURING_WALLET_COLLAPSE",
                previous_candidates=previous_candidates,
                freeze_wallet=freeze_wallet,
                freeze_fingerprint=freeze_fingerprint,
                now=now,
            )
        result["freeze_allpass_observation_fence"] = {
            "wallet": freeze_wallet,
            "wide_policy_fingerprint": freeze_fingerprint,
            "status": "EXACT_IDENTITY_MUST_CLEAR_NORMAL_GATE_OBSERVATION",
        }

    eligible_rows = [
        row
        for row in result.get("rows") or []
        if isinstance(row, dict) and row.get("eligible") is True
    ]
    raw_eligible_rows = eligible_rows
    eligible_by_wallet: dict[str, list[dict[str, Any]]] = {}
    for eligible_row in raw_eligible_rows:
        eligible_by_wallet.setdefault(
            _normalize_wallet(eligible_row.get("wallet")), []
        ).append(eligible_row)
    eligible_rows = []
    for wallet_rows in eligible_by_wallet.values():
        wallet_rows = sorted(wallet_rows, key=_normal_gate_rank)
        wallet = _normalize_wallet(wallet_rows[0].get("wallet"))
        exact_freeze_rows = [
            row
            for row in wallet_rows
            if freeze_direct_eligible
            and wallet == freeze_wallet
            and str(row.get("wide_policy_fingerprint") or "")
            == freeze_fingerprint
        ]
        selected_row = (
            exact_freeze_rows[0] if len(exact_freeze_rows) == 1 else wallet_rows[0]
        )
        selected_policy = dict(selected_row)
        if exact_freeze_rows:
            exact_rank = _normal_gate_rank(exact_freeze_rows[0])
            outranked = [
                row for row in wallet_rows if _normal_gate_rank(row) < exact_rank
            ]
            previous_preselection = (
                previous_candidates.get("freeze_exact_preselection")
                if isinstance(
                    previous_candidates.get("freeze_exact_preselection"), dict
                )
                else {}
            )
            current_generation = str(
                exact_freeze_rows[0].get("source_generation") or ""
            )
            previous_generation = str(
                previous_preselection.get("source_generation") or ""
            )
            previous_streak = int(
                previous_preselection.get("consecutive_outranked_generations") or 0
            )
            if not outranked:
                outranked_streak = 0
            elif previous_generation == current_generation:
                outranked_streak = max(1, previous_streak)
            else:
                transition = _source_generation_transition(
                    {
                        "source_generation": previous_generation,
                        "source_identity": previous_preselection.get(
                            "source_identity"
                        ),
                    },
                    exact_freeze_rows[0],
                )
                outranked_streak = (
                    previous_streak + 1
                    if previous_streak and transition == "FORWARD"
                    else 1
                )
            preselection_status = (
                "FREEZE_EXACT_PERSISTENTLY_OUTRANKED"
                if outranked_streak >= 3
                else "FREEZE_EXACT_PRESELECTED"
            )
            result["freeze_exact_preselection"] = {
                "status": preselection_status,
                "wallet": freeze_wallet,
                "wide_policy_fingerprint": freeze_fingerprint,
                "source_generation": current_generation,
                "source_identity": exact_freeze_rows[0].get("source_identity"),
                "exact_rank": list(exact_rank),
                "outranked_siblings": [
                    {
                        "wide_policy_fingerprint": row.get(
                            "wide_policy_fingerprint"
                        ),
                        "source_generation": row.get("source_generation"),
                        "rank": list(_normal_gate_rank(row)),
                    }
                    for row in outranked
                ],
                "consecutive_outranked_generations": outranked_streak,
            }
            if outranked_streak >= 3:
                result["requires_fable_ping"] = True
        alternates = [
            str(row.get("wide_policy_fingerprint") or "")
            for row in wallet_rows
            if row is not selected_row
            if str(row.get("wide_policy_fingerprint") or "")
        ]
        if alternates:
            selected_policy["collapsed_same_wallet_alternates"] = alternates
        eligible_rows.append(selected_policy)
    result["raw_eligible_count"] = len(raw_eligible_rows)
    result["eligible_count"] = len(eligible_rows)
    previous_observation = (
        previous_candidates.get("normal_gate_unique_allpass_observation")
        if isinstance(
            previous_candidates.get("normal_gate_unique_allpass_observation"),
            dict,
        )
        else {}
    )
    original_previous_observation = dict(previous_observation)
    observed_wallet = _normalize_wallet(previous_observation.get("wallet"))
    observed_fingerprint = str(previous_observation.get("wide_policy_fingerprint") or "")
    observed_consecutive = int(previous_observation.get("consecutive_heartbeats") or 0)
    if observed_consecutive >= 1 and observed_wallet and observed_fingerprint:
        incumbent_matches = [
            row
            for row in eligible_rows
            if _normalize_wallet(row.get("wallet")) == observed_wallet
            and str(row.get("wide_policy_fingerprint") or "") == observed_fingerprint
            and _source_generation_transition(previous_observation, row)
            in {"SAME", "FORWARD"}
        ]
        if len(incumbent_matches) == 1:
            deferred = [
                str(row.get("wide_policy_fingerprint") or "")
                for row in eligible_rows
                if row is not incumbent_matches[0]
                and str(row.get("wide_policy_fingerprint") or "")
            ]
            eligible_rows = incumbent_matches
            result["eligible_count"] = 1
            if deferred:
                result["deferred_allpass_challengers"] = deferred
        elif eligible_rows:
            result["requires_fable_ping"] = True
            result["normal_gate_unique_allpass_observation"] = None
            result["inflight_observation_lost_eligibility"] = {
                "wallet": observed_wallet,
                "wide_policy_fingerprint": observed_fingerprint,
            }
            previous_observation = {}
    if freeze_direct_eligible:
        exact_freeze_rows = [
            row
            for row in eligible_rows
            if _normalize_wallet(row.get("wallet")) == freeze_wallet
            and str(row.get("wide_policy_fingerprint") or "") == freeze_fingerprint
        ]
        if len(exact_freeze_rows) != 1:
            return _freeze_refusal_with_grace(
                result,
                status="FREEZE_ALLPASS_IDENTITY_LOST_DURING_WALLET_COLLAPSE",
                previous_candidates=previous_candidates,
                freeze_wallet=freeze_wallet,
                freeze_fingerprint=freeze_fingerprint,
                now=now,
            )
        deferred = [
            str(row.get("wide_policy_fingerprint") or "")
            for row in eligible_rows
            if row is not exact_freeze_rows[0]
            and str(row.get("wide_policy_fingerprint") or "")
        ]
        eligible_rows = exact_freeze_rows
        result["eligible_count"] = 1
        if deferred:
            result["deferred_allpass_challengers"] = deferred
    elif len(eligible_rows) >= 2:
        eligible_rows.sort(key=_normal_gate_rank)
        winner = eligible_rows[0]
        deferred = [
            str(row.get("wide_policy_fingerprint") or "")
            for row in eligible_rows[1:]
            if str(row.get("wide_policy_fingerprint") or "")
        ]
        eligible_rows = [winner]
        result["eligible_count"] = 1
        if deferred:
            result["deferred_allpass_challengers"] = deferred
        result["requires_fable_ping"] = True

    row = eligible_rows[0] if len(eligible_rows) == 1 else None
    checks = row.get("checks") if isinstance(row, dict) and isinstance(row.get("checks"), dict) else {}
    regime_evidence = (
        row.get("regime_evidence")
        if isinstance(row, dict) and isinstance(row.get("regime_evidence"), dict)
        else {}
    )
    strict_allpass = bool(
        row
        and result.get("eligible_count") == 1
        and not (row.get("evidence_deficits") or [])
        and checks
        and all(value is True for value in checks.values())
        and checks.get("f2_fresh_rows_and_own_policy_copyable") is True
        and int(row.get("fresh_own_source_buy_rows_30m") or 0) >= 10
        and int(row.get("f2_evaluated_copyable") or 0) > 0
        and float(regime_evidence.get("pnl_usd") or 0.0) > 0.0
        and float(regime_evidence.get("roi_pct") or 0.0) > 0.0
        and int(regime_evidence.get("resolved_signals") or 0) >= 200
        and checks.get("both_resolved_halves_positive") is True
        and money_and_tripwires_clear
    )
    if strict_allpass and now is not None:
        previous = previous_observation
        identity = {
            "wallet": _normalize_wallet(row.get("wallet")),
            "wide_policy_fingerprint": str(row.get("wide_policy_fingerprint") or ""),
            "source_generation": str(row.get("source_generation") or ""),
            "source_identity": (
                dict(row.get("source_identity"))
                if isinstance(row.get("source_identity"), dict)
                else {}
            ),
            "frontier_key": str(result.get("frontier_key") or ""),
        }
        transition_previous = previous
        if (
            not transition_previous
            and _normalize_wallet(original_previous_observation.get("wallet"))
            == identity["wallet"]
            and str(
                original_previous_observation.get("wide_policy_fingerprint") or ""
            )
            == identity["wide_policy_fingerprint"]
        ):
            transition_previous = original_previous_observation
        generation_transition = _source_generation_transition(
            transition_previous, row
        )
        same_identity = bool(
            previous.get("wallet") == identity["wallet"]
            and previous.get("wide_policy_fingerprint")
            == identity["wide_policy_fingerprint"]
            and previous.get("frontier_key") == identity["frontier_key"]
            and generation_transition in {"SAME", "FORWARD"}
        )
        first_observed_at = (
            _parse_ts(previous.get("first_observed_at")) if same_identity else None
        ) or now
        consecutive = int(previous.get("consecutive_heartbeats") or 0) + 1 if same_identity else 1
        observation = {
            **identity,
            "first_observed_at": first_observed_at.isoformat(),
            "last_observed_at": now.isoformat(),
            "consecutive_heartbeats": consecutive,
            "elapsed_s": round((now - first_observed_at).total_seconds(), 6),
            "money_and_tripwires_clear": True,
            "f2_gap_heartbeats": 0,
            "source_generation_transition": generation_transition,
        }
        result["normal_gate_unique_allpass_observation"] = observation
        if consecutive >= 2 and (now - first_observed_at).total_seconds() >= 300.0:
            incumbent = _incumbent_allpass_row(
                candidates=result,
                overlay=(
                    active_set_overlay
                    if isinstance(active_set_overlay, dict)
                    else {}
                ),
                source_generation=str(row.get("source_generation") or ""),
            )
            if incumbent is not None:
                challenger_rank = _normal_gate_rank(row)
                incumbent_rank = _normal_gate_rank(incumbent)
                if not challenger_rank < incumbent_rank:
                    result["selected"] = None
                    result["status"] = "NORMAL_GATE_ALLPASS_TURNOVER_REFUSED"
                    result["allpass_turnover_refusal"] = {
                        "status": "allpass_turnover_refused_incumbent_outranks",
                        "challenger_wallet": _normalize_wallet(row.get("wallet")),
                        "challenger_rank": list(challenger_rank),
                        "incumbent_wallet": _normalize_wallet(incumbent.get("wallet")),
                        "incumbent_rank": list(incumbent_rank),
                        "f3_waiver": "incumbency_only",
                    }
                    return result, False
            result["selected"] = row
            result["status"] = "NORMAL_GATE_UNIQUE_ALLPASS_SELECTED"
            result["selection_authority"] = "normal_gate_unique_all_pass"
            return result, True
        result["selected"] = None
        result["status"] = "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
        result["selection_authority"] = "normal_gate_unique_all_pass"
        return result, False

    if previous_observation and now is not None:
        f2_gap_row = _normal_gate_f2_gap_row(
            rows=[row for row in result.get("rows") or [] if isinstance(row, dict)],
            observation=previous_observation,
            frontier_key=str(result.get("frontier_key") or ""),
            money_and_tripwires_clear=money_and_tripwires_clear,
        )
        if f2_gap_row is not None:
            prior_gap_heartbeats = int(previous_observation.get("f2_gap_heartbeats") or 0)
            if prior_gap_heartbeats < 1:
                first_observed_at = (
                    _parse_ts(previous_observation.get("first_observed_at"))
                    or now
                )
                observation = {
                    **previous_observation,
                    "last_observed_at": now.isoformat(),
                    "elapsed_s": round((now - first_observed_at).total_seconds(), 6),
                    "money_and_tripwires_clear": True,
                    "f2_gap_heartbeats": prior_gap_heartbeats + 1,
                    "f2_gap_last_at": now.isoformat(),
                    "f2_gap_source_generation": f2_gap_row.get("source_generation"),
                }
                result["normal_gate_unique_allpass_observation"] = observation
                result["selected"] = None
                result["status"] = "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING_F2_GAP"
                result["selection_authority"] = "normal_gate_unique_all_pass"
                return result, False

    result["selected"] = None
    result["status"] = "MEASURED_EMPTY_SEAT_HOLD_FREEZE_NOT_ALL_PASS"
    result["selection_authority"] = "freeze_allpass_sidecar_exact_identity"
    result["normal_gate_unique_allpass_observation"] = None
    return result, False


def _forced_measured_positive_seat_candidate(
    candidates: dict[str, Any],
) -> dict[str, Any] | None:
    """Choose a bounded $1 emergency seat when our liveness gates empty it.

    This is deliberately narrower than changing the normal F1-F4 candidate
    contract.  An explicit forced sweep may waive only the current-supply F2
    and external-liveness F4 legs; the cell must still be economically
    positive, walk-forward/venue/concentration admissible, positive in both
    resolved halves, temporally non-negative, cooloff-clear, unparked, and
    joined to its own evidenced exact policy.
    """
    required = (
        "f1_measured_positive_regime_cell",
        "f1_walk_forward_admissible",
        "f1_concentration_admissible",
        "f1_venue_reachable_admissible",
        "both_resolved_halves_positive",
        "active_temporal_not_proven_negative",
        "active_temporal_regime_cell_measured",
        "f3_not_enabled_or_cooloff_or_fading",
        "not_terminal_park_red_clock_or_measured_loser",
        "own_evidenced_policy_available",
    )
    eligible: list[dict[str, Any]] = []
    for row in candidates.get("rows") or []:
        if not isinstance(row, dict):
            continue
        checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
        if not all(checks.get(key) is True for key in required):
            continue
        policy = row.get("policy") if isinstance(row.get("policy"), dict) else {}
        if not _normalize_wallet(row.get("wallet")) or not policy:
            continue
        eligible.append(
            {
                **row,
                "eligible": True,
                "forced_measured_positive_seat": True,
                "waived_liveness_checks": [
                    "f2_fresh_rows_and_own_policy_copyable",
                    "f4_external_liveness",
                ],
                "kill_line": {
                    "post_fee_pnl_usd_lte": -4.0,
                    "active_temporal_proven_negative": True,
                    "pin_ttl_s": POLICY_CHOKE_RUNG_B_TTL_S,
                },
            }
        )
    eligible.sort(
        key=lambda row: (
            -int((row.get("direct_source") or {}).get("copyable_continuity_pass") is True),
            -int((row.get("direct_source") or {}).get("attempt_continuity_pass") is True),
            -int((row.get("direct_source") or {}).get("copyable") or 0),
            -float((row.get("regime_evidence") or {}).get("pnl_usd") or 0.0),
            -float((row.get("regime_evidence") or {}).get("roi_pct") or 0.0),
            str(row.get("wallet") or ""),
        )
    )
    return eligible[0] if eligible else None


def _execute_policy_choke_rung_b(
    *,
    overlay: dict[str, Any],
    candidate: dict[str, Any] | None,
    now: datetime,
    probe_cap_usd: float = 1.0,
    dry_run: bool = False,
    supply_rung: str = "B",
    admission_authority: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    pin_expiry = _parse_ts(pin.get("expires_at"))
    active_pin = bool(
        pin.get("enabled") is not False
        and pin_expiry is not None
        and pin_expiry > now
    )
    if active_pin and pin.get("pin_id") != POLICY_CHOKE_RUNG_B_PIN_ID:
        return overlay, {
            "status": "ACTIVE_SELECTION_PIN_AUTHORITY_PRESERVED",
            "selection_pin": pin,
            "rule": "deadman cannot replace an enabled unexpired external selection pin",
        }
    if pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID and active_pin:
        pinned_candidate_id = str(pin.get("candidate_id") or "")
        repaired_members: list[dict[str, Any]] = []
        policy_repaired = False
        for row in overlay.get("members") or []:
            if not isinstance(row, dict) or str(row.get("candidate_id") or "") != pinned_candidate_id:
                repaired_members.append(row)
                continue
            member_policy = row.get("policy") if isinstance(row.get("policy"), dict) else {}
            if not str(member_policy.get("policy_id") or "").startswith("wide_fp_"):
                repaired_members.append(row)
                continue
            request_cap = min(
                1.0,
                float(row.get("max_order_usd") or member_policy.get("max_order_usd") or 1.0),
            )
            funded_policy = {
                **member_policy,
                "max_order_usd": request_cap,
                "maker_min_share_funding_cap_usd": 2.5,
                "maker_min_share_original_policy_cap_usd": max(
                    2.5,
                    float(
                        member_policy.get("maker_min_share_original_policy_cap_usd")
                        or 4.0
                    ),
                ),
                "maker_min_share_base_request_cap_usd": request_cap,
            }
            policy_repaired = funded_policy != member_policy
            repaired_members.append({**row, "policy": funded_policy})
        if policy_repaired:
            updated = {
                **overlay,
                "members": repaired_members,
                "updated_at": now.isoformat(),
            }
            return updated, {
                "status": "DIRECT_SOURCE_SELECTION_POLICY_REPAIRED",
                "selection_pin": pin,
                "pin_expiry_preserved": pin.get("expires_at"),
                "maker_min_share_funding": {
                    "base_request_cap_usd": 1.0,
                    "funding_cap_usd": 2.5,
                    "shares": 5.0,
                },
            }
        status = (
            "RUNG_B_ALREADY_ACTIVE" if supply_rung == "B"
            else "DIRECT_SOURCE_SELECTION_ALREADY_ACTIVE" if supply_rung == "DIRECT"
            else "RECOVERY_SELECTION_ALREADY_ACTIVE" if supply_rung == "RECOVERY"
            else "RUNG_C_FULL_POOL_SWEEP_ALREADY_ACTIVE"
        )
        report = {"status": status, "selection_pin": pin}
        if pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID:
            report["policy_choke_rung_b_refusal"] = _policy_choke_rung_b_refusal_record(
                selection_pin=pin,
                candidate_evidence={},
                overlay=overlay,
                now=now,
                supply_rung=supply_rung,
            )
        return overlay, report
    if not candidate or not candidate.get("eligible"):
        return overlay, {"status": "RUNG_C_METHOD_SWITCH_DUE", "reason": "no_F1_F4_rung_b_candidate"}
    if not dry_run and admission_authority is not None and admission_authority.get("authorized") is not True:
        return overlay, {
            "status": "DIRECT_PIN_ADMISSION_REFUSED_EMPTY",
            "reason": "no submitted order or fresh policy-compatible 01a BUY evidence",
            "admission_authority": admission_authority,
        }
    guard_temporal_negative = [
        row
        for row in candidate.get("active_temporal_slices") or []
        if isinstance(row, dict)
        and str(row.get("label") or "").upper() == "PROVEN-NEGATIVE"
    ]
    if guard_temporal_negative:
        return overlay, {
            "status": "REFUSED_GUARD_TEMPORAL_CROSSCHECK",
            "reason": "candidate excluded by guard-equivalent venue temporal evidence",
            "candidate_wallet": candidate.get("wallet"),
            "active_temporal_slices": guard_temporal_negative,
        }
    wallet = _normalize_wallet(candidate.get("wallet"))
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    policy_id = str(candidate.get("paper_policy_id") or policy.get("policy_id") or "")
    if not wallet or not policy_id or not policy:
        return overlay, {"status": "RUNG_C_METHOD_SWITCH_DUE", "reason": "candidate_exact_policy_missing"}
    size_usd = min(1.0, max(0.0, float(probe_cap_usd)))
    policy_request_cap = min(
        size_usd, float(policy.get("max_order_usd") or size_usd)
    )
    funded_policy = {
        **policy,
        "max_order_usd": policy_request_cap,
    }
    if str(policy_id).startswith("wide_fp_"):
        funded_policy.update(
            {
                "maker_min_share_funding_cap_usd": 2.5,
                "maker_min_share_original_policy_cap_usd": max(
                    2.5,
                    float(
                        policy.get("maker_min_share_original_policy_cap_usd")
                        or 4.0
                    ),
                ),
                "maker_min_share_base_request_cap_usd": policy_request_cap,
            }
        )
    expires = now + timedelta(seconds=POLICY_CHOKE_RUNG_B_TTL_S)
    candidate_id = f"policy_choke_rung_{supply_rung.lower()}_{wallet[-10:]}"
    member = {
        "candidate_id": candidate_id,
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": wallet,
        "enabled": True,
        "status": f"POLICY_CHOKE_RUNG_{supply_rung}_EMERGENCY_ADMISSION",
        "policy_id": policy_id,
        "policy": funded_policy,
        "copy_size_usd": size_usd,
        "max_order_usd": policy_request_cap,
        "kill_line": candidate.get("kill_line"),
        "operator_emergency_seat": candidate.get("operator_emergency_seat"),
        "source_liveness_evidence": candidate.get("source_liveness_evidence"),
        "summary": {"direction_id": POLICY_CHOKE_RUNG_B_DIRECTION_ID, "evidence": candidate},
    }
    selection_pin = {
        "enabled": True,
        "pin_id": POLICY_CHOKE_RUNG_B_PIN_ID,
        "direction_id": POLICY_CHOKE_RUNG_B_DIRECTION_ID,
        "created_at": now.isoformat(),
        "expires_at": expires.isoformat(),
        "candidate_id": candidate_id,
        "source_wallet": wallet,
        "reason": f"POLICY_CHOKE rung {supply_rung} emergency admission under own evidenced policy",
        "kill_line": candidate.get("kill_line"),
        "operator_emergency_seat": candidate.get("operator_emergency_seat"),
        "source_liveness_evidence": candidate.get("source_liveness_evidence"),
    }
    report = {
        "status": (
            f"RUNG_{supply_rung}_DRY_RUN_PASS"
            if dry_run
            else "RUNG_B_ADMISSION_PIN_WRITTEN"
            if supply_rung == "B"
            else "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"
            if supply_rung == "DIRECT"
            else "RECOVERY_SELECTION_PIN_WRITTEN"
            if supply_rung == "RECOVERY"
            else "RUNG_C_FULL_POOL_SWEEP_ADMISSION_PIN_WRITTEN"
        ),
        "candidate": candidate,
        "member": member,
        "selection_pin": selection_pin,
        "ttl_s": POLICY_CHOKE_RUNG_B_TTL_S,
        "single_submitter_invariant": "run_wallet_copy_live_guard.py remains the only order submitter",
    }
    if selection_pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID:
        report["policy_choke_rung_b_refusal"] = _policy_choke_rung_b_refusal_record(
            selection_pin=selection_pin,
            candidate_evidence={},
            overlay=overlay,
            now=now,
            supply_rung=supply_rung,
        )
    if dry_run:
        return overlay, report
    updated = dict(overlay)
    updated["members"] = [*(overlay.get("members") or []), member]
    updated["selection_pin"] = selection_pin
    updated["latest_policy_choke_rung_b_admission"] = report
    updated["updated_at"] = now.isoformat()
    return updated, report


def _direct_pin_admission_authority(
    *,
    candidate: dict[str, Any] | None,
    ledger: dict[str, Any],
    hot_history: dict[str, Any],
    qualified_pool_stakeout: dict[str, Any] | None = None,
    previous_admission_authority: dict[str, Any] | None = None,
    now: datetime,
    max_age_s: float = 30.0,
) -> dict[str, Any]:
    admission_history = _union_qualified_pool_stakeout(
        hot_history,
        qualified_pool_stakeout or {},
        candidate=candidate,
    )
    wallet = _normalize_wallet((candidate or {}).get("wallet"))
    previous_admission_authority = (
        previous_admission_authority
        if isinstance(previous_admission_authority, dict)
        else {}
    )
    previous_check = (
        _parse_ts(previous_admission_authority.get("checked_at"))
        if _normalize_wallet(previous_admission_authority.get("wallet")) == wallet
        else None
    )
    submitted = (
        _accepted_orders_for_wallet(
            ledger,
            wallet,
            since=now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S),
            until=now,
        )
        if wallet
        else 0
    )
    qualifying: list[dict[str, Any]] = []
    rows_scanned = 0
    refusal_counts: Counter[str] = Counter()
    ingest_ages: list[float] = []
    clock_skews: list[float] = []
    current_window_start_s = int(now.timestamp()) // 300 * 300
    current_window_start = datetime.fromtimestamp(
        current_window_start_s, tz=timezone.utc
    )
    interval_start = (
        max(previous_check - timedelta(seconds=max_age_s), current_window_start)
        if previous_check
        else current_window_start
    )
    interval_source = "window_open" if interval_start == current_window_start else "last_check"
    for row in admission_history.get("events") or []:
        if not isinstance(row, dict):
            continue
        event_s = _as_float(
            row.get("event_ts") or row.get("block_ts") or row.get("observed_ts")
        )
        received_s = _as_float(
            row.get("received_at_s") or row.get("observed_ts") or event_s
        )
        price = _as_float(row.get("price") or row.get("source_price"))
        source = str(row.get("source") or row.get("detection_source") or "").lower()
        market_slug = str(row.get("market_slug") or row.get("market") or "")
        market_match = re.search(r"(?:btc-updown-5m-|btc-5m-)(\d{10})$", market_slug)
        candidate_current_source_row = bool(
            wallet
            and _normalize_wallet(row.get("source_wallet")) == wallet
            and str(row.get("action") or "").upper() == "BUY"
            and source in DIRECT_ADMISSION_EVENT_SOURCES
            and market_match is not None
            and int(market_match.group(1)) == current_window_start_s
        )
        if candidate_current_source_row:
            rows_scanned += 1
            if event_s is not None and received_s is not None:
                raw_ingest_age = received_s - event_s
                ingest_ages.append(max(0.0, raw_ingest_age))
                if raw_ingest_age < 0:
                    clock_skews.append(raw_ingest_age)
        if not candidate_current_source_row:
            continue
        refusal_reason = None
        if price is None or not 0.25 <= price < 0.32:
            refusal_reason = "price_out_of_band"
        elif event_s is None or received_s is None or received_s - event_s > max_age_s:
            refusal_reason = "ingest_age_gt_30s"
        elif event_s < interval_start.timestamp():
            refusal_reason = "event_before_interval_start"
            if (
                previous_check is not None
                and event_s >= current_window_start.timestamp()
                and received_s > previous_check.timestamp()
            ):
                refusal_counts["event_before_interval_start_unseen"] += 1
        elif event_s > now.timestamp():
            refusal_reason = "event_after_now"
        if refusal_reason is not None:
            refusal_counts[refusal_reason] += 1
            continue
        market_start_s = int(market_match.group(1))
        if not market_start_s <= now.timestamp() < market_start_s + 300:
            continue
        qualifying.append(
            {
                "event_id": row.get("event_id") or row.get("source_fingerprint"),
                "event_ts": event_s,
                "received_at_s": received_s,
                "ingest_age_s": round(received_s - event_s, 6),
                "price": price,
                "source": source,
                "market_slug": market_slug,
            }
        )
    qualifying.sort(key=lambda row: float(row["event_ts"]), reverse=True)
    winning_row = qualifying[0] if qualifying else None
    return {
        "authorized": submitted > 0 or bool(qualifying),
        "checked_at": now.isoformat(),
        "wallet": wallet or None,
        "orders_submitted": submitted,
        "fresh_policy_compatible_01a_buy": winning_row,
        "admission_interval_read": {
            "interval_start": interval_start.isoformat(),
            "interval_end": now.isoformat(),
            "interval_source": interval_source,
            "rows_scanned": rows_scanned,
            "rows_qualifying": len(qualifying),
            "refusal_counts": dict(sorted(refusal_counts.items())),
            "ingest_age_s_histogram": {
                "lte_5": sum(value <= 5 for value in ingest_ages),
                "gt_5_lte_15": sum(5 < value <= 15 for value in ingest_ages),
                "gt_15_lte_30": sum(15 < value <= 30 for value in ingest_ages),
                "gt_30": sum(value > 30 for value in ingest_ages),
                "sample_count": len(ingest_ages),
                "negative_clock_skew_count": len(clock_skews),
                "minimum_raw_clock_skew_s": round(min(clock_skews), 6) if clock_skews else None,
            },
            "winning_row": winning_row,
            "ingest_freshness_max_s": max_age_s,
            "current_window_open_at": current_window_start.isoformat(),
        },
        "admission_read_gate": (
            admission_history.get("qualified_pool_orderfilled_union") or {}
        ).get("admission_read_gate"),
        "admission_event_sources": sorted(DIRECT_ADMISSION_EVENT_SOURCES),
        "rule": "orders_submitted>0 OR current-window RTDS/OrderFilled BUY price [0.25,0.32) captured since max(last_check,window_open) with ingest_age<=30s",
    }


def _admission_authority_for_publish(
    authority: dict[str, Any], *, published_at: datetime
) -> dict[str, Any]:
    """Fail closed when an admission read no longer belongs to the live window."""

    published = dict(authority)
    interval = published.get("admission_interval_read")
    interval = dict(interval) if isinstance(interval, dict) else {}
    interval_end = _parse_ts(interval.get("interval_end"))
    current_window_start_s = int(published_at.timestamp()) // 300 * 300
    interval_window_start_s = (
        int(interval_end.timestamp()) // 300 * 300 if interval_end is not None else None
    )
    if interval_window_start_s != current_window_start_s:
        published["authorized"] = False
        published["status"] = "ADMISSION_STALE_WINDOW"
        published["stale_window_refusal"] = {
            "published_at": published_at.isoformat(),
            "interval_end": interval.get("interval_end"),
            "interval_window_start_s": interval_window_start_s,
            "publish_window_start_s": current_window_start_s,
        }
    published["published_at"] = published_at.isoformat()
    published["publish_lag_s"] = (
        round((published_at - interval_end).total_seconds(), 6)
        if interval_end is not None
        else None
    )
    return published


def _append_admission_publish_audit(
    state_path: Path,
    *,
    published: dict[str, Any],
    checked_at: datetime,
    published_at: datetime,
    stage: str,
    cycle_elapsed_s: float | None = None,
) -> None:
    interval = published.get("admission_interval_read") or {}
    interval_start = _parse_ts(interval.get("interval_start"))
    interval_end = _parse_ts(interval.get("interval_end"))
    interval_window = int(interval_end.timestamp()) // 300 * 300 if interval_end else None
    publish_window = int(published_at.timestamp()) // 300 * 300
    audit_row = {
        "kind": "order_flow_deadman_admission_publish",
        "checked_at": checked_at.isoformat(),
        "published_at": published_at.isoformat(),
        "publish_lag_s": published.get("publish_lag_s"),
        "stage": stage,
        "cycle_elapsed_s": round(cycle_elapsed_s, 6) if cycle_elapsed_s is not None else None,
        "interval_window_start_s": interval_window,
        "publish_window_start_s": publish_window,
        "crossed": interval_window != publish_window,
        "status": published.get("status"),
        "authorized": published.get("authorized"),
        "interval_source": interval.get("interval_source"),
        "interval_width_s": round((interval_end - interval_start).total_seconds(), 6) if interval_start and interval_end else None,
        "refusal_counts": interval.get("refusal_counts") or {},
        "ingest_age_s_histogram": interval.get("ingest_age_s_histogram") or {},
    }
    audit_path = state_path.parent / Path(DEFAULT_ADMISSION_INTERVAL_LOG).name
    with audit_path.open("a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.write(json.dumps(audit_row, sort_keys=True) + "\n")
        handle.flush()
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _heartbeat_due(
    last_monotonic: float, current_monotonic: float, *, interval_s: float = 60.0
) -> bool:
    return current_monotonic - last_monotonic >= interval_s


def _publish_admission_authority(
    state_path: Path,
    *,
    authority: dict[str, Any],
    checked_at: datetime,
    published_at: datetime | None = None,
    stage: str = "unspecified",
    cycle_elapsed_s: float | None = None,
) -> dict[str, Any]:
    """Targeted admission publish by the existing singleton lock owner."""

    publish_now = published_at or _utc_now()
    published = _admission_authority_for_publish(authority, published_at=publish_now)
    try:
        current = json.loads(state_path.read_text())
        current = current if isinstance(current, dict) else {}
    except (OSError, json.JSONDecodeError):
        current = {}
    policy_choke = dict(current.get("policy_choke") or {})
    actuator = dict(policy_choke.get("actuator") or {})
    actuator["admission_authority"] = published
    policy_choke["actuator"] = actuator
    current.update(
        {
            "admission_checked_at": checked_at.isoformat(),
            "admission_published_at": publish_now.isoformat(),
            "early_admission_authority": published,
            "policy_choke": policy_choke,
        }
    )
    atomic_write_json(state_path, current)
    _append_admission_publish_audit(
        state_path,
        published=published,
        checked_at=checked_at,
        published_at=publish_now,
        stage=stage,
        cycle_elapsed_s=cycle_elapsed_s,
    )
    return published


def _renew_direct_pin_from_incumbent_allpass(
    *,
    overlay: dict[str, Any],
    candidate_evidence: dict[str, Any],
    now: datetime,
    guard: dict[str, Any] | None = None,
    ledger: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Renew a seated DIRECT lease without admission or guard adoption."""

    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    expiry = _parse_ts(pin.get("expires_at"))
    if (
        pin.get("pin_id") != POLICY_CHOKE_RUNG_B_PIN_ID
        or pin.get("enabled") is False
        or expiry is None
        or expiry <= now
    ):
        return overlay, {"status": "NO_RENEWABLE_DIRECT_PIN"}
    incumbent = _incumbent_allpass_row(
        candidates=candidate_evidence,
        overlay=overlay,
    )
    if incumbent is None:
        return overlay, {
            "status": "DIRECT_PIN_RENEWAL_EVIDENCE_NOT_ALLPASS",
            "selection_pin": pin,
        }
    wallet = _normalize_wallet(pin.get("source_wallet"))
    created_at = _parse_ts(pin.get("created_at"))
    accepted_orders = _accepted_orders_for_wallet(
        ledger or {}, wallet, since=created_at, until=now
    )
    fetch_meta = (
        ((guard or {}).get("active_set_dataapi_poller") or {}).get("fetch_meta")
        if isinstance((guard or {}).get("active_set_dataapi_poller"), dict)
        else {}
    )
    wallet_meta = fetch_meta.get(wallet) if isinstance(fetch_meta, dict) else {}
    policy_feedback = (
        wallet_meta.get("policy_feedback")
        if isinstance(wallet_meta, dict) and isinstance(wallet_meta.get("policy_feedback"), dict)
        else {}
    )
    compatible_lag_s = _as_float(
        policy_feedback.get("freshest_policy_compatible_buy_lag_s")
    )
    fresh_compatible_buy = compatible_lag_s is not None and 0.0 <= compatible_lag_s <= 30.0
    if accepted_orders <= 0 and not fresh_compatible_buy:
        return overlay, {
            "status": "DIRECT_PIN_RENEWAL_SKIPPED_EMPTY",
            "selection_pin": pin,
            "incumbent_wallet": wallet,
            "accepted_orders_since_pin": accepted_orders,
            "freshest_policy_compatible_buy_lag_s": compatible_lag_s,
            "renewal_rule": "renew only after an accepted order or a policy-compatible buy observed within 30s",
        }
    renewed_pin = {
        **pin,
        "expires_at": (now + timedelta(seconds=POLICY_CHOKE_RUNG_B_TTL_S)).isoformat(),
        "last_renewed_at": now.isoformat(),
        "renewal_authority": "incumbent_allpass_with_f3_incumbency_only_waiver",
    }
    updated = {**overlay, "selection_pin": renewed_pin, "updated_at": now.isoformat()}
    admission = (
        overlay.get("latest_policy_choke_rung_b_admission")
        if isinstance(overlay.get("latest_policy_choke_rung_b_admission"), dict)
        else None
    )
    if admission is not None:
        updated["latest_policy_choke_rung_b_admission"] = {
            **admission,
            "selection_pin": renewed_pin,
        }
    return updated, {
        "status": "DIRECT_SOURCE_SELECTION_PIN_RENEWED",
        "selection_pin": renewed_pin,
        "incumbent_wallet": _normalize_wallet(incumbent.get("wallet")),
        "incumbent_rank": list(_normal_gate_rank(incumbent)),
        "admission_replayed": False,
        "guard_restart_requested": False,
    }


def _disable_temporally_ineligible_direct_pin(
    *,
    overlay: dict[str, Any],
    candidate_evidence: dict[str, Any],
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    pin = (
        overlay.get("selection_pin")
        if isinstance(overlay.get("selection_pin"), dict)
        else {}
    )
    if (
        pin.get("pin_id") != POLICY_CHOKE_RUNG_B_PIN_ID
        or pin.get("enabled") is False
    ):
        return overlay, {"status": "NO_ACTIVE_DIRECT_PIN"}
    wallet = _normalize_wallet(pin.get("source_wallet") or pin.get("wallet"))
    candidate_id = str(pin.get("candidate_id") or "")
    pinned_member = next(
        (
            member
            for member in overlay.get("members") or []
            if isinstance(member, dict)
            and _normalize_wallet(member.get("source_wallet") or member.get("wallet"))
            == wallet
            and (
                not candidate_id
                or str(member.get("candidate_id") or "") == candidate_id
            )
        ),
        {},
    )
    pinned_policy = (
        pinned_member.get("policy")
        if isinstance(pinned_member.get("policy"), dict)
        else {}
    )
    pinned_fingerprint = str(
        pinned_policy.get("wide_policy_fingerprint")
        or pinned_member.get("wide_policy_fingerprint")
        or ""
    )
    pinned_row = next(
        (
            row
            for row in candidate_evidence.get("rows") or []
            if isinstance(row, dict)
            and _normalize_wallet(row.get("wallet")) == wallet
            and (
                not pinned_fingerprint
                or str(row.get("wide_policy_fingerprint") or "")
                == pinned_fingerprint
            )
        ),
        None,
    )
    if not isinstance(pinned_row, dict):
        return overlay, {"status": "PINNED_IDENTITY_NOT_IN_CURRENT_FRONTIER"}
    temporal_rows = (
        pinned_row.get("active_temporal_slices")
        if isinstance(pinned_row.get("active_temporal_slices"), list)
        else []
    )
    negative = next(
        (
            row
            for row in temporal_rows
            if isinstance(row, dict)
            and str(row.get("label") or "").upper() == "PROVEN-NEGATIVE"
        ),
        None,
    )
    if not isinstance(negative, dict):
        return overlay, {"status": "PIN_TEMPORAL_SLICES_CLEAR"}
    slice_name = str(negative.get("slice") or negative.get("regime") or "active")
    reason = f"temporal_slice_{slice_name}_proven_negative"
    disabled_at = now.isoformat()
    updated_members: list[Any] = []
    disabled_member = False
    for member in overlay.get("members") or []:
        if not isinstance(member, dict):
            updated_members.append(member)
            continue
        if (
            _normalize_wallet(member.get("source_wallet") or member.get("wallet"))
            == wallet
            and (
                not candidate_id
                or str(member.get("candidate_id") or "") == candidate_id
            )
        ):
            member = {
                **member,
                "enabled": False,
                "status": "DISABLED_ACTIVE_TEMPORAL_PROVEN_NEGATIVE",
                "disabled_at": disabled_at,
                "disabled_reason": reason,
                "disabled_direction_id": "2026-07-26T20:23:15Z-fable-multi-slice-parity",
            }
            disabled_member = True
        updated_members.append(member)
    updated = {
        **overlay,
        "members": updated_members,
        "selection_pin": {
            **pin,
            "enabled": False,
            "disabled_at": disabled_at,
            "disabled_reason": reason,
            "disabled_direction_id": "2026-07-26T20:23:15Z-fable-multi-slice-parity",
        },
        "updated_at": disabled_at,
        "last_action": "DIRECT_PIN_DISABLED_ACTIVE_TEMPORAL_PROVEN_NEGATIVE",
    }
    report = {
        "status": "DIRECT_PIN_DISABLED_ACTIVE_TEMPORAL_PROVEN_NEGATIVE",
        "wallet": wallet,
        "candidate_id": candidate_id,
        "disabled_member": disabled_member,
        "reason": reason,
        "temporal_evidence": negative,
        "direction_id": "2026-07-26T20:23:15Z-fable-multi-slice-parity",
        "single_submitter_preserved": True,
    }
    updated["latest_policy_choke_direct_temporal_disable"] = report
    return updated, report


def _recovery_ttl_candidate(
    *, states: list[dict[str, Any]], preregs: list[dict[str, Any]], now: datetime,
    existing_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fail closed until both immutable recovery cells reach their joint TTL."""
    if len(states) != 2 or len(preregs) != 2:
        return {"status": "RECOVERY_STATE_MISSING", "candidate": None}
    checksums = []
    deadlines = []
    tuples: list[dict[str, Any]] = []
    for state, prereg in zip(states, preregs, strict=True):
        body = {key: value for key, value in prereg.items() if key != "checksum"}
        checksum = str(prereg.get("checksum") or "")
        if not checksum or checksum != _stable_checksum(body) or state.get("preregistration_checksum") != checksum:
            return {"status": "REFUSED_RECOVERY_CHECKSUM_MISMATCH", "candidate": None}
        deadline = _parse_ts(state.get("observation_deadline_at"))
        if deadline is None:
            return {"status": "REFUSED_RECOVERY_DEADLINE_MISSING", "candidate": None}
        checksums.append(checksum)
        deadlines.append(deadline)
        tuples.extend(row for row in state.get("tuple_evidence") or [] if isinstance(row, dict))
    joint_deadline = max(deadlines)
    identity = _stable_checksum({"checksums": sorted(checksums), "joint_deadline": joint_deadline.isoformat()})
    if existing_decision and existing_decision.get("identity") == identity:
        return {**existing_decision, "status": "RECOVERY_TTL_DECISION_ALREADY_RECORDED"}
    if now < joint_deadline:
        return {"status": "RECOVERY_TTL_ACCRUING", "candidate": None, "joint_deadline_at": joint_deadline.isoformat(), "identity": identity}
    eligible = [row for row in tuples if row.get("eligible") is True]
    eligible.sort(key=lambda row: (-float(row.get("post_fee_pnl_usd") or 0.0), -int(row.get("resolved") or 0), str(row.get("tuple_id") or "")))
    if eligible:
        top = eligible[0]
        candidate = {"eligible": True, "wallet": top.get("wallet"), "paper_policy_id": top.get("policy_id"), "policy": top.get("policy"), "recovery_tuple": top}
        return {"status": "TTL_ALL_PASS_PIN_DUE", "candidate": candidate, "identity": identity, "joint_deadline_at": joint_deadline.isoformat(), "eligible_tuple_count": len(eligible)}
    return {
        "schema_version": 1, "kind": "wide_recovery_method_switch_decision", "status": "TTL_NO_ALL_PASS_METHOD_SWITCH",
        "identity": identity, "decided_at": now.isoformat(), "joint_deadline_at": joint_deadline.isoformat(), "candidate": None,
        "eligible_tuple_count": 0, "tuple_count": len(tuples), "checksums": sorted(checksums), "immutable": True,
        "next_generations": ["multivenue_passive_residual", "multivenue_maker_first_residual"],
    }


def _rung_c_escalation_label(
    *,
    recovery_ttl: dict[str, Any],
    successor_states: list[dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    """Report a settled recovery decision without re-requesting Rung C."""
    terminal_rows = []
    for state in successor_states:
        status = str(state.get("status") or "")
        terminal = bool(
            state.get("stop_writer") is True
            and (
                status.startswith("PARK_")
                or "NO_FLIP" in status
            )
        )
        terminal_rows.append(
            {
                "kind": state.get("kind"),
                "status": status,
                "stop_writer": state.get("stop_writer"),
                "terminal": terminal,
            }
        )
    satisfied = bool(
        recovery_ttl.get("status") == "RECOVERY_TTL_DECISION_ALREADY_RECORDED"
        and successor_states
        and len(terminal_rows) == len(successor_states)
        and all(row["terminal"] for row in terminal_rows)
    )
    return (
        "RUNG_C_SATISFIED_BY_RECORDED_DECISION"
        if satisfied
        else "RUNG_C_METHOD_SWITCH_DUE",
        {
            "status": (
                "SATISFIED_BY_RECORDED_DECISION"
                if satisfied
                else "METHOD_SWITCH_NOT_TERMINALLY_SETTLED"
            ),
            "recovery_ttl_status": recovery_ttl.get("status"),
            "successors": terminal_rows,
        },
    )


def _policy_choke_fire_drill_verdict(
    *,
    rung_a_gate_pass: bool,
    rung_b_gate_pass: bool,
    queue_gate_pass: bool,
    rung_c_settlement: dict[str, Any],
) -> tuple[str, str]:
    gate_broken = not (rung_a_gate_pass and rung_b_gate_pass and queue_gate_pass)
    gate_verdict = "GATE_BROKEN" if gate_broken else "PASS"
    verdict = (
        "GATE_BROKEN"
        if gate_broken
        else "INAPPLICABLE_TERMINAL_SETTLEMENT"
        if rung_c_settlement.get("status") == "SATISFIED_BY_RECORDED_DECISION"
        else "PASS"
    )
    return gate_verdict, verdict


def _accepted_orders_for_wallet(
    ledger: dict[str, Any], wallet: str, *, since: datetime | None = None, until: datetime | None = None
) -> int:
    accepted_statuses = {"FILLED", "LIVE_FILLED", "LIVE_MAKER_FILLED", "LIVE_SUBMITTED", "MATCHED", "SUBMITTED"}
    seen: set[str] = set()
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict) or _normalize_wallet(row.get("source_wallet") or row.get("wallet")) != wallet:
            continue
        statuses = {str(row.get("status") or "").upper(), str(row.get("final_status") or "").upper()}
        row_ts = _order_event_ts(row)
        if not statuses & accepted_statuses or row_ts is None:
            continue
        if since is not None and row_ts < since:
            continue
        if until is not None and row_ts > until:
            continue
        identity = str(row.get("order_id") or row.get("intent_id") or "")
        if identity:
            seen.add(identity)
    return len(seen)


def _reconcile_policy_choke_rung_b(
    *,
    overlay: dict[str, Any],
    ledger: dict[str, Any],
    previous_state: dict[str, Any],
    now: datetime,
    hot_history: dict[str, Any] | None = None,
    successor_ready: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    raw_cooloffs = dict(previous_state.get("policy_choke_rung_b_cooloffs") or {})
    cooloffs: dict[str, Any] = {}
    for key, value in raw_cooloffs.items():
        _wallet, expires, _fingerprint = _cooloff_record(str(key), value)
        if expires is not None and expires > now:
            cooloffs[str(key)] = value
    current_pin = (
        overlay.get("selection_pin")
        if isinstance(overlay.get("selection_pin"), dict)
        else {}
    )
    current_pin_expiry = _parse_ts(current_pin.get("expires_at"))
    current_pin_wallet = _normalize_wallet(current_pin.get("source_wallet"))
    current_pin_members = [
        member
        for member in overlay.get("members") or []
        if isinstance(member, dict)
        and str(member.get("candidate_id") or "")
        == str(current_pin.get("candidate_id") or "")
    ]
    current_pin_member = next(
        (
            member
            for member in current_pin_members
            if member.get("enabled") is not False
        ),
        current_pin_members[-1] if current_pin_members else {},
    )
    current_pin_fingerprint = _member_wide_fingerprint(current_pin_member)
    if (
        current_pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID
        and current_pin.get("enabled") is not False
        and current_pin_expiry is not None
        and current_pin_expiry > now
    ):
        # A live non-refreshing DIRECT pin owns its natural TTL. A stale
        # prior-cycle early-terminal cooloff must not disable a pin that the
        # current overlay still declares active; loss/TTL/successor-ready
        # rules remain the only clear authorities.
        for key, value in list(cooloffs.items()):
            record_wallet, _expires, record_fingerprint = _cooloff_record(
                key, value
            )
            if record_wallet != current_pin_wallet:
                continue
            if (
                not record_fingerprint
                or not current_pin_fingerprint
                or record_fingerprint == current_pin_fingerprint
            ):
                cooloffs.pop(key, None)
    mechanical_demotion = (
        overlay.get("latest_mechanical_temporal_loss_demotion")
        if isinstance(overlay.get("latest_mechanical_temporal_loss_demotion"), dict)
        else {}
    )
    if mechanical_demotion.get("status") == "APPLIED":
        demoted_wallet = _normalize_wallet(mechanical_demotion.get("target_wallet"))
        demoted_at = _parse_ts(mechanical_demotion.get("generated_at"))
        if demoted_wallet and demoted_at is not None:
            demotion_cooloff_until = demoted_at + timedelta(hours=24)
            if demotion_cooloff_until > now:
                # Derive from the immutable demotion timestamp on every pass:
                # generated deadman state cannot lose or extend this cooloff.
                demoted_member = next(
                    (
                        member
                        for member in overlay.get("members") or []
                        if isinstance(member, dict)
                        and _normalize_wallet(
                            member.get("source_wallet") or member.get("wallet")
                        )
                        == demoted_wallet
                        and member.get("enabled") is False
                    ),
                    {},
                )
                demoted_fingerprint = str(
                    mechanical_demotion.get("wide_policy_fingerprint")
                    or _member_wide_fingerprint(demoted_member)
                    or ""
                )
                _set_identity_cooloff(
                    cooloffs,
                    wallet=demoted_wallet,
                    fingerprint=demoted_fingerprint,
                    expires_at=demotion_cooloff_until,
                    reason=str(
                        mechanical_demotion.get("cooloff_reason")
                        or "mechanical_loss_demotion"
                    ),
                )
    cooloff_members_changed = False
    cooloff_members: list[dict[str, Any]] = []
    for member in overlay.get("members") or []:
        if not isinstance(member, dict):
            continue
        wallet = _normalize_wallet(member.get("source_wallet") or member.get("wallet"))
        candidate_id = str(member.get("candidate_id") or "")
        member_fingerprint = _member_wide_fingerprint(member)
        member_cooloff = _cooloff_scope_for_identity(
            cooloffs=cooloffs,
            overlay=overlay,
            wallet=wallet,
            fingerprint=member_fingerprint,
            now=now,
        )
        if (
            member_cooloff["active"]
            and candidate_id.startswith("policy_choke_rung_")
            and member.get("enabled") is not False
        ):
            member = {
                **member,
                "enabled": False,
                "status": "AUTO_DISABLED_RUNG_B_TTL",
            }
            cooloff_members_changed = True
        cooloff_members.append(member)
    if cooloff_members_changed:
        overlay = {
            **overlay,
            "members": cooloff_members,
            "updated_at": now.isoformat(),
        }
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    admission = overlay.get("latest_policy_choke_rung_b_admission") if isinstance(overlay.get("latest_policy_choke_rung_b_admission"), dict) else {}
    recorded_pin = admission.get("selection_pin") if isinstance(admission.get("selection_pin"), dict) else {}
    rung_b_pin = pin if pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID else recorded_pin
    if rung_b_pin.get("pin_id") != POLICY_CHOKE_RUNG_B_PIN_ID:
        return overlay, {
            "status": "NO_ACTIVE_RUNG_B",
            "cooloff_emergency_members_disabled": cooloff_members_changed,
        }, cooloffs
    matching_rung_members = [
        member
        for member in overlay.get("members") or []
        if isinstance(member, dict)
        and str(member.get("candidate_id") or "") == str(rung_b_pin.get("candidate_id") or "")
    ]
    rung_member = next(
        (
            member for member in matching_rung_members
            if member.get("enabled") is not False
        ),
        matching_rung_members[0] if matching_rung_members else None,
    )
    if not rung_member or rung_member.get("enabled") is False:
        return overlay, {"status": "RUNG_B_LOSS_LEG_DISABLED", "candidate_id": rung_b_pin.get("candidate_id")}, cooloffs
    expires = _parse_ts(rung_b_pin.get("expires_at"))
    created = _parse_ts(rung_b_pin.get("created_at"))
    wallet = _normalize_wallet(rung_b_pin.get("source_wallet"))
    rung_fingerprint = _member_wide_fingerprint(rung_member)
    if (
        pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID
        and expires is not None
        and now < expires
    ):
        for key, value in list(cooloffs.items()):
            record_wallet, _expires, record_fingerprint = _cooloff_record(
                key, value
            )
            if record_wallet != wallet:
                continue
            if (
                not record_fingerprint
                or not rung_fingerprint
                or record_fingerprint == rung_fingerprint
            ):
                cooloffs.pop(key, None)
    
    is_early_terminal = False
    pin_age_s = (now - created).total_seconds() if created is not None else 0.0
    if expires is not None and now < expires:
        if pin_age_s >= 900.0:
            accepted = _accepted_orders_for_wallet(ledger, wallet, since=created, until=now)
            if accepted == 0:
                fresh_counts = _recent_buy_counts(
                    hot_history or {},
                    now=now,
                    lookback_s=POLICY_CHOKE_LOOKBACK_S,
                )
                fresh_rows = fresh_counts.get(wallet, 0)
                if fresh_rows == 0 and successor_ready:
                    is_early_terminal = True
        if not is_early_terminal:
            if pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID:
                return overlay, {"status": "RUNG_B_ACTIVE", "expires_at": rung_b_pin.get("expires_at")}, cooloffs
            updated = {**overlay, "selection_pin": rung_b_pin, "updated_at": now.isoformat()}
            return updated, {"status": "RUNG_B_PIN_RESTORED", "expires_at": rung_b_pin.get("expires_at")}, cooloffs
            
    accepted = _accepted_orders_for_wallet(ledger, wallet, since=created, until=expires if not is_early_terminal else now)
    updated = dict(overlay)
    updated_members = []
    for member in overlay.get("members") or []:
        if not isinstance(member, dict):
            updated_members.append(member)
            continue
        if str(member.get("candidate_id") or "") == str(rung_b_pin.get("candidate_id") or ""):
            member = {**member, "enabled": False, "status": "AUTO_DISABLED_RUNG_B_TTL"}
        updated_members.append(member)
    updated["members"] = updated_members
    if pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID:
        updated.pop("selection_pin", None)
    updated["updated_at"] = now.isoformat()
    status = "RUNG_B_TTL_DISABLED_AFTER_CONVERSION"
    cooloff_key = ""
    if accepted == 0:
        cooloff_key = _set_identity_cooloff(
            cooloffs,
            wallet=wallet,
            fingerprint=rung_fingerprint,
            expires_at=now + timedelta(seconds=POLICY_CHOKE_RUNG_B_COOLOFF_S),
            reason="rung_b_zero_accept_ttl",
        )
        status = "RUNG_C_METHOD_SWITCH_DUE"
    report = {
        "status": status,
        "wallet": wallet,
        "accepted_orders_during_ttl": accepted,
        "disabled_candidate_id": rung_b_pin.get("candidate_id"),
        "cooloff_until": (
            cooloffs.get(cooloff_key) if cooloff_key else None
        ),
    }
    if is_early_terminal:
        report["reason"] = "early_ttl_equivalent_zero_accept; failed_15m_accepted_liveness; lifecycle_F2_active_market_buys=0; exact_successor_ready_for_same_heartbeat_repin"
        report["terminal_at"] = now.isoformat()
    updated["latest_policy_choke_rung_b_expiry"] = report
    return updated, report, cooloffs


def _positive_counts(counts: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    if not isinstance(counts, dict):
        return out
    for key, value in counts.items():
        try:
            count = int(value or 0)
        except (TypeError, ValueError):
            continue
        if count > 0:
            out[str(key)] = count
    return out


def _normalize_wallet(value: Any) -> str:
    return str(value or "").strip().lower()


def _direct_manifest_identity_fallback(
    *,
    root: Path,
    direct_source: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Resolve exact direct-generation identities before the heavy index catches up.

    The WIDE supervisor publishes a new manifest before rebuilding the global
    fingerprint-evidence artifact.  During that bounded lag, direct capture
    rows already name the exact run and manifest but the global
    ``manifest_wallet_fingerprints`` map cannot contain them yet.  Loading only
    the named run manifest keeps the admission join fail-closed without
    rescanning every historical manifest or fabricating f2 evidence.
    """

    result: dict[str, dict[str, Any]] = {}
    generations = (
        direct_source.get("per_wallet_generation")
        if isinstance(direct_source.get("per_wallet_generation"), dict)
        else {}
    )
    loaded_runs: dict[str, tuple[str, dict[str, dict[str, Any]]]] = {}
    for supply in generations.values():
        if not isinstance(supply, dict):
            continue
        wallet = _normalize_wallet(supply.get("wallet"))
        identity = (
            supply.get("generation_identity")
            if isinstance(supply.get("generation_identity"), dict)
            else {}
        )
        run_id = str(identity.get("run_id") or "")
        manifest_id = str(identity.get("manifest_id") or "")
        if not wallet or not run_id or not manifest_id:
            continue
        if run_id not in loaded_runs:
            path = root / "data" / "research" / f"wide_exact_policy_manifest_{run_id}.json"
            manifest = load_json(path, default={})
            if not isinstance(manifest, dict):
                loaded_runs[run_id] = ("", {})
            else:
                loaded_runs[run_id] = (
                    str(manifest.get("manifest_id") or ""),
                    manifest_wallet_policy_identities(manifest),
                )
        loaded_manifest_id, identities = loaded_runs[run_id]
        manifest_identity = identities.get(wallet)
        if loaded_manifest_id != manifest_id or not isinstance(manifest_identity, dict):
            continue
        result[f"{manifest_id}|{wallet}"] = {
            **manifest_identity,
            "manifest_identity_source": "exact_direct_generation_manifest_fallback",
            "manifest_path": (
                f"data/research/wide_exact_policy_manifest_{run_id}.json"
            ),
        }
    return result


def _selected_runtime_identity(guard: dict) -> dict[str, Any]:
    """Resolve the live seat from the guard's dated authoritative identity.

    The active-set selected member is an undated rotation snapshot.  It is
    useful only as a fallback when the guard's top-level identity and dated
    runtime submittability pair are absent.
    """
    submittability = (
        guard.get("runtime_member_submittability")
        if isinstance(guard.get("runtime_member_submittability"), dict)
        else {}
    )
    top_wallet = _normalize_wallet(guard.get("source_wallet"))
    top_candidate = str(guard.get("candidate_id") or "")
    dated_wallet = _normalize_wallet(submittability.get("source_wallet"))
    dated_candidate = str(submittability.get("candidate_id") or "")
    dated_at = _parse_ts(submittability.get("generated_at"))
    top_complete = bool(top_wallet and top_candidate)
    dated_complete = bool(dated_wallet and dated_candidate and dated_at is not None)
    authoritative_present = bool(top_complete or dated_complete)
    authoritative_agrees = bool(
        top_complete
        and dated_complete
        and top_wallet == dated_wallet
        and top_candidate == dated_candidate
    )
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    selected = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    fallback_wallet = _normalize_wallet(selected.get("source_wallet"))
    fallback_candidate = str(selected.get("candidate_id") or "")
    if authoritative_agrees:
        status = (
            "PASS_LEGACY_SNAPSHOT_DIVERGED"
            if fallback_wallet and fallback_wallet != dated_wallet
            else "PASS"
        )
        return {
            "status": status,
            "source_wallet": dated_wallet,
            "candidate_id": dated_candidate,
            "generated_at": dated_at.isoformat(),
            "source": "runtime_member_submittability_crosschecked_top_level",
            "legacy_snapshot_wallet": fallback_wallet or None,
            "legacy_snapshot_candidate_id": fallback_candidate or None,
        }
    if authoritative_present:
        return {
            "status": "REFUSED_AUTHORITATIVE_IDENTITY_DISAGREEMENT",
            "source_wallet": None,
            "candidate_id": None,
            "generated_at": submittability.get("generated_at"),
            "source": "fail_closed",
            "top_level_wallet": top_wallet or None,
            "top_level_candidate_id": top_candidate or None,
            "runtime_submittability_wallet": dated_wallet or None,
            "runtime_submittability_candidate_id": dated_candidate or None,
            "legacy_snapshot_wallet": fallback_wallet or None,
            "legacy_snapshot_candidate_id": fallback_candidate or None,
        }
    return {
        "status": "FALLBACK_UNDATED_ACTIVE_SET_RUNTIME" if fallback_wallet else "MISSING",
        "source_wallet": fallback_wallet or None,
        "candidate_id": fallback_candidate or None,
        "generated_at": selected.get("generated_at"),
        "source": "active_set_runtime.selected_member" if fallback_wallet else "missing",
    }


def _selected_runtime_wallet(guard: dict) -> str:
    return _normalize_wallet(_selected_runtime_identity(guard).get("source_wallet"))


def _sum_count_values(values: dict) -> int:
    total = 0
    if not isinstance(values, dict):
        return total
    for value in values.values():
        try:
            total += int(value or 0)
        except (TypeError, ValueError):
            continue
    return total


def _active_set_fresh_demand_split(guard: dict) -> dict[str, Any]:
    """Split selected-member demand from bench demand used only for rotation.

    The live guard submits only for the selected runtime member. Fresh rows for
    other active-set members are pressure to rotate, not proof that the guard
    failed to submit.
    """
    selected_identity = _selected_runtime_identity(guard)
    selected_wallet = _normalize_wallet(selected_identity.get("source_wallet"))
    poller = guard.get("active_set_dataapi_poller")
    poller = poller if isinstance(poller, dict) else {}
    summary = poller.get("summary") if isinstance(poller.get("summary"), dict) else {}
    fetch_meta = poller.get("fetch_meta") if isinstance(poller.get("fetch_meta"), dict) else {}
    drought = guard.get("drought_funnel") if isinstance(guard.get("drought_funnel"), dict) else {}
    try:
        active_set_fresh_total = int(drought.get("active_set_fresh_signal_rows") or 0)
    except (TypeError, ValueError):
        active_set_fresh_total = 0

    fetch_fresh_by_wallet: dict[str, int] = {}
    freshest_lag_by_wallet: dict[str, float | None] = {}
    for wallet, meta in fetch_meta.items():
        wallet_key = _normalize_wallet(wallet)
        if not wallet_key or not isinstance(meta, dict):
            continue
        fresh_by_source = (
            meta.get("fresh_buy_rows_le_10s_by_source")
            if isinstance(meta.get("fresh_buy_rows_le_10s_by_source"), dict)
            else {}
        )
        fetch_fresh_by_wallet[wallet_key] = _sum_count_values(fresh_by_source)
        lag_by_source = (
            meta.get("freshest_buy_lag_s_by_source")
            if isinstance(meta.get("freshest_buy_lag_s_by_source"), dict)
            else {}
        )
        lags = [_as_float(value) for value in lag_by_source.values()]
        lags = [value for value in lags if value is not None and value >= 0]
        freshest_lag_by_wallet[wallet_key] = min(lags) if lags else None

    summary_fresh_by_wallet = {}
    raw_summary_fresh = summary.get("fresh_poll_only_by_wallet")
    if isinstance(raw_summary_fresh, dict):
        summary_fresh_by_wallet = {
            _normalize_wallet(wallet): count
            for wallet, count in _positive_counts(raw_summary_fresh).items()
            if _normalize_wallet(wallet)
        }
    attributed_fresh_by_wallet = summary_fresh_by_wallet or {
        wallet: count for wallet, count in fetch_fresh_by_wallet.items() if count > 0
    }
    attributed_total = sum(attributed_fresh_by_wallet.values())

    if selected_wallet:
        selected_active_set_fresh = int(attributed_fresh_by_wallet.get(selected_wallet, 0))
        nonselected_active_set_fresh = sum(
            count for wallet, count in attributed_fresh_by_wallet.items() if wallet != selected_wallet
        )
        # If the guard only exposes an aggregate count, keep it actionable
        # rather than hiding it as bench demand.
        unattributed_active_set_fresh = max(0, active_set_fresh_total - attributed_total)
        selected_active_set_fresh += unattributed_active_set_fresh
    else:
        selected_active_set_fresh = active_set_fresh_total
        nonselected_active_set_fresh = 0
        unattributed_active_set_fresh = 0

    fetch_total = sum(fetch_fresh_by_wallet.values())
    if selected_wallet:
        selected_fresh_buy_rows = int(fetch_fresh_by_wallet.get(selected_wallet, 0))
        nonselected_fresh_buy_rows = max(0, fetch_total - selected_fresh_buy_rows)
    else:
        selected_fresh_buy_rows = fetch_total
        nonselected_fresh_buy_rows = 0

    rtds = guard.get("active_set_rtds_premerge") if isinstance(guard.get("active_set_rtds_premerge"), dict) else {}
    rtds_rows = rtds.get("rows") if isinstance(rtds.get("rows"), list) else []
    rtds_by_wallet: dict[str, int] = defaultdict(int)
    raw_rtds_by_wallet: dict[str, int] = defaultdict(int)
    rtds_event_age_by_wallet: dict[str, float | None] = {}
    reference_ts = _parse_ts(guard.get("generated_at"))
    retained_by_wallet: dict[str, int] = defaultdict(int)
    latest_event_by_wallet: dict[str, Any] = {}
    latest_observed_by_wallet: dict[str, Any] = {}
    for row in rtds_rows:
        if not isinstance(row, dict):
            continue
        wallet = _normalize_wallet(row.get("source_wallet"))
        if not wallet:
            continue
        try:
            new_matching = int(row.get("new_matching_events") or 0)
        except (TypeError, ValueError):
            new_matching = 0
        try:
            retained = int(row.get("retained_matching_rows") or 0)
        except (TypeError, ValueError):
            retained = 0
        if new_matching > 0:
            raw_rtds_by_wallet[wallet] += new_matching
            latest_event_ts = _parse_ts(row.get("latest_event_ts"))
            event_age_s = (
                (reference_ts - latest_event_ts).total_seconds()
                if reference_ts is not None and latest_event_ts is not None
                else None
            )
            rtds_event_age_by_wallet[wallet] = event_age_s
            if event_age_s is not None and 0 <= event_age_s <= 10.0:
                rtds_by_wallet[wallet] += new_matching
        if retained > 0:
            retained_by_wallet[wallet] += retained
        if row.get("latest_event_ts") is not None:
            latest_event_by_wallet[wallet] = row.get("latest_event_ts")
        if row.get("latest_observed_ts") is not None:
            latest_observed_by_wallet[wallet] = row.get("latest_observed_ts")

    selected_rtds_new = int(rtds_by_wallet.get(selected_wallet, 0)) if selected_wallet else sum(rtds_by_wallet.values())
    nonselected_rtds_new = (
        sum(count for wallet, count in rtds_by_wallet.items() if wallet != selected_wallet)
        if selected_wallet
        else 0
    )
    nonselected_wallets = sorted(
        {
            wallet
            for wallet, count in {
                **fetch_fresh_by_wallet,
                **attributed_fresh_by_wallet,
                **raw_rtds_by_wallet,
            }.items()
            if selected_wallet and wallet != selected_wallet and int(count or 0) > 0
        }
    )
    nonselected_samples = []
    for wallet in nonselected_wallets[:10]:
        nonselected_samples.append(
            {
                "source_wallet": wallet,
                "fresh_buy_rows_le_10s": int(fetch_fresh_by_wallet.get(wallet, 0)),
                "active_set_fresh_signal_rows": int(attributed_fresh_by_wallet.get(wallet, 0)),
                "rtds_new_matching_events": int(rtds_by_wallet.get(wallet, 0)),
                "rtds_new_matching_events_raw": int(raw_rtds_by_wallet.get(wallet, 0)),
                "rtds_latest_event_age_s": rtds_event_age_by_wallet.get(wallet),
                "rtds_retained_matching_rows": int(retained_by_wallet.get(wallet, 0)),
                "freshest_buy_lag_s": freshest_lag_by_wallet.get(wallet),
                "latest_event_ts": latest_event_by_wallet.get(wallet),
                "latest_observed_ts": latest_observed_by_wallet.get(wallet),
            }
        )

    nonselected_rotation_pressure_rows = (
        int(nonselected_active_set_fresh)
        + int(nonselected_fresh_buy_rows)
        + int(nonselected_rtds_new)
    )
    raw_nonselected_rtds_new = (
        sum(count for wallet, count in raw_rtds_by_wallet.items() if wallet != selected_wallet)
        if selected_wallet
        else 0
    )
    nonselected_rotation_event_rows = max(
        0, int(raw_nonselected_rtds_new) - int(nonselected_rtds_new)
    )
    demand_evidence_limb = []
    if nonselected_active_set_fresh > 0:
        demand_evidence_limb.append("active_set_fresh_signal_rows")
    if nonselected_fresh_buy_rows > 0:
        demand_evidence_limb.append("dataapi_fresh_buy_rows_le_10s")
    if nonselected_rtds_new > 0:
        demand_evidence_limb.append("rtds_new_matching_events_le_10s")
    unresolved_attributed_demand_rows = (
        sum(attributed_fresh_by_wallet.values())
        + sum(fetch_fresh_by_wallet.values())
        + sum(rtds_by_wallet.values())
        if not selected_wallet
        else 0
    )
    return {
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "selected_wallet": selected_wallet or None,
        "selected_identity": selected_identity,
        "active_set_fresh_signal_rows_total": active_set_fresh_total,
        "active_set_selected_fresh_signal_rows": int(selected_active_set_fresh),
        "active_set_nonselected_fresh_signal_rows": int(nonselected_active_set_fresh),
        "active_set_unattributed_fresh_signal_rows": int(unattributed_active_set_fresh),
        "fresh_buy_rows_le_10s_total": int(fetch_total),
        "selected_fresh_buy_rows_le_10s": int(selected_fresh_buy_rows),
        "nonselected_fresh_buy_rows_le_10s": int(nonselected_fresh_buy_rows),
        "active_set_rtds_new_matching_events_total": int(rtds.get("new_matching_events") or 0),
        "active_set_rtds_fresh_matching_events_total": int(sum(rtds_by_wallet.values())),
        "selected_rtds_new_matching_events": int(selected_rtds_new),
        "nonselected_rtds_new_matching_events": int(nonselected_rtds_new),
        "nonselected_rotation_pressure_rows": int(nonselected_rotation_pressure_rows),
        "nonselected_rotation_event_rows": int(nonselected_rotation_event_rows),
        "demand_evidence_limb": demand_evidence_limb,
        "identity_unresolved_attributed_demand_rows": int(
            unresolved_attributed_demand_rows
        ),
        "nonselected_wallets": nonselected_wallets,
        "nonselected_samples": nonselected_samples,
        "classification": (
            "MEASURED_NONSELECTED_DEMAND"
            if nonselected_rotation_pressure_rows > 0 and selected_active_set_fresh <= 0 and selected_fresh_buy_rows <= 0
            else "MEASURED_NONSELECTED_ROTATION_EVENT"
            if nonselected_rotation_event_rows > 0 and selected_active_set_fresh <= 0 and selected_fresh_buy_rows <= 0
            else "SELECTED_DEMAND_PRESENT"
            if selected_active_set_fresh > 0 or selected_fresh_buy_rows > 0
            else "NO_FRESH_DEMAND"
        ),
        "rule": (
            "only <=10s freshness-gated non-selected rows classify as demand; older RTDS events "
            "are rotation telemetry, and only selected-member fresh demand can trigger guard-side halt"
        ),
    }


def _selected_identity_projection(guard: dict, demand_split: dict[str, Any]) -> dict[str, Any]:
    """Expose the authoritative selected identity without copying guard bulk."""
    identity = (
        demand_split.get("selected_identity")
        if isinstance(demand_split.get("selected_identity"), dict)
        else _selected_runtime_identity(guard)
    )
    selected_wallet = _normalize_wallet(
        demand_split.get("selected_wallet") or identity.get("source_wallet")
    )
    selected_candidate_id = str(identity.get("candidate_id") or "")
    runtime = (
        guard.get("active_set_runtime")
        if isinstance(guard.get("active_set_runtime"), dict)
        else {}
    )
    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    selected_policy_id = ""
    for member in members:
        if not isinstance(member, dict):
            continue
        if _normalize_wallet(member.get("source_wallet")) == selected_wallet:
            selected_policy_id = str(member.get("policy_id") or "")
            break
    if not selected_policy_id and selected_wallet == _normalize_wallet(guard.get("source_wallet")):
        selected_policy_id = str(guard.get("policy_id") or "")
    identity_status = str(identity.get("status") or "MISSING")
    identity_resolved = bool(
        selected_wallet
        and identity_status
        not in {"MISSING", "REFUSED_AUTHORITATIVE_IDENTITY_DISAGREEMENT"}
    )
    observed_demand_rows = int(
        demand_split.get("identity_unresolved_attributed_demand_rows") or 0
    )
    return {
        "selected_wallet": selected_wallet or None,
        "selected_candidate_id": selected_candidate_id or None,
        "selected_policy_id": selected_policy_id or None,
        "selected_identity": identity,
        "selected_identity_resolved": identity_resolved,
        "selected_identity_unresolved_with_observed_demand": bool(
            not identity_resolved and observed_demand_rows > 0
        ),
        "selected_identity_observed_demand_rows": observed_demand_rows,
        "active_member_count": runtime.get("member_count", len(members)),
        "qualified_member_count": runtime.get("qualified_member_count"),
    }


def _selected_seat_epoch(
    *,
    selected: dict[str, Any],
    previous: dict[str, Any],
    now: datetime,
    latest_order: datetime | None,
) -> datetime | None:
    """Persist the earliest evidenced epoch for the currently selected triple."""

    identity = (
        selected.get("selected_wallet"),
        selected.get("selected_candidate_id"),
        selected.get("selected_policy_id"),
    )
    previous_identity = (
        previous.get("selected_wallet"),
        previous.get("selected_candidate_id"),
        previous.get("selected_policy_id"),
    )
    if not all(identity):
        return None
    if identity == previous_identity:
        persisted = _parse_ts(previous.get("selected_seat_epoch_at"))
        if persisted is not None:
            return persisted
        candidates = [
            _parse_ts(previous.get("episode_fire_at")),
            _parse_ts((previous.get("selected_identity") or {}).get("generated_at")),
            _parse_ts(previous.get("checked_at")),
        ]
        candidates = [
            value
            for value in candidates
            if value is not None
            and value <= now
            and (latest_order is None or value > latest_order)
        ]
        if candidates:
            return min(candidates)
    generated_at = _parse_ts((selected.get("selected_identity") or {}).get("generated_at"))
    if generated_at is not None and generated_at <= now:
        return generated_at
    return now


def _post_selection_liveness_floor(
    *,
    guard: dict[str, Any],
    selected: dict[str, Any],
    previous: dict[str, Any],
    latest_accepted: dict[str, Any],
    now: datetime,
    can_trade: bool,
    max_idle_s: float,
    generation_mismatch: bool,
) -> dict[str, Any]:
    """Keep prior-seat accept debt from declaring a newly adopted seat dead."""

    latest_order = latest_accepted.get("timestamp")
    seat_epoch = _selected_seat_epoch(
        selected=selected,
        previous=previous,
        now=now,
        latest_order=latest_order,
    )
    identity = selected.get("selected_identity") or {}
    selected_wallet = _normalize_wallet(selected.get("selected_wallet"))
    accepted_wallet = _normalize_wallet(latest_accepted.get("source_wallet"))
    live_execution = (
        guard.get("live_execution")
        if isinstance(guard.get("live_execution"), dict)
        else {}
    )
    armed_status = str(live_execution.get("status") or "")
    identity_status = str(identity.get("status") or "")
    post_selection_idle_s = (
        max(0.0, (now - seat_epoch).total_seconds())
        if seat_epoch is not None
        else None
    )
    inherited = bool(
        latest_order is not None
        and seat_epoch is not None
        and latest_order < seat_epoch
        and (not accepted_wallet or accepted_wallet != selected_wallet)
    )
    matching_adopted_runtime = bool(
        selected_wallet
        and identity_status.startswith("PASS")
        and _normalize_wallet(guard.get("source_wallet")) == selected_wallet
        and str(guard.get("candidate_id") or "")
        == str(selected.get("selected_candidate_id") or "")
        and str(guard.get("policy_id") or "")
        == str(selected.get("selected_policy_id") or "")
    )
    active = bool(
        inherited
        and matching_adopted_runtime
        and guard.get("status") == "LIVE_GUARD_RUNNING"
        and guard.get("live_orders_allowed") is True
        and can_trade
        and not generation_mismatch
        and armed_status
        in {"LIVE_ARMED_NO_FRESH_INTENTS", "LIVE_GUARD_RUNNING", "LIVE_ORDERS_SUBMITTED"}
        and post_selection_idle_s is not None
        and post_selection_idle_s <= max_idle_s
    )
    return {
        "active": active,
        "inherited_prior_seat_idle": inherited,
        "matching_adopted_runtime": matching_adopted_runtime,
        "selected_seat_epoch_at": seat_epoch.isoformat() if seat_epoch else None,
        "post_selection_idle_s": post_selection_idle_s,
        "post_selection_threshold_s": max_idle_s,
        "latest_accepted_order_ts": latest_order.isoformat() if latest_order else None,
        "latest_accepted_order_wallet": accepted_wallet or None,
        "selected_wallet": selected_wallet or None,
        "armed_status": armed_status or None,
        "generation_mismatch": generation_mismatch,
        "rule": (
            "an adopted live-armed successor gets its own accepted-order drought clock; "
            "prior-seat accept debt remains observable but cannot trigger red before that clock"
        ),
    }


def _iter_live_event_prefilter_rows(guard: dict):
    live_execution = guard.get("live_execution") if isinstance(guard.get("live_execution"), dict) else {}
    summary = (
        live_execution.get("candidate_intent_summary")
        if isinstance(live_execution.get("candidate_intent_summary"), dict)
        else {}
    )
    prefilter = (
        summary.get("live_event_prefilter")
        if isinstance(summary.get("live_event_prefilter"), dict)
        else {}
    )
    seen: set[tuple[Any, ...]] = set()
    for key in ("sample_filtered_events", "inventory_window_participation", "sample_inventory_groups"):
        rows = prefilter.get(key) if isinstance(prefilter.get(key), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            dedupe_key = (
                row.get("source_wallet"),
                row.get("market_slug"),
                row.get("condition_id"),
                row.get("outcome"),
                row.get("window_start_s"),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            yield row


def _active_set_floor_blocked_fresh_rows(
    guard: dict,
    *,
    source_wallet: str | None = None,
) -> dict[str, Any]:
    source_wallet_key = _normalize_wallet(source_wallet)
    rows = [
        row
        for row in _iter_live_event_prefilter_rows(guard)
        if (not source_wallet_key or _normalize_wallet(row.get("source_wallet")) == source_wallet_key)
        and floor_blocked_miss(row)
    ]
    samples = []
    for row in rows[:5]:
        samples.append(
            {
                "source_wallet": row.get("source_wallet"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "reason": row.get("dominant_skip_reason") or row.get("reason"),
                "gap_usd_at_vwap": row.get("gap_usd_at_vwap"),
                "window_budget_usd": row.get("window_budget_usd"),
                "effective_min_submit_usd": max(
                    _as_float(row.get("process_min_live_order_usd")) or 0.0,
                    _as_float(row.get("effective_min_tranche_usd")) or 0.0,
                    _as_float(row.get("drip_min_tranche_usd")) or 0.0,
                    _as_float(row.get("would_floor_min_order_usd")) or 0.0,
                ),
            }
        )
    return {"count": len(rows), "samples": samples}


def _price_policy_blocked_row(row: dict) -> bool:
    reason = str(
        row.get("skip_reason")
        or row.get("dominant_skip_reason")
        or row.get("reason")
        or ""
    )
    return "price_outside_policy" in reason


def _active_set_price_policy_blocked_fresh_rows(
    guard: dict,
    *,
    source_wallet: str | None = None,
) -> dict[str, Any]:
    source_wallet_key = _normalize_wallet(source_wallet)
    rows = [
        row
        for row in _iter_live_event_prefilter_rows(guard)
        if (not source_wallet_key or _normalize_wallet(row.get("source_wallet")) == source_wallet_key)
        and _price_policy_blocked_row(row)
    ]
    samples = []
    for row in rows[:5]:
        samples.append(
            {
                "source_wallet": row.get("source_wallet"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "reason": row.get("skip_reason") or row.get("dominant_skip_reason") or row.get("reason"),
                "source_price": row.get("source_price") or row.get("price"),
            }
        )
    return {"count": len(rows), "samples": samples}


def _participation_row_ts(row: dict) -> datetime | None:
    timestamps: list[datetime] = []
    for key in ("last_seen_at", "updated_at", "generated_at", "first_seen_at"):
        parsed = _parse_ts(row.get(key))
        if parsed is not None:
            timestamps.append(parsed)
    for key in ("latest_observed_ts", "latest_event_ts", "window_start_s"):
        value = _as_float(row.get(key))
        if value is None or value <= 0:
            continue
        try:
            timestamps.append(datetime.fromtimestamp(value, timezone.utc))
        except (OSError, OverflowError, ValueError):
            continue
    return max(timestamps) if timestamps else None


def _participation_row_reason(row: dict) -> str:
    for key in ("dominant_skip_reason", "skip_reason", "reason", "participation_skip_reason"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _participation_rows(guard: dict) -> list[dict]:
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    if rows:
        return [row for row in rows if isinstance(row, dict)]
    rollups = participation.get("window_rollups") if isinstance(participation.get("window_rollups"), list) else []
    return [row for row in rollups if isinstance(row, dict)]


def _participation_taxonomy_from_rows(
    guard: dict,
    *,
    since: datetime | None,
) -> tuple[dict[str, int], dict[str, Any]]:
    taxonomy: dict[str, int] = defaultdict(int)
    rows = _participation_rows(guard)
    missing_ts = 0
    included = 0
    for row in rows:
        row_ts = _participation_row_ts(row)
        if row_ts is None:
            missing_ts += 1
            if since is not None:
                continue
        elif since is not None and row_ts < since:
            continue
        reason = _participation_row_reason(row)
        if not reason:
            continue
        included += 1
        taxonomy[reason] += 1
        if not reason.startswith("window:"):
            taxonomy[f"window:{reason}"] += 1
    return dict(sorted(taxonomy.items())), {
        "since": since.isoformat() if since else None,
        "rows_seen": len(rows),
        "rows_in_lookback": included,
        "rows_missing_timestamp": missing_ts,
        "source": "window_participation.rows",
        "rule": "floor-deadlock taxonomy uses only rows inside max(halt_duration,6h)",
    }


def _base_taxonomy_reasons(taxonomy: dict[str, int]) -> set[str]:
    return {
        str(reason).removeprefix("window:")
        for reason, count in taxonomy.items()
        if int(count or 0) > 0
    }


def _row_window_elapsed_s(row: dict) -> float | None:
    for key in ("observed_slug_epoch_delta_s", "window_elapsed_s", "slug_observed_delta_s"):
        value = _as_float(row.get(key))
        if value is not None and value >= 0:
            return value
    observed = _as_float(
        row.get("effective_latest_observed_ts")
        or row.get("latest_observed_ts")
        or row.get("source_detection_observed_ts")
        or row.get("alternate_observed_ts")
    )
    start = _as_float(row.get("window_start_s"))
    if observed is None or start is None:
        return None
    elapsed = observed - start
    return elapsed if elapsed >= 0 else None


def _row_would_size_usd(row: dict) -> float | None:
    for key in (
        "guard_sized_copy_usd",
        "target_usd_at_vwap",
        "gap_usd_at_vwap",
        "source_order_usd",
        "latest_source_usd",
    ):
        value = _as_float(row.get(key))
        if value is not None and value >= 0:
            return value
    return None


def _window_time_near_miss_summary(
    guard: dict,
    *,
    since: datetime | None,
    min_elapsed_s: float = WINDOW_TIME_NEAR_MISS_MIN_S,
    max_elapsed_s: float = WINDOW_TIME_NEAR_MISS_MAX_S,
) -> dict[str, Any]:
    rows_seen = 0
    rows_in_lookback = 0
    matches: list[dict[str, Any]] = []
    wallets: set[str] = set()
    would_size_sum = 0.0
    would_size_rows = 0
    for row in _participation_rows(guard):
        row_ts = _participation_row_ts(row)
        if since is not None and (row_ts is None or row_ts < since):
            continue
        rows_in_lookback += 1
        if not _is_window_time_taxonomy(_participation_row_reason(row)):
            continue
        rows_seen += 1
        elapsed = _row_window_elapsed_s(row)
        if elapsed is None or elapsed < min_elapsed_s or elapsed > max_elapsed_s:
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if wallet:
            wallets.add(wallet)
        would_size = _row_would_size_usd(row)
        if would_size is not None:
            would_size_sum += would_size
            would_size_rows += 1
        matches.append(
            {
                "source_wallet": wallet or None,
                "market_slug": row.get("market_slug"),
                "elapsed_s": round(float(elapsed), 6),
                "would_size_usd": None if would_size is None else round(float(would_size), 6),
                "last_seen_at": row.get("last_seen_at"),
            }
        )
    return {
        "flow_stage": "LIVE/DEFEND",
        "reporting_only": True,
        "reason": "window_time_gte_180s",
        "elapsed_range_s": [float(min_elapsed_s), float(max_elapsed_s)],
        "since": since.isoformat() if since else None,
        "rows_in_lookback": rows_in_lookback,
        "window_time_rows_in_lookback": rows_seen,
        "count": len(matches),
        "wallets": sorted(wallets),
        "would_size_usd_sum": round(would_size_sum, 6),
        "would_size_rows": would_size_rows,
        "samples": matches[:10],
        "rule": "counts current-halt window_time_gte_180s rows whose observed window elapsed is in [180s,200s]; reporting only",
    }


def _guard_reject_taxonomy(
    guard: dict,
    *,
    participation_since: datetime | None = None,
) -> dict[str, int]:
    taxonomy: dict[str, int] = defaultdict(int)
    sources = []
    drought = guard.get("drought_funnel") if isinstance(guard.get("drought_funnel"), dict) else {}
    live_execution = guard.get("live_execution") if isinstance(guard.get("live_execution"), dict) else {}
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    if participation_since is None:
        sources.append(drought.get("reject_taxonomy_counts"))
        sources.append(participation.get("dominant_skip_reason_counts"))
    else:
        drought_counts = {
            key: count
            for key, count in _positive_counts(drought.get("reject_taxonomy_counts")).items()
            if not str(key).startswith("window:")
        }
        row_taxonomy, _row_meta = _participation_taxonomy_from_rows(guard, since=participation_since)
        sources.append(drought_counts)
        sources.append(row_taxonomy)
    for gate_name in ("profit_latency_suppression", "toxicity_protection"):
        gate = live_execution.get(gate_name) if isinstance(live_execution.get(gate_name), dict) else {}
        sources.append(gate.get("taxonomy_counts"))
    for counts in sources:
        for key, count in _positive_counts(counts).items():
            taxonomy[key] += count
    return dict(sorted(taxonomy.items()))


def _order_statuses(row: dict) -> set[str]:
    return {
        str(row.get("status") or "").lower(),
        str(row.get("final_status") or "").lower(),
    }


def _order_event_ts(row: dict) -> datetime | None:
    for key in ("updated_at", "submitted_at", "accepted_at", "timestamp", "ts", "created_at"):
        dt = _parse_ts(row.get(key))
        if dt:
            return dt
    return None


def _is_rejected_order(row: dict) -> bool:
    statuses = _order_statuses(row)
    return "rejected" in statuses or "live_rejected" in statuses


def _is_price_band_skip_order(row: dict) -> bool:
    trade_result = row.get("trade_result") if isinstance(row.get("trade_result"), dict) else {}
    price_band = (
        row.get("wallet_copy_price_band")
        if isinstance(row.get("wallet_copy_price_band"), dict)
        else {}
    )
    values = {
        *(_order_statuses(row)),
        str(trade_result.get("error_class") or "").lower(),
        str(trade_result.get("final_status") or "").lower(),
        str(price_band.get("taxonomy") or "").lower(),
    }
    return "price_band_skip" in values


def _classified_reject_reason(row: dict) -> str | None:
    trade_result = row.get("trade_result") if isinstance(row.get("trade_result"), dict) else {}
    fallback = (
        trade_result.get("fallback_parent_result")
        if isinstance(trade_result.get("fallback_parent_result"), dict)
        else {}
    )
    raw_reject = (
        row.get("raw_clob_reject_payload")
        if isinstance(row.get("raw_clob_reject_payload"), dict)
        else {}
    )
    lifecycle_payloads = [
        event.get("payload")
        for event in (row.get("lifecycle") or [])
        if isinstance(event, dict) and isinstance(event.get("payload"), dict)
    ]
    reject_payloads = [trade_result, raw_reject, *lifecycle_payloads]
    error_classes = {
        str(payload.get("error_class") or "").lower()
        for payload in reject_payloads
        if str(payload.get("error_class") or "")
    }
    error_text = " ".join(
        str(payload.get(key) or "").lower()
        for payload in reject_payloads
        for key in ("error", "error_message", "message")
    )
    fallback_error_class = str(fallback.get("error_class") or "").lower()
    if "fak_no_match" in error_classes:
        return "fak_no_match"
    if (
        bool(error_classes & PRE_SUBMIT_REFUSAL_CLASSES)
        and fallback_error_class == "fak_no_match"
    ):
        return "policy_cap_maker_fallback"
    pre_submit_classes = sorted(error_classes & PRE_SUBMIT_REFUSAL_CLASSES)
    if pre_submit_classes:
        return pre_submit_classes[0]
    if "post_only_rejected" in error_classes and "order crosses book" in error_text:
        return "post_only_crosses_book"
    if "post_only_rejected" in error_classes:
        return "post_only_rejected"
    if _is_price_band_skip_order(row):
        return "price_band_skip"
    return None


def _live_order_reject_price_band_summary(
    ledger: dict,
    *,
    since: datetime | None,
) -> dict[str, Any]:
    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    rejected: list[dict] = []
    for row in orders[-500:]:
        if not isinstance(row, dict) or not _is_rejected_order(row):
            continue
        row_ts = _order_event_ts(row)
        if since is not None and row_ts is not None and row_ts <= since:
            continue
        rejected.append(row)
    if not rejected:
        rejected = [
            row
            for row in orders[-50:]
            if isinstance(row, dict) and _is_rejected_order(row)
        ]
    price_band = [row for row in rejected if _is_price_band_skip_order(row)]
    classified_reasons = [_classified_reject_reason(row) for row in rejected]
    classified_counts: dict[str, int] = defaultdict(int)
    for reason in classified_reasons:
        if reason:
            classified_counts[reason] += 1
    latest_ts = max((_order_event_ts(row) for row in rejected), default=None)
    return {
        "recent_rejected_orders": len(rejected),
        "price_band_skip_orders": len(price_band),
        "price_band_skip_only": bool(rejected) and len(price_band) == len(rejected),
        "classified_reject_counts": dict(sorted(classified_counts.items())),
        "all_rejects_classified": bool(rejected) and all(classified_reasons),
        "latest_rejected_ts": latest_ts.isoformat() if latest_ts else None,
    }


def _normalized_gated_quiet_taxonomy(
    taxonomy: dict[str, int],
    ledger: dict,
    *,
    latest_order: datetime | None,
) -> tuple[dict[str, int], dict[str, Any]]:
    reject_summary = _live_order_reject_price_band_summary(ledger, since=latest_order)
    normalized = dict(taxonomy)
    live_reject_count = int(normalized.get("live_order_rejected") or 0)
    if live_reject_count > 0 and reject_summary["price_band_skip_only"]:
        normalized.pop("live_order_rejected", None)
        normalized["price_band_skip"] = int(normalized.get("price_band_skip") or 0) + live_reject_count
    elif live_reject_count > 0 and reject_summary["all_rejects_classified"]:
        normalized.pop("live_order_rejected", None)
        dominant = max(
            reject_summary["classified_reject_counts"],
            key=lambda reason: reject_summary["classified_reject_counts"][reason],
        )
        normalized[dominant] = int(normalized.get(dominant) or 0) + live_reject_count
    ready_count = 0
    for reason in list(normalized):
        if str(reason).removeprefix("window:") == "ready_to_submit":
            ready_count += int(normalized.pop(reason, 0) or 0)
    reject_summary["inflight_ready_to_submit_excluded"] = ready_count
    return dict(sorted(normalized.items())), reject_summary


def _deduplicated_window_alias_taxonomy(taxonomy: dict[str, int]) -> dict[str, int]:
    """Collapse exact reporting aliases without changing deadman decisions."""
    reported = dict(taxonomy)
    for reason, count in list(reported.items()):
        if not str(reason).startswith("window:"):
            continue
        base_reason = str(reason).removeprefix("window:")
        if base_reason in reported and int(reported.get(base_reason) or 0) == int(count or 0):
            reported.pop(reason, None)
    return dict(sorted(reported.items()))


def _armed_probe_latch(guard: dict[str, Any]) -> dict[str, Any]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    candidates = [
        runtime.get("size_defense"),
        runtime.get("roster_size_defense"),
        *[
            row.get("size_defense")
            for row in runtime.get("members") or []
            if isinstance(row, dict)
        ],
    ]
    for raw in candidates:
        row = raw if isinstance(raw, dict) else {}
        armed = bool(
            str(row.get("status") or "") == "PROBE_CAPS_REST_OF_UTC_DAY"
            and (row.get("intraday_probe_latched") is True or row.get("active") is True)
        )
        if armed:
            return {
                "armed": True,
                "status": row.get("status"),
                "intraday_probe_latched": row.get("intraday_probe_latched"),
                "source": row.get("source"),
            }
    return {"armed": False, "status": None, "intraday_probe_latched": False, "source": None}


def _unapproved_gated_quiet_reasons(
    taxonomy: dict[str, int],
    *,
    armed_probe_latch: bool = False,
    local_only_rejects: bool = False,
) -> list[str]:
    unapproved: list[str] = []
    if not taxonomy:
        return unapproved
    for reason in taxonomy:
        base_reason = str(reason).removeprefix("window:")
        if base_reason in GATE_EVIDENCE_STALE_EXACT:
            unapproved.append(reason)
            continue
        if local_only_rejects and (
            base_reason == "live_order_rejected"
            or base_reason in LOCAL_REFUSAL_REJECT_CLASSES
        ):
            continue
        if reason in ARMED_PROBE_GATED_QUIET_EXACT:
            if armed_probe_latch:
                continue
            unapproved.append(reason)
            continue
        if _is_window_time_taxonomy(reason):
            continue
        if reason in APPROVED_GATED_QUIET_EXACT:
            continue
        if any(reason.startswith(prefix) for prefix in APPROVED_GATED_QUIET_PREFIXES):
            continue
        unapproved.append(reason)
    return sorted(unapproved)


def _taxonomy_reason_class(reason: str, *, armed_probe_latch: bool = False) -> str:
    if str(reason).removeprefix("window:") in GATE_EVIDENCE_STALE_EXACT:
        return "gate_evidence_stale_requires_incident_attribution"
    if reason in {"filled", "submitted"}:
        return "terminal_live_participation_residue"
    if reason == "filtered_after_inventory_build":
        return "post_inventory_filter_correct_skip"
    if reason == "hard_entry_floor_skip":
        return "hard_entry_floor_policy_skip"
    if reason == "inventory_residual_gap_below_min_order":
        return "legitimate_inventory_residual_below_min_order"
    if reason == "inventory_target_already_met":
        return "inventory_target_satisfied"
    if reason == "live_order_rejected":
        return "live_reject_requires_price_band_proof"
    if reason in ARMED_PROBE_GATED_QUIET_EXACT:
        return (
            "approved_armed_probe_defense_taxonomy"
            if armed_probe_latch
            else "armed_probe_latch_required"
        )
    if any(reason.startswith(prefix) for prefix in APPROVED_GATED_QUIET_PREFIXES):
        return "approved_prefix_taxonomy"
    if _is_window_time_taxonomy(reason):
        return "approved_gated_quiet_taxonomy"
    if reason in APPROVED_GATED_QUIET_EXACT:
        return "approved_gated_quiet_taxonomy"
    return "unapproved_taxonomy_requires_incident_attribution"


def _taxonomy_attribution(
    taxonomy: dict[str, int],
    unapproved: list[str],
    *,
    armed_probe_latch: bool = False,
) -> dict[str, Any]:
    unapproved_set = set(unapproved)
    rows: dict[str, dict[str, Any]] = {}
    for reason, count in taxonomy.items():
        rows[reason] = {
            "count": int(count or 0),
            "classification": _taxonomy_reason_class(
                str(reason),
                armed_probe_latch=armed_probe_latch,
            ),
            "approval": "unapproved" if reason in unapproved_set else "approved",
            "next_action": (
                "treat as real order-flow evidence until a narrower classifier approves it"
                if reason in unapproved_set
                else "eligible for gated-quiet classification when the other gates pass"
            ),
        }
    return {
        "approved_reasons": {
            reason: row for reason, row in rows.items() if row["approval"] == "approved"
        },
        "unapproved_reasons": {
            reason: row for reason, row in rows.items() if row["approval"] == "unapproved"
        },
    }


def _approved_gated_quiet_taxonomy(
    taxonomy: dict[str, int],
    *,
    armed_probe_latch: bool = False,
) -> bool:
    return not _unapproved_gated_quiet_reasons(
        taxonomy,
        armed_probe_latch=armed_probe_latch,
    )


def _source_coverage_full(guard: dict) -> tuple[bool, dict[str, int | float | None]]:
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    adjusted = (
        participation.get("adjusted_participation")
        if isinstance(participation.get("adjusted_participation"), dict)
        else {}
    )
    denominator = adjusted.get("source_coverage_denominator_windows")
    covered = adjusted.get("source_coverage_windows")
    pct = adjusted.get("source_coverage_rate_pct")
    try:
        denominator_i = int(denominator or 0)
        covered_i = int(covered or 0)
    except (TypeError, ValueError):
        denominator_i = 0
        covered_i = 0
    full = denominator_i > 0 and covered_i >= denominator_i
    return full, {
        "source_coverage_windows": covered_i,
        "source_coverage_denominator_windows": denominator_i,
        "source_coverage_rate_pct": pct,
    }


def _guard_side_halt_signal(
    guard: dict,
    pipe_quiet: dict[str, Any],
    *,
    idle_s: float | None = None,
    latest_order: datetime | None = None,
    now: datetime | None = None,
    previous_last_fresh_actionable_ts: datetime | None = None,
) -> dict[str, Any]:
    taxonomy = _guard_reject_taxonomy(guard)
    price_outside_policy_rejects = sum(
        int(count or 0)
        for reason, count in taxonomy.items()
        if "price_outside_policy" in str(reason)
    )
    drought = guard.get("drought_funnel") if isinstance(guard.get("drought_funnel"), dict) else {}
    try:
        active_set_fresh_signal_rows_total = int(drought.get("active_set_fresh_signal_rows") or 0)
    except (TypeError, ValueError):
        active_set_fresh_signal_rows_total = 0
    demand_split = _active_set_fresh_demand_split(guard)
    selected_wallet = str(demand_split.get("selected_wallet") or "")
    active_set_fresh_signal_rows = int(
        demand_split.get("active_set_selected_fresh_signal_rows")
        if selected_wallet
        else active_set_fresh_signal_rows_total
    )
    floor_blocked = _active_set_floor_blocked_fresh_rows(
        guard,
        source_wallet=selected_wallet or None,
    )
    active_set_floor_blocked_signal_rows = min(
        active_set_fresh_signal_rows,
        int(floor_blocked.get("count") or 0),
    )
    price_policy_blocked = _active_set_price_policy_blocked_fresh_rows(
        guard,
        source_wallet=selected_wallet or None,
    )
    active_set_price_policy_blocked_signal_rows = min(
        max(0, active_set_fresh_signal_rows - active_set_floor_blocked_signal_rows),
        int(price_policy_blocked.get("count") or 0),
    )
    active_set_actionable_signal_rows = max(
        0,
        active_set_fresh_signal_rows
        - active_set_floor_blocked_signal_rows
        - active_set_price_policy_blocked_signal_rows,
    )
    try:
        fresh_buy_rows_le_10s_total = int(pipe_quiet.get("fresh_buy_rows_le_10s") or 0)
    except (TypeError, ValueError):
        fresh_buy_rows_le_10s_total = 0
    fresh_buy_rows_le_10s_total = int(
        demand_split.get("fresh_buy_rows_le_10s_total") or fresh_buy_rows_le_10s_total
    )
    fresh_buy_rows_le_10s = int(
        demand_split.get("selected_fresh_buy_rows_le_10s")
        if selected_wallet
        else fresh_buy_rows_le_10s_total
    )
    submittability = (
        guard.get("runtime_member_submittability")
        if isinstance(guard.get("runtime_member_submittability"), dict)
        else {}
    )
    member_unsubmittable = str(submittability.get("status") or "") == "INCIDENT_MEMBER_UNSUBMITTABLE"
    # A pulse whose every active-set fresh row is a deliberate price-band
    # reject may double-count the same physical buy in fresh_buy_rows_le_10s;
    # only then discount it, so a genuine in-band fresh buy always counts.
    fresh_buy_rows_effective = fresh_buy_rows_le_10s
    if active_set_actionable_signal_rows == 0 and active_set_price_policy_blocked_signal_rows > 0:
        fresh_buy_rows_effective = max(
            0,
            fresh_buy_rows_le_10s - active_set_price_policy_blocked_signal_rows,
        )
    selected_fresh_actionable_signal_rows = active_set_actionable_signal_rows + fresh_buy_rows_effective
    fresh_actionable_signal_rows = selected_fresh_actionable_signal_rows
    last_fresh_actionable_ts = previous_last_fresh_actionable_ts
    last_fresh_actionable_source = "previous_state" if last_fresh_actionable_ts is not None else None
    if fresh_actionable_signal_rows > 0 and now is not None:
        last_fresh_actionable_ts = now
        last_fresh_actionable_source = "current_pulse"
    last_fresh_newer_than_latest_order = (
        last_fresh_actionable_ts is not None
        and (latest_order is None or last_fresh_actionable_ts > latest_order)
    )
    guard_event_fresh = bool(
        pipe_quiet.get("guard_event_fresh")
        or pipe_quiet.get("guard_event_log_fresh")
        or pipe_quiet.get("guard_state_fresh")
    )
    taxonomy_has_activity = bool(taxonomy)
    taxonomy_total = sum(int(count or 0) for count in taxonomy.values())
    inventory_met_rows = sum(
        int(count or 0)
        for reason, count in taxonomy.items()
        if "inventory_target_already_met" in str(reason)
    )
    inventory_met_dominant = taxonomy_total > 0 and inventory_met_rows / taxonomy_total >= 0.8
    benign_inventory_met_skip = (
        inventory_met_dominant
        and fresh_actionable_signal_rows == 0
        and not member_unsubmittable
    )
    guard_evaluation_absent_or_stale = not taxonomy_has_activity or not guard_event_fresh
    stale_guard_fresh_actionable = (
        fresh_actionable_signal_rows > 0
        and guard_evaluation_absent_or_stale
    )
    backstop_active = (
        idle_s is not None
        and idle_s >= GUARD_SIDE_HALT_BACKSTOP_S
        and last_fresh_newer_than_latest_order
    )
    would_fire = []
    if stale_guard_fresh_actionable:
        would_fire.append("fresh_actionable_guard_evaluation_stale")
    if member_unsubmittable:
        would_fire.append("member_unsubmittable")
    if backstop_active:
        would_fire.append("backstop_active")
    benign_skip_overrode = would_fire if benign_inventory_met_skip else []
    warning = (
        "UNSUBMITTABLE_MEMBER_UNDER_BENIGN_SKIP"
        if "member_unsubmittable" in benign_skip_overrode
        else None
    )
    active = bool(would_fire) and not benign_inventory_met_skip
    return {
        "active": active,
        "member_unsubmittable": member_unsubmittable,
        "runtime_member_submittability": submittability,
        "price_outside_policy_rejects": price_outside_policy_rejects,
        "active_set_fresh_signal_rows": active_set_fresh_signal_rows,
        "active_set_fresh_signal_rows_total": active_set_fresh_signal_rows_total,
        "active_set_actionable_signal_rows": active_set_actionable_signal_rows,
        "active_set_floor_blocked_signal_rows": active_set_floor_blocked_signal_rows,
        "active_set_floor_blocked_samples": floor_blocked.get("samples", []),
        "active_set_price_policy_blocked_signal_rows": active_set_price_policy_blocked_signal_rows,
        "active_set_price_policy_blocked_samples": price_policy_blocked.get("samples", []),
        "active_set_fresh_demand_split": demand_split,
        "fresh_buy_rows_le_10s": fresh_buy_rows_le_10s,
        "fresh_buy_rows_le_10s_total": fresh_buy_rows_le_10s_total,
        "fresh_buy_rows_effective": fresh_buy_rows_effective,
        "selected_fresh_actionable_signal_rows": selected_fresh_actionable_signal_rows,
        "nonselected_rotation_pressure_rows": int(
            demand_split.get("nonselected_rotation_pressure_rows") or 0
        ),
        "nonselected_rotation_event_rows": int(
            demand_split.get("nonselected_rotation_event_rows") or 0
        ),
        "demand_evidence_limb": demand_split.get("demand_evidence_limb", []),
        "nonselected_rotation_pressure": {
            "active": int(demand_split.get("nonselected_rotation_pressure_rows") or 0) > 0,
            "classification": demand_split.get("classification"),
            "wallets": demand_split.get("nonselected_wallets", []),
            "samples": demand_split.get("nonselected_samples", []),
            "rule": demand_split.get("rule"),
        },
        "fresh_actionable_signal_rows": fresh_actionable_signal_rows,
        "last_fresh_actionable_ts": (
            last_fresh_actionable_ts.isoformat() if last_fresh_actionable_ts else None
        ),
        "last_fresh_actionable_source": last_fresh_actionable_source,
        "last_fresh_actionable_newer_than_latest_order": last_fresh_newer_than_latest_order,
        "guard_event_fresh": guard_event_fresh,
        "taxonomy_has_activity": taxonomy_has_activity,
        "taxonomy_total_rows": taxonomy_total,
        "inventory_target_already_met_rows": inventory_met_rows,
        "inventory_met_dominant": inventory_met_dominant,
        "benign_inventory_met_skip": benign_inventory_met_skip,
        "benign_skip_overrode": benign_skip_overrode,
        "warning": warning,
        "guard_evaluation_absent_or_stale": guard_evaluation_absent_or_stale,
        "backstop_active": backstop_active,
        "backstop_s": GUARD_SIDE_HALT_BACKSTOP_S,
        "taxonomy": taxonomy,
        "rule": (
            "fresh actionable selected-member signal plus absent/stale guard evaluation "
            "is guard-side halt; stale observations and policy taxonomy alone are source quiet; "
            "90min backstop uses persisted last_fresh_actionable_ts after the latest accepted order; "
            "inventory-target-met, effective-min floor-blocked, and deliberate "
            "price-band policy rejects are satisfied or unsubmittable demand and remain measured; "
            "non-selected active-set demand is rotation pressure, not submit demand"
        ),
    }


def _active_morning_bench_ruling(active_set_state: dict, guard: dict, now: datetime) -> dict[str, Any]:
    members = active_set_state.get("members") if isinstance(active_set_state.get("members"), list) else []
    active_rows: list[dict[str, Any]] = []
    has_ruling = False
    for member in members:
        if not isinstance(member, dict):
            continue
        bench = member.get("hour_band_bench") if isinstance(member.get("hour_band_bench"), dict) else {}
        status = str(member.get("status") or "")
        if not bench and not status.startswith("HOUR_BENCHED"):
            continue
        has_ruling = True
        expiry = _parse_ts(bench.get("auto_return_at") or member.get("auto_return_at"))
        if expiry is None or now >= expiry:
            continue
        active_rows.append(
            {
                "candidate_id": str(member.get("candidate_id") or ""),
                "source_wallet": str(member.get("source_wallet") or member.get("wallet") or "").lower(),
                "status": status,
                "classification": bench.get("classification"),
                "fable_direction_id": bench.get("fable_direction_id"),
                "auto_return_at": expiry.isoformat(),
                "seat_preserved": bench.get("seat_preserved"),
                "morning_min_size_probe_usd": bench.get("morning_min_size_probe_usd"),
                "trigger": bench.get("trigger"),
                "trigger_pnl_usd": bench.get("trigger_pnl_usd"),
                "next_action": bench.get("next_action"),
            }
        )
    active_set_runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    size_defense = active_set_runtime.get("size_defense") if isinstance(active_set_runtime.get("size_defense"), dict) else {}
    selected_member = (
        active_set_runtime.get("selected_member")
        if isinstance(active_set_runtime.get("selected_member"), dict)
        else {}
    )
    selected_size_defense = (
        selected_member.get("size_defense")
        if isinstance(selected_member.get("size_defense"), dict)
        else {}
    )
    selected_status = str(selected_size_defense.get("status") or size_defense.get("status") or "")
    ruling_active = bool(active_rows)
    return {
        "active": ruling_active,
        "has_ruling": has_ruling,
        "status": "ACTIVE" if ruling_active else "CLEAR",
        "classification": "MEASURED_SKIP_MORNING_BENCH" if ruling_active else "CLEAR",
        "flow_stage": "LIVE/DEFEND",
        "rows": active_rows,
        "expires_at": min((row["auto_return_at"] for row in active_rows), default=None),
        "selected_member_probe_cap_status": selected_status,
        "selected_member": {
            "candidate_id": str(selected_member.get("candidate_id") or ""),
            "source_wallet": str(selected_member.get("source_wallet") or "").lower(),
            "policy_id": str(selected_member.get("policy_id") or ""),
            "max_order_usd": selected_member.get("max_order_usd"),
            "size_defense": selected_size_defense or size_defense,
        },
        "rule": (
            "Fable-ruled hour-band bench with a future auto-return is governed morning bench; "
            "it may classify a source-quiet/order-idle span as measured skip until expiry"
        ),
    }


def _morning_bench_gate_attribution(
    *,
    morning_bench: dict[str, Any],
    morning_bench_pass: bool,
    source_quiet_verified: bool,
    eligible_drought_status: str,
    guard_side_halt: dict[str, Any],
) -> dict[str, Any]:
    failing_reasons: list[str] = []
    has_ruling = bool(morning_bench.get("has_ruling"))
    active = bool(morning_bench.get("active"))
    if has_ruling and not active:
        failing_reasons.append("no_active_future_hour_band_bench_ruling")
    elif active:
        if not source_quiet_verified:
            failing_reasons.append("pipe_source_quiet_not_verified")
        if eligible_drought_status != "OK":
            failing_reasons.append(f"eligible_drought_status_{eligible_drought_status}")
        if bool(guard_side_halt.get("fresh_actionable_signal_rows")):
            failing_reasons.append("fresh_actionable_signal_rows_present")
    return {
        "gate": "morning_bench_governed_until_expiry",
        "passed": bool(morning_bench_pass),
        "active_ruling": active,
        "has_ruling": has_ruling,
        "expires_at": morning_bench.get("expires_at"),
        "failing_reasons": failing_reasons,
        "rule": (
            "passes vacuously when no Fable hour-band bench exists; otherwise passes only while "
            "a bench has a future auto-return, the pipe is verified source-quiet, eligible "
            "drought is OK, and no fresh actionable rows are present"
        ),
    }


def _floor_deadlock_classification(
    *,
    guard_side_halt: dict[str, Any],
    taxonomy: dict[str, int],
    unapproved_reasons: list[str],
    lookback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    submittability = (
        guard_side_halt.get("runtime_member_submittability")
        if isinstance(guard_side_halt.get("runtime_member_submittability"), dict)
        else {}
    )
    member_submittable = str(submittability.get("status") or "") == "PASS"
    guard_fresh = bool(guard_side_halt.get("guard_event_fresh")) and not bool(
        guard_side_halt.get("guard_evaluation_absent_or_stale")
    )
    try:
        active_set_actionable = int(guard_side_halt.get("active_set_actionable_signal_rows") or 0)
    except (TypeError, ValueError):
        active_set_actionable = 0
    floor_taxonomy_present = any(
        str(reason).removeprefix("window:") in FLOOR_DEADLOCK_EXACT_REASONS
        for reason, count in taxonomy.items()
        if int(count or 0) > 0
    )
    taxonomy_accounted = bool(taxonomy) and not unapproved_reasons
    active = (
        bool(guard_side_halt.get("active"))
        and member_submittable
        and guard_fresh
        and taxonomy_accounted
        and floor_taxonomy_present
        and active_set_actionable == 0
        and not bool(guard_side_halt.get("member_unsubmittable"))
    )
    return {
        "active": active,
        "status": "FLOOR_DEADLOCK" if active else "CLEAR",
        "flow_stage": "LIVE/DEFEND",
        "member_submittable": member_submittable,
        "guard_event_fresh": guard_fresh,
        "active_set_actionable_signal_rows": active_set_actionable,
        "floor_taxonomy_present": floor_taxonomy_present,
        "taxonomy_accounted": taxonomy_accounted,
        "unapproved_gate_reasons": unapproved_reasons,
        "taxonomy": taxonomy,
        "lookback": lookback or {},
        "rule": (
            "guard-side halt is measured floor_deadlock only when the runtime member is submittable, "
            "guard evaluation is fresh, active-set demand is fully accounted by approved floor/late/met/policy "
            "classes inside the current halt lookback, and no unaccounted actionable active-set row remains"
        ),
    }


def _current_halt_timing_skip_classification(
    *,
    guard_side_halt: dict[str, Any],
    taxonomy: dict[str, int],
    unapproved_reasons: list[str],
    floor_taxonomy_present: bool,
    eligible_drought_status: str,
    lookback: dict[str, Any],
) -> dict[str, Any]:
    reasons = _base_taxonomy_reasons(taxonomy)
    try:
        active_set_actionable = int(guard_side_halt.get("active_set_actionable_signal_rows") or 0)
    except (TypeError, ValueError):
        active_set_actionable = 0
    non_timing_reasons = {
        reason for reason in reasons if reason not in TIMING_SKIP_EXACT_REASONS and not _is_window_time_taxonomy(reason)
    }
    timing_only = bool(reasons) and not non_timing_reasons
    active = (
        bool(guard_side_halt.get("active"))
        and timing_only
        and not unapproved_reasons
        and not floor_taxonomy_present
        and active_set_actionable == 0
        and eligible_drought_status == "OK"
        and bool(guard_side_halt.get("guard_event_fresh"))
        and not bool(guard_side_halt.get("member_unsubmittable"))
    )
    return {
        "active": active,
        "status": "MEASURED_TIMING_SKIP" if active else "CLEAR",
        "flow_stage": "LIVE/DEFEND",
        "active_set_actionable_signal_rows": active_set_actionable,
        "timing_only": timing_only,
        "reasons": sorted(reasons),
        "taxonomy": taxonomy,
        "unapproved_gate_reasons": unapproved_reasons,
        "lookback": lookback,
        "rule": (
            "current-halt late-window/terminal timing taxonomy with zero actionable active-set rows "
            "is measured, not guard-side halt or floor-deadlock"
        ),
    }


def _adjusted_consecutive_missed(guard: dict) -> int | None:
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    adjusted = (
        participation.get("adjusted_participation")
        if isinstance(participation.get("adjusted_participation"), dict)
        else {}
    )
    value = adjusted.get("adjusted_consecutive_missed_active_windows")
    if value is None:
        value = participation.get("consecutive_missed_active_windows")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _gated_quiet_classification(
    *,
    guard: dict,
    active_set_state: dict,
    ledger: dict,
    now: datetime,
    idle_s: float | None,
    max_idle_s: float,
    latest_order: datetime | None,
    eligible_drought_status: str,
    latest_suppression: datetime | None,
    pipe_quiet: dict[str, Any] | None = None,
    previous_last_fresh_actionable_ts: datetime | None = None,
    local_only_rejects: bool = False,
    accepted_orders: int = 0,
    pipe_healthy_terms: bool = False,
) -> dict[str, Any]:
    pipe_quiet_payload = pipe_quiet if isinstance(pipe_quiet, dict) else {}
    source_quiet_verified = bool(pipe_quiet_payload.get("verified"))
    source_quiet_within_legacy_backstop = (
        idle_s is not None and idle_s <= SOURCE_QUIET_HARD_BACKSTOP_S
    )
    raw_taxonomy = _guard_reject_taxonomy(guard)
    taxonomy, live_reject_summary = _normalized_gated_quiet_taxonomy(
        raw_taxonomy,
        ledger,
        latest_order=latest_order,
    )
    armed_probe_latch = _armed_probe_latch(guard)
    unapproved_reasons = _unapproved_gated_quiet_reasons(
        taxonomy,
        armed_probe_latch=bool(armed_probe_latch.get("armed")),
        local_only_rejects=local_only_rejects,
    )
    taxonomy_attribution = _taxonomy_attribution(
        taxonomy,
        unapproved_reasons,
        armed_probe_latch=bool(armed_probe_latch.get("armed")),
    )
    taxonomy_approved = not unapproved_reasons
    source_coverage_full, source_coverage = _source_coverage_full(guard)
    adjusted_consecutive = _adjusted_consecutive_missed(guard)
    suppression_age_s = (now - latest_suppression).total_seconds() if latest_suppression else None
    guard_side_halt = _guard_side_halt_signal(
        guard,
        pipe_quiet_payload,
        idle_s=idle_s,
        latest_order=latest_order,
        now=now,
        previous_last_fresh_actionable_ts=previous_last_fresh_actionable_ts,
    )
    selected_demand_after_latest_order = bool(
        guard_side_halt.get("last_fresh_actionable_newer_than_latest_order")
    )
    measured_skip_within_backstop = bool(
        idle_s is not None
        and (
            idle_s <= GATED_QUIET_HARD_BACKSTOP_S
            or not selected_demand_after_latest_order
        )
    )
    morning_bench = _active_morning_bench_ruling(active_set_state, guard, now)
    morning_bench_has_ruling = bool(morning_bench.get("has_ruling"))
    morning_bench_pass = (
        not morning_bench_has_ruling
        or (
            bool(morning_bench.get("active"))
            and
            source_quiet_verified
            and eligible_drought_status == "OK"
            and not bool(guard_side_halt.get("fresh_actionable_signal_rows"))
        )
    )
    morning_bench_active_pass = bool(morning_bench.get("active")) and morning_bench_pass
    morning_bench_attribution = _morning_bench_gate_attribution(
        morning_bench=morning_bench,
        morning_bench_pass=morning_bench_pass,
        source_quiet_verified=source_quiet_verified,
        eligible_drought_status=eligible_drought_status,
        guard_side_halt=guard_side_halt,
    )
    floor_lookback_s = max(
        FLOOR_DEADLOCK_MIN_LOOKBACK_S,
        float(idle_s) if idle_s is not None and idle_s > 0 else 0.0,
    )
    floor_lookback_start = now - timedelta(seconds=floor_lookback_s)
    floor_taxonomy_since = (
        latest_order
        if latest_order is not None and latest_order > floor_lookback_start
        else floor_lookback_start
    )
    floor_raw_taxonomy = _guard_reject_taxonomy(guard, participation_since=floor_taxonomy_since)
    floor_taxonomy, _floor_live_reject_summary = _normalized_gated_quiet_taxonomy(
        floor_raw_taxonomy,
        ledger,
        latest_order=latest_order,
    )
    floor_unapproved_reasons = _unapproved_gated_quiet_reasons(
        floor_taxonomy,
        armed_probe_latch=bool(armed_probe_latch.get("armed")),
        local_only_rejects=local_only_rejects,
    )
    _floor_participation_taxonomy, floor_participation_lookback = _participation_taxonomy_from_rows(
        guard,
        since=floor_taxonomy_since,
    )
    floor_participation_lookback.update(
        {
            "lookback_s": round(float(floor_lookback_s), 6),
            "idle_s": None if idle_s is None else round(float(idle_s), 6),
            "min_lookback_s": FLOOR_DEADLOCK_MIN_LOOKBACK_S,
            "lookback_start": floor_lookback_start.isoformat(),
            "latest_order_floor": latest_order.isoformat() if latest_order else None,
            "raw_taxonomy": floor_raw_taxonomy,
        }
    )
    floor_deadlock = _floor_deadlock_classification(
        guard_side_halt=guard_side_halt,
        taxonomy=floor_taxonomy,
        unapproved_reasons=floor_unapproved_reasons,
        lookback=floor_participation_lookback,
    )
    timing_skip = _current_halt_timing_skip_classification(
        guard_side_halt=guard_side_halt,
        taxonomy=floor_taxonomy,
        unapproved_reasons=floor_unapproved_reasons,
        floor_taxonomy_present=bool(floor_deadlock.get("floor_taxonomy_present")),
        eligible_drought_status=eligible_drought_status,
        lookback=floor_participation_lookback,
    )
    window_time_near_miss = _window_time_near_miss_summary(
        guard,
        since=floor_taxonomy_since,
    )
    checks = {
        "idle_over_deadman_s": idle_s is not None and idle_s > max_idle_s,
        "idle_under_hard_backstop_s": measured_skip_within_backstop,
        "source_quiet_idle_under_legacy_hard_backstop_s": source_quiet_within_legacy_backstop,
        "eligible_drought_ok": eligible_drought_status == "OK",
        "guard_state_fresh": bool(pipe_quiet_payload.get("guard_state_fresh")),
        "poller_fresh": bool(pipe_quiet_payload.get("poller_fresh")),
        "api_clean": bool(pipe_quiet_payload.get("api_clean")),
        "taxonomy_has_activity": bool(guard_side_halt.get("taxonomy_has_activity")),
        "source_coverage_full": source_coverage_full,
        "adjusted_consecutive_missed_lte_1": (
            adjusted_consecutive is not None and adjusted_consecutive <= 1
        ),
        "reject_taxonomy_approved_only": taxonomy_approved,
        "pipe_verified_source_quiet": source_quiet_verified,
        "no_guard_side_halt_signal": not bool(guard_side_halt.get("active")),
        "morning_bench_governed_until_expiry": morning_bench_pass,
    }
    source_quiet_checks = {
        key: checks[key]
        for key in (
            "idle_over_deadman_s",
            "idle_under_hard_backstop_s",
            "eligible_drought_ok",
            "pipe_verified_source_quiet",
            "no_guard_side_halt_signal",
        )
    }
    measured_skip_checks = {
        key: checks[key]
        for key in (
            "idle_over_deadman_s",
            "idle_under_hard_backstop_s",
            "eligible_drought_ok",
            "guard_state_fresh",
            "poller_fresh",
            "api_clean",
            "taxonomy_has_activity",
            "source_coverage_full",
            "adjusted_consecutive_missed_lte_1",
            "reject_taxonomy_approved_only",
        )
    }
    source_quiet_pass = all(source_quiet_checks.values())
    measured_skip_pass = all(measured_skip_checks.values())
    nonselected_pressure_active = (
        int(guard_side_halt.get("nonselected_rotation_pressure_rows") or 0) > 0
        and int(guard_side_halt.get("selected_fresh_actionable_signal_rows") or 0) == 0
        and not bool(guard_side_halt.get("member_unsubmittable"))
    )
    nonselected_rotation_event_active = (
        int(guard_side_halt.get("nonselected_rotation_event_rows") or 0) > 0
        and int(guard_side_halt.get("selected_fresh_actionable_signal_rows") or 0) == 0
        and not bool(guard_side_halt.get("member_unsubmittable"))
    )
    classification = (
        "MEASURED_SKIP_MORNING_BENCH"
        if morning_bench_active_pass
        else "FLOW_ALIVE_ACCEPTED_ORDERS"
        if int(accepted_orders or 0) > 0 and pipe_healthy_terms
        else "FLOOR_DEADLOCK"
        if bool(floor_deadlock.get("active"))
        else "MEASURED_TIMING_SKIP"
        if bool(timing_skip.get("active"))
        else "GUARD_SIDE_HALT"
        if bool(guard_side_halt.get("active"))
        else "MEASURED_NONSELECTED_DEMAND"
        if source_quiet_pass and nonselected_pressure_active
        else "MEASURED_NONSELECTED_ROTATION_EVENT"
        if source_quiet_pass and nonselected_rotation_event_active
        else "MEASURED_SOURCE_QUIET"
        if source_quiet_pass
        else "MEASURED_SKIP_GATED_QUIET"
        if measured_skip_pass
        else "ORDER_FLOW_DEAD"
    )
    return {
        "status": "PASS" if classification != "ORDER_FLOW_DEAD" else "FAIL",
        "classification": classification,
        "checks": checks,
        "source_quiet_checks": source_quiet_checks,
        "measured_skip_checks": measured_skip_checks,
        "raw_gate_taxonomy": raw_taxonomy,
        "deduplicated_gate_taxonomy": _deduplicated_window_alias_taxonomy(taxonomy),
        "decision_gate_taxonomy": taxonomy,
        "unapproved_gate_reasons": unapproved_reasons,
        "taxonomy_attribution": taxonomy_attribution,
        "armed_probe_latch": armed_probe_latch,
        "live_order_reject_price_band_summary": live_reject_summary,
        "accepted_orders": int(accepted_orders or 0),
        "pipe_healthy_terms": bool(pipe_healthy_terms),
        "latest_approved_suppression_age_s": suppression_age_s,
        "selected_demand_after_latest_order": selected_demand_after_latest_order,
        "source_coverage": source_coverage,
        "adjusted_consecutive_missed_active_windows": adjusted_consecutive,
        "hard_backstop_s": GATED_QUIET_HARD_BACKSTOP_S,
        "measured_skip_hard_backstop_s": GATED_QUIET_HARD_BACKSTOP_S,
        "source_quiet_hard_backstop_s": GATED_QUIET_HARD_BACKSTOP_S,
        "source_quiet_hard_backstop_mode": "demand_aware_selected_member_only",
        "guard_side_halt_signal": guard_side_halt,
        "floor_deadlock_classification": floor_deadlock,
        "current_halt_timing_skip_classification": timing_skip,
        "window_time_near_miss_summary": window_time_near_miss,
        "morning_bench_ruling": morning_bench,
        "morning_bench_gate_attribution": morning_bench_attribution,
        "nonselected_rotation_pressure": guard_side_halt.get("nonselected_rotation_pressure", {}),
    }


def _guard_pid_from_state(guard: dict[str, Any]) -> int | None:
    candidates = [
        guard.get("pid") if isinstance(guard, dict) else None,
        (
            guard.get("guard_code_identity", {}).get("pid")
            if isinstance(guard.get("guard_code_identity"), dict)
            else None
        ),
    ]
    for value in candidates:
        try:
            pid = int(value or 0)
        except (TypeError, ValueError):
            continue
        if pid > 0:
            return pid
    return None


def _process_memory_probe(pid: int) -> dict[str, Any]:
    command = ["ps", "-p", str(int(pid)), "-o", "rss="]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=5.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "command": command,
            "verified_pid": int(pid),
            "returncode": None,
            "raw_ps_line": None,
            "rss_kib": None,
            "phys_footprint_bytes": None,
            "error": str(exc),
        }
    raw_ps_line = next((line for line in (completed.stdout or "").splitlines() if line.strip()), None)
    rss_kib = None
    if completed.returncode != 0:
        raw_ps_line = raw_ps_line.strip() if raw_ps_line else None
    elif raw_ps_line:
        try:
            parsed = int(float(raw_ps_line.strip().split()[0]))
        except (TypeError, ValueError, IndexError):
            parsed = 0
        rss_kib = parsed if parsed > 0 else None
    return {
        "command": command,
        "verified_pid": int(pid),
        "returncode": completed.returncode,
        "raw_ps_line": raw_ps_line.strip() if raw_ps_line else None,
        "rss_kib": rss_kib,
        "phys_footprint_bytes": None,
        "source": "ps -p <verified_guard_pid> -o rss=",
    }


def _process_rss_kib(pid: int) -> int | None:
    return _process_memory_probe(pid).get("rss_kib")


def _parse_last_stdout_json(stdout: str) -> dict[str, Any]:
    for line in reversed((stdout or "").splitlines()):
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _run_memory_restart_actuator(
    root: Path,
    restart_script: str,
    *,
    cooldown_s: float,
) -> dict[str, Any]:
    script = Path(restart_script)
    if not script.is_absolute():
        script = root / script
    command = [
        sys.executable,
        str(script),
        "--execute",
        "--cooldown-s",
        str(float(cooldown_s)),
        "--force-restart-reason",
        "guard_memory_rss_threshold",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
            timeout=60.0,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        return {
            "status": "ACTUATOR_TIMEOUT",
            "returncode": None,
            "command": command,
            "stdout_tail": stdout[-2000:],
            "stderr_tail": stderr[-2000:],
            "decision": {},
        }
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "status": "ACTUATOR_ERROR",
            "returncode": None,
            "command": command,
            "stdout_tail": "",
            "stderr_tail": str(exc)[-2000:],
            "decision": {},
        }
    decision = _parse_last_stdout_json(completed.stdout or "")
    return {
        "status": str(decision.get("status") or ("PASS" if completed.returncode == 0 else "ACTUATOR_ERROR")),
        "returncode": completed.returncode,
        "command": command,
        "stdout_tail": (completed.stdout or "")[-2000:],
        "stderr_tail": (completed.stderr or "")[-2000:],
        "decision": decision,
    }


def _guard_memory_sample_attribution(guard: dict[str, Any], now: datetime) -> dict[str, Any]:
    profile = guard.get("guard_loop_profile") if isinstance(guard.get("guard_loop_profile"), dict) else {}
    stage_timers = profile.get("stage_timers") if isinstance(profile.get("stage_timers"), list) else []
    stage_rows = [row for row in stage_timers if isinstance(row, dict)]
    last_stage = stage_rows[-1] if stage_rows else {}
    top_stage = max(
        stage_rows,
        key=lambda row: _as_float(row.get("duration_s")) or 0.0,
        default={},
    )
    cycle_started_at = _parse_ts(profile.get("cycle_started_at"))
    cycle_duration_s = _as_float(profile.get("cycle_duration_s") or profile.get("total_s_before_state_write"))
    sample_offset_s = None
    active_stage = None
    if cycle_started_at is not None:
        sample_offset_s = round(max(0.0, (now - cycle_started_at).total_seconds()), 6)
    if sample_offset_s is not None and cycle_duration_s is not None and 0.0 <= sample_offset_s <= cycle_duration_s:
        for row in stage_rows:
            elapsed_s = _as_float(row.get("elapsed_s"))
            duration_s = _as_float(row.get("duration_s"))
            if elapsed_s is None or duration_s is None:
                continue
            started_s = max(0.0, elapsed_s - duration_s)
            if started_s <= sample_offset_s <= elapsed_s:
                active_stage = str(row.get("name") or "")
                break
    attribution = {
        "source": "wallet_copy_live_guard_state.guard_loop_profile",
        "cycle": guard.get("cycle"),
        "cycle_outcome": guard.get("cycle_outcome"),
        "cycle_started_at": profile.get("cycle_started_at"),
        "cycle_duration_s": cycle_duration_s,
        "sample_offset_s": sample_offset_s,
        "active_stage": active_stage,
        "last_completed_stage": last_stage.get("name"),
        "last_completed_stage_elapsed_s": last_stage.get("elapsed_s"),
        "last_completed_stage_duration_s": last_stage.get("duration_s"),
        "top_stage": top_stage.get("name"),
        "top_stage_duration_s": top_stage.get("duration_s"),
        "stage_timer_count": len(stage_rows),
        "rule": "RSS samples carry guard cycle/stage context for same-day leak diagnosis",
    }
    if sample_offset_s is not None and cycle_duration_s is not None and sample_offset_s > cycle_duration_s:
        attribution["active_stage_status"] = "BETWEEN_STATE_WRITES_OR_LATER_CYCLE"
        attribution["state_age_s"] = round(sample_offset_s - cycle_duration_s, 6)
    elif active_stage:
        attribution["active_stage_status"] = "IN_RECORDED_CYCLE_WINDOW"
    else:
        attribution["active_stage_status"] = "UNKNOWN_NO_STAGE_TIMER_MATCH"
    return attribution


def _guard_memory_spike_reference(samples: list[dict[str, Any]], pid: int | None) -> dict[str, Any]:
    values = [
        _as_float(row.get("rss_gib"))
        for row in samples
        if row.get("pid") == pid and _as_float(row.get("rss_gib")) is not None
    ]
    values = [float(value) for value in values if value is not None]
    if not values:
        return {"median_rss_gib": None, "sample_count": 0}
    return {
        "median_rss_gib": round(float(statistics.median(values)), 6),
        "sample_count": len(values),
    }


def _out_of_band_attribution(value: Any) -> dict[str, Any]:
    attribution = value if isinstance(value, dict) else {}
    while isinstance(attribution.get("out_of_band"), dict):
        attribution = attribution["out_of_band"]
    return attribution


def _guard_memory_spike_annotation(
    *,
    rss_gib: float | None,
    spike_reference: dict[str, Any],
    attribution: dict[str, Any],
) -> dict[str, Any] | None:
    median_rss_gib = _as_float(spike_reference.get("median_rss_gib"))
    if rss_gib is None or median_rss_gib is None or median_rss_gib <= 0:
        return None
    multiple = float(rss_gib) / median_rss_gib
    if multiple <= 2.0:
        return None
    return {
        "status": "RSS_GT_2X_ROLLING_MEDIAN",
        "rss_gib": rss_gib,
        "rolling_median_rss_gib": round(median_rss_gib, 6),
        "multiple": round(multiple, 6),
        "reference_sample_count": spike_reference.get("sample_count"),
        "cycle": attribution.get("cycle"),
        "task_name": attribution.get("active_stage") or attribution.get("last_completed_stage"),
        "active_stage": attribution.get("active_stage"),
        "last_completed_stage": attribution.get("last_completed_stage"),
        "cycle_outcome": attribution.get("cycle_outcome"),
        "rule": "record cycle/task context when RSS sample exceeds 2x rolling median",
    }


def _guard_memory_snapshot(
    root: Path,
    guard: dict[str, Any],
    prev: dict[str, Any],
    now: datetime,
    *,
    warn_gib: float,
    restart_gib: float,
    cooldown_s: float,
    restart_script: str,
) -> dict[str, Any]:
    pid = _guard_pid_from_state(guard)
    memory_probe = _process_memory_probe(pid) if pid is not None else {
        "command": None,
        "verified_pid": None,
        "returncode": None,
        "raw_ps_line": None,
        "rss_kib": None,
        "phys_footprint_bytes": None,
        "error": "guard pid unavailable",
    }
    rss_kib = memory_probe.get("rss_kib")
    rss_gib = None if rss_kib is None else round(float(rss_kib) / (1024.0 * 1024.0), 6)
    previous = prev.get("guard_memory") if isinstance(prev.get("guard_memory"), dict) else {}
    samples = previous.get("samples") if isinstance(previous.get("samples"), list) else []
    clean_samples = [row for row in samples if isinstance(row, dict)]
    previous_same_pid = next(
        (
            row
            for row in reversed(clean_samples)
            if row.get("pid") == pid and _as_float(row.get("rss_gib")) is not None
        ),
        None,
    )
    attribution = _guard_memory_sample_attribution(guard, now)
    spike_reference = _guard_memory_spike_reference(clean_samples, pid)
    spike = _guard_memory_spike_annotation(
        rss_gib=rss_gib,
        spike_reference=spike_reference,
        attribution=attribution,
    )
    sample = {
        "checked_at": now.isoformat(),
        "pid": pid,
        "rss_gib": rss_gib,
        "attribution": attribution,
    }
    if rss_kib is not None:
        sample["rss_kib"] = int(rss_kib)
    if spike is not None:
        sample["rss_spike"] = spike
    next_samples = [*clean_samples, sample][-6:]
    same_pid_samples = [row for row in next_samples if row.get("pid") == pid]
    rss_samples = [
        row for row in same_pid_samples if _as_float(row.get("rss_gib")) is not None
    ]
    peak_sample = max(
        rss_samples,
        key=lambda row: _as_float(row.get("rss_gib")) or 0.0,
        default={},
    )
    rss_peak_gib = _as_float(peak_sample.get("rss_gib"))
    rss_peak_at = peak_sample.get("checked_at") if peak_sample else None
    dated_rss_samples = [
        (row, _parse_ts(row.get("checked_at")))
        for row in rss_samples
    ]
    dated_rss_samples = [
        (row, checked_at)
        for row, checked_at in dated_rss_samples
        if checked_at is not None
    ]
    dated_rss_samples.sort(key=lambda item: item[1])
    rss_values = [
        float(_as_float(row.get("rss_gib")) or 0.0)
        for row, _ in dated_rss_samples
    ]
    rss_observation_span_s = (
        round(
            (
                dated_rss_samples[-1][1] - dated_rss_samples[0][1]
            ).total_seconds(),
            6,
        )
        if len(dated_rss_samples) >= 2
        else 0.0
    )
    rss_observation = {
        "status": (
            "MEASURED"
            if len(rss_values) >= 3 and rss_observation_span_s >= 120.0
            else "INSUFFICIENT_OBSERVATION"
        ),
        "pid": pid,
        "sample_count": len(rss_values),
        "span_s": rss_observation_span_s,
        "min_rss_gib": round(min(rss_values), 6) if rss_values else None,
        "max_rss_gib": round(max(rss_values), 6) if rss_values else None,
        "amplitude_gib": (
            round(max(rss_values) - min(rss_values), 6)
            if rss_values
            else None
        ),
        "required_sample_count": 3,
        "required_span_s": 120.0,
        "method": "same-PID retained RSS min/max/amplitude; no slope computed",
    }
    guard_profile = (
        guard.get("guard_loop_profile")
        if isinstance(guard.get("guard_loop_profile"), dict)
        else {}
    )
    guard_cycle_series = [
        row
        for row in guard_profile.get("cycle_duration_series") or []
        if isinstance(row, dict)
        and row.get("cycle_started_at")
        and _as_float(row.get("cycle_duration_s")) is not None
    ][-12:]
    rolling_cycle_samples = [
        (
            {
                "checked_at": row.get("cycle_started_at"),
                "attribution": {"cycle_duration_s": row.get("cycle_duration_s")},
            },
            _as_float(row.get("cycle_duration_s")),
        )
        for row in guard_cycle_series
    ]
    rolling_cycle_samples = [
        (row, value)
        for row, value in rolling_cycle_samples
        if value is not None
    ]
    retained_cycle_samples = [
        (
            row,
            _as_float(
                _out_of_band_attribution(row.get("attribution")).get(
                    "cycle_duration_s"
                )
            ),
        )
        for row in same_pid_samples
    ]
    retained_cycle_samples = [
        (row, value)
        for row, value in retained_cycle_samples
        if value is not None
    ]
    cycle_samples = rolling_cycle_samples or retained_cycle_samples
    cycle_values = [float(value) for _, value in cycle_samples]
    rolling_cycle_values = [float(value) for _, value in rolling_cycle_samples]
    retained_cycle_values = [float(value) for _, value in retained_cycle_samples]
    in_process_stage_samples = [
        row
        for row in guard_profile.get("stage_timers") or []
        if isinstance(row, dict)
        and _as_float(row.get("rss_gib")) is not None
        and _as_float(row.get("rss_sample_offset_s")) == 0.0
    ]
    in_process_peak_sample = max(
        in_process_stage_samples,
        key=lambda row: _as_float(row.get("rss_gib")) or 0.0,
        default={},
    )
    in_process_peak_gib = _as_float(in_process_peak_sample.get("rss_gib"))
    in_process_peak_stage = in_process_peak_sample.get("name")
    for row in next_samples:
        row_attribution = _out_of_band_attribution(row.get("attribution"))
        row["attribution"] = {
            "out_of_band": {
                **row_attribution,
                "allocation_admissibility": (
                    "INADMISSIBLE_SUPERSEDED_BY_IN_PROCESS_STAGE_BOUNDARIES"
                ),
            }
        }
    freshness_budget_s = _as_float(guard_profile.get("live_build_max_observed_age_s"))
    rolling_max_s = max(rolling_cycle_values) if rolling_cycle_values else None
    retained_max_s = max(retained_cycle_values) if retained_cycle_values else None
    rolling_max_index = (
        rolling_cycle_values.index(rolling_max_s) if rolling_max_s is not None else None
    )
    retained_max_index = (
        retained_cycle_values.index(retained_max_s)
        if retained_max_s is not None
        else None
    )
    rolling_max_sample = (
        rolling_cycle_samples[rolling_max_index][0]
        if rolling_max_index is not None
        else {}
    )
    retained_max_sample = (
        retained_cycle_samples[retained_max_index][0]
        if retained_max_index is not None
        else {}
    )
    if retained_max_s is not None and (
        rolling_max_s is None or retained_max_s > rolling_max_s
    ):
        cycle_duration_max_s = retained_max_s
        cycle_max_sample = retained_max_sample
        peak_samples = retained_cycle_samples
        peak_values = retained_cycle_values
        peak_index = retained_max_index
        peak_basis = "retained"
    else:
        cycle_duration_max_s = rolling_max_s
        cycle_max_sample = rolling_max_sample
        peak_samples = rolling_cycle_samples
        peak_values = rolling_cycle_values
        peak_index = rolling_max_index
        peak_basis = "rolling" if rolling_max_s is not None else None
    cycle_max_at = cycle_max_sample.get("checked_at") if cycle_max_sample else None
    cycle_latest_at = (
        peak_samples[-1][0].get("checked_at") if peak_samples else None
    )
    cycle_max_ts = _parse_ts(cycle_max_at)
    cycle_latest_ts = _parse_ts(cycle_latest_at)
    latest_to_max_elapsed_s = (
        round(max(0.0, (cycle_latest_ts - cycle_max_ts).total_seconds()), 6)
        if cycle_max_ts is not None and cycle_latest_ts is not None
        else None
    )
    post_peak_values = peak_values[peak_index:] if peak_index is not None else []
    peak_is_decaying = bool(
        len(post_peak_values) >= 2
        and all(
            later < earlier
            for earlier, later in zip(post_peak_values, post_peak_values[1:])
        )
    )
    cycle_latest_s = peak_values[-1] if peak_values else None
    cycle_duration_health = {
        "status": (
            "WARN_STALE_CYCLE_PEAK_DECAYING"
            if cycle_duration_max_s is not None
            and freshness_budget_s is not None
            and cycle_duration_max_s > freshness_budget_s
            and peak_is_decaying
            else "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"
            if cycle_duration_max_s is not None
            and freshness_budget_s is not None
            and cycle_duration_max_s > freshness_budget_s
            else "OK"
            if cycle_duration_max_s is not None and freshness_budget_s is not None
            else "UNKNOWN"
        ),
        "peak_basis": peak_basis,
        "min_s": round(min(peak_values), 6) if peak_values else None,
        "max_s": round(cycle_duration_max_s, 6)
        if cycle_duration_max_s is not None
        else None,
        "max_at": cycle_max_at,
        "rolling_max_s": round(rolling_max_s, 6)
        if rolling_max_s is not None
        else None,
        "rolling_max_at": rolling_max_sample.get("checked_at")
        if rolling_max_sample
        else None,
        "rolling_window_cycles": len(rolling_cycle_values),
        "retained_max_s": round(retained_max_s, 6)
        if retained_max_s is not None
        else None,
        "retained_max_at": retained_max_sample.get("checked_at")
        if retained_max_sample
        else None,
        "retained_sample_count": len(retained_cycle_values),
        "latest_s": round(cycle_latest_s, 6)
        if cycle_latest_s is not None
        else None,
        "peak_decaying": peak_is_decaying,
        "latest_under_budget": (
            cycle_latest_s <= freshness_budget_s
            if cycle_latest_s is not None and freshness_budget_s is not None
            else None
        ),
        "latest_to_max_ratio": (
            round(cycle_latest_s / cycle_duration_max_s, 5)
            if cycle_latest_s is not None
            and cycle_duration_max_s is not None
            and cycle_duration_max_s > 0
            else None
        ),
        "max_to_latest_elapsed_s": latest_to_max_elapsed_s,
        "freshness_budget_s": freshness_budget_s,
        "max_to_freshness_ratio": (
            round(cycle_duration_max_s / freshness_budget_s, 6)
            if cycle_duration_max_s is not None
            and freshness_budget_s is not None
            and freshness_budget_s > 0
            else None
        ),
        "sample_count": len(peak_values),
        "rule": (
            "non-blocking measurement warning when the worse of retained "
            "reservoir max and rolling-window max exceeds the live-build "
            "freshness budget"
        ),
    }
    trend_gib = None
    trend_window_s = None
    if previous_same_pid is not None and rss_gib is not None:
        previous_rss = _as_float(previous_same_pid.get("rss_gib"))
        previous_ts = _parse_ts(previous_same_pid.get("checked_at"))
        if previous_rss is not None:
            trend_gib = round(float(rss_gib) - previous_rss, 6)
        if previous_ts is not None:
            trend_window_s = round(max(0.0, (now - previous_ts).total_seconds()), 6)

    auto_restart: dict[str, Any] = {
        "status": "NOT_APPLICABLE",
        "threshold_gib": float(restart_gib),
    }
    retained_peak_gib = (
        _as_float(rss_observation.get("max_rss_gib"))
        if rss_observation.get("status") == "MEASURED"
        else None
    )
    threshold_rss_gib = (
        max(
            value
            for value in (rss_gib, retained_peak_gib, in_process_peak_gib)
            if value is not None
        )
        if any(
            value is not None
            for value in (rss_gib, retained_peak_gib, in_process_peak_gib)
        )
        else None
    )
    status = "UNKNOWN"
    notify_grade = False
    if pid is None:
        status = "UNKNOWN_NO_GUARD_PID"
    elif rss_gib is None:
        status = "UNKNOWN_RSS_UNAVAILABLE"
    elif threshold_rss_gib is not None and threshold_rss_gib >= float(restart_gib):
        auto_restart = _run_memory_restart_actuator(
            root,
            restart_script,
            cooldown_s=float(cooldown_s),
        )
        actuator_status = str(auto_restart.get("status") or "")
        if actuator_status == "RESTART_EXECUTED":
            status = "AUTO_RESTART_EXECUTED"
        elif actuator_status in {"WATCH_COOLDOWN", "ESCALATE_RESTART_STORM"}:
            status = "AUTO_RESTART_SUPPRESSED_NOTIFY"
            notify_grade = True
        else:
            status = "AUTO_RESTART_NOT_EXECUTED"
            notify_grade = True
    elif (
        threshold_rss_gib is not None
        and threshold_rss_gib >= float(warn_gib)
    ):
        status = "WARN"
    else:
        status = "OK"

    return {
        "flow_stage": "LIVE/DEFEND",
        "schema_version": 1,
        "checked_at": now.isoformat(),
        "status": status,
        "pid": pid,
        "rss_kib": rss_kib,
        "rss_gib": rss_gib,
        "rss_peak_gib": rss_peak_gib,
        "rss_peak_at": rss_peak_at,
        "rss_observation": rss_observation,
        "rss_observation_threshold_grade": (
            "AUTHORITATIVE"
            if rss_observation.get("status") == "MEASURED"
            else "EXCLUDED_INSUFFICIENT_OBSERVATION"
        ),
        "threshold_rss_gib": threshold_rss_gib,
        "threshold_rss_source": (
            "max(in_process_stage_boundary_peak_gib, instantaneous_rss_gib, "
            "rss_observation.max_rss_gib)"
        ),
        "in_process_stage_boundary_rss": {
            "status": (
                "AUTHORITATIVE"
                if in_process_peak_gib is not None
                else "UNAVAILABLE"
            ),
            "pid": pid,
            "sample_count": len(in_process_stage_samples),
            "peak_rss_gib": in_process_peak_gib,
            "peak_stage": in_process_peak_stage,
            "sample_offset_s": 0.0,
            "authority": "guard_loop_profile.stage_timers",
        },
        "out_of_band_allocation_attribution": {
            "status": "INADMISSIBLE_SUPERSEDED_BY_IN_PROCESS_STAGE_BOUNDARIES",
            "reason": (
                "deadman ps samples can land after the recorded cycle; retain "
                "them only for external liveness and trend history"
            ),
        },
        "memory_probe": memory_probe,
        "warn_gib": float(warn_gib),
        "restart_gib": float(restart_gib),
        "cooldown_s": float(cooldown_s),
        "samples": next_samples,
        "trend_gib": trend_gib,
        "trend_window_s": trend_window_s,
        "cycle_duration_health": cycle_duration_health,
        "auto_restart": auto_restart,
        "notify_grade": notify_grade,
        "rule": (
            f"warn at retained guard RSS peak >={float(warn_gib):g} GiB; "
            f"at >={float(restart_gib):g} GiB run canonical brainless live-guard restart "
            f"with {float(cooldown_s):g}s cooldown"
        ),
    }


def _main(argv: list[str] | None = None) -> int:
    cycle_started_monotonic = time.monotonic()
    stage_checkpoint_monotonic = cycle_started_monotonic
    stage_timing_rows: list[dict[str, Any]] = []

    def _stage_checkpoint(stage: str) -> None:
        nonlocal stage_checkpoint_monotonic
        checkpoint = time.monotonic()
        stage_timing_rows.append({"stage": stage, "duration_s": round(checkpoint - stage_checkpoint_monotonic, 6)})
        stage_checkpoint_monotonic = checkpoint
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--guard", default=DEFAULT_GUARD)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--guard-event-log", default=DEFAULT_GUARD_EVENT_LOG)
    parser.add_argument("--active-set-state", default=DEFAULT_ACTIVE_SET_STATE)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument(
        "--admission-interval-log",
        default=DEFAULT_ADMISSION_INTERVAL_LOG,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--deadman-cycle-log",
        default=DEFAULT_DEADMAN_CYCLE_LOG,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--lock", default=DEFAULT_LOCK, help=argparse.SUPPRESS)
    parser.add_argument("--handoff", default=DEFAULT_HANDOFF)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL)
    parser.add_argument("--hot-history", default=DEFAULT_HOT_HISTORY)
    parser.add_argument("--copy-intents-state", default=DEFAULT_COPY_INTENTS_STATE)
    parser.add_argument(
        "--qualified-pool-stakeout",
        default=DEFAULT_QUALIFIED_POOL_STAKEOUT,
    )
    parser.add_argument("--routing-shadow", default=DEFAULT_ROUTING_SHADOW)
    parser.add_argument("--ready-shadow", default=DEFAULT_READY_SHADOW)
    parser.add_argument("--cohort-admission", default=DEFAULT_COHORT_ADMISSION)
    parser.add_argument("--full-pool-queue", default=DEFAULT_FULL_POOL_QUEUE)
    parser.add_argument("--wide-direct-state", default=DEFAULT_WIDE_DIRECT_STATE)
    parser.add_argument("--rtds-capture", default=DEFAULT_RTDS_CAPTURE)
    parser.add_argument("--rtds-liveness-state", default=DEFAULT_RTDS_LIVENESS_STATE)
    parser.add_argument(
        "--wide-fingerprint-evidence",
        default=DEFAULT_WIDE_FINGERPRINT_EVIDENCE,
    )
    parser.add_argument("--bac25-forward-lane", default=DEFAULT_BAC25_FORWARD_LANE)
    parser.add_argument(
        "--bac25-forward-manifest",
        default=DEFAULT_BAC25_FORWARD_MANIFEST,
    )
    parser.add_argument(
        "--bac25-forward-evidence",
        default=DEFAULT_BAC25_FORWARD_EVIDENCE,
    )
    parser.add_argument(
        "--freeze-allpass-sidecar",
        default=DEFAULT_FREEZE_ALLPASS_SIDECAR,
    )
    parser.add_argument("--wide-direct-journal", default=DEFAULT_WIDE_DIRECT_JOURNAL)
    parser.add_argument("--wide-terminal-log", default=DEFAULT_WIDE_TERMINAL_LOG)
    parser.add_argument("--wide-frontier", default=DEFAULT_WIDE_FRONTIER)
    parser.add_argument("--recovery-token-state", default=DEFAULT_RECOVERY_TOKEN_STATE)
    parser.add_argument("--recovery-token-prereg", default=DEFAULT_RECOVERY_TOKEN_PREREG)
    parser.add_argument("--recovery-alpha-state", default=DEFAULT_RECOVERY_ALPHA_STATE)
    parser.add_argument("--recovery-alpha-prereg", default=DEFAULT_RECOVERY_ALPHA_PREREG)
    parser.add_argument("--recovery-decision", default=DEFAULT_RECOVERY_DECISION)
    parser.add_argument(
        "--recovery-passive-residual-state",
        default=DEFAULT_RECOVERY_PASSIVE_RESIDUAL_STATE,
    )
    parser.add_argument(
        "--recovery-maker-first-residual-state",
        default=DEFAULT_RECOVERY_MAKER_FIRST_RESIDUAL_STATE,
    )
    parser.add_argument("--standby-readiness", default=DEFAULT_STANDBY_READINESS)
    parser.add_argument(
        "--e5-live-actuator-state",
        default="data/research/e5_maker_first_live_actuator_latest.json",
    )
    parser.add_argument("--max-idle-s", type=int, default=1800)
    parser.add_argument("--realert-s", type=int, default=1800)
    parser.add_argument("--eligible-drought-alert-s", type=int, default=7200)
    parser.add_argument("--guard-memory-warn-gib", type=float, default=GUARD_MEMORY_WARN_GIB)
    parser.add_argument("--guard-memory-restart-gib", type=float, default=GUARD_MEMORY_RESTART_GIB)
    parser.add_argument(
        "--guard-memory-restart-cooldown-s",
        type=float,
        default=GUARD_MEMORY_AUTO_RESTART_COOLDOWN_S,
    )
    parser.add_argument("--guard-memory-restart-script", default=DEFAULT_GUARD_MEMORY_RESTART_SCRIPT)
    parser.add_argument(
        "--force-policy-choke-sweep",
        action="store_true",
        help=(
            "Force the existing Rung B/C admissible-target actuator after an "
            "explicit incident ruling, even when a recent pre-restart accept "
            "still keeps the rolling deadman green."
        ),
    )
    parser.add_argument("--now", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()

    now = _parse_ts(args.now) if args.now else _utc_now()
    if now is None:
        now = _utc_now()
    regime = "weekday" if now.weekday() < 5 else "weekend"
    result = {
        "kind": "order_flow_deadman",
        "checked_at": now.isoformat(),
        "max_idle_s": args.max_idle_s,
        "status": "OK",
    }
    state_path = root / args.state
    try:
        prev = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError):
        prev = {}

    try:
        guard = json.loads((root / args.guard).read_text())
    except (OSError, json.JSONDecodeError):
        guard = {}
    guard_code_identity = (
        guard.get("guard_code_identity")
        if isinstance(guard.get("guard_code_identity"), dict)
        else {}
    )
    result["guard_loaded_generation_started_at"] = guard_code_identity.get(
        "started_at_utc"
    )
    try:
        ledger = json.loads((root / args.ledger).read_text())
    except (OSError, json.JSONDecodeError):
        ledger = {}
    try:
        active_set_state = json.loads((root / args.active_set_state).read_text())
    except (OSError, json.JSONDecodeError):
        active_set_state = {}
    def _load_optional_json(relative_path: str) -> dict:
        try:
            value = json.loads((root / relative_path).read_text())
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    temporal = _load_optional_json(args.temporal)
    hot_history = _load_optional_json(args.hot_history)
    copy_intent_state = _load_optional_json(args.copy_intents_state)
    qualified_pool_stakeout = _load_optional_json(args.qualified_pool_stakeout)
    hot_history = _union_qualified_pool_stakeout(
        hot_history,
        qualified_pool_stakeout,
    )
    freeze_allpass_sidecar = _load_optional_json(args.freeze_allpass_sidecar)
    previous_policy_choke = (
        prev.get("policy_choke") if isinstance(prev.get("policy_choke"), dict) else {}
    )
    previous_policy_choke_actuator = (
        previous_policy_choke.get("actuator")
        if isinstance(previous_policy_choke.get("actuator"), dict)
        else {}
    )
    previous_direct_admission_authority = (
        previous_policy_choke_actuator.get("admission_authority")
        if isinstance(previous_policy_choke_actuator.get("admission_authority"), dict)
        else {}
    )
    previous_candidate_evidence = (
        previous_policy_choke_actuator.get("candidate_evidence")
        if isinstance(previous_policy_choke_actuator.get("candidate_evidence"), dict)
        else {}
    )
    early_direct_candidate = (
        previous_candidate_evidence.get("selected")
        if isinstance(previous_candidate_evidence.get("selected"), dict)
        else freeze_allpass_sidecar.get("primary")
        if isinstance(freeze_allpass_sidecar.get("primary"), dict)
        else None
    )
    early_direct_admission_authority = _direct_pin_admission_authority(
        candidate=early_direct_candidate,
        ledger=ledger,
        hot_history=hot_history,
        qualified_pool_stakeout=qualified_pool_stakeout,
        previous_admission_authority=previous_direct_admission_authority,
        now=now,
    )
    early_direct_admission_authority = _publish_admission_authority(
        state_path,
        authority=early_direct_admission_authority,
        checked_at=now,
        stage="early",
        cycle_elapsed_s=time.monotonic() - cycle_started_monotonic,
    )
    result["early_admission_authority"] = early_direct_admission_authority
    _stage_checkpoint("boot_load_and_early_publish")
    admission_tick_rows: list[dict[str, Any]] = []

    def _run_admission_tick(*, candidate: dict[str, Any] | None, stage: str) -> dict[str, Any]:
        tick_now = _utc_now()
        tick_pool = _load_optional_json(args.qualified_pool_stakeout)
        tick_hot = _union_qualified_pool_stakeout(_load_optional_json(args.hot_history), tick_pool)
        try:
            tick_state = json.loads(state_path.read_text())
        except (OSError, json.JSONDecodeError):
            tick_state = {}
        tick_previous = (((tick_state.get("policy_choke") or {}).get("actuator") or {}).get("admission_authority")) if isinstance(tick_state, dict) else {}
        authority = _direct_pin_admission_authority(
            candidate=candidate, ledger=ledger, hot_history=tick_hot,
            qualified_pool_stakeout=tick_pool,
            previous_admission_authority=tick_previous if isinstance(tick_previous, dict) else {},
            now=tick_now,
        )
        authority = _publish_admission_authority(
            state_path,
            authority=authority,
            checked_at=tick_now,
            stage=stage,
            cycle_elapsed_s=time.monotonic() - cycle_started_monotonic,
        )
        admission_tick_rows.append({
            "stage": stage, "checked_at": authority.get("checked_at"),
            "published_at": authority.get("published_at"), "publish_lag_s": authority.get("publish_lag_s"),
            "status": authority.get("status"),
        })
        return authority

    def _current_direct_pin_admission_authority(
        *, candidate: dict[str, Any] | None
    ) -> dict[str, Any]:
        if (
            isinstance(candidate, dict)
            and isinstance(early_direct_candidate, dict)
            and _normalize_wallet(candidate.get("wallet"))
            == _normalize_wallet(early_direct_candidate.get("wallet"))
            and str(candidate.get("wide_policy_fingerprint") or "")
            == str(early_direct_candidate.get("wide_policy_fingerprint") or "")
        ):
            return early_direct_admission_authority
        return _direct_pin_admission_authority(
            candidate=candidate,
            ledger=ledger,
            hot_history=hot_history,
            qualified_pool_stakeout=qualified_pool_stakeout,
            previous_admission_authority=previous_direct_admission_authority,
            now=now,
        )
    routing_shadow = _load_optional_json(args.routing_shadow)
    ready_shadow = _load_optional_json(args.ready_shadow)
    cohort_admission = _load_optional_json(args.cohort_admission)
    full_pool_queue = _load_optional_json(args.full_pool_queue)
    wide_direct_state = _load_optional_json(args.wide_direct_state)
    wide_fingerprint_evidence = _load_optional_json(args.wide_fingerprint_evidence)
    bac25_forward_lane = _load_optional_json(
        getattr(args, "bac25_forward_lane", DEFAULT_BAC25_FORWARD_LANE)
    )
    bac25_forward_manifest = _load_optional_json(
        getattr(args, "bac25_forward_manifest", DEFAULT_BAC25_FORWARD_MANIFEST)
    )
    bac25_forward_evidence = _load_optional_json(
        getattr(args, "bac25_forward_evidence", DEFAULT_BAC25_FORWARD_EVIDENCE)
    )
    wide_direct_state, wide_fingerprint_evidence = _admit_forward_only_supply(
        direct_source=wide_direct_state,
        fingerprint_evidence=wide_fingerprint_evidence,
        lane=bac25_forward_lane,
        manifest=bac25_forward_manifest,
        forward_evidence=bac25_forward_evidence,
    )
    recovery_states = [_load_optional_json(args.recovery_token_state), _load_optional_json(args.recovery_alpha_state)]
    recovery_preregs = [_load_optional_json(args.recovery_token_prereg), _load_optional_json(args.recovery_alpha_prereg)]
    recovery_existing_decision = _load_optional_json(args.recovery_decision)
    recovery_successor_states = [
        _load_optional_json(args.recovery_passive_residual_state),
        _load_optional_json(args.recovery_maker_first_residual_state),
    ]
    standby_readiness = _load_optional_json(args.standby_readiness)
    e5_live_actuator = _load_optional_json(args.e5_live_actuator_state)
    _run_admission_tick(candidate=early_direct_candidate, stage="inputs_loaded")
    _stage_checkpoint("remaining_inputs_and_tick")
    candidate_heartbeat_last_monotonic = time.monotonic()

    def _candidate_selection_heartbeat() -> None:
        nonlocal candidate_heartbeat_last_monotonic
        heartbeat_now = time.monotonic()
        if not _heartbeat_due(candidate_heartbeat_last_monotonic, heartbeat_now):
            return
        _run_admission_tick(
            candidate=early_direct_candidate,
            stage="candidate_selection_60s_heartbeat",
        )
        candidate_heartbeat_last_monotonic = heartbeat_now

    guard_summary_present = isinstance(guard.get("summary"), dict)
    ledger_summary_present = isinstance(ledger.get("summary"), dict)
    guard_summary = guard.get("summary") if guard_summary_present else {}
    ledger_summary = ledger.get("summary") if ledger_summary_present else {}
    guard_summary_can_trade = guard_summary.get("can_trade")
    ledger_summary_can_trade = ledger_summary.get("can_trade")
    live_blockers = guard.get("blockers") if isinstance(guard.get("blockers"), list) else []
    guard_fallback_can_trade = bool(
        guard.get("execute_live") is True
        and guard.get("live_orders_allowed") is True
        and not live_blockers
    )
    can_trade = bool(guard_summary_can_trade or ledger_summary_can_trade)
    if not can_trade:
        can_trade = guard_fallback_can_trade
    if guard_summary_can_trade is True:
        can_trade_reason = "guard_summary_true"
    elif ledger_summary_can_trade is True:
        can_trade_reason = "ledger_summary_true"
    elif guard_fallback_can_trade:
        can_trade_reason = "guard_top_level_live_no_blockers"
    elif live_blockers:
        can_trade_reason = "guard_blockers_present"
    elif not guard_summary_present:
        can_trade_reason = "guard_summary_null_and_ledger_not_true"
    elif ledger_summary_can_trade is False:
        can_trade_reason = "ledger_summary_false"
    elif not ledger_summary_present:
        can_trade_reason = "ledger_summary_null"
    else:
        can_trade_reason = "no_live_permission_source_true"
    latest_accepted_order = _latest_accepted_order(ledger)
    latest_order = latest_accepted_order["timestamp"]
    _candidate_selection_heartbeat()
    reconciled_overlay, rung_b_lifecycle, rung_b_cooloffs = _reconcile_policy_choke_rung_b(
        overlay=active_set_state,
        ledger=ledger,
        previous_state=prev if isinstance(prev, dict) else {},
        now=now,
        hot_history=hot_history,
    )
    if reconciled_overlay != active_set_state:
        atomic_write_json(root / args.active_set_state, reconciled_overlay)
        active_set_state = reconciled_overlay
    result["policy_choke_rung_b_lifecycle"] = rung_b_lifecycle
    result["policy_choke_rung_b_cooloffs"] = rung_b_cooloffs
    _candidate_selection_heartbeat()
    guard_memory = _guard_memory_snapshot(
        root,
        guard,
        prev if isinstance(prev, dict) else {},
        now,
        warn_gib=float(args.guard_memory_warn_gib),
        restart_gib=float(args.guard_memory_restart_gib),
        cooldown_s=float(args.guard_memory_restart_cooldown_s),
        restart_script=str(args.guard_memory_restart_script),
    )
    result["guard_memory"] = guard_memory
    _candidate_selection_heartbeat()
    previous_last_fresh_actionable_ts = _parse_ts(prev.get("last_fresh_actionable_ts"))
    previous_halt = (
        prev.get("guard_side_halt_signal")
        if isinstance(prev.get("guard_side_halt_signal"), dict)
        else {}
    )
    if previous_last_fresh_actionable_ts is not None and previous_halt:
        selected_wallet = _selected_runtime_wallet(guard)
        previous_selected_rows = previous_halt.get("selected_fresh_actionable_signal_rows")
        try:
            previous_fresh_actionable_rows = int(
                previous_selected_rows
                if previous_selected_rows is not None
                else 0
                if selected_wallet
                else previous_halt.get("fresh_actionable_signal_rows") or 0
            )
        except (TypeError, ValueError):
            previous_fresh_actionable_rows = 0
        if previous_fresh_actionable_rows <= 0:
            previous_last_fresh_actionable_ts = None
    guard_events = _merge_liveness_scans(
        [
            _scan_liveness_events(root / args.event_log),
            _scan_liveness_events(root / args.guard_event_log),
        ]
    )
    _candidate_selection_heartbeat()
    policy_choke_events = _merge_liveness_scans(
        [
            _scan_liveness_events(root / args.event_log, since=now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S)),
            _scan_liveness_events(root / args.guard_event_log, since=now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S)),
        ]
    )
    _candidate_selection_heartbeat()
    host_boot_time = _host_boot_time()
    latest_post_boot_suppression = guard_events.get("latest_approved_suppression_ts")
    accepted_liveness_predates_boot = bool(
        latest_order and host_boot_time and latest_order < host_boot_time
    )
    post_boot_recovery_liveness = (
        latest_post_boot_suppression
        if accepted_liveness_predates_boot
        and latest_post_boot_suppression is not None
        and latest_post_boot_suppression >= host_boot_time
        else host_boot_time
        if accepted_liveness_predates_boot
        else None
    )
    post_boot_recovery_idle_s = (
        (now - post_boot_recovery_liveness).total_seconds()
        if post_boot_recovery_liveness is not None
        else None
    )
    post_boot_recovery_grace = bool(
        accepted_liveness_predates_boot
        and latest_post_boot_suppression is not None
        and host_boot_time is not None
        and latest_post_boot_suppression >= host_boot_time
        and post_boot_recovery_idle_s is not None
        and post_boot_recovery_idle_s <= args.max_idle_s
    )
    post_boot_accepted_order_pending_since = host_boot_time if accepted_liveness_predates_boot else None
    post_boot_acceptance_path_uncertified = bool(
        post_boot_accepted_order_pending_since is not None
        and (now - post_boot_accepted_order_pending_since).total_seconds() > POST_BOOT_ACCEPTANCE_PATH_UNCERTIFIED_S
    )
    selected_runtime_wallet = _selected_runtime_wallet(guard)
    accepted_order_idle_s = (
        (now - latest_order).total_seconds() if latest_order is not None else None
    )
    acceptance_30m = acceptance_share_rows(
        guard=guard,
        temporal=temporal,
        hot_history=hot_history,
        routing_shadow=routing_shadow,
        ledger=ledger,
        now_s=now.timestamp(),
        lookback_s=POLICY_CHOKE_LOOKBACK_S,
        regime=regime,
    )
    rung_a = policy_choke_rung_a(
        acceptance_30m,
        selected_runtime_wallet,
        regime=regime,
    )
    effective_execution_lanes = _effective_execution_lanes(e5_live_actuator, now=now)
    policy_choke_submit_outcomes = _policy_choke_submit_outcomes(
        ledger,
        since=now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S),
        until=now,
        effective_lanes=effective_execution_lanes,
    )
    policy_choke = _policy_choke_incident(
        scan=_policy_choke_scan_from_acceptance(
            acceptance_30m,
            policy_choke_events,
            copy_intent_state,
            now_s=now.timestamp(),
            guard=guard,
        ),
        selected_wallet=selected_runtime_wallet,
        can_trade=can_trade,
        rung_a=rung_a,
        submit_outcomes=policy_choke_submit_outcomes,
        method_acceptance=_accepted_method_orders(
            ledger,
            since=now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S),
            until=now,
            effective_lanes=effective_execution_lanes,
        ),
    )
    _candidate_selection_heartbeat()
    policy_choke_reconciliation = _policy_choke_terminal_reconciliation(
        guard=guard,
        hot_history=hot_history,
        routing_shadow=routing_shadow,
        ledger=ledger,
        now=now,
    )
    policy_choke["terminal_reconciliation"] = policy_choke_reconciliation
    _candidate_selection_heartbeat()
    _apply_policy_choke_feedstock_gate(
        policy_choke,
        policy_choke_reconciliation,
    )
    _enforce_unattributed_selected_seat_attrition(policy_choke)
    policy_choke["local_skip_classification"] = (
        _classify_policy_choke_local_skip(
            policy_choke=policy_choke,
            reconciliation=policy_choke_reconciliation,
            can_trade=can_trade,
            guard_status=str(guard.get("status") or ""),
        )
    )
    if policy_choke["local_skip_classification"].get("qualifies"):
        policy_choke["mechanical_escalation_before_local_skip"] = policy_choke.get(
            "mechanical_escalation"
        )
        policy_choke["mechanical_escalation"] = policy_choke[
            "local_skip_classification"
        ].get("mechanical_escalation", "NONE")
        policy_choke["local_skip_escalation_rule"] = (
            "ORDER137-B: maker-minimum-only refusals name the local sizing choke"
            if policy_choke["local_skip_classification"].get("sizing_only_rejects")
            else "measured local pre-submit policy skips never actuate sizing, supply admission, or reload"
        )
    policy_choke["source_coverage_full"] = bool(
        policy_choke_reconciliation.get("source_coverage_full")
    )
    policy_choke["input_equals_terminal_rows"] = bool(
        policy_choke_reconciliation.get("input_equals_terminal_rows")
    )
    journal_path = root / args.wide_direct_journal
    recovered = envelopes_from_incidents(
        load_incident_rows(root / INCIDENT_EVIDENCE_JSONL),
        load_jsonl(root / args.wide_terminal_log),
    )
    _candidate_selection_heartbeat()
    wide_direct_state, wide_fingerprint_evidence, direct_snapshot_reread = (
        _reread_direct_snapshot_packet(
            wide_direct_state,
            load_packet=lambda: json.loads((root / args.wide_direct_state).read_text()),
            fingerprint_evidence=wide_fingerprint_evidence,
            lane=bac25_forward_lane,
            manifest=bac25_forward_manifest,
            forward_evidence=bac25_forward_evidence,
        )
    )
    direct_snapshot_now = _utc_now()
    current_envelope = envelope_from_packet(wide_direct_state)
    append_envelopes(journal_path, [*recovered, *([current_envelope] if current_envelope else [])])
    wide_direct_journal = load_jsonl(journal_path)
    _candidate_selection_heartbeat()
    direct_source = _wide_direct_source_snapshot(
        wide_direct_state,
        now=direct_snapshot_now,
        journal=wide_direct_journal,
    )
    direct_source["direct_snapshot_reread"] = direct_snapshot_reread
    direct_source["direct_snapshot_now"] = direct_snapshot_now.isoformat()
    result["wide_generation_churn"] = _wide_generation_churn(
        wide_direct_journal
    )
    rtds_liveness = _update_rtds_observed_liveness(
        root / args.rtds_capture,
        root / args.rtds_liveness_state,
        now=now,
    )
    _candidate_selection_heartbeat()
    total_loss_auto_disable = (
        (guard.get("active_set_runtime") or {}).get("total_loss_auto_disable")
        if isinstance(guard.get("active_set_runtime"), dict)
        else {}
    )
    source_drought_candidates = _select_source_drought_candidate(
        ready_shadow=ready_shadow,
        cohort_admission=cohort_admission,
        full_pool_queue=full_pool_queue,
        overlay=active_set_state,
        hot_history=hot_history,
        direct_source=direct_source,
        fingerprint_evidence=wide_fingerprint_evidence,
        exact_policy_holdouts=(
            ((qualified_pool_stakeout.get("prospective_current_market") or {}).get(
                "actuator_consumption_gate"
            ) or {}).get("exact_policy_chronological_holdout_by_wallet")
            if isinstance(qualified_pool_stakeout, dict)
            else {}
        ),
        manifest_identity_fallback=_direct_manifest_identity_fallback(
            root=root,
            direct_source=direct_source,
        ),
        standby_readiness=standby_readiness,
        now=now,
        regime=regime,
        cooloffs=rung_b_cooloffs,
        temporal_registry=temporal,
        rtds_liveness=rtds_liveness,
        total_loss_auto_disable=total_loss_auto_disable or {},
        heartbeat_callback=_candidate_selection_heartbeat,
    )
    selected_tick_candidate = source_drought_candidates.get("selected") if isinstance(source_drought_candidates.get("selected"), dict) else early_direct_candidate
    _run_admission_tick(candidate=selected_tick_candidate, stage="candidate_selected")
    _stage_checkpoint("candidate_derivation_and_tick")
    previous_candidate_evidence = (
        (
            (
                prev.get("policy_choke")
                if isinstance(prev.get("policy_choke"), dict)
                else {}
            ).get("source_roster_drought")
            or {}
        ).get("candidate_evidence")
        or {}
    )
    source_drought_candidates["supply_dropouts"] = _candidate_supply_dropouts(
        previous=(
            previous_candidate_evidence
            if isinstance(previous_candidate_evidence, dict)
            else {}
        ),
        current=source_drought_candidates,
        ready_shadow=ready_shadow,
        cohort_admission=cohort_admission,
        full_pool_queue=full_pool_queue,
        direct_source=direct_source,
        overlay=active_set_state,
    )
    active_set_state, direct_temporal_disable = (
        _disable_temporally_ineligible_direct_pin(
            overlay=active_set_state,
            candidate_evidence=source_drought_candidates,
            now=now,
        )
    )
    policy_choke["direct_temporal_disable"] = direct_temporal_disable
    if (
        direct_temporal_disable.get("status")
        == "DIRECT_PIN_DISABLED_ACTIVE_TEMPORAL_PROVEN_NEGATIVE"
    ):
        atomic_write_json(root / args.active_set_state, active_set_state)
    weekend_rotation = (
        guard.get("active_set_weekend_seat_loss_rotation")
        if isinstance(guard.get("active_set_weekend_seat_loss_rotation"), dict)
        else {}
    )
    operator_live_authority = _operator_live_authority(guard)
    result["operator_live_authority"] = operator_live_authority
    money_and_tripwires_clear = _money_and_tripwires_clear(
        operator_live_authority=operator_live_authority,
        weekend_rotation=weekend_rotation,
        total_loss_auto_disable=total_loss_auto_disable or {},
    )
    source_drought_candidates, direct_selection_eligible = (
        _enforce_freeze_only_direct_authority(
            candidates=source_drought_candidates,
            freeze_allpass_sidecar=freeze_allpass_sidecar,
            previous_candidates=(
                previous_candidate_evidence
                if isinstance(previous_candidate_evidence, dict)
                else {}
            ),
            active_set_overlay=active_set_state,
            now=now,
            money_and_tripwires_clear=money_and_tripwires_clear,
        )
    )
    renewed_active_set_state, direct_pin_renewal = (
        _renew_direct_pin_from_incumbent_allpass(
            overlay=active_set_state,
            candidate_evidence=source_drought_candidates,
            now=now,
            guard=guard,
            ledger=ledger,
        )
    )
    result["direct_pin_renewal"] = direct_pin_renewal
    if renewed_active_set_state != active_set_state:
        atomic_write_json(root / args.active_set_state, renewed_active_set_state)
        active_set_state = renewed_active_set_state
    policy_choke["rung_a_candidate_reconciliation"] = (
        _rung_a_candidate_reconciliation(
            rung_a=rung_a,
            candidates=source_drought_candidates,
            terminal_reconciliation=policy_choke_reconciliation,
        )
    )
    atomic_write_json(
        root / args.wide_frontier,
        {
            "schema_version": 1,
            "kind": "wide_direct_admissible_frontier",
            "generated_at": now.isoformat(),
            "flow_stage": "LIVE/ROTATE/PROMOTE/LEARN",
            "source_checksum": direct_source.get("checksum"),
            "frontier_checksum": source_drought_candidates.get("frontier_checksum"),
            "frontier_key": source_drought_candidates.get("frontier_key"),
            "candidate_count": source_drought_candidates.get("candidate_count"),
            "eligible_count": source_drought_candidates.get("eligible_count"),
            "selected": source_drought_candidates.get("selected"),
            "nearest_frontier": source_drought_candidates.get("nearest_frontier") or [],
            "refusal_counts": source_drought_candidates.get("refusal_counts") or {},
            "supply_dropouts": source_drought_candidates.get("supply_dropouts") or [],
            "quality_bars": source_drought_candidates.get("gate_digits") or {},
            "evidence_source": "immutable historical paper/replay plus exact generation direct handoff",
            "gap_closing_lane": {
                "status": "ACTIVE_PAPER_ONLY",
                "lane": "wide_exact_policy_paper",
                "policy_id": wide_direct_state.get("policy_id"),
                "source_state": args.wide_direct_state,
                "covers_wallets": _gap_closing_coverage(source_drought_candidates),
                "sticky_focus_identities": [
                    {"wallet": wallet, "wide_policy_fingerprint": fingerprint}
                    for wallet, fingerprint in STICKY_PAPER_ACCRUAL_FOCUS
                ],
                "purpose": "prospective exact-policy weekend F1 resolution accrual at unchanged >=200 gate",
                "live_authority": False,
            },
        },
    )
    source_roster_drought = _source_roster_drought_incident(
        can_trade=can_trade,
        accepted_order_idle_s=accepted_order_idle_s,
        runtime_fresh_rows=int(policy_choke.get("whole_runtime_fresh_source_rows") or 0),
        direct_source=direct_source,
        candidates=source_drought_candidates,
        max_idle_s=float(args.max_idle_s),
        live_build_authorized=guard.get("execute_live") is True,
    )
    recovery_ttl = _recovery_ttl_candidate(
        states=recovery_states,
        preregs=recovery_preregs,
        now=now,
        existing_decision=recovery_existing_decision,
    )
    policy_choke["recovery_ttl"] = recovery_ttl
    rung_c_label, rung_c_settlement = _rung_c_escalation_label(
        recovery_ttl=recovery_ttl,
        successor_states=recovery_successor_states,
    )
    policy_choke["rung_c_settlement"] = rung_c_settlement
    if recovery_ttl.get("status") == "TTL_NO_ALL_PASS_METHOD_SWITCH":
        atomic_write_json(root / args.recovery_decision, recovery_ttl)
    policy_choke["source_roster_drought"] = source_roster_drought
    if source_roster_drought["firing"]:
        if source_roster_drought.get("mechanical_escalation") == "RUNG_C_METHOD_SWITCH_DUE":
            source_roster_drought["mechanical_escalation"] = rung_c_label
            source_roster_drought["rung_c_settlement"] = rung_c_settlement
        policy_choke["status_before_source_roster_drought"] = policy_choke["status"]
        policy_choke["firing_before_source_roster_drought"] = policy_choke["firing"]
        policy_choke["status"] = "INCIDENT_SOURCE_ROSTER_DROUGHT"
        policy_choke["firing"] = True
        policy_choke["mechanical_escalation"] = source_roster_drought["mechanical_escalation"]
    if args.force_policy_choke_sweep:
        policy_choke["status_before_forced_sweep"] = policy_choke["status"]
        policy_choke["firing_before_forced_sweep"] = policy_choke["firing"]
        policy_choke["status"] = "INCIDENT_POLICY_CHOKE"
        policy_choke["firing"] = True
        policy_choke["mechanical_escalation"] = "RUNG_B_EMERGENCY_ADMISSION_DUE"
        policy_choke["forced_sweep"] = {
            "enabled": True,
            "reason": "explicit post-restart eligible-window no-accept incident branch",
            "quality_bars_unchanged": True,
        }
        if not source_drought_candidates.get("selected"):
            forced_candidate = _forced_measured_positive_seat_candidate(
                source_drought_candidates
            )
            if forced_candidate is not None:
                source_drought_candidates["selected"] = forced_candidate
                source_drought_candidates["status"] = (
                    "FORCED_MEASURED_POSITIVE_SEAT_SELECTED"
                )
                source_drought_candidates["selection_authority"] = (
                    "operator_2026-08-01_absolute_seat_fill_preemption"
                )
                source_drought_candidates["forced_measured_positive_inventory_count"] = sum(
                    1
                    for row in source_drought_candidates.get("rows") or []
                    if _forced_measured_positive_seat_candidate(
                        {"rows": [row]}
                    )
                    is not None
                )
    if post_boot_recovery_grace and policy_choke["firing"]:
        policy_choke["raw_status"] = policy_choke["status"]
        policy_choke["raw_firing"] = True
        policy_choke["status"] = "POST_BOOT_RECOVERY_LIVENESS_ADVANCED"
        policy_choke["firing"] = False
        policy_choke["mechanical_escalation"] = "NONE_POST_BOOT_RECOVERY_GRACE"
        policy_choke["deferred_until"] = (
            post_boot_recovery_liveness + timedelta(seconds=args.max_idle_s)
        ).isoformat()
        policy_choke["defer_rule"] = (
            "pre-boot accepted-order liveness cannot fire the policy-choke actuator "
            "until 30 minutes elapse without a post-boot accepted order or approved suppression"
        )
    active_rung_b_pin = active_set_state.get("selection_pin") if isinstance(active_set_state.get("selection_pin"), dict) else {}
    active_rung_b_pin_expiry = _parse_ts(active_rung_b_pin.get("expires_at"))
    active_direct_pin = bool(
        active_rung_b_pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID
        and active_rung_b_pin.get("enabled") is not False
        and active_rung_b_pin_expiry is not None
        and active_rung_b_pin_expiry > now
    )
    clear_non_rung_b_consecutive = 0
    if active_rung_b_pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID:
        rung_b_wallet = _normalize_wallet(active_rung_b_pin.get("source_wallet"))
        since = now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S)
        non_rung_accepted = sum(
            _accepted_orders_for_wallet(ledger, wallet, since=since, until=now)
            for wallet in {
                _normalize_wallet(row.get("source_wallet") or row.get("wallet"))
                for row in active_set_state.get("members") or [] if isinstance(row, dict)
            }
            if wallet and wallet != rung_b_wallet
        )
        if not policy_choke["firing"] and non_rung_accepted > 0:
            clear_non_rung_b_consecutive = int(prev.get("policy_choke_rung_b_clear_non_rung_consecutive") or 0) + 1
    result["policy_choke_rung_b_clear_non_rung_consecutive"] = clear_non_rung_b_consecutive
    policy_choke_actuator = {"status": "NOT_APPLICABLE"}
    freeze_pin_replaced = False
    selected_freeze_direct = source_drought_candidates.get("selected")
    reconciliation_gate_pass = bool(
        policy_choke["source_coverage_full"]
        and policy_choke["input_equals_terminal_rows"]
    )
    if policy_choke["firing"] and reconciliation_gate_pass and active_direct_pin:
        policy_choke_actuator = {
            "status": "DIRECT_SOURCE_SELECTION_ALREADY_ACTIVE",
            "selection_pin": active_rung_b_pin,
            "conversion_only": True,
            "selected_live_feedstock_rows": policy_choke.get(
                "selected_live_feedstock_rows"
            ),
            "selected_eligible_intents_pre_feedstock_gate": policy_choke.get(
                "selected_eligible_intents_pre_feedstock_gate"
            ),
            "selected_eligible_intents": policy_choke.get(
                "selected_eligible_intents"
            ),
            "terminal_stage_counts": policy_choke_reconciliation.get(
                "terminal_stage_counts", {}
            ),
            "rule": (
                "an unexpired exact DIRECT pin is conversion-only: diagnose and "
                "feed OrderFilled/WS rows to the sole guard; never query legacy "
                "ready-shadow/RUNG_C supply or write a second pin"
            ),
        }
    elif (
        direct_selection_eligible
        and isinstance(selected_freeze_direct, dict)
        and active_direct_pin
        and _normalize_wallet(active_rung_b_pin.get("source_wallet"))
        != _normalize_wallet(selected_freeze_direct.get("wallet"))
    ):
        stale_candidate_id = str(active_rung_b_pin.get("candidate_id") or "")
        normal_gate_replacement = (
            source_drought_candidates.get("selection_authority")
            == "normal_gate_unique_all_pass"
        )
        replacement_overlay = {
            **active_set_state,
            "selection_pin": {
                **active_rung_b_pin,
                "enabled": False,
                "expired_reason": (
                    "superseded_by_normal_gate_unique_allpass"
                    if normal_gate_replacement
                    else "superseded_by_freeze_allpass_exact_identity"
                ),
                "expired_at": now.isoformat(),
            },
            "members": [
                {
                    **row,
                    "enabled": False,
                    "status": (
                        "SUPERSEDED_BY_NORMAL_GATE_UNIQUE_ALLPASS"
                        if normal_gate_replacement
                        else "SUPERSEDED_BY_FREEZE_ALLPASS_EXACT_IDENTITY"
                    ),
                }
                if isinstance(row, dict)
                and str(row.get("candidate_id") or "") == stale_candidate_id
                else row
                for row in active_set_state.get("members") or []
            ],
            "updated_at": now.isoformat(),
        }
        replacement_overlay, policy_choke_actuator = _execute_policy_choke_rung_b(
            overlay=replacement_overlay,
            candidate=selected_freeze_direct,
            now=now,
            supply_rung="DIRECT",
            admission_authority=_current_direct_pin_admission_authority(
                candidate=selected_freeze_direct,
            ),
        )
        atomic_write_json(root / args.active_set_state, replacement_overlay)
        active_set_state = replacement_overlay
        freeze_pin_replaced = True
    if policy_choke_actuator.get("status") == "DIRECT_SOURCE_SELECTION_ALREADY_ACTIVE":
        pass
    elif freeze_pin_replaced:
        policy_choke_actuator["selection_authority"] = (
            source_drought_candidates.get("selection_authority")
        )
    elif (
        direct_selection_eligible
        and isinstance(selected_freeze_direct, dict)
        and not active_direct_pin
    ):
        updated_active_set_state, policy_choke_actuator = _execute_policy_choke_rung_b(
            overlay=active_set_state,
            candidate=selected_freeze_direct,
            now=now,
            supply_rung="DIRECT",
            admission_authority=_current_direct_pin_admission_authority(
                candidate=selected_freeze_direct,
            ),
        )
        policy_choke_actuator["candidate_evidence"] = source_drought_candidates
        policy_choke_actuator["selection_authority"] = (
            source_drought_candidates.get("selection_authority")
        )
        if policy_choke_actuator["status"] in {
            "DIRECT_SOURCE_SELECTION_PIN_WRITTEN",
            "DIRECT_SOURCE_SELECTION_POLICY_REPAIRED",
        }:
            atomic_write_json(root / args.active_set_state, updated_active_set_state)
            active_set_state = updated_active_set_state
    elif source_roster_drought["firing"]:
        updated_active_set_state, policy_choke_actuator = _execute_policy_choke_rung_b(
            overlay=active_set_state,
            candidate=source_drought_candidates.get("selected"),
            now=now,
            supply_rung="DIRECT",
            admission_authority=_current_direct_pin_admission_authority(
                candidate=source_drought_candidates.get("selected"),
            ),
        )
        policy_choke_actuator["candidate_evidence"] = source_drought_candidates
        policy_choke_actuator["direct_source"] = direct_source
        policy_choke_actuator["recovery_ttl"] = recovery_ttl
        if policy_choke_actuator["status"] in {
            "DIRECT_SOURCE_SELECTION_PIN_WRITTEN",
            "DIRECT_SOURCE_SELECTION_POLICY_REPAIRED",
        }:
            atomic_write_json(root / args.active_set_state, updated_active_set_state)
            active_set_state = updated_active_set_state
        elif not source_drought_candidates.get("selected"):
            policy_choke_actuator["status"] = rung_c_label
            policy_choke_actuator["rung_c_settlement"] = rung_c_settlement
            policy_choke_actuator["refusal_counts"] = source_drought_candidates.get(
                "refusal_counts", {}
            )
            policy_choke_actuator["quality_bars_unchanged"] = True
    elif policy_choke["firing"] and reconciliation_gate_pass:
        updated_active_set_state, policy_choke_actuator = _execute_policy_choke_rung_b(
            overlay=active_set_state,
            candidate=source_drought_candidates.get("selected"),
            now=now,
            supply_rung="DIRECT",
            admission_authority=_current_direct_pin_admission_authority(
                candidate=source_drought_candidates.get("selected"),
            ),
        )
        policy_choke_actuator["candidate_evidence"] = source_drought_candidates
        if policy_choke_actuator["status"] in {
            "DIRECT_SOURCE_SELECTION_PIN_WRITTEN",
            "DIRECT_SOURCE_SELECTION_POLICY_REPAIRED",
        }:
            atomic_write_json(root / args.active_set_state, updated_active_set_state)
            active_set_state = updated_active_set_state
        elif not source_drought_candidates.get("selected"):
            policy_choke_actuator = {
                "status": rung_c_label,
                "reason": "wide_direct_frontier_dry_label_only",
                "rung_c_settlement": rung_c_settlement,
                "candidate_evidence": source_drought_candidates,
                "refusal_counts": source_drought_candidates.get(
                    "refusal_counts", {}
                ),
                "terminal_outcome": True,
                "quality_bars_unchanged": True,
                "live_pin_write_authority": False,
                "rule": (
                    "when no exact wide_direct frontier row is eligible, RUNG_C "
                    "remains a visible label and never admits ready-shadow/full-pool supply"
                ),
            }
    elif policy_choke["firing"]:
        policy_choke_actuator = {
            "status": "REFUSED_INCOMPLETE_SOURCE_RECONCILIATION",
            "source_coverage_full": policy_choke["source_coverage_full"],
            "input_equals_terminal_rows": policy_choke["input_equals_terminal_rows"],
            "quality_bars_unchanged": True,
        }
    policy_choke_actuator.setdefault(
        "candidate_evidence", source_drought_candidates
    )
    policy_choke_actuator.setdefault(
        "rung_b_candidate_evidence", source_drought_candidates
    )
    policy_choke_actuator["pre_f2_roster_publication"] = {
        "status": "PUBLISHED",
        "candidate_rows": len(source_drought_candidates.get("rows") or []),
        "otherwise_qualified_except_f2": sum(
            1
            for row in source_drought_candidates.get("rows") or []
            if isinstance(row, dict)
            and row.get("checks", {}).get("f2_fresh_rows_and_own_policy_copyable")
            is False
            and all(
                passed is True
                for name, passed in (row.get("checks") or {}).items()
                if name != "f2_fresh_rows_and_own_policy_copyable"
            )
        ),
        "f2_feedstock_pipe_identity": "hot_history_union_qualified_pool_stakeout_only",
        "rtds_counts_toward_f2": False,
    }
    policy_choke["actuator"] = policy_choke_actuator
    result["policy_choke"] = policy_choke
    result["early_admission_authority"] = _run_admission_tick(candidate=selected_tick_candidate, stage="actuator_complete")
    result["admission_tick_rows"] = admission_tick_rows
    _stage_checkpoint("actuation_and_tick")
    fire_drill_rung = policy_choke_rung_a(
        [
            {"wallet": selected_runtime_wallet or "0xincumbent", "accepted_live_orders": 0, "acceptance_share_pct": 0.0, "materially_nonzero": False, "regime_slice_label": "PROVEN-POSITIVE", "regime_slice_pnl_usd": 1.0},
            {"wallet": "0xsyntheticweekdaypositive", "accepted_live_orders": 7, "acceptance_share_pct": 14.0, "materially_nonzero": True, "regime_slice_label": "PROVEN-POSITIVE", "regime_slice_pnl_usd": 2.0},
        ],
        selected_runtime_wallet or "0xincumbent",
        regime=regime,
    )
    fire_drill = _policy_choke_incident(
        scan={"member_signal_age": {(selected_runtime_wallet or "0xincumbent"): {"fresh_source_rows": 497, "suppressed_intents": 497, "eligible_intents": 0, "accepted_orders": 0, "suppression_taxonomy": {"price_outside_policy": 497}}}},
        selected_wallet=selected_runtime_wallet or "0xincumbent",
        can_trade=True,
        rung_a=fire_drill_rung,
    )
    fire_drill["kind"] = "policy_choke_gate_fire_drill"
    fire_drill["checked_at"] = now.isoformat()
    fire_drill["test_gate"] = "P-1"
    rung_a_gate_pass = bool(
        fire_drill["status"] == "INCIDENT_POLICY_CHOKE"
        and fire_drill["mechanical_escalation"] == "RUNG_A_RESELECT"
    )
    synthetic_wallet = "0x" + "b" * 40
    synthetic_policy = {"policy_id": "synthetic_evidenced_policy", "max_order_usd": 1.0, "min_order_usd": 1.0, "max_price": 0.7}
    synthetic_overlay = {"members": [{"source_wallet": "0x" + "c" * 40, "policy_id": "synthetic_evidenced_policy", "policy": synthetic_policy, "enabled": True}]}
    synthetic_ready = {"lanes": [{"wallet": synthetic_wallet, "paper_policy_id": "synthetic_evidenced_policy", "retrospective_gross_pnl_usd": 25.0, "retrospective_gross_roi_pct": 5.0, "retrospective_resolved_signals": 250, "fading_clear": True, "external_liveness_status": "PASS", "external_latest_trade_age_h": 1.0}]}
    synthetic_market_start = int(now.timestamp()) // 300 * 300
    synthetic_hot = {
        "events": [
            {
                "source_wallet": synthetic_wallet,
                "action": "BUY",
                "observed_ts": now.timestamp() - index,
                "event_id": f"synthetic-{index}",
                "market_slug": f"btc-updown-5m-{synthetic_market_start}",
            }
            for index in range(10)
        ]
    }
    synthetic_temporal = {
        "criteria": {"min_trades": 5},
        "wallets": [
            {
                "wallet": synthetic_wallet,
                "slice_labels": {
                    "weekday": {
                        "label": "PROVEN-POSITIVE",
                        "resolved_trades": 250,
                        "pnl_usd": 25.0,
                        "roi_pct": 5.0,
                    }
                },
            }
        ],
    }
    synthetic_candidates = _select_policy_choke_rung_b_candidate(
        ready_shadow=synthetic_ready,
        overlay=synthetic_overlay,
        hot_history=synthetic_hot,
        now=now,
        regime="weekday",
        temporal_registry=synthetic_temporal,
        cooloffs={},
    )
    _unused_overlay, rung_b_drill = _execute_policy_choke_rung_b(overlay=synthetic_overlay, candidate=synthetic_candidates.get("selected"), now=now, dry_run=True)
    fire_drill["rung_b_dry_run"] = {**rung_b_drill, "candidate_evidence": synthetic_candidates}
    rung_b_gate_pass = rung_b_drill.get("status") == "RUNG_B_DRY_RUN_PASS"
    rung_c_liveness_drill = _select_policy_choke_rung_c_candidate(
        cohort_admission=cohort_admission,
        full_pool_queue=full_pool_queue,
        overlay=active_set_state,
        hot_history=hot_history,
        now=now,
        regime="weekday" if now.weekday() < 5 else "weekend",
        cooloffs=rung_b_cooloffs,
        temporal_registry=temporal,
    )
    queue_liveness_rows = [
        row for row in rung_c_liveness_drill.get("rows", [])
        if row.get("supply_source") == "full_pool_member_queue"
        and row.get("checks", {}).get("f4_external_liveness") is True
    ]
    queue_candidate_count = sum(
        1 for row in rung_c_liveness_drill.get("rows", [])
        if row.get("supply_source") == "full_pool_member_queue"
    )
    fresh_own_source_positive_rows = [
        row for row in rung_c_liveness_drill.get("rows", [])
        if int(row.get("fresh_own_source_buy_rows_30m") or 0) > 0
    ]
    fire_drill["rung_c_full_pool_liveness_drill"] = {
        "status": "PASS" if queue_liveness_rows else "SKIP_NO_QUEUE_ROWS" if queue_candidate_count == 0 else "FAIL",
        "rule": "full_pool_member_queue rows must carry a real external_liveness_status PASS stamp for F4",
        "queue_candidate_count": queue_candidate_count,
        "rung_c_candidate_count": int(rung_c_liveness_drill.get("candidate_count") or 0),
        "fresh_own_source_positive_rows": len(fresh_own_source_positive_rows),
        "f4_external_liveness_true_queue_rows": len(queue_liveness_rows),
        "sample_rows": queue_liveness_rows[:5],
        "fresh_own_source_positive_sample_rows": fresh_own_source_positive_rows[:5],
        "candidate_evidence": rung_c_liveness_drill,
    }
    queue_gate_pass = not queue_candidate_count or bool(queue_liveness_rows)
    fire_drill["gate_verdict"], fire_drill["verdict"] = (
        _policy_choke_fire_drill_verdict(
            rung_a_gate_pass=rung_a_gate_pass,
            rung_b_gate_pass=rung_b_gate_pass,
            queue_gate_pass=queue_gate_pass,
            rung_c_settlement=rung_c_settlement,
        )
    )
    fire_drill_path = root / POLICY_CHOKE_FIRE_DRILL
    fire_drill_path.parent.mkdir(parents=True, exist_ok=True)
    fire_drill_path.write_text(json.dumps(fire_drill, indent=2, sort_keys=True) + "\n")
    result["policy_choke_fire_drill"] = fire_drill
    source_drought_fire_drill = _source_roster_drought_fire_drill(now=now)
    source_drill_path = root / SOURCE_ROSTER_DROUGHT_FIRE_DRILL
    source_drill_path.parent.mkdir(parents=True, exist_ok=True)
    source_drill_path.write_text(
        json.dumps(source_drought_fire_drill, indent=2, sort_keys=True) + "\n"
    )
    result["source_roster_drought_fire_drill"] = source_drought_fire_drill
    # Age the guard event log against a read-time clock, not the cycle-start `now`.
    # This body runs for p50 ~219s / p95 ~347s; tailing a live guard event log with the
    # cycle-start stamp made every healthy guard look future-dated, which pinned
    # pipe_event_fresh (and therefore `verified`) to False whenever the guard was alive.
    pipe_quiet_now = _parse_ts(args.now) if args.now else _utc_now()
    if pipe_quiet_now is None:
        pipe_quiet_now = now
    pipe_quiet = _pipe_verified_source_quiet(
        guard, root / args.guard_event_log, pipe_quiet_now
    )
    latest_suppression = guard_events.get("latest_approved_suppression_ts")
    latest_pipe_quiet = pipe_quiet.get("latest_ts")
    annotation_candidates = [
        dt for dt in (latest_suppression, latest_pipe_quiet) if dt is not None
    ]
    latest_annotation = max(annotation_candidates) if annotation_candidates else None
    if latest_pipe_quiet is not None and latest_annotation == latest_pipe_quiet:
        annotation_source = "pipe_verified_source_quiet"
    elif latest_suppression is not None and latest_annotation == latest_suppression:
        annotation_source = "approved_suppression"
    else:
        annotation_source = "none"
    latest = latest_order
    liveness_source = "accepted_order" if latest_order is not None else "none"
    idle_s = (now - latest).total_seconds() if latest else None
    latest_profit_pass = guard_events.get("latest_profit_filter_pass_ts")
    if latest_order and (latest_profit_pass is None or latest_order > latest_profit_pass):
        latest_profit_pass = latest_order
    eligible_drought_s = (now - latest_profit_pass).total_seconds() if latest_profit_pass else None
    result["can_trade"] = can_trade
    result["can_trade_reason"] = can_trade_reason
    result["can_trade_evidence"] = {
        "guard_summary_present": guard_summary_present,
        "guard_summary_can_trade": guard_summary_can_trade,
        "ledger_summary_present": ledger_summary_present,
        "ledger_summary_can_trade": ledger_summary_can_trade,
        "guard_execute_live": guard.get("execute_live"),
        "guard_live_orders_allowed": guard.get("live_orders_allowed"),
        "guard_operator_live_authority": operator_live_authority,
        "guard_blockers": live_blockers,
        "guard_top_level_fallback_can_trade": guard_fallback_can_trade,
    }
    result["latest_order_ts"] = latest_order.isoformat() if latest_order else None
    result["latest_approved_suppression_ts"] = latest_suppression.isoformat() if latest_suppression else None
    result["latest_pipe_verified_source_quiet_ts"] = (
        latest_pipe_quiet.isoformat() if latest_pipe_quiet else None
    )
    result["pipe_verified_source_quiet"] = {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in pipe_quiet.items()
        if key != "latest_ts"
    }
    result["guard_side_halt_signal"] = _guard_side_halt_signal(
        guard,
        pipe_quiet,
        idle_s=idle_s,
        latest_order=latest_order,
        now=now,
        previous_last_fresh_actionable_ts=previous_last_fresh_actionable_ts,
    )
    demand_split = result["guard_side_halt_signal"].get(
        "active_set_fresh_demand_split", {}
    )
    result.update(_selected_identity_projection(guard, demand_split))
    generation_mismatch = bool(
        guard_code_identity.get("live_guard_generation_sha256")
        != disk_generation().get("sha256")
    )
    post_selection_floor = _post_selection_liveness_floor(
        guard=guard,
        selected=result,
        previous=prev if isinstance(prev, dict) else {},
        latest_accepted=latest_accepted_order,
        now=now,
        can_trade=can_trade,
        max_idle_s=float(args.max_idle_s),
        generation_mismatch=generation_mismatch,
    )
    result["post_selection_liveness_floor"] = post_selection_floor
    result["selected_seat_epoch_at"] = post_selection_floor.get(
        "selected_seat_epoch_at"
    )
    policy_choke["paper_routing_shadow_intent_trace"] = _paper_routing_shadow_intent_trace(
        routing_shadow=routing_shadow,
        ledger=ledger,
        selected_wallet=selected_runtime_wallet,
        since=now - timedelta(seconds=POLICY_CHOKE_LOOKBACK_S),
        until=now,
        selected_seat_epoch_at=_parse_ts(
            post_selection_floor.get("selected_seat_epoch_at")
        ),
        authority_counter=int(policy_choke.get("selected_eligible_intents") or 0),
    )
    policy_choke["live_intent_to_submit_reconciliation"] = (
        _live_intent_to_submit_reconciliation(
            selected_eligible_intents=int(
                policy_choke.get("selected_eligible_intents") or 0
            ),
            selected_guard_submit_attempts=int(
                policy_choke.get("selected_guard_submit_attempts") or 0
            ),
        )
    )
    result["liveness_ts"] = latest.isoformat() if latest else None
    result["liveness_source"] = liveness_source
    result["liveness_basis"] = "accepted_order"
    result["annotation_liveness_ts"] = latest_annotation.isoformat() if latest_annotation else None
    result["annotation_liveness_source"] = annotation_source
    result["annotation_liveness_only"] = True
    result["accepted_order_idle_s"] = idle_s
    result["idle_s"] = idle_s
    utc_day_money = _utc_day_money(ledger, now)
    result["utc_day_money"] = utc_day_money
    result["money_anchored_status"] = _money_anchored_status(
        accepted_order_idle_s=idle_s,
        money=utc_day_money,
    )
    host_downtime = {
        "status": "UNKNOWN",
        "host_boot_time": host_boot_time.isoformat() if host_boot_time else None,
        "liveness_before_boot": bool(latest and host_boot_time and latest < host_boot_time),
        "downtime_s": round((host_boot_time - latest).total_seconds(), 6)
        if latest and host_boot_time and latest < host_boot_time
        else None,
        "rule": "if the accepted-order liveness timestamp predates host boot, attribute idle to host downtime instead of live guard runtime flow death",
        "post_boot_recovery_liveness_ts": post_boot_recovery_liveness.isoformat()
        if post_boot_recovery_liveness
        else None,
        "post_boot_recovery_idle_s": post_boot_recovery_idle_s,
        "post_boot_recovery_grace": post_boot_recovery_grace,
        "post_boot_accepted_order_pending_since": post_boot_accepted_order_pending_since.isoformat()
        if post_boot_accepted_order_pending_since
        else None,
        "post_boot_acceptance_path_uncertified": post_boot_acceptance_path_uncertified,
        "post_boot_acceptance_path_uncertified_after_s": POST_BOOT_ACCEPTANCE_PATH_UNCERTIFIED_S,
    }
    if host_boot_time is None:
        host_downtime["status"] = "BOOT_TIME_UNAVAILABLE"
    elif latest is None:
        host_downtime["status"] = "NO_ACCEPTED_ORDER_BASELINE"
    elif latest < host_boot_time:
        host_downtime["status"] = "HOST_DOWNTIME_RESTART"
    else:
        host_downtime["status"] = "LIVE_RUNTIME_IDLE"
    result["host_downtime_attribution"] = host_downtime
    result["eligible_drought_s"] = eligible_drought_s
    result["latest_profit_filter_pass_ts"] = latest_profit_pass.isoformat() if latest_profit_pass else None
    result["approved_suppression_tags"] = guard_events.get("approved_suppression_tags", [])
    result["approved_suppression_events"] = guard_events.get("approved_suppression_events", 0)
    result["eligible_profit_filter_pass_intents"] = guard_events.get("eligible_profit_filter_pass_intents", 0)
    result["fresh_stale_signal_rows"] = guard_events.get("fresh_stale_signal_rows", 0)
    result["member_signal_age"] = guard_events.get("member_signal_age", {})
    result["eligible_drought_status"] = (
        "ROTATION_EVIDENCE_DUE"
        if can_trade
        and idle_s is not None
        and idle_s <= args.max_idle_s
        and eligible_drought_s is not None
        and eligible_drought_s >= args.eligible_drought_alert_s
        and int(result["fresh_stale_signal_rows"] or 0) > 0
        else "OK"
    )
    gated_quiet = _gated_quiet_classification(
        guard=guard,
        active_set_state=active_set_state,
        ledger=ledger,
        now=now,
        idle_s=idle_s,
        max_idle_s=float(args.max_idle_s),
        latest_order=latest_order,
        eligible_drought_status=str(result["eligible_drought_status"]),
        latest_suppression=latest_suppression,
        pipe_quiet=pipe_quiet,
        previous_last_fresh_actionable_ts=previous_last_fresh_actionable_ts,
        local_only_rejects=bool(
            (policy_choke.get("local_skip_classification") or {}).get(
                "observed_reject_classes"
            )
            and (policy_choke.get("local_skip_classification") or {}).get(
                "local_only_rejects"
            )
        ),
        accepted_orders=int(
            (policy_choke.get("local_skip_classification") or {}).get(
                "accepted_orders"
            )
            or 0
        ),
        pipe_healthy_terms=bool(
            (policy_choke.get("local_skip_classification") or {}).get(
                "pipe_healthy_terms"
            )
        ),
    )
    reconciliation = (
        policy_choke.get("terminal_reconciliation")
        if isinstance(policy_choke.get("terminal_reconciliation"), dict)
        else {}
    )
    _sync_terminal_source_coverage(result, gated_quiet, reconciliation)
    result["gated_quiet_classification"] = gated_quiet
    result["window_time_near_miss_summary"] = gated_quiet.get(
        "window_time_near_miss_summary",
        {},
    )
    result["last_fresh_actionable_ts"] = gated_quiet.get("guard_side_halt_signal", {}).get(
        "last_fresh_actionable_ts"
    )
    result["last_fresh_actionable_source"] = gated_quiet.get("guard_side_halt_signal", {}).get(
        "last_fresh_actionable_source"
    )
    result["deadman_warning"] = gated_quiet.get("guard_side_halt_signal", {}).get("warning")
    result["benign_skip_overrode"] = gated_quiet.get("guard_side_halt_signal", {}).get(
        "benign_skip_overrode",
        [],
    )

    effective_idle_s = (
        post_boot_recovery_idle_s if post_boot_recovery_grace else idle_s
    )
    result["effective_deadman_idle_s"] = effective_idle_s
    local_skip = policy_choke.get("local_skip_classification") or {}
    local_skip_starvation = bool(local_skip.get("qualifies"))
    local_skip_idle_threshold_s = max(float(args.max_idle_s), 3600.0)
    ruled_posture_exemption = bool(
        local_skip_starvation
        and idle_s is not None
        and idle_s < local_skip_idle_threshold_s
    )
    global_accepted_order_deadman = bool(
        can_trade
        and not ruled_posture_exemption
        and (
            latest is None
            or (
                host_downtime.get("status") != "HOST_DOWNTIME_RESTART"
                and idle_s is not None
                and idle_s >= args.max_idle_s
            )
        )
    )
    raw_accepted_order_deadman = bool(
        global_accepted_order_deadman and not post_selection_floor["active"]
    )
    result["raw_accepted_order_deadman"] = {
        "firing": raw_accepted_order_deadman,
        "global_firing": global_accepted_order_deadman,
        "accepted_order_idle_s": idle_s,
        "threshold_s": (
            local_skip_idle_threshold_s
            if local_skip_starvation
            else float(args.max_idle_s)
        ),
        "deadman_class": local_skip.get("deadman_class") or "ORDER_FLOW_DEAD",
        "ruled_posture_exemption": ruled_posture_exemption,
        "local_skip_classification": local_skip,
        "rule": (
            "attempted-and-venue-refused flow retains the base idle threshold; "
            "POLICY_CHOKE_LOCAL_SKIP permits submits and named local "
            "maker_min_share_bump_exceeds_policy_cap refusals when FAK is zero, "
            "accepted orders are zero, and local_terminal_share >=0.60; it watches "
            "until the 3600s local-skip threshold"
        ),
    }
    selected_candidate = (
        (policy_choke.get("actuator") or {}).get("candidate_evidence", {}).get(
            "selected"
        )
        if isinstance(policy_choke.get("actuator"), dict)
        else None
    )
    candidate_evidence = (
        (policy_choke.get("actuator") or {}).get("candidate_evidence") or {}
        if isinstance(policy_choke.get("actuator"), dict)
        else {}
    )
    rung_b_candidate_evidence = (
        (policy_choke.get("actuator") or {}).get("rung_b_candidate_evidence")
        if isinstance(policy_choke.get("actuator"), dict)
        else None
    )
    if not isinstance(rung_b_candidate_evidence, dict):
        rung_b_candidate_evidence = candidate_evidence
    rung_b_pin = (
        active_set_state.get("selection_pin")
        if isinstance(active_set_state.get("selection_pin"), dict)
        else {}
    )
    if (
        str((result.get("policy_choke_rung_b_lifecycle") or {}).get("status"))
        == "RUNG_B_ACTIVE"
        and rung_b_pin.get("pin_id") == POLICY_CHOKE_RUNG_B_PIN_ID
        and int(rung_b_candidate_evidence.get("eligible_count") or 0) == 0
    ):
        pinned_candidate_id = str(rung_b_pin.get("candidate_id") or "")
        supply_rung = (
            "DIRECT"
            if "_rung_direct_" in pinned_candidate_id
            else "RECOVERY"
            if "_rung_recovery_" in pinned_candidate_id
            else "RUNG_C"
            if "_rung_rung_c_" in pinned_candidate_id
            else "B"
        )
        result["policy_choke_rung_b_refusal"] = _policy_choke_rung_b_refusal_record(
            selection_pin=rung_b_pin,
            candidate_evidence=rung_b_candidate_evidence,
            overlay=active_set_state,
            now=now,
            supply_rung=supply_rung,
        )
    measured_empty_seat_no_target = bool(
        selected_candidate is None
        and int(candidate_evidence.get("candidate_count") or 0) > 0
        and int(candidate_evidence.get("eligible_count") or 0) == 0
        and str(candidate_evidence.get("status") or "")
        in {
            "NO_ADMISSIBLE_TARGET",
            "MEASURED_EMPTY_SEAT_HOLD_FREEZE_NOT_ALL_PASS",
        }
    )
    result["suppressed_limb_status"] = policy_choke.get("status")
    result["suppressed_limb_firing"] = bool(policy_choke.get("firing"))
    result["suppressed_limb_mechanical_escalation"] = policy_choke.get(
        "mechanical_escalation"
    )
    result["suppressed_limb_rule"] = (
        "always disclose the policy-choke limb even when top-level disposition "
        "selects a measured no-target headline"
    )
    selected_wallet = _normalize_wallet(
        selected_candidate.get("wallet") if isinstance(selected_candidate, dict) else ""
    )
    selected_candidate_id = (
        f"policy_choke_rung_direct_{selected_wallet[-10:]}"
        if selected_wallet
        else ""
    )
    runtime_candidate_ids = {
        str(member.get("candidate_id") or "")
        for member in ((guard.get("active_set_runtime") or {}).get("members") or [])
        if isinstance(member, dict)
    }
    selection_already_adopted = bool(
        post_selection_floor["matching_adopted_runtime"]
    )
    selection_pending_adoption = bool(
        selected_candidate_id
        and selected_candidate_id not in runtime_candidate_ids
        and not selection_already_adopted
    )
    wallet_policy_diagnostic = _wallet_policy_disposition_diagnostic(
        policy_choke=policy_choke,
        previous=prev,
        selected_candidate=selected_candidate,
    )
    accepted_deadman_disposition = _accepted_order_deadman_disposition(
        raw_firing=raw_accepted_order_deadman,
        local_skip_qualifies=local_skip_starvation,
        ruled_posture_exemption=ruled_posture_exemption,
        accepted_orders=int(local_skip.get("accepted_orders") or 0),
        local_terminal_share=float(local_skip.get("local_terminal_share") or 0.0),
        fak_no_match_outcomes=int(local_skip.get("fak_no_match_outcomes") or 0),
        liquidity_drought=bool(policy_choke.get("liquidity_drought")),
        generation_mismatch=generation_mismatch,
        guard_status=str(guard.get("status") or ""),
        selection_pending_adoption=selection_pending_adoption,
        wallet_policy_diagnostic=wallet_policy_diagnostic,
        measured_empty_seat_no_target=measured_empty_seat_no_target,
        selected_eligible_intents=int(
            policy_choke.get("selected_eligible_intents") or 0
        ),
        selected_guard_submit_attempts=int(
            policy_choke.get("selected_guard_submit_attempts") or 0
        ),
        local_skip_deadman_class=str(
            local_skip.get("deadman_class") or "POLICY_CHOKE_LOCAL_SKIP"
        ),
        local_skip_mechanical_escalation=str(
            local_skip.get("mechanical_escalation")
            or "LOCAL_POLICY_SIZING_CHOKE_DUE"
        ),
    )
    result["mechanical_escalation"] = (
        accepted_deadman_disposition["mechanical_escalation"]
        if accepted_deadman_disposition is not None
        else policy_choke.get("mechanical_escalation", "NONE")
    )
    roster_collapse = _roster_collapse_limb(
        guard=guard,
        previous=prev,
        now=now,
        threshold_s=float(args.max_idle_s),
    )
    result["roster_collapse_limb"] = roster_collapse
    zero_supply_seat = _zero_supply_seat_limb(
        guard=guard,
        policy_choke=policy_choke,
        idle_s=idle_s,
        threshold_s=float(args.max_idle_s),
        post_selection_floor=post_selection_floor,
    )
    result["zero_supply_seat_limb"] = zero_supply_seat
    _enforce_policy_choke_zero_supply_status(
        policy_choke,
        idle_s=idle_s,
        threshold_s=float(args.max_idle_s),
    )
    firing = raw_accepted_order_deadman or (
        can_trade
        and not post_selection_floor["active"]
        and (latest is None or effective_idle_s >= args.max_idle_s)
    )
    if roster_collapse["firing"]:
        firing = True
        if not selection_pending_adoption:
            result["mechanical_escalation"] = "NONE"
    if zero_supply_seat["firing"]:
        firing = True
        if not selection_pending_adoption:
            result["mechanical_escalation"] = "NONE"
    if policy_choke["firing"] and not ruled_posture_exemption:
        firing = True
    elif _post_selection_floor_can_downgrade(
        post_selection_floor=post_selection_floor,
        zero_supply_seat=zero_supply_seat,
        roster_collapse=roster_collapse,
    ):
        result["status"] = "WATCH_INHERITED_PRIOR_SEAT_IDLE"
        result["deadman_class"] = "INHERITED_PRIOR_SEAT_IDLE"
        result["failed_gates"] = _failed_gates(gated_quiet)
        result["last_alert_at"] = prev.get("last_alert_at")
        result["mechanical_escalation"] = "SELECTION_ALREADY_ADOPTED"
    elif ruled_posture_exemption:
        firing = False
    if firing:
        gated_quiet_firing = gated_quiet.get("status") == "PASS"
        classification = str(gated_quiet.get("classification") or "")
        selected_identity_incident = bool(
            result.get("selected_identity_unresolved_with_observed_demand")
            and classification in {"GUARD_SIDE_HALT", "MEASURED_NONSELECTED_DEMAND"}
        )
        specific_accepted_disposition = _accepted_disposition_outranks_policy_choke(
            accepted_deadman_disposition,
            policy_choke,
        )
        result["status"] = (
            "INCIDENT_SELECTED_IDENTITY_UNRESOLVED"
            if selected_identity_incident
            else "INCIDENT_ZERO_SUPPLY_SEAT"
            if zero_supply_seat["firing"]
            else roster_collapse["incident_class"]
            if roster_collapse["firing"]
            else accepted_deadman_disposition["status"]
            if specific_accepted_disposition
            else
            "UNATTRIBUTED_SELECTED_SEAT_ATTRITION"
            if policy_choke.get("unattributed_selected_seat_attrition") is True
            else
            "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
            if (
                policy_choke.get("status") == "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
                and float(policy_choke.get("selected_source_row_identity_coverage") or 0.0)
                >= SOURCE_ROW_IDENTITY_COVERAGE_FLOOR
            )
            else
            "INCIDENT_POLICY_CHOKE"
            if policy_choke["firing"]
            else
            "HOST_DOWNTIME_RESTART"
            if host_downtime.get("status") == "HOST_DOWNTIME_RESTART"
            else
            "INCIDENT_GUARD_SIDE_HALT"
            if classification == "GUARD_SIDE_HALT"
            else
            "MEASURED_FLOOR_DEADLOCK"
            if gated_quiet_firing and classification == "FLOOR_DEADLOCK"
            else
            "MEASURED_SKIP_MORNING_BENCH"
            if gated_quiet_firing and classification == "MEASURED_SKIP_MORNING_BENCH"
            else
            "MEASURED_TIMING_SKIP"
            if gated_quiet_firing and classification == "MEASURED_TIMING_SKIP"
            else
            "MEASURED_SOURCE_QUIET"
            if gated_quiet_firing and classification == "MEASURED_SOURCE_QUIET"
            else
            "MEASURED_NONSELECTED_DEMAND"
            if gated_quiet_firing and classification == "MEASURED_NONSELECTED_DEMAND"
            else "MEASURED_NONSELECTED_ROTATION_EVENT"
            if gated_quiet_firing and classification == "MEASURED_NONSELECTED_ROTATION_EVENT"
            else "INCIDENT_MEASURED_SKIP_GATED_QUIET"
            if gated_quiet_firing
            else accepted_deadman_disposition["status"]
            if accepted_deadman_disposition is not None
            else "INCIDENT_ORDER_FLOW_DEAD"
        )
        result["deadman_class"] = (
            "SELECTED_IDENTITY_UNRESOLVED"
            if selected_identity_incident
            else "ZERO_SUPPLY_SEAT"
            if zero_supply_seat["firing"]
            else ("ROSTER_COLLAPSE" if roster_collapse["incident_class"] == "INCIDENT_ROSTER_COLLAPSE" else "ROSTER_INADMISSIBLE")
            if roster_collapse["firing"]
            else accepted_deadman_disposition["deadman_class"]
            if specific_accepted_disposition
            else "UNATTRIBUTED_SELECTED_SEAT_ATTRITION"
            if policy_choke.get("unattributed_selected_seat_attrition") is True
            else "POLICY_CHOKE_LOCAL_SKIP"
            if local_skip_starvation
            else
            "POLICY_CHOKE"
            if policy_choke["firing"]
            else
            "HOST_DOWNTIME_RESTART"
            if result["status"] == "HOST_DOWNTIME_RESTART"
            else classification
            if gated_quiet_firing and classification
            else accepted_deadman_disposition["deadman_class"]
            if accepted_deadman_disposition is not None
            else classification or result["status"]
        )
        result["failed_gates"] = _failed_gates(gated_quiet)
        last_alert = _parse_ts(prev.get("last_alert_at"))
        floor_deadlock_state_change = (
            result["status"] == "MEASURED_FLOOR_DEADLOCK"
            and str(prev.get("deadman_class") or "") != "FLOOR_DEADLOCK"
        )
        host_downtime_state_change = (
            result["status"] == "HOST_DOWNTIME_RESTART"
            and str(prev.get("deadman_class") or "") != "HOST_DOWNTIME_RESTART"
        )
        should_alert = (
            floor_deadlock_state_change
            or host_downtime_state_change
            or (
                str(result["status"]).startswith("INCIDENT_")
                and _incident_notify_allowed(
                    status=str(result["status"]),
                    can_trade=can_trade,
                )
                and (
                    last_alert is None
                    or (now - last_alert).total_seconds() >= args.realert_s
                )
            )
        )
        if should_alert:
            result["last_alert_at"] = now.isoformat()
            idle_min = int(idle_s // 60) if idle_s else -1
            if result["status"] == "INCIDENT_POLICY_CHOKE":
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN POLICY-CHOKE\n"
                    f"- can_trade={can_trade} and fresh source flow has zero accepted live orders over "
                    f"rolling 30m: selected_fresh={policy_choke['selected_fresh_source_rows']}, "
                    f"whole_runtime_fresh={policy_choke['whole_runtime_fresh_source_rows']}, "
                    f"taxonomy={policy_choke['selected_suppression_taxonomy']}.\n"
                    f"- MECHANICAL RULE: {policy_choke['mechanical_escalation']}; "
                    f"rung_a_target={policy_choke['rung_a_seat_read'].get('target_wallet')}. "
                    f"No expectancy, price-bar, fill-cap, or size-cap loosening.\n"
                )
            elif result["status"] == "INCIDENT_LOCAL_POLICY_SKIP":
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN LOCAL-POLICY-SKIP\n"
                    f"- can_trade=True and live submit attempts reached named local refusal classes "
                    f"with zero venue no-match outcomes and zero accepts: "
                    f"local_skip={local_skip}; idle_min={idle_min}.\n"
                    f"- MECHANICAL RULE: LOCAL_SKIP_GENERATION_RELOAD_DUE; spend only the "
                    f"budget-capped local_skip_generation_mismatch reload. No cap, binding, "
                    f"roster, expectancy, or price-bar loosening.\n"
                )
            elif result["status"] == "INCIDENT_GUARD_SIDE_HALT":
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN GUARD-SIDE HALT\n"
                    f"- can_trade=True but source-emitting evidence exists with no accepted live order for {idle_min} min "
                    f"(threshold {args.max_idle_s // 60} min; guard_side_halt={result['guard_side_halt_signal']}; "
                    f"failed_gates={result['failed_gates']}).\n"
                    f"- MECHANICAL RULE: this is not SOURCE_QUIET; next heartbeat/pulse restores the guard path "
                    f"or falls through to an executable member before reporting.\n"
                )
            elif result["status"] == "MEASURED_FLOOR_DEADLOCK":
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN FLOOR-DEADLOCK\n"
                    f"- can_trade=True and no accepted live order for {idle_min} min, "
                    f"but the guard-side halt is fully accounted as measured floor_deadlock: "
                    f"floor_deadlock={gated_quiet.get('floor_deadlock_classification')}; "
                    f"failed_gates={result['failed_gates']}.\n"
                    f"- MECHANICAL RULE: keep global floor/probe caps intact; only an explicitly ruled executable "
                    f"carve-out may restore live flow.\n"
                )
            elif gated_quiet_firing and not raw_accepted_order_deadman:
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN MEASURED-SKIP GATED QUIET\n"
                    f"- can_trade=True and no accepted live order for {idle_min} min, "
                    f"but the gated-quiet classifier passed: eligible_drought_status=OK, "
                    f"latest_approved_suppression={result['latest_approved_suppression_ts']}, "
                    f"source_coverage={gated_quiet['source_coverage']}, "
                    f"adjusted_consecutive_missed={gated_quiet['adjusted_consecutive_missed_active_windows']}, "
                    f"taxonomy={gated_quiet['deduplicated_gate_taxonomy']}, "
                    f"failed_gates={result['failed_gates']}.\n"
                    f"- MECHANICAL RULE: quality-over-volume measured skip; do not preempt "
                    f"foreground work or loosen trading gates. Hard backstop remains "
                    f"{int(GATED_QUIET_HARD_BACKSTOP_S // 60)} min accepted-order idle.\n"
                )
            elif result["status"] == "HOST_DOWNTIME_RESTART":
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN HOST-DOWNTIME\n"
                    f"- can_trade=True and accepted-order liveness predates host boot: "
                    f"liveness_ts={result['liveness_ts']}, host_boot_time={host_downtime.get('host_boot_time')}, "
                    f"downtime_s={host_downtime.get('downtime_s')}, idle_min={idle_min}. "
                    f"This is host downtime attribution, not live runtime ORDER_FLOW_DEAD.\n"
                    f"- MECHANICAL RULE: run boot-recovery audit/repair, then judge post-boot flow only on "
                    f"fresh cycles after host_boot_time.\n"
                )
            else:
                entry = (
                    f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                    f"ORDER_FLOW_DEADMAN INCIDENT\n"
                    f"- can_trade=True but NO accepted live order for {idle_min} min "
                    f"(threshold {args.max_idle_s // 60} min; liveness_ts={result['liveness_ts']}; "
                    f"source={result['liveness_source']}; latest_order={result['latest_order_ts']}; "
                    f"latest_approved_suppression={result['latest_approved_suppression_ts']}; "
                    f"annotation_source={result['annotation_liveness_source']}; "
                    f"gated_quiet_status={gated_quiet.get('status')}; "
                    f"failed_gates={result['failed_gates']}).\n"
                    f"- MECHANICAL RULE: this outranks everything — next heartbeat/pulse "
                    f"drops all other work, names the choke, restores flow, and reports "
                    f"the participation table. Operator order 2026-07-07: live orders "
                    f"must flow continuously.\n"
                )
            notification_message = (
                (
                    "Measured floor deadlock for "
                    if result["status"] == "MEASURED_FLOOR_DEADLOCK"
                    else "Measured local policy skip for "
                    if result["status"] == "INCIDENT_LOCAL_POLICY_SKIP"
                    else
                    "Measured-skip gated quiet for "
                    if gated_quiet_firing and not raw_accepted_order_deadman
                    else "No live orders for "
                )
                + str(idle_min)
                + (
                    " min — floor deadlock"
                    if result["status"] == "MEASURED_FLOOR_DEADLOCK"
                    else " min — local policy skip"
                    if result["status"] == "INCIDENT_LOCAL_POLICY_SKIP"
                    else
                    " min — gated quiet"
                    if gated_quiet_firing and not raw_accepted_order_deadman
                    else " min — ORDER FLOW DEAD"
                )
                + " [" + now.strftime("%H:%M") + "Z]"
            )
            notification = _notify(
                notification_message,
                incident_class=f"ORDER_FLOW_DEADMAN_{result['status']}",
                root_path=root,
            )
            result["operator_notification"] = notification
            if notification.get("notify"):
                try:
                    with (root / args.handoff).open("a") as fh:
                        fh.write(entry)
                except OSError:
                    pass
        else:
            result["last_alert_at"] = prev.get("last_alert_at")
    elif ruled_posture_exemption:
        result["status"] = "WATCH_LOCAL_SKIP_STARVATION"
        result["deadman_class"] = "POLICY_CHOKE_LOCAL_SKIP"
        result["failed_gates"] = _failed_gates(gated_quiet)
        result["last_alert_at"] = prev.get("last_alert_at")
        result["mechanical_escalation"] = "NONE"
    elif post_boot_acceptance_path_uncertified:
        result["status"] = "UNCERTIFIED_ACCEPTANCE_PATH"
        result["deadman_class"] = "UNCERTIFIED_ACCEPTANCE_PATH"
        result["failed_gates"] = _failed_gates(gated_quiet)
        result["last_alert_at"] = prev.get("last_alert_at")
        result["acceptance_path_certification"] = {
            "status": "UNCERTIFIED_ACCEPTANCE_PATH",
            "rule": "post-boot approved suppressions certify gates only; an accepted order is required to certify submit/sign/CLOB acceptance",
            "host_boot_time": host_boot_time.isoformat() if host_boot_time else None,
            "pending_since": post_boot_accepted_order_pending_since.isoformat()
            if post_boot_accepted_order_pending_since
            else None,
            "uncertified_after_s": POST_BOOT_ACCEPTANCE_PATH_UNCERTIFIED_S,
            "latest_accepted_order_ts": latest_order.isoformat() if latest_order else None,
            "latest_approved_suppression_ts": latest_suppression.isoformat() if latest_suppression else None,
            "live_mutation": False,
        }
    # RECOVERY NOTIFICATION (operator UX, 2026-07-10): when a previously
    # red incident turns OK, say so ONCE — stacked red alerts in
    # Notification Center must not look like an ongoing incident.
    prev_status = str(prev.get("status") or "")
    if result.get("status") == "OK" and prev_status.startswith("INCIDENT_"):
        result["operator_notification_recovery"] = _notify(
            "Order flow RESTORED — accepted order "
            + (str(int(idle_s // 60)) if idle_s is not None else "?")
            + " min ago [" + now.strftime("%H:%M") + "Z]",
            incident_class=f"ORDER_FLOW_DEADMAN_{prev_status}",
            root_path=root,
            self_healed=True,
        )
    if guard_memory.get("notify_grade"):
        last_memory_alert = _parse_ts(prev.get("guard_memory_last_alert_at"))
        if last_memory_alert is None or (now - last_memory_alert).total_seconds() >= args.realert_s:
            result["guard_memory_last_alert_at"] = now.isoformat()
            auto_restart = (
                guard_memory.get("auto_restart")
                if isinstance(guard_memory.get("auto_restart"), dict)
                else {}
            )
            entry = (
                f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — "
                f"ORDER_FLOW_DEADMAN GUARD-MEMORY\n"
                f"- guard RSS {guard_memory.get('rss_gib')} GiB for pid={guard_memory.get('pid')} "
                f"(warn {guard_memory.get('warn_gib')} GiB; restart {guard_memory.get('restart_gib')} GiB; "
                f"trend={guard_memory.get('trend_gib')} GiB/{guard_memory.get('trend_window_s')}s; "
                f"auto_restart_status={auto_restart.get('status')}; returncode={auto_restart.get('returncode')}).\n"
                f"- MECHANICAL RULE: high-RSS repeat inside restart cooldown is human-visible until memory drops "
                f"or the cooldown clears; next action is preserve live flow and inspect guard memory root cause.\n"
            )
            notification = _notify(
                "Guard memory high: "
                + str(guard_memory.get("rss_gib"))
                + " GiB, restart status "
                + str(auto_restart.get("status"))
                + " ["
                + now.strftime("%H:%M")
                + "Z]",
                incident_class="ORDER_FLOW_DEADMAN_GUARD_MEMORY",
                root_path=root,
            )
            result["guard_memory_operator_notification"] = notification
            if notification.get("notify"):
                try:
                    with (root / args.handoff).open("a") as fh:
                        fh.write(entry)
                except OSError:
                    pass
        else:
            result["guard_memory_last_alert_at"] = prev.get("guard_memory_last_alert_at")
    else:
        result["guard_memory_last_alert_at"] = prev.get("guard_memory_last_alert_at")
    incident_status = str(result.get("status") or "").startswith("INCIDENT_")
    result["consecutive_incidents"] = (
        int(prev.get("consecutive_incidents") or 0) + 1 if incident_status else 0
    )
    if incident_status:
        result.update(_episode_fire_fields(prev, result))
        _append_incident_evidence(root, result)
    else:
        episode_closeout = _append_episode_closeout(root, prev, result)
        if episode_closeout is not None:
            result["episode_closeout"] = episode_closeout

    cycle_duration_s = round(time.monotonic() - cycle_started_monotonic, 6)
    _stage_checkpoint("reporting_tail")
    cycle_log_path = root / args.deadman_cycle_log
    prior_cycle_rows = _tail_jsonl(cycle_log_path, max_lines=24, tail_bytes=1024 * 1024)
    cycle_values = [
        value
        for row in prior_cycle_rows[-23:]
        if isinstance(row, dict)
        for value in [_as_float(row.get("cycle_duration_s"))]
        if value is not None
    ]
    cycle_values.append(cycle_duration_s)
    result["deadman_cycle_duration_health"] = {
        "cycle_duration_s": cycle_duration_s,
        "p50_s": _percentile(cycle_values, 0.50),
        "p95_s": _percentile(cycle_values, 0.95),
        "sample_count": len(cycle_values),
        "cadence_cut_gate_s": 75.0,
        "cadence_cut_allowed": bool(_percentile(cycle_values, 0.95) < 75.0),
        "rule": "deadman process wall time; cut cadence only when rolling p95 < 75s",
    }
    cycle_log_path.parent.mkdir(parents=True, exist_ok=True)
    with cycle_log_path.open("a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.write(
            json.dumps(
                {
                    "kind": "order_flow_deadman_cycle",
                    "checked_at": now.isoformat(),
                    "completed_at": _utc_now().isoformat(),
                    "cycle_duration_s": cycle_duration_s,
                    "stage_timers": stage_timing_rows,
                },
                sort_keys=True,
            )
            + "\n"
        )
        handle.flush()
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    actuator_admission_authority = (
        ((result.get("policy_choke") or {}).get("actuator") or {}).get(
            "admission_authority"
        )
        if isinstance(result.get("policy_choke"), dict)
        else None
    )
    admission_authority = (
        actuator_admission_authority
        if isinstance(actuator_admission_authority, dict)
        and actuator_admission_authority.get("checked_at") == now.isoformat()
        else early_direct_admission_authority
    )
    if (
        isinstance(admission_authority, dict)
        and admission_authority.get("checked_at") == now.isoformat()
        and isinstance(admission_authority.get("admission_interval_read"), dict)
    ):
        interval_row = {
            "kind": "order_flow_deadman_admission_interval",
            "checked_at": now.isoformat(),
            "wallet": admission_authority.get("wallet"),
            "authorized": admission_authority.get("authorized"),
            **admission_authority["admission_interval_read"],
        }
        interval_log_path = root / args.admission_interval_log
        interval_log_path.parent.mkdir(parents=True, exist_ok=True)
        with interval_log_path.open("a") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.write(json.dumps(interval_row, sort_keys=True) + "\n")
            handle.flush()
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    final_publish_now = _utc_now()
    final_policy_choke = result.get("policy_choke")
    if isinstance(final_policy_choke, dict):
        final_actuator = final_policy_choke.get("actuator")
        if isinstance(final_actuator, dict) and isinstance(
            final_actuator.get("admission_authority"), dict
        ):
            final_actuator["admission_authority"] = _admission_authority_for_publish(
                final_actuator["admission_authority"],
                published_at=final_publish_now,
            )
    if isinstance(result.get("early_admission_authority"), dict):
        result["early_admission_authority"] = _admission_authority_for_publish(
            result["early_admission_authority"],
            published_at=final_publish_now,
        )
    result["final_published_at"] = final_publish_now.isoformat()
    result["body_checked_at"] = result.get("checked_at")
    final_audit_authority = (
        (((result.get("policy_choke") or {}).get("actuator") or {}).get("admission_authority"))
        if isinstance(result.get("policy_choke"), dict)
        else None
    )
    if not isinstance(final_audit_authority, dict):
        final_audit_authority = result.get("early_admission_authority")
    if isinstance(final_audit_authority, dict):
        final_checked_at = _parse_ts(final_audit_authority.get("checked_at")) or now
        _append_admission_publish_audit(
            state_path,
            published=final_audit_authority,
            checked_at=final_checked_at,
            published_at=final_publish_now,
            stage="final",
            cycle_elapsed_s=time.monotonic() - cycle_started_monotonic,
        )
    atomic_write_json(state_path, result)
    print(
        json.dumps(
            {
                k: result.get(k)
                for k in (
                    "checked_at",
                    "status",
                    "money_anchored_status",
                    "utc_day_money",
                    "idle_s",
                    "can_trade",
                    "liveness_source",
                    "liveness_ts",
                    "eligible_drought_s",
                    "eligible_drought_status",
                    "fresh_stale_signal_rows",
                    "failed_gates",
                    "deadman_warning",
                    "benign_skip_overrode",
                    "guard_memory_status",
                    "guard_memory_rss_gib",
                    "guard_memory_auto_restart_status",
                )
            }
            | {
                "policy_choke": _brain_policy_choke(result.get("policy_choke")),
                "guard_memory_status": guard_memory.get("status"),
                "guard_memory_rss_gib": guard_memory.get("rss_gib"),
                "guard_memory_auto_restart_status": (
                    guard_memory.get("auto_restart", {}).get("status")
                    if isinstance(guard_memory.get("auto_restart"), dict)
                    else None
                ),
            }
        )
    )
    return 0


def _record_lock_skip(root: Path, lock_path: Path) -> dict[str, Any]:
    """Count a non-blocking singleton refusal without touching shared state."""

    event = {
        "kind": "order_flow_deadman_lock_skip",
        "status": "SKIPPED_LOCK_HELD",
        "checked_at": _utc_now().isoformat(),
        "pid": os.getpid(),
        "lock_path": str(lock_path),
    }
    skip_log = root / DEFAULT_LOCK_SKIP_LOG
    skip_log.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(skip_log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, (json.dumps(event, sort_keys=True) + "\n").encode("utf-8"))
    finally:
        os.close(fd)
    print(json.dumps(event, sort_keys=True))
    return event


def main(argv: list[str] | None = None) -> int:
    """Run one deadman pass, or count and skip when another writer is active."""

    lock_parser = argparse.ArgumentParser(add_help=False)
    lock_parser.add_argument("--root", default=".")
    lock_parser.add_argument("--lock", default=DEFAULT_LOCK)
    lock_args, _ = lock_parser.parse_known_args(argv)
    root = Path(lock_args.root).resolve()
    lock_path = root / lock_args.lock
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _record_lock_skip(root, lock_path)
            return 0
        return _main(argv)
    finally:
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
