#!/usr/bin/env python3
"""Build the PROMOTE/ROTATE decision state from live and paper evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.promotion_rotation import (  # noqa: E402
    PromotionRotationConfig,
    build_promotion_rotation_state,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def _num(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _profile_dict(*sources: dict) -> dict:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in ("execution_profile", "copyability_profile"):
            profile = source.get(key)
            if isinstance(profile, dict) and profile:
                return profile
    return {}


def _profile_fill_sample(profile: dict, *sources: dict) -> int:
    values = [profile.get("fill_sample") if isinstance(profile, dict) else None]
    for source in sources:
        if not isinstance(source, dict):
            continue
        values.extend(
            [
                source.get("copyability_profile_fill_sample"),
                source.get("fill_sample"),
            ]
        )
    for value in values:
        try:
            sample = int(value)
        except (TypeError, ValueError):
            continue
        if sample > 0:
            return sample
    return 0


def _profile_with_sample(profile: dict, fill_sample: int) -> dict:
    output = dict(profile) if isinstance(profile, dict) else {}
    output["fill_sample"] = int(fill_sample)
    return output


def _candidate_evidence_bar(
    *,
    copyable_events: int,
    max_recent_ask_depth_usd: float,
    fill_sample: int,
    config: PromotionRotationConfig,
    min_copyable_buy_events: int | None = None,
) -> dict:
    required_copyable = int(
        config.min_candidate_copyable_buy_events
        if min_copyable_buy_events is None
        else min_copyable_buy_events
    )
    blockers: list[str] = []
    if int(copyable_events) < required_copyable:
        blockers.append("copyable_buy_events_below_minimum")
    if float(max_recent_ask_depth_usd) <= 0.0:
        blockers.append("max_recent_ask_depth_usd_missing_or_zero")
    if int(fill_sample) <= 0:
        blockers.append("copyability_profile_fill_sample_missing")
    return {
        "status": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
        "min_candidate_copyable_buy_events": required_copyable,
        "copyable_buy_events": int(copyable_events),
        "max_recent_ask_depth_usd": round(float(max_recent_ask_depth_usd), 6),
        "copyability_profile_fill_sample": int(fill_sample),
    }


def _discover_rows(discover_payload: dict, config: PromotionRotationConfig) -> tuple[list[dict], dict]:
    rows: list[dict] = []
    candidates = discover_payload.get("candidates") if isinstance(discover_payload.get("candidates"), list) else []
    complete = 0
    promotable = 0
    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            continue
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        status = str(replay.get("status") or "").upper()
        eligibility_status = str(replay.get("eligibility_status") or status).upper()
        if status not in {"COMPLETE", "PASS"}:
            continue
        complete += 1
        wallet = str(candidate.get("wallet") or "").lower()
        policy_id = str(
            replay.get("policy_id")
            or "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
        )
        paper_pnl = _num(replay.get("paper_pnl_usd"), _num(replay.get("realized_pnl_usd")))
        copyable_events = int(
            replay.get("copyable_buy_events")
            or replay.get("paper_orders")
            or replay.get("orders")
            or candidate.get("matched_buy_events")
            or 0
        )
        clob_backed_orders = int(
            replay.get("candidate_clob_backed_orders")
            or replay.get("clob_backed_orders")
            or 0
        )
        max_depth = _num(replay.get("max_recent_ask_depth_usd"), _num(candidate.get("max_recent_ask_depth_usd")))
        profile = _profile_dict(candidate, replay)
        fill_sample = _profile_fill_sample(profile, candidate, replay)
        evidence_bar = _candidate_evidence_bar(
            copyable_events=copyable_events,
            max_recent_ask_depth_usd=max_depth,
            fill_sample=fill_sample,
            config=config,
        )
        eligible = (
            eligibility_status == "PASS"
            and paper_pnl > float(config.min_candidate_paper_pnl_usd)
            and copyable_events >= int(config.min_candidate_copyable_buy_events)
            and clob_backed_orders > 0
            and evidence_bar["status"] == "PASS"
        )
        if eligible:
            promotable += 1
        rows.append(
            {
                "rank": 10_000 + index,
                "wallet": wallet,
                "wallet_name": candidate.get("candidate_id") or "",
                "live_executable_paper_eligible": eligible,
                "paper_eligible_policy_ids": [policy_id] if eligible else [],
                "copyable_buy_events": copyable_events,
                "recent_copy_sized_buy_events": int(candidate.get("matched_buy_events") or copyable_events),
                "recent_liquid_copy_sized_buy_events": clob_backed_orders,
                "paper_pnl_usd": round(paper_pnl, 6),
                "max_recent_ask_depth_usd": round(max_depth, 6),
                "copyability_profile_gate_enabled": True,
                "copyability_profile_eligible": evidence_bar["status"] == "PASS",
                "execution_profile": _profile_with_sample(profile, fill_sample),
                "copyability_profile": _profile_with_sample(profile, fill_sample),
                "candidate_evidence_bar": evidence_bar,
                "paper_policy_gate": {
                    "eligible_policy_ids": [policy_id] if eligible else [],
                    "best_policy_id": policy_id,
                    "best_policy_paper_pnl_usd": round(paper_pnl, 6),
                    "best_policy_copyable_buy_events": copyable_events,
                    "candidate_clob_backed_orders": clob_backed_orders,
                    "max_recent_ask_depth_usd": round(max_depth, 6),
                    "copyability_profile_fill_sample": fill_sample,
                    "candidate_evidence_bar": evidence_bar,
                    "discover_replay_status": status,
                    "discover_replay_eligibility_status": eligibility_status,
                },
                "evidence_source": "discover_live_band_candidates",
                "discover_candidate_id": candidate.get("candidate_id") or "",
            }
        )
    return rows, {
        "path_kind": discover_payload.get("kind"),
        "candidate_count": int(discover_payload.get("candidate_count") or len(candidates)),
        "complete_replays": complete,
        "promotable_replays": promotable,
    }


def _candidate_index(discover_payload: dict) -> dict[str, dict]:
    candidates = discover_payload.get("candidates") if isinstance(discover_payload.get("candidates"), list) else []
    index: dict[str, dict] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("candidate_id") or "").strip()
        if candidate_id:
            index[candidate_id] = candidate
    return index


def _bookcovered_rows(
    bookcovered_payload: list,
    discover_payload: dict,
    config: PromotionRotationConfig,
) -> tuple[list[dict], dict]:
    rows: list[dict] = []
    candidate_lookup = _candidate_index(discover_payload)
    considered = 0
    promotable = 0
    for index, item in enumerate(bookcovered_payload, start=1):
        if not isinstance(item, dict):
            continue
        considered += 1
        candidate_id = str(item.get("candidate_id") or "").strip()
        candidate = candidate_lookup.get(candidate_id, {})
        wallet = str(item.get("wallet") or candidate.get("wallet") or "").lower()
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        policy_id = str(
            item.get("policy_id")
            or replay.get("policy_id")
            or "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
        )
        status = str(item.get("status") or "").upper()
        paper_pnl = _num(item.get("paper_pnl_usd"), _num(replay.get("paper_pnl_usd")))
        copyable_events = int(item.get("copyable_buy_events") or replay.get("copyable_buy_events") or 0)
        clob_backed_orders = int(
            item.get("candidate_clob_backed_orders")
            or item.get("clob_backed_orders")
            or replay.get("candidate_clob_backed_orders")
            or copyable_events
            or 0
        )
        max_depth = _num(
            item.get("max_recent_ask_depth_usd"),
            _num(replay.get("max_recent_ask_depth_usd"), _num(candidate.get("max_recent_ask_depth_usd"))),
        )
        profile = _profile_dict(item, candidate, replay)
        fill_sample = _profile_fill_sample(profile, item, candidate, replay)
        evidence_bar = _candidate_evidence_bar(
            copyable_events=copyable_events,
            max_recent_ask_depth_usd=max_depth,
            fill_sample=fill_sample,
            config=config,
        )
        rejected_fill_ratio = _num(item.get("rejected_fill_ratio"), _num(replay.get("candidate_rejected_fill_ratio")))
        eligible = (
            status == "PASS_BOOKCOVERED"
            and paper_pnl > float(config.min_candidate_paper_pnl_usd)
            and copyable_events >= int(config.min_candidate_copyable_buy_events)
            and clob_backed_orders > 0
            and bool(wallet)
            and evidence_bar["status"] == "PASS"
        )
        if eligible:
            promotable += 1
        rows.append(
            {
                "rank": 9_000 + index,
                "wallet": wallet,
                "wallet_name": candidate_id,
                "live_executable_paper_eligible": eligible,
                "paper_eligible_policy_ids": [policy_id] if eligible else [],
                "copyable_buy_events": copyable_events,
                "recent_copy_sized_buy_events": int(candidate.get("matched_buy_events") or copyable_events),
                "recent_liquid_copy_sized_buy_events": clob_backed_orders,
                "paper_pnl_usd": round(paper_pnl, 6),
                "max_recent_ask_depth_usd": round(max_depth, 6),
                "copyability_profile_gate_enabled": True,
                "copyability_profile_eligible": evidence_bar["status"] == "PASS",
                "execution_profile": _profile_with_sample(profile, fill_sample),
                "copyability_profile": _profile_with_sample(profile, fill_sample),
                "candidate_evidence_bar": evidence_bar,
                "paper_policy_gate": {
                    "eligible_policy_ids": [policy_id] if eligible else [],
                    "best_policy_id": policy_id,
                    "best_policy_paper_pnl_usd": round(paper_pnl, 6),
                    "best_policy_copyable_buy_events": copyable_events,
                    "candidate_clob_backed_orders": clob_backed_orders,
                    "max_recent_ask_depth_usd": round(max_depth, 6),
                    "copyability_profile_fill_sample": fill_sample,
                    "candidate_evidence_bar": evidence_bar,
                    "bookcovered_status": status,
                    "bookcovered_rejected_fill_ratio": round(rejected_fill_ratio, 6),
                },
                "evidence_source": "discover_live_band_bookcovered_summary",
                "discover_candidate_id": candidate_id,
            }
        )
    return rows, {
        "path_kind": "discover_live_band_bookcovered_summary",
        "candidate_count": considered,
        "promotable_replays": promotable,
    }


def _merge_discover_candidates(
    lane_state: dict,
    discover_payload: dict,
    config: PromotionRotationConfig,
) -> dict:
    merged = dict(lane_state)
    existing = merged.get("ranked_wallets") if isinstance(merged.get("ranked_wallets"), list) else []
    rows, summary = _discover_rows(discover_payload, config)
    merged["ranked_wallets"] = [*existing, *rows]
    merged["discover_candidates_summary"] = summary
    blockers = [str(item) for item in (merged.get("blockers") or []) if item]
    if summary["candidate_count"] and summary["complete_replays"] <= 0:
        blockers.append("discover_candidate_replays_incomplete")
    elif summary["complete_replays"] and summary["promotable_replays"] <= 0:
        blockers.append("discover_candidate_replays_not_promotable")
    merged["blockers"] = sorted(set(blockers))
    if not merged.get("status"):
        merged["status"] = "ANALYZE"
    return merged


def _merge_bookcovered_candidates(
    lane_state: dict,
    bookcovered_payload: list,
    discover_payload: dict,
    config: PromotionRotationConfig,
) -> dict:
    merged = dict(lane_state)
    existing = merged.get("ranked_wallets") if isinstance(merged.get("ranked_wallets"), list) else []
    rows, summary = _bookcovered_rows(bookcovered_payload, discover_payload, config)
    merged["ranked_wallets"] = [*existing, *rows]
    merged["bookcovered_candidates_summary"] = summary
    blockers = [str(item) for item in (merged.get("blockers") or []) if item]
    if summary["candidate_count"] and summary["promotable_replays"] <= 0:
        blockers.append("bookcovered_candidate_replays_not_promotable")
    merged["blockers"] = sorted(set(blockers))
    if not merged.get("status"):
        merged["status"] = "ANALYZE"
    return merged


def _active_wallets_from_guard(guard_payload: dict) -> set[str]:
    active_set = guard_payload.get("active_set") if isinstance(guard_payload.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    wallets: set[str] = set()
    for member in members:
        if not isinstance(member, dict):
            continue
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
        if wallet:
            wallets.add(wallet)
    return wallets


def _clearance_rows(
    clearance_payload: dict,
    config: PromotionRotationConfig,
    *,
    active_wallets: set[str] | None = None,
    current_queue_ready_wallets: set[str] | None = None,
) -> tuple[list[dict], dict]:
    rows: list[dict] = []
    candidates = (
        clearance_payload.get("candidates") if isinstance(clearance_payload.get("candidates"), list) else []
    )
    clear = 0
    promotable = 0
    skipped_active = 0
    active = active_wallets or set()
    current_ready = current_queue_ready_wallets
    for index, item in enumerate(candidates, start=1):
        if not isinstance(item, dict):
            continue
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        coverage = item.get("window_coverage") if isinstance(item.get("window_coverage"), dict) else {}
        wallet = str(item.get("wallet") or "").lower()
        if wallet and wallet in active:
            skipped_active += 1
            continue
        classification = str(item.get("classification") or item.get("clearance_status") or "").upper()
        historical_ready = bool(item.get("ready_for_live"))
        ready = historical_ready and (
            current_ready is None or wallet in current_ready
        )
        paper_pnl = _num(metrics.get("paper_pnl_usd"))
        copyable_events = int(metrics.get("copyable_buy_events") or 0)
        clob_backed_orders = int(metrics.get("candidate_clob_backed_orders") or metrics.get("filled_orders") or 0)
        resolved_orders = int(metrics.get("resolved_orders") or 0)
        unresolved_filled = int(metrics.get("unresolved_filled_order_count") or 0)
        missing_resolution_markets = int(coverage.get("missing_resolution_market_count") or 0)
        max_depth = _num(metrics.get("max_recent_ask_depth_usd"), _num(item.get("max_recent_ask_depth_usd")))
        profile = _profile_dict(item, metrics)
        fill_sample = max(
            _profile_fill_sample(profile, item, metrics),
            resolved_orders,
            clob_backed_orders,
        )
        clearance_ready = ready and classification == "CLEAR"
        clob_floor_passed = clob_backed_orders >= int(config.min_candidate_copyable_buy_events)
        clearance_required_copyable = (
            min(int(config.min_candidate_copyable_buy_events), max(1, copyable_events))
            if clearance_ready
            else int(config.min_candidate_copyable_buy_events)
        )
        evidence_depth = (
            max_depth
            if max_depth > 0.0
            else float(clob_backed_orders if (clob_floor_passed or (clearance_ready and clob_backed_orders > 0)) else 0)
        )
        evidence_bar = _candidate_evidence_bar(
            copyable_events=copyable_events,
            max_recent_ask_depth_usd=evidence_depth,
            fill_sample=fill_sample,
            config=config,
            min_copyable_buy_events=clearance_required_copyable,
        )
        if classification == "CLEAR":
            clear += 1
        eligible = (
            ready
            and classification == "CLEAR"
            and bool(wallet)
            and paper_pnl > float(config.min_candidate_paper_pnl_usd)
            and (copyable_events >= int(config.min_candidate_copyable_buy_events) or clearance_ready)
            and clob_backed_orders > 0
            and resolved_orders > 0
            and unresolved_filled == 0
            and missing_resolution_markets == 0
            and evidence_bar["status"] == "PASS"
        )
        if eligible:
            promotable += 1
        policy_id = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
        queue_rank = int(item.get("queue_rank") or index)
        rows.append(
            {
                "rank": queue_rank,
                "wallet": wallet,
                "wallet_name": f"clearance_rank_{queue_rank}",
                "live_executable_paper_eligible": eligible,
                "ready_for_live": ready,
                "paper_eligible_policy_ids": [policy_id] if eligible else [],
                "copyable_buy_events": copyable_events,
                "recent_copy_sized_buy_events": copyable_events,
                "recent_liquid_copy_sized_buy_events": clob_backed_orders,
                "paper_pnl_usd": round(paper_pnl, 6),
                "max_recent_ask_depth_usd": round(evidence_depth, 6),
                "copyability_profile_gate_enabled": True,
                "copyability_profile_eligible": evidence_bar["status"] == "PASS",
                "execution_profile": _profile_with_sample(profile, fill_sample),
                "copyability_profile": _profile_with_sample(profile, fill_sample),
                "candidate_evidence_bar": evidence_bar,
                "paper_policy_gate": {
                    "eligible_policy_ids": [policy_id] if eligible else [],
                    "best_policy_id": policy_id,
                    "best_policy_paper_pnl_usd": round(paper_pnl, 6),
                    "best_policy_copyable_buy_events": copyable_events,
                    "candidate_clob_backed_orders": clob_backed_orders,
                    "max_recent_ask_depth_usd": round(evidence_depth, 6),
                    "copyability_profile_fill_sample": fill_sample,
                    "candidate_evidence_bar": evidence_bar,
                    "clearance_ready_duplicate_copyable_floor_bypass": bool(
                        clearance_ready and copyable_events < int(config.min_candidate_copyable_buy_events)
                    ),
                    "resolved_orders": resolved_orders,
                    "unresolved_filled_order_count": unresolved_filled,
                    "missing_resolution_market_count": missing_resolution_markets,
                    "clearance_status": classification,
                    "ready_for_live": ready,
                    "historical_clearance_ready_for_live": historical_ready,
                    "current_queue_ready_for_live": ready,
                },
                "evidence_source": "wallet_copy_queue_clearance_gaps",
            }
        )
    return rows, {
        "path_kind": clearance_payload.get("kind"),
        "candidate_count": len(candidates),
        "clear_candidates": clear,
        "promotable_replays": promotable,
        "skipped_active_candidates": skipped_active,
    }


def _merge_clearance_candidates(
    lane_state: dict,
    clearance_payload: dict,
    config: PromotionRotationConfig,
    *,
    active_wallets: set[str] | None = None,
    current_queue_ready_wallets: set[str] | None = None,
) -> dict:
    merged = dict(lane_state)
    existing = merged.get("ranked_wallets") if isinstance(merged.get("ranked_wallets"), list) else []
    rows, summary = _clearance_rows(
        clearance_payload,
        config,
        active_wallets=active_wallets,
        current_queue_ready_wallets=current_queue_ready_wallets,
    )
    merged["ranked_wallets"] = [*existing, *rows]
    merged["clearance_candidates_summary"] = summary
    blockers = [str(item) for item in (merged.get("blockers") or []) if item]
    if summary["candidate_count"] and summary["promotable_replays"] <= 0:
        blockers.append("clearance_candidates_not_promotable")
    merged["blockers"] = sorted(set(blockers))
    if not merged.get("status"):
        merged["status"] = "ANALYZE"
    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-fill-report", default="data/research/wallet_copy_live_fill_quality_report.json")
    parser.add_argument("--live-execution-state", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument(
        "--paper-lane-state",
        default="data/research/wallet_copy_top10_broad_paper_lane_state.json",
        help="OBSERVE top-10 broad paper lane state with paper eligibility gates.",
    )
    parser.add_argument(
        "--discover-candidates",
        default="",
        help="DISCOVER live-band candidate artifact with completed paper_replay metrics.",
    )
    parser.add_argument(
        "--bookcovered-summary",
        default="",
        help="Book-covered near-miss summary rows to ingest as Fable-pinned promotion evidence.",
    )
    parser.add_argument(
        "--clearance-gaps",
        default="data/research/wallet_copy_queue_clearance_gaps.json",
        help="PROMOTE clearance lane artifact with CLEAR ready_for_live candidates.",
    )
    parser.add_argument(
        "--live-guard-state",
        default="data/research/wallet_copy_live_guard_state.json",
        help="Current live guard state; used to skip already-active clearance candidates.",
    )
    parser.add_argument(
        "--member-queue",
        default="data/research/wallet_copy_full_pool_member_queue.json",
        help="Canonical current member queue; rotation destinations must be ready_for_live here.",
    )
    parser.add_argument(
        "--order-flow-deadman-state",
        default="data/research/order_flow_deadman_state.json",
        help="Order-flow deadman state; pauses tripwire clocks while order flow is red.",
    )
    parser.add_argument(
        "--dow-profile-state",
        default="data/research/member_dow_profiles_latest.json",
        help="Calendar-aware member DOW profile artifact for inactivity-clock evidence.",
    )
    parser.add_argument("--output", default="data/research/wallet_copy_promotion_rotation_state.json")
    parser.add_argument("--live-max-buy-price", type=float, default=0.50)
    parser.add_argument("--min-live-resolved-fills", type=int, default=10)
    parser.add_argument("--min-live-fill-rate-pct", type=float, default=40.0)
    parser.add_argument("--min-candidate-paper-pnl-usd", type=float, default=0.0)
    parser.add_argument("--min-candidate-copyable-buy-events", type=int, default=20)
    parser.add_argument("--max-candidates", type=int, default=5)
    parser.add_argument("--inactivity-rotation-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--live-inactivity-rotation-threshold-s", type=float, default=3 * 60 * 60)
    parser.add_argument("--alternate-activity-window-s", type=float, default=3 * 60 * 60)
    parser.add_argument("--min-alternate-recent-buy-events", type=int, default=1)
    parser.add_argument(
        "--require-leak-rule-1-2-before-rotation-application",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--live-our-fill-pnl-outranks-paper-for-retention",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--pause-tripwire-clocks-while-deadman-red-or-unsubmittable",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = PromotionRotationConfig(
        live_max_buy_price=float(args.live_max_buy_price),
        min_live_resolved_fills=int(args.min_live_resolved_fills),
        min_live_fill_rate_pct=float(args.min_live_fill_rate_pct),
        min_candidate_paper_pnl_usd=float(args.min_candidate_paper_pnl_usd),
        min_candidate_copyable_buy_events=int(args.min_candidate_copyable_buy_events),
        max_candidates=int(args.max_candidates),
        inactivity_rotation_enabled=bool(args.inactivity_rotation_enabled),
        live_inactivity_rotation_threshold_s=float(args.live_inactivity_rotation_threshold_s),
        alternate_activity_window_s=float(args.alternate_activity_window_s),
        min_alternate_recent_buy_events=int(args.min_alternate_recent_buy_events),
        require_leak_rule_1_2_before_rotation_application=bool(
            args.require_leak_rule_1_2_before_rotation_application
        ),
        live_our_fill_pnl_outranks_paper_for_retention=bool(
            args.live_our_fill_pnl_outranks_paper_for_retention
        ),
        pause_tripwire_clocks_while_deadman_red_or_unsubmittable=bool(
            args.pause_tripwire_clocks_while_deadman_red_or_unsubmittable
        ),
    )
    live_fill_report = load_json(args.live_fill_report, default={})
    live_execution_state = load_json(args.live_execution_state, default={})
    lane_state = load_json(args.paper_lane_state, default={})
    discover_payload = load_json(args.discover_candidates, default={}) if args.discover_candidates else {}
    bookcovered_payload = load_json(args.bookcovered_summary, default=[]) if args.bookcovered_summary else []
    clearance_payload = load_json(args.clearance_gaps, default={}) if args.clearance_gaps else {}
    guard_payload = load_json(args.live_guard_state, default={}) if args.live_guard_state else {}
    member_queue_payload = load_json(args.member_queue, default={}) if args.member_queue else {}
    deadman_payload = load_json(args.order_flow_deadman_state, default={}) if args.order_flow_deadman_state else {}
    dow_profile_payload = load_json(args.dow_profile_state, default={}) if args.dow_profile_state else {}
    if isinstance(lane_state, dict) and isinstance(discover_payload, dict) and discover_payload:
        lane_state = _merge_discover_candidates(lane_state, discover_payload, config)
    if isinstance(lane_state, dict) and isinstance(bookcovered_payload, list) and bookcovered_payload:
        lane_state = _merge_bookcovered_candidates(
            lane_state,
            bookcovered_payload,
            discover_payload if isinstance(discover_payload, dict) else {},
            config,
        )
    if isinstance(lane_state, dict) and isinstance(clearance_payload, dict) and clearance_payload:
        active_wallets = _active_wallets_from_guard(guard_payload if isinstance(guard_payload, dict) else {})
        current_queue_ready_wallets = {
            str(row.get("wallet") or "").lower()
            for row in (
                member_queue_payload.get("ranked_members")
                if isinstance(member_queue_payload, dict)
                and isinstance(member_queue_payload.get("ranked_members"), list)
                else []
            )
            if isinstance(row, dict) and row.get("ready_for_live") is True
        }
        lane_state = _merge_clearance_candidates(
            lane_state,
            clearance_payload,
            config,
            active_wallets=active_wallets,
            current_queue_ready_wallets=current_queue_ready_wallets,
        )
    payload = build_promotion_rotation_state(
        live_fill_report=live_fill_report if isinstance(live_fill_report, dict) else {},
        live_execution_state=live_execution_state if isinstance(live_execution_state, dict) else {},
        lane_state=lane_state if isinstance(lane_state, dict) else {},
        order_flow_deadman_state=deadman_payload if isinstance(deadman_payload, dict) else {},
        dow_profile_state=dow_profile_payload if isinstance(dow_profile_payload, dict) else {},
        config=config,
    )
    if isinstance(lane_state, dict) and isinstance(lane_state.get("discover_candidates_summary"), dict):
        payload["discover_candidates_summary"] = lane_state["discover_candidates_summary"]
    if isinstance(lane_state, dict) and isinstance(lane_state.get("bookcovered_candidates_summary"), dict):
        payload["bookcovered_candidates_summary"] = lane_state["bookcovered_candidates_summary"]
    if isinstance(lane_state, dict) and isinstance(lane_state.get("clearance_candidates_summary"), dict):
        payload["clearance_candidates_summary"] = lane_state["clearance_candidates_summary"]
    replay_summary = discover_payload.get("replay_summary") if isinstance(discover_payload.get("replay_summary"), dict) else {}
    if replay_summary.get("near_miss_replays"):
        payload["discover_near_miss_replays"] = replay_summary.get("near_miss_replays")
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["decision"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
