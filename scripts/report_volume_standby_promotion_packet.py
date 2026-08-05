#!/usr/bin/env python3
"""Build the exact-policy, paper-only promotion packet for a standby wallet."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

VOLUME_DECISION_HOURS = 72.0
PRECONDITION_INPUT_FRESHNESS_H = 1.0
TEMPORAL_EVIDENCE_CLASSIFICATIONS = {
    "CONTINUOUS",
    "WEEKDAY-ONLY",
    "WEEKEND-ONLY",
    "BAND-SPECIALIST",
    "FADING",
}


def _parse_iso(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _age_h(value: Any, *, now: datetime | None) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None or now is None:
        return None
    return round(max(0.0, (now - parsed).total_seconds() / 3600.0), 6)


def build_packet(
    state: dict[str, Any],
    *,
    wallet: str,
    clearance_packet: dict[str, Any] | None = None,
    previous_packet: dict[str, Any] | None = None,
    terminal_decision: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or utc_now_iso()
    wallet_lc = wallet.lower()
    lane = next(
        (
            row
            for row in state.get("lanes") or []
            if isinstance(row, dict) and str(row.get("wallet") or "").lower() == wallet_lc
        ),
        {},
    )
    orders = int(lane.get("paper_orders") or 0)
    copyable = int(lane.get("copyable_buy_events") or 0)
    resolved = int(lane.get("resolved_paper_fills") or 0)
    pnl = float(lane.get("paper_pnl_usd") or 0.0)
    source_liveness = lane.get("source_liveness") if isinstance(lane.get("source_liveness"), dict) else {}
    policy_id = str(lane.get("paper_policy_id") or lane.get("copy_policy_family") or "")
    parity = lane.get("copyintent_parity_capture") if isinstance(lane.get("copyintent_parity_capture"), dict) else {}
    clearance = clearance_packet or {}
    fee_shadow = (
        clearance.get("exact_policy_post_fee_shadow")
        if isinstance(clearance.get("exact_policy_post_fee_shadow"), dict)
        else {}
    )
    clearance_policy = (
        clearance.get("exact_policy")
        if isinstance(clearance.get("exact_policy"), dict)
        else {}
    )
    post_fee_pnl = fee_shadow.get("post_fee_pnl_usd")
    post_fee_pnl = float(post_fee_pnl) if post_fee_pnl is not None else None
    clearance_pre_fee = fee_shadow.get("pre_fee_pnl_usd")
    clearance_pre_fee = float(clearance_pre_fee) if clearance_pre_fee is not None else None
    clearance_resolved = fee_shadow.get("resolved_orders")
    clearance_resolved = int(clearance_resolved) if clearance_resolved is not None else None
    divergence_checks = {
        "exact_policy_post_fee_packet_present": bool(fee_shadow),
        "wallet_match": str(clearance.get("wallet") or "").lower() == wallet_lc,
        "policy_match": str(clearance_policy.get("policy_id") or "") == policy_id,
        "pre_fee_pnl_match": clearance_pre_fee is not None and abs(clearance_pre_fee - pnl) <= 0.000001,
        "resolved_fill_count_match": clearance_resolved == resolved,
        "post_fee_not_above_pre_fee": (
            post_fee_pnl is not None
            and clearance_pre_fee is not None
            and post_fee_pnl <= clearance_pre_fee
        ),
    }
    divergence_clear = all(divergence_checks.values())
    enrolled_at = _parse_iso(lane.get("paper_canary_enrolled_at"))
    generated_dt = _parse_iso(generated_at)
    decision_at = enrolled_at + timedelta(hours=VOLUME_DECISION_HOURS) if enrolled_at else None
    elapsed_h = (
        max(0.0, (generated_dt - enrolled_at).total_seconds() / 3600.0)
        if generated_dt and enrolled_at
        else None
    )
    decision_due = bool(generated_dt and decision_at and generated_dt >= decision_at)
    preconditions = (
        lane.get("live_canary_packet_preconditions")
        if isinstance(lane.get("live_canary_packet_preconditions"), dict)
        else {}
    )
    precondition_evidence = (
        lane.get("live_canary_precondition_evidence")
        if isinstance(lane.get("live_canary_precondition_evidence"), dict)
        else {}
    )
    fading_evidence = precondition_evidence.get("fading") if isinstance(precondition_evidence.get("fading"), dict) else {}
    external_evidence = (
        precondition_evidence.get("external_liveness")
        if isinstance(precondition_evidence.get("external_liveness"), dict)
        else {}
    )
    defense_evidence = precondition_evidence.get("defense") if isinstance(precondition_evidence.get("defense"), dict) else {}
    freshness_ages_h = {
        "ready_shadow_state": _age_h(state.get("generated_at"), now=generated_dt),
        "temporal_profitability": _age_h(fading_evidence.get("generated_at"), now=generated_dt),
        "external_liveness_probe": _age_h(external_evidence.get("probe_observed_at"), now=generated_dt),
        "defense_scorecard": _age_h(defense_evidence.get("scorecard_generated_at"), now=generated_dt),
    }
    freshness_checks = {
        key: age is not None and age <= PRECONDITION_INPUT_FRESHNESS_H
        for key, age in freshness_ages_h.items()
    }
    temporal_classification = str(fading_evidence.get("classification") or "").upper()
    freshness_checks["temporal_wallet_classification_present"] = (
        temporal_classification in TEMPORAL_EVIDENCE_CLASSIFICATIONS
    )
    if defense_evidence.get("utc_release_due") is True:
        freshness_checks["defense_scorecard"] = True
    fresh_inputs_ready = all(freshness_checks.values())
    mechanical_inputs = {
        "clock_elapsed_h_gte_72": bool(elapsed_h is not None and elapsed_h >= VOLUME_DECISION_HOURS),
        "cumulative_would_pnl_post_fee_positive": post_fee_pnl is not None and post_fee_pnl > 0.0,
        "copyable_buy_floor_met": copyable >= int(lane.get("promotion_copyable_buy_gate") or 20),
        "divergence_review_clear": divergence_clear,
        "exact_policy_match": bool(policy_id) and policy_id == str(parity.get("policy_id") or ""),
        "copyintent_parity_capture_armed": lane.get("copyintent_parity_capture_armed") is True,
        "full_utc_day_complete": lane.get("ready_shadow_full_utc_day") is True,
        "hot_standby_ready": preconditions.get("hot_standby_ready") is True,
        "source_fresh": preconditions.get("fresh_external_btc5m_lt_24h") is True,
        "fading_clear": preconditions.get("fading_clear") is True,
        "defense_clear": preconditions.get("defense_not_in_triggered_rung") is True,
    }
    branch_inputs_pass = all(mechanical_inputs.values())
    previous_terminal = str((previous_packet or {}).get("terminal_decision") or "")
    terminal_decision = str(terminal_decision or previous_terminal or "")
    current_branch = (
        "ACCRUE_PAPER_ONLY_NO_LIVE_MUTATION"
        if not decision_due
        else "DEFER_VOLUME_DECISION_FRESH_INPUT_REQUIRED"
        if not fresh_inputs_ready
        else "PROMOTE_PACKET_READY_FOR_FABLE_LIVE_DECISION"
        if branch_inputs_pass
        else "PARK_VOLUME_STANDBY_PAPER_ONLY"
    )
    if terminal_decision == "PARK_VOLUME_STANDBY_PAPER_ONLY":
        current_branch = terminal_decision
    failures = [
        key
        for key, passed in (lane.get("live_canary_packet_preconditions") or {}).items()
        if passed is False
    ]
    decision = (
        "FABLE_PROMOTION_DECISION_REQUIRED"
        if decision_due and fresh_inputs_ready and branch_inputs_pass
        else "FRESH_PRECONDITION_INPUTS_REQUIRED"
        if decision_due and not fresh_inputs_ready
        else "PARK_VOLUME_STANDBY_PAPER_ONLY"
        if decision_due
        else "FRESH_EXPIRY_READ_REQUIRED"
    )
    if terminal_decision == "PARK_VOLUME_STANDBY_PAPER_ONLY":
        decision = terminal_decision
    return {
        "kind": "volume_standby_exact_policy_promotion_packet",
        "schema_version": 1,
        "flow_stage": "PROMOTE/LEARN/OBSERVE",
        "generated_at": generated_at,
        "wallet": wallet_lc,
        "measurement_only": True,
        "paper_only": True,
        "live_mutation_allowed": False,
        "exact_policy": {
            "policy_id": policy_id,
            "parity": parity,
            "policy_match": bool(policy_id) and policy_id == str(parity.get("policy_id") or ""),
        },
        "funnel": {
            "paper_orders": orders,
            "copyable_buy_events": copyable,
            "noncopyable_or_unjoined_orders": max(0, orders - copyable),
            "resolved_paper_fills": resolved,
            "unresolved_copyable_fills": max(0, copyable - resolved),
            "copyable_rate_pct": round(100.0 * copyable / orders, 6) if orders else None,
            "resolution_rate_pct": round(100.0 * resolved / copyable, 6) if copyable else None,
            "accounting_gap": max(0, copyable - resolved - max(0, copyable - resolved)),
        },
        "ev": {
            "resolved_paper_pnl_usd": round(pnl, 6),
            "resolved_paper_fills": resolved,
            "pnl_per_resolved_fill_usd": round(pnl / resolved, 6) if resolved else None,
            "cost_usd": None,
            "roi_pct": None,
            "caveat": "The standby artifact has resolved PnL but no per-fill cost series; ROI is withheld rather than inferred.",
        },
        "fee_aware_economics": {
            "pre_fee_pnl_usd": clearance_pre_fee,
            "expected_fee_usd": fee_shadow.get("expected_fee_usd"),
            "post_fee_pnl_usd": post_fee_pnl,
            "post_fee_positive": post_fee_pnl is not None and post_fee_pnl > 0.0,
            "resolved_orders": clearance_resolved,
            "fee_rate": fee_shadow.get("fee_rate"),
            "fee_formula": fee_shadow.get("fee_formula"),
            "source": "ranked_queue_clearance_packet exact-policy shadow",
        },
        "divergence_review": {
            "status": "CLEAR" if divergence_clear else "FAIL",
            "clear": divergence_clear,
            "checks": divergence_checks,
            "rule": "ready-shadow and exact-policy post-fee packet must agree on wallet, policy, pre-fee PnL and resolved-fill count",
        },
        "precondition_input_freshness": {
            "status": "PASS" if fresh_inputs_ready else "STALE_OR_MISSING",
            "limit_h": PRECONDITION_INPUT_FRESHNESS_H,
            "ages_h": freshness_ages_h,
            "checks": freshness_checks,
            "fresh_inputs_ready": fresh_inputs_ready,
            "rule": "stale or missing snapshots defer the expiry decision; they can neither PARK nor PASS the lane",
        },
        "clock": {
            "enrolled_at": lane.get("paper_canary_enrolled_at"),
            "elapsed_h": lane.get("paper_canary_elapsed_h"),
            "minimum_h": lane.get("paper_canary_minimum_h"),
            "full_utc_day": lane.get("ready_shadow_full_utc_day"),
        },
        "decision_clock": {
            "clock_start": _iso(enrolled_at),
            "decision_at": _iso(decision_at),
            "minimum_h": VOLUME_DECISION_HOURS,
            "elapsed_h": round(elapsed_h, 6) if elapsed_h is not None else None,
            "due": decision_due,
            "basis": "fixed paper-canary enrollment clock; 72h means 72h",
        },
        "source_liveness": source_liveness,
        "promotion_gate": {
            "preconditions": lane.get("live_canary_packet_preconditions") or {},
            "failed_preconditions": failures,
            "status": "HARD_FAIL" if failures else "PASS",
            "readiness_verdict": lane.get("readiness_verdict"),
            "decision": "NO_LIVE_PROMOTION" if failures else "READY_FOR_FABLE_ADJUDICATION",
        },
        "evidence_gate_pass": decision_due and fresh_inputs_ready and branch_inputs_pass,
        "decision": decision,
        "terminal_decision": terminal_decision or None,
        "terminal_decision_monotone": bool(terminal_decision),
        "prederived_decision_branches": {
            "before_decision_at": "ACCRUE_PAPER_ONLY_NO_LIVE_MUTATION",
            "at_or_after_decision_if_any_input_stale_or_missing": "DEFER_VOLUME_DECISION_FRESH_INPUT_REQUIRED",
            "at_or_after_decision_if_all_inputs_pass": "PROMOTE_PACKET_READY_FOR_FABLE_LIVE_DECISION",
            "at_or_after_decision_if_any_input_fails": "PARK_VOLUME_STANDBY_PAPER_ONLY",
            "current_branch": current_branch,
            "mechanical_inputs": mechanical_inputs,
        },
        "source_generated_at": state.get("generated_at"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--state", default="data/research/wallet_copy_ready_shadow_lanes_state.json")
    parser.add_argument(
        "--clearance-packet",
        default="data/research/ranked_queue_clearance_packet_0x13e0d447520ebe7f8eeaf7817211201b2c585204.json",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--terminal-decision",
        choices=["PARK_VOLUME_STANDBY_PAPER_ONLY"],
        default=None,
        help="Persist a Fable-adjudicated terminal decision across later refreshes.",
    )
    args = parser.parse_args()
    output_path = ROOT / args.output
    packet = build_packet(
        load_json(ROOT / args.state, default={}),
        wallet=args.wallet,
        clearance_packet=load_json(ROOT / args.clearance_packet, default={}),
        previous_packet=load_json(output_path, default={}),
        terminal_decision=args.terminal_decision,
    )
    atomic_write_json(output_path, packet)
    print(
        f"wrote {args.output}; wallet={packet['wallet']}; "
        f"resolved={packet['ev']['resolved_paper_fills']}; "
        f"pnl={packet['ev']['resolved_paper_pnl_usd']}; "
        f"gate={packet['promotion_gate']['status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
