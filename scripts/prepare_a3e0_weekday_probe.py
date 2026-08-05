#!/usr/bin/env python3
"""Prepare Fable's future-effective a3e0 $1 live probe and evidence contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


WALLET = "0xa3e0985f2d0b3209a52f171660287863690d095d"
CANDIDATE_ID = "market_cohort_alive_a3e0985f2d"
POLICY_ID = "fast_wf_0.10_cap_1_all_prices_minusd_0_all_window"
DIRECTION_ID = "2026-07-20T05:53Z-fable-a3e0-weekday-probe-admission"
ARM_AT = "2026-07-21T00:00:00Z"


def _deep_gate(packet: dict[str, Any]) -> dict[str, Any]:
    history = packet.get("history_depth") if isinstance(packet.get("history_depth"), dict) else {}
    decision = packet.get("decision") if isinstance(packet.get("decision"), dict) else {}
    temporal = packet.get("temporal_hour_match") if isinstance(packet.get("temporal_hour_match"), dict) else {}
    failures: list[str] = []
    if str(packet.get("wallet") or "").lower() != WALLET:
        failures.append("wallet_mismatch")
    if history.get("status") != "COMPLETE_TO_PREREGISTERED_LOOKBACK":
        failures.append("history_not_complete")
    if decision.get("p1_pass") is not True or decision.get("verdict") != "PROBE_READY_PENDING_FABLE_AUDIT":
        failures.append("p1_verdict_regressed")
    if temporal.get("status") != "PASS" or temporal.get("candidate_classification") != "WEEKDAY-ONLY":
        failures.append("weekday_temporal_gate_regressed")
    return {
        "status": "PASS" if not failures else "FAIL_DO_NOT_ARM",
        "failures": failures,
        "history_status": history.get("status"),
        "p1_verdict": decision.get("verdict"),
        "temporal_status": temporal.get("status"),
        "classification": temporal.get("candidate_classification"),
        "history_sha256": ((packet.get("inputs") or {}).get("history_sha256")),
    }


def build_member(packet: dict[str, Any]) -> dict[str, Any]:
    gate = _deep_gate(packet)
    if gate["status"] != "PASS":
        raise ValueError(f"a3e0 pre-arm deep gate failed: {gate['failures']}")
    return {
        "candidate_id": CANDIDATE_ID,
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": WALLET,
        "policy_id": POLICY_ID,
        "wallet_fraction": 0.10,
        "max_order_usd": 1.0,
        "fable_cap_max_order_usd": 1.0,
        "rolling_loss_trigger_usd": -16.0,
        "enabled": True,
        "activate_not_before_utc": ARM_AT,
        "queue_position": 0.1005,
        "status": "FABLE_0553_A3E0_WEEKDAY_PROBE_ARMED_NOT_BEFORE",
        "policy": {
            "policy_id": POLICY_ID,
            "min_price": 0.01,
            "max_price": 0.50,
            "min_seconds_from_open": 0,
            "max_seconds_from_open": 300,
            "wallet_fraction": 0.10,
            "max_order_usd": 1.0,
            "min_order_usd": 1.0,
        },
        "prearm_deep_history_gate": {
            **gate,
            "artifact": "data/research/focused_candidate_p1_a3e0985f2d_latest.json",
            "rule": "if a pre-arm refresh regresses history, P1, or weekday temporal verdict, do not arm",
        },
        "paper_shadow_parity": {
            "required_from": ARM_AT,
            "scope": "same selected policy-eligible CopyIntent lifecycle",
            "live_only_difference": "execution permission through the sole live guard",
            "status": "PREREGISTERED",
        },
        "summary": {
            "direction_id": DIRECTION_ID,
            "promotion_basis": "complete 30d deep P1 pass; positive aggregate; weekday-only; unconcentrated",
            "probe_size_usd": 1.0,
            "evaluation_window": "72 accumulated UTC weekday hours from first live fill",
            "copyintent_parity": "required; live guard remains sole submitter",
        },
    }


def prepare(*, overlay: dict[str, Any], packet: dict[str, Any], generated_at: str) -> tuple[dict[str, Any], dict[str, Any]]:
    member = build_member(packet)
    members = [
        dict(row)
        for row in (overlay.get("members") or [])
        if isinstance(row, dict) and str(row.get("source_wallet") or "").lower() != WALLET
    ]
    members.append(member)
    updated = dict(overlay)
    updated.update({
        "schema_version": 1,
        "kind": "wallet_copy_active_set_auto_degrade_state",
        "updated_at": generated_at,
        "direction_id": DIRECTION_ID,
        "members": members,
        "latest_admission": member,
        "latest_a3e0_weekday_probe_admission": {
            "status": "CONFIG_PREPARED_ARMED_NOT_BEFORE",
            "direction_id": DIRECTION_ID,
            "prepared_at": generated_at,
            "activate_not_before_utc": ARM_AT,
            "candidate_id": CANDIDATE_ID,
            "source_wallet": WALLET,
            "max_order_usd": 1.0,
            "sole_submitter": "scripts/run_wallet_copy_live_guard.py",
        },
    })
    evidence = {
        "schema_version": 1,
        "kind": "a3e0_weekday_live_probe_evidence",
        "flow_stage": "PROMOTE/LIVE/OBSERVE",
        "generated_at": generated_at,
        "status": "PREREGISTERED_AWAITING_ARM",
        "direction_id": DIRECTION_ID,
        "candidate_id": CANDIDATE_ID,
        "source_wallet": WALLET,
        "activate_not_before_utc": ARM_AT,
        "first_fill_at": None,
        "evaluation_clock": {
            "target_weekday_hours": 72,
            "elapsed_weekday_hours": 0.0,
            "window_close_at": None,
            "rule": "start at first live fill and accumulate only Monday-Friday UTC hours",
        },
        "metrics": {
            "fills": 0,
            "fee_adjusted_realized_pnl_usd": 0.0,
            "slippage_vs_paper_usd": 0.0,
            "per_cell_hour_attribution": {},
        },
        "paper_shadow_parity": member["paper_shadow_parity"],
        "prearm_deep_history_gate": member["prearm_deep_history_gate"],
        "live_mutation_before_arm": False,
        "verdict_authority": "Fable audit after the 72-weekday-hour window: scale, hold, or retire",
    }
    return updated, evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", default="data/research/wallet_copy_active_set_auto_degrade_state.json")
    parser.add_argument("--focused-packet", default="data/research/focused_candidate_p1_a3e0985f2d_latest.json")
    parser.add_argument("--evidence", default="data/research/a3e0_weekday_live_probe_evidence_latest.json")
    args = parser.parse_args()
    generated_at = utc_now_iso()
    overlay, evidence = prepare(
        overlay=load_json(args.overlay, default={}),
        packet=load_json(args.focused_packet, default={}),
        generated_at=generated_at,
    )
    atomic_write_json(args.overlay, overlay)
    atomic_write_json(args.evidence, evidence)
    print(json.dumps({"status": evidence["status"], "candidate_id": CANDIDATE_ID, "arm_at": ARM_AT}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
