#!/usr/bin/env python3
"""Wire Fable's future-effective ee888f member-pool admission."""

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


WALLET = "0xee888fa7b96007f7fa270988e92bddb0ae19ed10"
A689_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
CANDIDATE_ID = "market_cohort_alive_ddb0ae19ed10"
POLICY_ID = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
DIRECTION_ID = "2026-07-20T17:11Z-fable-ee888f-weekday-admission"
ACTIVATE_AT = "2026-07-21T00:00:00Z"


def _candidate_gate(candidate: dict[str, Any]) -> dict[str, Any]:
    external = candidate.get("external_liveness") if isinstance(candidate.get("external_liveness"), dict) else {}
    source = candidate.get("source_active") if isinstance(candidate.get("source_active"), dict) else {}
    temporal = candidate.get("temporal_evidence") if isinstance(candidate.get("temporal_evidence"), dict) else {}
    matched = temporal.get("matched_slice") if isinstance(temporal.get("matched_slice"), dict) else {}
    failures: list[str] = []
    if str(candidate.get("wallet") or "").lower() != WALLET:
        failures.append("wallet_mismatch")
    if str(candidate.get("candidate_id") or "") != CANDIDATE_ID:
        failures.append("candidate_id_mismatch")
    if candidate.get("recommendation") != "ADMISSION_PACKET_READY":
        failures.append("recommendation_not_ready")
    if candidate.get("history_completeness") != "complete":
        failures.append("history_incomplete")
    if external.get("status") != "PASS":
        failures.append("external_liveness_not_pass")
    if source.get("source_active_tally_status") != "PASS":
        failures.append("source_active_not_pass")
    if source.get("policy_eligible_tally_status") != "PASS":
        failures.append("policy_eligible_not_pass")
    if candidate.get("hour_match_status") != "PASS_PROVEN_POSITIVE_ACTIVE_SLICE":
        failures.append("hour_match_not_pass")
    if matched.get("slice") != "weekday" or matched.get("label") != "PROVEN-POSITIVE":
        failures.append("weekday_slice_not_proven_positive")
    if candidate.get("denylist_cells") not in ([], None):
        failures.append("denylist_not_empty")
    return {
        "status": "PASS" if not failures else "FAIL_DO_NOT_ADMIT",
        "failures": failures,
        "external_liveness_status": external.get("status"),
        "history_completeness": candidate.get("history_completeness"),
        "source_active_status": source.get("source_active_tally_status"),
        "policy_eligible_status": source.get("policy_eligible_tally_status"),
        "hour_match_status": candidate.get("hour_match_status"),
        "matched_slice": matched,
    }


def build_member(candidate: dict[str, Any]) -> dict[str, Any]:
    gate = _candidate_gate(candidate)
    if gate["status"] != "PASS":
        raise ValueError(f"ee888f admission gate failed: {gate['failures']}")
    return {
        "candidate_id": CANDIDATE_ID,
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": WALLET,
        "policy_id": POLICY_ID,
        "wallet_fraction": 0.10,
        "max_order_usd": 4.0,
        "fable_cap_max_order_usd": 4.0,
        "rolling_loss_trigger_usd": -16.0,
        "enabled": True,
        "activate_not_before_utc": ACTIVATE_AT,
        "queue_position": 0.10025,
        "status": "FABLE_1711_EE888F_WEEKDAY_ADMISSION_ARMED_NOT_BEFORE",
        "policy": {
            "policy_id": POLICY_ID,
            "min_price": 0.0,
            "max_price": 0.50,
            "min_seconds_from_open": 0.0,
            "max_seconds_from_open": 300.0,
            "wallet_fraction": 0.10,
            "max_order_usd": 4.0,
            "min_order_usd": 1.0,
        },
        "prearm_four_way_gate": {
            **gate,
            "artifact": "data/research/cohort_alive_admission_packets_latest.json",
            "candidate_object": "top_four_way_candidate",
            "rule": "hold admission if any audited four-way fact regresses before activation",
        },
        "summary": {
            "direction_id": DIRECTION_ID,
            "promotion_basis": "Fable-audited first four-way-ready candidate across 950 reports",
            "activation_scope": "weekday only; selectable by normal rung rules, never forced seat",
            "a689_pin_unchanged": True,
            "copyintent_parity": "unchanged; live guard remains sole submitter",
            "system_caps_unchanged": {"max_order_usd": 8.0, "drip_max_tranche_usd": 2.5, "per_window_fill_cap": 1},
        },
    }


def prepare(*, overlay: dict[str, Any], admission: dict[str, Any], generated_at: str) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = admission.get("top_four_way_candidate")
    if not isinstance(candidate, dict):
        raise ValueError("top_four_way_candidate missing from admission artifact")
    member = build_member(candidate)
    original_members = [dict(row) for row in (overlay.get("members") or []) if isinstance(row, dict)]
    a689_before = [row for row in original_members if str(row.get("source_wallet") or "").lower() == A689_WALLET]
    members = [row for row in original_members if str(row.get("source_wallet") or "").lower() != WALLET]
    members.append(member)
    a689_after = [row for row in members if str(row.get("source_wallet") or "").lower() == A689_WALLET]
    if a689_after != a689_before:
        raise ValueError("a689 pin/member changed during ee888f admission")
    updated = dict(overlay)
    updated.update({
        "schema_version": 1,
        "kind": "wallet_copy_active_set_auto_degrade_state",
        "updated_at": generated_at,
        "members": members,
        "latest_admission": member,
        "latest_ee888f_weekday_admission": {
            "status": "CONFIG_PREPARED_ARMED_NOT_BEFORE",
            "direction_id": DIRECTION_ID,
            "prepared_at": generated_at,
            "activate_not_before_utc": ACTIVATE_AT,
            "candidate_id": CANDIDATE_ID,
            "source_wallet": WALLET,
            "selectable_not_forced": True,
            "a689_pin_unchanged": True,
            "sole_submitter": "scripts/run_wallet_copy_live_guard.py",
        },
    })
    evidence = {
        "schema_version": 1,
        "kind": "ee888f_weekday_admission_evidence",
        "flow_stage": "PROMOTE/LIVE/DEFEND",
        "generated_at": generated_at,
        "status": "ARMED_AWAITING_WEEKDAY_ACTIVATION",
        "direction_id": DIRECTION_ID,
        "candidate_id": CANDIDATE_ID,
        "source_wallet": WALLET,
        "activate_not_before_utc": ACTIVATE_AT,
        "prearm_four_way_gate": member["prearm_four_way_gate"],
        "a689_pin_unchanged": True,
        "selectable_not_forced": True,
        "live_mutation_before_activation": False,
        "copyintent_parity_violations": 0,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
    }
    return updated, evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", default="data/research/wallet_copy_active_set_auto_degrade_state.json")
    parser.add_argument("--admission", default="data/research/cohort_alive_admission_packets_latest.json")
    parser.add_argument("--evidence", default="data/research/ee888f_weekday_admission_evidence_latest.json")
    args = parser.parse_args()
    generated_at = utc_now_iso()
    overlay, evidence = prepare(
        overlay=load_json(args.overlay, default={}),
        admission=load_json(args.admission, default={}),
        generated_at=generated_at,
    )
    atomic_write_json(args.overlay, overlay)
    atomic_write_json(args.evidence, evidence)
    print(json.dumps({"status": evidence["status"], "candidate_id": CANDIDATE_ID, "activate_at": ACTIVATE_AT}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
