#!/usr/bin/env python3
"""Atomically vacate a false-negative demotion and restore its unexpired pin."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_OVERLAY = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_DEADMAN = ROOT / "data/research/order_flow_deadman_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/active_set_member_demotion_vacate_latest.json"


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_vacate(
    *,
    overlay: dict[str, Any],
    deadman: dict[str, Any],
    target_wallet: str,
    candidate_id: str,
    direction_id: str,
    evidence: str,
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    target_wallet = _wallet(target_wallet)
    if not target_wallet:
        raise ValueError("target wallet must be a full 0x address")
    now = _parse_ts(generated_at)
    if now is None:
        raise ValueError("generated_at must be an ISO timestamp")

    pin = overlay.get("selection_pin")
    if not isinstance(pin, dict):
        raise ValueError("selection pin is missing")
    if _wallet(pin.get("source_wallet")) != target_wallet:
        raise ValueError("selection pin wallet does not match target")
    if str(pin.get("candidate_id") or "") != candidate_id:
        raise ValueError("selection pin candidate does not match target")
    expires_at = _parse_ts(pin.get("expires_at"))
    if expires_at is None or now >= expires_at:
        raise ValueError("selection pin has already expired")

    demotion = overlay.get("latest_mechanical_temporal_loss_demotion")
    if not isinstance(demotion, dict) or demotion.get("status") != "APPLIED":
        raise ValueError("latest mechanical demotion is not APPLIED")
    if _wallet(demotion.get("target_wallet")) != target_wallet:
        raise ValueError("latest mechanical demotion wallet does not match target")

    members: list[Any] = []
    target_found = False
    for raw_member in overlay.get("members") or []:
        if not isinstance(raw_member, dict):
            members.append(raw_member)
            continue
        member = dict(raw_member)
        wallet = _wallet(member.get("source_wallet") or member.get("wallet"))
        if wallet == target_wallet and str(member.get("candidate_id") or "") == candidate_id:
            if target_found:
                raise ValueError("duplicate target member")
            target_found = True
            member["enabled"] = True
            member["status"] = "POLICY_CHOKE_RUNG_DIRECT_EMERGENCY_ADMISSION"
            member["mechanical_temporal_loss_demotion"] = {
                "status": "VACATED_FALSE_NEGATIVE",
                "direction_id": direction_id,
                "generated_at": generated_at,
                "evidence": evidence,
            }
        members.append(member)
    if not target_found:
        raise ValueError("target member is missing")

    vacated = {
        **demotion,
        "status": "VACATED_FALSE_NEGATIVE",
        "vacated_at": generated_at,
        "vacated_direction_id": direction_id,
        "vacated_evidence": evidence,
    }
    updated_overlay = {
        **overlay,
        "members": members,
        "selection_pin": {
            **pin,
            "enabled": True,
            "restored_at": generated_at,
            "restored_reason": "false_negative_resolution_vacate",
        },
        "latest_mechanical_temporal_loss_demotion": vacated,
        "updated_at": generated_at,
        "direction_id": direction_id,
        "last_action": "FALSE_NEGATIVE_DEMOTION_VACATE_PIN_RESTORE",
    }
    updated_overlay["selection_pin"].pop("disabled_at", None)
    updated_overlay["selection_pin"].pop("disabled_reason", None)

    cooloffs = dict(deadman.get("policy_choke_rung_b_cooloffs") or {})
    removed_cooloff = cooloffs.pop(target_wallet, None)
    updated_deadman = {
        **deadman,
        "policy_choke_rung_b_cooloffs": cooloffs,
    }
    report = {
        "kind": "active_set_member_demotion_vacate",
        "schema_version": 1,
        "flow_stage": "LIVE/ROTATE",
        "status": "APPLIED",
        "generated_at": generated_at,
        "direction_id": direction_id,
        "target_wallet": target_wallet,
        "target_candidate_id": candidate_id,
        "pin_expires_at": pin.get("expires_at"),
        "pin_expiry_unchanged": True,
        "removed_false_cooloff": removed_cooloff,
        "evidence": evidence,
        "single_submitter_preserved": True,
        "copyintent_parity_preserved": True,
    }
    return updated_overlay, updated_deadman, report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--deadman", default=str(DEFAULT_DEADMAN))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--target-wallet", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--direction-id", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--expected-overlay-sha256", required=True)
    parser.add_argument("--expected-deadman-sha256", required=True)
    args = parser.parse_args()

    overlay_path = Path(args.overlay)
    deadman_path = Path(args.deadman)
    overlay_before = _sha256(overlay_path)
    deadman_before = _sha256(deadman_path)
    if overlay_before != args.expected_overlay_sha256:
        raise SystemExit("overlay generation fence mismatch")
    if deadman_before != args.expected_deadman_sha256:
        raise SystemExit("deadman generation fence mismatch")

    updated_overlay, updated_deadman, report = build_vacate(
        overlay=load_json(overlay_path, default={}) or {},
        deadman=load_json(deadman_path, default={}) or {},
        target_wallet=args.target_wallet,
        candidate_id=args.candidate_id,
        direction_id=args.direction_id,
        evidence=args.evidence,
        generated_at=_utc_now(),
    )
    if _sha256(overlay_path) != overlay_before or _sha256(deadman_path) != deadman_before:
        raise SystemExit("generation fence changed before atomic write")

    report["overlay_sha256_before"] = overlay_before
    report["deadman_sha256_before"] = deadman_before
    atomic_write_json(overlay_path, updated_overlay)
    atomic_write_json(deadman_path, updated_deadman)
    report["overlay_sha256_after"] = _sha256(overlay_path)
    report["deadman_sha256_after"] = _sha256(deadman_path)
    atomic_write_json(Path(args.output), report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
