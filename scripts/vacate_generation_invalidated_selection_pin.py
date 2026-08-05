#!/usr/bin/env python3
"""Atomically vacate a live selection pin whose admission generation invalidated."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_OVERLAY = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def build_vacate(
    overlay: dict[str, Any],
    *,
    wallet: str,
    candidate_id: str,
    fingerprint: str,
    direction_id: str,
    evidence: str,
    residual_order_id: str,
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    wallet = _wallet(wallet)
    if not wallet:
        raise ValueError("wallet must be a full 0x address")
    pin = overlay.get("selection_pin")
    if not isinstance(pin, dict):
        raise ValueError("selection pin is missing")
    if _wallet(pin.get("source_wallet")) != wallet:
        raise ValueError("selection pin wallet does not match")
    if str(pin.get("candidate_id") or "") != candidate_id:
        raise ValueError("selection pin candidate does not match")
    if pin.get("enabled") is not True:
        raise ValueError("selection pin is not enabled")

    disabled_members = 0
    members: list[Any] = []
    for raw in overlay.get("members") or []:
        if not isinstance(raw, dict):
            members.append(raw)
            continue
        member = dict(raw)
        if (
            _wallet(member.get("source_wallet") or member.get("wallet")) == wallet
            and str(member.get("candidate_id") or "") == candidate_id
            and member.get("enabled") is True
        ):
            member.update(
                {
                    "enabled": False,
                    "status": "VACATED_FABLE_GENERATION_FENCE_PROVEN_LOSING_D277",
                    "disabled_at": generated_at,
                    "disabled_reason": (
                        "generation_fence_evidence_invalidated_proven_losing_identity"
                    ),
                    "disabled_direction_id": direction_id,
                }
            )
            disabled_members += 1
        members.append(member)
    if disabled_members < 1:
        raise ValueError("enabled target emergency member is missing")

    release_reason = (
        "fable_direction_2026-07-27T15:17:37Z_vacate_b27b_d277_"
        "post_generation_rollover"
    )
    disabled_pin = {
        **pin,
        "enabled": False,
        "disabled_at": generated_at,
        "disabled_reason": "generation_fence_evidence_invalidated_proven_losing_identity",
        "release_reason": release_reason,
        "disabled_wide_policy_fingerprint": fingerprint,
    }
    previous = [
        row for row in overlay.get("previous_selection_pins") or [] if isinstance(row, dict)
    ]
    previous.append(dict(disabled_pin))
    stamp = {
        "status": "VACATED_GENERATION_FENCE_PROVEN_LOSING",
        "direction_id": direction_id,
        "generated_at": generated_at,
        "wallet": wallet,
        "wide_policy_fingerprint": fingerprint,
        "candidate_id": candidate_id,
        "evidence": evidence,
        "residual_order_id": residual_order_id,
        "pin_created_at": pin.get("created_at"),
        "pin_expires_at": pin.get("expires_at"),
        "single_submitter_preserved": True,
    }
    updated = {
        **overlay,
        "members": members,
        "selection_pin": disabled_pin,
        "previous_selection_pins": previous,
        "latest_policy_choke_direct_temporal_disable": stamp,
        "latest_generation_fence_selection_pin_vacate": stamp,
        "updated_at": generated_at,
        "direction_id": direction_id,
        "last_action": "VACATED_GENERATION_FENCE_PROVEN_LOSING_SELECTION_PIN",
    }
    report = {
        **stamp,
        "disabled_members": disabled_members,
        "selection_pin_enabled": False,
        "residual_order_action": "HOLD_NO_CANCEL_NO_CHASE",
    }
    return updated, report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--fingerprint", required=True)
    parser.add_argument("--direction-id", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--residual-order-id", required=True)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args()

    path = Path(args.overlay)
    before = _sha256(path)
    if before != args.expected_sha256:
        raise SystemExit("overlay generation fence mismatch")
    updated, report = build_vacate(
        load_json(path, default={}) or {},
        wallet=args.wallet,
        candidate_id=args.candidate_id,
        fingerprint=args.fingerprint,
        direction_id=args.direction_id,
        evidence=args.evidence,
        residual_order_id=args.residual_order_id,
        generated_at=_utc_now(),
    )
    if _sha256(path) != before:
        raise SystemExit("overlay generation fence changed before atomic write")
    atomic_write_json(path, updated)
    report["overlay_sha256_before"] = before
    report["overlay_sha256_after"] = _sha256(path)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
