#!/usr/bin/env python3
"""Atomically disable one active-set overlay member behind a generation fence."""

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
DEFAULT_OUTPUT = ROOT / "data/research/active_set_member_demotion_execution_latest.json"


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _member_wide_fingerprint(member: dict[str, Any]) -> str:
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    for raw in (
        member.get("wide_policy_fingerprint"),
        policy.get("wide_policy_fingerprint"),
        member.get("policy_id"),
        policy.get("policy_id"),
    ):
        text = str(raw or "").strip()
        if not text:
            continue
        if text.startswith("wide_fp_"):
            # policy_id form wide_fp_<prefix> is a stable identity prefix, not full fp
            return text
        return text
    return ""


def _fingerprint_matches(member_fp: str, target_fp: str) -> bool:
    member = str(member_fp or "").strip().lower()
    target = str(target_fp or "").strip().lower()
    if not member or not target:
        return False
    if member == target:
        return True
    # Accept policy_id prefix form wide_fp_<24hex> against full 64-hex fingerprint.
    member_body = member[8:] if member.startswith("wide_fp_") else member
    target_body = target[8:] if target.startswith("wide_fp_") else target
    if not member_body or not target_body:
        return False
    return member_body == target_body or target_body.startswith(member_body) or member_body.startswith(
        target_body
    )


def _member_matches_identity(
    member: dict[str, Any],
    *,
    target_wallet: str,
    target_wide_policy_fingerprint: str,
    target_policy_id: str,
) -> bool:
    wallet = _wallet(member.get("source_wallet") or member.get("wallet"))
    if wallet != target_wallet:
        return False
    if target_wide_policy_fingerprint:
        member_fp = _member_wide_fingerprint(member)
        if not _fingerprint_matches(member_fp, target_wide_policy_fingerprint):
            # also allow exact policy_id compare when only prefix is on member
            policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
            policy_id = str(member.get("policy_id") or policy.get("policy_id") or "")
            if not _fingerprint_matches(policy_id, target_wide_policy_fingerprint):
                return False
        return True
    if target_policy_id:
        policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
        policy_id = str(member.get("policy_id") or policy.get("policy_id") or "").strip().lower()
        return policy_id == target_policy_id.strip().lower()
    return True


def validate_canonical_loss(
    *,
    canonical_resolved: bool,
    canonical_pnl_usd: float,
    resolution_winner: str,
) -> None:
    if canonical_resolved is not True:
        raise ValueError("demotion requires canonical resolved=true")
    if str(resolution_winner or "").upper() not in {"UP", "DOWN"}:
        raise ValueError("demotion requires an attached canonical resolution winner")
    if float(canonical_pnl_usd) >= 0:
        raise ValueError("demotion requires canonical pnl_usd < 0")


def build_demotion(
    *,
    overlay: dict[str, Any],
    target_wallet: str,
    preserve_wallets: set[str],
    direction_id: str,
    reason: str,
    evidence: str,
    generated_at: str,
    canonical_resolved: bool,
    canonical_pnl_usd: float,
    resolution_winner: str,
    target_wide_policy_fingerprint: str = "",
    target_policy_id: str = "",
    cooloff_until: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_canonical_loss(
        canonical_resolved=canonical_resolved,
        canonical_pnl_usd=canonical_pnl_usd,
        resolution_winner=resolution_winner,
    )
    target_wallet = _wallet(target_wallet)
    if not target_wallet:
        raise ValueError("target wallet must be a full 0x address")
    target_wide_policy_fingerprint = str(target_wide_policy_fingerprint or "").strip()
    target_policy_id = str(target_policy_id or "").strip()
    preserve_wallets = {_wallet(wallet) for wallet in preserve_wallets}
    preserve_wallets.discard("")
    if target_wallet in preserve_wallets:
        raise ValueError("target wallet cannot also be preserved")

    members = overlay.get("members")
    if not isinstance(members, list):
        raise ValueError("overlay members are missing")

    wallet_member_count = 0
    identity_match_indexes: list[int] = []
    for member_index, raw_member in enumerate(members):
        if not isinstance(raw_member, dict):
            continue
        wallet = _wallet(raw_member.get("source_wallet") or raw_member.get("wallet"))
        if wallet != target_wallet:
            continue
        wallet_member_count += 1
        if _member_matches_identity(
            raw_member,
            target_wallet=target_wallet,
            target_wide_policy_fingerprint=target_wide_policy_fingerprint,
            target_policy_id=target_policy_id,
        ):
            identity_match_indexes.append(member_index)

    if wallet_member_count > 1 and not target_wide_policy_fingerprint and not target_policy_id:
        raise ValueError(
            "duplicate target wallet members require --target-wide-policy-fingerprint "
            f"or --target-policy-id: {target_wallet} count={wallet_member_count}"
        )
    if len(identity_match_indexes) > 1:
        enabled_identity_indexes = [
            member_index
            for member_index in identity_match_indexes
            if isinstance(members[member_index], dict)
            and members[member_index].get("enabled") is True
        ]
        if len(enabled_identity_indexes) != 1:
            raise ValueError(
                "duplicate target member identity without one enabled live member for "
                f"{target_wallet} fingerprint={target_wide_policy_fingerprint or target_policy_id or 'wallet-only'}"
            )
        identity_match_indexes = enabled_identity_indexes
    target_member_index = identity_match_indexes[0] if identity_match_indexes else None

    updated_members: list[Any] = []
    target_before: dict[str, Any] | None = None
    target_after: dict[str, Any] | None = None
    preserved_before: dict[str, bool] = {}
    demoted_fingerprint = ""
    demoted_policy_id = ""
    for member_index, raw_member in enumerate(members):
        if not isinstance(raw_member, dict):
            updated_members.append(raw_member)
            continue
        member = dict(raw_member)
        wallet = _wallet(member.get("source_wallet") or member.get("wallet"))
        if wallet in preserve_wallets:
            preserved_before[wallet] = member.get("enabled") is not False
        if member_index == target_member_index:
            if target_before is not None:
                raise ValueError(
                    "duplicate target member identity for "
                    f"{target_wallet} fingerprint={target_wide_policy_fingerprint or target_policy_id or 'wallet-only'}"
                )
            target_before = dict(member)
            policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
            demoted_fingerprint = str(
                target_wide_policy_fingerprint
                or member.get("wide_policy_fingerprint")
                or policy.get("wide_policy_fingerprint")
                or ""
            )
            demoted_policy_id = str(
                target_policy_id
                or member.get("policy_id")
                or policy.get("policy_id")
                or ""
            )
            member["enabled"] = False
            member["status"] = "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
            member["mechanical_temporal_loss_demotion"] = {
                "flow_stage": "LIVE/ROTATE",
                "direction_id": direction_id,
                "generated_at": generated_at,
                "reason": reason,
                "evidence": evidence,
                "wide_policy_fingerprint": demoted_fingerprint or None,
                "policy_id": demoted_policy_id or None,
                "readmission_rule": "requires a newer evidence-positive Fable DIRECTION",
            }
            target_after = dict(member)
        updated_members.append(member)
    if target_before is None or target_after is None:
        scope = target_wide_policy_fingerprint or target_policy_id or "wallet-only"
        raise ValueError(f"target member absent: {target_wallet} scope={scope}")
    missing_preserved = preserve_wallets - set(preserved_before)
    if missing_preserved:
        raise ValueError(f"preserved wallets absent: {sorted(missing_preserved)}")

    updated = dict(overlay)
    updated["members"] = updated_members
    updated["updated_at"] = generated_at
    updated["direction_id"] = direction_id
    updated["last_action"] = "MECHANICAL_TEMPORAL_LOSS_MEMBER_DEMOTION"
    demotion = {
        "flow_stage": "LIVE/ROTATE",
        "status": "APPLIED",
        "direction_id": direction_id,
        "generated_at": generated_at,
        "target_wallet": target_wallet,
        "target_candidate_id": target_after.get("candidate_id"),
        "target_was_enabled": target_before.get("enabled") is not False,
        "wide_policy_fingerprint": demoted_fingerprint or None,
        "policy_id": demoted_policy_id or None,
        "reason": reason,
        "evidence": evidence,
        "canonical_resolved": canonical_resolved,
        "canonical_pnl_usd": canonical_pnl_usd,
        "resolution_winner": resolution_winner.upper(),
        "preserved_wallets": sorted(preserve_wallets),
        "preserved_enabled_before": preserved_before,
        "replacement_selected": False,
        "single_submitter_preserved": True,
    }
    if cooloff_until:
        demotion["cooloff_until"] = cooloff_until
    updated["latest_mechanical_temporal_loss_demotion"] = demotion
    pin = updated.get("selection_pin")
    if isinstance(pin, dict) and _wallet(pin.get("source_wallet")) == target_wallet:
        updated["selection_pin"] = {
            **pin,
            "enabled": False,
            "disabled_at": generated_at,
            "disabled_reason": "target_member_mechanically_demoted",
            "disabled_wide_policy_fingerprint": demoted_fingerprint or None,
            "disabled_policy_id": demoted_policy_id or None,
        }
        demotion["target_selection_pin_disabled"] = True
    else:
        demotion["target_selection_pin_disabled"] = False

    report = {
        "kind": "active_set_member_demotion_execution",
        "schema_version": 1,
        **demotion,
        "copyintent_parity_preserved": True,
        "live_path_mutated": True,
        "mutation_scope": "active-set overlay membership only",
    }
    return updated, report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--target-wallet", required=True)
    parser.add_argument(
        "--target-wide-policy-fingerprint",
        default="",
        help="Exact wide_policy_fingerprint (or wide_fp_ prefix) when overlay has duplicate wallet members",
    )
    parser.add_argument(
        "--target-policy-id",
        default="",
        help="Exact policy_id when overlay has duplicate wallet members",
    )
    parser.add_argument("--preserve-wallet", action="append", default=[])
    parser.add_argument("--direction-id", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--canonical-resolved", action="store_true", required=True)
    parser.add_argument("--canonical-pnl-usd", type=float, required=True)
    parser.add_argument("--resolution-winner", choices=("UP", "DOWN"), required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument(
        "--cooloff-until",
        default="",
        help="Optional ISO cooloff expiry recorded on the demotion report (deadman derives 24h from generated_at if omitted)",
    )
    args = parser.parse_args()

    overlay_path = Path(args.overlay)
    output_path = Path(args.output)
    before_sha256 = _sha256(overlay_path)
    if before_sha256 != args.expected_sha256:
        raise SystemExit(
            f"generation fence mismatch: expected {args.expected_sha256}, found {before_sha256}"
        )
    generated_at = _utc_now()
    updated, report = build_demotion(
        overlay=load_json(overlay_path, default={}) or {},
        target_wallet=args.target_wallet,
        preserve_wallets=set(args.preserve_wallet),
        direction_id=args.direction_id,
        reason=args.reason,
        evidence=args.evidence,
        generated_at=generated_at,
        canonical_resolved=args.canonical_resolved,
        canonical_pnl_usd=args.canonical_pnl_usd,
        resolution_winner=args.resolution_winner,
        target_wide_policy_fingerprint=args.target_wide_policy_fingerprint,
        target_policy_id=args.target_policy_id,
        cooloff_until=args.cooloff_until or None,
    )
    if _sha256(overlay_path) != before_sha256:
        raise SystemExit("generation fence changed before atomic write")
    report["overlay_sha256_before"] = before_sha256
    atomic_write_json(overlay_path, updated)
    report["overlay_sha256_after"] = _sha256(overlay_path)
    atomic_write_json(output_path, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
