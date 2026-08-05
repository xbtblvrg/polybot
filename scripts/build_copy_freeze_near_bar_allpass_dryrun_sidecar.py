#!/usr/bin/env python3
"""Emit a paper-only, generation-fenced gate packet for the frozen WIDE cell."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json
from src.wallet_copy.venue_executability import venue_gate_summary

DEFAULT_FINGERPRINT_EVIDENCE = (
    "data/research/wide_policy_fingerprint_evidence_latest.json"
)
DEFAULT_DEADMAN = "data/research/order_flow_deadman_state.json"
DEFAULT_FREEZE_SHADOW = (
    "data/research/frozen_fingerprint_f2_prewarm_backup_rank_shadow_latest.json"
)
DEFAULT_OUTPUT = (
    "data/research/copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json"
)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _checksum(value: dict[str, Any]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _frontier_rows(deadman: dict[str, Any]) -> list[dict[str, Any]]:
    choke = deadman.get("policy_choke")
    if not isinstance(choke, dict):
        return []
    packets: list[dict[str, Any]] = []
    actuator = choke.get("actuator")
    if isinstance(actuator, dict) and isinstance(actuator.get("candidate_evidence"), dict):
        packets.append(actuator["candidate_evidence"])
    drought = choke.get("source_roster_drought")
    if isinstance(drought, dict) and isinstance(drought.get("candidate_evidence"), dict):
        packets.append(drought["candidate_evidence"])
    return [
        row
        for packet in packets
        for key in ("rows", "nearest_frontier")
        for row in (packet.get(key) or [])
        if isinstance(row, dict)
    ]


def _wallet_authority(
    deadman: dict[str, Any], wallet: str, fingerprint: str
) -> tuple[dict[str, Any], int, dict[str, Any]]:
    wallet_rows = [
        row
        for row in _frontier_rows(deadman)
        if str(row.get("wallet") or "").lower() == wallet
    ]
    supply_fingerprints = sorted(
        {
            str(row.get("wide_policy_fingerprint") or "")
            for row in wallet_rows
            if str(row.get("wide_policy_fingerprint") or "")
        }
    )
    rows = [
        row
        for row in wallet_rows
        if str(row.get("wide_policy_fingerprint") or "") == fingerprint
    ]
    if not rows:
        return {}, 10, {
            "pin_fingerprint_unmatched_in_supply": bool(wallet_rows),
            "supply_fingerprints_for_pinned_wallet": supply_fingerprints,
        }
    row = max(
        rows,
        key=lambda item: (
            int(item.get("fresh_own_source_buy_rows_30m") or 0),
            (item.get("checks") or {}).get("f4_external_liveness") is True,
            (item.get("checks") or {}).get("own_evidenced_policy_available") is True,
        ),
    )
    choke = deadman.get("policy_choke") or {}
    packets = [
        (choke.get("actuator") or {}).get("candidate_evidence") or {},
        (choke.get("source_roster_drought") or {}).get("candidate_evidence") or {},
    ]
    minimum = next(
        (
            int((packet.get("gate_digits") or {}).get(
                "f2_min_fresh_own_source_buy_rows_30m"
            ))
            for packet in packets
            if (packet.get("gate_digits") or {}).get(
                "f2_min_fresh_own_source_buy_rows_30m"
            )
        ),
        10,
    )
    return row, minimum, {
        "pin_fingerprint_unmatched_in_supply": False,
        "supply_fingerprints_for_pinned_wallet": supply_fingerprints,
    }


def _directed_climb_primary(
    fingerprint_evidence: dict[str, Any],
) -> tuple[str, str] | None:
    """Select the sole direction-owned climb override, if one is present."""

    rows: list[tuple[str, str]] = []
    overrides = fingerprint_evidence.get("freeze_overrides")
    if not isinstance(overrides, dict):
        return None
    for raw_wallet, raw in overrides.items():
        if not isinstance(raw, dict) or not str(raw.get("reason") or "").startswith(
            "climb_priority_exact_fp_"
        ):
            continue
        f1 = raw.get("f1") if isinstance(raw.get("f1"), dict) else {}
        fingerprint = str(raw.get("wide_policy_fingerprint") or "")
        if (
            not fingerprint
            or num(f1.get("post_fee_pnl_usd")) <= 0
            or num(f1.get("first_half_post_fee_pnl_usd")) <= 0
            or num(f1.get("second_half_post_fee_pnl_usd")) <= 0
        ):
            continue
        rows.append((str(raw_wallet).lower(), fingerprint))
    if len(rows) > 1:
        raise ValueError("multiple direction-owned climb freeze overrides")
    return rows[0] if rows else None


def build_sidecar(
    fingerprint_evidence: dict[str, Any],
    deadman: dict[str, Any],
    freeze_shadow: dict[str, Any],
    *,
    near_bar_distance: int = 25,
) -> dict[str, Any]:
    frozen = freeze_shadow.get("primary")
    if not isinstance(frozen, dict):
        raise ValueError("freeze shadow primary is absent")
    directed_primary = _directed_climb_primary(fingerprint_evidence)
    wallet, fingerprint = directed_primary or (
        str(frozen.get("wallet") or "").lower(),
        str(frozen.get("wide_policy_fingerprint") or ""),
    )
    if not wallet or not fingerprint:
        raise ValueError("freeze shadow primary identity is incomplete")

    cell = next(
        (
            row
            for row in fingerprint_evidence.get("cells") or []
            if isinstance(row, dict)
            and str((row.get("identity") or {}).get("wallet") or "").lower()
            == wallet
            and str(row.get("wide_policy_fingerprint") or "") == fingerprint
        ),
        None,
    )
    if cell is None:
        raise ValueError("frozen primary is absent from fingerprint evidence")
    rescore = venue_gate_summary(cell)
    resolved_target = 200
    resolved = int(rescore.get("resolved") or 0)
    remaining = max(0, resolved_target - resolved)
    f1 = (
        resolved >= resolved_target
        and num(rescore.get("post_fee_pnl_usd")) > 0
        and num(rescore.get("roi_pct")) > 0
        and num(rescore.get("first_half_post_fee_pnl_usd")) > 0
        and num(rescore.get("second_half_post_fee_pnl_usd")) > 0
    )

    authority, f2_minimum, pin_diagnostic = _wallet_authority(
        deadman, wallet, fingerprint
    )
    checks = authority.get("checks") if isinstance(authority.get("checks"), dict) else {}
    f2_count = int(authority.get("fresh_own_source_buy_rows_30m") or 0)
    cooloffs = deadman.get("policy_choke_rung_b_cooloffs") or {}
    cooloff_until = cooloffs.get(wallet)
    f2 = f2_count >= f2_minimum and checks.get(
        "f2_fresh_rows_and_own_policy_copyable"
    ) is True
    f3 = (
        cooloff_until is None
        and checks.get("f3_not_enabled_or_cooloff_or_fading") is True
        and checks.get("not_terminal_park_red_clock_or_measured_loser") is True
    )
    f4 = (
        checks.get("f4_external_liveness") is True
        and checks.get("own_evidenced_policy_available") is True
    )
    active_temporal = checks.get("active_temporal_not_proven_negative") is True
    near_bar = remaining <= near_bar_distance
    all_pass = f1 and f2 and f3 and f4 and active_temporal
    status = (
        "WAIT_PIN_UNRESOLVED"
        if pin_diagnostic["pin_fingerprint_unmatched_in_supply"]
        else "ALL_PASS_READY"
        if all_pass
        else "WAIT_F1_RESOLVED"
        if near_bar and not f1 and f2 and f3 and f4 and active_temporal
        else "NOT_NEAR_BAR"
        if not near_bar
        else "WAIT_OTHER_GATE"
    )
    return {
        "schema_version": 1,
        "kind": "copy_freeze_near_bar_allpass_dryrun_sidecar",
        "flow_stage": "PROMOTE/LIVE/LEARN/OBSERVE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "status": status,
        "primary": {
            "wallet": wallet,
            "wide_policy_fingerprint": fingerprint,
            "resolved": resolved,
            "resolved_target": resolved_target,
            "remaining_resolved": remaining,
            "near_bar_distance": near_bar_distance,
            "near_bar": near_bar,
            "post_fee_pnl_usd": rescore.get("post_fee_pnl_usd"),
            "roi_pct": rescore.get("roi_pct"),
            "first_half_post_fee_pnl_usd": rescore.get(
                "first_half_post_fee_pnl_usd"
            ),
            "second_half_post_fee_pnl_usd": rescore.get(
                "second_half_post_fee_pnl_usd"
            ),
        },
        "checks": {
            "f1_measured_positive_regime_cell": f1,
            "f2_fresh_rows_and_own_policy_copyable": f2,
            "f3_not_enabled_or_cooloff_or_fading": f3,
            "f4_external_liveness": f4,
            "active_temporal_not_proven_negative": active_temporal,
            "all_pass": all_pass,
            "fresh_own_source_buy_rows_30m": f2_count,
            "f2_minimum": f2_minimum,
            "cooloff_until": cooloff_until,
            **(
                pin_diagnostic
                if pin_diagnostic["pin_fingerprint_unmatched_in_supply"]
                else {}
            ),
        },
        "generation_fence": {
            "primary_selection_source": (
                "direction_climb_freeze_override"
                if directed_primary is not None
                else "freeze_shadow_primary"
            ),
            "fingerprint_evidence_generated_at": fingerprint_evidence.get(
                "generated_at"
            ),
            "fingerprint_evidence_sha256": _checksum(fingerprint_evidence),
            "deadman_checked_at": deadman.get("checked_at"),
            "deadman_sha256": _checksum(deadman),
            "freeze_shadow_generated_at": freeze_shadow.get("generated_at"),
            "freeze_shadow_primary_identity_only": True,
        },
        "actuator_contract": {
            "invoke_when_status": "ALL_PASS_READY",
            "path": "scripts/order_flow_deadman.py::_execute_policy_choke_rung_b",
            "supply_rung": "DIRECT",
            "selection_pin_ttl_s": 3600,
            "selection_pin_refresh_allowed": False,
            "request_cap_usd": 1.0,
            "maker_min_share_funding_cap_usd": 2.5,
            "sole_submitter": "scripts/run_wallet_copy_live_guard.py",
            "eligible_to_invoke": all_pass,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fingerprint-evidence", default=DEFAULT_FINGERPRINT_EVIDENCE)
    parser.add_argument("--deadman", default=DEFAULT_DEADMAN)
    parser.add_argument("--freeze-shadow", default=DEFAULT_FREEZE_SHADOW)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--near-bar-distance", type=int, default=25)
    args = parser.parse_args()
    packet = build_sidecar(
        _load(ROOT / args.fingerprint_evidence),
        _load(ROOT / args.deadman),
        _load(ROOT / args.freeze_shadow),
        near_bar_distance=max(0, args.near_bar_distance),
    )
    atomic_write_json(ROOT / args.output, packet)
    print(
        json.dumps(
            {
                "output": args.output,
                "status": packet["status"],
                "all_pass": packet["checks"]["all_pass"],
                "remaining_resolved": packet["primary"]["remaining_resolved"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
