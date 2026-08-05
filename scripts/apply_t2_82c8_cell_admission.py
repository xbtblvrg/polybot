#!/usr/bin/env python3
"""Clear 82c8's unbound red clock and admit its ruled exact cell at $1."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_wallet_copy_live_guard as live_guard  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.venue_executability import venue_gate_summary  # noqa: E402


WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
FINGERPRINT = "bac25beda563430ef4f482544eccda250f4e3f7eb3bed91362fb70093dbc1fce"
DIRECTION_ID = "2026-07-28T17:58Z-fable-t2-82c8-cell-admission"
DEFAULT_EVIDENCE = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_TERMINAL = "data/research/82c8_terminal_decision_latest.json"
DEFAULT_OUTPUT = "data/research/t2_82c8_cell_admission_latest.json"


def _target_cell(evidence: dict[str, Any]) -> dict[str, Any]:
    return next(
        (
            cell
            for cell in evidence.get("cells") or []
            if isinstance(cell, dict)
            and str((cell.get("identity") or {}).get("wallet") or "").lower()
            == WALLET
            and str(cell.get("wide_policy_fingerprint") or "") == FINGERPRINT
        ),
        {},
    )


def build_packet(
    evidence: dict[str, Any],
    terminal: dict[str, Any],
    *,
    generated_at: str,
) -> dict[str, Any]:
    cell = _target_cell(evidence)
    identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
    f1 = venue_gate_summary(cell)
    checks = terminal.get("checks") if isinstance(terminal.get("checks"), dict) else {}
    red_clock_unbound = bool(
        terminal.get("execution_status") == "PARK_COMMITTED"
        and checks.get("wallet_present") is False
        and checks.get("source_binding_exact") is False
        and checks.get("clock_start_exact") is False
    )
    evidence_pass = bool(
        f1.get("f1_pass") is True
        and int(f1.get("resolved") or 0) >= 200
        and float(f1.get("post_fee_pnl_usd") or 0.0) > 0.0
        and float(f1.get("roi_pct") or 0.0) > 0.0
        and float(f1.get("first_half_post_fee_pnl_usd") or 0.0) > 0.0
        and float(f1.get("second_half_post_fee_pnl_usd") or 0.0) > 0.0
    )
    defects = []
    if not cell:
        defects.append("exact_cell_missing")
    if not red_clock_unbound:
        defects.append("terminal_red_clock_has_binding_evidence")
    if not evidence_pass:
        defects.append("exact_cell_f1_or_dual_half_gate_failed")
    move_slice_keys = [
        str(value) for value in identity.get("move_slice_keys") or [] if str(value)
    ]
    admission = {
        "status": "ACTIVE",
        "direction_id": DIRECTION_ID,
        "activated_at": generated_at,
        "evidence_artifact": DEFAULT_EVIDENCE,
        "terminal_artifact": DEFAULT_TERMINAL,
        "wide_policy_fingerprint": FINGERPRINT,
        "move_slice_keys": move_slice_keys,
        "venue_executable_full_stream_rescore": f1,
        "fixed_policy_full_stream_rescore": (
            cell.get("fixed_policy_full_stream_rescore") or {}
        ),
        "per_cell_loss_line_usd": -4.0,
        "first_slice_kill": {
            "min_resolved_fills": 3,
            "pnl_lte_usd": 0.0,
            "action": "AUTO_DISABLE_CELL",
        },
    }
    policy_id = f"wide_cell_scoped_{FINGERPRINT[:12]}"
    member = {
        "candidate_id": f"cell_scoped_{WALLET[-12:]}_{FINGERPRINT[:8]}",
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": WALLET,
        "policy_id": policy_id,
        "wallet_fraction": 0.1,
        "max_order_usd": 1.0,
        "maker_min_share_funding_cap_usd": 2.5,
        "maker_min_share_original_policy_cap_usd": 4.0,
        "maker_min_share_base_request_cap_usd": 1.0,
        "max_price": 1.0,
        "rolling_loss_trigger_usd": -4.0,
        "enabled": True,
        "status": "PASS",
        "queue_position": 0.00005,
        "auto_degrade_replaces_existing_wallet": True,
        "policy": {
            "policy_id": policy_id,
            "min_price": 0.0,
            "max_price": 1.0,
            "min_seconds_from_open": 0.0,
            "max_seconds_from_open": 300.0,
            "move_slice_keys": move_slice_keys,
            "wallet_fraction": 0.1,
            "max_order_usd": 1.0,
            "min_order_usd": 1.0,
            "maker_min_share_funding_cap_usd": 2.5,
            "maker_min_share_original_policy_cap_usd": 4.0,
            "maker_min_share_base_request_cap_usd": 1.0,
        },
        "cell_scoped_admission": admission,
        "summary": {
            "direction_id": DIRECTION_ID,
            "promotion_basis": "Fable-ruled exact fingerprint cell",
            "single_submitter_preserved": True,
            "copyintent_parity_preserved": True,
        },
    }
    return {
        "schema_version": 1,
        "kind": "t2_82c8_cell_admission",
        "flow_stage": "LIVE/PROMOTE/DEFEND",
        "generated_at": generated_at,
        "direction_id": DIRECTION_ID,
        "status": "PASS" if not defects else "DEFECT",
        "defects": defects,
        "blocker_sweep": {
            "wallet": WALLET,
            "wide_policy_fingerprint": FINGERPRINT,
            "blocker": "82c8_terminal_red_clock",
            "classification": "CONVENIENT" if red_clock_unbound else "REAL",
            "evidence_pointer": DEFAULT_TERMINAL,
            "evidence": {
                "execution_status": terminal.get("execution_status"),
                "wallet_present": checks.get("wallet_present"),
                "source_binding_exact": checks.get("source_binding_exact"),
                "clock_start_exact": checks.get("clock_start_exact"),
            },
            "action": "CLEAR_FOR_EXACT_CELL_ONLY" if red_clock_unbound else "STAND",
        },
        "member": member,
    }


def apply_packet(packet: dict[str, Any]) -> None:
    if packet.get("status") != "PASS":
        raise RuntimeError(f"refusing T2 packet defects={packet.get('defects')}")
    overlay = live_guard._load_auto_degrade_active_set_overlay()
    retained = [
        dict(row)
        for row in overlay.get("members") or []
        if isinstance(row, dict)
        and str(row.get("source_wallet") or row.get("wallet") or "").lower()
        != WALLET
    ]
    updated = dict(overlay)
    updated["members"] = [packet["member"], *retained]
    updated["updated_at"] = packet["generated_at"]
    updated["direction_id"] = DIRECTION_ID
    updated["latest_t2_blocker_sweep"] = packet["blocker_sweep"]
    updated["latest_t2_82c8_cell_admission"] = {
        key: value for key, value in packet.items() if key != "member"
    }
    live_guard._atomic_write_auto_degrade_overlay(updated)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", default=DEFAULT_EVIDENCE)
    parser.add_argument("--terminal", default=DEFAULT_TERMINAL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    packet = build_packet(
        load_json(args.evidence, default={}),
        load_json(args.terminal, default={}),
        generated_at=utc_now_iso(),
    )
    atomic_write_json(args.output, packet)
    if args.apply:
        apply_packet(packet)
    print(json.dumps(packet, sort_keys=True))
    return 0 if packet["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
