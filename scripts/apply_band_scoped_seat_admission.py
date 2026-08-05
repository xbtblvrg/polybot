#!/usr/bin/env python3
"""Admit OP-TOMORROW wallets only inside F1 + dual-half positive bands."""

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
from src.wallet_copy.venue_executability import VENUE_EVIDENCE_AUTHORITY  # noqa: E402


DIRECTION_ID = "2026-07-28T07:55Z-OP-TOMORROW"
TARGETS = (
    "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
    "0x6663e52b3683832aa611b0c7e0e91bc654d368ca",
)
DEFAULT_EVIDENCE = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_OUTPUT = "data/research/band_scoped_admission_latest.json"


def _positive_band_rows(cell: dict[str, Any]) -> list[dict[str, Any]]:
    summaries = (
        cell.get("move_slice_venue_executable_full_stream_rescore")
        if isinstance(cell.get("move_slice_venue_executable_full_stream_rescore"), dict)
        else {}
    )
    admitted: list[dict[str, Any]] = []
    for move_slice_key, summary in sorted(summaries.items()):
        if not isinstance(summary, dict):
            continue
        if summary.get("evidence_authority") != VENUE_EVIDENCE_AUTHORITY:
            continue
        if not (
            summary.get("f1_pass") is True
            and int(summary.get("resolved") or 0) >= 200
            and float(summary.get("post_fee_pnl_usd") or 0.0) > 0.0
            and float(summary.get("roi_pct") or 0.0) > 0.0
            and float(summary.get("first_half_post_fee_pnl_usd") or 0.0) > 0.0
            and float(summary.get("second_half_post_fee_pnl_usd") or 0.0) > 0.0
        ):
            continue
        admitted.append(
            {
                "move_slice_key": str(move_slice_key),
                "evidence_authority": VENUE_EVIDENCE_AUTHORITY,
                "venue_executable_full_stream_rescore": dict(summary),
            }
        )
    return admitted


def build_packet(evidence: dict[str, Any], *, generated_at: str) -> dict[str, Any]:
    best = evidence.get("best_by_wallet") if isinstance(evidence.get("best_by_wallet"), dict) else {}
    members: list[dict[str, Any]] = []
    defects: list[dict[str, Any]] = []
    for index, wallet in enumerate(TARGETS, start=1):
        cell = best.get(wallet) if isinstance(best.get(wallet), dict) else {}
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        admitted_bands = _positive_band_rows(cell)
        if not cell or not admitted_bands:
            defects.append(
                {
                    "source_wallet": wallet,
                    "reason": "no_f1_and_dual_half_positive_band",
                }
            )
            continue
        move_slice_keys = [row["move_slice_key"] for row in admitted_bands]
        fingerprint = str(cell.get("wide_policy_fingerprint") or "")
        policy_id = f"wide_band_scoped_{fingerprint[:12]}"
        admission = {
            "status": "ACTIVE",
            "direction_id": DIRECTION_ID,
            "activated_at": generated_at,
            "evidence_artifact": DEFAULT_EVIDENCE,
            "evidence_generated_at": evidence.get("generated_at"),
            "wide_policy_fingerprint": fingerprint,
            "selection_rule_id": identity.get("selection_rule_id"),
            "per_member_loss_line_usd": -4.0,
            "admitted_bands": admitted_bands,
        }
        members.append(
            {
                "candidate_id": f"band_scoped_{wallet[-12:]}",
                "candidate_type": "SINGLE_WALLET",
                "source_wallet": wallet,
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
                "queue_position": index / 10_000,
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
                "band_scoped_admission": admission,
                "summary": {
                    "direction_id": DIRECTION_ID,
                    "promotion_basis": (
                        "fixed-policy full-stream F1 plus positive chronological "
                        "halves independently for every included move slice"
                    ),
                    "single_submitter_preserved": True,
                    "copyintent_parity_preserved": True,
                },
            }
        )
    status = "PASS" if len(members) == len(TARGETS) and not defects else "DEFECT"
    return {
        "schema_version": 1,
        "kind": "band_scoped_seat_admission",
        "flow_stage": "LIVE/PROMOTE/DEFEND",
        "generated_at": generated_at,
        "direction_id": DIRECTION_ID,
        "status": status,
        "paper_only": status != "PASS",
        "live_orders_allowed": status == "PASS" and bool(members),
        "evidence_artifact": DEFAULT_EVIDENCE,
        "members": members,
        "defects": defects,
        "guardrails": {
            "min_order_usd": 1.0,
            "max_order_usd": 1.0,
            "venue_minimum_shares": 5.0,
            "venue_minimum_hard_ceiling_usd": 2.5,
            "venue_minimum_max_price": 0.5,
            "per_member_loss_line_usd": -4.0,
            "probe_caps_rest_of_utc_day_honored": True,
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
            "copyintent_parity": True,
        },
    }


def apply_packet(packet: dict[str, Any]) -> dict[str, Any]:
    if packet.get("status") != "PASS":
        raise RuntimeError(f"refusing incomplete band-scoped packet: {packet.get('defects')}")
    overlay = live_guard._load_auto_degrade_active_set_overlay()
    targets = set(TARGETS)
    retained = [
        dict(row)
        for row in overlay.get("members") or []
        if isinstance(row, dict)
        and str(row.get("source_wallet") or row.get("wallet") or "").lower()
        not in targets
    ]
    members = [*packet["members"], *retained]
    updated = dict(overlay)
    updated["members"] = members
    updated["updated_at"] = packet["generated_at"]
    updated["direction_id"] = DIRECTION_ID
    updated["latest_band_scoped_admission"] = {
        key: value for key, value in packet.items() if key != "members"
    } | {
        "members": [
            {
                "candidate_id": row["candidate_id"],
                "source_wallet": row["source_wallet"],
                "policy_id": row["policy_id"],
                "move_slice_keys": row["policy"]["move_slice_keys"],
                "rolling_loss_trigger_usd": row["rolling_loss_trigger_usd"],
            }
            for row in packet["members"]
        ]
    }
    live_guard._atomic_write_auto_degrade_overlay(updated)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", default=DEFAULT_EVIDENCE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    evidence = load_json(args.evidence, default={})
    packet = build_packet(
        evidence if isinstance(evidence, dict) else {},
        generated_at=utc_now_iso(),
    )
    atomic_write_json(args.output, packet)
    if args.apply:
        apply_packet(packet)
    print(json.dumps(packet, sort_keys=True))
    return 0 if packet["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
