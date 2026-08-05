#!/usr/bin/env python3
"""Diagnose the sticky ee3f venue-reachability gate without retargeting it."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

STICKY_WALLET = "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b"
STICKY_FINGERPRINT = "ee3f44cad1e78f050d38408b03c4301a03cd0ffb0835ae4c71faaefcde1e08cb"
HISTORY_LIMIT = 10


def _cell_index(evidence: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (
            str((cell.get("identity") or {}).get("wallet") or "").lower(),
            str(cell.get("wide_policy_fingerprint") or ""),
        ): cell
        for cell in evidence.get("cells") or []
        if isinstance(cell, dict)
    }


def _comparison_row(
    row: dict[str, Any], cell: dict[str, Any]
) -> dict[str, Any]:
    venue = cell.get("venue_executable_full_stream_rescore") or {}
    first = venue.get("first_half") or {}
    second = venue.get("second_half") or {}
    return {
        "wallet": str(row.get("wallet") or "").lower(),
        "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
        "venue_reachable_share_pct": venue.get("venue_reachable_share_pct"),
        "venue_executable_resolved": venue.get("venue_executable_resolved"),
        "venue_unreachable_resolved": venue.get("venue_unreachable_resolved"),
        "residual_to_200": max(0, 200 - int(venue.get("resolved") or 0)),
        "post_fee_pnl_usd": venue.get("post_fee_pnl_usd"),
        "first_half_post_fee_pnl_usd": first.get("post_fee_pnl_usd"),
        "second_half_post_fee_pnl_usd": second.get("post_fee_pnl_usd"),
        "concentration_admissible": venue.get("concentration_admissible"),
        "temporal_classification": (row.get("active_temporal") or {}).get(
            "classification"
        ),
        "evidence_deficits": row.get("evidence_deficits") or [],
    }


def build_diagnosis(
    *,
    fingerprint_evidence: dict[str, Any],
    candidate_evidence: dict[str, Any],
    prior: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.UTC)
    cells = _cell_index(fingerprint_evidence)
    focus_cell = cells.get((STICKY_WALLET, STICKY_FINGERPRINT), {})
    focus = focus_cell.get("venue_executable_full_stream_rescore") or {}
    reachable = int(focus.get("venue_executable_resolved") or 0)
    unreachable = int(focus.get("venue_unreachable_resolved") or 0)
    denominator = reachable + unreachable
    residual = max(0, 200 - int(focus.get("resolved") or reachable))
    projected_numerator = reachable + residual
    projected_denominator = denominator + residual
    projected_share = (
        round(100.0 * projected_numerator / projected_denominator, 6)
        if projected_denominator
        else None
    )

    current_snapshot = {
        "generated_at": fingerprint_evidence.get("generated_at")
        or now.isoformat(),
        "venue_executable_resolved": reachable,
        "all_policy_resolved": denominator,
        "venue_unreachable_resolved": unreachable,
        "venue_reachable_share_pct": focus.get("venue_reachable_share_pct"),
        "residual_to_200": residual,
        "post_fee_pnl_usd": focus.get("post_fee_pnl_usd"),
    }
    history = [
        row
        for row in ((prior or {}).get("sample_stability") or {}).get(
            "rescore_history", []
        )
        if isinstance(row, dict)
    ]
    if not history or history[-1] != current_snapshot:
        history.append(current_snapshot)
    history = history[-HISTORY_LIMIT:]
    shares = [
        float(row["venue_reachable_share_pct"])
        for row in history
        if row.get("venue_reachable_share_pct") is not None
    ]

    comparisons_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for row in candidate_evidence.get("rows") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").lower()
        fingerprint = str(row.get("wide_policy_fingerprint") or "")
        cell = cells.get((wallet, fingerprint))
        checks = row.get("checks") or {}
        venue = (cell or {}).get("venue_executable_full_stream_rescore") or {}
        first = venue.get("first_half") or {}
        second = venue.get("second_half") or {}
        lawful = bool(
            checks.get("own_evidenced_policy_available") is True
            and checks.get("not_terminal_park_red_clock_or_measured_loser") is True
            and not bool((row.get("cooloff_scope") or {}).get("active"))
            and (row.get("active_temporal") or {}).get("classification") != "FADING"
        )
        clean = bool(
            float(first.get("post_fee_pnl_usd") or 0) > 0
            and float(second.get("post_fee_pnl_usd") or 0) > 0
            and venue.get("concentration_admissible") is True
        )
        if lawful and clean and cell:
            comparisons_by_identity[(wallet, fingerprint)] = _comparison_row(row, cell)
    comparisons = sorted(
        comparisons_by_identity.values(),
        key=lambda row: (
            -float(row.get("venue_reachable_share_pct") or 0),
            int(row.get("residual_to_200") or 0),
            -float(row.get("post_fee_pnl_usd") or 0),
            str(row.get("wallet") or ""),
        ),
    )[:10]
    alternative = next(
        (
            row
            for row in comparisons
            if (row.get("wallet"), row.get("wide_policy_fingerprint"))
            != (STICKY_WALLET, STICKY_FINGERPRINT)
        ),
        None,
    )
    threshold = float(focus.get("venue_reachable_share_min_pct") or 40.0)
    structural = bool(
        projected_share is not None and projected_share < threshold
    )
    return {
        "schema_version": 1,
        "kind": "wide_ee3f_venue_reachable_diagnosis",
        "flow_stage": "OBSERVE/PROMOTE/LEARN",
        "generated_at": now.isoformat(),
        "paper_only": True,
        "live_orders_allowed": False,
        "sticky_identity": {
            "wallet": STICKY_WALLET,
            "wide_policy_fingerprint": STICKY_FINGERPRINT,
            "retarget_applied": False,
        },
        "venue_share_definition": {
            "numerator": "resolved rows passing row_is_venue_executable at the frozen policy minimum order USD",
            "denominator": "all resolved rows belonging to the exact wallet plus policy fingerprint",
            "numerator_value": reachable,
            "denominator_value": denominator,
            "formula": "100 * venue_executable_resolved / all_exact_policy_resolved",
            "threshold_pct": threshold,
        },
        "sample_stability": {
            "rescore_count": len(history),
            "rescore_history": history,
            "share_min_pct": min(shares) if shares else None,
            "share_max_pct": max(shares) if shares else None,
            "share_delta_pct_points": round(shares[-1] - shares[0], 6)
            if shares
            else None,
            "trend": (
                "UP" if len(shares) > 1 and shares[-1] > shares[0]
                else "DOWN" if len(shares) > 1 and shares[-1] < shares[0]
                else "FLAT_OR_SINGLE_SAMPLE"
            ),
        },
        "residual_zero_projection": {
            "assumption": "every remaining signal needed to reach 200 resolves venue-reachable",
            "current_residual_to_200": residual,
            "projected_numerator": projected_numerator,
            "projected_denominator": projected_denominator,
            "projected_share_pct": projected_share,
            "threshold_pct": threshold,
        },
        "diagnosis": (
            "VENUE_REACHABLE_STRUCTURAL"
            if structural
            else "VENUE_REACHABLE_SAMPLE_CAN_REACH_GATE"
        ),
        "top_10_owned_policy_both_halves_positive_concentration_pass": comparisons,
        "highest_share_lawful_alternative_for_fable_disposition_only": alternative,
        "retarget_applied": False,
        "admission_applied": False,
        "gate_mutated": False,
        "roster_mutated": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fingerprint-evidence",
        default="data/research/wide_policy_fingerprint_evidence_latest.json",
    )
    parser.add_argument(
        "--deadman", default="data/research/order_flow_deadman_state.json"
    )
    parser.add_argument(
        "--output",
        default="data/research/wide_ee3f_venue_reachable_diagnosis_latest.json",
    )
    args = parser.parse_args()
    evidence = load_json(args.fingerprint_evidence, default={}) or {}
    deadman = load_json(args.deadman, default={}) or {}
    candidates = (
        (((deadman.get("policy_choke") or {}).get("source_roster_drought") or {}).get("candidate_evidence"))
        or {}
    )
    prior = load_json(args.output, default={}) or {}
    report = build_diagnosis(
        fingerprint_evidence=evidence,
        candidate_evidence=candidates,
        prior=prior,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
