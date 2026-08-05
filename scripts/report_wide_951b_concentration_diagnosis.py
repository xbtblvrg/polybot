#!/usr/bin/env python3
"""Diagnose concentration for the directed 951b sticky paper identity."""

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

WALLET = "0x951bd740ef681d05891ca35440232488271d433e"
FOCUS_FP = "57d944ade6d26e904f6df3360e3e4425f528222ed82b7ec63fe27a1be756d9f0"
RUNNER_UP_FP = "5d113e3b966ec2645cafa46d78c95354b5727d842122659a28a04a2f90f51934"


def _cell(evidence: dict[str, Any], fingerprint: str) -> dict[str, Any]:
    return next(
        (
            row
            for row in evidence.get("cells") or []
            if isinstance(row, dict)
            and str((row.get("identity") or {}).get("wallet") or "").lower()
            == WALLET
            and str(row.get("wide_policy_fingerprint") or "") == fingerprint
        ),
        {},
    )


def _summary(cell: dict[str, Any]) -> dict[str, Any]:
    venue = cell.get("venue_executable_full_stream_rescore") or {}
    first = venue.get("first_half") or {}
    second = venue.get("second_half") or {}
    resolved = int(venue.get("resolved") or 0)
    residual = max(0, 200 - resolved)
    pnl = float(venue.get("post_fee_pnl_usd") or 0)
    excluding_top = float(venue.get("pnl_excluding_top_1_market") or 0)
    top_pnl = float(venue.get("top_1_market_pnl_usd") or (pnl - excluding_top))
    required_future_pnl = max(0.0, 2.0 * top_pnl - pnl)
    required_avg = required_future_pnl / residual if residual else None
    observed_avg = pnl / resolved if resolved else None
    plausibly_clears = bool(
        venue.get("concentration_admissible") is True
        or (
            residual > 0
            and pnl > 0
            and float(first.get("post_fee_pnl_usd") or 0) > 0
            and float(second.get("post_fee_pnl_usd") or 0) > 0
            and observed_avg is not None
            and required_avg is not None
            and observed_avg >= required_avg
        )
    )
    return {
        "wide_policy_fingerprint": cell.get("wide_policy_fingerprint"),
        "resolved": resolved,
        "residual_to_200": residual,
        "post_fee_pnl_usd": venue.get("post_fee_pnl_usd"),
        "pnl_excluding_top_1_market": venue.get("pnl_excluding_top_1_market"),
        "top_1_market_pnl_usd": top_pnl,
        "top_1_market_share_pct": venue.get("top_1_market_share_pct"),
        "distinct_markets": venue.get("distinct_markets"),
        "concentration_admissible": venue.get("concentration_admissible"),
        "concentration_deficits": venue.get("concentration_deficits") or [],
        "venue_reachable_share_pct": venue.get("venue_reachable_share_pct"),
        "first_half": {
            "resolved": first.get("resolved"),
            "post_fee_pnl_usd": first.get("post_fee_pnl_usd"),
            "concentration_admissible": first.get("concentration_admissible"),
        },
        "second_half": {
            "resolved": second.get("resolved"),
            "post_fee_pnl_usd": second.get("post_fee_pnl_usd"),
            "concentration_admissible": second.get("concentration_admissible"),
        },
        "residual_zero_clearance_projection": {
            "method": "hold current top-market PnL fixed; require future non-top PnL to make top share <50% and ex-top PnL positive",
            "required_future_non_top_pnl_usd": round(required_future_pnl, 6),
            "required_average_pnl_per_remaining_signal_usd": round(required_avg, 6)
            if required_avg is not None
            else None,
            "observed_average_pnl_per_resolved_signal_usd": round(observed_avg, 6)
            if observed_avg is not None
            else None,
            "plausibly_clears_at_residual_zero": plausibly_clears,
        },
    }


def build_report(
    evidence: dict[str, Any], now: dt.datetime | None = None
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.UTC)
    focus = _summary(_cell(evidence, FOCUS_FP))
    runner = _summary(_cell(evidence, RUNNER_UP_FP))
    projection = focus.get("residual_zero_clearance_projection") or {}
    if focus.get("concentration_admissible") is True:
        diagnosis = "CONCENTRATION_PASS"
    elif int(focus.get("resolved") or 0) < 200 and projection.get(
        "plausibly_clears_at_residual_zero"
    ):
        diagnosis = "THIN_SAMPLE_PLAUSIBLY_CLEARS_AT_RESIDUAL_ZERO"
    elif int(focus.get("resolved") or 0) < 200:
        diagnosis = "THIN_SAMPLE_NOT_YET_PROJECTED_TO_CLEAR"
    else:
        diagnosis = "CONCENTRATION_STRUCTURAL_AT_F1_SAMPLE"
    return {
        "schema_version": 1,
        "kind": "wide_951b_concentration_diagnosis",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": now.isoformat(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallet": WALLET,
        "focus": focus,
        "diagnosis": diagnosis,
        "runner_up_for_conditional_fable_disposition_only": runner,
        "retarget_trigger": "focus halves flip non-positive or venue residual-zero projection falls below 40%, while runner-up remains both-halves-positive and VR-pass",
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
        "--output",
        default="data/research/wide_951b_concentration_diagnosis_latest.json",
    )
    args = parser.parse_args()
    report = build_report(load_json(args.fingerprint_evidence, default={}) or {})
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
