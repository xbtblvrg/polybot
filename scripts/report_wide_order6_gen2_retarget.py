#!/usr/bin/env python3
"""Apply the pre-authorized ORDER6 gen2 sticky-focus retarget rule."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def evaluate(gen1: dict[str, Any], gen2: dict[str, Any]) -> dict[str, Any]:
    before = gen1.get("focus") or {}
    after = gen2.get("focus") or {}
    runner = gen2.get("runner_up_for_conditional_fable_disposition_only") or {}
    resolved_gain = int(after.get("resolved") or 0) - int(before.get("resolved") or 0)
    residual_drop = int(before.get("residual_to_200") or 0) - int(after.get("residual_to_200") or 0)
    runner_first = float(((runner.get("first_half") or {}).get("post_fee_pnl_usd") or 0))
    runner_second = float(((runner.get("second_half") or {}).get("post_fee_pnl_usd") or 0))
    checks = {
        "velocity_near_zero": resolved_gain < 5 or residual_drop < 5,
        "focus_projection_still_false": not bool(
            ((after.get("residual_zero_clearance_projection") or {}).get("plausibly_clears_at_residual_zero"))
        ),
        "runner_both_halves_positive": runner_first > 0 and runner_second > 0,
        "runner_venue_reachable_pass": float(runner.get("venue_reachable_share_pct") or 0) >= 40.0,
        "runner_projection_true": bool(
            ((runner.get("residual_zero_clearance_projection") or {}).get("plausibly_clears_at_residual_zero"))
        ),
    }
    applied = all(checks.values())
    return {
        "schema_version": 1,
        "kind": "wide_order6_gen2_mechanical_retarget",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "decision": "RETARGET_RUNNER_UP" if applied else "HOLD_FOCUS_CONTINUE_GEN3",
        "retarget_applied": applied,
        "wallet": gen2.get("wallet"),
        "from_fingerprint": before.get("wide_policy_fingerprint"),
        "to_fingerprint": runner.get("wide_policy_fingerprint") if applied else before.get("wide_policy_fingerprint"),
        "gen1": {"resolved": before.get("resolved"), "residual_to_200": before.get("residual_to_200")},
        "gen2": {"resolved": after.get("resolved"), "residual_to_200": after.get("residual_to_200")},
        "velocity": {"resolved_gain": resolved_gain, "residual_drop": residual_drop},
        "checks": checks,
        "bars_mutated": False,
        "roster_mutated": False,
        "admission_applied": False,
        "effective_scope": "next naturally generated WIDE paper manifest; no supervisor restart",
    }


def _git_json(revision: str, path: str) -> dict[str, Any]:
    raw = subprocess.check_output(["git", "show", f"{revision}:{path}"], cwd=ROOT, text=True)
    return json.loads(raw)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gen1-revision", default="adcd0962")
    parser.add_argument("--gen2", default="data/research/wide_951b_concentration_diagnosis_latest.json")
    parser.add_argument("--output", default="data/research/wide_order6_gen2_retarget_latest.json")
    args = parser.parse_args()
    report = evaluate(
        _git_json(args.gen1_revision, "data/research/wide_951b_concentration_diagnosis_latest.json"),
        load_json(args.gen2, default={}),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
