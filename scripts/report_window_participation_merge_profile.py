#!/usr/bin/env python3
"""Profile the guard window-participation merge against its retained live frame."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_wallet_copy_live_guard import _merge_window_participation
from src.wallet_copy.store import atomic_write_json, load_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--guard-state",
        default="data/research/wallet_copy_live_guard_state.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/window_participation_merge_profile_latest.json",
    )
    parser.add_argument(
        "--baseline",
        default="data/research/window_participation_merge_profile_baseline.json",
    )
    args = parser.parse_args()
    state = load_json(args.guard_state, default={})
    previous = (
        state.get("window_participation")
        if isinstance(state.get("window_participation"), dict)
        else {}
    )
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report = _merge_window_participation(
        previous,
        {},
        generated_at=generated_at,
        set_generation_id=str(previous.get("set_generation_id") or "profile"),
        active_set_rtds_premerge={},
    )
    profile = report.get("merge_profile") or {}
    baseline_artifact = load_json(args.baseline, default={})
    baseline_profile = (
        baseline_artifact.get("profile")
        if isinstance(baseline_artifact.get("profile"), dict)
        else {}
    )
    baseline_duration_s = float(baseline_profile.get("total_duration_s") or 0.0)
    optimized_duration_s = float(profile.get("total_duration_s") or 0.0)
    artifact = {
        "schema_version": 1,
        "kind": "window_participation_merge_profile",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": generated_at,
        "source": args.guard_state,
        "paper_measurement_only": True,
        "live_orders_allowed": False,
        "freshness_budget_s": 30.0,
        "profile": profile,
        "baseline": {
            "source": args.baseline,
            "total_duration_s": baseline_duration_s or None,
            "top_stage": baseline_profile.get("top_stage"),
            "top_stage_duration_s": baseline_profile.get("top_stage_duration_s"),
        },
        "improvement": {
            "duration_reduction_s": (
                round(baseline_duration_s - optimized_duration_s, 6)
                if baseline_duration_s > 0
                else None
            ),
            "speedup_x": (
                round(baseline_duration_s / optimized_duration_s, 6)
                if baseline_duration_s > 0 and optimized_duration_s > 0
                else None
            ),
        },
        "budget_share_pct": round(
            100.0 * float(profile.get("total_duration_s") or 0.0) / 30.0,
            6,
        ),
        "verdict": (
            "WINDOW_PARTICIPATION_MERGE_OPTIMIZED_BELOW_BUDGET"
            if baseline_duration_s > optimized_duration_s
            and optimized_duration_s < 30.0
            else "WINDOW_PARTICIPATION_MERGE_EXCEEDS_FRESHNESS_BUDGET"
            if float(profile.get("total_duration_s") or 0.0) >= 30.0
            else "WINDOW_PARTICIPATION_MERGE_PROFILED_BELOW_FULL_BUDGET"
        ),
    }
    atomic_write_json(args.output, artifact)
    print(json.dumps(artifact, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
