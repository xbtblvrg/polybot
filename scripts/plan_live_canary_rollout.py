#!/usr/bin/env python3
"""Create a producing-protected canary rollout plan without mutating live state."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_plan(args: argparse.Namespace) -> dict[str, object]:
    if not args.enemy_id and not args.defect_id:
        raise ValueError("canary plans require --enemy-id or --defect-id")
    if not args.justification.strip():
        raise ValueError("--justification is required")
    plan = {
        "kind": "producing_protected_canary_rollout_plan",
        "schema_version": 1,
        "created_at": _utc_now(),
        "change_id": args.change_id,
        "status": "PLAN_ONLY_NO_LIVE_MUTATION",
        "golden_snapshot": args.golden_snapshot,
        "enemy_id": args.enemy_id,
        "defect_id": args.defect_id,
        "justification": args.justification,
        "expected_effect": args.expected_effect,
        "slice": {"type": args.slice_type, "value": args.slice_value},
        "stages": [
            {"name": "canary", "scope": args.slice_value, "pass_metric": args.pass_metric, "pass_threshold": args.pass_threshold},
            {"name": "half", "scope": "50_percent_after_canary_pass", "pass_metric": args.pass_metric, "pass_threshold": args.pass_threshold},
            {"name": "full", "scope": "100_percent_after_half_pass", "pass_metric": args.pass_metric, "pass_threshold": args.pass_threshold},
        ],
        "auto_revert": {
            "metric": args.auto_revert_metric,
            "threshold": args.auto_revert_threshold,
            "rollback_command": args.rollback_command,
        },
        "required_journal": {
            "path": "data/research/live_change_journal.jsonl",
            "mode": "canary",
            "change_id": args.change_id,
        },
        "live_mutation": False,
    }
    return plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--change-id", required=True)
    parser.add_argument("--golden-snapshot", required=True)
    parser.add_argument("--enemy-id", default="")
    parser.add_argument("--defect-id", default="")
    parser.add_argument("--justification", required=True)
    parser.add_argument("--expected-effect", required=True)
    parser.add_argument("--slice-type", choices=["member", "price_bucket", "min_size"], required=True)
    parser.add_argument("--slice-value", required=True)
    parser.add_argument("--pass-metric", required=True)
    parser.add_argument("--pass-threshold", required=True)
    parser.add_argument("--auto-revert-metric", required=True)
    parser.add_argument("--auto-revert-threshold", required=True)
    parser.add_argument("--rollback-command", required=True)
    args = parser.parse_args(argv)

    plan = build_plan(args)
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": "PASS", "plan": str(path), "change_id": args.change_id}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
