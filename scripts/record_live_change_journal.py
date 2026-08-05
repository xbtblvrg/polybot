#!/usr/bin/env python3
"""Append a traceable live-path change journal row."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_JOURNAL = "data/research/live_change_journal.jsonl"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_entry(args: argparse.Namespace) -> dict[str, object]:
    if not args.enemy_id and not args.defect_id:
        raise ValueError("live-path journal entries require --enemy-id or --defect-id")
    if not args.justification.strip():
        raise ValueError("--justification is required")
    if not args.expected_effect.strip():
        raise ValueError("--expected-effect is required")
    if not args.rollback_command.strip():
        raise ValueError("--rollback-command is required")
    return {
        "ts": _utc_now(),
        "change_id": args.change_id,
        "mode": args.mode,
        "enemy_id": args.enemy_id,
        "defect_id": args.defect_id,
        "justification": args.justification,
        "expected_effect": args.expected_effect,
        "diff_or_commit_ref": args.diff_or_commit_ref,
        "golden_snapshot": args.golden_snapshot,
        "rollback_command": args.rollback_command,
        "touched_paths": args.touched_path,
        "measured_outcome": args.measured_outcome,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", default=DEFAULT_JOURNAL)
    parser.add_argument("--change-id", required=True)
    parser.add_argument("--mode", choices=["sos", "canary", "staged", "restore", "bootstrap"], required=True)
    parser.add_argument("--enemy-id", default="")
    parser.add_argument("--defect-id", default="")
    parser.add_argument("--justification", required=True)
    parser.add_argument("--expected-effect", required=True)
    parser.add_argument("--diff-or-commit-ref", default="")
    parser.add_argument("--golden-snapshot", required=True)
    parser.add_argument("--rollback-command", required=True)
    parser.add_argument("--touched-path", action="append", default=[])
    parser.add_argument("--measured-outcome", default="")
    args = parser.parse_args(argv)

    entry = build_entry(args)
    path = Path(args.journal)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")
    print(json.dumps({"status": "PASS", "journal": str(path), "change_id": args.change_id}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
