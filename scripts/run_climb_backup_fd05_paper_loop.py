#!/usr/bin/env python3
"""Keep the isolated fd05 climb-backup paper measurement clock fresh."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "data/research/wide_exact_policy_manifest_climb_backup_fd05.json"
STATE = "data/research/wide_exact_policy_paper_state_climb_backup_fd05.json"
LEDGER = "data/research/wide_exact_policy_paper_orders_climb_backup_fd05.jsonl"
SUPERVISOR = "data/research/wide_prospective_supervisor_state_climb_backup_fd05.json"
POLYGON = (
    "data/research/"
    "polygon_orderfilled_ws_capture_alpha_decay_wide_20260727T091131Z.jsonl"
)
ALPHA = "data/research/alpha_decay_report_wide_20260727T091131Z.json"
RUN_ID = "wide_climb_backup_fd05_20260727T101000Z"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def isolated_write_path(value: str) -> str:
    path = Path(value)
    if "climb_backup_fd05" not in path.name:
        raise ValueError(f"refusing non-isolated write path: {value}")
    return value


def validate_manifest(path: Path) -> None:
    manifest = json.loads(path.read_text())
    if manifest.get("paper_only") is not True:
        raise ValueError("backup manifest must be paper_only=true")
    if manifest.get("live_orders_allowed") is not False:
        raise ValueError("backup manifest must have live_orders_allowed=false")
    rows = manifest.get("capture_watch_wallets") or []
    if len(rows) != 1 or rows[0].get("paper_measurement_only") is not True:
        raise ValueError("backup manifest must contain one paper-only capture row")
    if rows[0].get("promotion_authority") is not False:
        raise ValueError("backup manifest must have promotion_authority=false")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-s", type=float, default=90.0)
    parser.add_argument("--iterations", type=int, default=0)
    parser.add_argument("--manifest", default=MANIFEST)
    parser.add_argument("--state", default=STATE)
    parser.add_argument("--ledger", default=LEDGER)
    parser.add_argument("--supervisor-state", default=SUPERVISOR)
    parser.add_argument("--polygon-jsonl", default=POLYGON)
    parser.add_argument("--alpha-report", default=ALPHA)
    parser.add_argument("--run-id", default=RUN_ID)
    args = parser.parse_args()

    state_path = isolated_write_path(args.state)
    ledger_path = isolated_write_path(args.ledger)
    supervisor_path = isolated_write_path(args.supervisor_state)
    validate_manifest(ROOT / args.manifest)

    tick_count = 0
    while args.iterations <= 0 or tick_count < args.iterations:
        started_at = utc_now()
        command = [
            sys.executable,
            "scripts/reconcile_wide_exact_policy_paper.py",
            "--run-id",
            args.run_id,
            "--manifest",
            args.manifest,
            "--polygon-jsonl",
            args.polygon_jsonl,
            "--alpha-report",
            args.alpha_report,
            "--token-metadata-cache",
            "data/research/wide_token_metadata_cache.json",
            "--resolutions",
            "data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
            "--state",
            state_path,
            "--ledger",
            ledger_path,
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=180,
            check=False,
        )
        tick_count += 1
        atomic_write(
            ROOT / supervisor_path,
            {
                "schema_version": 1,
                "kind": "climb_backup_fd05_paper_supervisor",
                "flow_stage": "OBSERVE/LEARN/PROMOTE",
                "paper_only": True,
                "live_orders_allowed": False,
                "promotion_authority": False,
                "status": "RUNNING" if completed.returncode in (0, 2) else "TICK_FAILED",
                "pid": os.getpid(),
                "tick_count": tick_count,
                "tick_started_at": started_at,
                "tick_completed_at": utc_now(),
                "last_returncode": completed.returncode,
                "stdout_tail": completed.stdout[-1000:],
                "stderr_tail": completed.stderr[-1000:],
                "interval_s": args.interval_s,
                "manifest": args.manifest,
                "state": state_path,
                "ledger": ledger_path,
            },
        )
        if completed.returncode not in (0, 2):
            return completed.returncode
        if args.iterations <= 0 or tick_count < args.iterations:
            time.sleep(max(1.0, args.interval_s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
