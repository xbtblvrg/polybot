#!/usr/bin/env python3
"""Run post-kickstart wallet-copy proof steps only after source-relay preflight is ready."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "data/research/wallet_copy_post_kickstart_repair_state.json"
LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID = "OP-LIVE-20260703-BELA"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.check_wallet_copy_operator_gate_preflight import build_preflight_state


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_command(command: list[str], timeout_s: float) -> dict[str, Any]:
    started = datetime.now(UTC)
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "command": command,
            "status": "TIMEOUT",
            "timeout_s": timeout_s,
            "stdout_tail": (exc.stdout or "")[-2000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-2000:] if isinstance(exc.stderr, str) else "",
            "started_at": started.isoformat(),
            "finished_at": _utc_now(),
        }
    return {
        "command": command,
        "status": "PASS" if completed.returncode == 0 else "FAIL",
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
        "started_at": started.isoformat(),
        "finished_at": _utc_now(),
    }


def build_plan_commands() -> list[list[str]]:
    return [
        [sys.executable, "scripts/probe_polymarket_source_routes.py", "--write-state"],
        [sys.executable, "scripts/run_wallet_copy_hotlane_tick.py", "--ticks", "1", "--wallets-per-tick", "4"],
        [
            sys.executable,
            "scripts/run_wallet_copy_profit_engine.py",
            "--live-today-sprint-operator-approval-id",
            LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID,
        ],
        [sys.executable, "scripts/select_wallet_copy_strategy_direction.py"],
    ]


def run_repair(args: argparse.Namespace) -> dict[str, Any]:
    preflight = build_preflight_state(expected_upstream_retries=args.expected_upstream_retries)
    if preflight.get("status") != "OPERATOR_GATE_PREFLIGHT_READY":
        state = {
            "generated_at": _utc_now(),
            "status": "POST_KICKSTART_REPAIR_BLOCKED_BY_PREFLIGHT",
            "paper_only": True,
            "live_orders_allowed": False,
            "preflight": preflight,
            "commands_run": [],
            "next_action": "operator must kickstart source relay and rerun preflight",
        }
        _write_state(args.state, state)
        return state

    commands_run: list[dict[str, Any]] = []
    for command in build_plan_commands():
        result = _run_command(command, timeout_s=args.command_timeout_s)
        commands_run.append(result)
        if result.get("status") != "PASS" and not args.continue_on_failure:
            break
    state = {
        "generated_at": _utc_now(),
        "status": "POST_KICKSTART_REPAIR_COMPLETED"
        if commands_run and all(item.get("status") == "PASS" for item in commands_run)
        else "POST_KICKSTART_REPAIR_CORRECTION",
        "paper_only": True,
        "live_orders_allowed": False,
        "preflight": preflight,
        "commands_run": commands_run,
        "next_action": "inspect refreshed source-route, hotlane, profit, and strategy states",
    }
    _write_state(args.state, state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--expected-upstream-retries", type=int, default=2)
    parser.add_argument("--command-timeout-s", type=float, default=180.0)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--print", action="store_true", dest="print_state")
    args = parser.parse_args()
    state = run_repair(args)
    if args.print_state:
        print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if state.get("status") == "POST_KICKSTART_REPAIR_COMPLETED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
