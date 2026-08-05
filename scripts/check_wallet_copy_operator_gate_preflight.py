#!/usr/bin/env python3
"""Read-only preflight check for the wallet-copy source relay operator gate."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "data/research/wallet_copy_operator_gate_preflight_state.json"
DEFAULT_LIVE_EXECUTION_STATE = ROOT / "data/research/wallet_copy_live_execution_state.json"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _process_commands() -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["ps", "-axo", "pid,ppid,command"],
        capture_output=True,
        check=False,
        text=True,
        timeout=5,
    )
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines()[1:]:
        parts = line.strip().split(maxsplit=2)
        if len(parts) != 3:
            continue
        pid, ppid, command = parts
        try:
            rows.append({"pid": int(pid), "ppid": int(ppid), "command": command})
        except ValueError:
            continue
    return rows


def _arg_after(command: str, option: str) -> str | None:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if option not in tokens:
        return None
    index = tokens.index(option)
    return tokens[index + 1] if index + 1 < len(tokens) else None


def build_preflight_state(
    *,
    expected_upstream_retries: int,
    live_execution_state: Path = DEFAULT_LIVE_EXECUTION_STATE,
    now: str | None = None,
) -> dict[str, Any]:
    rows = _process_commands()
    live_guards = [row for row in rows if "scripts/run_wallet_copy_live_guard.py" in row["command"]]
    repair_writers = [row for row in rows if "scripts/run_wallet_copy_autonomous_repair.py" in row["command"]]
    relay_guards = [row for row in rows if "scripts/run_wallet_copy_source_relay_guard.py" in row["command"]]
    relays = [row for row in rows if "scripts/run_polymarket_jina_source_relay.py" in row["command"]]
    relay_retry_values = [_arg_after(row["command"], "--upstream-retries") for row in relays]
    relay_retry_ints: list[int] = []
    for value in relay_retry_values:
        try:
            relay_retry_ints.append(int(value))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            pass
    operator_action_observed = bool(relays) and all(value == expected_upstream_retries for value in relay_retry_ints)
    live_state = _load_json(live_execution_state)
    permission = live_state.get("runtime_permission") if isinstance(live_state.get("runtime_permission"), dict) else {}
    summary = live_state.get("summary") if isinstance(live_state.get("summary"), dict) else {}
    process_overlap_count = max(0, len(live_guards) - 1)
    status = "OPERATOR_GATE_PREFLIGHT_READY" if operator_action_observed else "OPERATOR_GATE_PREFLIGHT_BLOCKED"
    return {
        "generated_at": now or _utc_now(),
        "status": status,
        "primary_blocker_category": "source-route/live-source approval",
        "actual_live_trading": False,
        "guard_process_count": len(live_guards),
        "process_overlap_count": process_overlap_count,
        "writer_overlap_count": len(repair_writers),
        "live_execution": {
            "runtime_permission_status": permission.get("status"),
            "runtime_permission_can_trade": bool(permission.get("can_trade")),
            "live_orders_allowed": bool(permission.get("live_orders_allowed")),
            "paper_only": bool(permission.get("paper_only", True)),
            "latest_order_ts": summary.get("latest_order_ts"),
            "live_orders": summary.get("live_orders"),
            "filled_orders": summary.get("filled_orders"),
            "rejected_orders": summary.get("rejected_orders"),
            "submitted_orders": summary.get("submitted_orders"),
        },
        "source_relay_preflight": {
            "operator_action_observed": operator_action_observed,
            "source_relay_guard_pids": [row["pid"] for row in relay_guards],
            "source_relay_subprocess_pids": [row["pid"] for row in relays],
            "running_relay_upstream_retries": relay_retry_ints,
            "expected_relay_upstream_retries_after_operator_gate": expected_upstream_retries,
            "ready_for_post_gate_checks": operator_action_observed,
            "block_reason": None
            if operator_action_observed
            else "launchd-owned relay has not been kickstarted after plist update",
        },
        "post_gate_checks_decision": {
            "decision": "ready_to_run_post_gate_checks" if operator_action_observed else "do_not_run_post_gate_checks_yet",
            "reason": "relay subprocess has expected --upstream-retries"
            if operator_action_observed
            else "running relay arguments still prove the operator-gated kickstart has not happened",
        },
        "operator_gated_action_needed": {
            "command": "launchctl kickstart -k gui/$(id -u)/com.wallet-copy.polymarket-source-relay",
            "after_action_expected_change": f"run_polymarket_jina_source_relay.py subprocess includes --upstream-retries {expected_upstream_retries}",
        },
    }


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--live-execution-state", type=Path, default=DEFAULT_LIVE_EXECUTION_STATE)
    parser.add_argument("--expected-upstream-retries", type=int, default=2)
    parser.add_argument("--print", action="store_true", dest="print_state")
    args = parser.parse_args()

    state = build_preflight_state(
        expected_upstream_retries=args.expected_upstream_retries,
        live_execution_state=args.live_execution_state,
    )
    _write_state(args.state, state)
    if args.print_state:
        print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if state.get("status") == "OPERATOR_GATE_PREFLIGHT_READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
