#!/usr/bin/env python3
"""Guard the local read-only Polymarket relay used for paper/proof sources."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "data/research/wallet_copy_source_relay_guard_state.json"
DEFAULT_STDOUT = ROOT / "data/research/wallet_copy_jina_source_relay.out"
DEFAULT_HEALTH_URL = "http://127.0.0.1:8787/healthz"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _relay_pids(port: int) -> list[int]:
    try:
        completed = subprocess.run(
            ["lsof", "-tiTCP:%d" % port, "-sTCP:LISTEN"],
            capture_output=True,
            check=False,
            text=True,
            timeout=3,
        )
    except Exception:
        return []
    pids: list[int] = []
    for line in completed.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pids.append(int(line))
        except ValueError:
            continue
    return sorted(set(pids))


def _pid_command(pid: int) -> str:
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            check=False,
            text=True,
            timeout=3,
        )
    except Exception:
        return ""
    return completed.stdout.strip()


def _relay_config_check(pids: list[int], args: argparse.Namespace) -> dict[str, Any]:
    expected_options = {
        "--bind": str(args.bind),
        "--port": str(args.port),
        "--cache-ttl-s": str(args.cache_ttl_s),
        "--timeout-s": str(args.relay_timeout_s),
        "--max-bytes": str(args.max_bytes),
        "--upstream-retries": str(args.upstream_retries),
        "--upstream-min-interval-s": str(args.upstream_min_interval_s),
        "--max-upstream-in-flight": str(args.max_upstream_in_flight),
        "--busy-timeout-s": str(args.busy_timeout_s),
    }
    details: list[dict[str, Any]] = []
    commands: dict[str, str] = {}
    for pid in pids:
        command = _pid_command(pid)
        commands[str(pid)] = command[:1000]
        try:
            tokens = shlex.split(command)
        except ValueError:
            details.append({"pid": pid, "status": "DRIFT", "detail": "command_parse_failed"})
            continue
        for option, expected in expected_options.items():
            if option not in tokens:
                details.append({"pid": pid, "status": "DRIFT", "option": option, "expected": expected, "actual": None})
                continue
            index = tokens.index(option)
            actual = tokens[index + 1] if index + 1 < len(tokens) else None
            if actual != expected:
                details.append({"pid": pid, "status": "DRIFT", "option": option, "expected": expected, "actual": actual})
    return {
        "status": "DRIFT" if details else "PASS",
        "details": details[:20],
        "commands": commands,
    }


def _healthcheck(url: str, timeout_s: float) -> dict[str, Any]:
    started = time.perf_counter()
    request = Request(url, headers={"Accept": "application/json,text/plain,*/*", "Connection": "close"})
    try:
        with urlopen(request, timeout=timeout_s) as response:  # noqa: S310 - fixed local relay URL from config.
            body = response.read(256)
            status = int(getattr(response, "status", 0) or 0)
    except Exception as exc:
        return {
            "status": "FAIL",
            "elapsed_s": round(time.perf_counter() - started, 3),
            "exception": type(exc).__name__,
            "detail": str(exc)[:300],
            "url": url,
        }
    return {
        "status": "PASS" if 200 <= status < 300 else "FAIL",
        "elapsed_s": round(time.perf_counter() - started, 3),
        "http_status": status,
        "sample_bytes": len(body),
        "url": url,
    }


def _terminate_pids(pids: list[int], *, grace_s: float) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            actions.append({"pid": pid, "signal": "TERM", "status": "SENT"})
        except ProcessLookupError:
            actions.append({"pid": pid, "signal": "TERM", "status": "MISSING"})
        except PermissionError as exc:
            actions.append({"pid": pid, "signal": "TERM", "status": "ERROR", "detail": str(exc)})
    if pids:
        time.sleep(max(0.0, grace_s))
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
            actions.append({"pid": pid, "signal": "KILL", "status": "SENT"})
        except ProcessLookupError:
            pass
        except PermissionError as exc:
            actions.append({"pid": pid, "signal": "KILL", "status": "ERROR", "detail": str(exc)})
    return actions


def _start_relay(args: argparse.Namespace) -> subprocess.Popen[Any]:
    args.stdout_log.parent.mkdir(parents=True, exist_ok=True)
    log_handle = args.stdout_log.open("a", encoding="utf-8")
    cmd = [
        sys.executable,
        str(ROOT / "scripts/run_polymarket_jina_source_relay.py"),
        "--bind",
        args.bind,
        "--port",
        str(args.port),
        "--cache-ttl-s",
        str(args.cache_ttl_s),
        "--timeout-s",
        str(args.relay_timeout_s),
        "--max-bytes",
        str(args.max_bytes),
        "--upstream-retries",
        str(args.upstream_retries),
        "--upstream-min-interval-s",
        str(args.upstream_min_interval_s),
        "--max-upstream-in-flight",
        str(args.max_upstream_in_flight),
        "--busy-timeout-s",
        str(args.busy_timeout_s),
    ]
    return subprocess.Popen(cmd, cwd=str(ROOT), stdout=log_handle, stderr=log_handle, start_new_session=True)


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    previous_state: dict[str, Any] = {}
    if args.state.exists():
        try:
            previous_state = json.loads(args.state.read_text(encoding="utf-8"))
        except Exception:
            previous_state = {}
    pids_before = _relay_pids(args.port)
    health_before = _healthcheck(args.health_url, args.health_timeout_s) if pids_before else {"status": "FAIL", "detail": "not_listening"}
    relay_config_check = _relay_config_check(pids_before, args) if pids_before else {"status": "MISSING", "details": []}
    actions: list[dict[str, Any]] = []
    restarted = False
    previous_failure_streak = int(previous_state.get("failure_streak") or 0)
    failure_streak = 0 if health_before.get("status") == "PASS" else previous_failure_streak + 1
    should_restart = bool(pids_before) and (
        failure_streak >= args.restart_after_failures or relay_config_check.get("status") == "DRIFT"
    )
    if not pids_before or should_restart:
        actions.extend(_terminate_pids(pids_before, grace_s=args.terminate_grace_s))
        process = _start_relay(args)
        restarted = True
        actions.append({"action": "start_relay", "pid": process.pid, "status": "STARTED"})
        time.sleep(max(0.0, args.startup_grace_s))
    pids_after = _relay_pids(args.port)
    health_after = _healthcheck(args.health_url, args.health_timeout_s) if pids_after else {"status": "FAIL", "detail": "not_listening_after_start"}
    status = "PASS" if health_after.get("status") == "PASS" else "CORRECTION"
    state = {
        "generated_at": _utc_now(),
        "kind": "wallet_copy_source_relay_guard",
        "status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "port": args.port,
        "health_url": args.health_url,
        "health_timeout_s": args.health_timeout_s,
        "relay_timeout_s": args.relay_timeout_s,
        "upstream_retries": args.upstream_retries,
        "upstream_min_interval_s": args.upstream_min_interval_s,
        "max_upstream_in_flight": args.max_upstream_in_flight,
        "busy_timeout_s": args.busy_timeout_s,
        "pids_before": pids_before,
        "pids_after": pids_after,
        "relay_config_check": relay_config_check,
        "failure_streak_before": previous_failure_streak,
        "failure_streak": 0 if health_after.get("status") == "PASS" else failure_streak,
        "restart_after_failures": args.restart_after_failures,
        "should_restart": should_restart,
        "health_before": health_before,
        "health_after": health_after,
        "restarted": restarted,
        "actions": actions,
    }
    _write_state(args.state, state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--health-url", default=DEFAULT_HEALTH_URL)
    parser.add_argument("--health-timeout-s", type=float, default=24.0)
    parser.add_argument("--relay-timeout-s", type=float, default=8.0)
    parser.add_argument("--cache-ttl-s", type=float, default=8.0)
    parser.add_argument("--max-bytes", type=int, default=16_000_000)
    parser.add_argument("--upstream-retries", type=int, default=2)
    parser.add_argument("--upstream-min-interval-s", type=float, default=0.25)
    parser.add_argument("--max-upstream-in-flight", type=int, default=4)
    parser.add_argument("--busy-timeout-s", type=float, default=0.75)
    parser.add_argument("--terminate-grace-s", type=float, default=1.0)
    parser.add_argument("--startup-grace-s", type=float, default=2.0)
    parser.add_argument("--sleep-s", type=float, default=30.0)
    parser.add_argument("--cycles", type=int, default=1, help="0 means forever")
    parser.add_argument("--restart-after-failures", type=int, default=2)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--stdout-log", type=Path, default=DEFAULT_STDOUT)
    parser.add_argument("--print", action="store_true", dest="print_state")
    args = parser.parse_args()

    cycle = 0
    last_state: dict[str, Any] = {}
    while True:
        cycle += 1
        last_state = run_once(args)
        if args.print_state:
            print(json.dumps(last_state, indent=2, sort_keys=True), flush=True)
        if args.cycles and cycle >= args.cycles:
            break
        time.sleep(max(1.0, args.sleep_s))
    return 0 if last_state.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
