#!/usr/bin/env python3
"""Run the Polygon WS shadow probe as a resident, paper-only service."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PIDFILE = ROOT / "data/research/polygon_ws_shadow_service.pid"
DEFAULT_STATE = ROOT / "data/research/polygon_ws_shadow_service_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/polygon_orderfilled_ws_shadow_resident.jsonl"
DEFAULT_ORDERFILLED_OUTPUT = ROOT / "data/research/polygon_orderfilled_ws_orderfilled_only.jsonl"
DEFAULT_COMPARISON = ROOT / "data/research/polygon_ws_dataapi_active_set_comparison.jsonl"
DEFAULT_DATAAPI_FIRST_SEEN = ROOT / "data/research/dataapi_first_seen.jsonl"
DEFAULT_SERVICE_LOG = ROOT / "data/research/runtime_logs/polygon_ws_shadow_service.log"
DEFAULT_WSS_FALLBACK_URLS = ("wss://polygon.drpc.org",)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pidfile", type=Path, default=DEFAULT_PIDFILE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--orderfilled-output", type=Path, default=DEFAULT_ORDERFILLED_OUTPUT)
    parser.add_argument("--comparison-jsonl", type=Path, default=DEFAULT_COMPARISON)
    parser.add_argument("--dataapi-first-seen-jsonl", type=Path, default=DEFAULT_DATAAPI_FIRST_SEEN)
    parser.add_argument("--service-log", type=Path, default=DEFAULT_SERVICE_LOG)
    parser.add_argument("--duration-s", type=float, default=300.0)
    parser.add_argument("--sleep-s", type=float, default=2.0)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--http-poll-s", type=float, default=5.0)
    parser.add_argument("--ws-retry-s", type=float, default=2.0)
    parser.add_argument("--lookback-blocks", type=int, default=20)
    parser.add_argument("--max-runtime-s", type=float, default=0.0, help="0 means run forever.")
    parser.add_argument("--log-max-bytes", type=int, default=5 * 1024 * 1024)
    parser.add_argument(
        "--polygon-wss-fallback-url",
        action="append",
        default=[
            item.strip()
            for item in os.getenv("POLYGON_WSS_FALLBACK_URLS", ",".join(DEFAULT_WSS_FALLBACK_URLS)).split(",")
            if item.strip()
        ],
    )
    return parser.parse_args()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _rotate(path: Path, *, max_bytes: int) -> bool:
    if max_bytes <= 0 or not path.exists() or path.stat().st_size < max_bytes:
        return False
    rotated = path.with_name(path.name + ".1")
    if rotated.exists():
        rotated.unlink()
    path.replace(rotated)
    return True


def _command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "scripts/probe_polygon_orderfilled_ws.py"),
        "--duration-s",
        str(float(args.duration_s)),
        "--timeout-s",
        str(float(args.timeout_s)),
        "--http-poll-s",
        str(float(args.http_poll_s)),
        "--ws-retry-s",
        str(float(args.ws_retry_s)),
        "--lookback-blocks",
        str(int(args.lookback_blocks)),
        "--output",
        str(args.output),
        "--orderfilled-output",
        str(args.orderfilled_output),
        "--comparison-jsonl",
        str(args.comparison_jsonl),
        "--dataapi-first-seen-jsonl",
        str(args.dataapi_first_seen_jsonl),
    ]
    for url in args.polygon_wss_fallback_url or []:
        if str(url).strip():
            command.extend(["--polygon-wss-fallback-url", str(url).strip()])
    return command


def _service_process_count(*, exclude_pid: int | None = None) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,stat=,etime=,command="],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=3,
            check=False,
        )
    except Exception as exc:  # pragma: no cover - diagnostic only.
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "count": None, "rows": []}
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        if "scripts/run_polygon_ws_shadow_service.py" not in line:
            continue
        if "/bin/zsh -c" in line or "bash -lc" in line:
            continue
        parts = line.strip().split(None, 4)
        if len(parts) < 5:
            continue
        pid_raw, ppid_raw, stat, etime, command = parts
        try:
            pid = int(pid_raw)
            ppid = int(ppid_raw)
        except ValueError:
            continue
        if exclude_pid is not None and pid == int(exclude_pid):
            continue
        rows.append({"pid": pid, "ppid": ppid, "stat": stat, "etime": etime, "command": command[:240]})
    return {"status": "PASS", "count": len(rows), "rows": rows[:5]}


def main() -> int:
    args = parse_args()
    existing_pid = _read_pid(args.pidfile)
    if existing_pid and existing_pid != os.getpid() and _pid_alive(existing_pid):
        prior_state = {}
        if args.state.exists():
            try:
                prior_state = json.loads(args.state.read_text(encoding="utf-8"))
            except Exception:
                prior_state = {}
        generated_at = _utc_now()
        _write_json(
            args.state,
            {
                "kind": "polygon_ws_shadow_service",
                "status": "RUNNING",
                "pid": existing_pid,
                "existing_runner_pid": existing_pid,
                "started_at": prior_state.get("started_at") or generated_at,
                "generated_at": generated_at,
                "paper_only": True,
                "live_orders_allowed": False,
                "process_invariant": _service_process_count(exclude_pid=os.getpid()),
            },
        )
        return 0

    args.pidfile.parent.mkdir(parents=True, exist_ok=True)
    args.pidfile.write_text(f"{os.getpid()}\n", encoding="utf-8")
    args.service_log.parent.mkdir(parents=True, exist_ok=True)
    started_at = _utc_now()
    _write_json(
        args.state,
        {
            "kind": "polygon_ws_shadow_service",
            "status": "RUNNING",
            "pid": os.getpid(),
            "pidfile": str(args.pidfile),
            "started_at": started_at,
            "generated_at": started_at,
            "cycles": 0,
            "output": str(args.output),
            "orderfilled_output": str(args.orderfilled_output),
            "comparison_jsonl": str(args.comparison_jsonl),
            "dataapi_first_seen_jsonl": str(args.dataapi_first_seen_jsonl),
            "paper_only": True,
            "live_orders_allowed": False,
            "process_invariant": _service_process_count(),
        },
    )
    deadline = time.time() + float(args.max_runtime_s) if float(args.max_runtime_s) > 0 else None
    cycles = 0
    last_summary: dict[str, Any] = {}

    def _handle_signal(_signum: int, _frame: Any) -> None:
        _write_json(
            args.state,
            {
                "kind": "polygon_ws_shadow_service",
                "status": "STOPPING",
                "pid": os.getpid(),
                "started_at": started_at,
                "generated_at": _utc_now(),
                "cycles": cycles,
                "paper_only": True,
                "live_orders_allowed": False,
                "process_invariant": _service_process_count(),
            },
        )
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while deadline is None or time.time() < deadline:
        cycles += 1
        rotated_log = _rotate(args.service_log, max_bytes=int(args.log_max_bytes))
        command = _command(args)
        cycle_started = _utc_now()
        _write_json(
            args.state,
            {
                "kind": "polygon_ws_shadow_service",
                "status": "RUNNING",
                "pid": os.getpid(),
                "pidfile": str(args.pidfile),
                "started_at": started_at,
                "generated_at": cycle_started,
                "cycles": cycles,
                "current_command": command,
                "output": str(args.output),
                "orderfilled_output": str(args.orderfilled_output),
                "comparison_jsonl": str(args.comparison_jsonl),
                "dataapi_first_seen_jsonl": str(args.dataapi_first_seen_jsonl),
                "paper_only": True,
                "live_orders_allowed": False,
                "process_invariant": _service_process_count(),
            },
        )
        with args.service_log.open("a", encoding="utf-8") as log:
            log.write(json.dumps({"event": "polygon_ws_shadow_cycle_start", "ts": cycle_started, "cycle": cycles}) + "\n")
            log.flush()
            proc = subprocess.run(command, cwd=str(ROOT), stdout=log, stderr=log, text=True, check=False)
        last_summary = {
            "cycle": cycles,
            "cycle_started_at": cycle_started,
            "cycle_finished_at": _utc_now(),
            "returncode": proc.returncode,
            "rotated_service_log": rotated_log,
            "command": command,
        }
        _write_json(
            args.state,
            {
                "kind": "polygon_ws_shadow_service",
                "status": "PASS" if proc.returncode == 0 else "PROBE_ERROR",
                "pid": os.getpid(),
                "pidfile": str(args.pidfile),
                "started_at": started_at,
                "generated_at": _utc_now(),
                "cycles": cycles,
                "last_summary": last_summary,
                "output": str(args.output),
                "orderfilled_output": str(args.orderfilled_output),
                "comparison_jsonl": str(args.comparison_jsonl),
                "dataapi_first_seen_jsonl": str(args.dataapi_first_seen_jsonl),
                "paper_only": True,
                "live_orders_allowed": False,
                "process_invariant": _service_process_count(),
            },
        )
        if deadline is not None and time.time() >= deadline:
            break
        time.sleep(max(0.0, float(args.sleep_s)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
