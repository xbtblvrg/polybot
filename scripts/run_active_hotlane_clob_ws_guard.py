#!/usr/bin/env python3
"""Continuously capture CLOB market WebSocket rows for active hot-lane tokens."""

from __future__ import annotations

import argparse
from collections import deque
import gc
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.source_route import (  # noqa: E402
    DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE,
    source_route_allows_measurement,
    source_route_probe_progress_blocker,
)
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json  # noqa: E402


PYTHON = "python3"
DEFAULT_OUTPUT_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_OUTPUT_KEEP_TAIL_BYTES = 16 * 1024 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="data/research/wallet_copy_active_hotlane_live_tracking_state.json")
    parser.add_argument("--event-log", default="data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl")
    parser.add_argument("--asset-ids-output", default="data/research/wallet_copy_active_hotlane_clob_asset_ids.json")
    parser.add_argument("--output", default="data/research/clob_market_ws_events.jsonl")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_active_hotlane_clob_ws_guard_state.json")
    parser.add_argument("--source-route-state", default="data/research/wallet_copy_source_route_state.json")
    parser.add_argument(
        "--autonomous-repair-command-progress-state",
        default=DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE,
    )
    parser.add_argument("--duration-s", type=float, default=55.0)
    parser.add_argument("--sleep-s", type=float, default=0.2)
    parser.add_argument("--route-block-sleep-s", type=float, default=60.0)
    parser.add_argument("--max-ids", type=int, default=80)
    parser.add_argument("--event-log-tail", type=int, default=3000)
    parser.add_argument("--max-state-bytes", type=int, default=20 * 1024 * 1024)
    parser.add_argument("--output-max-bytes", type=int, default=DEFAULT_OUTPUT_MAX_BYTES)
    parser.add_argument("--output-keep-tail-bytes", type=int, default=DEFAULT_OUTPUT_KEEP_TAIL_BYTES)
    parser.add_argument(
        "--output-rotation-state",
        default="data/research/wallet_copy_clob_market_ws_rotation_state.json",
    )
    parser.add_argument(
        "--output-rotation-event-log",
        default="data/research/wallet_copy_runtime_log_rotation_events.jsonl",
    )
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def _collect_token_ids(obj: Any, out: set[str]) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in {"token_id", "asset_id"} and value is not None:
                token = str(value)
                if token.isdigit() and len(token) > 20:
                    out.add(token)
            _collect_token_ids(value, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_token_ids(item, out)


def export_asset_ids(args: argparse.Namespace) -> list[str]:
    token_ids: set[str] = set()
    state_path = Path(args.state)
    if state_path.exists() and state_path.stat().st_size <= max(0, int(args.max_state_bytes)):
        try:
            _collect_token_ids(json.loads(state_path.read_text(encoding="utf-8")), token_ids)
        except Exception:
            pass

    event_log = Path(args.event_log)
    if event_log.exists():
        try:
            with event_log.open("r", encoding="utf-8", errors="ignore") as handle:
                lines = deque(handle, maxlen=max(0, int(args.event_log_tail)))
        except Exception:
            lines = []
        for line in lines:
            try:
                _collect_token_ids(json.loads(line), token_ids)
            except Exception:
                continue

    selected = sorted(token_ids)[: max(1, int(args.max_ids))]
    Path(args.asset_ids_output).write_text(json.dumps({"asset_ids": selected}, indent=2), encoding="utf-8")
    return selected


def _rotate_output_if_needed(args: argparse.Namespace) -> dict[str, Any]:
    target = Path(getattr(args, "output", "data/research/clob_market_ws_events.jsonl"))
    max_bytes = int(getattr(args, "output_max_bytes", DEFAULT_OUTPUT_MAX_BYTES) or 0)
    keep_tail_bytes = int(getattr(args, "output_keep_tail_bytes", DEFAULT_OUTPUT_KEEP_TAIL_BYTES) or 0)
    state_path = getattr(args, "output_rotation_state", "data/research/wallet_copy_clob_market_ws_rotation_state.json")
    event_log_path = getattr(
        args,
        "output_rotation_event_log",
        "data/research/wallet_copy_runtime_log_rotation_events.jsonl",
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "wallet_copy_clob_market_ws_rotation",
        "generated_at": utc_now_iso(),
        "path": str(target),
        "max_bytes": max_bytes,
        "keep_tail_bytes": keep_tail_bytes,
        "paper_only": True,
        "live_orders_allowed": False,
        "status": "PASS",
        "action": "none",
        "size_before_bytes": 0,
        "size_after_bytes": 0,
        "source_of_truth_note": (
            "raw market websocket evidence is bounded to a recent tail; live admission still requires "
            "candidate-scoped current-poll Data/Gamma/CLOB truth and exact CopyIntent lifecycle evidence"
        ),
    }
    if max_bytes <= 0:
        payload["status"] = "DISABLED"
        payload["reason"] = "output_max_bytes_not_positive"
        atomic_write_json(state_path, payload)
        return payload
    if not target.exists():
        payload["reason"] = "output_missing_no_rotation_needed"
        atomic_write_json(state_path, payload)
        return payload

    size_before = target.stat().st_size
    payload["size_before_bytes"] = int(size_before)
    if size_before <= max_bytes:
        payload["reason"] = "below_cap"
        payload["size_after_bytes"] = int(size_before)
        atomic_write_json(state_path, payload)
        return payload

    keep_tail_bytes = max(0, min(keep_tail_bytes, size_before))
    tail = b""
    if keep_tail_bytes:
        with target.open("rb") as handle:
            handle.seek(max(0, size_before - keep_tail_bytes))
            tail = handle.read(keep_tail_bytes)
        if tail and not tail.startswith(b"\n"):
            newline_index = tail.find(b"\n")
            if newline_index >= 0:
                tail = tail[newline_index + 1 :]

    with target.open("wb") as handle:
        if tail:
            handle.write(tail)

    size_after = target.stat().st_size
    payload.update(
        {
            "status": "REPAIRED",
            "action": "copytruncate_tail",
            "reason": "above_cap_tail_preserved",
            "size_after_bytes": int(size_after),
            "bytes_removed": int(size_before - size_after),
        }
    )
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    return payload


def capture_once(args: argparse.Namespace) -> int:
    rotation_before = _rotate_output_if_needed(args)
    asset_ids = export_asset_ids(args)
    if not asset_ids:
        payload = {
            "status": "NO_ASSET_IDS",
            "output": args.asset_ids_output,
            "output_rotation": rotation_before,
            "paper_only": True,
            "live_orders_allowed": False,
        }
        atomic_write_json(args.guard_state, payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 2
    result = subprocess.run(
        [
            PYTHON,
            "scripts/run_clob_market_ws_capture.py",
            "--asset-ids-file",
            str(args.asset_ids_output),
            "--output",
            str(args.output),
            "--duration-s",
            str(max(1.0, float(args.duration_s))),
        ],
        cwd=ROOT,
        text=True,
        check=False,
    )
    rotation_after = _rotate_output_if_needed(args)
    atomic_write_json(
        args.guard_state,
        {
            "status": "PASS" if result.returncode == 0 else "CAPTURE_FAILED",
            "returncode": int(result.returncode),
            "asset_ids": len(asset_ids),
            "output": args.output,
            "output_rotation": rotation_after,
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
    return int(result.returncode)


def _source_route_blocked(args: argparse.Namespace) -> dict[str, Any] | None:
    probe_blocker = source_route_probe_progress_blocker(
        getattr(args, "autonomous_repair_command_progress_state", None)
    )
    if probe_blocker:
        return {
            **probe_blocker,
            "status": "SOURCE_ROUTE_BLOCKED_SLEEP",
            "source_route_status": "SOURCE_ROUTE_PROBE_RUNNING",
            "source_route_state": args.source_route_state,
            "blockers": ["source_route_probe_running"],
            "route_class_counts": {},
            "source_proxy_configured": None,
            "external_route_required": None,
            "next_action": "pause CLOB WS capture until the fresh source-route probe finishes",
        }
    state = load_json(args.source_route_state, default={})
    if not isinstance(state, dict):
        return {
            "status": "SOURCE_ROUTE_UNKNOWN",
            "source_route_state": args.source_route_state,
            "paper_only": True,
            "live_orders_allowed": False,
        }
    status = str(state.get("status") or "")
    if not status or source_route_allows_measurement(status):
        return None
    return {
        "status": "SOURCE_ROUTE_BLOCKED_SLEEP",
        "source_route_status": status,
        "source_route_state": args.source_route_state,
        "route_class_counts": state.get("route_class_counts") or {},
        "source_proxy_configured": state.get("source_proxy_configured"),
        "external_route_required": state.get("external_route_required"),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def main() -> int:
    args = parse_args()
    last_rc = 0
    while True:
        output_rotation = _rotate_output_if_needed(args)
        blocked = _source_route_blocked(args)
        if blocked:
            blocked["output_rotation"] = output_rotation
            atomic_write_json(args.guard_state, blocked)
            print(json.dumps(blocked, sort_keys=True), flush=True)
            if args.once:
                return 2
            time.sleep(max(float(args.sleep_s), float(args.route_block_sleep_s)))
            continue
        last_rc = capture_once(args)
        gc.collect()
        if args.once:
            return last_rc
        time.sleep(max(0.0, float(args.sleep_s)))


if __name__ == "__main__":
    raise SystemExit(main())
