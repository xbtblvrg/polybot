#!/usr/bin/env python3
"""Capture selected-wallet Data API events through the proven poller path."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-wallet", action="append", default=[])
    parser.add_argument("--duration-s", type=float, default=7500.0)
    parser.add_argument("--poll-interval-s", type=float, default=15.0)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--history-state", required=True)
    parser.add_argument("--history-window-index", required=True)
    parser.add_argument("--wallet-event-log", required=True)
    parser.add_argument("--dataapi-first-seen-jsonl", required=True)
    parser.add_argument("--observation-watermark-state", required=True)
    parser.add_argument("--poll-state", required=True)
    parser.add_argument("--capture-state", required=True)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=8)
    return parser.parse_args()


def _wallets(values: list[str]) -> list[str]:
    return sorted({value.strip().lower() for value in values if value.strip().lower().startswith("0x") and len(value.strip()) == 42})


def _poll_command(args: argparse.Namespace, wallets: list[str]) -> list[str]:
    return [
        str(args.python),
        "scripts/merge_dataapi_active_set_events.py",
        "--source-wallets",
        ",".join(wallets),
        "--history-state",
        str(args.history_state),
        "--history-window-index",
        str(args.history_window_index),
        "--wallet-event-log",
        str(args.wallet_event_log),
        "--dataapi-first-seen-jsonl",
        str(args.dataapi_first_seen_jsonl),
        "--observation-watermark-state",
        str(args.observation_watermark_state),
        "--state",
        str(args.poll_state),
        "--limit",
        str(int(args.limit)),
        "--pages",
        str(int(args.pages)),
        "--timeout-s",
        str(float(args.timeout_s)),
        "--retries",
        str(int(args.retries)),
        "--max-workers",
        str(int(args.max_workers)),
        "--poll-interval-s",
        str(float(args.poll_interval_s)),
        "--max-event-age-s",
        str(max(300.0, float(args.duration_s) + 300.0)),
        "--disable-source-base-overrides",
    ]


def main() -> int:
    args = parse_args()
    wallets = _wallets(args.source_wallet)
    if not wallets:
        raise SystemExit("at least one valid --source-wallet is required")
    for value in (
        args.history_state,
        args.history_window_index,
        args.wallet_event_log,
        args.dataapi_first_seen_jsonl,
        args.observation_watermark_state,
        args.poll_state,
        args.capture_state,
    ):
        Path(value).parent.mkdir(parents=True, exist_ok=True)
    started_at = utc_now_iso()
    started = time.time()
    deadline = started + max(1.0, float(args.duration_s))
    polls = 0
    failures = 0
    total_new = 0
    command = _poll_command(args, wallets)
    while time.time() < deadline:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        polls += 1
        if result.returncode != 0:
            failures += 1
        poll_state = load_json(args.poll_state, default={})
        summary = poll_state.get("summary") if isinstance(poll_state, dict) else {}
        total_new += int((summary or {}).get("poll_only_signals") or 0)
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(max(0.1, float(args.poll_interval_s)), remaining))
    state = {
        "schema_version": 1,
        "kind": "selected_wallet_dataapi_capture",
        "flow_stages": ["DISCOVER", "LEARN", "OBSERVE"],
        "status": "PASS" if polls > 0 and failures == 0 else "PARTIAL" if polls > failures else "ERROR",
        "started_at": started_at,
        "completed_at": utc_now_iso(),
        "duration_s": round(time.time() - started, 6),
        "wallets": wallets,
        "polls": polls,
        "poll_failures": failures,
        "new_events": total_new,
        "paper_only": True,
        "live_orders_allowed": False,
        "paths": {
            "history_state": args.history_state,
            "wallet_event_log": args.wallet_event_log,
            "dataapi_first_seen_jsonl": args.dataapi_first_seen_jsonl,
            "poll_state": args.poll_state,
        },
    }
    atomic_write_json(args.capture_state, state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if state["status"] != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
