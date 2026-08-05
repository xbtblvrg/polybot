#!/usr/bin/env python3
"""Bounded rotation for wallet-copy runtime text logs.

Structured JSONL files are the source of truth for learning and admission.
This helper only bounds verbose guard stdout/stderr files so the runtime can
stay observable without hiding real wallet-copy evidence.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import append_jsonl, atomic_write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", default="data/research/wallet_copy_active_hotlane_guard.out")
    parser.add_argument("--state", default="data/research/wallet_copy_runtime_log_rotation_state.json")
    parser.add_argument("--event-log", default="data/research/wallet_copy_runtime_log_rotation_events.jsonl")
    parser.add_argument("--max-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--keep-tail-bytes", type=int, default=2 * 1024 * 1024)
    return parser.parse_args()


def rotate_log(
    *,
    log_path: str | Path,
    state_path: str | Path,
    event_log_path: str | Path,
    max_bytes: int,
    keep_tail_bytes: int,
) -> dict:
    target = Path(log_path)
    generated_at = utc_now_iso()
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_runtime_log_rotation",
        "generated_at": generated_at,
        "path": str(target),
        "max_bytes": int(max_bytes),
        "keep_tail_bytes": int(keep_tail_bytes),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_of_truth_note": "structured wallet_copy JSONL/state files are preserved; this bounds verbose guard stdout only",
        "status": "SKIPPED",
        "action": "none",
        "size_before_bytes": 0,
        "size_after_bytes": 0,
    }
    if not target.exists():
        payload["status"] = "PASS"
        payload["reason"] = "log_missing_no_rotation_needed"
        atomic_write_json(state_path, payload)
        append_jsonl(event_log_path, payload)
        return payload

    size_before = target.stat().st_size
    payload["size_before_bytes"] = int(size_before)
    if size_before <= max_bytes:
        payload["status"] = "PASS"
        payload["reason"] = "below_cap"
        payload["size_after_bytes"] = int(size_before)
        atomic_write_json(state_path, payload)
        append_jsonl(event_log_path, payload)
        return payload

    keep_tail_bytes = max(0, min(int(keep_tail_bytes), int(size_before)))
    tail = b""
    if keep_tail_bytes:
        with target.open("rb") as handle:
            handle.seek(max(0, size_before - keep_tail_bytes))
            tail = handle.read(keep_tail_bytes)

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


def main() -> int:
    args = parse_args()
    payload = rotate_log(
        log_path=args.log,
        state_path=args.state,
        event_log_path=args.event_log,
        max_bytes=args.max_bytes,
        keep_tail_bytes=args.keep_tail_bytes,
    )
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
