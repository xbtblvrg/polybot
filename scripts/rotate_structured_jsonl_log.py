#!/usr/bin/env python3
"""Archive and reseed large structured JSONL logs with a recent line-aligned tail."""

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
    parser.add_argument("--log", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--state", default="data/research/wallet_copy_guard_event_log_rotation_state.json")
    parser.add_argument("--event-log", default="data/research/wallet_copy_runtime_log_rotation_events.jsonl")
    parser.add_argument("--archive-dir", default="data/research/log_archives")
    parser.add_argument("--max-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--keep-tail-bytes", type=int, default=64 * 1024 * 1024)
    return parser.parse_args()


def _line_aligned_tail(path: Path, keep_tail_bytes: int) -> tuple[bytes, bool]:
    size = path.stat().st_size
    keep_tail_bytes = max(0, min(int(keep_tail_bytes), int(size)))
    if keep_tail_bytes <= 0:
        return b"", True
    with path.open("rb") as handle:
        handle.seek(max(0, size - keep_tail_bytes))
        tail = handle.read(keep_tail_bytes)
    if not tail or tail.startswith(b"\n") or keep_tail_bytes >= size:
        return tail.lstrip(b"\n"), True
    newline_index = tail.find(b"\n")
    if newline_index < 0:
        return b"", False
    return tail[newline_index + 1 :], True


def _archive_path(target: Path, archive_dir: Path, generated_at: str) -> Path:
    stamp = generated_at.replace(":", "").replace("-", "").replace("+00:00", "Z")
    archive_dir.mkdir(parents=True, exist_ok=True)
    candidate = archive_dir / f"{target.stem}_{stamp}{target.suffix}"
    if not candidate.exists():
        return candidate
    index = 1
    while True:
        replacement = archive_dir / f"{target.stem}_{stamp}_{index}{target.suffix}"
        if not replacement.exists():
            return replacement
        index += 1


def rotate_structured_jsonl(
    *,
    log_path: str | Path,
    state_path: str | Path,
    event_log_path: str | Path,
    archive_dir: str | Path,
    max_bytes: int,
    keep_tail_bytes: int,
) -> dict:
    target = Path(log_path)
    generated_at = utc_now_iso()
    payload: dict = {
        "schema_version": 1,
        "kind": "wallet_copy_structured_jsonl_rotation",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": generated_at,
        "path": str(target),
        "max_bytes": int(max_bytes),
        "keep_tail_bytes": int(keep_tail_bytes),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_of_truth_note": (
            "full pre-rotation structured JSONL is moved to archive_path; hot-path log is reseeded "
            "with a recent line-aligned tail, preserving current liveness readers without deleting evidence"
        ),
        "status": "SKIPPED",
        "action": "none",
        "size_before_bytes": 0,
        "size_after_bytes": 0,
    }
    if int(max_bytes) <= 0:
        payload["status"] = "DISABLED"
        payload["reason"] = "max_bytes_not_positive"
        atomic_write_json(state_path, payload)
        append_jsonl(event_log_path, payload)
        return payload
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

    tail, tail_line_aligned = _line_aligned_tail(target, keep_tail_bytes)
    archive = _archive_path(target, Path(archive_dir), generated_at)
    target.replace(archive)

    race_file_existed = target.exists()
    if race_file_existed:
        with target.open("ab") as handle:
            if tail:
                if target.stat().st_size > 0:
                    handle.write(b"\n")
                handle.write(tail)
    else:
        with target.open("wb") as handle:
            if tail:
                handle.write(tail)

    size_after = target.stat().st_size if target.exists() else 0
    payload.update(
        {
            "status": "REPAIRED",
            "action": "rename_archive_reseed_tail",
            "reason": "above_cap_archived_tail_reseeded",
            "archive_path": str(archive),
            "archive_size_bytes": int(archive.stat().st_size),
            "size_after_bytes": int(size_after),
            "bytes_removed_from_hot_path": int(size_before - size_after),
            "retained_tail_bytes": len(tail),
            "tail_line_aligned": bool(tail_line_aligned),
            "race_file_existed_after_rename": bool(race_file_existed),
        }
    )
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    return payload


def main() -> int:
    args = parse_args()
    payload = rotate_structured_jsonl(
        log_path=args.log,
        state_path=args.state,
        event_log_path=args.event_log,
        archive_dir=args.archive_dir,
        max_bytes=int(args.max_bytes),
        keep_tail_bytes=int(args.keep_tail_bytes),
    )
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
