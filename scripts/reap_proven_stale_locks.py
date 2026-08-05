#!/usr/bin/env python3
"""Targeted no-restart reap for the four midnight sweep-race lock remnants."""

from __future__ import annotations

import fcntl
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.post_boot_recovery_audit import _lock_holder, process_rows  # noqa: E402

TARGETS = (
    ROOT / "data/research/.btc_resolutions_from_btcusdt_ticks.jsonl.lock",
    ROOT / "data/research/.wallet_copy_ready_shadow_lanes_state.json.lock",
    ROOT / "data/research/order_flow_deadman_incidents.jsonl.lock",
    ROOT / "data/research/wide_direct_handoff_journal.jsonl.lock",
)
DEFAULT_JOURNAL = ROOT / "data/research/targeted_stale_lock_reap_events.jsonl"
DEFAULT_STATE = ROOT / "data/research/targeted_stale_lock_reap_latest.json"


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def reap_targets(
    *,
    targets: tuple[Path, ...] = TARGETS,
    allowed_paths: set[Path] | None = None,
    journal_path: Path = DEFAULT_JOURNAL,
    state_path: Path = DEFAULT_STATE,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Re-prove and unlink only explicit lock paths while holding their flock."""
    generated_at = _utc_now()
    now_ts = time.time() if now_ts is None else now_ts
    allowlist_source = set(TARGETS) if allowed_paths is None else allowed_paths
    allowlist = {path.resolve() for path in allowlist_source}
    processes = process_rows()
    rows: list[dict[str, Any]] = []

    for path in targets:
        base = {
            "generated_at": generated_at,
            "flow_stage": "LIVE/SELF-DEV",
            "path": _display(path),
            "allowlisted": path.resolve() in allowlist,
        }
        if not base["allowlisted"]:
            row = {**base, "status": "RETAINED_NOT_ALLOWLISTED"}
            rows.append(row)
            _append_jsonl(journal_path, row)
            continue
        if not path.exists():
            row = {**base, "status": "PASS_ALREADY_ABSENT"}
            rows.append(row)
            _append_jsonl(journal_path, row)
            continue
        try:
            handle = path.open("a+")
        except OSError as exc:
            row = {
                **base,
                "status": "RETAINED_OPEN_FAILED",
                "error": f"{type(exc).__name__}: {exc}",
            }
            rows.append(row)
            _append_jsonl(journal_path, row)
            continue
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                flock_acquired = True
            except BlockingIOError:
                flock_acquired = False
            holder = _lock_holder(path, processes)
            age_s = max(0.0, now_ts - path.stat().st_mtime)
            proof = {
                **base,
                **holder,
                "age_s": round(age_s, 6),
                "exclusive_nonblocking_flock_acquired": flock_acquired,
            }
            if not flock_acquired:
                row = {**proof, "status": "RETAINED_KERNEL_HELD"}
            elif holder["holder_alive"]:
                row = {**proof, "status": "RETAINED_LIVE_HOLDER"}
            else:
                path.unlink()
                row = {
                    **proof,
                    "status": "DELETED_PROVEN_STALE_LOCK",
                    "unlinked_while_flock_held": True,
                    "proof": "exact_allowlist+exclusive_nonblocking_flock+dead_holder",
                }
            rows.append(row)
            _append_jsonl(journal_path, row)
        except OSError as exc:
            row = {
                **base,
                "status": "RETAINED_REPROOF_OR_DELETE_FAILED",
                "error": f"{type(exc).__name__}: {exc}",
            }
            rows.append(row)
            _append_jsonl(journal_path, row)
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            handle.close()

    failures = [
        row for row in rows
        if row["status"] not in {"DELETED_PROVEN_STALE_LOCK", "PASS_ALREADY_ABSENT"}
    ]
    result = {
        "generated_at": generated_at,
        "flow_stage": "LIVE/SELF-DEV",
        "status": "PASS" if not failures else "DEFECT",
        "targets": rows,
        "deleted_count": sum(row["status"] == "DELETED_PROVEN_STALE_LOCK" for row in rows),
        "already_absent_count": sum(row["status"] == "PASS_ALREADY_ABSENT" for row in rows),
        "failure_count": len(failures),
        "journal": _display(journal_path),
        "next_action": (
            "none; targeted stale locks are absent and the live guard was not restarted"
            if not failures
            else "re-prove and resolve every retained exact target without restarting the live guard"
        ),
    }
    _write_json(state_path, result)
    return result


def main() -> int:
    result = reap_targets()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
