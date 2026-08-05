#!/usr/bin/env python3
"""Audit append-only JSONL tails after a reboot or panic.

Flow stage: LIVE/DEFEND/SELF-DEV. Default mode is report-only. Passing
``--repair-torn-tail`` truncates only a final invalid JSONL row to the end of
the previous valid row and records the repair in the output artifact.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_INVENTORY = ROOT / "data/research/research_capture_rotation_inventory.json"
DEFAULT_OUTPUT = ROOT / "data/research/post_panic_integrity_audit_latest.json"
DEFAULT_LEDGER_JSONL = (
    "data/research/wallet_copy_live_execution_events.jsonl",
    "data/research/wallet_copy_live_guard_events.jsonl",
)


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _resolve(path: str | Path) -> Path:
    target = Path(path)
    return target if target.is_absolute() else ROOT / target


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _inventory_paths(path: Path) -> list[str]:
    payload = load_json(path, default={})
    entries = payload.get("entries") if isinstance(payload, dict) else []
    paths: list[str] = []
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, dict) and entry.get("path"):
                paths.append(str(entry["path"]))
    return paths


def _tail_lines_with_offsets(path: Path, *, max_tail_bytes: int) -> list[tuple[int, int, bytes]]:
    size = path.stat().st_size
    if size <= 0:
        return []
    read_size = min(size, int(max_tail_bytes))
    start = size - read_size
    with path.open("rb") as handle:
        handle.seek(start)
        tail = handle.read(read_size)
    if start > 0:
        newline = tail.find(b"\n")
        if newline < 0:
            return []
        start += newline + 1
        tail = tail[newline + 1 :]
    rows: list[tuple[int, int, bytes]] = []
    offset = start
    for chunk in tail.splitlines(keepends=True):
        end = offset + len(chunk)
        stripped = chunk.strip()
        if stripped:
            rows.append((offset, end, stripped))
        offset = end
    return rows


def audit_jsonl_tail(path: str | Path, *, repair: bool = False, max_tail_bytes: int = 8 * 1024 * 1024) -> dict[str, Any]:
    target = _resolve(path)
    row: dict[str, Any] = {
        "path": _rel(target),
        "status": "MISSING",
        "exists": target.exists(),
        "size_bytes": None,
        "repair_action": "none",
    }
    if target.suffix != ".jsonl":
        row.update({"status": "SKIP_NON_JSONL", "exists": target.exists()})
        if target.exists():
            row["size_bytes"] = int(target.stat().st_size)
        return row
    if not target.exists():
        return row
    size = target.stat().st_size
    row["size_bytes"] = int(size)
    if size == 0:
        row.update({"status": "PASS_EMPTY"})
        return row
    tail_rows = _tail_lines_with_offsets(target, max_tail_bytes=int(max_tail_bytes))
    if not tail_rows:
        row.update({"status": "FAIL_NO_COMPLETE_TAIL_LINE"})
        return row
    last_start, last_end, last_line = tail_rows[-1]
    row.update({"last_line_start": int(last_start), "last_line_end": int(last_end)})
    try:
        json.loads(last_line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        row.update({"status": "FAIL_TORN_TAIL", "error": str(exc), "last_line_preview": last_line[:200].decode("utf-8", "replace")})
        if not repair:
            return row
        for _, previous_end, candidate in reversed(tail_rows[:-1]):
            try:
                json.loads(candidate.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            with target.open("r+b") as handle:
                handle.truncate(previous_end)
                handle.flush()
                os.fsync(handle.fileno())
            row.update(
                {
                    "status": "REPAIRED_TORN_TAIL",
                    "repair_action": "truncate_to_previous_valid_jsonl_row",
                    "truncated_from_bytes": int(size),
                    "truncated_to_bytes": int(previous_end),
                    "bytes_removed": int(size - previous_end),
                }
            )
            return row
        row.update({"status": "FAIL_REPAIR_NO_PREVIOUS_VALID_ROW"})
        return row
    row.update({"status": "PASS"})
    return row


def build_report(
    *,
    inventory: Path,
    ledger_jsonl: list[str],
    output: Path,
    repair: bool,
    max_tail_bytes: int,
) -> dict[str, Any]:
    sources: list[dict[str, str]] = []
    for path in _inventory_paths(inventory):
        sources.append({"path": path, "source": "rotation_inventory"})
    for path in ledger_jsonl:
        sources.append({"path": path, "source": "day_ledger"})
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for source in sources:
        rel_path = _rel(_resolve(source["path"]))
        if rel_path in seen:
            continue
        seen.add(rel_path)
        audit = audit_jsonl_tail(rel_path, repair=repair, max_tail_bytes=max_tail_bytes)
        audit["source"] = source["source"]
        rows.append(audit)
    passing_statuses = {"PASS", "PASS_EMPTY", "REPAIRED_TORN_TAIL", "SKIP_NON_JSONL"}
    failures = [row for row in rows if str(row.get("status")) not in passing_statuses]
    repairs = [row for row in rows if row.get("status") == "REPAIRED_TORN_TAIL"]
    return {
        "schema_version": 1,
        "kind": "post_panic_integrity_audit",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "PASS" if not failures else "FAIL",
        "repair_enabled": bool(repair),
        "inventory": _rel(inventory),
        "output": _rel(output),
        "checked_count": len(rows),
        "failure_count": len(failures),
        "repair_count": len(repairs),
        "rows": rows,
        "next_action": "refresh scorecard same-cut basis and continue 20:30Z packet prep" if not failures else "repair or explicitly rule every failed JSONL tail before sizing work",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", default=str(DEFAULT_INVENTORY))
    parser.add_argument("--ledger-jsonl", action="append", default=list(DEFAULT_LEDGER_JSONL))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--repair-torn-tail", action="store_true")
    parser.add_argument("--max-tail-bytes", type=int, default=8 * 1024 * 1024)
    args = parser.parse_args()
    output = _resolve(args.output)
    report = build_report(
        inventory=_resolve(args.inventory),
        ledger_jsonl=list(args.ledger_jsonl or []),
        output=output,
        repair=bool(args.repair_torn_tail),
        max_tail_bytes=int(args.max_tail_bytes),
    )
    atomic_write_json(output, report)
    print(
        "post_panic_integrity "
        f"status={report['status']} checked={report['checked_count']} "
        f"failures={report['failure_count']} repairs={report['repair_count']} output={report['output']}"
    )
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
