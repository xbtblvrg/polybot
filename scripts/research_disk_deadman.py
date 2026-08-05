#!/usr/bin/env python3
"""Bound research-capture growth and alert on uncapped disk risk.

Flow stage: LIVE/DEFEND/SELF-DEV. This script does not submit orders or mutate
live strategy config. Its write actions are limited to research-capture
copytruncate/gzip maintenance and deadman state/log output.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json
from scripts.rotate_structured_jsonl_log import rotate_structured_jsonl

DATA = ROOT / "data" / "research"
DEFAULT_STATE = DATA / "research_disk_deadman_state.json"
DEFAULT_EVENT_LOG = DATA / "research_disk_deadman_events.jsonl"
DEFAULT_INVENTORY = DATA / "research_capture_rotation_inventory.json"
DEFAULT_ARCHIVE_DIR = DATA / "archive"
DEFAULT_LOG_ARCHIVE_DIR = DATA / "log_archives"
DEFAULT_FREE_INCIDENT_BYTES = 150 * 1024**3
DEFAULT_UNINVENTORIED_BYTES = 5 * 1024**3
DEFAULT_SWAPFILES_INCIDENT_COUNT = 20
DEFAULT_MEMORY_PRESSURE_CRITICAL_FREE_PCT = 5
DEFAULT_ARCHIVE_COMPRESS_OLDER_THAN_S = 3600
DEFAULT_RTDS_ARCHIVE_RETENTION_BYTES = 30 * 1024**3


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


def _write_line_aligned_tail_to_temp(path: Path, keep_tail_bytes: int, temp_path: Path) -> tuple[int, bool]:
    size = path.stat().st_size
    keep_tail_bytes = max(0, min(int(keep_tail_bytes), int(size)))
    if keep_tail_bytes <= 0:
        temp_path.write_bytes(b"")
        return 0, True
    start = max(0, size - keep_tail_bytes)
    aligned = True
    retained = 0
    chunk_size = 8 * 1024 * 1024
    with path.open("rb") as src, temp_path.open("wb") as dst:
        src.seek(start)
        if start > 0:
            aligned = False
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    dst.flush()
                    os.fsync(dst.fileno())
                    return 0, False
                newline_index = chunk.find(b"\n")
                if newline_index >= 0:
                    payload = chunk[newline_index + 1 :]
                    if payload:
                        dst.write(payload)
                        retained += len(payload)
                    aligned = True
                    break
        shutil.copyfileobj(src, dst, length=chunk_size)
        dst.flush()
        os.fsync(dst.fileno())
    retained = temp_path.stat().st_size
    return retained, aligned


def copytruncate_tail(
    *,
    path: str | Path,
    keep_tail_bytes: int,
    state_path: str | Path = DEFAULT_STATE,
    event_log_path: str | Path = DEFAULT_EVENT_LOG,
    reason: str = "manual_copytruncate",
) -> dict[str, Any]:
    target = _resolve(path)
    generated_at = utc_now_iso()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "research_capture_copytruncate",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": generated_at,
        "path": _rel(target),
        "keep_tail_bytes": int(keep_tail_bytes),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "reason": reason,
        "status": "SKIPPED",
    }
    if not target.exists():
        payload.update({"status": "PASS", "action": "none", "skip_reason": "missing"})
        atomic_write_json(state_path, payload)
        append_jsonl(event_log_path, payload)
        return payload

    before = target.stat()
    temp_name = ""
    try:
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.tail.", suffix=".tmp", dir=str(target.parent))
        os.close(fd)
        temp_path = Path(temp_name)
        retained_bytes, aligned = _write_line_aligned_tail_to_temp(target, keep_tail_bytes, temp_path)
        with target.open("r+b") as handle, temp_path.open("rb") as tail_handle:
            handle.seek(0)
            handle.truncate(0)
            shutil.copyfileobj(tail_handle, handle, length=8 * 1024 * 1024)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if temp_name:
            try:
                Path(temp_name).unlink()
            except FileNotFoundError:
                pass
    after = target.stat()
    payload.update(
        {
            "status": "REPAIRED",
            "action": "copytruncate_line_aligned_tail",
            "inode_preserved": before.st_ino == after.st_ino,
            "size_before_bytes": int(before.st_size),
            "size_after_bytes": int(after.st_size),
            "bytes_removed_from_hot_path": int(before.st_size - after.st_size),
            "retained_tail_bytes": int(retained_bytes),
            "tail_line_aligned": bool(aligned),
        }
    )
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    return payload


def gzip_dead_capture(
    *,
    path: str | Path,
    archive_dir: str | Path = DEFAULT_ARCHIVE_DIR,
    state_path: str | Path = DEFAULT_STATE,
    event_log_path: str | Path = DEFAULT_EVENT_LOG,
    remove_original: bool = True,
) -> dict[str, Any]:
    target = _resolve(path)
    archive_root = _resolve(archive_dir)
    generated_at = utc_now_iso()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "research_capture_gzip_dead",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": generated_at,
        "path": _rel(target),
        "archive_dir": _rel(archive_root),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "status": "SKIPPED",
    }
    if not target.exists():
        payload.update({"status": "PASS", "action": "none", "skip_reason": "missing"})
        atomic_write_json(state_path, payload)
        append_jsonl(event_log_path, payload)
        return payload
    archive_root.mkdir(parents=True, exist_ok=True)
    archive = archive_root / f"{target.name}.{generated_at.replace(':', '').replace('-', '')}.gz"
    size_before = target.stat().st_size
    with target.open("rb") as src, gzip.open(archive, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
    with gzip.open(archive, "rb") as check:
        while check.read(8 * 1024 * 1024):
            pass
    if remove_original:
        target.unlink()
    payload.update(
        {
            "status": "REPAIRED",
            "action": "gzip_archive_remove_original" if remove_original else "gzip_archive_only",
            "archive_path": _rel(archive),
            "size_before_bytes": int(size_before),
            "archive_size_bytes": int(archive.stat().st_size),
            "bytes_removed_from_hot_path": int(size_before if remove_original else 0),
            "gzip_test": "PASS",
        }
    )
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    return payload


def _gzip_in_place(path: Path) -> dict[str, Any]:
    generated_at = utc_now_iso().replace(":", "").replace("-", "")
    archive = path.with_name(f"{path.name}.gz")
    if archive.exists():
        archive = path.with_name(f"{path.name}.{generated_at}.gz")
    size_before = path.stat().st_size
    with path.open("rb") as src, gzip.open(archive, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
    with gzip.open(archive, "rb") as check:
        while check.read(8 * 1024 * 1024):
            pass
    path.unlink()
    return {
        "action": "gzip_log_archive_in_place",
        "path": _rel(path),
        "archive_path": _rel(archive),
        "size_before_bytes": int(size_before),
        "archive_size_bytes": int(archive.stat().st_size),
        "bytes_removed_from_uncompressed_archive": int(size_before),
        "gzip_test": "PASS",
    }


def _is_rtds_raw_archive(path: Path) -> bool:
    name = path.name
    if "wallet_copy_live_guard_events" in name or "wallet_copy_live_guard_wallet_events" in name:
        return False
    return "polymarket_activity_ws_capture" in name or "rtds_capture" in name


def compress_log_archives(
    *,
    archive_dir: str | Path = DEFAULT_LOG_ARCHIVE_DIR,
    state_path: str | Path = DEFAULT_STATE,
    event_log_path: str | Path = DEFAULT_EVENT_LOG,
    older_than_s: float = DEFAULT_ARCHIVE_COMPRESS_OLDER_THAN_S,
    rtds_retention_cap_bytes: int = DEFAULT_RTDS_ARCHIVE_RETENTION_BYTES,
) -> dict[str, Any]:
    archive_root = _resolve(archive_dir)
    generated_at = utc_now_iso()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "research_log_archive_maintenance",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": generated_at,
        "archive_dir": _rel(archive_root),
        "compress_older_than_s": float(older_than_s),
        "rtds_retention_cap_bytes": int(rtds_retention_cap_bytes),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "status": "PASS",
        "compressed": [],
        "deleted_for_retention": [],
    }
    if not archive_root.exists():
        payload.update({"action": "none", "skip_reason": "missing_archive_dir"})
        atomic_write_json(state_path, payload)
        append_jsonl(event_log_path, payload)
        return payload

    now_s = datetime.now(tz=UTC).timestamp()
    compressed: list[dict[str, Any]] = []
    for path in sorted(archive_root.glob("*.jsonl")):
        try:
            age_s = max(0.0, now_s - float(path.stat().st_mtime))
        except OSError:
            continue
        if age_s < float(older_than_s):
            continue
        action = _gzip_in_place(path)
        action["archive_age_s"] = round(age_s, 6)
        compressed.append(action)

    rtds_archives: list[Path] = []
    for path in archive_root.glob("*.gz"):
        if path.is_file() and _is_rtds_raw_archive(path):
            rtds_archives.append(path)
    rtds_sizes: list[tuple[Path, int, float]] = []
    for path in rtds_archives:
        try:
            stat = path.stat()
        except OSError:
            continue
        rtds_sizes.append((path, int(stat.st_size), float(stat.st_mtime)))
    total_bytes = sum(size for _, size, _ in rtds_sizes)
    total_before = total_bytes
    deleted: list[dict[str, Any]] = []
    for path, size, mtime in sorted(rtds_sizes, key=lambda item: (item[2], item[0].name)):
        if total_bytes <= int(rtds_retention_cap_bytes):
            break
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        total_bytes -= size
        deleted.append(
            {
                "action": "delete_rtds_archive_oldest_first",
                "path": _rel(path),
                "size_bytes": int(size),
                "mtime_s": round(mtime, 6),
            }
        )

    if compressed or deleted:
        payload["status"] = "REPAIRED"
    payload.update(
        {
            "compressed": compressed,
            "deleted_for_retention": deleted,
            "compressed_count": len(compressed),
            "retention_deleted_count": len(deleted),
            "rtds_archive_bytes_before_retention": int(total_before),
            "rtds_archive_bytes_after_retention": int(total_bytes),
            "next_action": "continue archive compression+retention maintenance from inventory enforcement",
        }
    )
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    return payload


def enforce_inventory(
    *,
    inventory_path: str | Path = DEFAULT_INVENTORY,
    state_path: str | Path = DEFAULT_STATE,
    event_log_path: str | Path = DEFAULT_EVENT_LOG,
) -> dict[str, Any]:
    inventory = load_json(inventory_path, default={})
    entries = inventory.get("entries") if isinstance(inventory, dict) and isinstance(inventory.get("entries"), list) else []
    actions: list[dict[str, Any]] = []
    generated_at = utc_now_iso()
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("path"):
            continue
        target = _resolve(str(entry["path"]))
        cap_bytes = int(entry.get("cap_bytes") or 0)
        keep_tail_bytes = int(entry.get("keep_tail_bytes") or cap_bytes or 0)
        max_segment_age_s = int(entry.get("max_segment_age_s") or 0)
        if (cap_bytes <= 0 and max_segment_age_s <= 0) or not target.exists():
            continue
        stat = target.stat()
        size = stat.st_size
        birthtime_s = float(getattr(stat, "st_birthtime", stat.st_ctime))
        segment_age_s = max(0.0, datetime.now(tz=UTC).timestamp() - birthtime_s)
        size_due = cap_bytes > 0 and size > cap_bytes
        age_due = max_segment_age_s > 0 and segment_age_s > max_segment_age_s
        if not size_due and not age_due:
            continue
        rotation_action = str(entry.get("rotation_action") or "copytruncate_line_aligned_tail")
        if rotation_action == "structured_jsonl_archive_reseed":
            rotation_max_bytes = cap_bytes
            if age_due and not size_due:
                rotation_max_bytes = max(0, int(size) - 1)
            action = rotate_structured_jsonl(
                log_path=target,
                state_path=entry.get("state_path") or state_path,
                event_log_path=entry.get("event_log_path") or event_log_path,
                archive_dir=entry.get("archive_dir") or DEFAULT_ARCHIVE_DIR,
                max_bytes=rotation_max_bytes,
                keep_tail_bytes=keep_tail_bytes,
            )
            action["rotation_trigger"] = "size" if size_due else "age"
            action["segment_age_s"] = round(segment_age_s, 6)
            action["max_segment_age_s"] = max_segment_age_s or None
        else:
            action = copytruncate_tail(
                path=target,
                keep_tail_bytes=keep_tail_bytes,
                state_path=state_path,
                event_log_path=event_log_path,
                reason=f"inventory_enforce_cap_{cap_bytes}",
            )
            action["rotation_trigger"] = "size" if size_due else "age"
            action["segment_age_s"] = round(segment_age_s, 6)
            action["max_segment_age_s"] = max_segment_age_s or None
        actions.append(action)
    archive_maintenance = compress_log_archives(
        archive_dir=DATA / "log_archives",
        state_path=state_path,
        event_log_path=event_log_path,
    )
    maintenance_repairs = int(archive_maintenance.get("compressed_count") or 0) + int(
        archive_maintenance.get("retention_deleted_count") or 0
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "research_capture_inventory_enforcement",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": generated_at,
        "status": "REPAIRED" if actions or maintenance_repairs else "PASS",
        "inventory_path": _rel(_resolve(inventory_path)),
        "entries": len(entries),
        "actions": actions,
        "archive_maintenance": archive_maintenance,
        "repaired_count": len(actions) + maintenance_repairs,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "next_action": "run research disk audit after inventory enforcement",
    }
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    return payload


def _inventory_paths(inventory: dict[str, Any]) -> set[str]:
    entries = inventory.get("entries") if isinstance(inventory.get("entries"), list) else []
    paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        value = entry.get("path")
        if value:
            paths.add(_rel(_resolve(str(value))))
    return paths


def _discover_large_research_files(*, data_dir: Path, threshold_bytes: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    excluded_parts = {"archive", "log_archives", "runtime_logs"}
    if not data_dir.exists():
        return rows
    for path in data_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            rel_parts = path.relative_to(data_dir).parts
        except ValueError:
            rel_parts = ()
        if rel_parts and rel_parts[0] in excluded_parts:
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size > threshold_bytes:
            rows.append({"path": _rel(path), "size_bytes": int(size)})
    rows.sort(key=lambda row: int(row["size_bytes"]), reverse=True)
    return rows


def _disk_free(path: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(path)
    return {
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
        "free_gib": round(usage.free / 1024**3, 3),
        "used_pct": round((usage.used / usage.total) * 100, 3) if usage.total else None,
    }


def _run_command_text(cmd: list[str], *, timeout_s: float = 5.0) -> dict[str, Any]:
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, check=False, timeout=timeout_s)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc), "command": cmd}
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
        "command": cmd,
    }


def _memory_size_to_mib(value: str, unit: str) -> float:
    parsed = float(value)
    normalized = unit.lower()
    if normalized.startswith("g"):
        return parsed * 1024.0
    if normalized.startswith("k"):
        return parsed / 1024.0
    return parsed


def _parse_swapusage(text: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {"raw": text.strip()[:500]}
    for key in ("total", "used", "free"):
        match = re.search(rf"{key}\s*=\s*([0-9.]+)\s*([KMG])", text, flags=re.IGNORECASE)
        if match:
            parsed[f"{key}_mib"] = round(_memory_size_to_mib(match.group(1), match.group(2)), 3)
    used = parsed.get("used_mib")
    total = parsed.get("total_mib")
    if isinstance(used, (int, float)) and isinstance(total, (int, float)) and total > 0:
        parsed["used_pct"] = round((float(used) / float(total)) * 100.0, 3)
    return parsed


def _parse_memory_pressure(text: str, *, critical_free_pct: int) -> dict[str, Any]:
    parsed: dict[str, Any] = {"raw_tail": text.strip()[-1000:]}
    free_match = re.search(r"free percentage:\s*([0-9]+)%", text, flags=re.IGNORECASE)
    if free_match:
        parsed["free_pct"] = int(free_match.group(1))
        parsed["pressure_pct"] = 100 - int(free_match.group(1))
    segment_match = re.search(r"([0-9]+)%\s+of\s+segments\s+limit", text, flags=re.IGNORECASE)
    if segment_match:
        parsed["compressor_segment_limit_pct"] = int(segment_match.group(1))
    lowered = text.lower()
    critical_by_text = "critical" in lowered or "segments limit (bad)" in lowered
    critical_by_free_pct = (
        isinstance(parsed.get("free_pct"), int) and int(parsed["free_pct"]) <= int(critical_free_pct)
    )
    critical_by_segments = (
        isinstance(parsed.get("compressor_segment_limit_pct"), int)
        and int(parsed["compressor_segment_limit_pct"]) >= 100
    )
    parsed["critical"] = bool(critical_by_text or critical_by_free_pct or critical_by_segments)
    return parsed


def _count_swapfiles(vm_dir: Path = Path("/private/var/vm")) -> int | None:
    try:
        return sum(1 for path in vm_dir.glob("swapfile*") if path.is_file())
    except OSError:
        return None


def build_memory_swap_vitals(
    *,
    previous_state: dict[str, Any] | None,
    swapusage_text: str,
    swapusage_ok: bool,
    memory_pressure_text: str,
    memory_pressure_ok: bool,
    swapfile_count: int | None,
    swapfile_incident_count: int = DEFAULT_SWAPFILES_INCIDENT_COUNT,
    critical_free_pct: int = DEFAULT_MEMORY_PRESSURE_CRITICAL_FREE_PCT,
) -> dict[str, Any]:
    previous_memory = previous_state.get("memory_swap") if isinstance(previous_state, dict) else {}
    previous_swapfiles = previous_memory.get("swapfiles") if isinstance(previous_memory, dict) else {}
    previous_above = bool(previous_swapfiles.get("above_threshold")) if isinstance(previous_swapfiles, dict) else False
    above_threshold = isinstance(swapfile_count, int) and swapfile_count > int(swapfile_incident_count)
    sustained = bool(above_threshold and previous_above)
    pressure = _parse_memory_pressure(memory_pressure_text, critical_free_pct=int(critical_free_pct))
    incident_keys: list[str] = []
    if sustained:
        incident_keys.append("swapfile_count_sustained_gt_threshold")
    if bool(pressure.get("critical")):
        incident_keys.append("memory_pressure_critical")
    status = "INCIDENT_MEMORY_SWAP" if incident_keys else ("WATCH_MEMORY_SWAP" if above_threshold else "OK")
    return {
        "status": status,
        "incident": bool(incident_keys),
        "incident_keys": incident_keys,
        "swapusage": {
            "ok": bool(swapusage_ok),
            **_parse_swapusage(swapusage_text),
        },
        "swapfiles": {
            "count": swapfile_count,
            "threshold_count": int(swapfile_incident_count),
            "above_threshold": bool(above_threshold),
            "sustained_above_threshold": bool(sustained),
        },
        "memory_pressure": {
            "ok": bool(memory_pressure_ok),
            **pressure,
            "critical_free_pct": int(critical_free_pct),
        },
        "lane_shedder": {
            "mechanism": "brainless_ops_memory_pressure_pause",
            "action": "pause low-priority paper/member/factory refreshes; never stop guard/feeds/ledger",
        },
    }


def collect_memory_swap_vitals(
    *,
    previous_state: dict[str, Any] | None,
    swapfile_incident_count: int = DEFAULT_SWAPFILES_INCIDENT_COUNT,
    critical_free_pct: int = DEFAULT_MEMORY_PRESSURE_CRITICAL_FREE_PCT,
) -> dict[str, Any]:
    swapusage = _run_command_text(["sysctl", "vm.swapusage"])
    pressure = _run_command_text(["memory_pressure", "-Q"])
    return build_memory_swap_vitals(
        previous_state=previous_state,
        swapusage_text=str(swapusage.get("stdout") or swapusage.get("stderr") or ""),
        swapusage_ok=bool(swapusage.get("ok")),
        memory_pressure_text=str(pressure.get("stdout") or pressure.get("stderr") or ""),
        memory_pressure_ok=bool(pressure.get("ok")),
        swapfile_count=_count_swapfiles(),
        swapfile_incident_count=int(swapfile_incident_count),
        critical_free_pct=int(critical_free_pct),
    )


def audit_disk(
    *,
    inventory_path: str | Path = DEFAULT_INVENTORY,
    state_path: str | Path = DEFAULT_STATE,
    event_log_path: str | Path = DEFAULT_EVENT_LOG,
    handoff_path: str | Path | None = None,
    free_incident_bytes: int = DEFAULT_FREE_INCIDENT_BYTES,
    uninventoried_threshold_bytes: int = DEFAULT_UNINVENTORIED_BYTES,
    swapfile_incident_count: int = DEFAULT_SWAPFILES_INCIDENT_COUNT,
    memory_pressure_critical_free_pct: int = DEFAULT_MEMORY_PRESSURE_CRITICAL_FREE_PCT,
    write_handoff_on_incident: bool = False,
) -> dict[str, Any]:
    previous_state = load_json(state_path, default={})
    inventory = load_json(inventory_path, default={})
    inventory_paths = _inventory_paths(inventory if isinstance(inventory, dict) else {})
    large_files = _discover_large_research_files(data_dir=DATA, threshold_bytes=uninventoried_threshold_bytes)
    control_paths = {
        _rel(_resolve(inventory_path)),
        _rel(_resolve(state_path)),
        _rel(_resolve(event_log_path)),
    }
    large_files = [row for row in large_files if row["path"] not in control_paths]
    uninventoried = [row for row in large_files if row["path"] not in inventory_paths]
    disk = _disk_free(ROOT)
    incident_keys: list[str] = []
    if int(disk["free_bytes"]) < int(free_incident_bytes):
        incident_keys.append("free_space_lt_threshold")
    if uninventoried:
        incident_keys.append("uninventoried_large_research_files")
    memory_swap = collect_memory_swap_vitals(
        previous_state=previous_state if isinstance(previous_state, dict) else {},
        swapfile_incident_count=int(swapfile_incident_count),
        critical_free_pct=int(memory_pressure_critical_free_pct),
    )
    incident_keys.extend(str(key) for key in memory_swap.get("incident_keys", []) if key)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "research_disk_deadman",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "INCIDENT_RESEARCH_DISK" if incident_keys else "OK",
        "incident": bool(incident_keys),
        "incident_keys": incident_keys,
        "disk": disk,
        "thresholds": {
            "free_incident_bytes": int(free_incident_bytes),
            "free_incident_gib": round(int(free_incident_bytes) / 1024**3, 3),
            "uninventoried_file_bytes": int(uninventoried_threshold_bytes),
            "uninventoried_file_gib": round(int(uninventoried_threshold_bytes) / 1024**3, 3),
            "swapfiles_incident_count": int(swapfile_incident_count),
            "memory_pressure_critical_free_pct": int(memory_pressure_critical_free_pct),
        },
        "memory_swap": memory_swap,
        "inventory_path": _rel(_resolve(inventory_path)),
        "inventory_count": len(inventory_paths),
        "large_files": large_files,
        "uninventoried_large_files": uninventoried,
        "retention_rule": (
            "raw research captures with no pending decision and no consumer older than 7 days are compressed; "
            "compressed archives older than 30 days require Fable deletion ruling"
        ),
        "next_action": (
            "copytruncate/enroll every uninventoried >5GiB research file, free disk above threshold, or pause low-priority lanes on memory/swap incident"
            if incident_keys
            else "continue 10-minute brainless disk deadman"
        ),
    }
    atomic_write_json(state_path, payload)
    append_jsonl(event_log_path, payload)
    if write_handoff_on_incident and incident_keys and handoff_path:
        handoff = _resolve(handoff_path)
        handoff.parent.mkdir(parents=True, exist_ok=True)
        top = ", ".join(f"{row['path']}={row['size_bytes']}" for row in uninventoried[:5]) or "none"
        with handoff.open("a", encoding="utf-8") as handle:
            handle.write(
                f"\n## {payload['generated_at']} brainless NOTIFY - RESEARCH_DISK_DEADMAN INCIDENT\n"
                f"- defect | [LIVE/DEFEND/SELF-DEV] research disk/memory deadman incident keys={incident_keys} free_gib={disk['free_gib']} uninventoried_top={top} swapfiles={(memory_swap.get('swapfiles') or {}).get('count')} | attempts: disk free scan, auto-discovery size scan, rotation inventory diff, swap/compressor vital scan | next=copytruncate/enroll every uncapped >5GiB research file or pause lowest-value paper lanes, then rerun scripts/research_disk_deadman.py audit until status OK.\n"
            )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    copy = sub.add_parser("copytruncate", help="Keep a line-aligned tail in-place, preserving inode.")
    copy.add_argument("--path", required=True)
    copy.add_argument("--keep-tail-bytes", type=int, required=True)
    copy.add_argument("--state", default=str(DEFAULT_STATE))
    copy.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    copy.add_argument("--reason", default="manual_copytruncate")

    gz = sub.add_parser("gzip-dead", help="Gzip a dead capture and remove the original after test.")
    gz.add_argument("--path", required=True)
    gz.add_argument("--archive-dir", default=str(DEFAULT_ARCHIVE_DIR))
    gz.add_argument("--state", default=str(DEFAULT_STATE))
    gz.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    gz.add_argument("--keep-original", action="store_true")

    archives = sub.add_parser(
        "compress-archives",
        help="Gzip log_archives/*.jsonl in-place and enforce compressed RTDS archive retention.",
    )
    archives.add_argument("--archive-dir", default=str(DEFAULT_LOG_ARCHIVE_DIR))
    archives.add_argument("--state", default=str(DEFAULT_STATE))
    archives.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    archives.add_argument("--older-than-s", type=float, default=DEFAULT_ARCHIVE_COMPRESS_OLDER_THAN_S)
    archives.add_argument("--rtds-retention-cap-bytes", type=int, default=DEFAULT_RTDS_ARCHIVE_RETENTION_BYTES)

    enforce = sub.add_parser("enforce", help="Copytruncate any inventoried capture above its cap.")
    enforce.add_argument("--inventory", default=str(DEFAULT_INVENTORY))
    enforce.add_argument("--state", default=str(DEFAULT_STATE))
    enforce.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))

    audit = sub.add_parser("audit", help="Run the free-space + uninventoried-file deadman.")
    audit.add_argument("--inventory", default=str(DEFAULT_INVENTORY))
    audit.add_argument("--state", default=str(DEFAULT_STATE))
    audit.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    audit.add_argument("--handoff", default="docs/agents/HANDOFF.md")
    audit.add_argument("--free-incident-bytes", type=int, default=DEFAULT_FREE_INCIDENT_BYTES)
    audit.add_argument("--uninventoried-threshold-bytes", type=int, default=DEFAULT_UNINVENTORIED_BYTES)
    audit.add_argument("--swapfile-incident-count", type=int, default=DEFAULT_SWAPFILES_INCIDENT_COUNT)
    audit.add_argument(
        "--memory-pressure-critical-free-pct",
        type=int,
        default=DEFAULT_MEMORY_PRESSURE_CRITICAL_FREE_PCT,
    )
    audit.add_argument("--write-handoff-on-incident", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "copytruncate":
        payload = copytruncate_tail(
            path=args.path,
            keep_tail_bytes=int(args.keep_tail_bytes),
            state_path=args.state,
            event_log_path=args.event_log,
            reason=str(args.reason),
        )
    elif args.command == "gzip-dead":
        payload = gzip_dead_capture(
            path=args.path,
            archive_dir=args.archive_dir,
            state_path=args.state,
            event_log_path=args.event_log,
            remove_original=not bool(args.keep_original),
        )
    elif args.command == "compress-archives":
        payload = compress_log_archives(
            archive_dir=args.archive_dir,
            state_path=args.state,
            event_log_path=args.event_log,
            older_than_s=float(args.older_than_s),
            rtds_retention_cap_bytes=int(args.rtds_retention_cap_bytes),
        )
    elif args.command == "enforce":
        payload = enforce_inventory(
            inventory_path=args.inventory,
            state_path=args.state,
            event_log_path=args.event_log,
        )
    else:
        payload = audit_disk(
            inventory_path=args.inventory,
            state_path=args.state,
            event_log_path=args.event_log,
            handoff_path=args.handoff,
            free_incident_bytes=int(args.free_incident_bytes),
            uninventoried_threshold_bytes=int(args.uninventoried_threshold_bytes),
            swapfile_incident_count=int(args.swapfile_incident_count),
            memory_pressure_critical_free_pct=int(args.memory_pressure_critical_free_pct),
            write_handoff_on_incident=bool(args.write_handoff_on_incident),
        )
    print(json.dumps(payload, sort_keys=True))
    return 2 if payload.get("incident") else 0


if __name__ == "__main__":
    raise SystemExit(main())
