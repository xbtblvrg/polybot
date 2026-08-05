#!/usr/bin/env python3
"""Lossless, lock-aware archive support for the order-flow incident journal."""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

DEFAULT_ACTIVE = Path("data/research/order_flow_deadman_incidents.jsonl")
DEFAULT_MANIFEST = Path("data/research/order_flow_deadman_incidents_manifest.json")
MAX_ACTIVE_TAIL_BYTES = 8 * 1024 * 1024


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _jsonl_rows(payload: bytes) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in payload.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _row_identity(row: dict[str, Any]) -> str:
    explicit = str(row.get("incident_id") or row.get("id") or "")
    if explicit:
        return explicit
    return _sha256(
        json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def _manifest_path(active_path: Path, manifest_path: Path | None) -> Path:
    return manifest_path or active_path.with_name(f"{active_path.stem}_manifest.json")


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": 1,
            "kind": "order_flow_incident_archive_manifest",
            "archives": [],
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("incident archive manifest must be an object")
    payload.setdefault("archives", [])
    return payload


def load_incident_rows(
    active_path: Path,
    *,
    manifest_path: Path | None = None,
) -> list[dict[str, Any]]:
    manifest = load_manifest(_manifest_path(active_path, manifest_path))
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in manifest.get("archives") or []:
        archive = active_path.parent / str(entry.get("path") or "")
        compressed = archive.read_bytes()
        if _sha256(compressed) != entry.get("gzip_sha256"):
            raise RuntimeError(f"incident archive gzip checksum mismatch: {archive}")
        raw = gzip.decompress(compressed)
        if _sha256(raw) != entry.get("uncompressed_sha256"):
            raise RuntimeError(f"incident archive raw checksum mismatch: {archive}")
        for row in _jsonl_rows(raw):
            identity = _row_identity(row)
            if identity not in seen:
                seen.add(identity)
                rows.append(row)
    if active_path.exists():
        for row in _jsonl_rows(active_path.read_bytes()):
            identity = _row_identity(row)
            if identity not in seen:
                seen.add(identity)
                rows.append(row)
    return rows


def tail_incident_rows(
    active_path: Path,
    *,
    manifest_path: Path | None = None,
    max_rows: int = 20,
) -> list[dict[str, Any]]:
    return load_incident_rows(
        active_path, manifest_path=manifest_path
    )[-max(0, int(max_rows)) :]


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        tmp = Path(handle.name)
    os.replace(tmp, path)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_bytes(
        path,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )


def rotate_incident_journal(
    active_path: Path,
    *,
    manifest_path: Path | None = None,
    max_tail_bytes: int = MAX_ACTIVE_TAIL_BYTES,
) -> dict[str, Any]:
    manifest_file = _manifest_path(active_path, manifest_path)
    lock_path = active_path.with_suffix(active_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        original = active_path.read_bytes() if active_path.exists() else b""
        if len(original) <= int(max_tail_bytes):
            return {
                "status": "NO_ROTATION_NEEDED",
                "source_bytes": len(original),
                "active_tail_bytes": len(original),
                "manifest": str(manifest_file),
            }
        split_at = max(0, len(original) - int(max_tail_bytes))
        newline = original.find(b"\n", split_at)
        if newline < 0:
            raise RuntimeError("journal has no line-aligned split point")
        prefix, tail = original[: newline + 1], original[newline + 1 :]
        prefix_rows = _jsonl_rows(prefix)
        tail_rows = _jsonl_rows(tail)
        if prefix + tail != original:
            raise RuntimeError("journal split reconstruction mismatch")
        manifest = load_manifest(manifest_file)
        index = len(manifest.get("archives") or []) + 1
        archive_name = f"{active_path.stem}_{index:04d}.jsonl.gz"
        compressed = gzip.compress(prefix, compresslevel=9, mtime=0)
        entry = {
            "path": archive_name,
            "source_byte_count": len(prefix),
            "row_count": len(prefix_rows),
            "first_timestamp": (
                prefix_rows[0].get("checked_at")
                or prefix_rows[0].get("generated_at")
                if prefix_rows
                else None
            ),
            "last_timestamp": (
                prefix_rows[-1].get("checked_at")
                or prefix_rows[-1].get("generated_at")
                if prefix_rows
                else None
            ),
            "uncompressed_sha256": _sha256(prefix),
            "gzip_sha256": _sha256(compressed),
        }
        archive_path = active_path.parent / archive_name
        _atomic_bytes(archive_path, compressed)
        reconstructed = gzip.decompress(archive_path.read_bytes()) + tail
        if reconstructed != original:
            raise RuntimeError("archive reconstruction verification failed")
        archives = [*(manifest.get("archives") or []), entry]
        updated = {
            "schema_version": 1,
            "kind": "order_flow_incident_archive_manifest",
            "active_path": active_path.name,
            "archives": archives,
            "archive_count": len(archives),
            "active_tail_byte_count": len(tail),
            "active_tail_row_count": len(tail_rows),
            "original_source_byte_count": len(original),
            "reconstruction_sha256": _sha256(original),
            "reconstruction_verified": True,
        }
        _atomic_json(manifest_file, updated)
        _atomic_bytes(active_path, tail)
    return {
        "status": "ROTATED",
        "source_bytes": len(original),
        "archived_bytes": len(prefix),
        "gzip_bytes": len(compressed),
        "active_tail_bytes": len(tail),
        "archived_rows": len(prefix_rows),
        "active_tail_rows": len(tail_rows),
        "archive": str(archive_path),
        "manifest": str(manifest_file),
        "reconstruction_verified": True,
    }


def append_incident_row(
    active_path: Path,
    row: dict[str, Any],
    *,
    manifest_path: Path | None = None,
) -> None:
    rotate_incident_journal(active_path, manifest_path=manifest_path)
    lock_path = active_path.with_suffix(active_path.suffix + ".lock")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        active_path.parent.mkdir(parents=True, exist_ok=True)
        with active_path.open("ab") as handle:
            handle.write(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
                + b"\n"
            )
            handle.flush()
            os.fsync(handle.fileno())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active", default=str(DEFAULT_ACTIVE))
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--max-tail-bytes", type=int, default=MAX_ACTIVE_TAIL_BYTES)
    args = parser.parse_args()
    result = rotate_incident_journal(
        Path(args.active),
        manifest_path=Path(args.manifest),
        max_tail_bytes=int(args.max_tail_bytes),
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
