#!/usr/bin/env python3
"""Reconstruct the resident guard generation and publish per-file drift."""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = Path("data/research/brainless_live_guard_restart_state.json")
DEFAULT_OUTPUT = Path("data/research/guard_generation_delta_latest.json")
HISTORICAL_GENERATION_SOURCE = "scripts/brainless_live_guard_restart.py"


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _aggregate_generation(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(str(row["path"]).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(row.get("sha256") or "MISSING").encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _git(root: Path, *args: str, check: bool = True) -> bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        check=False,
    )
    if check and completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail or completed.returncode}")
    return completed.stdout if completed.returncode == 0 else b""


def _path_value(node: ast.AST, values: dict[str, PurePosixPath]) -> PurePosixPath:
    if isinstance(node, ast.Name) and node.id in values:
        return values[node.id]
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return PurePosixPath(node.value)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return _path_value(node.left, values) / _path_value(node.right, values)
    raise ValueError(f"unsupported generation path expression: {ast.dump(node)}")


def _historical_generation_paths(source: str) -> list[str]:
    tree = ast.parse(source)
    values: dict[str, PurePosixPath] = {"ROOT": PurePosixPath(".")}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        value = node.value
        names = [target.id for target in targets if isinstance(target, ast.Name)]
        if not names or value is None:
            continue
        name = names[0]
        if name == "GENERATION_FILES":
            if not isinstance(value, (ast.Tuple, ast.List)):
                raise ValueError("GENERATION_FILES is not a literal tuple/list")
            paths = [_path_value(item, values) for item in value.elts]
            return [str(path).removeprefix("./") for path in paths]
        try:
            values[name] = _path_value(value, values)
        except ValueError:
            continue
    raise ValueError("GENERATION_FILES assignment not found")


def _blob_at_commit(root: Path, commit: str, path: str) -> bytes | None:
    completed = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=root,
        capture_output=True,
        check=False,
    )
    return completed.stdout if completed.returncode == 0 else None


def _resolve_loaded_commit(root: Path, started_at_utc: str) -> str:
    commit = _git(root, "rev-list", "-1", f"--before={started_at_utc}", "HEAD").decode().strip()
    if not commit:
        raise ValueError(f"no commit at or before loaded start {started_at_utc}")
    return commit


def build_report(*, root: Path, state: dict[str, Any], generated_at: str | None = None) -> dict[str, Any]:
    loaded = state.get("loaded_generation") if isinstance(state.get("loaded_generation"), dict) else {}
    disk = state.get("disk_generation") if isinstance(state.get("disk_generation"), dict) else {}
    started_at = str(loaded.get("started_at_utc") or "").strip()
    published_loaded_sha = str(loaded.get("sha256") or loaded.get("generation_sha256") or "").strip()
    disk_rows = disk.get("files") if isinstance(disk.get("files"), list) else []
    if not started_at or not published_loaded_sha or not disk_rows:
        raise ValueError("restart state lacks loaded start/sha or disk generation rows")

    loaded_commit = _resolve_loaded_commit(root, started_at)
    source_blob = _blob_at_commit(root, loaded_commit, HISTORICAL_GENERATION_SOURCE)
    if source_blob is None:
        raise ValueError(f"historical generation source missing at {loaded_commit}")
    loaded_paths = _historical_generation_paths(source_blob.decode("utf-8"))
    loaded_rows: list[dict[str, Any]] = []
    loaded_sha_by_path: dict[str, str | None] = {}
    for path in loaded_paths:
        blob = _blob_at_commit(root, loaded_commit, path)
        file_sha = _sha256(blob) if blob is not None else None
        loaded_sha_by_path[path] = file_sha
        loaded_rows.append({"path": path, "exists": blob is not None, "sha256": file_sha})
    reconstructed_sha = _aggregate_generation(loaded_rows)
    reconstruction_status = "MATCH" if reconstructed_sha == published_loaded_sha else "MISMATCH"

    rows: list[dict[str, Any]] = []
    disk_paths: set[str] = set()
    for disk_row in disk_rows:
        if not isinstance(disk_row, dict):
            continue
        path = str(disk_row.get("path") or "").strip()
        if not path:
            continue
        disk_paths.add(path)
        loaded_sha = loaded_sha_by_path.get(path)
        disk_sha = str(disk_row.get("sha256") or "").strip() or None
        loaded_exists = path in loaded_sha_by_path and loaded_sha is not None
        disk_exists = bool(disk_row.get("exists")) and disk_sha is not None
        rows.append(
            {
                "path": path,
                "loaded_sha256": loaded_sha,
                "disk_sha256": disk_sha,
                "loaded_exists": loaded_exists,
                "disk_exists": disk_exists,
                "changed": loaded_exists != disk_exists or loaded_sha != disk_sha,
                "loaded_commit": loaded_commit,
            }
        )

    changed_count = sum(1 for row in rows if row["changed"])
    return {
        "schema_version": 1,
        "kind": "guard_generation_delta",
        "flow_stage": "LIVE/DEFEND/MEASURE",
        "generated_at": generated_at or _utc_now(),
        "status": "PASS_CITABLE" if reconstruction_status == "MATCH" else "FAIL_RECONSTRUCTION_MISMATCH",
        "measurement_only": True,
        "live_mutation": False,
        "reconstructed": True,
        "reconstruction_status": reconstruction_status,
        "loaded_started_at_utc": started_at,
        "loaded_pid": loaded.get("pid"),
        "loaded_commit": loaded_commit,
        "published_loaded_generation_sha256": published_loaded_sha,
        "reconstructed_loaded_generation_sha256": reconstructed_sha,
        "disk_generation_sha256": str(disk.get("sha256") or "").strip() or None,
        "historical_loaded_file_count": len(loaded_rows),
        "comparison_file_count": len(rows),
        "loaded_only_paths": [path for path in loaded_paths if path not in disk_paths],
        "changed_count": changed_count,
        "rows": rows,
        "citation_allowed": reconstruction_status == "MATCH",
        "rule": "per-file delta is citable only when the historical tuple reconstructs the resident aggregate sha",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    root = args.root.resolve()
    state_path = args.state if args.state.is_absolute() else root / args.state
    output_path = args.output if args.output.is_absolute() else root / args.output
    state = json.loads(state_path.read_text(encoding="utf-8"))
    payload = build_report(root=root, state=state)
    _write_json(output_path, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["citation_allowed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
