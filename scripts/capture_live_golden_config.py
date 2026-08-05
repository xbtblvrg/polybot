#!/usr/bin/env python3
"""Capture the current producing live config as a golden rollback snapshot."""

from __future__ import annotations

import argparse
import fnmatch
import gzip
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = "data/research"
LATEST_NAME = "golden_config_latest.json"
RESTORE_SCRIPT = "scripts/restore_live_golden_config.py"
MAX_INLINE_TEXT_BYTES = 1_000_000

LIVE_CONFIG_GLOBS = (
    "configs/wallet_copy/*.json",
    "src/config.py",
    "src/wallet_copy/mission.py",
    "src/wallet_copy/promotion_rotation.py",
    "scripts/run_wallet_copy_live_guard.py",
    "scripts/run_wallet_copy_live_execution.py",
    "scripts/run_wallet_copy_hotlane_tick.py",
    "docs/agents/AUTONOMOUS_FLOW.md",
    "docs/agents/LIVE_TODAY_SPRINT.md",
    "docs/WALLET_COPY_OPERATING_FRAMEWORK.md",
)

RUNTIME_STATE_GLOBS = (
    "data/research/wallet_copy_live_guard_state.json",
    "data/research/wallet_copy_live_execution_state.json",
    "data/research/wallet_copy_active_set_overlay.json",
    "data/research/order_flow_deadman_state.json",
    "data/research/state_digest.json",
)

SECRET_PATTERNS = (
    ".env",
    ".env.*",
    "*secret*",
    "*private*",
    "*key*",
    "*token*",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_rel(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _is_secretish(rel: str) -> bool:
    name = Path(rel).name.lower()
    lowered = rel.lower()
    return any(fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(lowered, pat) for pat in SECRET_PATTERNS)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_text_file(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    meta: dict[str, Any] = {
        "sha256": _sha256(raw),
        "bytes": len(raw),
        "inline": len(raw) <= MAX_INLINE_TEXT_BYTES,
    }
    if len(raw) <= MAX_INLINE_TEXT_BYTES:
        meta["text"] = text
    return meta


def _compact_runtime_summary(payload: Any, *, max_inline_bytes: int = MAX_INLINE_TEXT_BYTES) -> Any:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    if len(encoded) <= max_inline_bytes:
        return payload if isinstance(payload, dict) else {}
    if not isinstance(payload, dict):
        return {"type": type(payload).__name__, "compact_reason": "large_non_dict"}
    summary: dict[str, Any] = {"compact_reason": "large_runtime_state", "top_level_keys": sorted(payload.keys())}
    for key, value in payload.items():
        if isinstance(value, dict):
            summary[key] = {"type": "dict", "keys": sorted(value.keys())[:40], "len": len(value)}
        elif isinstance(value, list):
            summary[key] = {"type": "list", "len": len(value)}
        elif isinstance(value, (str, int, float, bool)) or value is None:
            summary[key] = value
        else:
            summary[key] = {"type": type(value).__name__}
    return summary


def _glob_existing(root: Path, patterns: tuple[str, ...]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(path for path in root.glob(pattern) if path.is_file())
    return sorted(set(paths), key=lambda path: path.as_posix())


def _git_head(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return ""
    return result.stdout.strip()


def _git_status(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return []
    return [line.rstrip() for line in result.stdout.splitlines() if line.strip()]


def _guard_processes(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-fl", "run_wallet_copy_live_guard.py"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return []
    return [line.rstrip() for line in result.stdout.splitlines() if line.strip()]


def build_snapshot(root: Path, *, note: str = "") -> dict[str, Any]:
    config_files: dict[str, Any] = {}
    skipped: list[dict[str, str]] = []
    for path in _glob_existing(root, LIVE_CONFIG_GLOBS):
        rel = _safe_rel(path, root)
        if _is_secretish(rel):
            skipped.append({"path": rel, "reason": "secret_like_name"})
            continue
        try:
            config_files[rel] = _read_text_file(path)
        except UnicodeDecodeError:
            skipped.append({"path": rel, "reason": "non_utf8"})

    runtime_states: dict[str, Any] = {}
    for path in _glob_existing(root, RUNTIME_STATE_GLOBS):
        rel = _safe_rel(path, root)
        try:
            payload = json.loads(path.read_text())
        except Exception:
            payload = {}
        runtime_states[rel] = {
            "sha256": _sha256(path.read_bytes()),
            "bytes": path.stat().st_size,
            "summary": _compact_runtime_summary(payload),
        }

    snapshot: dict[str, Any] = {
        "kind": "live_golden_config_snapshot",
        "schema_version": 1,
        "created_at": _utc_now(),
        "flow_stage": "SELF-DEV/LIVE",
        "direction": "2026-07-10T19:10Z fable DIRECTION",
        "note": note,
        "git_head": _git_head(root),
        "git_status_short": _git_status(root),
        "guard_processes": _guard_processes(root),
        "config_files": config_files,
        "runtime_states": runtime_states,
        "skipped": skipped,
        "rollback": {
            "command_template": (
                f"{sys.executable} {RESTORE_SCRIPT} --snapshot {{snapshot_path}} "
                "--dry-run"
            ),
            "actual_restore_requires": "--confirm-live-restore",
        },
    }
    return snapshot


def write_snapshot(root: Path, snapshot: dict[str, Any], output_dir: str) -> Path:
    out_dir = root / output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = str(snapshot["created_at"]).replace(":", "").replace("-", "")
    path = out_dir / f"golden_config_{ts}.json"
    assets_dir_name = f"golden_config_{ts}_assets"
    config_files = snapshot.get("config_files") if isinstance(snapshot.get("config_files"), dict) else {}
    for rel, meta in config_files.items():
        if not isinstance(meta, dict) or meta.get("inline"):
            continue
        source = root / rel
        asset = out_dir / assets_dir_name / f"{rel}.gz"
        asset.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as src, gzip.open(asset, "wb") as dst:
            dst.write(src.read())
        meta["content_asset"] = f"{assets_dir_name}/{rel}.gz"
    text = json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
    path.write_text(text)
    (out_dir / LATEST_NAME).write_text(text)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--note", default="")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    snapshot = build_snapshot(root, note=args.note)
    path = write_snapshot(root, snapshot, args.output_dir)
    print(json.dumps({"status": "PASS", "snapshot": str(path), "files": len(snapshot["config_files"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
