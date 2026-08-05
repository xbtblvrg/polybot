#!/usr/bin/env python3
"""Restore files captured by a live golden config snapshot."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any


def _load_snapshot(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict) or payload.get("kind") != "live_golden_config_snapshot":
        raise ValueError(f"not a live golden config snapshot: {path}")
    return payload


def restore(snapshot_path: Path, root: Path, *, dry_run: bool, confirm: bool) -> dict[str, Any]:
    snapshot = _load_snapshot(snapshot_path)
    files = snapshot.get("config_files") if isinstance(snapshot.get("config_files"), dict) else {}
    plan: list[dict[str, Any]] = []
    for rel, meta in sorted(files.items()):
        if not isinstance(meta, dict):
            continue
        if "text" in meta:
            content = str(meta["text"])
        elif meta.get("content_asset"):
            asset = snapshot_path.parent / str(meta["content_asset"])
            with gzip.open(asset, "rb") as handle:
                content = handle.read().decode("utf-8")
        else:
            continue
        path = root / rel
        plan.append({"path": rel, "bytes": len(content.encode("utf-8"))})
        if dry_run:
            continue
        if not confirm:
            raise ValueError("actual restore requires --confirm-live-restore")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return {
        "status": "DRY_RUN" if dry_run else "RESTORED",
        "snapshot": str(snapshot_path),
        "restorable_files": len(plan),
        "plan": plan,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-live-restore", action="store_true")
    args = parser.parse_args(argv)

    result = restore(
        Path(args.snapshot),
        Path(args.root).resolve(),
        dry_run=bool(args.dry_run),
        confirm=bool(args.confirm_live_restore),
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
