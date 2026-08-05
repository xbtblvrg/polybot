"""Small JSONL/log retention helpers for wallet-copy data files."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


MIB = 1024 * 1024


DEFAULT_MAX_BYTES = 512 * MIB
DEFAULT_KEEP_BYTES = 128 * MIB
CHECK_INTERVAL_S = 30.0

RETENTION_RULES: dict[str, tuple[int, int]] = {
    "data/lead_lag_raw_pm_events.jsonl": (256 * MIB, 128 * MIB),
    "data/research/wallet_copy_events.jsonl": (256 * MIB, 128 * MIB),
    "data/research/wallet_copy_ml_dataset.jsonl": (256 * MIB, 128 * MIB),
    "data/research/wallet_copy_paper_events.jsonl": (256 * MIB, 128 * MIB),
    "data/research/wallet_copy_inventory_paper_events.jsonl": (256 * MIB, 128 * MIB),
    "data/research/weird_peak_exact_copy_paper_flow_events.jsonl": (256 * MIB, 128 * MIB),
    "data/research/weird_peak_exact_copy_wallet_tracker_events.jsonl": (128 * MIB, 64 * MIB),
}

_LAST_CHECK_MONOTONIC: dict[str, float] = {}


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _rel_key(path: Path) -> str:
    try:
        return path.resolve().relative_to(_root()).as_posix()
    except ValueError:
        return path.as_posix()


def retention_limits(path: str | Path) -> tuple[int, int]:
    key = _rel_key(Path(path))
    return RETENTION_RULES.get(key, (DEFAULT_MAX_BYTES, DEFAULT_KEEP_BYTES))


def truncate_log_tail(
    path: str | Path,
    *,
    max_bytes: int | None = None,
    keep_bytes: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Keep only the tail of an oversized text log, in-place.

    The file name and inode stay stable, which is friendlier for live processes
    that keep appending with ``open(..., "a")``. For huge single-line JSONL
    records, the retained tail would be invalid JSON; in that case we replace it
    with a compact sentinel row and deliberately drop the oversized line.
    """

    p = Path(path)
    max_limit, keep_limit = retention_limits(p)
    max_bytes = int(max_bytes if max_bytes is not None else max_limit)
    keep_bytes = int(keep_bytes if keep_bytes is not None else keep_limit)
    if max_bytes <= 0 or keep_bytes <= 0:
        return {"path": str(p), "action": "disabled", "max_bytes": max_bytes, "keep_bytes": keep_bytes}
    if not p.exists() or not p.is_file():
        return {"path": str(p), "action": "missing", "max_bytes": max_bytes, "keep_bytes": keep_bytes}

    before = p.stat().st_size
    if before <= max_bytes:
        return {"path": str(p), "action": "skip", "before_bytes": before, "max_bytes": max_bytes}

    if dry_run:
        return {
            "path": str(p),
            "action": "would_truncate",
            "before_bytes": before,
            "max_bytes": max_bytes,
            "keep_bytes": keep_bytes,
        }

    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("rb+") as handle:
        start = max(0, before - keep_bytes)
        handle.seek(start)
        data = handle.read()
        partial_line_dropped = False
        oversized_single_line = False
        if start > 0:
            newline_index = data.find(b"\n")
            if newline_index >= 0:
                data = data[newline_index + 1 :]
                partial_line_dropped = True
            else:
                oversized_single_line = True
                data = (
                    json.dumps(
                        {
                            "kind": "log_retention_truncated_oversized_line",
                            "path": _rel_key(p),
                            "original_size_bytes": before,
                            "max_bytes": max_bytes,
                            "keep_bytes": keep_bytes,
                            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                ).encode("utf-8")
        handle.seek(0)
        handle.truncate(0)
        handle.write(data)
        handle.flush()

    after = p.stat().st_size
    return {
        "path": str(p),
        "action": "truncated",
        "before_bytes": before,
        "after_bytes": after,
        "freed_bytes": max(0, before - after),
        "max_bytes": max_bytes,
        "keep_bytes": keep_bytes,
        "partial_line_dropped": partial_line_dropped,
        "oversized_single_line": oversized_single_line,
    }


def maybe_truncate_log_tail(path: str | Path, *, interval_s: float = CHECK_INTERVAL_S) -> dict[str, Any] | None:
    if os.environ.get("POLYMARKET_DISABLE_LOG_RETENTION") == "1":
        return None
    p = Path(path)
    key = str(p.resolve()) if p.exists() else str(p)
    now = time.monotonic()
    last = _LAST_CHECK_MONOTONIC.get(key, 0.0)
    if now - last < interval_s:
        return None
    _LAST_CHECK_MONOTONIC[key] = now
    try:
        return truncate_log_tail(p)
    except OSError:
        return None
