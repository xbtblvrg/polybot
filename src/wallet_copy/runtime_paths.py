"""Canonical runtime data paths shared by producers and consumers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNTIME_PATHS_CONFIG = ROOT / "configs/wallet_copy/runtime_paths.json"
LEGACY_RTDS_ACTIVITY_JSONL = (
    "data/research/polymarket_activity_ws_capture_vpn_burnin_20260703T180934Z.jsonl"
)


def _load_runtime_paths(path: Path | None = None) -> dict[str, Any]:
    config_path = path or Path(
        os.getenv("WALLET_COPY_RUNTIME_PATHS_CONFIG", str(DEFAULT_RUNTIME_PATHS_CONFIG))
    )
    try:
        payload = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def rtds_activity_jsonl(path: Path | None = None) -> str:
    """Return the one configured RTDS producer/consumer path.

    ``WALLET_COPY_RTDS_JSONL`` remains an emergency runtime override, while
    the checked-in config is the normal single source shared by launchd and
    Python callers.
    """

    override = os.getenv("WALLET_COPY_RTDS_JSONL", "").strip()
    if override:
        return override
    value = str(_load_runtime_paths(path).get("rtds_activity_jsonl") or "").strip()
    return value or LEGACY_RTDS_ACTIVITY_JSONL


DEFAULT_RTDS_ACTIVITY_JSONL = rtds_activity_jsonl()
