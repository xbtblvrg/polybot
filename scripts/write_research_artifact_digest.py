#!/usr/bin/env python3
"""Write bounded, committable digests for large local research artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_ARTIFACTS = (
    (
        "data/research/routing_shadow_validation_latest.json",
        "data/research/routing_shadow_validation_latest_digest.json",
    ),
    (
        "data/research/wallet_market_cohort_replay_latest.json",
        "data/research/wallet_market_cohort_replay_latest_digest.json",
    ),
)
CLOCK_FRAGMENTS = ("clock", "deadline", "expires", "earliest", "not_before", "lock_at", "unlock_at")


def _list_row_count(value: Any) -> int:
    if isinstance(value, list):
        return len(value) + sum(_list_row_count(item) for item in value)
    if isinstance(value, dict):
        return sum(_list_row_count(item) for item in value.values())
    return 0


def _clock_fields(value: Any, *, prefix: str = "", limit: int = 64) -> dict[str, Any]:
    found: dict[str, Any] = {}
    if not isinstance(value, dict):
        return found
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        lowered = str(key).lower()
        if any(fragment in lowered for fragment in CLOCK_FRAGMENTS) and not isinstance(item, (dict, list)):
            found[path] = item
            if len(found) >= limit:
                break
        if isinstance(item, dict) and len(found) < limit:
            found.update(_clock_fields(item, prefix=path, limit=limit - len(found)))
    return found


def _bounded_counters(value: Any, *, depth: int = 0, max_entries: int = 128) -> Any:
    """Keep scalar counter structure bounded; represent row arrays by counts."""
    if isinstance(value, list):
        return {"row_count": len(value)}
    if not isinstance(value, dict):
        return value
    if depth >= 3:
        return {"field_count": len(value)}
    bounded: dict[str, Any] = {}
    for key in sorted(value)[:max_entries]:
        item = value[key]
        if isinstance(item, (str, int, float, bool)) or item is None:
            bounded[key] = item
        elif isinstance(item, list):
            bounded[key] = {"row_count": len(item)}
        elif isinstance(item, dict):
            bounded[key] = _bounded_counters(item, depth=depth + 1, max_entries=max_entries)
    if len(value) > max_entries:
        bounded["_omitted_field_count"] = len(value) - max_entries
    return bounded


def build_digest(source: Path, *, repo_root: Path = ROOT) -> dict[str, Any]:
    raw = source.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"research artifact must be a JSON object: {source}")
    try:
        source_label = str(source.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        source_label = str(source)
    top_level_lists = {
        key: len(value)
        for key, value in payload.items()
        if isinstance(value, list)
    }
    counters = _bounded_counters(payload.get("summary") if isinstance(payload.get("summary"), dict) else {})
    return {
        "schema_version": 1,
        "kind": "bounded_research_artifact_digest",
        "flow_stage": payload.get("flow_stage") or payload.get("flow_stages") or "OBSERVE/LEARN",
        "generated_at": utc_now_iso(),
        "source_artifact": source_label,
        "source_status": payload.get("status"),
        "source_generated_at": payload.get("generated_at") or payload.get("updated_at"),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": _list_row_count(payload),
        "top_level_list_counts": top_level_lists,
        "counters": counters,
        "preregistered_clock_fields": _clock_fields(payload),
        "full_payload_policy": "local ignored state; verify integrity with sha256 before use",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", action="append", nargs=2, metavar=("SOURCE", "DIGEST"))
    args = parser.parse_args()
    artifacts = args.artifact or DEFAULT_ARTIFACTS
    outputs = []
    for source_value, output_value in artifacts:
        source = ROOT / source_value
        output = ROOT / output_value
        digest = build_digest(source)
        atomic_write_json(output, digest)
        outputs.append({"source": source_value, "digest": output_value, "sha256": digest["sha256"], "rows": digest["row_count"]})
    print(json.dumps({"status": "PASS", "artifacts": outputs}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
