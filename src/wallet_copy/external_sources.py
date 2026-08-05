"""External bot-source registry for wallet-copy research.

The registry is deliberately advisory. A source can inspire code, metrics, or
backlog items, but it cannot grant live admission. Local paper/live-tracker
truth remains the only admission path.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from src.wallet_copy.store import load_json


PRIORITY_ORDER = {
    "P0": 0,
    "P1": 1,
    "P2": 2,
    "research": 3,
    "blocked": 4,
}


def _priority_rank(priority: str) -> int:
    return PRIORITY_ORDER.get(priority, 99)


def load_external_bot_sources(path: str | Path = "configs/wallet_copy/external_bot_sources.json") -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    return payload


def validate_external_bot_sources(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    sources = payload.get("sources")
    if not isinstance(sources, list) or not sources:
        return ["missing_sources"]
    seen: set[str] = set()
    required = {"id", "name", "url", "priority", "status", "primary_use", "dry_run_claim_status"}
    for index, source in enumerate(sources):
        if not isinstance(source, dict):
            issues.append(f"source_{index}_not_object")
            continue
        missing = sorted(required - set(source))
        for key in missing:
            issues.append(f"{source.get('id', f'source_{index}')}:missing_{key}")
        source_id = str(source.get("id") or "")
        if not source_id:
            issues.append(f"source_{index}:missing_id")
        elif source_id in seen:
            issues.append(f"{source_id}:duplicate_id")
        seen.add(source_id)
        if source.get("priority") not in PRIORITY_ORDER:
            issues.append(f"{source_id}:unknown_priority")
        dry_run_status = str(source.get("dry_run_claim_status") or "")
        if dry_run_status == "accepted_as_live_evidence":
            issues.append(f"{source_id}:dry_run_claim_wrongly_accepted")
        if source.get("priority") == "P0":
            capability_map = source.get("capability_map")
            if not isinstance(capability_map, list) or not capability_map:
                issues.append(f"{source_id}:p0_missing_capability_map")
            else:
                for item in capability_map:
                    if not isinstance(item, dict):
                        issues.append(f"{source_id}:capability_map_item_not_object")
                        continue
                    if not item.get("capability"):
                        issues.append(f"{source_id}:capability_missing_name")
                    if item.get("status") not in {"implemented", "partial", "missing"}:
                        issues.append(f"{source_id}:{item.get('capability', 'capability')}:unknown_capability_status")
    return issues


def summarize_external_bot_sources(payload: dict[str, Any]) -> dict[str, Any]:
    sources = [row for row in payload.get("sources") or [] if isinstance(row, dict)]
    priority_counts = Counter(str(row.get("priority") or "unknown") for row in sources)
    status_counts = Counter(str(row.get("status") or "unknown") for row in sources)
    relevance_counts = Counter(str(row.get("btc_5m_relevance") or "unknown") for row in sources)
    actionable = sorted(
        (
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "priority": row.get("priority"),
                "primary_use": row.get("primary_use"),
                "url": row.get("url"),
                "import_actions": row.get("import_actions") or [],
            }
            for row in sources
            if str(row.get("priority")) in {"P0", "P1"}
        ),
        key=lambda row: (_priority_rank(str(row.get("priority"))), str(row.get("id"))),
    )
    blocked = [
        {
            "id": row.get("id"),
            "name": row.get("name"),
            "reason": row.get("non_import_reasons") or [],
            "url": row.get("url"),
        }
        for row in sources
        if row.get("status") == "blocked" or row.get("priority") == "blocked"
    ]
    capability_rows: list[dict[str, Any]] = []
    missing_capabilities: list[dict[str, Any]] = []
    for row in sources:
        for item in row.get("capability_map") or []:
            if not isinstance(item, dict):
                continue
            capability = {
                "source_id": row.get("id"),
                "source_name": row.get("name"),
                "priority": row.get("priority"),
                "capability": item.get("capability"),
                "status": item.get("status"),
                "local_modules": item.get("local_modules") or [],
                "local_tests": item.get("local_tests") or [],
                "backlog": item.get("backlog"),
            }
            capability_rows.append(capability)
            if item.get("status") in {"missing", "partial"}:
                missing_capabilities.append(capability)
    return {
        "schema_version": 1,
        "kind": "external_wallet_copy_source_summary",
        "source_count": len(sources),
        "priority_counts": dict(sorted(priority_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "btc_5m_relevance_counts": dict(sorted(relevance_counts.items())),
        "actionable_sources": actionable,
        "blocked_sources": blocked,
        "local_capabilities": capability_rows,
        "missing_local_capabilities": missing_capabilities,
        "validation_issues": validate_external_bot_sources(payload),
        "adoption_rules": payload.get("adoption_rules") or [],
    }
