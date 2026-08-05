#!/usr/bin/env python3
"""Append-only experiment pre-registration.

Flow stage: LEARN/SELF-DEV. Experiments must declare their success and
failure criteria before result inspection. This script is the mechanical
entry point: incomplete records fail, duplicates fail unless explicitly
amended, and every run writes a latest summary for digest/background audits.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_REGISTRY = "data/research/experiment_preregistry.jsonl"
DEFAULT_SUMMARY = "data/research/experiment_preregistration_latest.json"
REQUIRED_TEXT_FIELDS = (
    "experiment_id",
    "flow_stage",
    "hypothesis",
    "success_criterion",
    "failure_criterion",
    "primary_metric",
    "measurement_window",
    "decision_rule",
    "deadline_utc",
    "owner",
)
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.:-]{2,120}$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _load_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                rows.append({"status": "INVALID_JSONL_ROW", "raw": line.strip()})
                continue
            rows.append(row if isinstance(row, dict) else {"status": "INVALID_JSONL_ROW", "raw": row})
    return rows


def _validate_record(record: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in REQUIRED_TEXT_FIELDS:
        if not str(record.get(field) or "").strip():
            errors.append(f"missing_{field}")
    experiment_id = str(record.get("experiment_id") or "")
    if experiment_id and not ID_PATTERN.match(experiment_id):
        errors.append("invalid_experiment_id")
    artifacts = record.get("result_artifacts")
    if not isinstance(artifacts, list) or not [item for item in artifacts if str(item or "").strip()]:
        errors.append("missing_result_artifacts")
    try:
        parsed = datetime.fromisoformat(str(record.get("deadline_utc") or "").replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            errors.append("deadline_utc_missing_timezone")
    except ValueError:
        errors.append("invalid_deadline_utc")
    return errors


def _summary(records: list[dict[str, Any]], *, generated_at: str, required_ids: list[str]) -> dict[str, Any]:
    valid_records = [row for row in records if isinstance(row, dict) and not _validate_record(row)]
    latest_by_id: dict[str, dict[str, Any]] = {}
    for row in valid_records:
        latest_by_id[str(row.get("experiment_id"))] = row
    missing_required = [item for item in required_ids if item not in latest_by_id]
    active = [row for row in latest_by_id.values() if str(row.get("status") or "") == "PREREGISTERED"]
    latest = valid_records[-1] if valid_records else {}
    return {
        "schema_version": 1,
        "kind": "experiment_preregistration_summary",
        "flow_stage": "LEARN/SELF-DEV",
        "generated_at": generated_at,
        "status": "PASS" if not missing_required else "MISSING_REQUIRED",
        "registry_records": len(records),
        "valid_records": len(valid_records),
        "active_count": len(active),
        "latest_experiment_id": latest.get("experiment_id"),
        "latest_success_criterion": latest.get("success_criterion"),
        "latest_deadline_utc": latest.get("deadline_utc"),
        "required_ids": required_ids,
        "missing_required_ids": missing_required,
        "active_ids": sorted(str(row.get("experiment_id") or "") for row in active),
        "rule": "experiments must pre-register success/failure criteria before result inspection",
    }


def _append_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--summary", default=DEFAULT_SUMMARY)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--tombstone-experiment-id", default="")
    parser.add_argument("--require-experiment-id", action="append", default=[])
    parser.add_argument("--amend", action="store_true")
    parser.add_argument("--experiment-id", default="")
    parser.add_argument("--flow-stage", default="")
    parser.add_argument("--hypothesis", default="")
    parser.add_argument("--success-criterion", default="")
    parser.add_argument("--failure-criterion", default="")
    parser.add_argument("--primary-metric", default="")
    parser.add_argument("--measurement-window", default="")
    parser.add_argument("--decision-rule", default="")
    parser.add_argument("--deadline-utc", default="")
    parser.add_argument("--owner", default="")
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--notes", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generated_at = _utc_now_iso()
    registry = _rooted(args.registry)
    summary_path = _rooted(args.summary)
    records = _load_records(registry)
    tombstone_id = str(args.tombstone_experiment_id or "").strip()
    if tombstone_id:
        latest = next(
            (
                row
                for row in reversed(records)
                if isinstance(row, dict)
                and str(row.get("experiment_id") or "") == tombstone_id
                and not _validate_record(row)
            ),
            None,
        )
        if latest is None:
            payload = {
                "status": "INVALID",
                "generated_at": generated_at,
                "errors": ["tombstone_experiment_id_not_found"],
                "experiment_id": tombstone_id,
            }
            atomic_write_json(summary_path, payload)
            print(json.dumps(payload, sort_keys=True))
            return 2
        record = dict(latest)
        record.update(
            {
                "status": "TOMBSTONED",
                "registered_at": generated_at,
                "notes": str(args.notes or "experiment retired by evidence ruling").strip(),
            }
        )
        _append_record(registry, record)
        records.append(record)
    elif not args.audit_only:
        record = {
            "schema_version": 1,
            "kind": "experiment_preregistration",
            "status": "PREREGISTERED",
            "registered_at": generated_at,
            "experiment_id": str(args.experiment_id).strip(),
            "flow_stage": str(args.flow_stage).strip(),
            "hypothesis": str(args.hypothesis).strip(),
            "success_criterion": str(args.success_criterion).strip(),
            "failure_criterion": str(args.failure_criterion).strip(),
            "primary_metric": str(args.primary_metric).strip(),
            "measurement_window": str(args.measurement_window).strip(),
            "decision_rule": str(args.decision_rule).strip(),
            "deadline_utc": str(args.deadline_utc).strip(),
            "owner": str(args.owner).strip(),
            "result_artifacts": [str(item).strip() for item in args.artifact if str(item).strip()],
            "notes": str(args.notes).strip(),
        }
        errors = _validate_record(record)
        existing_ids = {
            str(row.get("experiment_id") or "")
            for row in records
            if isinstance(row, dict) and not _validate_record(row)
        }
        if record["experiment_id"] in existing_ids and not args.amend:
            errors.append("duplicate_experiment_id_requires_amend")
        if errors:
            payload = {
                "status": "INVALID",
                "generated_at": generated_at,
                "errors": sorted(set(errors)),
                "experiment_id": record.get("experiment_id"),
            }
            atomic_write_json(summary_path, payload)
            print(json.dumps(payload, sort_keys=True))
            return 2
        _append_record(registry, record)
        records.append(record)
    payload = _summary(records, generated_at=generated_at, required_ids=list(args.require_experiment_id or []))
    atomic_write_json(summary_path, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
