#!/usr/bin/env python3
"""Append an experiment verdict to the local experiment verdict registry.

Flow stage: LEARN/SELF-DEV. This complements experiment pre-registration:
pre-registration says what would count; verdict records say what happened.
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

from src.wallet_copy.store import append_jsonl, atomic_write_json  # noqa: E402


DEFAULT_REGISTRY = "data/research/experiment_verdict_registry.jsonl"
DEFAULT_LATEST = "data/research/experiment_verdict_latest.json"
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.:-]{2,160}$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def build_verdict(args: argparse.Namespace, *, generated_at: str) -> dict[str, Any]:
    artifacts = [str(item).strip() for item in args.artifact if str(item).strip()]
    payload = {
        "schema_version": 1,
        "kind": "experiment_verdict",
        "flow_stage": str(args.flow_stage).strip(),
        "recorded_at": generated_at,
        "experiment_id": str(args.experiment_id).strip(),
        "verdict": str(args.verdict).strip(),
        "decision": str(args.decision).strip(),
        "primary_metric": str(args.primary_metric).strip(),
        "metric_value": float(args.metric_value),
        "threshold": str(args.threshold).strip(),
        "evidence": str(args.evidence).strip(),
        "result_artifacts": artifacts,
        "owner": str(args.owner).strip(),
    }
    errors: list[str] = []
    if not payload["experiment_id"]:
        errors.append("missing_experiment_id")
    elif not ID_PATTERN.match(payload["experiment_id"]):
        errors.append("invalid_experiment_id")
    for field in ("flow_stage", "verdict", "decision", "primary_metric", "threshold", "evidence", "owner"):
        if not str(payload.get(field) or "").strip():
            errors.append(f"missing_{field}")
    if not artifacts:
        errors.append("missing_result_artifacts")
    if errors:
        raise ValueError(",".join(sorted(errors)))
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--latest", default=DEFAULT_LATEST)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--flow-stage", required=True)
    parser.add_argument("--verdict", required=True)
    parser.add_argument("--decision", required=True)
    parser.add_argument("--primary-metric", required=True)
    parser.add_argument("--metric-value", type=float, required=True)
    parser.add_argument("--threshold", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--owner", default="Codex/Fable")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generated_at = _utc_now_iso()
    try:
        payload = build_verdict(args, generated_at=generated_at)
    except ValueError as exc:
        error_payload = {
            "schema_version": 1,
            "kind": "experiment_verdict_error",
            "status": "INVALID",
            "generated_at": generated_at,
            "errors": str(exc).split(","),
            "experiment_id": str(args.experiment_id).strip(),
        }
        atomic_write_json(_rooted(args.latest), error_payload)
        print(json.dumps(error_payload, sort_keys=True))
        return 2
    append_jsonl(_rooted(args.registry), payload)
    latest = {
        "schema_version": 1,
        "kind": "experiment_verdict_latest",
        "status": "PASS",
        "generated_at": generated_at,
        "latest": payload,
    }
    atomic_write_json(_rooted(args.latest), latest)
    print(json.dumps(latest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
