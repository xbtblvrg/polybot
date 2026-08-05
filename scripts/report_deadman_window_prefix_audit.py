#!/usr/bin/env python3
"""Audit seven-day deadman approvals granted through the ``window:`` prefix."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.order_flow_deadman import (  # noqa: E402
    GATE_EVIDENCE_STALE_EXACT,
    _taxonomy_reason_class,
    _unapproved_gated_quiet_reasons,
)
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402

DEFAULT_INCIDENTS = ROOT / "data/research/order_flow_deadman_incidents.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/deadman_window_prefix_audit_latest.json"


def _parse(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.astimezone(dt.timezone.utc)


def build_report(*, incidents_path: Path, now: dt.datetime, days: int = 7) -> dict[str, Any]:
    start = now - dt.timedelta(days=days)
    counts: Counter[str] = Counter()
    incident_snapshots: Counter[str] = Counter()
    incident_rows = 0
    earliest = None
    latest = None
    if incidents_path.exists():
        for line in incidents_path.open("r", encoding="utf-8", errors="ignore"):
            try:
                incident = json.loads(line)
            except json.JSONDecodeError:
                continue
            checked = _parse(incident.get("checked_at"))
            if checked is None or checked < start or checked > now:
                continue
            incident_rows += 1
            earliest = checked if earliest is None or checked < earliest else earliest
            latest = checked if latest is None or checked > latest else latest
            quiet = incident.get("gated_quiet_classification") or {}
            taxonomy = quiet.get("decision_gate_taxonomy") or {}
            for reason, count in taxonomy.items():
                if str(reason).startswith("window:"):
                    counts[str(reason)] += int(count or 0)
                    incident_snapshots[str(reason)] += 1

    rows = []
    for reason, count in sorted(counts.items()):
        base = reason.removeprefix("window:")
        classification = _taxonomy_reason_class(reason)
        full_unapproved = _unapproved_gated_quiet_reasons({reason: 1})
        base_unapproved = _unapproved_gated_quiet_reasons({base: 1})
        approved = not full_unapproved
        prefix_only_approval = approved and bool(base_unapproved)
        rows.append(
            {
                "reason": reason,
                "base_reason": base,
                "snapshot_multiplicative_count": count,
                "distinct_incident_snapshots": incident_snapshots[reason],
                "classification": classification,
                "approved": approved,
                "full_reason_unapproved": full_unapproved,
                "base_reason_unapproved": base_unapproved,
                "base_reason_evidence_stale": base in GATE_EVIDENCE_STALE_EXACT,
                "prefix_only_approval": prefix_only_approval,
            }
        )

    observed_span_s = (
        max(0.0, (latest - earliest).total_seconds())
        if earliest is not None and latest is not None
        else None
    )
    full_coverage = bool(earliest and earliest <= start)
    verdict = "PASS" if full_coverage else "INSUFFICIENT_OBSERVED_SPAN"

    return {
        "schema_version": 1,
        "kind": "deadman_window_prefix_audit",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "measurement_only": True,
        "live_mutation": False,
        "window": {"days": days, "start": start.isoformat(), "end": now.isoformat()},
        "source": str(incidents_path),
        "source_coverage": {
            "incident_rows": incident_rows,
            "earliest_checked_at": earliest.isoformat() if earliest else None,
            "latest_checked_at": latest.isoformat() if latest else None,
            "observed_span_s": observed_span_s,
            "observed_span_h": (
                round(observed_span_s / 3600.0, 6)
                if observed_span_s is not None
                else None
            ),
            "full_seven_day_coverage": full_coverage,
        },
        "rows": rows,
        "summary": {
            "verdict": verdict,
            "requested_window_days": days,
            "observed_span_s": observed_span_s,
            "full_seven_day_coverage": full_coverage,
            "counter_semantics": "snapshot_multiplicative_not_distinct_windows",
            "distinct_incident_snapshots": incident_rows,
            "distinct_window_prefix_reasons": len(rows),
            "approved_reasons": sum(1 for row in rows if row["approved"]),
            "prefix_only_approved_reasons": [
                row["reason"] for row in rows if row["prefix_only_approval"]
            ],
            "evidence_stale_reasons": [
                row["reason"] for row in rows if row["base_reason_evidence_stale"]
            ],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incidents", type=Path, default=DEFAULT_INCIDENTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    now = dt.datetime.now(dt.timezone.utc)
    payload = build_report(incidents_path=args.incidents, now=now, days=args.days)
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
