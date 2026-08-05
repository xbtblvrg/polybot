#!/usr/bin/env python3
"""Build read-only row-level selected-member eligible-intent to guard-submit attribution."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_f418_acceptance_funnel import _iso_epoch, _jsonl, build_report  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


def build_selected_report(
    *,
    routing_shadow: dict[str, Any],
    guard_cycles: list[dict[str, Any]],
    execution_events: list[dict[str, Any]],
    ledger: dict[str, Any],
    day: str,
    since_at: str,
    generated_at: str,
) -> dict[str, Any]:
    since_s = _iso_epoch(since_at)
    if since_s is None:
        raise ValueError(f"invalid since_at: {since_at}")
    selected_wallets: list[str] = []
    for cycle in guard_cycles:
        if str(cycle.get("event") or "") != "wallet_copy_live_guard_cycle":
            continue
        cycle_s = _iso_epoch(cycle.get("generated_at"))
        wallet = str(cycle.get("source_wallet") or "").lower()
        if cycle_s is not None and cycle_s >= since_s and wallet and wallet not in selected_wallets:
            selected_wallets.append(wallet)

    member_reports = [
        build_report(
            routing_shadow=routing_shadow,
            guard_cycles=guard_cycles,
            execution_events=execution_events,
            ledger=ledger,
            source_wallet=wallet,
            day=day,
            generated_at=generated_at,
            since_at=since_at,
        )
        for wallet in selected_wallets
    ]
    rows = sorted(
        (row for report in member_reports for row in report["selected_member_rows"]),
        key=lambda row: (float(row.get("observed_ts") or 0.0), str(row.get("intent_id") or "")),
    )
    stage_counts = Counter(str(row.get("terminal_stage") or "") for row in rows)
    class_counts = Counter(str(row.get("attribution_class") or "") for row in rows)
    defect = (
        "WIRING_DEFECT"
        if class_counts["wiring_defect"]
        else ("TELEMETRY_DEFECT" if class_counts["telemetry_defect"] else "NONE")
    )
    return {
        "schema_version": 1,
        "kind": "selected_member_guard_submit_attribution",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "generated_at": generated_at,
        "day_utc": day,
        "measurement_started_at": since_at,
        "status": "FAIL" if defect != "NONE" else "PASS",
        "defect_classification": defect,
        "selected_wallets": selected_wallets,
        "selected_wallet_count": len(selected_wallets),
        "selected_policy_eligible_unique_intents": len(rows),
        "guard_terminal_intents": class_counts["guard_terminal"],
        "submitted_intents": class_counts["submitted"],
        "telemetry_defects": class_counts["telemetry_defect"],
        "wiring_defects": class_counts["wiring_defect"],
        "terminal_stage_counts": dict(sorted(stage_counts.items())),
        "rule": (
            "a selected eligible intent with a positive post-window-fill-cap count and no exact "
            "ledger order is a wiring defect; an otherwise selected intent lacking an exact terminal "
            "gate row is a telemetry defect"
        ),
        "member_reports": [
            {
                "source_wallet": report["source_wallet"],
                "selected_policy_eligible_unique_intents": report[
                    "selected_policy_eligible_unique_intents"
                ],
                "terminal_stage_counts": dict(
                    sorted(Counter(row["terminal_stage"] for row in report["selected_member_rows"]).items())
                ),
                "attribution": report["selected_member_attribution"],
            }
            for report in member_reports
        ],
        "rows": rows,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--since", required=True)
    parser.add_argument("--routing-shadow", default="data/research/routing_shadow_validation_latest.json")
    parser.add_argument("--guard-events", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--execution-events", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument(
        "--output", default="data/research/selected_member_guard_submit_attribution_latest.json"
    )
    args = parser.parse_args()
    report = build_selected_report(
        routing_shadow=json.loads((ROOT / args.routing_shadow).read_text()),
        guard_cycles=list(_jsonl(ROOT / args.guard_events) or []),
        execution_events=list(_jsonl(ROOT / args.execution_events) or []),
        ledger=json.loads((ROOT / args.ledger).read_text()),
        day=args.day,
        since_at=args.since,
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    atomic_write_json(ROOT / args.output, report)
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "defect_classification",
                    "selected_wallet_count",
                    "selected_policy_eligible_unique_intents",
                    "submitted_intents",
                    "terminal_stage_counts",
                )
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
