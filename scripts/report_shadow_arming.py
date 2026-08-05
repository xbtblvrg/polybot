#!/usr/bin/env python3
"""Report measurement-only shadow arming latency from realtime detections.

This script deliberately does not construct CopyIntents and does not touch
paper/live execution state. It only answers: if the Polygon WSS detection had
been the trigger, how much earlier could an intent have been armed compared
with the existing Data API first-seen path?
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_detection_latency import _iter_jsonl, _latency_stats, _matched_polygon_dataapi_rows
from src.wallet_copy.mission import WALLET_COPY_MISSION_CONTRACT
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-jsonl", default="data/research/polygon_orderfilled_ws_capture.jsonl")
    parser.add_argument("--dataapi-first-seen-jsonl", default="data/research/dataapi_first_seen.jsonl")
    parser.add_argument("--report", default="data/research/detection_latency_report.json")
    parser.add_argument("--sample-limit", type=int, default=500)
    parser.add_argument(
        "--decision-overhead-s",
        action="append",
        type=float,
        default=[],
        help="Decision overhead variants to add to WSS receive time. Defaults to 0.0 and 0.25.",
    )
    return parser.parse_args()


def _mission_max_event_age_s() -> float:
    runtime = WALLET_COPY_MISSION_CONTRACT.get("current_runtime_phase_contract") or {}
    profitability = runtime.get("profitability_filter_contract") if isinstance(runtime, dict) else {}
    try:
        return float((profitability or {}).get("max_event_age_s"))
    except (TypeError, ValueError):
        return 30.0


def _overhead_key(overhead_s: float) -> str:
    return f"overhead_{str(overhead_s).replace('.', '_')}s"


def _shadow_row(match: dict[str, Any], *, overhead_s: float, max_event_age_s: float) -> dict[str, Any] | None:
    try:
        ws_recv_ts = float(match.get("ws_recv_ts"))
        dataapi_seen_ts = float(match.get("dataapi_first_seen_ts"))
        block_ts = float(match.get("block_ts"))
    except (TypeError, ValueError):
        return None
    ws_armable_ts = ws_recv_ts + max(0.0, float(overhead_s))
    dataapi_armable_ts = dataapi_seen_ts
    ws_age_at_arm_s = ws_armable_ts - block_ts
    dataapi_age_at_arm_s = dataapi_armable_ts - block_ts
    return {
        "wallet": match.get("wallet"),
        "tx": match.get("tx"),
        "block_ts": block_ts,
        "ws_recv_ts": ws_recv_ts,
        "ws_armable_ts": ws_armable_ts,
        "dataapi_armable_ts": dataapi_armable_ts,
        "ws_age_at_arm_s": ws_age_at_arm_s,
        "dataapi_age_at_arm_s": dataapi_age_at_arm_s,
        "arming_advantage_s": dataapi_armable_ts - ws_armable_ts,
        "ws_would_pass_freshness": ws_age_at_arm_s <= max_event_age_s,
        "dataapi_would_pass_freshness": dataapi_age_at_arm_s <= max_event_age_s,
        "ws_would_pass_2s_budget": ws_age_at_arm_s <= 2.0,
        "dataapi_would_pass_2s_budget": dataapi_age_at_arm_s <= 2.0,
        "polygon_side": match.get("polygon_side"),
        "dataapi_side": match.get("dataapi_side"),
        "note": "measurement_only_no_copyintent_created",
    }


def _variant_summary(
    matched_rows: list[dict[str, Any]],
    *,
    overhead_s: float,
    max_event_age_s: float,
    sample_limit: int,
) -> dict[str, Any]:
    rows = [
        row
        for match in matched_rows
        if (row := _shadow_row(match, overhead_s=overhead_s, max_event_age_s=max_event_age_s)) is not None
    ]
    ws_ages = [float(row["ws_age_at_arm_s"]) for row in rows]
    dataapi_ages = [float(row["dataapi_age_at_arm_s"]) for row in rows]
    advantages = [float(row["arming_advantage_s"]) for row in rows]
    sample_rows = sorted(rows, key=lambda row: (float(row.get("ws_armable_ts") or 0.0), row.get("wallet") or ""))
    count = len(rows)
    return {
        "decision_overhead_s": overhead_s,
        "count": count,
        "ws_age_at_arm": _latency_stats(ws_ages),
        "dataapi_age_at_arm": _latency_stats(dataapi_ages),
        "arming_advantage": _latency_stats(advantages),
        "ws_freshness_pass_count": sum(1 for row in rows if row["ws_would_pass_freshness"]),
        "dataapi_freshness_pass_count": sum(1 for row in rows if row["dataapi_would_pass_freshness"]),
        "ws_freshness_pass_fraction": (sum(1 for row in rows if row["ws_would_pass_freshness"]) / count) if count else 0.0,
        "dataapi_freshness_pass_fraction": (sum(1 for row in rows if row["dataapi_would_pass_freshness"]) / count) if count else 0.0,
        "ws_2s_budget_pass_count": sum(1 for row in rows if row["ws_would_pass_2s_budget"]),
        "dataapi_2s_budget_pass_count": sum(1 for row in rows if row["dataapi_would_pass_2s_budget"]),
        "ws_2s_budget_pass_fraction": (sum(1 for row in rows if row["ws_would_pass_2s_budget"]) / count) if count else 0.0,
        "dataapi_2s_budget_pass_fraction": (sum(1 for row in rows if row["dataapi_would_pass_2s_budget"]) / count) if count else 0.0,
        "sample_rows": sample_rows[-max(0, int(sample_limit)):],
    }


def build_shadow_summary(
    polygon_rows: list[dict[str, Any]],
    dataapi_rows: list[dict[str, Any]],
    *,
    sample_limit: int,
    decision_overheads_s: list[float] | None = None,
) -> dict[str, Any]:
    matched_rows, backfill_rows = _matched_polygon_dataapi_rows(polygon_rows, dataapi_rows)
    overheads = decision_overheads_s or [0.0, 0.25]
    normalized_overheads = []
    for overhead in overheads:
        value = max(0.0, float(overhead))
        if value not in normalized_overheads:
            normalized_overheads.append(value)
    max_event_age_s = _mission_max_event_age_s()
    variants = {
        _overhead_key(overhead): _variant_summary(
            matched_rows,
            overhead_s=overhead,
            max_event_age_s=max_event_age_s,
            sample_limit=sample_limit,
        )
        for overhead in normalized_overheads
    }
    return {
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "copyintents_created": 0,
        "orders_submitted": 0,
        "source": "polygon_ws_orderfilled_v2_primary_exchange",
        "max_event_age_s": max_event_age_s,
        "informational_budget_s": 2.0,
        "matched_count": len(matched_rows),
        "backfill_excluded_count": len(backfill_rows),
        "variants": variants,
    }


def main() -> int:
    args = parse_args()
    summary = build_shadow_summary(
        _iter_jsonl(args.polygon_jsonl),
        _iter_jsonl(args.dataapi_first_seen_jsonl),
        sample_limit=int(args.sample_limit),
        decision_overheads_s=args.decision_overhead_s or [0.0, 0.25],
    )
    report = load_json(args.report, default={})
    if not isinstance(report, dict):
        report = {}
    report_summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    report_summary["shadow_arming"] = summary
    report["summary"] = report_summary
    report["updated_at"] = utc_now_iso()
    atomic_write_json(args.report, report)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
