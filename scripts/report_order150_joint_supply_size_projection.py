#!/usr/bin/env python3
"""Price the selected $12.138 clip against P4's recovered window supply."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json


def build_report(
    *,
    attribution: dict[str, Any],
    depth: dict[str, Any],
    target_usd: float = 12.138,
    gross_roi_bps: float = 286.06,
    active_live_cap_usd: float = 2.5,
    fee_usd_per_fill: float = 0.04441418,
) -> dict[str, Any]:
    elapsed = int(attribution.get("elapsed_windows") or 0)
    recovered_rows = [
        row for row in (attribution.get("rows") or [])
        if row.get("category") == "BOOK_OBSERVED_NO_GUARD_REASON"
    ]
    depth_row = next(
        (row for row in (depth.get("targets") or []) if abs(float(row.get("target_usd") or 0.0) - target_usd) < 1e-6),
        {},
    )
    median_impact_bps = float(((depth_row.get("vwap_impact_bps_vs_best_ask") or {}).get("p50")) or 0.0)
    impact_adjusted_gross_roi_bps = max(0.0, gross_roi_bps - median_impact_bps)
    effective_observed = sum(float(row.get("target_fillable_rate") or 0.0) for row in recovered_rows)
    projected_recovered = (len(recovered_rows) / elapsed * 288.0) if elapsed else 0.0
    projected_effective = (effective_observed / elapsed * 288.0) if elapsed else 0.0
    gross_profit_per_fill = target_usd * impact_adjusted_gross_roi_bps / 10_000.0
    projected_gross_profit = projected_effective * gross_profit_per_fill
    projected_profit = projected_effective * (gross_profit_per_fill - fee_usd_per_fill)
    active_gross_per_fill = active_live_cap_usd * impact_adjusted_gross_roi_bps / 10_000.0
    active_projected_profit = projected_effective * (active_gross_per_fill - fee_usd_per_fill)
    return {
        "schema_version": 1,
        "kind": "order150_joint_supply_size_projection",
        "flow_stage": "LIVE/MEASURE/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "target_usd": target_usd,
        "active_live_cap_usd": active_live_cap_usd,
        "target_is_live_authorized": False,
        "gross_roi_bps": gross_roi_bps,
        "median_impact_bps": median_impact_bps,
        "impact_adjusted_gross_roi_bps": round(impact_adjusted_gross_roi_bps, 6),
        "fee_usd_per_fill": fee_usd_per_fill,
        "fee_treatment": "fixed resolved-fill fee term subtracted once per projected fill",
        "roi_transfer_assumption": "counterfactual only: assumes the historical 286.06 bps gross ROI transfers unchanged to recovered windows and $12.138 clips; neither transfer is evidenced or live-authorized",
        "elapsed_windows": elapsed,
        "recovered_windows_observed": len(recovered_rows),
        "effective_fillable_window_equivalents_observed": round(effective_observed, 6),
        "projected_recovered_windows_per_288": round(projected_recovered, 6),
        "projected_effective_fillable_windows_per_288": round(projected_effective, 6),
        "projected_incremental_daily_gross_profit_after_impact_usd": round(projected_gross_profit, 6),
        "projected_incremental_daily_profit_usd": round(projected_profit, 6),
        "active_live_cap_projected_daily_profit_usd": round(active_projected_profit, 6),
        "goal_floor_usd": 100.0,
        "gap_to_goal_floor_usd": round(max(0.0, 100.0 - projected_profit), 6),
        "status": "JOINT_PROJECTION_BELOW_GOAL_FLOOR" if projected_profit < 100.0 else "JOINT_PROJECTION_REACHES_GOAL_FLOOR",
        "scope_fence": "incremental book-observed windows lacking a guard reason only; future windows, named abstains, and submitted windows receive zero recovered-supply credit",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attribution", default="data/research/order150_window_supply_attribution_latest.json")
    parser.add_argument("--depth", default="data/research/order149_depth_at_size_latest.json")
    parser.add_argument("--state-digest", default="data/research/state_digest.json")
    parser.add_argument("--output", default="data/research/order150_joint_supply_size_projection_latest.json")
    args = parser.parse_args()
    digest = load_json(args.state_digest, default={})
    wired_cap = float((((digest.get("live") or {}).get("guard_caps") or {}).get("drip_max_tranche_usd")) or 0.0)
    if wired_cap <= 0:
        raise SystemExit("state digest missing wired live drip_max_tranche_usd")
    report = build_report(
        attribution=load_json(args.attribution, default={}),
        depth=load_json(args.depth, default={}),
        active_live_cap_usd=wired_cap,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
