#!/usr/bin/env python3
"""Measure the preregistered f418 micro-size fee-leak shadow.

Flow stage: OBSERVE/LEARN. This reporter reads live fill truth as evidence,
but it is report-only and cannot change live sizing, policy, or submission.
"""

from __future__ import annotations

import argparse
import math
import statistics
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_scorecard import _default_resolutions_path  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/f418_size_clamp_fee_leak_shadow_latest.json"
MICRO_MIN_USD = 0.90
MICRO_MAX_USD = 1.10
STANDING_MIN_USD = 2.00
STANDING_MAX_USD = 2.50
TARGET_N = 40
Z_95 = 1.96


def _num(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _expected_fee_usd(order: dict[str, Any]) -> tuple[float | None, str]:
    comparison = _dict(order.get("expected_vs_realized_fee"))
    for key in ("response_expected_fee_usd", "pre_submit_expected_fee_usd"):
        if (value := _num(comparison.get(key))) is not None:
            return value, f"expected_vs_realized_fee.{key}"
    gate = _dict(order.get("expected_fee_gate"))
    if (value := _num(gate.get("expected_fee_usd"))) is not None:
        return value, "expected_fee_gate.expected_fee_usd"
    intent = _dict(order.get("source_intent"))
    metadata = _dict(intent.get("metadata"))
    intent_gate = _dict(metadata.get("expected_fee_gate"))
    if (value := _num(intent_gate.get("expected_fee_usd"))) is not None:
        return value, "source_intent.metadata.expected_fee_gate.expected_fee_usd"
    return None, "missing"


def _sample_summary(rows: list[dict[str, float]], *, target_n: int) -> dict[str, Any]:
    pnl = [row["pnl_usd"] for row in rows]
    fee_rows = [row for row in rows if row.get("expected_fee_usd") is not None]
    pnl_sum = sum(pnl)
    cost_sum = sum(row["cost_usd"] for row in rows)
    fee_sum = sum(row["expected_fee_usd"] for row in fee_rows)
    return {
        "n": len(rows),
        "target_n": target_n,
        "n_gap": max(0, target_n - len(rows)),
        "gate_crossed": len(rows) >= target_n,
        "post_fee_pnl_usd": round(pnl_sum, 6),
        "post_fee_ev_per_fill_usd": round(pnl_sum / len(rows), 6) if rows else None,
        "post_fee_roi_pct": round(100.0 * pnl_sum / cost_sum, 6) if cost_sum else None,
        "filled_cost_usd": round(cost_sum, 6),
        "expected_fee_rows": len(fee_rows),
        "expected_fee_coverage_pct": round(100.0 * len(fee_rows) / len(rows), 6) if rows else 0.0,
        "expected_fee_usd": round(fee_sum, 6),
        "expected_fee_share_of_filled_cost_pct": round(100.0 * fee_sum / cost_sum, 6) if cost_sum else None,
        "positive_fills": sum(value > 0 for value in pnl),
        "negative_fills": sum(value < 0 for value in pnl),
    }


def _difference(micro: list[dict[str, float]], standing: list[dict[str, float]]) -> dict[str, Any]:
    micro_pnl = [row["pnl_usd"] for row in micro]
    standing_pnl = [row["pnl_usd"] for row in standing]
    if len(micro_pnl) < 2 or len(standing_pnl) < 2:
        return {
            "status": "INSUFFICIENT_VARIANCE_SAMPLE",
            "micro_minus_standing_ev_usd": None,
            "standard_error_usd": None,
            "ci95_low_usd": None,
            "ci95_high_usd": None,
            "statistically_significant": False,
            "micro_ev_significantly_worse": False,
        }
    delta = statistics.mean(micro_pnl) - statistics.mean(standing_pnl)
    standard_error = math.sqrt(
        statistics.variance(micro_pnl) / len(micro_pnl)
        + statistics.variance(standing_pnl) / len(standing_pnl)
    )
    low = delta - Z_95 * standard_error
    high = delta + Z_95 * standard_error
    significant = low > 0.0 or high < 0.0
    return {
        "status": "MEASURED_WELCH_NORMAL_CI",
        "method": "difference of means; unequal-variance standard error; two-sided normal 95% CI",
        "micro_minus_standing_ev_usd": round(delta, 6),
        "standard_error_usd": round(standard_error, 6),
        "ci95_low_usd": round(low, 6),
        "ci95_high_usd": round(high, 6),
        "statistically_significant": significant,
        "micro_ev_significantly_worse": high < 0.0,
    }


def build_packet(
    *,
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    cohorts: dict[str, list[dict[str, float]]] = {"micro_1usd": [], "standing_2_to_2_5usd": []}
    fee_sources: dict[str, int] = {}
    excluded_resolved_fills = 0
    unresolved_fills = 0
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict) or str(order.get("source_wallet") or "").lower() != F418:
            continue
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        scored = score_order(order, resolutions)
        if not scored.get("resolved"):
            unresolved_fills += 1
            continue
        intended = _num(scored.get("intended_cost_usd"))
        if intended is None:
            excluded_resolved_fills += 1
            continue
        if MICRO_MIN_USD <= intended <= MICRO_MAX_USD:
            cohort = "micro_1usd"
        elif STANDING_MIN_USD <= intended <= STANDING_MAX_USD:
            cohort = "standing_2_to_2_5usd"
        else:
            excluded_resolved_fills += 1
            continue
        fee, fee_source = _expected_fee_usd(order)
        fee_sources[fee_source] = fee_sources.get(fee_source, 0) + 1
        cohorts[cohort].append(
            {
                "pnl_usd": float(scored.get("pnl_usd") or 0.0),
                "cost_usd": float(scored.get("cost_usd") or intended),
                "intended_cost_usd": intended,
                "expected_fee_usd": fee,
            }
        )

    micro = _sample_summary(cohorts["micro_1usd"], target_n=TARGET_N)
    standing = _sample_summary(cohorts["standing_2_to_2_5usd"], target_n=TARGET_N)
    difference = _difference(cohorts["micro_1usd"], cohorts["standing_2_to_2_5usd"])
    sample_gate = micro["gate_crossed"] and standing["gate_crossed"]
    fee_coverage_gate = (
        micro["expected_fee_coverage_pct"] >= 95.0
        and standing["expected_fee_coverage_pct"] >= 95.0
    )
    primary_drag = (
        sample_gate
        and fee_coverage_gate
        and difference["micro_ev_significantly_worse"]
        and (micro["expected_fee_share_of_filled_cost_pct"] or 0.0)
        > (standing["expected_fee_share_of_filled_cost_pct"] or 0.0)
    )
    if not sample_gate:
        verdict = "ACCRUE_PREREGISTERED_SAMPLE"
    elif primary_drag:
        verdict = "PASS_FEE_LEAK_PRIMARY_DRAG_READY_FOR_FABLE_HOLDOUT_RULING"
    else:
        verdict = "FAIL_NO_SIGNIFICANT_MICRO_FEE_LEAK_PRIMARY_DRAG"
    return {
        "schema_version": 1,
        "kind": "f418_size_clamp_fee_leak_shadow",
        "flow_stage": "OBSERVE/LEARN",
        "generated_at": generated_at,
        "status": verdict,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "single_submitter_unchanged": True,
        "preregistration": {
            "experiment_id": "copy-f418-size-clamp-fee-leak-ev-shadow",
            "micro_cohort": f"resolved f418 fills with intended cost in [{MICRO_MIN_USD},{MICRO_MAX_USD}]",
            "standing_cohort": f"resolved f418 fills with intended cost in [{STANDING_MIN_USD},{STANDING_MAX_USD}] (standing 2.5 drip tranche under $8 window budget)",
            "target_n_each": TARGET_N,
            "success": "micro post-fee EV is significantly worse at 95% CI and expected fee share of filled cost is higher",
        },
        "accounting_basis": {
            "post_fee_pnl": "canonical pnl_truth response-filled-cost PnL; embedded fee is already in CLOB cash cost and is not subtracted twice",
            "fee_ratio": "response expected embedded fee / canonical response filled cost",
        },
        "micro_1usd": micro,
        "standing_2_to_2_5usd": standing,
        "comparison": {
            **difference,
            "sample_gate_crossed": sample_gate,
            "fee_coverage_gate_crossed": fee_coverage_gate,
            "primary_drag_gate_crossed": primary_drag,
            "expected_fee_share_delta_pp": round(
                (micro["expected_fee_share_of_filled_cost_pct"] or 0.0)
                - (standing["expected_fee_share_of_filled_cost_pct"] or 0.0),
                6,
            ),
        },
        "audit": {
            "unresolved_f418_fills": unresolved_fills,
            "excluded_resolved_f418_fills_outside_fixed_cohorts": excluded_resolved_fills,
            "expected_fee_sources": dict(sorted(fee_sources.items())),
        },
        "decision": "REPORT_ONLY_NO_LIVE_SIZE_OR_POLICY_CHANGE",
        "next": (
            "ask Fable for the preregistered separate holdout ruling"
            if sample_gate
            else "accrue fixed cohorts until both reach n>=40"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    resolutions_path = args.resolutions or str(ROOT / _default_resolutions_path())
    packet = build_packet(
        ledger=load_json(Path(args.ledger), default={}),
        resolutions=load_resolutions(resolutions_path),
        generated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    atomic_write_json(Path(args.output), packet)
    print(Path(args.output))


if __name__ == "__main__":
    main()
