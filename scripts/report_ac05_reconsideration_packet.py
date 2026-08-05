#!/usr/bin/env python3
"""Build the report-only ac05 reconsideration packet."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AC05_WALLET = "0xac0586732786905d285959613f1813bc89246729"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def _wallet_rows(report: dict[str, Any], key: str, wallet: str) -> list[dict[str, Any]]:
    wallet = wallet.lower()
    return [
        row
        for row in report.get(key, [])
        if str(row.get("source_wallet") or row.get("winning_source_wallet") or "").lower() == wallet
    ]


def _resolved_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = []
    unresolved = []
    unmeasured = []
    pnl = 0.0
    wins = 0
    losses = 0
    fees = 0.0
    by_market: dict[str, dict[str, Any]] = {}
    for row in rows:
        outcome = row.get("realized_paper_outcome") or {}
        status = str(outcome.get("status") or outcome.get("resolution_status") or "").upper()
        market = str(row.get("market_slug") or "")
        fee = float(row.get("expected_fee_usd") or 0.0)
        fees += fee
        if status in {"RESOLVED", "YES", "NO"} or outcome.get("paper_pnl_usd") is not None:
            if outcome.get("paper_pnl_usd") is None:
                unmeasured.append(row)
                continue
            resolved.append(row)
            value = float(outcome.get("paper_pnl_usd") or 0.0)
            pnl += value
            if bool(outcome.get("wins")):
                wins += 1
            else:
                losses += 1
            by_market.setdefault(
                market,
                {
                    "market_slug": market,
                    "rows": 0,
                    "paper_pnl_usd": 0.0,
                    "expected_fee_usd": 0.0,
                },
            )
            by_market[market]["rows"] += 1
            by_market[market]["paper_pnl_usd"] += value
            by_market[market]["expected_fee_usd"] += fee
        else:
            unresolved.append(row)
    post_fee = pnl - fees
    return {
        "rows": len(rows),
        "measurable_resolved_intents": len(resolved),
        "unmeasured_resolved_intents": len(unmeasured),
        "unresolved_intents": len(unresolved),
        "wins": wins,
        "losses": losses,
        "pre_fee_pnl_usd": round(pnl, 6),
        "expected_fee_usd": round(fees, 6),
        "post_fee_pnl_usd": round(post_fee, 6),
        "unique_windows": len({row.get("window_start_s") for row in rows}),
        "resolved_unique_windows": len({row.get("window_start_s") for row in resolved}),
        "dominant_skip_reasons": dict(Counter(str(row.get("dominant_skip_reason") or "") for row in rows)),
        "sample_rows": rows[:5],
        "by_market": list(by_market.values())[:20],
    }


def _summary_stats(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if not summary:
        return None
    return {
        "fee_gated_intents": int(summary.get("fee_gated_intents") or 0),
        "measurable_resolved_intents": int(summary.get("measurable_resolved_intents") or 0),
        "resolved_intents": int(summary.get("resolved_intents") or 0),
        "unmeasured_resolved_intents": int(summary.get("unmeasured_resolved_intents") or 0),
        "unresolved_intents": int(summary.get("unresolved_intents") or 0),
        "wins": int(summary.get("wins") or 0),
        "losses": int(summary.get("losses") or 0),
        "pre_fee_pnl_usd": round(float(summary.get("pre_fee_pnl_usd") or 0.0), 6),
        "expected_fee_usd": round(float(summary.get("expected_fee_usd_sum") or 0.0), 6),
        "post_fee_pnl_usd": round(float(summary.get("post_fee_pnl_usd") or 0.0), 6),
        "unique_windows": int(summary.get("unique_windows") or 0),
        "resolved_unique_windows": int(summary.get("resolved_unique_windows") or 0),
        "pnl_measurement_gap_reasons": dict(summary.get("pnl_measurement_gap_reasons") or {}),
        "regime_counts": dict(summary.get("regime_counts") or {}),
    }


def build_packet(args: argparse.Namespace) -> dict[str, Any]:
    latest = _load(args.routing_shadow_latest)
    pin = _load(args.routing_shadow_attribution_pin)
    latest_rows = _wallet_rows(latest, "fee_gated_measurement_rows", AC05_WALLET)
    pin_rows = _wallet_rows(pin, "fee_gated_measurement_rows", AC05_WALLET)
    latest_stats = _resolved_stats(latest_rows)
    pin_stats = _resolved_stats(pin_rows)
    current_summary_by_member = (
        latest.get("summary", {})
        .get("extra_would_submit_post_fee_measurement", {})
        .get("by_member", {})
        .get(AC05_WALLET)
    )
    pin_summary_by_member = (
        pin.get("summary", {})
        .get("fee_gate_calibration_retained", {})
        .get("by_member", {})
        .get(AC05_WALLET)
    )

    current_summary_stats = _summary_stats(current_summary_by_member)
    pin_summary_stats = _summary_stats(pin_summary_by_member)
    current_stats = current_summary_stats or latest_stats
    pin_packet_stats = pin_summary_stats or pin_stats
    ruling_stats = current_stats
    if ruling_stats["measurable_resolved_intents"] >= 10:
        if ruling_stats["post_fee_pnl_usd"] > 0:
            recommendation = "RECONSIDER_WITH_FABLE_ONLY"
            decision_basis = "current_latest_positive_n_ge_10"
        else:
            recommendation = "SUPPRESSION_STANDS_CURRENT_LATEST_NEGATIVE"
            decision_basis = "current_latest_negative_n_ge_10"
    elif ruling_stats["resolved_intents"] or ruling_stats["measurable_resolved_intents"]:
        recommendation = "INSUFFICIENT_N_SUPPRESSION_STANDS"
        decision_basis = "current_latest_n_lt_10"
    elif pin_packet_stats["measurable_resolved_intents"] >= 10:
        if pin_packet_stats["post_fee_pnl_usd"] > 0:
            recommendation = "RECONSIDER_WITH_FABLE_ONLY"
            decision_basis = "attribution_pin_positive_n_ge_10"
        else:
            recommendation = "SUPPRESSION_STANDS_ATTRIBUTION_PIN_NEGATIVE"
            decision_basis = "attribution_pin_negative_n_ge_10"
    else:
        recommendation = "INSUFFICIENT_N_SUPPRESSION_STANDS"
        decision_basis = "n_lt_10_under_available_current_sources"

    return {
        "kind": "ac05_reconsideration_packet",
        "generated_at": _now_iso(),
        "flow_stage": "LIVE/PROMOTE/MEASURE",
        "source_wallet": AC05_WALLET,
        "source_wallet_short": "0xac05...6729",
        "report_only": True,
        "live_path_mutated": False,
        "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains sole live submitter",
        "constraints": {
            "fable_1413_e4_suppression_stands_until_fresh_fable_ruling": True,
            "resolved_post_fee_rows_only_for_recommendation": True,
            "no_config_roster_or_guard_mutation": True,
            "admit_requires_fresh_fable_ruling": True,
        },
        "inputs": {
            "routing_shadow_latest": args.routing_shadow_latest,
            "routing_shadow_latest_generated_at": latest.get("generated_at"),
            "routing_shadow_attribution_pin": args.routing_shadow_attribution_pin,
            "routing_shadow_attribution_pin_generated_at": pin.get("generated_at"),
        },
        "current_latest": {
            "stats": current_stats,
            "raw_row_stats": latest_stats,
            "summary_stats": current_summary_stats,
            "summary_by_member": current_summary_by_member,
        },
        "attribution_pin": {
            "stats": pin_packet_stats,
            "raw_row_stats": pin_stats,
            "summary_stats": pin_summary_stats,
            "summary_by_member": pin_summary_by_member,
        },
        "recommendation": recommendation,
        "decision_basis": decision_basis,
        "next_action": "ask Fable for a fresh ruling only if it wants to supersede suppression; no admission or mutation from this packet",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--routing-shadow-latest", default="data/research/routing_shadow_validation_latest.json")
    parser.add_argument(
        "--routing-shadow-attribution-pin",
        default="data/research/routing_shadow_validation_attribution_pin_latest.json",
    )
    parser.add_argument("--output", default="data/research/ac05_reconsideration_packet_latest.json")
    args = parser.parse_args()
    packet = build_packet(args)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
    print(json.dumps(packet, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
