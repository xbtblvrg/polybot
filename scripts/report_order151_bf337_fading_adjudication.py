#!/usr/bin/env python3
"""Adjudicate ORDER151 R1 from the temporal classifier's persisted basis."""

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

WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"


def build_report(*, temporal: dict[str, Any], deadman: dict[str, Any], wallet: str = WALLET) -> dict[str, Any]:
    row = next((r for r in temporal.get("wallets") or [] if str(r.get("wallet") or "").lower() == wallet), {})
    criteria = temporal.get("criteria") or {}
    all_profile, recent = row.get("all") or {}, row.get("recent") or {}
    min_trades = int(criteria.get("min_trades") or 5)
    fading_min_resolved = int(criteria.get("fading_min_resolved_trades") or 200)
    fading_max_gap_sigma = float(criteria.get("fading_max_gap_in_sigma") or -1.0)
    all_positive = int(all_profile.get("resolved_trades") or 0) >= min_trades and float(all_profile.get("roi_pct") or 0) > 0
    recent_negative = (
        all_positive
        and int(recent.get("resolved_trades") or 0) >= fading_min_resolved
        and float(recent.get("roi_pct") or 0) <= 0
        and float(recent.get("gap_in_sigma") or 0) <= fading_max_gap_sigma
    )
    frontier = (((deadman.get("policy_choke") or {}).get("actuator") or {}).get("candidate_evidence") or {}).get("nearest_frontier") or []
    fingerprints = []
    for candidate in frontier:
        if str(candidate.get("wallet") or "").lower() != wallet:
            continue
        evidence = candidate.get("regime_evidence") or {}
        fingerprints.append({
            "wide_policy_fingerprint": candidate.get("wide_policy_fingerprint"),
            "basis": evidence.get("source"),
            "first_half_post_fee_pnl_usd": evidence.get("first_half_post_fee_pnl_usd"),
            "second_half_post_fee_pnl_usd": evidence.get("second_half_post_fee_pnl_usd"),
            "resolved_signals": evidence.get("resolved_signals"),
        })
    return {
        "schema_version": 1,
        "kind": "order151_bf337_fading_adjudication",
        "flow_stage": "ROTATE/LIVE/MEASURE",
        "wallet": wallet,
        "classifier_source": "data/research/wallet_temporal_profitability_latest.json",
        "classifier_generated_at": temporal.get("generated_at"),
        "classifier_algorithm": {
            "recent_sample": "last min(50, max(5, len(all_trades)//4)) resolved trades",
            "all_positive_comparator": f"all.resolved_trades >= {min_trades} and all.roi_pct > 0",
            "recent_decay_comparator": f"all_positive and recent.resolved_trades >= {fading_min_resolved} and recent.roi_pct <= 0 and recent.gap_in_sigma <= {fading_max_gap_sigma}",
            "decay_roi_threshold_pct": 0.0,
            "first_vs_second_half_comparator": "NOT_IMPLEMENTED_BY_CLASSIFIER",
        },
        "classifier_own_basis": {
            "historical_profile": all_profile,
            "recent_profile": recent,
            "historical_window_bounds": [all_profile.get("first_event_ts"), all_profile.get("latest_event_ts")],
            "recent_window_bounds": [recent.get("first_event_ts"), recent.get("latest_event_ts")],
            "all_positive": all_positive,
            "recent_negative": recent_negative,
            "classification": row.get("classification"),
            "classification_reason": row.get("classification_reason"),
        },
        "fingerprint_half_evidence": fingerprints,
        "basis_reconciliation": "Fingerprint halves are full-stream venue-executable rescoring, not the classifier's last-50-trade basis; they cannot falsify the persisted classifier result.",
        "fading_clear": not recent_negative,
        "rotation_authorized": not recent_negative,
        "verdict": "GENUINE_DECAY_REFUSE_ROTATION" if recent_negative else "INSUFFICIENT_POWER_FOR_FADING_ROTATION_AUTHORIZED",
        "live_mutation_performed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--temporal", default="data/research/wallet_temporal_profitability_latest.json")
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--output", default="data/research/order151_bf337_fading_adjudication_latest.json")
    args = parser.parse_args()
    report = build_report(temporal=load_json(args.temporal, default={}), deadman=load_json(args.deadman, default={}))
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
