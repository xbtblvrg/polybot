#!/usr/bin/env python3
"""Evaluate the preregistered market-scope experiment against the WIDE frontier."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

CHECKS = (
    "f1_walk_forward_admissible",
    "f1_concentration_admissible",
    "both_resolved_halves_positive",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--intake",
        default="data/research/wallet_market_scan_intake_scoped_experiment.json",
    )
    parser.add_argument(
        "--deadman",
        default="data/research/order_flow_deadman_state.json",
    )
    parser.add_argument("--expected-frontier-checksum", required=True)
    parser.add_argument(
        "--output",
        default="data/research/scoped_market_frontier_flip_experiment_latest.json",
    )
    return parser.parse_args()


def _candidate_evidence(deadman: dict[str, Any]) -> dict[str, Any]:
    policy_choke = deadman.get("policy_choke") if isinstance(deadman.get("policy_choke"), dict) else {}
    drought = (
        policy_choke.get("source_roster_drought")
        if isinstance(policy_choke.get("source_roster_drought"), dict)
        else {}
    )
    evidence = drought.get("candidate_evidence")
    return evidence if isinstance(evidence, dict) else {}


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    intake = load_json(args.intake, default={})
    deadman = load_json(args.deadman, default={})
    intake = intake if isinstance(intake, dict) else {}
    deadman = deadman if isinstance(deadman, dict) else {}
    evidence = _candidate_evidence(deadman)
    checksum = str(evidence.get("frontier_checksum") or "")
    if checksum != str(args.expected_frontier_checksum):
        raise ValueError(f"frontier checksum changed: {checksum or 'missing'}")
    rows = [row for row in evidence.get("rows") or [] if isinstance(row, dict)]
    if len(rows) != int(evidence.get("candidate_count") or 0):
        raise ValueError("frontier candidate count does not reconcile")
    source = intake.get("source") if isinstance(intake.get("source"), dict) else {}
    window = intake.get("window") if isinstance(intake.get("window"), dict) else {}
    summary = intake.get("summary") if isinstance(intake.get("summary"), dict) else {}
    ranked = [row for row in intake.get("ranked_wallets") or [] if isinstance(row, dict)]
    scoped_wallets = {
        str(row.get("wallet") or row.get("source_wallet") or "").lower()
        for row in ranked
        if str(row.get("wallet") or row.get("source_wallet") or "")
    }
    frontier_wallets = {str(row.get("wallet") or "").lower() for row in rows}
    baseline = {
        "|".join(
            (
                str(row.get("wallet") or "").lower(),
                str(row.get("wide_policy_fingerprint") or ""),
                str(row.get("source_generation") or ""),
            )
        ): {name: bool((row.get("checks") or {}).get(name)) for name in CHECKS}
        for row in rows
    }
    # Raw Data API rows have no fingerprint identity or CLOB rest-book evidence,
    # so they cannot become authoritative F1 rows. The unchanged checks are the
    # conservative rescore result; admitting them would loosen an evidence gate.
    flips: list[dict[str, Any]] = []
    flip_count = 0
    verdict = "COPY_WIDE_MEASUREMENT_CAPPED_MOVE_TO_LIVE_FILL_REALISM"
    return {
        "schema_version": 1,
        "kind": "scoped_market_frontier_flip_experiment",
        "flow_stage": "DISCOVER/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "preregistration_authority": "Fable DIRECTION 2026-07-31T02:32:00Z",
        "frontier_checksum": checksum,
        "frontier_candidate_count": len(rows),
        "checks_compared": list(CHECKS),
        "market_scope": {
            "enabled": source.get("market_scoped"),
            "condition_ids_requested": source.get("market_scope_condition_ids_requested"),
            "condition_ids_completed": source.get("market_scope_condition_ids_completed"),
            "lookback_complete": window.get("lookback_complete"),
            "cutoff_iso": window.get("cutoff_iso"),
            "run_start_iso": window.get("now_iso"),
            "oldest_trade_iso_seen": window.get("oldest_trade_iso_seen"),
            "newest_trade_iso_seen": window.get("newest_trade_iso_seen"),
            "trades_scanned": summary.get("trades_scanned"),
            "crypto5m_trades_matched": summary.get("crypto5m_trades_matched"),
            "wallets_ranked": summary.get("wallets_ranked"),
            "frontier_wallets_observed": len(frontier_wallets & scoped_wallets),
        },
        "baseline_checks": baseline,
        "authoritative_rescore_status": "NO_NEW_GATE_AUTHORITY_ELIGIBLE_ROWS",
        "authority_reason": (
            "market-scoped Data API trades lack fingerprint identity and CLOB rest-book "
            "snapshots required by venue_executable_full_stream_rescore"
        ),
        "flips": flips,
        "flip_count": flip_count,
        "acceptance": {
            "continue_copy_wide_discovery_if_flip_count_gte": 1,
            "passed": flip_count >= 1,
        },
        "verdict": verdict,
        "next_action": (
            "stop copy-WIDE discovery cadence and move budget permanently to live-lane "
            "fill realism; do not loosen F1-F4"
        ),
    }


def main() -> int:
    args = parse_args()
    payload = build_report(args)
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
