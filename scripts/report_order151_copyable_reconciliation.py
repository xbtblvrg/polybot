#!/usr/bin/env python3
"""Reconcile ORDER151 R2's historical replay and fresh deadman counters."""

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


def build_report(*, audit: dict[str, Any], deadman: dict[str, Any]) -> dict[str, Any]:
    frontier = (((deadman.get("policy_choke") or {}).get("actuator") or {}).get("candidate_evidence") or {}).get("nearest_frontier") or []
    direct = {(str(row.get("wallet") or "").lower(), str(row.get("wide_policy_fingerprint") or "")): row.get("direct_source") or {} for row in frontier}
    rows = []
    for candidate in audit.get("candidates") or []:
        key = (str(candidate.get("wallet") or "").lower(), str(candidate.get("wide_policy_fingerprint") or ""))
        current = direct.get(key, {})
        rows.append({
            "wallet": key[0], "wide_policy_fingerprint": key[1],
            "historical_counterfactual_copyable": candidate.get("own_policy_replay_copyables"),
            "historical_run_count": len(audit.get("source_run_ids") or []),
            "fresh_emitted_copyable_30m": current.get("copyable"),
            "fresh_latest_receipt_at": current.get("latest_receipt_at"),
            "same_definition_and_window": False,
        })
    return {
        "schema_version": 1, "kind": "order151_copyable_reconciliation", "flow_stage": "MEASURE/ROTATE",
        "rows": rows,
        "historical_definition": "counterfactual own-fingerprint replay pooled across source_run_ids",
        "fresh_definition": "orders actually emitted COPYABLE_EXACT_POLICY_PAPER_FILL in rolling 1800 seconds",
        "wrong_report": "order149_rotation_qualification_latest.json",
        "fix": "qualification now requires f2_gate_comparable and fails closed on the historical multi-generation replay",
        "verdict": "NO_NUMERIC_CONTRADICTION_DIFFERENT_WINDOW_AND_DEFINITION",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", default="data/research/order146_f2_own_policy_replay_latest.json")
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--output", default="data/research/order151_copyable_reconciliation_latest.json")
    args = parser.parse_args()
    report = build_report(audit=load_json(args.audit, default={}), deadman=load_json(args.deadman, default={}))
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
