#!/usr/bin/env python3
"""Resolve ORDER149 R2 own-policy replay qualification without synthesis."""

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


SHORTLIST_WALLETS = {
    "0x4c9497941333332d29f1c235dd23200f3623ffad",
    "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
}


def build_report(*, replay_audit: dict[str, Any]) -> dict[str, Any]:
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate in replay_audit.get("candidates") or []:
        wallet = str(candidate.get("wallet") or "").lower()
        if wallet not in SHORTLIST_WALLETS:
            continue
        fingerprint = str(candidate.get("wide_policy_fingerprint") or "")
        key = (wallet, fingerprint)
        row = by_identity.setdefault(
            key,
            {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint or None,
                "joined_raw_attempts": int(candidate.get("joined_raw_attempts") or 0),
                "own_policy_replay_copyables": candidate.get("own_policy_replay_copyables"),
                "missing_required_fields": {},
                "still_owes_f3": True,
            },
        )
        row["joined_raw_attempts"] = max(
            int(row["joined_raw_attempts"]),
            int(candidate.get("joined_raw_attempts") or 0),
        )
        if candidate.get("own_policy_replay_copyables") is not None:
            row["own_policy_replay_copyables"] = int(candidate["own_policy_replay_copyables"])
        for field, count in (candidate.get("missing_required_fields") or {}).items():
            row["missing_required_fields"][str(field)] = max(
                int(row["missing_required_fields"].get(str(field)) or 0),
                int(count or 0),
            )

    candidates = sorted(
        by_identity.values(),
        key=lambda row: (row["wallet"], str(row["wide_policy_fingerprint"])),
    )
    missing_fields = sorted(
        {
            field
            for row in candidates
            for field, count in row["missing_required_fields"].items()
            if int(count or 0) > 0
        }
    )
    positive = [
        row for row in candidates if int(row.get("own_policy_replay_copyables") or 0) > 0
    ]
    unresolved = [
        row for row in candidates if row.get("own_policy_replay_copyables") is None
    ]
    comparable = replay_audit.get("f2_gate_comparable") is not False
    if positive and comparable:
        branch = "E2⁹"
        status = "E2_OWN_POLICY_COPYABLE_CANDIDATES_FOUND"
    elif not comparable:
        branch = "E3⁹"
        status = "E3_SCOPE_MISMATCH_NOT_A_FRESH_F2_VERDICT"
    elif unresolved or missing_fields:
        branch = "E3⁹"
        status = (
            "E3_OWN_POLICY_REPLAY_JOIN_RATE_FAIL"
            if replay_audit.get("status") == "JOIN_RATE_VERDICT_FAIL"
            else "E3_OWN_POLICY_REPLAY_INPUTS_NOT_PERSISTED_STOP"
        )
    else:
        branch = "E1⁹"
        status = "E1_F2_REFUSALS_SURVIVE_OWN_POLICY_REPLAY"

    return {
        "schema_version": 1,
        "kind": "order149_rotation_qualification",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": status,
        "pre_registered_branch": branch,
        "source_audit_status": replay_audit.get("status"),
        "f2_gate_comparable": comparable,
        "raw_join_coverage": replay_audit.get("raw_join_coverage"),
        "stopped_before_policy_replay": bool(
            replay_audit.get("stopped_before_policy_replay")
        ),
        "synthesis_permitted": False,
        "shortlist_wallet_count": len({row["wallet"] for row in candidates}),
        "shortlist_candidate_count": len(candidates),
        "positive_copyable_candidates": positive,
        "exact_blocking_fields": missing_fields,
        "candidates": candidates,
        "excluded_by_direction": {
            "0x00033f1089ff061813850e5135483bed39ce3b49": "ORDER149 stays dropped",
            "0xbf337426aa856996b8bb79b238345dd1a0276bf7": "ORDER149 bf33 refusal stands",
            "0x13e0d447520ebe7f8eeaf7817211201b2c585204": "terminal volume-lane park",
            "0x82c857cb4d18e919c1b7d3c6865be4debe50da77": "terminal seat park",
        },
        "next_action": (
            "repair the exact measured metadata/book join deficits, then replay each "
            "candidate's exact evidenced policy; do not synthesize inputs"
            if branch == "E3⁹"
            else "apply unchanged F1/F3 scoring to positive own-policy copyable candidates"
            if branch == "E2⁹"
            else "publish that the qualified replacement pool is genuinely empty"
        ),
        "rule": (
            "ORDER149 R2 only; no F1-bar change, admission, seat rotation, overlay, "
            "guard mutation, reload, restart, size change, or threshold change"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--replay-audit",
        default="data/research/order146_f2_own_policy_replay_latest.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/order149_rotation_qualification_latest.json",
    )
    args = parser.parse_args()
    report = build_report(replay_audit=load_json(args.replay_audit, default={}))
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
