#!/usr/bin/env python3
"""Identity-safe merge of a bounded source-active pass into its cohort rollup."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.probe_cohort_source_active_liveness import _cohort_wallets  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def repair_source_active_cumulative(cohort: dict[str, Any]) -> dict[str, Any]:
    reports = [row for row in cohort.get("reports") or [] if isinstance(row, dict)]
    source_active_pass = sum(row.get("source_active_tally_status") == "PASS" for row in reports)
    policy_eligible_pass = sum(row.get("policy_eligible_tally_status") == "PASS" for row in reports)
    return {
        **cohort,
        "wallet_count": len(reports),
        "source_active_pass": source_active_pass,
        "policy_eligible_pass": policy_eligible_pass,
        "source_active_cumulative": {
            "reports": len(reports),
            "source_active_pass": source_active_pass,
            "policy_eligible_pass": policy_eligible_pass,
        },
    }


def merge_pass(
    *,
    replay: dict[str, Any],
    batch: dict[str, Any],
    cohort: dict[str, Any],
    pass_number: int,
    expected_offset: int,
    previous_last_wallet: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    wallets = _cohort_wallets(replay)
    anchor = str(previous_last_wallet or "").lower()
    if wallets.count(anchor) != 1:
        raise ValueError(f"identity anchor must appear exactly once; wallet={anchor} count={wallets.count(anchor)}")
    actual_offset = wallets.index(anchor) + 1
    batch_offset = int(batch.get("wallet_offset") or 0)
    reports = [row for row in batch.get("reports") or [] if isinstance(row, dict)]
    batch_wallets = [str(row.get("wallet") or "").lower() for row in reports]
    expected_wallets = wallets[actual_offset : actual_offset + len(reports)]
    if batch_offset != actual_offset or batch_wallets != expected_wallets:
        raise ValueError("batch identities do not match the identity-rebased replay slice")
    prior_reports = [row for row in cohort.get("reports") or [] if isinstance(row, dict)]
    prior_wallets = {str(row.get("wallet") or "").lower() for row in prior_reports}
    duplicates = sorted(prior_wallets & set(batch_wallets))
    if duplicates or len(batch_wallets) != len(set(batch_wallets)):
        raise ValueError(f"already-processed identities detected: {duplicates[:5]}")
    last_wallet = batch_wallets[-1] if batch_wallets else anchor
    identity_rebase = {
        "acceptance_gate": "PASS_ZERO_ALREADY_PROCESSED_IDENTITIES",
        "expected_wallet_offset": int(expected_offset),
        "actual_wallet_offset": actual_offset,
        "previous_last_processed_wallet": anchor,
        "last_processed_wallet": last_wallet,
        "already_processed_identity_count": 0,
        "rule": "locate previous last-processed wallet identity in current replay; start at index+1; reject any duplicate identities",
    }
    updated_batch = {
        **batch,
        "identity_rebase": identity_rebase,
        "last_processed_wallet": last_wallet,
    }
    merged_reports = sorted([*prior_reports, *reports], key=lambda row: str(row.get("wallet") or ""))
    updated_cohort = repair_source_active_cumulative({
        **cohort,
        "batch_id": batch.get("batch_id"),
        "cohort_live_ready_wallets": batch.get("cohort_live_ready_wallets"),
        "cohort_rollup_generated_at": batch.get("generated_at"),
        "generated_at": batch.get("generated_at"),
        "identity_rebase": identity_rebase,
        "last_processed_wallet": last_wallet,
        "wallet_offset": actual_offset,
        "wallet_count": len(merged_reports),
        "source_active_pass": int(cohort.get("source_active_pass") or 0) + int(batch.get("source_active_pass") or 0),
        "policy_eligible_pass": int(cohort.get("policy_eligible_pass") or 0) + int(batch.get("policy_eligible_pass") or 0),
        "total_source_active_windows": int(cohort.get("total_source_active_windows") or 0) + int(batch.get("total_source_active_windows") or 0),
        "total_policy_eligible_windows": int(cohort.get("total_policy_eligible_windows") or 0) + int(batch.get("total_policy_eligible_windows") or 0),
        "reports": merged_reports,
        "latest_source_active_pass_window": {
            "pass": int(pass_number),
            "generated_at": batch.get("generated_at"),
            "wallet_offset": actual_offset,
            "wallets": len(reports),
            "source_active_pass": batch.get("source_active_pass"),
            "policy_eligible_pass": batch.get("policy_eligible_pass"),
            "last_processed_wallet": last_wallet,
            "identity_rebase_acceptance_gate": identity_rebase["acceptance_gate"],
            "note": "source-active cohort replay pass; external DataAPI liveness counters intentionally unchanged",
        },
        "next_action": (
            f"rebase from last_processed_wallet identity in current replay before pass{int(pass_number) + 1}; "
            f"expected offset={actual_offset + len(reports)}; require zero duplicate identities"
        ),
    })
    return updated_batch, updated_cohort


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", default="data/research/source_active_liveness_replay_ordering_latest.json")
    parser.add_argument("--batch", default="data/research/source_active_liveness_batch_latest.json")
    parser.add_argument("--cohort", default="data/research/source_active_liveness_cohort_latest.json")
    parser.add_argument("--pass-number", type=int)
    parser.add_argument("--expected-offset", type=int)
    parser.add_argument("--previous-last-wallet")
    parser.add_argument("--repair-cumulative-only", action="store_true")
    args = parser.parse_args()
    batch_path = Path(args.batch)
    cohort_path = Path(args.cohort)
    if args.repair_cumulative_only:
        updated_cohort = repair_source_active_cumulative(load_json(cohort_path, default={}))
        atomic_write_json(cohort_path, updated_cohort)
        print(json.dumps({"status": "PASS_REPAIRED_CUMULATIVE", "source_active_cumulative": updated_cohort["source_active_cumulative"]}, sort_keys=True))
        return 0
    if args.pass_number is None or args.expected_offset is None or not args.previous_last_wallet:
        parser.error("--pass-number, --expected-offset, and --previous-last-wallet are required for merge")
    updated_batch, updated_cohort = merge_pass(
        replay=load_json(args.replay, default={}),
        batch=load_json(batch_path, default={}),
        cohort=load_json(cohort_path, default={}),
        pass_number=int(args.pass_number),
        expected_offset=int(args.expected_offset),
        previous_last_wallet=str(args.previous_last_wallet),
    )
    atomic_write_json(batch_path, updated_batch)
    atomic_write_json(cohort_path, updated_cohort)
    print(json.dumps({"status": "PASS", "pass": args.pass_number, "identity_rebase": updated_batch["identity_rebase"], "wallet_count": updated_cohort["wallet_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
