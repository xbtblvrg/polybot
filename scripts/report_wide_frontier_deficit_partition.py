#!/usr/bin/env python3
"""Partition a frozen WIDE candidate frontier by its measured deficit sets."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

EXPECTED_SOURCE_CHECKSUM = (
    "ac2469053339adf336e677271c45261c42c3e8ba60efcb31a6eba41cc954e8f7"
)


def build_report(
    frontier: dict[str, Any],
    *,
    expected_source_checksum: str = EXPECTED_SOURCE_CHECKSUM,
    expected_frontier_checksum: str,
) -> dict[str, Any]:
    source_checksum = str(frontier.get("source_checksum") or "")
    if source_checksum != expected_source_checksum:
        raise ValueError(
            f"frontier source checksum changed: {source_checksum or 'missing'}"
        )
    frontier_checksum = str(frontier.get("frontier_checksum") or "")
    if frontier_checksum != expected_frontier_checksum:
        raise ValueError(
            f"frontier checksum changed: {frontier_checksum or 'missing'}"
        )
    candidates = frontier.get("nearest_frontier")
    if not isinstance(candidates, list):
        raise ValueError("nearest_frontier must be a list")
    if int(frontier.get("candidate_count") or 0) != len(candidates):
        raise ValueError("candidate_count does not reconcile to nearest_frontier")

    rows: list[dict[str, Any]] = []
    check_names = sorted(str(key) for key in (frontier.get("refusal_counts") or {}))
    if not check_names:
        raise ValueError("refusal_counts must define the check universe")
    not_passed_prevalence: Counter[str] = Counter()
    explicit_false_prevalence: Counter[str] = Counter()
    not_evaluated_prevalence: Counter[str] = Counter()
    not_evaluated_rows: dict[str, list[dict[str, Any]]] = {
        check: [] for check in check_names
    }
    exactly_one_flip_counts: Counter[str] = Counter()
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            raise ValueError(f"candidate row {index} is not an object")
        checks = candidate.get("checks")
        if not isinstance(checks, dict):
            raise ValueError(f"candidate row {index} lacks checks")
        explicit_false = sorted(
            check for check in check_names if checks.get(check) is False
        )
        not_evaluated = sorted(check for check in check_names if check not in checks)
        not_passed = sorted(set(explicit_false) | set(not_evaluated))
        published_deficits = sorted(
            str(value) for value in candidate.get("evidence_deficits") or []
        )
        if explicit_false != published_deficits:
            raise ValueError(
                f"candidate row {index} explicit-false/evidence-deficit reconciliation failed"
            )
        explicit_false_prevalence.update(explicit_false)
        not_evaluated_prevalence.update(not_evaluated)
        not_passed_prevalence.update(not_passed)
        row_identity = {
            "row_index": index,
            "wallet": candidate.get("wallet"),
            "paper_policy_id": candidate.get("paper_policy_id"),
            "wide_policy_fingerprint": candidate.get("wide_policy_fingerprint"),
        }
        for check in not_evaluated:
            not_evaluated_rows[check].append(row_identity)
        if len(not_passed) == 1:
            exactly_one_flip_counts[not_passed[0]] += 1
        partition = (
            "zero_deficit"
            if not not_passed
            else "exactly_one_deficit"
            if len(not_passed) == 1
            else "multi_deficit"
        )
        rows.append(
            {
                **row_identity,
                "eligible": bool(candidate.get("eligible")),
                "explicit_false_set": explicit_false,
                "not_evaluated_set": not_evaluated,
                "not_passed_set": not_passed,
                "not_passed_count": len(not_passed),
                "partition": partition,
            }
        )

    published_refusals = {
        str(key): int(value)
        for key, value in (frontier.get("refusal_counts") or {}).items()
    }
    recomputed_not_passed = {
        check: int(not_passed_prevalence.get(check) or 0) for check in check_names
    }
    if published_refusals != recomputed_not_passed:
        raise ValueError("source refusal counts do not equal row-level not-passed counts")
    partition_counts = Counter(row["partition"] for row in rows)
    flip_ranking = [
        {
            "check": check,
            "rows_flipped_if_check_alone_cleared": count,
        }
        for check, count in sorted(
            exactly_one_flip_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]
    return {
        "schema_version": 1,
        "kind": "wide_frontier_deficit_partition",
        "flow_stage": "DISCOVER/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "measurement_only": True,
        "live_orders_allowed": False,
        "admission_authority": False,
        "recommendation": None,
        "source": {
            "path": "data/research/wide_direct_admissible_frontier_latest.json",
            "generated_at": frontier.get("generated_at"),
            "source_checksum": source_checksum,
            "frontier_checksum": frontier_checksum,
            "frontier_key": frontier.get("frontier_key"),
            "candidate_count": len(rows),
            "eligible_count": int(frontier.get("eligible_count") or 0),
        },
        "quality_bars_unchanged": frontier.get("quality_bars") or {},
        "rows": rows,
        "partition": {
            "zero_deficit_rows": int(partition_counts["zero_deficit"]),
            "exactly_one_deficit_rows": int(
                partition_counts["exactly_one_deficit"]
            ),
            "multi_deficit_rows": int(partition_counts["multi_deficit"]),
            "row_count_reconciles": sum(partition_counts.values()) == len(rows),
        },
        "check_outcomes": {
            check: {
                "explicit_false": int(explicit_false_prevalence.get(check) or 0),
                "not_evaluated": int(not_evaluated_prevalence.get(check) or 0),
                "not_passed": int(not_passed_prevalence.get(check) or 0),
                "not_evaluated_rows": not_evaluated_rows[check],
            }
            for check in check_names
        },
        "not_passed_prevalence": recomputed_not_passed,
        "source_refusal_counts": published_refusals,
        "single_check_flip_ranking": flip_ranking,
        "decision_rule": (
            "descriptive only: a single check flips a row iff that row's "
            "not-passed set contains exactly that one check; not-passed is "
            "explicit false plus not evaluated"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--frontier",
        default="data/research/wide_direct_admissible_frontier_latest.json",
    )
    parser.add_argument(
        "--expected-source-checksum",
        default=EXPECTED_SOURCE_CHECKSUM,
    )
    parser.add_argument("--expected-frontier-checksum", required=True)
    parser.add_argument(
        "--output",
        default="data/research/wide_frontier_deficit_partition_latest.json",
    )
    args = parser.parse_args()
    report = build_report(
        load_json(args.frontier, default={}),
        expected_source_checksum=args.expected_source_checksum,
        expected_frontier_checksum=args.expected_frontier_checksum,
    )
    atomic_write_json(args.output, report)
    print(
        json.dumps(
            {
                **report["partition"],
                "single_check_flip_ranking": report[
                    "single_check_flip_ranking"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
