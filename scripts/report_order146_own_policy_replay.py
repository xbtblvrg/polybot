#!/usr/bin/env python3
"""Audit whether ORDER146 own-policy F2 replay is possible from captured rows."""

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

from src.wallet_copy.store import atomic_write_json, load_json


REPLAY_REQUIRED_FIELDS = {
    "alpha_profile": ("market_slug", "source_event_ts", "source_price"),
    "size_bounds": ("source_price", "source_shares"),
    "freshness": ("source_received_at_s", "book_fetch_started_at_s"),
    "depth": ("book_snapshot",),
}


def _packets(path: Path) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return latest
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            generation = str(row.get("source_generation") or "")
            if generation:
                latest[generation] = row
    return latest


def build_report(*, deadman: dict[str, Any], packets: dict[str, dict[str, Any]]) -> dict[str, Any]:
    frontier = (
        (((deadman.get("policy_choke") or {}).get("actuator") or {}).get("candidate_evidence") or {}).get("nearest_frontier")
        or []
    )[:20]
    wallet_rows: list[dict[str, Any]] = []
    global_missing: set[str] = set()
    for candidate in frontier:
        wallet = str(candidate.get("wallet") or "").lower()
        direct = candidate.get("direct_source") or {}
        generation = str(candidate.get("source_generation") or "")
        packet = packets.get(generation) or {}
        captured = [
            row
            for row in packet.get("rows") or []
            if str(row.get("wallet") or "").lower() == wallet
        ]
        missing_by_conjunct = {
            conjunct: sorted(
                field
                for field in fields
                if not captured or any(row.get(field) is None for row in captured)
            )
            for conjunct, fields in REPLAY_REQUIRED_FIELDS.items()
        }
        missing_by_conjunct = {
            key: value for key, value in missing_by_conjunct.items() if value
        }
        global_missing.update(
            field for values in missing_by_conjunct.values() for field in values
        )
        taxonomy = Counter(
            str((row.get("f1_f4_terminal") or {}).get("terminal") or "UNKNOWN")
            for row in captured
        )
        attempts_by_window = [int(value or 0) for value in direct.get("attempts_by_continuity_window") or []]
        copyables_by_window = [int(value or 0) for value in direct.get("copyables_by_continuity_window") or []]
        wallet_rows.append(
            {
                "wallet": wallet,
                "wide_policy_fingerprint": candidate.get("wide_policy_fingerprint"),
                "base_policy_id": candidate.get("f2_copyable_policy_id"),
                "source_generation": generation or None,
                "requested_recorded_attempts": sum(attempts_by_window),
                "captured_current_generation_rows": len(captured),
                "base_observed_copyables_by_continuity_window": copyables_by_window,
                "base_observed_copyables": sum(copyables_by_window),
                "base_current_generation_terminal_taxonomy": dict(sorted(taxonomy.items())),
                "own_policy_replay_copyables": None,
                "missing_fields_by_conjunct": missing_by_conjunct,
                "replay_status": "MISSING_FIELDS_STOP" if missing_by_conjunct else "FIELDS_PRESENT_NOT_SCORED",
            }
        )
    return {
        "schema_version": 1,
        "kind": "order146_own_policy_copyable_replay",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": "ORDER146_MISSING_FIELDS_STOP" if global_missing else "ORDER146_FIELDS_PRESENT",
        "pre_registered_branch": "E3''''''" if global_missing else None,
        "frontier_candidates": len(wallet_rows),
        "unique_frontier_wallets": len({row["wallet"] for row in wallet_rows}),
        "missing_field_list": sorted(global_missing),
        "rule": "never synthesize fields required to re-evaluate alpha profile, size bounds, freshness, or executable depth",
        "wallets": wallet_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--journal", default="data/research/wide_direct_handoff_journal.jsonl")
    parser.add_argument("--output", default="data/research/order146_own_policy_replay_latest.json")
    args = parser.parse_args()
    report = build_report(
        deadman=load_json(args.deadman, default={}),
        packets=_packets(Path(args.journal)),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
