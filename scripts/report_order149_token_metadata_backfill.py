#!/usr/bin/env python3
"""Join ORDER149 shortlist rows to immutable token metadata."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_order146_f2_own_policy_replay_audit import load_envelopes
from src.wallet_copy.store import atomic_write_json, load_json

FIELDS = ("condition_id", "market_slug", "outcome")


def build_report(*, qualification: dict[str, Any], envelopes: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[str, Any]:
    rows_by_wallet: dict[str, dict[str, dict[str, Any]]] = {}
    for envelope in envelopes:
        for row in envelope.get("rows") or []:
            wallet = str(row.get("wallet") or "").lower()
            attempt = str(row.get("attempt_id") or row.get("row_identity") or "")
            if wallet and attempt:
                rows_by_wallet.setdefault(wallet, {})[attempt] = row
    identities = []
    aggregate_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate in qualification.get("candidates") or []:
        wallet = str(candidate.get("wallet") or "").lower()
        rows = list((rows_by_wallet.get(wallet) or {}).values())
        unresolved: set[str] = set()
        resolved = 0
        token_present = 0
        for row in rows:
            token_id = str(row.get("token_id") or "")
            if token_id:
                token_present += 1
            meta = metadata.get(token_id) if token_id else None
            if isinstance(meta, dict) and all(meta.get(field) for field in FIELDS):
                resolved += 1
            elif token_id:
                unresolved.add(token_id)
            aggregate_rows[(wallet, str(row.get("attempt_id") or row.get("row_identity") or ""))] = row
        identities.append({
            "wallet": wallet,
            "wide_policy_fingerprint": candidate.get("wide_policy_fingerprint"),
            "rows": len(rows),
            "token_id_present": token_present,
            "metadata_resolved": resolved,
            "metadata_unresolved": len(rows) - resolved,
            "unresolved_token_ids": sorted(unresolved),
        })
    unique_rows = list(aggregate_rows.values())
    resolved_unique = 0
    unresolved_tokens: set[str] = set()
    for row in unique_rows:
        token_id = str(row.get("token_id") or "")
        meta = metadata.get(token_id) if token_id else None
        if isinstance(meta, dict) and all(meta.get(field) for field in FIELDS):
            resolved_unique += 1
        elif token_id:
            unresolved_tokens.add(token_id)
    total = len(unique_rows)
    branch = "E1¹⁰" if total and resolved_unique == total else "E2¹⁰" if resolved_unique else "E3¹⁰"
    return {
        "schema_version": 1,
        "kind": "order149_token_metadata_backfill",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": {"E1¹⁰": "ALL_METADATA_RESOLVED", "E2¹⁰": "PARTIAL_METADATA_RESOLVED", "E3¹⁰": "METADATA_JOIN_STRUCTURAL_FAILURE"}[branch],
        "pre_registered_branch": branch,
        "unique_rows": total,
        "token_id_present": sum(bool(row.get("token_id")) for row in unique_rows),
        "metadata_resolved": resolved_unique,
        "metadata_unresolved": total - resolved_unique,
        "metadata_resolved_pct": round(100.0 * resolved_unique / total, 6) if total else None,
        "unresolved_token_ids": sorted(unresolved_tokens),
        "identities": identities,
        "field_fence": list(FIELDS),
        "forbidden_fields": ["price", "size", "book", "depth", "spread", "timestamp"],
        "rule": "immutable token identity join only; no time-varying field synthesis",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification", default="data/research/order149_rotation_qualification_latest.json")
    parser.add_argument("--journal", default="data/research/wide_direct_handoff_journal.jsonl")
    parser.add_argument("--metadata", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--output", default="data/research/order149_token_metadata_backfill_latest.json")
    args = parser.parse_args()
    report = build_report(qualification=load_json(args.qualification, default={}), envelopes=load_envelopes(Path(args.journal)), metadata=load_json(args.metadata, default={}))
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
