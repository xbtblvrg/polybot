#!/usr/bin/env python3
"""Recover only immutable token identity for ORDER149 metadata residuals."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.merge_rtds_wallet_events import _gamma_token_metadata
from scripts.report_order146_f2_own_policy_replay_audit import load_envelopes
from src.wallet_copy.store import atomic_write_json, load_json


def candidate_starts(*, unresolved: set[str], envelopes: list[dict[str, Any]]) -> set[int]:
    starts: set[int] = set()
    for envelope in envelopes:
        for row in envelope.get("rows") or []:
            if str(row.get("token_id") or "") not in unresolved:
                continue
            raw_ts = row.get("source_event_ts")
            if raw_ts is None and row.get("recorded_at"):
                try:
                    raw_ts = datetime.fromisoformat(str(row["recorded_at"]).replace("Z", "+00:00")).timestamp()
                except ValueError:
                    raw_ts = None
            if raw_ts is None:
                continue
            base = int(float(raw_ts)) // 300 * 300
            starts.update(base + offset for offset in (-900, -600, -300, 0, 300))
    return starts


def build_report(*, unresolved: set[str], recovered: dict[str, dict[str, str]], starts: set[int]) -> dict[str, Any]:
    fenced = {
        token_id: {key: str((recovered.get(token_id) or {}).get(key) or "") for key in ("condition_id", "market_slug", "outcome")}
        for token_id in sorted(unresolved)
        if all((recovered.get(token_id) or {}).get(key) for key in ("condition_id", "market_slug", "outcome"))
    }
    residual = sorted(unresolved - set(fenced))
    return {
        "schema_version": 1,
        "kind": "order149_gamma_metadata_recovery",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": "RECOVERED_ALL" if not residual else "RECOVERED_PARTIAL_RESIDUAL_PUBLISHED" if fenced else "GAMMA_UNREACHABLE_OR_TOKEN_UNKNOWN_RESIDUAL_PUBLISHED",
        "requested_token_ids": len(unresolved),
        "queried_btc5m_starts": sorted(starts),
        "recovered_count": len(fenced),
        "residual_count": len(residual),
        "recovered": fenced,
        "residual_token_ids": residual,
        "field_fence": ["condition_id", "market_slug", "outcome"],
        "rule": "Gamma identity lookup only; never recover price, size, book, depth, spread, or timestamp",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backfill", default="data/research/order149_token_metadata_backfill_latest.json")
    parser.add_argument("--journal", default="data/research/wide_direct_handoff_journal.jsonl")
    parser.add_argument("--gamma-base-url", default="https://gamma-api.polymarket.com")
    parser.add_argument("--output", default="data/research/order149_gamma_metadata_recovery_latest.json")
    args = parser.parse_args()
    unresolved = set(load_json(args.backfill, default={}).get("unresolved_token_ids") or [])
    envelopes = load_envelopes(Path(args.journal))
    starts = candidate_starts(unresolved=unresolved, envelopes=envelopes)
    recovered = _gamma_token_metadata(args.gamma_base_url, starts=starts, timeout_s=3.0)
    report = build_report(unresolved=unresolved, recovered=recovered, starts=starts)
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
