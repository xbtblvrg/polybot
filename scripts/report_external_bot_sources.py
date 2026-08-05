#!/usr/bin/env python3
"""Report reviewed external bot sources for wallet-copy development."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.external_sources import load_external_bot_sources, summarize_external_bot_sources
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default="configs/wallet_copy/external_bot_sources.json")
    parser.add_argument("--output", default="data/research/external_bot_source_review_state.json")
    parser.add_argument("--print-full", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = load_external_bot_sources(args.sources)
    summary = summarize_external_bot_sources(payload)
    state = {
        "schema_version": 1,
        "kind": "external_bot_source_review_state",
        "generated_at": utc_now_iso(),
        "sources_path": args.sources,
        "summary": summary,
    }
    atomic_write_json(args.output, state)
    if args.print_full:
        printed = state
    else:
        printed = {
            "output": args.output,
            "source_count": summary.get("source_count"),
            "priority_counts": summary.get("priority_counts"),
            "status_counts": summary.get("status_counts"),
            "validation_issues": summary.get("validation_issues"),
            "missing_local_capabilities": [
                {
                    "source_id": row.get("source_id"),
                    "capability": row.get("capability"),
                    "status": row.get("status"),
                    "backlog": row.get("backlog"),
                }
                for row in summary.get("missing_local_capabilities") or []
            ],
            "actionable": [
                {
                    "id": row.get("id"),
                    "priority": row.get("priority"),
                    "primary_use": row.get("primary_use"),
                }
                for row in summary.get("actionable_sources") or []
            ],
            "blocked": summary.get("blocked_sources"),
        }
    print(json.dumps(printed, indent=2, sort_keys=True, default=str))
    return 1 if summary.get("validation_issues") else 0


if __name__ == "__main__":
    raise SystemExit(main())
