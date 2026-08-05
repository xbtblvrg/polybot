#!/usr/bin/env python3
"""Publish an exact per-wallet terminal taxonomy from the WIDE handoff journal."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.order_flow_deadman import (
    DEFAULT_WIDE_DIRECT_JOURNAL,
    DEFAULT_WIDE_DIRECT_STATE,
    _wide_direct_source_snapshot,
)
from scripts.wide_direct_handoff_journal import load_jsonl
from src.wallet_copy.store import atomic_write_json


DEFAULT_OUTPUT = "data/research/wide_wallet_terminal_breakdown_latest.json"


def _parse_now(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text()) if path.exists() else {}


def build_report(
    *,
    wallet: str,
    now: datetime,
    direct_state: dict[str, Any],
    journal: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized = wallet.strip().lower()
    snapshot = _wide_direct_source_snapshot(direct_state, now=now, journal=journal)
    row = (snapshot.get("per_wallet") or {}).get(normalized) or {}
    taxonomy = dict(row.get("terminal_taxonomy") or {})
    attempts = int(row.get("attempts") or 0)
    metadata_missing = int(taxonomy.get("REFUSED_METADATA_MISSING") or 0)
    return {
        "schema_version": 1,
        "kind": "wide_wallet_terminal_breakdown",
        "flow_stage": "DISCOVER/PROMOTE/SELF-DEV",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "measurement_cut_at": now.isoformat(),
        "wallet": normalized,
        "lookback_s": snapshot.get("lookback_s"),
        "attempts": attempts,
        "copyable": int(row.get("copyable") or 0),
        "policy_depth_pass": int(row.get("policy_depth_pass") or 0),
        "terminal_taxonomy": taxonomy,
        "terminal_taxonomy_total": sum(int(value or 0) for value in taxonomy.values()),
        "metadata_missing": metadata_missing,
        "metadata_missing_share_pct": (
            round(metadata_missing / attempts * 100.0, 6) if attempts else None
        ),
        "metadata_missing_predominant": bool(
            attempts and metadata_missing > attempts / 2.0
        ),
        "source_generation": row.get("source_generation"),
        "generation_identity": row.get("generation_identity"),
        "latest_receipt_at": row.get("latest_receipt_at"),
        "snapshot_checksum": snapshot.get("checksum"),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--now")
    parser.add_argument("--direct-state", default=DEFAULT_WIDE_DIRECT_STATE)
    parser.add_argument("--journal", default=DEFAULT_WIDE_DIRECT_JOURNAL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    report = build_report(
        wallet=args.wallet,
        now=_parse_now(args.now),
        direct_state=_load_json(Path(args.direct_state)),
        journal=load_jsonl(Path(args.journal)),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
