#!/usr/bin/env python3
"""Publish all-venue versus venue-executable temporal label divergences."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402

DEFAULT_INPUT = ROOT / "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_OUTPUT = ROOT / "data/research/temporal_slice_label_divergence_latest.json"


def build_report(registry: dict[str, Any], *, slice_name: str, generated_at: str) -> dict[str, Any]:
    wallets = registry.get("wallets") or []
    if isinstance(wallets, dict):
        wallets = [{**row, "wallet": row.get("wallet") or wallet} for wallet, row in wallets.items() if isinstance(row, dict)]
    rows = []
    for wallet in wallets:
        if not isinstance(wallet, dict):
            continue
        all_labels = wallet.get("slice_labels") if isinstance(wallet.get("slice_labels"), dict) else {}
        venue_labels = wallet.get("venue_slice_labels") if isinstance(wallet.get("venue_slice_labels"), dict) else {}
        all_row = all_labels.get(slice_name) if isinstance(all_labels.get(slice_name), dict) else {}
        venue_row = venue_labels.get(slice_name) if isinstance(venue_labels.get(slice_name), dict) else {}
        all_label = str(all_row.get("label") or "MISSING").upper()
        venue_label = str(venue_row.get("label") or "MISSING").upper()
        if all_label == venue_label:
            continue
        rows.append({
            "wallet": wallet.get("wallet") or wallet.get("source_wallet"),
            "slice": slice_name,
            "all_venue": {key: all_row.get(key) for key in ("label", "resolved_trades", "roi_pct", "pnl_usd", "reason")},
            "venue_executable": {key: venue_row.get(key) for key in ("label", "resolved_trades", "roi_pct", "pnl_usd", "reason")},
            "transition": f"{all_label}_TO_{venue_label}",
        })
    positive_to_negative = sum(1 for row in rows if row["transition"] == "PROVEN-POSITIVE_TO_PROVEN-NEGATIVE")
    negative_to_positive = sum(1 for row in rows if row["transition"] == "PROVEN-NEGATIVE_TO_PROVEN-POSITIVE")
    return {
        "schema_version": 1,
        "kind": "temporal_slice_label_divergence",
        "flow_stage": "LIVE/DEFEND/MEASURE/SELF-DEV",
        "generated_at": generated_at,
        "registry_generated_at": registry.get("generated_at"),
        "slice": slice_name,
        "authority": "venue_slice_labels preferred; slice_labels fallback",
        "summary": {
            "wallets_total": len(wallets),
            "divergent_wallets": len(rows),
            "all_venue_positive_to_venue_negative": positive_to_negative,
            "all_venue_negative_to_venue_positive": negative_to_positive,
            "other_divergence": len(rows) - positive_to_negative - negative_to_positive,
        },
        "rows": rows,
        "status": "PASS_DIVERGENCE_DISCLOSED",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--slice", default="weekday")
    args = parser.parse_args()
    report = build_report(json.loads(Path(args.input).read_text()), slice_name=args.slice, generated_at=datetime.now(UTC).isoformat())
    atomic_write_json(Path(args.output), report)
    print(json.dumps({"status": report["status"], "summary": report["summary"]}, sort_keys=True))


if __name__ == "__main__":
    main()
