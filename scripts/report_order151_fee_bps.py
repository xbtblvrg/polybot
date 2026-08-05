#!/usr/bin/env python3
"""Express the resolved-fill fee evidence at ORDER151 notionals."""

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


def build_report(evidence: dict[str, Any], notionals: tuple[float, ...] = (0.5, 1.0, 12.138)) -> dict[str, Any]:
    embedded = evidence.get("embedded_fee_evidence") or {}
    effective_bps = float(embedded.get("fee_pct_of_response_cost_weighted") or 0.0) * 100.0
    rows = [{"notional_usd": value, "fee_usd_proportional": round(value * effective_bps / 10_000.0, 6), "fee_bps": round(effective_bps, 6)} for value in notionals]
    return {
        "schema_version": 1,
        "kind": "order151_fee_bps",
        "flow_stage": "MEASURE/DEFEND",
        "resolved_fill_basis": {
            "row_count": embedded.get("row_count"),
            "response_cost_usd": embedded.get("response_cost_usd"),
            "inferred_fee_usd": embedded.get("inferred_our_embedded_fee_usd"),
            "weighted_fee_bps": round(effective_bps, 6),
            "source": "data/research/wallet_copy_fee_model_proposal_2026-07-06T1924Z.json",
        },
        "notional_rows": rows,
        "proportional_model": "fee scales linearly with shares/notional at fixed price; bps is unchanged across clip sizes",
        "per_fill_effect": "no flat per-fill fee was evidenced; actual settlement rounding makes tiny-fill realized bps price/rounding dependent",
        "transfer_assumption": "The 53-fill weighted effective bps is transferred unchanged only as a disclosed resolved-fill benchmark; it is not a price-specific fee quote.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", default="data/research/wallet_copy_fee_model_proposal_2026-07-06T1924Z.json")
    parser.add_argument("--output", default="data/research/order151_fee_bps_latest.json")
    args = parser.parse_args()
    report = build_report(load_json(args.evidence, default={}))
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
