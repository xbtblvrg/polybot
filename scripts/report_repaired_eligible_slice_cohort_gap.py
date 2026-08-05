#!/usr/bin/env python3
"""Report cohort bindings for repaired eligible alpha slices, without mutation."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def build_report(
    *,
    alpha: dict[str, Any],
    cohort_manifest: dict[str, Any],
    temporal: dict[str, Any],
    frontier_checksum: str,
    source_packet_sha256: str,
) -> dict[str, Any]:
    profiles = (
        (alpha.get("execution_profiles") or {}).get("profiles_by_wallet") or {}
    )
    cohort_wallets = {
        _wallet(row.get("wallet"))
        for key in ("capture_watch_wallets", "admitted_wallets")
        for row in (cohort_manifest.get(key) or [])
        if isinstance(row, dict) and _wallet(row.get("wallet"))
    }
    temporal_rows = {
        _wallet(row.get("wallet")): row
        for row in (temporal.get("rows") or [])
        if isinstance(row, dict) and _wallet(row.get("wallet"))
    }
    eligible_outside: dict[str, list[dict[str, Any]]] = {}
    for wallet_value, profile in profiles.items():
        wallet = _wallet(wallet_value)
        if not wallet or wallet in cohort_wallets or not isinstance(profile, dict):
            continue
        slices = [
            row
            for row in (profile.get("move_slices") or [])
            if isinstance(row, dict) and row.get("eligible") is True
        ]
        if slices:
            eligible_outside[wallet] = slices

    rows: list[dict[str, Any]] = []
    for wallet, slices in sorted(eligible_outside.items()):
        temporal_row = temporal_rows.get(wallet)
        temporal_label = str(
            (temporal_row or {}).get("classification")
            or (temporal_row or {}).get("label")
            or ""
        ).upper()
        terminal = "PROVEN-NEGATIVE" in temporal_label or "TERMINAL" in temporal_label
        exclusions = [
            ("not_in_cohort_manifest", not terminal),
            ("no_direct_source_generation", not terminal),
        ]
        if temporal_row is None:
            exclusions.append(("no_temporal_row", True))
        for exclusion, repairable in exclusions:
            rows.append(
                {
                    "key": f"{wallet}|{exclusion}|{str(repairable).lower()}",
                    "wallet": wallet,
                    "binding_exclusion": exclusion,
                    "repairable_by_next_wide_manifest": repairable,
                    "terminal": not repairable,
                    "eligible_slices": [
                        {
                            "move_slice_key": row.get("move_slice_key"),
                            "fill_sample": row.get("fill_sample"),
                            "copyable_rate_pct": row.get("copyable_rate_pct"),
                            "mean_edge": row.get("mean_edge"),
                        }
                        for row in slices
                    ],
                    "frontier_checksum": frontier_checksum,
                }
            )
    return {
        "schema_version": 1,
        "kind": "repaired_eligible_slice_cohort_gap",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "source_packet_sha256": source_packet_sha256,
        "frontier_checksum": frontier_checksum,
        "cohort_manifest_id": cohort_manifest.get("manifest_id"),
        "rows": rows[:10],
        "summary": {
            "eligible_outside_cohort_wallets": len(eligible_outside),
            "binding_rows": min(10, len(rows)),
            "repairable_rows": sum(row["repairable_by_next_wide_manifest"] for row in rows[:10]),
            "terminal_rows": sum(row["terminal"] for row in rows[:10]),
            "all_rows_emitted": len(rows) <= 10,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--alpha",
        default="data/research/alpha_decay_report_wide_order100_repaired_20260731T1110Z.json",
    )
    parser.add_argument(
        "--supervisor",
        default="data/research/wide_prospective_supervisor_state.json",
    )
    parser.add_argument(
        "--temporal",
        default="data/research/wallet_temporal_profitability_latest.json",
    )
    parser.add_argument(
        "--deadman",
        default="data/research/order_flow_deadman_state.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/repaired_eligible_slice_cohort_gap_latest.json",
    )
    args = parser.parse_args()
    alpha_raw = Path(args.alpha).read_bytes()
    supervisor = load_json(args.supervisor, default={}) or {}
    manifest_path = str(supervisor.get("manifest") or "")
    if not manifest_path:
        raise SystemExit("supervisor manifest is required")
    deadman = load_json(args.deadman, default={}) or {}
    candidate = (
        (((deadman.get("policy_choke") or {}).get("source_roster_drought") or {}).get("candidate_evidence"))
        or {}
    )
    report = build_report(
        alpha=json.loads(alpha_raw),
        cohort_manifest=load_json(manifest_path, default={}) or {},
        temporal=load_json(args.temporal, default={}) or {},
        frontier_checksum=str(candidate.get("frontier_checksum") or ""),
        source_packet_sha256=hashlib.sha256(alpha_raw).hexdigest(),
    )
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, **report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
