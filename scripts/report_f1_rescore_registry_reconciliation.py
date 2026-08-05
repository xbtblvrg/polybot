#!/usr/bin/env python3
"""Report F1 full-stream rescore coverage against same-regime registry evidence."""

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


DEFAULT_DEADMAN = Path("data/research/order_flow_deadman_state.json")
DEFAULT_TEMPORAL = Path("data/research/wallet_temporal_profitability_latest.json")
DEFAULT_OUTPUT = Path(
    "data/research/f1_rescore_registry_reconciliation_latest.json"
)


def _wallet(value: Any) -> str:
    return str(value or "").strip().lower()


def _temporal_by_wallet(temporal: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = temporal.get("wallets") or []
    if isinstance(rows, dict):
        return {
            _wallet(wallet): row
            for wallet, row in rows.items()
            if isinstance(row, dict)
        }
    return {
        _wallet(row.get("wallet") or row.get("source_wallet")): row
        for row in rows
        if isinstance(row, dict)
    }


def build_packet(
    deadman: dict[str, Any],
    temporal: dict[str, Any],
    *,
    generated_at: str,
) -> dict[str, Any]:
    policy_choke = (
        deadman.get("policy_choke")
        if isinstance(deadman.get("policy_choke"), dict)
        else {}
    )
    drought = (
        policy_choke.get("source_roster_drought")
        if isinstance(policy_choke.get("source_roster_drought"), dict)
        else {}
    )
    evidence = (
        drought.get("candidate_evidence")
        if isinstance(drought.get("candidate_evidence"), dict)
        else {}
    )
    regime = str(evidence.get("regime") or "")
    temporal_rows = _temporal_by_wallet(temporal)
    rows: list[dict[str, Any]] = []
    for candidate in evidence.get("rows") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = _wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        regime_evidence = (
            candidate.get("regime_evidence")
            if isinstance(candidate.get("regime_evidence"), dict)
            else {}
        )
        policy = (
            candidate.get("policy")
            if isinstance(candidate.get("policy"), dict)
            else {}
        )
        registry = temporal_rows.get(wallet) or {}
        slice_labels = (
            registry.get("slice_labels")
            if isinstance(registry.get("slice_labels"), dict)
            else {}
        )
        registry_slice = (
            slice_labels.get(regime)
            if isinstance(slice_labels.get(regime), dict)
            else {}
        )
        rescore_n = int(regime_evidence.get("resolved_signals") or 0)
        registry_n = (
            int(registry_slice.get("resolved_trades") or 0)
            if registry_slice
            else None
        )
        rows.append(
            {
                "wallet": wallet,
                "regime": regime,
                "wide_policy_fingerprint": candidate.get(
                    "wide_policy_fingerprint"
                ),
                "paper_policy_id": candidate.get("paper_policy_id"),
                "move_slice_keys": list(policy.get("move_slice_keys") or []),
                "move_slice_key_count": len(policy.get("move_slice_keys") or []),
                "rescore_resolved_signals": rescore_n,
                "registry_same_regime_resolved_trades": registry_n,
                "rescore_to_registry_ratio_pct": (
                    round(100.0 * rescore_n / registry_n, 6)
                    if registry_n is not None and registry_n > 0
                    else None
                ),
                "rescore_clears_f1_200_floor": rescore_n >= 200,
                "registry_slice_label": registry_slice.get("label"),
                "registry_slice_pnl_usd": registry_slice.get("pnl_usd"),
                "registry_slice_roi_pct": registry_slice.get("roi_pct"),
            }
        )
    rows.sort(
        key=lambda row: (
            row["wallet"],
            str(row.get("wide_policy_fingerprint") or ""),
        )
    )
    nonparked_floor_passes = [
        row
        for row in rows
        if row["rescore_clears_f1_200_floor"]
        and row["wallet"]
        != "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    ]
    return {
        "schema_version": 1,
        "kind": "f1_rescore_registry_reconciliation",
        "flow_stage": "PROMOTE/SELF-DEV",
        "generated_at": generated_at,
        "regime": regime,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "f1_floor_unchanged": 200,
        "candidate_count": len(rows),
        "rows": rows,
        "summary": {
            "rescore_floor_pass_count": sum(
                row["rescore_clears_f1_200_floor"] for row in rows
            ),
            "non_permanently_parked_rescore_floor_pass_count": len(
                nonparked_floor_passes
            ),
            "minimum_ratio_pct": min(
                (
                    row["rescore_to_registry_ratio_pct"]
                    for row in rows
                    if row["rescore_to_registry_ratio_pct"] is not None
                ),
                default=None,
            ),
            "maximum_ratio_pct": max(
                (
                    row["rescore_to_registry_ratio_pct"]
                    for row in rows
                    if row["rescore_to_registry_ratio_pct"] is not None
                ),
                default=None,
            ),
        },
        "decision": (
            "WIDEN_FINGERPRINT_PAPER_ONLY_KEEP_F1_FLOOR_200"
            if not nonparked_floor_passes
            else "REPORT_ONLY_REVIEW_NONPARKED_FLOOR_PASSES"
        ),
        "rule": (
            "compare venue-executable full-stream rescore n with the canonical "
            "temporal registry n for the same wallet and regime; never lower "
            "the 200 floor from this reporting artifact"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default=str(DEFAULT_DEADMAN))
    parser.add_argument("--temporal", default=str(DEFAULT_TEMPORAL))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    packet = build_packet(
        json.loads(Path(args.deadman).read_text(encoding="utf-8")),
        json.loads(Path(args.temporal).read_text(encoding="utf-8")),
        generated_at=datetime.now(UTC).isoformat(),
    )
    atomic_write_json(Path(args.output), packet)
    print(Path(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
