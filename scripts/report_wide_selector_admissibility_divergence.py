#!/usr/bin/env python3
"""Measure frozen-selector choices against currently admissible WIDE cells."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import wide_policy_identity  # noqa: E402
from scripts.report_wide_resolved_signal_accrual import (  # noqa: E402
    DEFAULT_MANIFEST_GLOB,
    _manifest_set_checksum,
)
from src.wallet_copy.models import num, parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_EVIDENCE = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_OUTPUT = (
    "data/research/wide_selector_admissibility_divergence_latest.json"
)
SELECTION_RULE = "frozen_positive_70pct_move_slices_v1"
PER_HALF_RESOLVED_BAR = 200
COMPOSITE_MIN_RESOLVED = 400


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _admissible(cell: dict[str, Any]) -> bool:
    summary = (
        cell.get("venue_executable_full_stream_rescore")
        if isinstance(
            cell.get("venue_executable_full_stream_rescore"), dict
        )
        else {}
    )
    return bool(
        summary.get("f1_pass") is True
        and summary.get("f1_walk_forward_admissible") is True
        and summary.get("concentration_admissible") is True
        and num(summary.get("venue_reachable_share_pct")) >= 40.0
    )


def build_report(
    *,
    evidence: dict[str, Any],
    evidence_checksum: str,
    manifest_paths: list[Path],
) -> dict[str, Any]:
    cells = evidence.get("cells")
    if not isinstance(cells, list):
        raise ValueError("fingerprint evidence cells must be a list")
    admissible_by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    walk_forward_cells = 0
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        wallet = str(identity.get("wallet") or "").lower()
        summary = (
            cell.get("venue_executable_full_stream_rescore")
            if isinstance(
                cell.get("venue_executable_full_stream_rescore"), dict
            )
            else {}
        )
        if summary.get("f1_walk_forward_admissible") is True:
            walk_forward_cells += 1
        if not wallet or not _admissible(cell):
            continue
        admissible_by_wallet[wallet].append(
            {
                "wide_policy_fingerprint": cell.get(
                    "wide_policy_fingerprint"
                ),
                "move_slice_key_count": len(identity.get("move_slice_keys") or []),
                "move_slice_keys": sorted(identity.get("move_slice_keys") or []),
                "resolved_signals": int(summary.get("resolved") or 0),
                "first_half_resolved": int(
                    (summary.get("first_half") or {}).get("resolved") or 0
                ),
                "second_half_resolved": int(
                    (summary.get("second_half") or {}).get("resolved") or 0
                ),
                "post_fee_pnl_usd": summary.get("post_fee_pnl_usd"),
                "roi_pct": summary.get("roi_pct"),
                "venue_reachable_share_pct": summary.get(
                    "venue_reachable_share_pct"
                ),
            }
        )

    selection_observations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in manifest_paths:
        manifest = load_json(path, default={})
        if not isinstance(manifest, dict):
            continue
        generated_at = str(manifest.get("generated_at") or "")
        generated_ts = parse_ts(generated_at)
        if not generated_ts:
            continue
        for row in manifest.get("capture_watch_wallets") or []:
            if not isinstance(row, dict):
                continue
            wallet = str(row.get("wallet") or "").lower()
            if wallet not in admissible_by_wallet:
                continue
            move_slice_keys = sorted(
                {
                    str(value)
                    for value in row.get("move_slice_keys") or []
                    if str(value)
                }
            )
            fingerprint = str(row.get("wide_policy_fingerprint") or "")
            if not fingerprint:
                if not move_slice_keys:
                    continue
                fingerprint = str(
                    wide_policy_identity(
                        wallet=wallet,
                        move_slice_keys=move_slice_keys,
                    )["wide_policy_fingerprint"]
                )
            selection_observations[wallet].append(
                {
                    "generated_at": generated_at,
                    "generated_ts": generated_ts,
                    "manifest_id": manifest.get("manifest_id"),
                    "score_run_id": manifest.get("score_run_id"),
                    "selected_fingerprint": fingerprint,
                    "selected_move_slice_key_count": len(move_slice_keys),
                }
            )

    wallet_rows: list[dict[str, Any]] = []
    for wallet, admissible_cells in sorted(admissible_by_wallet.items()):
        admissible_fingerprints = {
            str(cell["wide_policy_fingerprint"]) for cell in admissible_cells
        }
        observations = sorted(
            selection_observations.get(wallet, []),
            key=lambda row: (
                float(row["generated_ts"]),
                str(row["score_run_id"]),
            ),
        )
        by_fingerprint: dict[str, dict[str, Any]] = {}
        for observation in observations:
            fingerprint = str(observation["selected_fingerprint"])
            bucket = by_fingerprint.setdefault(
                fingerprint,
                {
                    "selected_fingerprint": fingerprint,
                    "selected_move_slice_key_count": observation[
                        "selected_move_slice_key_count"
                    ],
                    "first_selected_at": observation["generated_at"],
                    "last_selected_at": observation["generated_at"],
                    "manifest_selection_count": 0,
                    "currently_admissible": (
                        fingerprint in admissible_fingerprints
                    ),
                },
            )
            bucket["last_selected_at"] = observation["generated_at"]
            bucket["manifest_selection_count"] += 1
        matched = sum(
            str(row["selected_fingerprint"]) in admissible_fingerprints
            for row in observations
        )
        latest = observations[-1] if observations else {}
        wallet_rows.append(
            {
                "wallet": wallet,
                "admissible_cells": sorted(
                    admissible_cells,
                    key=lambda row: str(row["wide_policy_fingerprint"]),
                ),
                "admissible_cell_count": len(admissible_cells),
                "manifest_selection_observations": len(observations),
                "manifest_selections_of_currently_admissible_cell": matched,
                "manifest_selections_of_other_cell": (
                    len(observations) - matched
                ),
                "historical_match_rate_pct": (
                    round(100.0 * matched / len(observations), 6)
                    if observations
                    else None
                ),
                "latest_selected_fingerprint": latest.get(
                    "selected_fingerprint"
                ),
                "latest_selected_move_slice_key_count": latest.get(
                    "selected_move_slice_key_count"
                ),
                "latest_selected_is_currently_admissible": (
                    latest.get("selected_fingerprint")
                    in admissible_fingerprints
                    if latest
                    else None
                ),
                "selection_history_by_fingerprint": sorted(
                    by_fingerprint.values(),
                    key=lambda row: (
                        str(row["first_selected_at"]),
                        str(row["selected_fingerprint"]),
                    ),
                ),
            }
        )
    return {
        "schema_version": 1,
        "kind": "wide_selector_admissibility_divergence",
        "flow_stage": "DISCOVER/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "measurement_only": True,
        "live_orders_allowed": False,
        "admission_authority": False,
        "recommendation": None,
        "source": {
            "evidence_path": DEFAULT_EVIDENCE,
            "evidence_generated_at": evidence.get("generated_at"),
            "evidence_checksum": evidence_checksum,
            "evidence_cell_count": len(cells),
            "manifest_count": len(manifest_paths),
            "manifest_set_checksum": _manifest_set_checksum(manifest_paths),
        },
        "quality_bars_unchanged": {
            "selection_rule": SELECTION_RULE,
            "f1_resolved_signal_bar_per_half": PER_HALF_RESOLVED_BAR,
            "composite_walk_forward_min_total_resolved": (
                COMPOSITE_MIN_RESOLVED
            ),
            "midpoint_split_rule": "first=(n+1)//2; second=n-first",
            "venue_reachable_share_min_pct": 40.0,
        },
        "predicate": (
            "f1_pass AND f1_walk_forward_admissible AND "
            "concentration_admissible AND venue_reachable_share_pct >= 40.0"
        ),
        "admissibility_time_basis": (
            "current_full_stream_rescore_applied_to_historical_selection_choices"
        ),
        "wallets": wallet_rows,
        "summary": {
            "walk_forward_admissible_cells_before_other_legs": (
                walk_forward_cells
            ),
            "simultaneously_admissible_cells": sum(
                row["admissible_cell_count"] for row in wallet_rows
            ),
            "wallets_with_simultaneously_admissible_cell": len(wallet_rows),
            "latest_selection_matches_admissible_cell": sum(
                row["latest_selected_is_currently_admissible"] is True
                for row in wallet_rows
            ),
            "latest_selection_misses_admissible_cell": sum(
                row["latest_selected_is_currently_admissible"] is False
                for row in wallet_rows
            ),
            "historical_manifest_selection_observations": sum(
                row["manifest_selection_observations"] for row in wallet_rows
            ),
            "historical_selections_of_currently_admissible_cell": sum(
                row[
                    "manifest_selections_of_currently_admissible_cell"
                ]
                for row in wallet_rows
            ),
        },
        "decision_rule": (
            "read-only chronic-versus-one-cut selector divergence; no "
            "selection-rule, threshold, fingerprint, or live mutation"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", default=DEFAULT_EVIDENCE)
    parser.add_argument("--expected-evidence-checksum", required=True)
    parser.add_argument("--manifest-glob", default=DEFAULT_MANIFEST_GLOB)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    evidence_path = Path(args.evidence)
    checksum = _sha256(evidence_path)
    if checksum != args.expected_evidence_checksum:
        raise ValueError(f"evidence checksum changed: {checksum}")
    evidence = load_json(evidence_path, default={})
    report = build_report(
        evidence=evidence if isinstance(evidence, dict) else {},
        evidence_checksum=checksum,
        manifest_paths=sorted(ROOT.glob(args.manifest_glob)),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
