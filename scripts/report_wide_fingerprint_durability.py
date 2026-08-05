#!/usr/bin/env python3
"""Describe the manifest-evidenced span of WIDE fingerprints."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import wide_policy_identity  # noqa: E402
from scripts.report_wide_resolved_signal_accrual import (  # noqa: E402
    DEFAULT_FRONTIER,
    DEFAULT_MANIFEST_GLOB,
    F1_RESOLVED_SIGNAL_BAR,
    _manifest_set_checksum,
)
from src.wallet_copy.models import parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_OUTPUT = "data/research/wide_fingerprint_durability_latest.json"


def _largest_gap_hours(timestamps: list[float]) -> float | None:
    ordered = sorted(set(timestamps))
    if len(ordered) < 2:
        return None
    return round(
        max(right - left for left, right in zip(ordered, ordered[1:]))
        / 3600.0,
        6,
    )


def build_report(
    *,
    frontier: dict[str, Any],
    manifest_paths: list[Path],
    expected_frontier_checksum: str,
) -> dict[str, Any]:
    frontier_checksum = str(frontier.get("frontier_checksum") or "")
    if frontier_checksum != expected_frontier_checksum:
        raise ValueError(
            f"frontier checksum changed: {frontier_checksum or 'missing'}"
        )
    candidates = frontier.get("nearest_frontier")
    if not isinstance(candidates, list):
        raise ValueError("nearest_frontier must be a list")
    frontier_rows = [
        row for row in candidates if isinstance(row, dict)
    ]
    frontier_wallets = {
        str(row.get("wallet") or "").lower() for row in frontier_rows
    }
    if not frontier_wallets:
        raise ValueError("frontier must contain candidate wallets")

    observations: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    recovered_from_keys = 0
    policy_absent_placeholder = 0
    explicit_fingerprint_observations = 0
    explicit_timestamps: list[float] = []
    identity_timestamps: list[float] = []
    synthetic_empty_wallets: dict[str, int] = defaultdict(int)
    manifest_frame: list[dict[str, Any]] = []
    for path in manifest_paths:
        manifest = load_json(path, default={})
        if not isinstance(manifest, dict):
            continue
        generated_at = str(manifest.get("generated_at") or "")
        generated_ts = parse_ts(generated_at)
        if not generated_ts:
            continue
        frame = {
            "path": str(path),
            "generated_at": generated_at,
            "manifest_kind": str(manifest.get("kind") or "UNKNOWN"),
            "frontier_wallet_rows": 0,
            "explicit_fingerprint_rows": 0,
            "recovered_from_keys": 0,
            "policy_absent_placeholder": 0,
        }
        for manifest_row in manifest.get("capture_watch_wallets") or []:
            if not isinstance(manifest_row, dict):
                continue
            wallet = str(manifest_row.get("wallet") or "").lower()
            if wallet not in frontier_wallets:
                continue
            frame["frontier_wallet_rows"] += 1
            move_slice_keys = sorted(
                {
                    str(value)
                    for value in manifest_row.get("move_slice_keys") or []
                    if str(value)
                }
            )
            fingerprint = str(
                manifest_row.get("wide_policy_fingerprint") or ""
            )
            if fingerprint:
                source = "EXPLICIT_MANIFEST_FIELD"
                explicit_fingerprint_observations += 1
                frame["explicit_fingerprint_rows"] += 1
                explicit_timestamps.append(float(generated_ts))
            elif move_slice_keys:
                fingerprint = str(
                    wide_policy_identity(
                        wallet=wallet,
                        move_slice_keys=move_slice_keys,
                    )["wide_policy_fingerprint"]
                )
                source = "RECOVERED_FROM_KEYS"
                recovered_from_keys += 1
                frame["recovered_from_keys"] += 1
            else:
                policy_absent_placeholder += 1
                frame["policy_absent_placeholder"] += 1
                synthetic_empty_wallets[wallet] += 1
                continue
            freeze = (
                manifest_row.get("slice_freeze")
                if isinstance(manifest_row.get("slice_freeze"), dict)
                else {}
            )
            f1 = freeze.get("f1") if isinstance(freeze.get("f1"), dict) else {}
            resolved = (
                int(f1["resolved"])
                if f1.get("resolved") is not None
                else None
            )
            identity_timestamps.append(float(generated_ts))
            observations[(wallet, fingerprint)].append(
                {
                    "generated_at": generated_at,
                    "generated_ts": generated_ts,
                    "resolved_signals": resolved,
                    "move_slice_keys": move_slice_keys,
                    "score_run_id": manifest.get("score_run_id"),
                    "fingerprint_source": source,
                }
            )
        manifest_frame.append(frame)

    rows: list[dict[str, Any]] = []
    for (wallet, fingerprint), fingerprint_observations in sorted(
        observations.items()
    ):
        ordered = sorted(
            fingerprint_observations,
            key=lambda row: (
                float(row["generated_ts"]),
                str(row["score_run_id"]),
            ),
        )
        key_sets = {tuple(row["move_slice_keys"]) for row in ordered}
        if len(key_sets) != 1:
            raise ValueError(
                f"{wallet}|{fingerprint} move-slice keys changed within fingerprint"
            )
        birth = ordered[0]
        last = ordered[-1]
        measured = [
            row for row in ordered if row["resolved_signals"] is not None
        ]
        first_200 = next(
            (
                row
                for row in measured
                if int(row["resolved_signals"]) >= F1_RESOLVED_SIGNAL_BAR
            ),
            None,
        )
        explicit_birth = (
            birth["fingerprint_source"] == "EXPLICIT_MANIFEST_FIELD"
        )
        birth_resolved = birth["resolved_signals"]
        pre_mature = bool(
            explicit_birth
            and birth_resolved is not None
            and int(birth_resolved) >= F1_RESOLVED_SIGNAL_BAR
        )
        observed_crossing = bool(
            explicit_birth
            and birth_resolved is not None
            and int(birth_resolved) < F1_RESOLVED_SIGNAL_BAR
            and first_200 is not None
        )
        if pre_mature:
            maturity_status = "PRE_MATURE_AT_FIRST_OBSERVATION"
        elif observed_crossing:
            maturity_status = "OBSERVED_CROSSING_FROM_BELOW"
        elif not explicit_birth:
            maturity_status = "BIRTH_IDENTITY_RECOVERED_FROM_KEYS"
        elif birth_resolved is None:
            maturity_status = "EXPLICIT_BIRTH_RESOLUTION_UNMEASURED"
        else:
            maturity_status = "EXPLICIT_BIRTH_OBSERVED_BELOW"
        evidenced_span_hours = round(
            (float(last["generated_ts"]) - float(birth["generated_ts"]))
            / 3600.0,
            6,
        )
        rows.append(
            {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "first_evidenced_at": birth["generated_at"],
                "birth_observation_source": birth["fingerprint_source"],
                "last_seen_at": last["generated_at"],
                "evidenced_span_hours": evidenced_span_hours,
                "manifest_observations": len(ordered),
                "observation_count": len(ordered),
                "measured_resolution_observation_count": len(measured),
                "recovered_from_keys_observation_count": sum(
                    row["fingerprint_source"] == "RECOVERED_FROM_KEYS"
                    for row in ordered
                ),
                "first_resolved": (
                    int(measured[0]["resolved_signals"]) if measured else None
                ),
                "last_resolved": (
                    int(measured[-1]["resolved_signals"]) if measured else None
                ),
                "max_resolved": (
                    max(int(row["resolved_signals"]) for row in measured)
                    if measured
                    else None
                ),
                "ever_reached_200": first_200 is not None,
                "hours_from_explicit_first_evidence_to_first_200_observation": (
                    round(
                        (
                            float(first_200["generated_ts"])
                            - float(birth["generated_ts"])
                        )
                        / 3600.0,
                        6,
                    )
                    if explicit_birth and first_200 is not None
                    else None
                ),
                "PRE_MATURE_AT_FIRST_OBSERVATION": pre_mature,
                "observed_crossing_from_below": observed_crossing,
                "maturity_observation_status": maturity_status,
                "move_slice_key_count": len(birth["move_slice_keys"]),
                "move_slice_keys": birth["move_slice_keys"],
            }
        )
    analysis_rows = [
        row for row in rows if not row["PRE_MATURE_AT_FIRST_OBSERVATION"]
    ]
    reached_spans = [
        float(row["evidenced_span_hours"])
        for row in rows
        if row["ever_reached_200"]
    ]
    per_wallet = {
        wallet: {
            "fingerprint_count": sum(row["wallet"] == wallet for row in rows),
            "fingerprints_reached_200": sum(
                row["wallet"] == wallet and row["ever_reached_200"]
                for row in rows
            ),
            "max_resolved_any_fingerprint": max(
                (
                    int(row["max_resolved"])
                    for row in rows
                    if row["wallet"] == wallet
                    and row["max_resolved"] is not None
                ),
                default=None,
            ),
        }
        for wallet in sorted(frontier_wallets)
    }
    return {
        "schema_version": 1,
        "kind": "wide_fingerprint_durability",
        "flow_stage": "DISCOVER/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "measurement_only": True,
        "live_orders_allowed": False,
        "admission_authority": False,
        "recommendation": None,
        "source": {
            "frontier_path": DEFAULT_FRONTIER,
            "frontier_generated_at": frontier.get("generated_at"),
            "frontier_checksum": frontier_checksum,
            "frontier_key": frontier.get("frontier_key"),
            "manifest_count": len(manifest_paths),
            "manifest_set_checksum": _manifest_set_checksum(manifest_paths),
        },
        "quality_bars_unchanged": {
            "f1_resolved_signal_bar_per_half": F1_RESOLVED_SIGNAL_BAR,
            "composite_walk_forward_min_total_resolved": (
                2 * F1_RESOLVED_SIGNAL_BAR
            ),
            "midpoint_split_rule": "symmetric_midpoint",
            "selection_rule": "frozen_positive_70pct_move_slices_v1",
        },
        "frontier_wallets": [
            {
                "wallet": str(row.get("wallet") or "").lower(),
                "current_resolved_signals": int(
                    (row.get("regime_evidence") or {}).get(
                        "resolved_signals"
                    )
                    or 0
                ),
            }
            for row in frontier_rows
        ],
        "observation_frame": {
            "manifest_rows": manifest_frame,
            "explicit_manifest_field": explicit_fingerprint_observations,
            "recovered_from_keys": recovered_from_keys,
            "policy_absent_placeholder": policy_absent_placeholder,
            "blank_recoverability": (
                "PARTIAL_RECOVERY_KEYED_ROWS_ONLY_POLICY_ABSENT_EXCLUDED"
            ),
            "recovery_method": (
                "scripts.reconcile_wide_exact_policy_paper."
                "wide_policy_identity(wallet, move_slice_keys)"
            ),
            "explicit_fingerprint_largest_gap_hours": _largest_gap_hours(
                explicit_timestamps
            ),
            "identity_observation_largest_gap_hours": _largest_gap_hours(
                identity_timestamps
            ),
            "gap_classification": (
                "SERIALIZATION_DROP_NOT_PRODUCER_OUTAGE"
            ),
            "synthetic_empty_key_identity_count": len(
                synthetic_empty_wallets
            ),
            "excluded_rows": [
                {
                    "wallet": wallet,
                    "classification": "SYNTHETIC_EMPTY_KEY_IDENTITY",
                    "observation_count": count,
                }
                for wallet, count in sorted(
                    synthetic_empty_wallets.items()
                )
            ],
        },
        "rows": rows,
        "summary": {
            "fingerprint_rows": len(rows),
            "rows_with_measured_resolution": sum(
                row["measured_resolution_observation_count"] > 0
                for row in rows
            ),
            "analysis_rows_excluding_pre_mature": len(analysis_rows),
            "pre_mature_at_first_observation_count": (
                len(rows) - len(analysis_rows)
            ),
            "fingerprints_reached_200": sum(
                row["ever_reached_200"] for row in rows
            ),
            "observed_crossings_from_below": sum(
                row["observed_crossing_from_below"] for row in rows
            ),
            "median_lifetime_hours_reached_200": (
                round(statistics.median(reached_spans), 6)
                if reached_spans
                else None
            ),
            "median_lifetime_hours_reached_200_n": len(reached_spans),
            "per_wallet": per_wallet,
        },
        "decision_rule": (
            "descriptive durability audit only; missing F1 is unmeasured, "
            "not zero; do not change the per-half 200 bar, midpoint split, "
            "fingerprints, or selection rule"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frontier", default=DEFAULT_FRONTIER)
    parser.add_argument("--manifest-glob", default=DEFAULT_MANIFEST_GLOB)
    parser.add_argument("--expected-frontier-checksum", required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    frontier = load_json(args.frontier, default={})
    report = build_report(
        frontier=frontier if isinstance(frontier, dict) else {},
        manifest_paths=sorted(ROOT.glob(args.manifest_glob)),
        expected_frontier_checksum=args.expected_frontier_checksum,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
