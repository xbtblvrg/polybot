#!/usr/bin/env python3
"""Measure candidate venue reachability under maker and taker geometry."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json
from src.wallet_copy.venue_executability import VENUE_REACHABLE_SHARE_MIN_PCT

DEFAULT_DEADMAN = "data/research/order_flow_deadman_state.json"
DEFAULT_EVIDENCE = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_OUTPUT = "data/research/order134_e6_order_type_venue_reachability_latest.json"
MATERIAL_FLIP_SHARE_MIN_PCT = 5.0


def _checksum(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _candidate_rows(deadman: dict[str, Any]) -> list[dict[str, Any]]:
    choke = deadman.get("policy_choke") or {}
    drought = choke.get("source_roster_drought") or {}
    evidence = drought.get("candidate_evidence") or {}
    return [row for row in evidence.get("rows") or [] if isinstance(row, dict)]


def _taker_reachability(maker: dict[str, Any]) -> tuple[float | None, bool]:
    """Reclassify only the maker-share-ceiling rejects as taker reachable."""

    counts = maker.get("venue_discard_reason_counts_resolved") or {}
    total = sum(int(value or 0) for value in counts.values())
    if total <= 0:
        return None, False
    reachable = int(counts.get("executable") or 0) + int(
        counts.get("price_above_venue_minimum_max_price") or 0
    )
    share = round(100.0 * reachable / total, 6)
    return share, share >= VENUE_REACHABLE_SHARE_MIN_PCT


def build_report(
    *, deadman: dict[str, Any], evidence: dict[str, Any]
) -> dict[str, Any]:
    candidates = _candidate_rows(deadman)
    cells = {
        str(cell.get("wide_policy_fingerprint") or ""): cell
        for cell in evidence.get("cells") or []
        if isinstance(cell, dict)
    }
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        fingerprint = str(
            candidate.get("wide_policy_fingerprint")
            or (candidate.get("policy") or {}).get("wide_policy_fingerprint")
            or ""
        )
        cell = cells.get(fingerprint)
        maker = None
        if isinstance(cell, dict):
            maker = cell.get("maker_venue_executable_full_stream_rescore")
            if not isinstance(maker, dict):
                # Pre-E6 artifacts carried maker geometry under the canonical
                # venue field.  Keep the measurement reproducible across the
                # gate migration without treating a taker artifact as maker.
                legacy = cell.get("venue_executable_full_stream_rescore")
                if (
                    isinstance(legacy, dict)
                    and legacy.get("venue_order_type") in (None, "maker")
                ):
                    maker = legacy
        exact_evidence = isinstance(maker, dict)
        maker_reachable = bool(
            exact_evidence
            and maker.get("f1_venue_reachable_admissible") is True
        )
        taker_share, taker_reachable = (
            _taker_reachability(maker) if exact_evidence else (None, False)
        )
        checks = dict(candidate.get("checks") or {})
        checks["f1_venue_reachable_admissible"] = taker_reachable
        taker_eligible = bool(checks and all(value is True for value in checks.values()))
        rows.append(
            {
                "wallet": candidate.get("wallet"),
                "wide_policy_fingerprint": fingerprint,
                "exact_fingerprint_evidence": exact_evidence,
                "venue_reachable_maker": maker_reachable,
                "venue_reachable_taker": taker_reachable,
                "maker_reachable_share_pct": (
                    maker.get("venue_reachable_share_pct")
                    if exact_evidence
                    else None
                ),
                "taker_reachable_share_pct": taker_share,
                "maker_to_taker_flip": bool(
                    not maker_reachable and taker_reachable
                ),
                "eligible_before": candidate.get("eligible") is True,
                "eligible_after_taker_reachability_only": taker_eligible,
                "paper_only": True,
                "promotion_authority": False,
            }
        )
    candidate_count = len(rows)
    flip_count = sum(row["maker_to_taker_flip"] for row in rows)
    material_min_count = max(
        1, math.ceil(candidate_count * MATERIAL_FLIP_SHARE_MIN_PCT / 100.0)
    )
    eligible_before = sum(row["eligible_before"] for row in rows)
    eligible_after = sum(
        row["eligible_after_taker_reachability_only"] for row in rows
    )
    return {
        "schema_version": 1,
        "kind": "order134_e6_order_type_venue_reachability",
        "flow_stage": "LIVE/DEFEND/PROMOTE/LEARN/SELF-DEV",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "measurement_rule": (
            "taker reclassifies only price_above_venue_minimum_max_price; "
            "missing exact fingerprint evidence remains fail-closed"
        ),
        "materiality_rule": (
            f"maker_to_taker_flip_count >= ceil(candidate_count * "
            f"{MATERIAL_FLIP_SHARE_MIN_PCT:.1f}%)"
        ),
        "source": {
            "deadman_checked_at": deadman.get("checked_at"),
            "candidate_frontier_checksum": (
                ((deadman.get("policy_choke") or {}).get("source_roster_drought") or {})
                .get("candidate_evidence", {})
                .get("frontier_checksum")
            ),
            "candidate_rows_checksum": _checksum(candidates),
            "wide_evidence_generated_at": evidence.get("generated_at"),
            "wide_evidence_cells_checksum": _checksum(evidence.get("cells") or []),
        },
        "summary": {
            "candidate_count": candidate_count,
            "exact_fingerprint_evidence_count": sum(
                row["exact_fingerprint_evidence"] for row in rows
            ),
            "missing_exact_fingerprint_evidence_count": sum(
                not row["exact_fingerprint_evidence"] for row in rows
            ),
            "venue_reachable_maker_count": sum(
                row["venue_reachable_maker"] for row in rows
            ),
            "venue_reachable_taker_count": sum(
                row["venue_reachable_taker"] for row in rows
            ),
            "maker_to_taker_flip_count": flip_count,
            "maker_to_taker_flip_share_pct": (
                round(100.0 * flip_count / candidate_count, 6)
                if candidate_count
                else 0.0
            ),
            "material_flip_min_count": material_min_count,
            "material_flip": flip_count >= material_min_count,
            "eligible_before": eligible_before,
            "eligible_after_taker_reachability_only": eligible_after,
            "eligible_count_delta": eligible_after - eligible_before,
        },
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default=DEFAULT_DEADMAN)
    parser.add_argument("--evidence", default=DEFAULT_EVIDENCE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    deadman = json.loads(Path(args.deadman).read_text(encoding="utf-8"))
    evidence = json.loads(Path(args.evidence).read_text(encoding="utf-8"))
    report = build_report(deadman=deadman, evidence=evidence)
    atomic_write_json(Path(args.output), report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
