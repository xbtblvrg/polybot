#!/usr/bin/env python3
"""Build the bounded paper-only roster for fresh same-window alpha capture."""

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

from src.wallet_copy.store import atomic_write_json
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report
from scripts.run_freeze_resolution_accelerator import (
    DIRECTION_DIRECT_CLIMB_PRIORITY,
)


DEFAULT_COPYABILITY = "data/research/wallet_copy_full_universe_copyability_latest.json"
DEFAULT_ALPHA = "data/research/alpha_decay_report.json"
DEFAULT_COHORT_DIGEST = "data/research/wallet_market_cohort_replay_latest_digest.json"
DEFAULT_OVERLAY = "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_MANIFEST = "data/research/wide_exact_policy_manifest_latest.json"
DEFAULT_TEMPORAL = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_FINGERPRINT_EVIDENCE = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_OUTPUT = "data/research/wide_alpha_capture_roster_latest.json"
DEFAULT_DEPTH_FRONTIER_OUTPUT = "data/research/wide_depth_priority_frontier_latest.json"
DEFAULT_POSITIVE_COPY_PNL_LIMIT = 0
DEPTH_PRIORITY_MIN_RESOLVED = 200
DEPTH_PRIORITY_TARGET_RESOLVED = 400
DEPTH_PRIORITY_MIN_VENUE_REACHABLE_PCT = 40.0
TERMINAL_DEPTH_WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"


def _load(path: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _policy_fingerprint(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("wide_fp_"):
        return text.removeprefix("wide_fp_")
    return text


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def build_roster(
    copyability: dict[str, Any],
    alpha: dict[str, Any],
    cohort_digest: dict[str, Any],
    overlay: dict[str, Any],
    manifest: dict[str, Any] | None = None,
    temporal: dict[str, Any] | None = None,
    positive_copy_pnl_limit: int = DEFAULT_POSITIVE_COPY_PNL_LIMIT,
    fingerprint_evidence: dict[str, Any] | None = None,
    previous_depth_frontier: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sources: dict[str, set[str]] = {}
    candidate_evidence: dict[str, dict[str, Any]] = {}

    def add(value: Any, source: str) -> None:
        wallet = _wallet(value)
        if wallet:
            sources.setdefault(wallet, set()).add(source)

    for row in copyability.get("ranked_queue") or []:
        if isinstance(row, dict) and row.get("admission_status") == "READY_QUEUE":
            add(row.get("wallet"), "ready_queue")
    temporal_by_wallet = {
        _wallet(row.get("wallet")): row
        for row in (temporal or {}).get("wallets") or []
        if isinstance(row, dict) and _wallet(row.get("wallet"))
    }
    positive_copy_rows: list[tuple[int, float, str]] = []
    for row in copyability.get("leaderboard") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("wallet"))
        replay = row.get("copy_replay") if isinstance(row.get("copy_replay"), dict) else {}
        paper_pnl = float(replay.get("paper_pnl_usd") or 0.0)
        if not wallet or paper_pnl <= 0.0:
            continue
        slice_labels = temporal_by_wallet.get(wallet, {}).get("slice_labels") or {}
        weekday = slice_labels.get("weekday") if isinstance(slice_labels, dict) else {}
        resolved = int((weekday or {}).get("resolved_trades") or 0)
        positive_copy_rows.append((resolved, paper_pnl, wallet))
    positive_copy_rows.sort(key=lambda row: (-row[0], -row[1], row[2]))
    positive_copy_pnl_limit = int(positive_copy_pnl_limit)
    selected_positive_copy_rows = (
        positive_copy_rows[:positive_copy_pnl_limit]
        if positive_copy_pnl_limit > 0
        else positive_copy_rows
    )
    for rank, (resolved, paper_pnl, wallet) in enumerate(
        selected_positive_copy_rows, start=1
    ):
        add(wallet, "positive_copy_pnl_depth")
        candidate_evidence[wallet] = {
            "positive_copy_pnl_depth_rank": rank,
            "weekday_resolved_trades": resolved,
            "copy_pnl_usd": round(paper_pnl, 6),
        }
    profiles = (
        (alpha.get("execution_profiles") or {}).get("profiles")
        if isinstance(alpha.get("execution_profiles"), dict)
        else []
    )
    for row in profiles or []:
        if isinstance(row, dict) and row.get("eligible") is True:
            add(row.get("wallet"), "historical_alpha_eligible_profile")
    add("0x224a89dbe0db0d6124b335edabd15b3f877da3d5", "slice_exception")
    add(
        (cohort_digest.get("counters") or {}).get("top_live_ready_wallet")
        if isinstance(cohort_digest.get("counters"), dict)
        else None,
        "retrospective_top_supply",
    )
    for row in (manifest or {}).get("capture_watch_wallets") or []:
        if isinstance(row, dict) and row.get("paper_measurement_only") is True:
            add(row.get("wallet"), "manifest_capture_watch")
    for wallet, _fingerprint in DIRECTION_DIRECT_CLIMB_PRIORITY:
        add(wallet, "direct_climb_priority")

    directed_climb = {
        (wallet, fingerprint) for wallet, fingerprint in DIRECTION_DIRECT_CLIMB_PRIORITY
    }
    directed_climb_wallets = {wallet for wallet, _fingerprint in directed_climb}
    demoted: set[str] = set()
    demoted_identities: set[tuple[str, str]] = set()
    for row in copyability.get("top_wallets") or []:
        if (
            isinstance(row, dict)
            and row.get("admission_status") == "PRIOR_LIVE_DEMOTION_REQUIRES_FRESH_READMISSION"
        ):
            wallet = _wallet(row.get("wallet"))
            if wallet:
                demoted.add(wallet)
    for row in overlay.get("members") or []:
        if not isinstance(row, dict) or row.get("enabled") is not False:
            continue
        status = str(row.get("status") or "").upper()
        if "DEMOT" in status or "LOSS" in status:
            wallet = _wallet(row.get("source_wallet"))
            if wallet:
                demoted.add(wallet)
                fingerprint = _policy_fingerprint(
                    row.get("wide_policy_fingerprint") or row.get("policy_id")
                )
                if fingerprint:
                    demoted_identities.add((wallet, fingerprint))

    excluded = {
        wallet
        for wallet in set(sources) & demoted
        if wallet not in directed_climb_wallets
        or any((wallet, fingerprint) in demoted_identities for _wallet, fingerprint in directed_climb if _wallet == wallet)
    }
    selected = sorted(
        wallet
        for wallet, wallet_sources in sources.items()
        if wallet not in excluded or "positive_copy_pnl_depth" in wallet_sources
    )
    selected_set = set(selected)
    selected_cells = [
        row
        for row in (fingerprint_evidence or {}).get("cells") or []
        if isinstance(row, dict)
        and _wallet((row.get("identity") or {}).get("wallet")) in selected_set
    ]

    def rescore(row: dict[str, Any]) -> dict[str, Any]:
        value = row.get("venue_executable_full_stream_rescore")
        return value if isinstance(value, dict) else {}

    generated_at = datetime.now(timezone.utc).isoformat()
    previous_cut_at = _parse_iso((previous_depth_frontier or {}).get("generated_at"))
    current_cut_at = _parse_iso((fingerprint_evidence or {}).get("generated_at")) or _parse_iso(
        generated_at
    )
    previous_depth_rows = {
        str(row.get("wide_policy_fingerprint") or ""): row
        for row in (previous_depth_frontier or {}).get("cells") or []
        if isinstance(row, dict) and row.get("wide_policy_fingerprint")
    }
    depth_candidates: list[dict[str, Any]] = []
    for cell in selected_cells:
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        wallet = _wallet(identity.get("wallet"))
        summary = rescore(cell)
        resolved = int(summary.get("resolved") or 0)
        if (
            wallet == TERMINAL_DEPTH_WALLET
            or not (DEPTH_PRIORITY_MIN_RESOLVED <= resolved < DEPTH_PRIORITY_TARGET_RESOLVED)
            or float(summary.get("post_fee_pnl_usd") or 0.0) <= 0.0
            or float(summary.get("roi_pct") or 0.0) <= 0.0
            or float(summary.get("first_half_post_fee_pnl_usd") or 0.0) <= 0.0
            or float(summary.get("second_half_post_fee_pnl_usd") or 0.0) <= 0.0
            or summary.get("concentration_admissible") is not True
            or float(summary.get("venue_reachable_share_pct") or 0.0)
            < DEPTH_PRIORITY_MIN_VENUE_REACHABLE_PCT
        ):
            continue
        fingerprint = str(identity.get("wide_policy_fingerprint") or "")
        previous_row = previous_depth_rows.get(fingerprint, {})
        elapsed_h = (
            (current_cut_at - previous_cut_at).total_seconds() / 3600.0
            if current_cut_at is not None and previous_cut_at is not None
            else 0.0
        )
        resolved_delta = resolved - int(previous_row.get("resolved") or 0)
        observed_rate = (
            round(max(0, resolved_delta) * 24.0 / elapsed_h, 6)
            if elapsed_h >= 0.25
            else None
        )
        depth_candidates.append(
            {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "move_slice_keys": sorted(
                    {str(value) for value in identity.get("move_slice_keys") or [] if str(value)}
                ),
                "resolved": resolved,
                "gap_to_400": DEPTH_PRIORITY_TARGET_RESOLVED - resolved,
                "post_fee_pnl_usd": round(float(summary.get("post_fee_pnl_usd") or 0.0), 6),
                "roi_pct": round(float(summary.get("roi_pct") or 0.0), 6),
                "first_half_post_fee_pnl_usd": round(
                    float(summary.get("first_half_post_fee_pnl_usd") or 0.0), 6
                ),
                "second_half_post_fee_pnl_usd": round(
                    float(summary.get("second_half_post_fee_pnl_usd") or 0.0), 6
                ),
                "concentration_admissible": True,
                "venue_reachable_share_pct": round(
                    float(summary.get("venue_reachable_share_pct") or 0.0), 6
                ),
                "weekday_resolved_trades": int(
                    candidate_evidence.get(wallet, {}).get("weekday_resolved_trades") or 0
                ),
                "observed_resolved_signals_per_day": observed_rate,
                "rate_observation_elapsed_h": round(elapsed_h, 6) if elapsed_h > 0 else None,
                "rate_observation_resolved_delta": resolved_delta if elapsed_h >= 0.25 else None,
                "rate_status": (
                    "TWO_CUT_RATE_OBSERVED"
                    if elapsed_h >= 0.25
                    else "RATE_PENDING_SECOND_CUT_GTE_15M"
                ),
                "paper_only": True,
                "live_orders_allowed": False,
                "promotion_authority": False,
            }
        )
    depth_candidates.sort(
        key=lambda row: (
            -int(row["weekday_resolved_trades"]),
            -float(row["venue_reachable_share_pct"]),
            -int(row["resolved"]),
            str(row["wallet"]),
            str(row["wide_policy_fingerprint"]),
        )
    )
    depth_by_wallet: dict[str, list[dict[str, Any]]] = {}
    for rank, row in enumerate(depth_candidates, start=1):
        row["depth_priority_rank"] = rank
        depth_by_wallet.setdefault(str(row["wallet"]), []).append(row)
    for wallet, rows in depth_by_wallet.items():
        sources.setdefault(wallet, set()).add("depth_priority_frontier")
        candidate_evidence.setdefault(wallet, {})["depth_priority_rank"] = rows[0][
            "depth_priority_rank"
        ]
        candidate_evidence[wallet]["depth_priority_cell"] = rows[0]
        candidate_evidence[wallet]["depth_priority_cells"] = rows

    evidence_projection = {
        "basis": "current fingerprint evidence restricted to expanded paper cohort; newly captured cells appear on later cuts",
        "source_generated_at": (fingerprint_evidence or {}).get("generated_at"),
        "selected_wallets": len(selected),
        "known_cells": len(selected_cells),
        "cells_reaching_n_gte_200": sum(
            int(rescore(row).get("resolved") or row.get("resolved") or 0) >= 200
            for row in selected_cells
        ),
        "f1_pass_cells": sum(
            rescore(row).get("f1_pass") is True or row.get("f1_pass") is True
            for row in selected_cells
        ),
        "walk_forward_admissible_count": sum(
            rescore(row).get("f1_walk_forward_admissible") is True
            or row.get("f1_walk_forward_admissible") is True
            for row in selected_cells
        ),
        "walk_forward_blocked_by_half_depth": sum(
            DEPTH_PRIORITY_MIN_RESOLVED
            <= int(rescore(row).get("resolved") or row.get("resolved") or 0)
            < DEPTH_PRIORITY_TARGET_RESOLVED
            and rescore(row).get("f1_pass") is True
            and rescore(row).get("f1_walk_forward_admissible") is not True
            for row in selected_cells
        ),
        "paper_only": True,
        "live_orders_allowed": False,
    }
    return {
        "schema_version": 1,
        "kind": "wallet_copy_registry",
        "flow_stage": "LEARN/OBSERVE",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "selection_rule": (
            "READY_QUEUE union historical eligible alpha profiles plus the slice "
            "exception, retrospective top supply, manifest paper capture-watch, "
            "and directed climb priority; prior live demotions excluded except "
            "directed climb identities are fingerprint-scoped"
        ),
        "source_counts": {
            "ready_queue": sum("ready_queue" in value for value in sources.values()),
            "historical_alpha_eligible_profile": sum(
                "historical_alpha_eligible_profile" in value for value in sources.values()
            ),
            "manifest_capture_watch": sum(
                "manifest_capture_watch" in value for value in sources.values()
            ),
            "positive_copy_pnl_depth": sum(
                "positive_copy_pnl_depth" in value for value in sources.values()
            ),
            "positive_copy_pnl_available": len(positive_copy_rows),
            "positive_copy_pnl_limit": (
                positive_copy_pnl_limit if positive_copy_pnl_limit > 0 else None
            ),
            "positive_copy_pnl_missing_weekday_depth": sum(
                row[0] == 0 for row in positive_copy_rows
            ),
            "depth_priority_wallets": len(depth_by_wallet),
            "depth_priority_cells": len(depth_candidates),
            "direct_climb_priority": sum(
                "direct_climb_priority" in value for value in sources.values()
            ),
            "union_before_exclusions": len(sources),
            "prior_live_demotion_excluded": len(excluded),
            "selected": len(selected),
        },
        "positive_copy_pnl_depth_ranking": [
            {
                "wallet": wallet,
                **candidate_evidence[wallet],
                "weekday_depth_status": (
                    "OBSERVED"
                    if candidate_evidence[wallet]["weekday_resolved_trades"] > 0
                    else "MISSING_OR_ZERO"
                ),
                "selected_after_exclusions": wallet in selected_set,
            }
            for _resolved, _paper_pnl, wallet in selected_positive_copy_rows
        ],
        "evidence_projection": evidence_projection,
        "depth_priority_frontier": {
            "schema_version": 1,
            "kind": "wide_depth_priority_frontier",
            "flow_stage": "PROMOTE/LEARN",
            "generated_at": (fingerprint_evidence or {}).get("generated_at") or generated_at,
            "selection_rule": (
                "non-terminal, non-demoted exact cells with n in [200,400), positive "
                "PnL/ROI and both halves, concentration admissible, venue reach >=40%"
            ),
            "target_resolved": DEPTH_PRIORITY_TARGET_RESOLVED,
            "paper_only": True,
            "live_orders_allowed": False,
            "promotion_authority": False,
            "cells": depth_candidates,
            "summary": {
                "cell_count": len(depth_candidates),
                "wallet_count": len(depth_by_wallet),
                "non_82c8_cell_count": len(depth_candidates),
                "two_cut_rate_observed": sum(
                    row["rate_status"] == "TWO_CUT_RATE_OBSERVED"
                    for row in depth_candidates
                ),
            },
        },
        "excluded_prior_live_demotion_wallets": sorted(excluded),
        "wallets": [
            {
                "address": wallet,
                "name": f"wide_alpha_{wallet[-8:]}",
                "enabled": True,
                "market_filter": "btc_5m",
                "asset_allowlist": ["BTC"],
                "tags": ["paper_only", "wide_alpha_capture", *sorted(sources[wallet])],
                **candidate_evidence.get(wallet, {}),
            }
            for wallet in selected
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--copyability", default=DEFAULT_COPYABILITY)
    parser.add_argument("--alpha", default=DEFAULT_ALPHA)
    parser.add_argument("--cohort-digest", default=DEFAULT_COHORT_DIGEST)
    parser.add_argument("--overlay", default=DEFAULT_OVERLAY)
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL)
    parser.add_argument("--fingerprint-evidence", default=DEFAULT_FINGERPRINT_EVIDENCE)
    parser.add_argument(
        "--positive-copy-pnl-limit",
        type=int,
        default=DEFAULT_POSITIVE_COPY_PNL_LIMIT,
        help="Maximum depth-ranked positive-copy-PnL wallets; <=0 includes all.",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--depth-frontier-output", default=DEFAULT_DEPTH_FRONTIER_OUTPUT)
    parser.add_argument(
        "--previous-depth-frontier",
        default=None,
        help="Prior cut used for >=15m rate measurement; defaults to the output path.",
    )
    args = parser.parse_args()
    alpha_report = _load(args.alpha)
    require_fresh_alpha_report(alpha_report, path=args.alpha)
    report = build_roster(
        _load(args.copyability),
        alpha_report,
        _load(args.cohort_digest),
        _load(args.overlay),
        _load(args.manifest),
        _load(args.temporal),
        args.positive_copy_pnl_limit,
        _load(args.fingerprint_evidence),
        _load(args.previous_depth_frontier or args.depth_frontier_output),
    )
    atomic_write_json(args.output, report)
    atomic_write_json(args.depth_frontier_output, report["depth_priority_frontier"])
    print(json.dumps({"output": args.output, **report["source_counts"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
