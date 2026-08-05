#!/usr/bin/env python3
"""Rank the unchanged-bar WIDE ALL_PASS seat path and sole paper accrual focus."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

CANONICAL_CHECK_ORDER = (
    "f1_measured_positive_regime_cell",
    "f1_venue_reachable_admissible",
    "f1_walk_forward_admissible",
    "f2_fresh_rows_and_own_policy_copyable",
    "f3_not_enabled_or_cooloff_or_fading",
    "f4_external_liveness",
    "own_evidenced_policy_available",
    "active_temporal_not_proven_negative",
    "active_temporal_regime_cell_measured",
    "not_terminal_park_red_clock_or_measured_loser",
    "both_resolved_halves_positive",
    "f1_concentration_admissible",
)
STICKY_FOCUS_IDENTITY: tuple[str, str] | None = None


def _key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("wallet") or "").lower(),
        str(row.get("wide_policy_fingerprint") or ""),
        str(row.get("source_generation") or ""),
    )


def _compact_row(row: dict[str, Any]) -> dict[str, Any]:
    checks = row.get("checks") or {}
    fails = [name for name in CANONICAL_CHECK_ORDER if checks.get(name) is not True]
    temporal = row.get("active_temporal") or {}
    direct = row.get("direct_source") or {}
    standby = row.get("standby_exclusion") or {}
    all_pass = not fails and all(checks.get(name) is True for name in CANONICAL_CHECK_ORDER)
    return {
        "wallet": str(row.get("wallet") or "").lower(),
        "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
        "fail_count": len(fails),
        "fail_list": fails,
        "binding_deficit": fails[0] if fails else None,
        "active_temporal": {
            key: temporal.get(key)
            for key in ("label", "classification", "pnl_usd", "roi_pct", "resolved_trades")
        },
        "direct": {
            key: direct.get(key)
            for key in ("attempts", "copyable", "policy_depth_pass", "latest_receipt_at")
        },
        "f1_resolved_signals": (row.get("f1_slice_basis") or {}).get("resolved_signals"),
        "cooloff_active": bool((row.get("cooloff_scope") or {}).get("active")),
        "cooloff_until": row.get("cooloff_until"),
        "terminal_or_measured_loser": checks.get(
            "not_terminal_park_red_clock_or_measured_loser"
        )
        is not True,
        "terminal_status": standby.get("status"),
        "own_evidenced_policy_available": checks.get("own_evidenced_policy_available") is True,
        "paper_policy_id": row.get("paper_policy_id"),
        "all_pass": all_pass,
        "admit_ready": all_pass,
    }


def _fingerprint_cell(
    evidence: dict[str, Any], wallet: str, fingerprint: str
) -> dict[str, Any]:
    return next(
        (
            cell
            for cell in evidence.get("cells") or []
            if isinstance(cell, dict)
            and str((cell.get("identity") or {}).get("wallet") or "").lower()
            == wallet
            and str(cell.get("wide_policy_fingerprint") or "") == fingerprint
        ),
        {},
    )


def _focus_policy_rank(
    row: dict[str, Any], fingerprint_evidence: dict[str, Any]
) -> tuple[int, int, float, str]:
    wallet = str(row.get("wallet") or "").lower()
    fingerprint = str(row.get("wide_policy_fingerprint") or "")
    cell = _fingerprint_cell(fingerprint_evidence, wallet, fingerprint)
    venue = cell.get("venue_executable_full_stream_rescore") or {}
    first = venue.get("first_half") or {}
    second = venue.get("second_half") or {}
    sign_and_concentration_clean = bool(
        float(first.get("post_fee_pnl_usd") or 0) > 0
        and float(second.get("post_fee_pnl_usd") or 0) > 0
        and venue.get("concentration_admissible") is True
    )
    return (
        0 if sign_and_concentration_clean else 1,
        len(row.get("evidence_deficits") or []),
        -float(venue.get("post_fee_pnl_usd") or 0),
        fingerprint,
    )


def _venue_residual_zero_projection(venue: dict[str, Any]) -> dict[str, Any]:
    reachable = int(venue.get("venue_executable_resolved") or 0)
    unreachable = int(venue.get("venue_unreachable_resolved") or 0)
    residual = max(0, 200 - int(venue.get("resolved") or reachable))
    denominator = reachable + unreachable + residual
    share = (
        round(100.0 * (reachable + residual) / denominator, 6)
        if denominator
        else None
    )
    threshold = float(venue.get("venue_reachable_share_min_pct") or 40.0)
    return {
        "assumption": "every remaining signal needed to reach 200 resolves venue-reachable",
        "current_residual_to_200": residual,
        "projected_numerator": reachable + residual,
        "projected_denominator": denominator,
        "projected_share_pct": share,
        "threshold_pct": threshold,
        "admissible": bool(share is not None and share >= threshold),
    }


def _walk_forward_diagnosis(
    row: dict[str, Any], fingerprint_evidence: dict[str, Any]
) -> dict[str, Any]:
    wallet = str(row.get("wallet") or "").lower()
    fingerprint = str(row.get("wide_policy_fingerprint") or "")
    exact_cell = _fingerprint_cell(fingerprint_evidence, wallet, fingerprint)
    walk_cell = exact_cell or (
        (fingerprint_evidence.get("walk_forward_best_by_wallet") or {}).get(wallet, {})
    )
    walk = walk_cell.get("venue_executable_full_stream_rescore") or {}
    first_half = walk.get("first_half") or {}
    second_half = walk.get("second_half") or {}
    temporal_fading = (row.get("active_temporal") or {}).get("classification") == "FADING"
    exact_match = bool(exact_cell)
    insufficient = any(int(half.get("resolved") or 0) < 200 for half in (first_half, second_half))
    if temporal_fading:
        classification = "TRUE_FADING"
        disposition = "DEMOTE_TRUE_FADING_NO_OVERRIDE"
    elif not exact_match:
        classification = "FINGERPRINT_MISMATCH"
        disposition = "REQUIRE_EXACT_FINGERPRINT_EVIDENCE"
    elif insufficient:
        classification = "INSUFFICIENT_FORWARD_SAMPLE"
        disposition = "PAPER_ACCRUAL_ONLY"
    elif walk.get("f1_walk_forward_admissible") is True:
        classification = "WALK_FORWARD_PASS"
        disposition = "RETAIN_NEAREST_SEAT_NARRATIVE"
    else:
        classification = "MEASURED_WALK_FORWARD_FAILURE"
        disposition = "DEMOTE_WALK_FORWARD_FAILURE_NO_OVERRIDE"
    concentration_failure = bool(
        first_half.get("concentration_admissible") is False
        or second_half.get("concentration_admissible") is False
    )
    if temporal_fading and concentration_failure:
        classification = "TRUE_FADING_WITH_HALF_CONCENTRATION_FAILURE"
    return {
        "wallet": wallet,
        "wide_policy_fingerprint": walk_cell.get("wide_policy_fingerprint") or fingerprint,
        "classification": classification,
        "measurement_seam": False,
        "fingerprint_mismatch": not exact_match,
        "walk_forward_selection_differs_from_frontier_fingerprint": (
            bool(walk_cell.get("wide_policy_fingerprint"))
            and walk_cell.get("wide_policy_fingerprint") != fingerprint
        ),
        "true_temporal_fading": temporal_fading,
        "first_half": {
            key: first_half.get(key)
            for key in (
                "resolved",
                "f1_pass",
                "post_fee_pnl_usd",
                "pnl_excluding_top_1_market",
                "concentration_admissible",
                "concentration_deficits",
            )
        },
        "second_half": {
            key: second_half.get(key)
            for key in (
                "resolved",
                "f1_pass",
                "post_fee_pnl_usd",
                "pnl_excluding_top_1_market",
                "concentration_admissible",
                "concentration_deficits",
            )
        },
        "required_resolved_per_half": 200,
        "walk_forward_admissible": walk.get("f1_walk_forward_admissible"),
        "root_cause": classification.lower(),
        "seat_narrative_disposition": disposition,
    }


def build_report(
    *,
    frontier: dict[str, Any],
    candidate_evidence: dict[str, Any],
    fingerprint_evidence: dict[str, Any],
    paper_state: dict[str, Any] | None = None,
    completed_paper_generation: dict[str, Any] | None = None,
    completed_paper_generations: list[dict[str, Any]] | None = None,
    prior: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.UTC)
    all_rows = [row for row in candidate_evidence.get("rows") or [] if isinstance(row, dict)]
    selected_keys = {
        _key(row) for row in frontier.get("nearest_frontier") or [] if isinstance(row, dict)
    }
    selected_keys.update(
        _key(row)
        for row in all_rows
        if (row.get("checks") or {}).get("own_evidenced_policy_available") is True
    )
    ranked = [_compact_row(row) for row in all_rows if _key(row) in selected_keys]
    ranked.sort(
        key=lambda row: (
            int(row["fail_count"]),
            -float((row.get("active_temporal") or {}).get("pnl_usd") or 0.0),
            str(row.get("wallet") or ""),
            str(row.get("wide_policy_fingerprint") or ""),
        )
    )

    diagnosis_row = next(
        (
            row
            for compact in ranked
            for row in all_rows
            if str(row.get("wallet") or "").lower() == compact.get("wallet")
            and str(row.get("wide_policy_fingerprint") or "")
            == compact.get("wide_policy_fingerprint")
            and (row.get("active_temporal") or {}).get("label") == "PROVEN-POSITIVE"
            and not bool((row.get("cooloff_scope") or {}).get("active"))
            and (row.get("checks") or {}).get(
                "not_terminal_park_red_clock_or_measured_loser"
            )
            is True
        ),
        {},
    )
    diagnosis = _walk_forward_diagnosis(diagnosis_row, fingerprint_evidence)

    focus_candidates = [
        row
        for row in all_rows
        if (row.get("active_temporal") or {}).get("label") == "PROVEN-POSITIVE"
        and (row.get("active_temporal") or {}).get("classification") != "FADING"
        and not bool((row.get("cooloff_scope") or {}).get("active"))
        and (row.get("checks") or {}).get("not_terminal_park_red_clock_or_measured_loser") is True
        and (row.get("checks") or {}).get("own_evidenced_policy_available") is True
    ]
    focus_candidates.sort(
        key=lambda row: _focus_policy_rank(row, fingerprint_evidence)
    )
    ranked_focus = next(
        (
            row
            for row in focus_candidates
            if (
                str(row.get("wallet") or "").lower(),
                str(row.get("wide_policy_fingerprint") or ""),
            )
            == STICKY_FOCUS_IDENTITY
            and STICKY_FOCUS_IDENTITY is not None
        ),
        focus_candidates[0] if focus_candidates else {},
    )
    sticky_wallet, sticky_fingerprint = STICKY_FOCUS_IDENTITY or ("", "")
    sticky_exact_row = next(
        (
            row
            for row in all_rows
            if str(row.get("wallet") or "").lower() == sticky_wallet
            and str(row.get("wide_policy_fingerprint") or "") == sticky_fingerprint
        ),
        {},
    )
    sticky_wallet_row = next(
        (
            row
            for row in all_rows
            if str(row.get("wallet") or "").lower() == sticky_wallet
        ),
        {},
    )
    sticky_cell = _fingerprint_cell(
        fingerprint_evidence, sticky_wallet, sticky_fingerprint
    )
    sticky_venue = sticky_cell.get("venue_executable_full_stream_rescore") or {}
    if sticky_cell and not sticky_exact_row:
        first = sticky_venue.get("first_half") or {}
        second = sticky_venue.get("second_half") or {}
        sticky_exact_row = {
            **sticky_wallet_row,
            "wallet": sticky_wallet,
            "wide_policy_fingerprint": sticky_fingerprint,
            "source_generation": f"fingerprint:{sticky_fingerprint}",
            "source_identity": {
                "manifest_ids": (sticky_cell.get("identity") or {}).get(
                    "manifest_ids"
                )
                or []
            },
            "f1_slice_basis": {
                "resolved_signals": sticky_venue.get("resolved"),
                "source": "venue_executable_full_stream_rescore",
                "slice": "full_stream",
                "regime_sliced": False,
            },
            "direct_source": {
                "attempts": 0,
                "copyable": 0,
                "policy_depth_pass": 0,
                "latest_receipt_at": None,
            },
            "evidence_deficits": [
                name
                for name, passed in (
                    (
                        "both_resolved_halves_positive",
                        float(first.get("post_fee_pnl_usd") or 0) > 0
                        and float(second.get("post_fee_pnl_usd") or 0) > 0,
                    ),
                    (
                        "f1_concentration_admissible",
                        sticky_venue.get("concentration_admissible") is True,
                    ),
                    ("f1_measured_positive_regime_cell", sticky_venue.get("f1_pass") is True),
                    (
                        "f1_walk_forward_admissible",
                        sticky_venue.get("f1_walk_forward_admissible") is True,
                    ),
                    ("f2_fresh_rows_and_own_policy_copyable", False),
                )
                if not passed
            ],
        }
    sticky_projection = _venue_residual_zero_projection(sticky_venue)
    focus = (
        sticky_exact_row
        if sticky_exact_row and sticky_projection.get("admissible") is True
        else ranked_focus
    )
    focus_wallet = str(focus.get("wallet") or "").lower()
    focus_fingerprint = str(focus.get("wide_policy_fingerprint") or "")
    focus_cell = _fingerprint_cell(
        fingerprint_evidence, focus_wallet, focus_fingerprint
    )
    focus_venue = focus_cell.get("venue_executable_full_stream_rescore") or {}
    focus_resolved = int(
        focus_venue.get("resolved")
        if focus_venue.get("resolved") is not None
        else (focus.get("f1_slice_basis") or {}).get("resolved_signals")
        or 0
    )
    current_counters = {
        "f1_resolved_signals": focus_resolved,
        "f1_residual_to_200": max(0, 200 - focus_resolved),
        "direct_attempts": (focus.get("direct_source") or {}).get("attempts"),
        "direct_copyable": (focus.get("direct_source") or {}).get("copyable"),
    }
    focus_generation = str(focus.get("source_generation") or "")
    prior_focus = (prior or {}).get("sole_accrual_focus") or {}
    prior_counters = (
        ((prior or {}).get("sole_accrual_focus") or {}).get("after")
        if prior_focus.get("wallet") == focus_wallet
        and prior_focus.get("source_generation") == focus_generation
        else None
    ) or {}
    counter_reset_detected = bool(
        prior_counters
        and any(
            int(current_counters.get(key) or 0) < int(prior_counters.get(key) or 0)
            for key in ("f1_resolved_signals", "direct_attempts", "direct_copyable")
        )
    )
    if counter_reset_detected:
        prior_counters = {}
    before = {**current_counters, **prior_counters}
    after = current_counters
    gap_lane = frontier.get("gap_closing_lane") or {}
    paper_state = paper_state or {}
    paper_identity = (
        (((paper_state.get("manifest") or {}).get("wallet_policy_identities") or {}).get(
            focus_wallet
        ))
        or {}
    )
    paper_wallet = ((paper_state.get("wallets") or {}).get(focus_wallet)) or {}
    paper_identity_active = (
        str(paper_identity.get("wide_policy_fingerprint") or "") == focus_fingerprint
    )
    completed_paper_generation = completed_paper_generation or {}
    completed_row = next(
        (
            row
            for row in completed_paper_generation.get("standings") or []
            if isinstance(row, dict)
            and str(row.get("wallet") or "").lower() == focus_wallet
            and str(row.get("wide_policy_fingerprint") or "") == focus_fingerprint
        ),
        {},
    )
    evidence_paper_wallet = completed_row or paper_wallet
    generation_history = []
    prior_generation_counters: dict[str, int] | None = None
    for generation in completed_paper_generations or [completed_paper_generation]:
        generation_row = next(
            (
                row
                for row in generation.get("standings") or []
                if isinstance(row, dict)
                and str(row.get("wallet") or "").lower() == focus_wallet
                and str(row.get("wide_policy_fingerprint") or "")
                == focus_fingerprint
            ),
            {},
        )
        if not generation_row:
            continue
        counters = {
            "attempted_exact_policy_buys": int(
                generation_row.get("attempted_exact_policy_buys") or 0
            ),
            "copyable_exact_policy_buys": int(
                generation_row.get("copyable_exact_policy_buys") or 0
            ),
            "resolved_orders": int(generation_row.get("resolved_orders") or 0),
        }
        generation_history.append(
            {
                "run_id": generation.get("_run_id"),
                "generated_at": generation.get("generated_at"),
                "identity_only_totals": counters,
                "delta_vs_previous_generation": {
                    key: counters[key] - (prior_generation_counters or {}).get(key, 0)
                    for key in counters
                },
                "refusal_taxonomy": generation_row.get("refusal_counts") or {},
            }
        )
        prior_generation_counters = counters
    refuse_counts = evidence_paper_wallet.get("refusal_counts") or {}
    paper_attempts = int(
        evidence_paper_wallet.get("attempted_exact_policy_buys") or 0
    )
    paper_copyable = int(
        evidence_paper_wallet.get("copyable_exact_policy_buys") or 0
    )
    paper_current = {
        "attempted_exact_policy_buys": paper_attempts,
        "copyable_exact_policy_buys": paper_copyable,
        "resolved_orders": int(evidence_paper_wallet.get("resolved_orders") or 0),
    }
    prior_paper = prior_focus.get("exact_policy_paper") or {}
    prior_paper_after = prior_paper.get("after") or {
        key: int(prior_paper.get(key) or 0) for key in paper_current
    }
    paper_before = paper_current
    if prior_focus.get("wide_policy_fingerprint") == focus_fingerprint:
        paper_before = (
            (prior_paper.get("before") or prior_paper_after)
            if completed_row
            else prior_paper_after
        )
    if not paper_identity_active:
        refuse_status = "FOCUS_IDENTITY_NOT_ACTIVE"
    elif paper_copyable > 0:
        refuse_status = "COPYABLE_EXACT_POLICY_BUY_OBSERVED"
    elif paper_attempts == 0:
        refuse_status = "SOURCE_QUIET_NO_EXACT_POLICY_ATTEMPTS"
    elif int(refuse_counts.get("upstream_fanout_delay_gt_5s") or 0) > 0:
        refuse_status = "IRREDUCIBLE_UPSTREAM_FANOUT_DELAY_GT_5S"
    elif int(refuse_counts.get("paper_prefetch_delay_gt_5s") or 0) > 0:
        refuse_status = "PAPER_PREFETCH_DELAY_GT_5S"
    elif int(refuse_counts.get("stale_receipt_to_fetch") or 0) > 0:
        refuse_status = "LEGACY_RECEIVER_UNPARTITIONED_END_TO_END_DELAY_GT_5S"
    else:
        refuse_status = "NO_COPYABLE_YET_WITHOUT_TERMINAL_CLASS"
    sole_focus = {
        "wallet": focus_wallet or None,
        "wide_policy_fingerprint": focus_fingerprint,
        "source_generation": focus_generation or None,
        "source_run_id": (focus.get("source_identity") or {}).get("run_id"),
        "selection_rule": "among non-FADING non-cooloff non-terminal owned-policy rows prefer venue both-halves-positive plus concentration-admissible, then fewest deficits, then highest venue post-fee PnL",
        "before": before,
        "after": after,
        "delta": {
            key: int(after.get(key) or 0) - int(before.get(key) or 0)
            for key in after
        },
        "residual_velocity": {
            "completed_identity_generation_count": len(generation_history),
            "resolved_signals_delta": int(after.get("f1_resolved_signals") or 0)
            - int(before.get("f1_resolved_signals") or 0),
            "residual_to_200_delta": int(after.get("f1_residual_to_200") or 0)
            - int(before.get("f1_residual_to_200") or 0),
            "signals_per_completed_generation": round(
                (
                    int(after.get("f1_resolved_signals") or 0)
                    - int(before.get("f1_resolved_signals") or 0)
                )
                / max(1, len(generation_history)),
                6,
            ),
        },
        "active_temporal": focus.get("active_temporal") or {},
        "direct_counter_reset_detected": counter_reset_detected,
        "binding_deficits": focus.get("evidence_deficits") or [],
        "single_fingerprint_f1": {
            "evidence_authority": "venue_executable_full_stream_rescore",
            "resolved": focus_resolved,
            "residual_to_200": max(0, 200 - focus_resolved),
            "post_fee_pnl_usd": focus_venue.get("post_fee_pnl_usd"),
            "roi_pct": focus_venue.get("roi_pct"),
            "concentration_admissible": focus_venue.get("concentration_admissible"),
            "venue_reachable_share_pct": focus_venue.get(
                "venue_reachable_share_pct"
            ),
            "venue_reachable_share_min_pct": focus_venue.get(
                "venue_reachable_share_min_pct"
            ),
            "venue_reachable_admissible": focus_venue.get(
                "f1_venue_reachable_admissible"
            ),
            "first_half": focus_venue.get("first_half") or {},
            "second_half": focus_venue.get("second_half") or {},
            "walk_forward_admissible": focus_venue.get(
                "f1_walk_forward_admissible"
            ),
        },
        "venue_residual_zero_projection": _venue_residual_zero_projection(
            focus_venue
        ),
        "walk_forward_diagnosis": _walk_forward_diagnosis(
            focus, fingerprint_evidence
        ),
        "pass_predicate": {
            "f1_min_resolved_signals": 200,
            "both_resolved_halves_positive": True,
            "f1_concentration_admissible": True,
            "f1_measured_positive_regime_cell": True,
            "f1_walk_forward_admissible": True,
        },
        "paper_lane": {
            "lane": gap_lane.get("lane"),
            "status": gap_lane.get("status"),
            "source_state": gap_lane.get("source_state"),
            "covers_wallet": focus_wallet in (gap_lane.get("covers_wallets") or []),
            "focus_identity_active": paper_identity_active,
            "active_wide_policy_fingerprint": paper_identity.get(
                "wide_policy_fingerprint"
            ),
            "latest_receipt_at": (focus.get("direct_source") or {}).get("latest_receipt_at"),
            "live_authority": gap_lane.get("live_authority"),
        },
        "exact_policy_paper": {
            "identity_active": paper_identity_active,
            "status": refuse_status,
            "before": paper_before,
            "after": paper_current,
            "delta": {
                key: int(paper_current.get(key) or 0)
                - int(paper_before.get(key) or 0)
                for key in paper_current
            },
            "completed_generation": {
                "run_id": completed_paper_generation.get("_run_id"),
                "generated_at": completed_paper_generation.get("generated_at"),
                "available": bool(completed_row),
            },
            "generation_history": generation_history,
            "current_active_generation": {
                "manifest_id": (paper_state.get("manifest") or {}).get("manifest_id"),
                "updated_at": paper_state.get("updated_at"),
                "attempted_exact_policy_buys": int(
                    paper_wallet.get("attempted_exact_policy_buys") or 0
                ),
                "copyable_exact_policy_buys": int(
                    paper_wallet.get("copyable_exact_policy_buys") or 0
                ),
                "resolved_orders": int(paper_wallet.get("resolved_orders") or 0),
            },
            "attempted_exact_policy_buys": paper_current["attempted_exact_policy_buys"] if paper_identity_active else 0,
            "copyable_exact_policy_buys": paper_current["copyable_exact_policy_buys"] if paper_identity_active else 0,
            "resolved_orders": paper_current["resolved_orders"] if paper_identity_active else 0,
            "refusal_taxonomy": refuse_counts if paper_identity_active else {},
            "inactive_identity_refusals_excluded": not paper_identity_active,
        },
        "local_refuse_diagnosis": {
            "alpha_profile_filter": {
                "count": int(refuse_counts.get("alpha_profile_filter") or 0) if paper_identity_active else 0,
                "code_site": "scripts/reconcile_wide_exact_policy_paper.py: slice membership before attempts",
                "resolution": "sticky exact fingerprint supplies its frozen move_slice_keys",
            },
            "stale_receipt_to_fetch": {
                "count": int(refuse_counts.get("stale_receipt_to_fetch") or 0) if paper_identity_active else 0,
                "code_site": "scripts/reconcile_wide_exact_policy_paper.py: receipt-to-book fetch lag > 5s",
                "resolution": "end-to-end 5s bar remains unchanged; new receiver instrumentation separates upstream fanout delay from paper prefetch delay on the next permitted resident generation",
            },
            "upstream_fanout_delay_gt_5s": {
                "count": int(refuse_counts.get("upstream_fanout_delay_gt_5s") or 0) if paper_identity_active else 0,
                "code_site": "scripts/reconcile_wide_exact_policy_paper.py: capture-prefetched upstream_to_fanout_ms > 5000 and fanout_to_fetch_ms <= 5000",
                "irreducible_paper_side": True,
            },
            "paper_prefetch_delay_gt_5s": {
                "count": int(refuse_counts.get("paper_prefetch_delay_gt_5s") or 0) if paper_identity_active else 0,
                "code_site": "scripts/reconcile_wide_exact_policy_paper.py: fanout_to_fetch_ms > 5000",
                "irreducible_paper_side": False,
            },
        },
    }
    return {
        "schema_version": 1,
        "kind": "wide_all_pass_seat_path",
        "flow_stage": "LIVE/PROMOTE/LEARN",
        "generated_at": now.isoformat(),
        "paper_only": True,
        "live_orders_allowed": False,
        "candidate_count": frontier.get("candidate_count"),
        "eligible_count": frontier.get("eligible_count"),
        "canonical_check_order": list(CANONICAL_CHECK_ORDER),
        "ranked_candidate_count": len(ranked),
        "top_row": ranked[0] if ranked else None,
        "top_10": ranked[:10],
        "walk_forward_diagnosis": diagnosis,
        "sole_accrual_focus": sole_focus,
        "admission_applied": False,
        "gate_mutated": False,
        "roster_mutated": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frontier", default="data/research/wide_direct_admissible_frontier_latest.json")
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--fingerprint-evidence", default="data/research/wide_policy_fingerprint_evidence_latest.json")
    parser.add_argument("--paper-state", default="data/research/wide_exact_policy_paper_state.json")
    parser.add_argument("--supervisor-state", default="data/research/wide_prospective_supervisor_state.json")
    parser.add_argument("--output", default="data/research/wide_all_pass_seat_path_latest.json")
    args = parser.parse_args()
    deadman = load_json(args.deadman, default={}) or {}
    candidate_evidence = (
        (((deadman.get("policy_choke") or {}).get("source_roster_drought") or {}).get("candidate_evidence"))
        or {}
    )
    prior = load_json(args.output, default={}) or {}
    supervisor = load_json(args.supervisor_state, default={}) or {}
    completed_runs = [
        row for row in supervisor.get("completed_runs") or [] if isinstance(row, dict)
    ]
    completed_paper_generations = []
    for completed in completed_runs[-10:]:
        run_id = str(completed.get("run_id") or "")
        generation = (
            load_json(
                f"data/research/wide_candidate_standings_{run_id}.json",
                default={},
            )
            if run_id
            else {}
        ) or {}
        if run_id and generation:
            completed_paper_generations.append({**generation, "_run_id": run_id})
    completed_paper_generation = (
        completed_paper_generations[-1] if completed_paper_generations else {}
    )
    report = build_report(
        frontier=load_json(args.frontier, default={}) or {},
        candidate_evidence=candidate_evidence,
        fingerprint_evidence=load_json(args.fingerprint_evidence, default={}) or {},
        paper_state=load_json(args.paper_state, default={}) or {},
        completed_paper_generation=completed_paper_generation,
        completed_paper_generations=completed_paper_generations,
        prior=prior,
    )
    atomic_write_json(args.output, report)
    print(json.dumps({
        "output": args.output,
        "eligible_count": report["eligible_count"],
        "top_wallet": (report.get("top_row") or {}).get("wallet"),
        "focus_wallet": (report.get("sole_accrual_focus") or {}).get("wallet"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
