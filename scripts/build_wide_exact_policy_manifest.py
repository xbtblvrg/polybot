#!/usr/bin/env python3
"""Freeze a completed WIDE alpha cut into the next-run admission manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, stable_id, utc_now_iso
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.venue_executability import venue_gate_summary
from scripts.run_freeze_resolution_accelerator import (
    DIRECTION_DIRECT_CLIMB_PRIORITY,
)
from scripts.reconcile_wide_exact_policy_paper import wide_policy_identity


STICKY_PAPER_ACCRUAL_FOCUS: tuple[tuple[str, str], ...] = ()


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def load_fresh_alpha_source(path: str) -> tuple[dict[str, Any], bytes]:
    raw = Path(path).read_bytes()
    alpha = json.loads(raw)
    require_fresh_alpha_report(alpha, path=path, max_age_h=24.0)
    return alpha, raw


def _iso_timestamp(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _walk_exclusions(value: Any) -> set[str]:
    excluded: set[str] = set()
    if isinstance(value, dict):
        wallet = _wallet(value.get("source_wallet") or value.get("wallet"))
        action = str(
            value.get("action")
            or value.get("status")
            or value.get("regime_slice_label")
            or value.get("label")
            or ""
        ).upper()
        if wallet and (
            value.get("rotation_triggered") is True
            or "DEMOT" in action
            or action.startswith("EXCLUDE")
            or "PROVEN_NEGATIVE" in action
            or "TAIL_NEGATIVE" in action
            or "REARM_PENDING" in action
        ):
            excluded.add(wallet)
        for child in value.values():
            excluded.update(_walk_exclusions(child))
    elif isinstance(value, list):
        for child in value:
            excluded.update(_walk_exclusions(child))
    return excluded


def _climb_freeze_overrides(
    fingerprint_evidence: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    cells = (
        fingerprint_evidence.get("cells")
        if isinstance(fingerprint_evidence, dict)
        and isinstance(fingerprint_evidence.get("cells"), list)
        else []
    )
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for cell in cells or []:
        if not isinstance(cell, dict):
            continue
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        by_identity[
            (
                _wallet(identity.get("wallet")),
                str(cell.get("wide_policy_fingerprint") or ""),
            )
        ] = cell
    overrides: dict[str, dict[str, Any]] = {}
    for wallet, fingerprint in DIRECTION_DIRECT_CLIMB_PRIORITY:
        if wallet in overrides:
            continue
        cell = by_identity.get((wallet, fingerprint))
        if not cell:
            continue
        identity = cell.get("identity") or {}
        f1 = venue_gate_summary(cell)
        f1_closed = f1.get("f1_pass") is True
        both_halves_positive = bool(
            num(f1.get("first_half_post_fee_pnl_usd")) > 0
            and num(f1.get("second_half_post_fee_pnl_usd")) > 0
        )
        overrides[wallet] = {
            "wide_policy_fingerprint": fingerprint,
            "move_slice_keys": sorted(
                {str(value) for value in identity.get("move_slice_keys") or [] if str(value)}
            ),
            "f1": f1,
            "reason": (
                "climb_priority_exact_fp_f1_closed_paper_feedstock"
                if f1_closed and both_halves_positive
                else (
                    "climb_priority_exact_fp_both_halves_positive_f1_open_paper_feedstock"
                    if both_halves_positive
                    else (
                        "climb_priority_exact_fp_f1_closed_halves_open_paper_feedstock"
                        if f1_closed
                        else "climb_priority_exact_fp_f1_and_halves_open_paper_feedstock"
                    )
                )
            ),
        }
    return overrides


def _sticky_paper_focus_overrides(
    fingerprint_evidence: dict[str, Any] | None,
    *,
    authorized: bool = True,
) -> dict[str, dict[str, Any]]:
    """Resolve the directed paper-only focus to its immutable evidence cell."""

    if not authorized:
        return {}

    cells = (
        fingerprint_evidence.get("cells")
        if isinstance(fingerprint_evidence, dict)
        and isinstance(fingerprint_evidence.get("cells"), list)
        else []
    )
    by_identity = {
        (
            _wallet((cell.get("identity") or {}).get("wallet")),
            str(cell.get("wide_policy_fingerprint") or ""),
        ): cell
        for cell in cells
        if isinstance(cell, dict) and isinstance(cell.get("identity"), dict)
    }
    overrides: dict[str, dict[str, Any]] = {}
    for wallet, fingerprint in STICKY_PAPER_ACCRUAL_FOCUS:
        cell = by_identity.get((wallet, fingerprint))
        if not cell:
            continue
        identity = cell.get("identity") or {}
        f1 = venue_gate_summary(cell)
        reachable = int(f1.get("venue_executable_resolved") or 0)
        unreachable = int(f1.get("venue_unreachable_resolved") or 0)
        residual = max(0, 200 - int(f1.get("resolved") or reachable))
        projected_denominator = reachable + unreachable + residual
        projected_share_pct = (
            100.0 * (reachable + residual) / projected_denominator
            if projected_denominator
            else 0.0
        )
        if projected_share_pct < float(
            f1.get("venue_reachable_share_min_pct") or 40.0
        ):
            continue
        overrides[wallet] = {
            "wide_policy_fingerprint": fingerprint,
            "move_slice_keys": sorted(
                {str(value) for value in identity.get("move_slice_keys") or [] if str(value)}
            ),
            "f1": f1,
            "venue_residual_zero_projection": {
                "projected_share_pct": round(projected_share_pct, 6),
                "admissible": True,
            },
            "reason": "directed_sticky_sole_focus_exact_fingerprint_paper_feedstock",
        }
    return overrides


def _policy_evidence_by_fingerprint(
    fingerprint_evidence: dict[str, Any] | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    cells = (
        fingerprint_evidence.get("cells")
        if isinstance(fingerprint_evidence, dict)
        and isinstance(fingerprint_evidence.get("cells"), list)
        else []
    )
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        identity = (
            cell.get("identity")
            if isinstance(cell.get("identity"), dict)
            else {}
        )
        wallet = _wallet(identity.get("wallet"))
        fingerprint = str(cell.get("wide_policy_fingerprint") or "")
        if wallet and fingerprint:
            result[(wallet, fingerprint)] = cell
    return result


def _capture_policy_fields(
    *,
    wallet: str,
    move_slice_keys: list[str],
    freeze: dict[str, Any] | None,
    evidence_by_fingerprint: dict[tuple[str, str], dict[str, Any]],
    freeze_status: str,
    capture_exclusion_overridden: bool,
    allow_pending_first_capture: bool = False,
) -> dict[str, Any]:
    if not move_slice_keys:
        return {
            "wide_policy_fingerprint": None,
            "slice_freeze": None,
            "policy_absent": True,
            "policy_absent_reason": "no_positive_70pct_move_slice",
        }
    if freeze:
        fingerprint = str(freeze.get("wide_policy_fingerprint") or "")
        f1 = freeze.get("f1") if isinstance(freeze.get("f1"), dict) else None
        reason = freeze.get("reason")
    else:
        fingerprint = str(
            wide_policy_identity(
                wallet=wallet,
                move_slice_keys=move_slice_keys,
            )["wide_policy_fingerprint"]
        )
        cell = evidence_by_fingerprint.get((wallet, fingerprint))
        if cell is None:
            if not allow_pending_first_capture:
                raise ValueError(
                    f"missing fingerprint evidence for capture policy "
                    f"{wallet}|{fingerprint}"
                )
            f1 = {
                "status": "PENDING_FIRST_CAPTURE",
                "f1_pass": False,
                "promotion_authority": False,
            }
            reason = "paper_cohort_pending_first_fingerprint_capture"
        else:
            f1 = venue_gate_summary(cell)
            reason = "canonical_policy_identity_f1_snapshot"
    if not fingerprint or not isinstance(f1, dict):
        raise ValueError(
            f"incomplete capture policy evidence for {wallet}"
        )
    return {
        "wide_policy_fingerprint": fingerprint,
        "slice_freeze": {
            "status": freeze_status,
            "reason": reason,
            "f1": f1,
            "capture_exclusion_overridden": capture_exclusion_overridden,
        },
        "policy_absent": False,
    }


def build_manifest(
    *,
    alpha: dict[str, Any],
    queue: dict[str, Any],
    roster: dict[str, Any],
    degrade: dict[str, Any],
    source_alpha_report: str,
    score_run_id: str,
    source_sha256: str,
    cohort: dict[str, Any] | None = None,
    fingerprint_evidence: dict[str, Any] | None = None,
    source_roster_drought_firing: bool = False,
    sticky_focus_authorized: bool = True,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if alpha.get("status") != "PASS_CURRENT_SOURCE":
        raise ValueError("completed current-source alpha PASS is required")
    freshness = alpha.get("source_freshness") if isinstance(alpha.get("source_freshness"), dict) else {}
    if freshness.get("pass") is not True or freshness.get("history_is_frozen_d97") is not False:
        raise ValueError("alpha freshness/frozen-history gate failed")
    generated_at = generated_at or utc_now_iso()
    source_timestamp = _iso_timestamp(alpha.get("updated_at"))
    generated_timestamp = _iso_timestamp(generated_at)
    freshness_limit_s = num(freshness.get("freshness_limit_s"))
    source_alpha_age_h = (
        max(0.0, generated_timestamp - source_timestamp) / 3600.0
        if source_timestamp is not None and generated_timestamp is not None
        else None
    )
    stale_alpha_source = bool(
        freshness_limit_s > 0
        and source_alpha_age_h is not None
        and source_alpha_age_h * 3600.0 > freshness_limit_s
    )
    source_alpha_status = (
        "STALE_SOURCE_REFUSED_NOT_CURRENT"
        if stale_alpha_source
        else str(alpha.get("status") or "")
    )
    ready_rows = {
        _wallet(row.get("wallet")): row
        for row in queue.get("ranked_queue") or []
        if isinstance(row, dict) and row.get("admission_status") == "READY_QUEUE" and _wallet(row.get("wallet"))
    }
    roster_by_wallet = {
        _wallet(row.get("address") or row.get("wallet")): row
        for row in roster.get("wallets") or []
        if isinstance(row, dict) and _wallet(row.get("address") or row.get("wallet"))
    }
    queue_rank = max(
        (int(row.get("queue_rank") or 0) for row in ready_rows.values()),
        default=0,
    )
    for row in roster.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("address") or row.get("wallet"))
        tags = {str(value) for value in row.get("tags") or []}
        if (
            not wallet
            or wallet in ready_rows
            or not tags.intersection({"ready_queue", "positive_copy_pnl_depth"})
        ):
            continue
        queue_rank += 1
        ready_rows[wallet] = {
            "wallet": wallet,
            "queue_rank": queue_rank,
            "admission_status": "PAPER_COHORT_POSITIVE_COPY_PNL",
            "paper_measurement_only": True,
            "promotion_authority": False,
            "weekday_resolved_trades": int(row.get("weekday_resolved_trades") or 0),
            "copy_pnl_usd": row.get("copy_pnl_usd"),
            "depth_priority_rank": row.get("depth_priority_rank"),
            "depth_priority_cell": row.get("depth_priority_cell"),
        }
    for wallet, queue_row in ready_rows.items():
        roster_row = roster_by_wallet.get(wallet, {})
        if roster_row.get("depth_priority_cell"):
            queue_row["depth_priority_rank"] = roster_row.get("depth_priority_rank")
            queue_row["depth_priority_cell"] = roster_row.get("depth_priority_cell")
    excluded = {
        _wallet(value)
        for value in roster.get("excluded_prior_live_demotion_wallets") or []
        if _wallet(value)
    }
    excluded.update(_walk_exclusions(degrade))
    profiles = ((alpha.get("execution_profiles") or {}).get("profiles_by_wallet") or {})
    profile_summary = alpha.get("execution_profiles") if isinstance(alpha.get("execution_profiles"), dict) else {}
    eligible_profile_wallets = sorted(
        wallet
        for wallet, profile in profiles.items()
        if _wallet(wallet) and isinstance(profile, dict) and profile.get("eligible") is True
    )
    cohort_live_ready = {
        _wallet(row.get("wallet"))
        for row in ((cohort or {}).get("live_ready_picks") or [])
        if isinstance(row, dict)
        and row.get("status") == "LIVE_READY_SHADOW_PICK"
        and _wallet(row.get("wallet"))
    }
    admitted: list[dict[str, Any]] = []
    capture_watch: list[dict[str, Any]] = []
    census: list[dict[str, Any]] = []
    freeze_overrides = dict(
        (fingerprint_evidence or {}).get("freeze_overrides")
        if isinstance((fingerprint_evidence or {}).get("freeze_overrides"), dict)
        else {}
    )
    freeze_overrides.update(_climb_freeze_overrides(fingerprint_evidence))
    sticky_focus_overrides = _sticky_paper_focus_overrides(
        fingerprint_evidence,
        authorized=sticky_focus_authorized,
    )
    freeze_overrides.update(sticky_focus_overrides)
    evidence_by_fingerprint = _policy_evidence_by_fingerprint(
        fingerprint_evidence
    )
    climb_priority_by_wallet = {
        wallet: fingerprint
        for wallet, fingerprint in DIRECTION_DIRECT_CLIMB_PRIORITY
    }
    paper_focus_by_wallet = {
        wallet: str(row.get("wide_policy_fingerprint") or "")
        for wallet, row in sticky_focus_overrides.items()
    }
    for wallet, queue_row in sorted(
        ready_rows.items(),
        key=lambda item: (
            0 if item[1].get("depth_priority_cell") else 1,
            int(item[1].get("depth_priority_rank") or 999999),
            int(item[1].get("queue_rank") or 999999),
        ),
    ):
        profile = profiles.get(wallet) if isinstance(profiles.get(wallet), dict) else {}
        slices = [
            row for row in profile.get("move_slices") or []
            if isinstance(row, dict)
            and num(row.get("mean_edge")) > 0
            and num(row.get("median_edge")) > 0
            and num(row.get("copyable_rate_pct")) >= 70.0
        ]
        depth_priority_cell = (
            queue_row.get("depth_priority_cell")
            if isinstance(queue_row.get("depth_priority_cell"), dict)
            else None
        )
        depth_priority_freeze = (
            {
                "wide_policy_fingerprint": depth_priority_cell.get(
                    "wide_policy_fingerprint"
                ),
                "move_slice_keys": depth_priority_cell.get("move_slice_keys") or [],
                "reason": "wide_depth_priority_frontier_exact_cell",
                "f1": venue_gate_summary(
                    evidence_by_fingerprint.get(
                        (
                            wallet,
                            str(depth_priority_cell.get("wide_policy_fingerprint") or ""),
                        ),
                        {},
                    )
                ),
            }
            if depth_priority_cell
            else None
        )
        freeze = depth_priority_freeze or (
            freeze_overrides.get(wallet)
            if isinstance(freeze_overrides.get(wallet), dict)
            and (
                source_roster_drought_firing
                or str(
                    freeze_overrides.get(wallet, {}).get(
                        "wide_policy_fingerprint"
                    )
                    or ""
                )
                == climb_priority_by_wallet.get(wallet)
                or str(
                    freeze_overrides.get(wallet, {}).get(
                        "wide_policy_fingerprint"
                    )
                    or ""
                )
                == paper_focus_by_wallet.get(wallet)
            )
            else None
        )
        climb_priority_freeze = bool(
            freeze
            and str(freeze.get("wide_policy_fingerprint") or "")
            == climb_priority_by_wallet.get(wallet)
        )
        sticky_focus_freeze = bool(
            freeze
            and str(freeze.get("wide_policy_fingerprint") or "")
            == paper_focus_by_wallet.get(wallet)
        )
        # A temporal loss classification may exclude a wallet from live
        # admission while its currently active slice is negative, but it must
        # not erase an explicitly frozen fingerprint from the paper capture.
        # The deadman re-checks every live gate (including the active temporal
        # slices) before it can consume this evidence.
        paper_cohort_override = bool(
            wallet in excluded
            and queue_row.get("paper_measurement_only") is True
            and queue_row.get("promotion_authority") is False
        )
        frozen_capture_override = bool(freeze and wallet in excluded)
        capture_exclusion_overridden = bool(
            frozen_capture_override or paper_cohort_override
        )
        effective_slice_keys = (
            sorted({str(value) for value in freeze.get("move_slice_keys") or [] if str(value)})
            if freeze
            else [str(row.get("move_slice_key")) for row in slices]
        )
        prospective_fingerprint = (
            str(
                wide_policy_identity(
                    wallet=wallet,
                    move_slice_keys=effective_slice_keys,
                )["wide_policy_fingerprint"]
            )
            if effective_slice_keys and not freeze
            else ""
        )
        policy_evidence_missing = bool(
            prospective_fingerprint
            and (wallet, prospective_fingerprint) not in evidence_by_fingerprint
        )
        reasons: list[str] = []
        if (
            queue_row.get("paper_measurement_only") is True
            and queue_row.get("promotion_authority") is False
        ):
            reasons.append("paper_cohort_measurement_only")
        if wallet in excluded:
            reasons.append("standing_demotion_or_negative_exclusion")
        if profile.get("eligible") is not True:
            reasons.append("completed_alpha_profile_not_eligible")
        if stale_alpha_source:
            reasons.append("stale_alpha_source")
        if policy_evidence_missing and wallet not in excluded:
            reasons.append("pending_first_fingerprint_capture")
        if cohort is not None and wallet not in cohort_live_ready:
            reasons.append("not_in_live_ready_market_cohort_replay")
        if not slices and not freeze:
            reasons.append("no_positive_70pct_move_slice")
        census.append(
            {
                "wallet": wallet,
                "queue_rank": queue_row.get("queue_rank"),
                "alpha_eligible": profile.get("eligible") is True,
                "move_slice_count": len(slices),
                "excluded": wallet in excluded,
                "refusal_reasons": reasons,
            }
        )
        if wallet not in excluded or capture_exclusion_overridden:
            capture_policy = _capture_policy_fields(
                wallet=wallet,
                move_slice_keys=effective_slice_keys,
                freeze=freeze,
                evidence_by_fingerprint=evidence_by_fingerprint,
                freeze_status=(
                    "FROZEN_DEPTH_PRIORITY_PAPER_FEEDSTOCK"
                    if depth_priority_freeze
                    else "FROZEN_SOLE_FOCUS_PAPER_FEEDSTOCK"
                    if sticky_focus_freeze
                    else "FROZEN_CLIMB_PRIORITY_PAPER_FEEDSTOCK"
                    if climb_priority_freeze
                    else (
                        "FROZEN_SOURCE_ROSTER_DROUGHT"
                        if freeze
                        else "SNAPSHOT_CANONICAL_POLICY_EVIDENCE"
                    )
                ),
                capture_exclusion_overridden=capture_exclusion_overridden,
                allow_pending_first_capture=(
                    policy_evidence_missing
                    or (
                        queue_row.get("paper_measurement_only") is True
                        and queue_row.get("promotion_authority") is False
                    )
                ),
            )
            capture_watch.append(
                {
                    "wallet": wallet,
                    "queue_rank": queue_row.get("queue_rank"),
                    "move_slice_keys": effective_slice_keys,
                    **capture_policy,
                    "capture_exclusion_overridden": capture_exclusion_overridden,
                    "promotion_authority": False,
                    "paper_measurement_only": True,
                    "depth_priority_rank": queue_row.get("depth_priority_rank"),
                    "depth_priority": bool(depth_priority_freeze),
                }
            )
        if reasons:
            continue
        admitted.append(
            {
                "wallet": wallet,
                "queue_rank": queue_row.get("queue_rank"),
                "move_slice_keys": effective_slice_keys,
                "wide_policy_fingerprint": (
                    capture_policy["wide_policy_fingerprint"]
                    if wallet not in excluded or capture_exclusion_overridden
                    else (
                        str(freeze.get("wide_policy_fingerprint") or "")
                        if freeze
                        else str(
                            wide_policy_identity(
                                wallet=wallet,
                                move_slice_keys=effective_slice_keys,
                            )["wide_policy_fingerprint"]
                        )
                    )
                ),
                "alpha_fill_sample": profile.get("fill_sample"),
                "alpha_copyable_rate_pct": profile.get("copyable_rate_pct"),
                "alpha_mean_edge": profile.get("mean_edge"),
                "alpha_median_edge": profile.get("median_edge"),
            }
        )
    captured_wallets = {row["wallet"] for row in capture_watch}
    for wallet, fingerprint in DIRECTION_DIRECT_CLIMB_PRIORITY:
        if wallet in captured_wallets:
            continue
        freeze = freeze_overrides.get(wallet)
        if (
            not isinstance(freeze, dict)
            or str(freeze.get("wide_policy_fingerprint") or "") != fingerprint
        ):
            continue
        capture_watch.append(
            {
                "wallet": wallet,
                "queue_rank": 0,
                "move_slice_keys": sorted(
                    {str(value) for value in freeze.get("move_slice_keys") or [] if str(value)}
                ),
                "wide_policy_fingerprint": fingerprint,
                "slice_freeze": {
                    "status": "FROZEN_CLIMB_PRIORITY_PAPER_FEEDSTOCK",
                    "reason": freeze.get("reason"),
                    "f1": freeze.get("f1"),
                    "capture_exclusion_overridden": wallet in excluded,
                },
                "policy_absent": False,
                "capture_exclusion_overridden": wallet in excluded,
                "promotion_authority": False,
                "paper_measurement_only": True,
            }
        )
        captured_wallets.add(wallet)
    for wallet, fingerprint in STICKY_PAPER_ACCRUAL_FOCUS:
        if paper_focus_by_wallet.get(wallet) != fingerprint:
            continue
        if wallet in captured_wallets:
            continue
        freeze = freeze_overrides.get(wallet)
        if (
            not isinstance(freeze, dict)
            or str(freeze.get("wide_policy_fingerprint") or "") != fingerprint
        ):
            continue
        capture_watch.append(
            {
                "wallet": wallet,
                "queue_rank": 0,
                "move_slice_keys": sorted(
                    {str(value) for value in freeze.get("move_slice_keys") or [] if str(value)}
                ),
                "wide_policy_fingerprint": fingerprint,
                "slice_freeze": {
                    "status": "FROZEN_SOLE_FOCUS_PAPER_FEEDSTOCK",
                    "reason": freeze.get("reason"),
                    "f1": freeze.get("f1"),
                    "capture_exclusion_overridden": wallet in excluded,
                },
                "policy_absent": False,
                "capture_exclusion_overridden": wallet in excluded,
                "promotion_authority": False,
                "paper_measurement_only": True,
            }
        )
        captured_wallets.add(wallet)
    effective_at = alpha.get("updated_at") or utc_now_iso()
    identity = {
        "source_sha256": source_sha256,
        "effective_at": effective_at,
        "score_run_id": score_run_id,
        "admitted": admitted,
    }
    return {
        "schema_version": 1,
        "kind": "wide_exact_policy_frozen_manifest",
        "flow_stage": "LEARN/OBSERVE/PROMOTE",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "manifest_id": stable_id("widemanifest", identity, length=32),
        "source_alpha_report": source_alpha_report,
        "source_alpha_sha256": source_sha256,
        "source_alpha_status": source_alpha_status,
        "source_alpha_age_h": (
            round(source_alpha_age_h, 6)
            if source_alpha_age_h is not None
            else None
        ),
        "source_alpha_freshness_limit_h": (
            round(freshness_limit_s / 3600.0, 6)
            if freshness_limit_s > 0
            else None
        ),
        "source_identity": {
            "source_artifact": source_alpha_report,
            "source_sha256": source_sha256,
            "source_status": alpha.get("status"),
            "source_timestamp": alpha.get("updated_at"),
            "fill_count": int(profile_summary.get("fills_total") or 0),
            "overlap_asset_count": int(alpha.get("asset_context_entries") or 0),
            "eligible_profile_count": len(eligible_profile_wallets),
            "eligible_profile_wallets": eligible_profile_wallets,
            "cohort_artifact": (cohort or {}).get("_source_path"),
            "cohort_generated_at": (cohort or {}).get("generated_at"),
            "cohort_live_ready_pick_count": len(cohort_live_ready),
            "intersection_required": cohort is not None,
        },
        "effective_at": effective_at,
        "score_run_id": score_run_id,
        "selection_rule": "completed current-source alpha PASS intersect paper cohort minus standing demotion/negative exclusions",
        "capture_selection_rule": "READY_QUEUE plus weekday-depth-ranked positive-copy-PnL wallets plus exact climb-priority/sticky-sole-focus paper-only inject; capture membership grants zero promotion authority",
        "fingerprint_slice_freeze": {
            "source_roster_drought_firing": bool(source_roster_drought_firing),
            "evidence_generated_at": (fingerprint_evidence or {}).get("generated_at"),
            "frozen_wallets": sorted(
                row["wallet"] for row in capture_watch if row.get("slice_freeze")
            ),
            "rule": "freeze per-wallet move_slice_keys to one immutable fingerprint while source-roster drought accrues",
        },
        "ready_queue_count": len(ready_rows),
        "excluded_wallets": sorted(excluded),
        "capture_watch_wallets": capture_watch,
        "promotion_admitted_wallets": admitted,
        "promotion_admitted_wallets_blocked_by": (
            "stale_alpha_source" if stale_alpha_source else None
        ),
        "admitted_wallets": admitted,
        "admitted_wallets_blocked_by": (
            "stale_alpha_source" if stale_alpha_source else None
        ),
        "refusal_census": census,
        "summary": {
            "ready_queue_wallets": len(ready_rows),
            "capture_watch_wallets": len(capture_watch),
            "promotion_admitted_wallets": len(admitted),
            "admitted_wallets": len(admitted),
            "refused_wallets": len(census) - len(admitted),
            "blocked_by": "stale_alpha_source" if stale_alpha_source else None,
            "order128_sticky_focus_authorized": sticky_focus_authorized,
        },
    }


def _order128_focus_authorized(
    packet: dict[str, Any], deadman: dict[str, Any]
) -> bool:
    """Authorize paper accrual only on a stable packet consumed at its exact cut."""

    binding = packet.get("binding_action") if isinstance(packet.get("binding_action"), dict) else {}
    stability = packet.get("rank_stability") if isinstance(packet.get("rank_stability"), dict) else {}
    return bool(
        packet.get("cut_consistent") is True
        and packet.get("deadman_checked_at")
        and packet.get("deadman_checked_at") == deadman.get("checked_at")
        and stability.get("stable_for_accrual") is True
        and int(stability.get("cuts_agreed") or 0) >= 2
        and (str(binding.get("wallet") or "").lower(), str(binding.get("wide_policy_fingerprint") or ""))
        in STICKY_PAPER_ACCRUAL_FOCUS
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alpha-report", required=True)
    parser.add_argument("--score-run-id", required=True)
    parser.add_argument("--queue", default="data/research/wallet_copy_full_universe_copyability_latest.json")
    parser.add_argument("--roster", default="data/research/wide_alpha_capture_roster_latest.json")
    parser.add_argument("--degrade", default="data/research/wallet_copy_active_set_auto_degrade_state.json")
    parser.add_argument("--cohort", default="data/research/wallet_market_cohort_replay_latest.json")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--fingerprint-evidence",
        default="data/research/wide_policy_fingerprint_evidence_latest.json",
    )
    parser.add_argument(
        "--order-flow-deadman-state",
        default="data/research/order_flow_deadman_state.json",
    )
    parser.add_argument(
        "--order128-packet",
        default="data/research/order128_fastest_lawful_path_latest.json",
    )
    args = parser.parse_args()
    if "_provisional" in Path(args.alpha_report).name:
        raise SystemExit("provisional alpha reports cannot seed a frozen manifest")
    alpha, raw = load_fresh_alpha_source(args.alpha_report)
    cohort = load_json(args.cohort, default={})
    if isinstance(cohort, dict):
        cohort = {**cohort, "_source_path": args.cohort}
    fingerprint_evidence = load_json(args.fingerprint_evidence, default={})
    deadman = load_json(args.order_flow_deadman_state, default={})
    order128_packet = load_json(args.order128_packet, default={})
    drought = (
        ((deadman.get("policy_choke") or {}).get("source_roster_drought") or {})
        if isinstance(deadman, dict)
        else {}
    )
    manifest = build_manifest(
        alpha=alpha,
        queue=load_json(args.queue, default={}),
        roster=load_json(args.roster, default={}),
        degrade=load_json(args.degrade, default={}),
        source_alpha_report=args.alpha_report,
        score_run_id=args.score_run_id,
        source_sha256=hashlib.sha256(raw).hexdigest(),
        cohort=cohort if isinstance(cohort, dict) else {},
        fingerprint_evidence=(
            fingerprint_evidence if isinstance(fingerprint_evidence, dict) else {}
        ),
        source_roster_drought_firing=bool(
            drought.get("firing")
            or str(drought.get("status") or "") == "INCIDENT_SOURCE_ROSTER_DROUGHT"
        ),
        sticky_focus_authorized=_order128_focus_authorized(
            order128_packet if isinstance(order128_packet, dict) else {},
            deadman if isinstance(deadman, dict) else {},
        ),
    )
    atomic_write_json(args.output, manifest)
    print(json.dumps({"output": args.output, "manifest_id": manifest["manifest_id"], **manifest["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
