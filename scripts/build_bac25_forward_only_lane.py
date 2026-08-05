#!/usr/bin/env python3
"""Register and summarize the forward-only bac25 exact-fingerprint paper lane."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, stable_id, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.venue_executability import (
    row_is_venue_executable,
    venue_gate_summary,
)

WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
FINGERPRINT = "bac25beda563430ef4f482544eccda250f4e3f7eb3bed91362fb70093dbc1fce"
POLICY_FAMILY = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
DEFAULT_GLOBAL_EVIDENCE = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_FORWARD_EVIDENCE = "data/research/bac25_forward_only_evidence_latest.json"
DEFAULT_ORDERS = "data/research/bac25_forward_only_orders.jsonl"
DEFAULT_MANIFEST = "data/research/wide_exact_policy_manifest_bac25_forward_only.json"
DEFAULT_OUTPUT = "data/research/bac25_forward_only_lane_latest.json"
DEFAULT_FAMILY_TERMINAL_REGISTRY = (
    "data/research/wide_policy_family_terminal_registry_latest.json"
)
OBSERVATION_WINDOW_S = 86_400


@dataclass(frozen=True)
class LaneSpec:
    wallet: str
    fingerprint: str
    policy_family: str
    score_run_id: str = "bac25_forward_only"
    lane_kind: str = "bac25_forward_only_paper_lane"
    observation_window_s: int = OBSERVATION_WINDOW_S


DEFAULT_SPEC = LaneSpec(WALLET, FINGERPRINT, POLICY_FAMILY)


def latest_resolution_batch_marginals(
    path: str | Path,
) -> tuple[float | None, float | None]:
    try:
        rows = [
            json.loads(line)
            for line in Path(path).read_text().splitlines()
            if line.strip()
        ]
    except (OSError, json.JSONDecodeError):
        return None, None
    filled_by_order = {
        str(row.get("order_id") or ""): row
        for row in rows
        if isinstance(row, dict)
        and row.get("event") == "wide_exact_policy_paper_order_filled"
    }
    resolved = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("event") == "wide_exact_policy_paper_order_resolved"
        and row.get("resolution_computed_at")
        and row.get("post_fee_pnl_usd") is not None
    ]
    if not resolved:
        return None, None
    latest_batch = str(resolved[-1]["resolution_computed_at"])
    batch_rows = [
        row
        for row in resolved
        if str(row.get("resolution_computed_at")) == latest_batch
    ]
    batch = [num(row["post_fee_pnl_usd"]) for row in batch_rows]
    venue_batch = [
        num(row["post_fee_pnl_usd"])
        for row in batch_rows
        if row_is_venue_executable(
            filled_by_order.get(str(row.get("order_id") or ""), {}),
            min_order_usd=1.0,
        )
    ]
    return (
        round(sum(batch) / len(batch), 6) if batch else None,
        round(sum(venue_batch) / len(venue_batch), 6) if venue_batch else None,
    )


def latest_resolution_batch_marginal(path: str | Path) -> float | None:
    return latest_resolution_batch_marginals(path)[0]


def _cell(
    evidence: dict[str, Any],
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any]:
    return next(
        (
            row
            for row in evidence.get("cells") or []
            if isinstance(row, dict)
            and str((row.get("identity") or {}).get("wallet") or "").lower()
            == spec.wallet
            and str(row.get("wide_policy_fingerprint") or "") == spec.fingerprint
        ),
        {},
    )


def register_manifest(
    global_evidence: dict[str, Any],
    *,
    registered_at: str,
    policy_family: str = POLICY_FAMILY,
    terminal_family_registry: list[dict[str, Any]] | None = None,
    terminal_family_registry_path: str | Path | None = None,
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any]:
    spec = LaneSpec(
        spec.wallet,
        spec.fingerprint,
        policy_family,
        spec.score_run_id,
        spec.lane_kind,
        spec.observation_window_s,
    )
    registry = terminal_family_registry
    if registry is None:
        registry_artifact = load_json(
            terminal_family_registry_path or DEFAULT_FAMILY_TERMINAL_REGISTRY,
            default={},
        )
        registry = (
            registry_artifact.get("entries")
            if isinstance(registry_artifact, dict)
            and isinstance(registry_artifact.get("entries"), list)
            else []
        )
    for prior in registry:
        if (
            str(prior.get("wide_policy_fingerprint") or "") == spec.fingerprint
            and prior.get("refuse_alias_reregistration") is True
        ):
            raise ValueError(
                "bac25 fingerprint already has a measured negative terminal verdict; "
                "refusing alias re-registration"
            )
    cell = _cell(global_evidence, spec)
    if not cell:
        raise ValueError("bac25 retrospective cell missing")
    identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
    venue = venue_gate_summary(cell)
    row = {
        "wallet": spec.wallet,
        "queue_rank": 0,
        "move_slice_keys": list(identity.get("move_slice_keys") or []),
        "wide_policy_fingerprint": spec.fingerprint,
        "slice_freeze": {
            "status": "FROZEN_FORWARD_ONLY_PAPER_LANE",
            "reason": "fable_2026-07-30T08:34:00Z_forward_only_zero_retrospective_credit",
            "retrospective_n": int(venue.get("resolved") or 0),
            "forward_n": 0,
            "retrospective_and_forward_may_be_summed": False,
        },
        "promotion_authority": False,
        "paper_measurement_only": True,
        "forward_only": True,
    }
    manifest_id = stable_id(
        "widemanifest",
        {"fingerprint": spec.fingerprint, "registered_at": registered_at},
        length=32,
    )
    deadline_at = (
        datetime.fromisoformat(registered_at.replace("Z", "+00:00"))
        + timedelta(seconds=spec.observation_window_s)
    ).isoformat()
    return {
        "schema_version": 1,
        "kind": "wide_exact_policy_frozen_manifest",
        "flow_stage": "LEARN/OBSERVE/PROMOTE",
        "generated_at": registered_at,
        "effective_at": registered_at,
        "observation_window_s": spec.observation_window_s,
        "observation_deadline_at": deadline_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "manifest_id": manifest_id,
        "score_run_id": spec.score_run_id,
        "policy_family": policy_family,
        "capture_watch_wallets": [row],
        "promotion_admitted_wallets": [],
        "admitted_wallets": [],
        "forward_only_gate": {
            "forward_n_min": 200,
            "both_halves_pnl_excluding_top_1_market_positive": True,
            "venue_reachable_share_pct_min": 40.0,
            "retrospective_credit_allowed": False,
        },
    }


def negative_family_outcome(
    forward_n: int,
    forward_pnl: Any,
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any] | None:
    if num(forward_pnl) >= 0:
        return None
    status = (
        "MEASURED_NEGATIVE_FULL_SAMPLE"
        if forward_n >= 200
        else "MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE"
    )
    return {
        "policy_family": spec.policy_family,
        "wide_policy_fingerprint": spec.fingerprint,
        "status": status,
        "sample_branch": "n>=200" if forward_n >= 200 else "n<200",
        "terminal": True,
        "stop_writer": True,
        "refuse_alias_reregistration": True,
        "promotion_authority": False,
        "live_authority": False,
    }


def family_terminal_outcome(
    state: dict[str, Any],
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any]:
    checks = state.get("checks") if isinstance(state.get("checks"), dict) else {}
    failed_checks = [name for name, passed in checks.items() if passed is not True]
    negative = negative_family_outcome(
        int(state.get("forward_n") or 0),
        state.get("forward_post_fee_pnl_usd"),
        spec,
    )
    if negative is not None:
        return {
            **negative,
            "reason_source": failed_checks or ["forward_post_fee_pnl_usd"],
        }
    if int(state.get("forward_n") or 0) == 0:
        return {
            "policy_family": spec.policy_family,
            "wide_policy_fingerprint": spec.fingerprint,
            "status": "PARK_ZERO_INTENT_GENERATION",
            "condition": "forward_n==0 at observation deadline",
            "reason_source": ["forward_n_eq_0"],
            "terminal": True,
            "stop_writer": True,
            "deadline_extension_allowed": False,
            "refuse_alias_reregistration": True,
            "promotion_authority": False,
            "live_authority": False,
        }
    if int(state.get("forward_n") or 0) < 200:
        return {
            "policy_family": spec.policy_family,
            "wide_policy_fingerprint": spec.fingerprint,
            "status": "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE",
            "condition": "forward_post_fee_pnl_usd>=0 and forward_n<200",
            "reason_source": failed_checks or ["forward_n_gte_200"],
            "terminal": True,
            "stop_writer": True,
            "refuse_alias_reregistration": True,
            "promotion_authority": False,
            "live_authority": False,
        }
    if state.get("admission_eligible") is not True:
        return {
            "policy_family": spec.policy_family,
            "wide_policy_fingerprint": spec.fingerprint,
            "status": "MEASURED_INADMISSIBLE_CONCENTRATION",
            "condition": "forward_post_fee_pnl_usd>=0 and admission_eligible false",
            "reason_source": failed_checks or ["admission_eligible"],
            "terminal": True,
            "stop_writer": True,
            "refuse_alias_reregistration": True,
            "promotion_authority": False,
            "live_authority": False,
        }
    return {
        "policy_family": spec.policy_family,
        "wide_policy_fingerprint": spec.fingerprint,
        "status": "MEASURED_ADMISSIBLE",
        "condition": "forward_post_fee_pnl_usd>=0 and admission_eligible true",
        "reason_source": [],
        "terminal": True,
        "stop_writer": True,
        "refuse_alias_reregistration": True,
        "promotion_authority": False,
        "live_authority": False,
    }


def update_family_terminal_registry(
    registry: dict[str, Any],
    state: dict[str, Any],
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any]:
    outcome = family_terminal_outcome(state, spec)
    generated = datetime.fromisoformat(
        str(state.get("generated_at") or "").replace("Z", "+00:00")
    )
    deadline = datetime.fromisoformat(
        str(state.get("observation_deadline_at") or "").replace("Z", "+00:00")
    )
    due = generated >= deadline
    reason_source = outcome.get("reason_source") or []
    if due and state.get("admission_eligible") is not True and not reason_source:
        checks = state.get("checks") if isinstance(state.get("checks"), dict) else {}
        reason_source = [
            name for name, passed in checks.items() if passed is not True
        ] or ["admission_eligible"]
    entries = [
        row
        for row in registry.get("entries") or []
        if isinstance(row, dict)
        and str(row.get("wide_policy_fingerprint") or "") != spec.fingerprint
    ]
    entries.append(
        {
            "policy_family": spec.policy_family,
            "wide_policy_fingerprint": spec.fingerprint,
            "score_run_id": state.get("score_run_id"),
            "status": (
                outcome["status"] if due else "REGISTERED_ACTIVE"
            ),
            "terminal": due,
            "stop_writer": due,
            "refuse_alias_reregistration": True,
            "promotion_authority": False,
            "live_authority": False,
            "precommitted_negative_outcomes": {
                "insufficient_sample": negative_family_outcome(199, -1.0, spec),
                "full_sample": negative_family_outcome(200, -1.0, spec),
            },
            "precommitted_nonnegative_outcomes": {
                "zero_intent_generation": "PARK_ZERO_INTENT_GENERATION",
                "insufficient_sample": "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE",
                "concentration": "MEASURED_INADMISSIBLE_CONCENTRATION",
                "admissible": "MEASURED_ADMISSIBLE",
            },
            "reason_source": reason_source if due else [],
            "headline_margin_rows": state.get("headline_margin_rows"),
            "boundary_proximity": state.get("boundary_proximity"),
            "marginal_basis": state.get("marginal_basis"),
            "headline_margin_rows_basis": state.get(
                "headline_margin_rows_basis"
            ),
            "marginal_pnl_per_row_venue_executable": state.get(
                "marginal_pnl_per_row_venue_executable"
            ),
            "headline_margin_rows_venue_executable": state.get(
                "headline_margin_rows_venue_executable"
            ),
            "headline_margin_rows_venue_executable_basis": state.get(
                "headline_margin_rows_venue_executable_basis"
            ),
            "effective_at": state.get("observation_deadline_at"),
            "evidence_generated_at": state.get("generated_at"),
        }
    )
    return {
        "schema_version": 1,
        "kind": "wide_policy_family_terminal_registry",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": state.get("generated_at"),
        "paper_only": True,
        "live_orders_allowed": False,
        "entries": entries,
    }


def ensure_manifest_clock(
    manifest: dict[str, Any],
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any]:
    """Anchor a missing clock to the immutable original t0 without extension."""
    terminal_outcome = {
        "status": "PARK_FORWARD_EVIDENCE_REFUSED",
        "terminal": True,
        "stop_writer": True,
        "reason_source": "lane.refusal_reasons_at_deadline",
        "deadline_extension_allowed": False,
        "promotion_authority": False,
        "live_authority": False,
        "policy_family_outcomes_at_deadline": {
            "zero_intent_generation": {
                **family_terminal_outcome(
                    {
                        "forward_n": 0,
                        "forward_post_fee_pnl_usd": 0,
                        "admission_eligible": False,
                        "checks": {"forward_n_gte_200": False},
                    },
                    spec,
                ),
            },
            "insufficient_sample": {
                **negative_family_outcome(199, -1.0, spec),
                "condition": "forward_post_fee_pnl_usd<0 and forward_n<200",
            },
            "full_sample": {
                **negative_family_outcome(200, -1.0, spec),
                "condition": "forward_post_fee_pnl_usd<0 and forward_n>=200",
            },
            "nonnegative_inadmissible": {
                "status": "MEASURED_INADMISSIBLE_CONCENTRATION",
                "condition": (
                    "forward_post_fee_pnl_usd>=0 and forward_n>=200 "
                    "and admission_eligible false"
                ),
                "reason_source": "lane.checks",
                "terminal": True,
                "stop_writer": True,
                "refuse_alias_reregistration": True,
                "promotion_authority": False,
                "live_authority": False,
            },
            "nonnegative_insufficient_sample": {
                "status": "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE",
                "condition": "forward_post_fee_pnl_usd>=0 and forward_n<200",
                "reason_source": "lane.checks",
                "terminal": True,
                "stop_writer": True,
                "refuse_alias_reregistration": True,
                "promotion_authority": False,
                "live_authority": False,
            },
            "nonnegative_admissible": {
                "status": "MEASURED_ADMISSIBLE",
                "condition": (
                    "forward_post_fee_pnl_usd>=0 and admission_eligible true"
                ),
                "terminal": True,
                "stop_writer": True,
                "refuse_alias_reregistration": True,
                "promotion_authority": False,
                "live_authority": False,
            },
        },
    }
    if (
        manifest.get("observation_window_s") == spec.observation_window_s
        and manifest.get("observation_deadline_at")
    ):
        return {
            **manifest,
            "score_run_id": spec.score_run_id,
            "policy_family": str(
                manifest.get("policy_family") or spec.policy_family
            ),
            "terminal_outcome_on_deadline": terminal_outcome,
        }
    registered_at = str(manifest.get("effective_at") or "")
    if not registered_at:
        raise ValueError("bac25 immutable effective_at missing; refusing to re-clock")
    registered = datetime.fromisoformat(registered_at.replace("Z", "+00:00"))
    return {
        **manifest,
        "score_run_id": spec.score_run_id,
        "policy_family": str(manifest.get("policy_family") or spec.policy_family),
        "observation_window_s": spec.observation_window_s,
        "observation_deadline_at": (
            registered + timedelta(seconds=spec.observation_window_s)
        ).isoformat(),
        "terminal_outcome_on_deadline": terminal_outcome,
    }


def refuse_observation_window_change_after_accrual(
    manifest: dict[str, Any],
    previous_state: dict[str, Any],
    spec: LaneSpec = DEFAULT_SPEC,
) -> None:
    previous_n = int(previous_state.get("forward_n") or 0)
    current_window_raw = manifest.get("observation_window_s")
    if previous_n > 0 and current_window_raw is None:
        raise ValueError(
            "FORWARD_WINDOW_CHANGE_REFUSED_AFTER_ACCRUAL: "
            f"forward_n={previous_n} current_window_s=missing "
            f"requested_window_s={spec.observation_window_s}"
        )
    current_window = int(current_window_raw or spec.observation_window_s)
    if previous_n > 0 and current_window != spec.observation_window_s:
        raise ValueError(
            "FORWARD_WINDOW_CHANGE_REFUSED_AFTER_ACCRUAL: "
            f"forward_n={previous_n} current_window_s={current_window} "
            f"requested_window_s={spec.observation_window_s}"
        )


def build_state(
    *,
    manifest: dict[str, Any],
    global_evidence: dict[str, Any],
    forward_evidence: dict[str, Any],
    generated_at: str,
    previous_state: dict[str, Any] | None = None,
    observed_marginal_pnl_per_row: float | None = None,
    observed_venue_marginal_pnl_per_row: float | None = None,
    spec: LaneSpec = DEFAULT_SPEC,
) -> dict[str, Any]:
    retrospective = venue_gate_summary(_cell(global_evidence, spec))
    manifest_member = next(
        (
            row
            for row in manifest.get("capture_watch_wallets") or []
            if isinstance(row, dict)
            and str(row.get("wide_policy_fingerprint") or "") == spec.fingerprint
        ),
        {},
    )
    slice_freeze = (
        manifest_member.get("slice_freeze")
        if isinstance(manifest_member.get("slice_freeze"), dict)
        else {}
    )
    retrospective_n_at_t0 = int(slice_freeze.get("retrospective_n") or 0)
    forward_cell = _cell(forward_evidence, spec)
    forward = venue_gate_summary(forward_cell) if forward_cell else {}
    forward_n = int(forward.get("resolved") or 0)
    halves = (
        forward.get("half_pnl_excluding_top_1_market")
        if isinstance(forward.get("half_pnl_excluding_top_1_market"), dict)
        else {}
    )
    first_positive = num(halves.get("first_half")) > 0
    second_positive = num(halves.get("second_half")) > 0
    reachable = forward.get("venue_reachable_share_pct")
    effective_at = str(manifest.get("effective_at") or "")
    if not effective_at:
        raise ValueError("bac25 immutable effective_at missing; refusing to score")
    registered = datetime.fromisoformat(effective_at.replace("Z", "+00:00"))
    generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    elapsed_s = max(0.0, (generated - registered).total_seconds())
    projected_forward_n = round(
        forward_n * spec.observation_window_s / elapsed_s,
        6,
    ) if elapsed_s > 0 else 0.0
    checks = {
        "forward_n_gte_200": forward_n >= 200,
        "forward_first_half_pnl_excluding_top_1_market_positive": first_positive,
        "forward_second_half_pnl_excluding_top_1_market_positive": second_positive,
        "forward_venue_reachable_share_gte_40pct": (
            reachable is not None and num(reachable) >= 40.0
        ),
        "retrospective_credit_zero": True,
    }
    substantive_checks = {
        name: passed
        for name, passed in checks.items()
        if name != "forward_n_gte_200"
    }
    forward_post_fee_pnl_usd = forward.get("post_fee_pnl_usd")
    top_1_market_contribution_usd = round(
        num(forward_post_fee_pnl_usd)
        - num(halves.get("first_half"))
        - num(halves.get("second_half")),
        6,
    )
    previous = previous_state if isinstance(previous_state, dict) else {}
    previous_n = int(previous.get("forward_n") or 0)
    row_delta = forward_n - previous_n
    marginal_pnl_per_row = (
        round(
            (
                num(forward_post_fee_pnl_usd)
                - num(previous.get("forward_post_fee_pnl_usd"))
            )
            / row_delta,
            6,
        )
        if row_delta > 0 and previous.get("forward_post_fee_pnl_usd") is not None
        else observed_marginal_pnl_per_row
        if observed_marginal_pnl_per_row is not None
        else previous.get("marginal_pnl_per_row")
    )
    headline_margin_rows = (
        round(num(forward_post_fee_pnl_usd) / abs(num(marginal_pnl_per_row)), 6)
        if marginal_pnl_per_row is not None and num(marginal_pnl_per_row) != 0
        else None
    )
    boundary_proximity = (
        abs(headline_margin_rows) < 5 if headline_margin_rows is not None else False
    )
    headline_margin_rows_venue_executable = (
        round(
            num(forward_post_fee_pnl_usd)
            / abs(num(observed_venue_marginal_pnl_per_row)),
            6,
        )
        if observed_venue_marginal_pnl_per_row is not None
        and num(observed_venue_marginal_pnl_per_row) != 0
        else None
    )
    refusal_reasons = []
    if projected_forward_n < 200:
        refusal_reasons.append("PROJECTED_EXPIRY_WITHOUT_EVIDENCE")
    if num(forward_post_fee_pnl_usd) < 0:
        refusal_reasons.append("NEGATIVE_FORWARD_PNL")
    if not all(substantive_checks.values()):
        refusal_reasons.append("FAILING_SUBSTANTIVE_CHECKS")
    projection_status = (
        "PROJECTED_EXPIRY_WITHOUT_EVIDENCE_AND_NEGATIVE_FORWARD_PNL"
        if "PROJECTED_EXPIRY_WITHOUT_EVIDENCE" in refusal_reasons
        and "NEGATIVE_FORWARD_PNL" in refusal_reasons
        else
        "PROJECTED_EXPIRY_WITHOUT_EVIDENCE"
        if projected_forward_n < 200
        else "PROJECTED_TO_REACH_N_WITH_NEGATIVE_FORWARD_PNL"
        if num(forward_post_fee_pnl_usd) < 0
        else "PROJECTED_TO_REACH_N_WITH_FAILING_SUBSTANTIVE_CHECKS"
        if not all(substantive_checks.values())
        else "PROJECTED_TO_REACH_EVIDENCE_GATE"
    )
    return {
        "schema_version": 1,
        "kind": spec.lane_kind,
        "flow_stage": "LEARN/OBSERVE/PROMOTE",
        "generated_at": generated_at,
        "registered_at": manifest.get("generated_at"),
        "observation_window_s": manifest.get("observation_window_s"),
        "observation_deadline_at": manifest.get("observation_deadline_at"),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallet": spec.wallet,
        "wide_policy_fingerprint": spec.fingerprint,
        "policy_family": str(manifest.get("policy_family") or spec.policy_family),
        "manifest_id": manifest.get("manifest_id"),
        "score_run_id": manifest.get("score_run_id"),
        "decision_variable": "forward_half_pnl_excluding_top_1_market",
        "headline_sign_reliability": (
            "UNRELIABLE_BELOW_REQUIRED_N"
            if forward_n < 200
            else "REQUIRED_N_REACHED"
        ),
        "top_1_market_contribution_usd": top_1_market_contribution_usd,
        "retrospective_n": retrospective_n_at_t0,
        "retrospective_post_fee_pnl_usd": retrospective.get("post_fee_pnl_usd"),
        "retrospective_roi_pct": retrospective.get("roi_pct"),
        "retrospective_is_admission_input": False,
        "forward_n": forward_n,
        "forward_post_fee_pnl_usd": forward_post_fee_pnl_usd,
        "marginal_pnl_per_row": marginal_pnl_per_row,
        "marginal_basis": "unfiltered_journal_batch",
        "headline_margin_rows": headline_margin_rows,
        "headline_margin_rows_basis": (
            "venue_executable_headline_over_unfiltered_journal_batch_marginal"
        ),
        "marginal_pnl_per_row_venue_executable": (
            observed_venue_marginal_pnl_per_row
        ),
        "headline_margin_rows_venue_executable": (
            headline_margin_rows_venue_executable
        ),
        "headline_margin_rows_venue_executable_basis": (
            "venue_executable_headline_over_venue_executable_newest_batch_marginal"
        ),
        "boundary_proximity": boundary_proximity,
        "forward_roi_pct": forward.get("roi_pct"),
        "forward_half_pnl_excluding_top_1_market": halves,
        "forward_venue_reachable_share_pct": reachable,
        "forward_evidence_projection": {
            "status": projection_status,
            "refusal_reasons": refusal_reasons,
            "forward_n_now": forward_n,
            "required_forward_n": 200,
            "elapsed_s": round(elapsed_s, 6),
            "total_window_s": spec.observation_window_s,
            "projected_forward_n_at_deadline": projected_forward_n,
            "substantive_checks": substantive_checks,
            "admission_forecast": False,
            "cause": (
                "projected sample size expires below n=200 and forward post-fee "
                "PnL is negative"
                if projection_status
                == "PROJECTED_EXPIRY_WITHOUT_EVIDENCE_AND_NEGATIVE_FORWARD_PNL"
                else "forward venue-executable resolutions accruing below the rate "
                "required to reach n=200 before the preregistered deadline"
                if projection_status == "PROJECTED_EXPIRY_WITHOUT_EVIDENCE"
                else "projected sample size reaches n=200 but forward post-fee "
                "PnL is negative"
                if projection_status
                == "PROJECTED_TO_REACH_N_WITH_NEGATIVE_FORWARD_PNL"
                else "projected sample size reaches n=200 but one or more "
                "substantive admission checks are failing"
                if projection_status
                == "PROJECTED_TO_REACH_N_WITH_FAILING_SUBSTANTIVE_CHECKS"
                else None
            ),
            "projection_rule": (
                "forward_n_now * total_window_s / elapsed_s; "
                "retrospective rows receive zero credit"
            ),
        },
        "retrospective_and_forward_may_be_summed": False,
        "checks": checks,
        "admission_eligible": all(checks.values()),
        "live_authority": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--global-evidence", default=DEFAULT_GLOBAL_EVIDENCE)
    parser.add_argument("--forward-evidence", default=DEFAULT_FORWARD_EVIDENCE)
    parser.add_argument("--orders", default=DEFAULT_ORDERS)
    parser.add_argument("--wallet", default=WALLET)
    parser.add_argument("--fingerprint", default=FINGERPRINT)
    parser.add_argument("--policy-family", default=POLICY_FAMILY)
    parser.add_argument("--score-run-id", default=DEFAULT_SPEC.score_run_id)
    parser.add_argument("--lane-kind", default=DEFAULT_SPEC.lane_kind)
    parser.add_argument(
        "--observation-window-s",
        type=int,
        default=DEFAULT_SPEC.observation_window_s,
    )
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--family-terminal-registry",
        default=DEFAULT_FAMILY_TERMINAL_REGISTRY,
    )
    args = parser.parse_args()
    spec = LaneSpec(
        str(args.wallet).lower(),
        str(args.fingerprint),
        str(args.policy_family),
        str(args.score_run_id),
        str(args.lane_kind),
        int(args.observation_window_s),
    )
    global_evidence = load_json(args.global_evidence, default={})
    manifest = load_json(args.manifest, default={})
    previous_state = load_json(args.output, default={})
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    if not isinstance(manifest, dict) or manifest.get("kind") != "wide_exact_policy_frozen_manifest":
        manifest = ensure_manifest_clock(
            register_manifest(
                global_evidence if isinstance(global_evidence, dict) else {},
                registered_at=utc_now_iso(),
                policy_family=spec.policy_family,
                terminal_family_registry_path=args.family_terminal_registry,
                spec=spec,
            ),
            spec,
        )
        atomic_write_json(args.manifest, manifest)
    else:
        refuse_observation_window_change_after_accrual(
            manifest,
            previous_state,
            spec,
        )
        clocked_manifest = ensure_manifest_clock(manifest, spec)
        if clocked_manifest != manifest:
            manifest = clocked_manifest
            atomic_write_json(args.manifest, manifest)
    journal_marginal, venue_marginal = latest_resolution_batch_marginals(
        args.orders
    )
    state = build_state(
        manifest=manifest,
        global_evidence=global_evidence if isinstance(global_evidence, dict) else {},
        forward_evidence=load_json(args.forward_evidence, default={}),
        generated_at=utc_now_iso(),
        previous_state=previous_state,
        observed_marginal_pnl_per_row=journal_marginal,
        observed_venue_marginal_pnl_per_row=venue_marginal,
        spec=spec,
    )
    atomic_write_json(args.output, state)
    registry = load_json(args.family_terminal_registry, default={})
    atomic_write_json(
        args.family_terminal_registry,
        update_family_terminal_registry(
            registry if isinstance(registry, dict) else {},
            state,
            spec,
        ),
    )
    print(json.dumps(state, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
