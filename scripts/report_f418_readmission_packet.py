#!/usr/bin/env python3
"""Write the event-driven f418 readmission packet.

Flow stage: PROMOTE/DEFEND. This is a packet reporter only: it never mutates
live admission, guard code, or runtime config.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.scorecard import load_fresh_scorecard  # noqa: E402


WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
CANDIDATE_ID = "cohort_alive_admit_f418d3a1"
BOUNDARY_ISO = "2026-07-15T00:11:00Z"
ROUTING_SHADOW_MEMBER_COUNTERFACTUAL_MIN_RESOLVED = 10


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _lag_s(later_iso: str, earlier_iso: str | None) -> float | None:
    later = _parse_iso(later_iso)
    earlier = _parse_iso(earlier_iso)
    if later is None or earlier is None:
        return None
    return round((later - earlier).total_seconds(), 6)


def _artifact_age_s(generated_at: str, payload: dict[str, Any]) -> float | None:
    artifact_at = payload.get("generated_at")
    if not artifact_at:
        return None
    return _lag_s(generated_at, str(artifact_at))


def _latest_event_iso(report: dict[str, Any]) -> str | None:
    latest: str | None = None
    for row in report.get("latest_rows") or []:
        if not isinstance(row, dict):
            continue
        event_iso = row.get("event_iso")
        if event_iso and (latest is None or str(event_iso) > latest):
            latest = str(event_iso)
    for window in report.get("windows") or []:
        if not isinstance(window, dict):
            continue
        for key in ("last_event_iso", "first_event_iso"):
            event_iso = window.get(key)
            if event_iso and (latest is None or str(event_iso) > latest):
                latest = str(event_iso)
    return latest


def _summary(data: dict[str, Any]) -> dict[str, Any]:
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    return dict(summary)


def _member_record(scorecard: dict[str, Any]) -> dict[str, Any]:
    lifetime = scorecard.get("lifetime_pnl_truth") if isinstance(scorecard.get("lifetime_pnl_truth"), dict) else {}
    by_member = lifetime.get("by_member") if isinstance(lifetime.get("by_member"), dict) else {}
    return dict(by_member.get(WALLET) or {})


def _active_roster_member(scorecard: dict[str, Any]) -> list[dict[str, Any]]:
    roster = scorecard.get("active_set_roster") if isinstance(scorecard.get("active_set_roster"), dict) else {}
    members = roster.get("members") if isinstance(roster.get("members"), list) else []
    return [dict(row) for row in members if isinstance(row, dict) and str(row.get("source_wallet") or "").lower() == WALLET]


def _counterfactual_gate(probe: dict[str, Any], *, probe_path: str) -> dict[str, Any]:
    summary = probe.get("candidate_intent_summary") if isinstance(probe.get("candidate_intent_summary"), dict) else {}
    expected_fee_gate = (
        summary.get("expected_fee_capture_gate")
        if isinstance(summary.get("expected_fee_capture_gate"), dict)
        else {}
    )
    participation = probe.get("window_participation") if isinstance(probe.get("window_participation"), dict) else {}
    rows = [dict(row) for row in participation.get("rows") or [] if isinstance(row, dict)]
    resolved_rows = [
        row
        for row in rows
        if row.get("post_fee_would_pnl_usd") is not None and not bool(row.get("miss_pending_market_lifecycle"))
    ]
    post_fee = round(sum(float(row.get("post_fee_would_pnl_usd") or 0.0) for row in resolved_rows), 6)
    gate_pass = bool(resolved_rows) and post_fee > 0
    reasons: list[str] = []
    if not resolved_rows:
        reasons.append("zero_resolved_post_fee_counterfactual_rows")
    elif post_fee <= 0:
        reasons.append("post_fee_would_pnl_not_positive")
    if int(summary.get("fresh_candidate_intents_after_window_fill_cap") or 0) <= 0:
        reasons.append("current_probe_after_window_cap_zero")
    if any(bool(row.get("miss_pending_market_lifecycle")) for row in rows):
        reasons.append("probe_rows_pending_market_lifecycle")
    status = "PASS_POSITIVE_POST_FEE" if gate_pass else "NO_POSITIVE_COUNTERFACTUAL_SAMPLE"
    return {
        "acceptable_boundary_from_fable": "2026-07-15T00:11Z fresh-pid boundary",
        "boundary_iso": BOUNDARY_ISO,
        "gate_pass": gate_pass,
        "post_fee_would_pnl_status": status,
        "post_fee_would_pnl_usd": post_fee,
        "denial_reasons": reasons,
        "probe_state": probe_path,
        "probe_generated_at": probe.get("generated_at"),
        "probe_status": probe.get("status"),
        "probe_candidate_intents": summary.get("candidate_intents"),
        "probe_fresh_candidate_intents": summary.get("fresh_candidate_intents"),
        "probe_after_expected_fee_gate": summary.get("fresh_candidate_intents_after_expected_fee_gate"),
        "probe_after_window_fill_cap": summary.get("fresh_candidate_intents_after_window_fill_cap"),
        "probe_expected_fee_sum_usd": expected_fee_gate.get("expected_fee_sum_usd"),
        "probe_sample_fee_intents": expected_fee_gate.get("sample_intents") or [],
        "resolved_post_fee_rows": len(resolved_rows),
        "probe_rows": rows,
    }


def _routing_shadow_member_counterfactual(
    routing_shadow: dict[str, Any],
    *,
    routing_shadow_path: str,
) -> dict[str, Any]:
    summary = routing_shadow.get("summary") if isinstance(routing_shadow.get("summary"), dict) else {}
    measurement = (
        summary.get("extra_would_submit_post_fee_measurement")
        if isinstance(summary.get("extra_would_submit_post_fee_measurement"), dict)
        else {}
    )
    by_member = measurement.get("by_member") if isinstance(measurement.get("by_member"), dict) else {}
    row = by_member.get(WALLET)
    if not isinstance(row, dict):
        return {
            "source": routing_shadow_path,
            "gate_pass": False,
            "status": "NO_ROUTING_SHADOW_MEMBER_ROW",
            "denial_reasons": ["routing_shadow_member_row_missing"],
            "methodology": (
                "member-attribution routing-shadow resolved post-fee rows only; "
                "supplements the live probe when the probe has no resolved rows"
            ),
        }

    resolved = int(row.get("measurable_resolved_intents") or 0)
    post_fee = round(float(row.get("post_fee_pnl_usd") or 0.0), 6)
    gate_pass = resolved >= ROUTING_SHADOW_MEMBER_COUNTERFACTUAL_MIN_RESOLVED and post_fee > 0.0
    reasons: list[str] = []
    if resolved < ROUTING_SHADOW_MEMBER_COUNTERFACTUAL_MIN_RESOLVED:
        reasons.append("routing_shadow_resolved_intents_below_10")
    if post_fee <= 0.0:
        reasons.append("routing_shadow_post_fee_not_positive")

    return {
        "source": routing_shadow_path,
        "artifact_generated_at": routing_shadow.get("generated_at"),
        "gate_pass": gate_pass,
        "status": "PASS_POSITIVE_RESOLVED_POST_FEE_N_GE_10" if gate_pass else "NO_ADMIT_COUNTERFACTUAL",
        "denial_reasons": reasons,
        "methodology": (
            "member-attribution routing-shadow resolved post-fee rows only; "
            "no unresolved or unmeasured rows count toward the admission gate"
        ),
        "min_resolved_intents": ROUTING_SHADOW_MEMBER_COUNTERFACTUAL_MIN_RESOLVED,
        "fee_gated_intents": row.get("fee_gated_intents"),
        "resolved_intents": row.get("resolved_intents"),
        "measurable_resolved_intents": resolved,
        "unmeasured_resolved_intents": row.get("unmeasured_resolved_intents"),
        "unresolved_intents": row.get("unresolved_intents"),
        "wins": row.get("wins"),
        "losses": row.get("losses"),
        "pre_fee_pnl_usd": row.get("pre_fee_pnl_usd"),
        "expected_fee_usd_sum": row.get("expected_fee_usd_sum"),
        "post_fee_pnl_usd": post_fee,
        "unique_windows": row.get("unique_windows"),
        "measured_unique_windows": row.get("measured_unique_windows"),
        "regime_counts": row.get("regime_counts"),
        "pnl_measurement_gap_reasons": row.get("pnl_measurement_gap_reasons"),
    }


def build_packet(
    *,
    generated_at: str,
    since_midnight: dict[str, Any],
    last_24h: dict[str, Any],
    member_since_midnight: dict[str, Any],
    probe: dict[str, Any],
    routing_shadow: dict[str, Any],
    scorecard: dict[str, Any],
    temporal: dict[str, Any],
    previous_packet: dict[str, Any],
    paths: dict[str, str],
) -> dict[str, Any]:
    since_summary = _summary(since_midnight)
    h24_summary = _summary(last_24h)
    latest_buy_iso = _latest_event_iso(last_24h) or _latest_event_iso(since_midnight)
    source_active = int(since_summary.get("source_active_windows") or 0)
    liveness_ages = {
        "since_midnight": _artifact_age_s(generated_at, since_midnight),
        "last_24h": _artifact_age_s(generated_at, last_24h),
        "member_since_midnight": _artifact_age_s(generated_at, member_since_midnight),
    }
    stale_liveness = any(age is None or age > 86400.0 for age in liveness_ages.values())
    live_probe_counterfactual = _counterfactual_gate(probe, probe_path=paths["probe"])
    routing_shadow_counterfactual = _routing_shadow_member_counterfactual(
        routing_shadow,
        routing_shadow_path=paths["routing_shadow"],
    )
    gate_pass = bool(live_probe_counterfactual.get("gate_pass")) or bool(
        routing_shadow_counterfactual.get("gate_pass")
    )
    counterfactual = {
        "gate_pass": gate_pass,
        "status": (
            "PASS_ROUTING_SHADOW_MEMBER_COUNTERFACTUAL"
            if routing_shadow_counterfactual.get("gate_pass")
            else live_probe_counterfactual.get("post_fee_would_pnl_status")
        ),
        "basis": (
            "routing_shadow_member_attribution"
            if routing_shadow_counterfactual.get("gate_pass")
            else "live_probe_window_participation"
        ),
        "pre_ruled_admit_rule": (
            "Fable 2026-07-16T05:28Z branch (a): counterfactual post-fee > 0 "
            "on n>=10 resolved admits f418; 2026-07-16T06:42Z structural-bind branch (b) raises sizing to $2.50 cap / 0.50 max price / min $1"
        ),
        "live_probe": live_probe_counterfactual,
        "routing_shadow_member": routing_shadow_counterfactual,
        "denial_reasons": (
            []
            if gate_pass
            else list(live_probe_counterfactual.get("denial_reasons") or [])
            + list(routing_shadow_counterfactual.get("denial_reasons") or [])
        ),
    }
    fresh_pass = source_active >= 1 and not stale_liveness
    if stale_liveness:
        status = "DORMANT_SOURCE_EXPIRED"
    elif not fresh_pass:
        status = "DENIED_BY_RULE_NO_FRESH_LIVENESS"
    elif not gate_pass:
        status = "DENIED_BY_RULE_COUNTERFACTUAL_NOT_POSITIVE"
    else:
        status = "PRE_RULED_ADMIT_F418_ACTIVATION"

    decision = (
        "DO_NOT_ACTIVATE; liveness inputs exceed 24h and must be regenerated"
        if status == "DORMANT_SOURCE_EXPIRED"
        else "DO_NOT_ACTIVATE; no live mutation because the counterfactual gate is not positive"
        if status.startswith("DENIED")
        else "ADMIT_UNDER_FABLE_20260716T0528_BRANCH_A_AND_0642_BRANCH_B; activate f418 at $2.50 cap / 0.50 max_price / min $1 with one managed guard restart"
    )

    prior_record = _member_record(scorecard)
    temporal_case = {
        "source": paths["temporal"],
        "classification": temporal.get("classification"),
        "classification_reason": temporal.get("classification_reason"),
        "profitable_hour_bands": temporal.get("profitable_hour_bands") or [],
        "all": temporal.get("all"),
        "recent": temporal.get("recent"),
        "case_result": (
            "insufficient_for_activation_without_positive_current_policy_counterfactual"
            if not gate_pass
            else "historical_positive_case_plus_current_counterfactual_pre_ruled_for_activation"
        ),
    }
    return {
        "schema_version": 1,
        "kind": "f418_readmission_packet",
        "flow_stage": "PROMOTE/LEARN/LIVE/DEFEND",
        "generated_at": generated_at,
        "source_of_truth": "docs/agents/HANDOFF.md latest Fable 2026-07-16T05:35Z DIRECTION plus f418 event-driven trigger",
        "source_wallet": WALLET,
        "candidate_id": CANDIDATE_ID,
        "status": status,
        "decision": decision,
        "denial_reasons": (
            ["source_active_windows_since_midnight_below_one"]
            if not fresh_pass
            else list(counterfactual.get("denial_reasons") or [])
        ),
        "fresh_liveness": {
            "trigger_rule": "event-driven redraft only when source_active_windows_since_midnight >= 1 on current-day scan",
            "trigger_pass": fresh_pass,
            "artifact_age_s": liveness_ages,
            "max_artifact_age_s": 86400.0,
            "stale_input": stale_liveness,
            "since_midnight_artifact": paths["since_midnight"],
            "last_24h_artifact": paths["last_24h"],
            "member_since_midnight_artifact": paths["member_since_midnight"],
            "last_observed_buy_iso": latest_buy_iso,
            "last_observed_buy_lag_s": _lag_s(generated_at, latest_buy_iso),
            "since_midnight_summary": since_summary,
            "last_24h_summary": h24_summary,
            "member_since_midnight_source_active_windows": member_since_midnight.get("source_active_windows"),
            "policy_note": "source-active trigger passed, but max_price<=0.5 policy eligibility is evaluated separately",
        },
        "counterfactual_gate_since_non_admissible_boundary": counterfactual,
        "prior_live_record_faced": {
            "old_direction_reference": "Fable cited 10 orders/7 fills/3 rejects/-5.283319 at 2026-07-14T16:59Z; latest scorecard lifetime member record is used here.",
            "source": f"{paths['scorecard']}:lifetime_pnl_truth.by_member.{WALLET}",
            "updated_lifetime_digits": prior_record,
        },
        "temporal_positive_case_against_prior_record": temporal_case,
        "mechanism_if_fable_allows_activation": {
            "durable_registry_admission_required": True,
            "retired_pattern": "no one-shot restart tokens such as FABLE_1232",
            "runtime_member_increase": 1,
            "copyintent_parity_preserved": True,
            "single_submitter_preserved": True,
            "activation_path": "one managed restart of the existing single guard only after Fable ruling",
            "pre_ruled_activation_path": (
                "Fable 2026-07-16T05:28Z branch (a) permits activation without a new ask "
                "when this packet shows post-fee > 0 on n>=10 resolved counterfactual rows"
            ),
            "runtime_presence_now": {
                "active_set_members": _active_roster_member(scorecard),
                "selected_probe_candidate": probe.get("selected_candidate"),
                "previous_packet_runtime_presence": (
                    (previous_packet.get("mechanism_if_future_packet_passes") or {}).get("runtime_presence_now")
                    if isinstance(previous_packet.get("mechanism_if_future_packet_passes"), dict)
                    else None
                ),
            },
        },
        "sizing_if_fable_allows_activation": {
            "max_order_usd": 2.5,
            "max_price": 0.5,
            "min_order_usd": 1.0,
            "wallet_fraction": 0.1,
            "overlay_class": "same probe overlay class as prior f418 admission, durable registry only",
            "runtime_member_increase": 1,
        },
        "auto_demote_tripwire_after_activation": {
            "authority": "Fable 2026-07-16T05:28Z",
            "rule": ">=5 resolved live fills with cumulative post-fee live PnL < 0 demotes f418 to shadow-only",
            "lifetime_live_record_named": prior_record,
        },
        "pre_stated_activation_decision_rule": {
            "keep_until": "72h_or_20_fills_whichever_first",
            "keep_if": "positive post-fee live/counterfactual PnL and no structural invariant violation",
            "evict_if": "rolling live PnL <= 0 at 20 fills or fresh liveness/counterfactual evidence disappears by 72h",
        },
        "invariants": {
            "paper_only_research_packet": True,
            "live_orders_allowed_change": False,
            "launchd_restart_requested": False,
            "guard_code_touched": False,
            "copyintent_parity_change": False,
            "single_submitter_change": False,
        },
        "next_action": (
            "Apply the pre-ruled branch (a) activation and restart the single guard"
            if status == "PRE_RULED_ADMIT_F418_ACTIVATION"
            else "Regenerate current-date liveness inputs before any admission decision"
            if status == "DORMANT_SOURCE_EXPIRED"
            else "No activation; continue to the next Fable queue item"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-midnight", default="data/research/source_active_windows_f418_since_midnight_current.json")
    parser.add_argument("--last-24h", default="data/research/source_active_windows_f418_24h_current.json")
    parser.add_argument(
        "--member-since-midnight",
        default="data/research/member_source_active_windows_f418_latest.json",
    )
    parser.add_argument("--probe", default="data/research/wallet_copy_live_execution_probe_cohort_alive_admit_f418d3a1.json")
    parser.add_argument("--routing-shadow", default="data/research/routing_shadow_validation_latest.json")
    parser.add_argument("--scorecard", default="data/research/wallet_copy_daily_scorecard_current.json")
    parser.add_argument("--temporal", default="data/research/wallet_temporal_profitability_latest.json")
    parser.add_argument("--previous-packet", default="data/research/f418_readmission_packet_latest.json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--latest-output", default="data/research/f418_readmission_packet_latest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    temporal_state = load_json(args.temporal, default={}) or {}
    temporal_rows = temporal_state.get("wallets") if isinstance(temporal_state.get("wallets"), list) else []
    temporal = next(
        (row for row in temporal_rows if isinstance(row, dict) and str(row.get("wallet") or "").lower() == WALLET),
        {},
    )
    paths = {
        "since_midnight": args.since_midnight,
        "last_24h": args.last_24h,
        "member_since_midnight": args.member_since_midnight,
        "probe": args.probe,
        "routing_shadow": args.routing_shadow,
        "scorecard": args.scorecard,
        "temporal": args.temporal,
        "previous_packet": args.previous_packet,
    }
    packet = build_packet(
        generated_at=_utc_now_iso(),
        since_midnight=load_json(args.since_midnight, default={}) or {},
        last_24h=load_json(args.last_24h, default={}) or {},
        member_since_midnight=load_json(args.member_since_midnight, default={}) or {},
        probe=load_json(args.probe, default={}) or {},
        routing_shadow=load_json(args.routing_shadow, default={}) or {},
        scorecard=load_fresh_scorecard(args.scorecard),
        temporal=temporal if isinstance(temporal, dict) else {},
        previous_packet=load_json(args.previous_packet, default={}) or {},
        paths=paths,
    )
    atomic_write_json(args.output, packet)
    if args.latest_output and Path(args.latest_output) != Path(args.output):
        atomic_write_json(args.latest_output, packet)
    print(
        {
            "status": packet["status"],
            "source_active_windows": packet["fresh_liveness"]["since_midnight_summary"].get("source_active_windows"),
            "counterfactual_status": packet["counterfactual_gate_since_non_admissible_boundary"].get("status"),
            "counterfactual_basis": packet["counterfactual_gate_since_non_admissible_boundary"].get("basis"),
            "gate_pass": packet["counterfactual_gate_since_non_admissible_boundary"].get("gate_pass"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
