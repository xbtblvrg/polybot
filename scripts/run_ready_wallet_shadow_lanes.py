#!/usr/bin/env python3
"""Track ready wallet paper-shadow lanes toward OP-FASTPROOF member gates."""

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

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.scorecard import load_fresh_scorecard  # noqa: E402
from src.wallet_copy.wide_standby import binding_terminally_executed  # noqa: E402
from scripts.report_volume_standby_promotion_packet import build_packet as build_volume_promotion_packet  # noqa: E402


DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_F1_ACCRUAL_STOP = "data/research/951b_f1_accrual_stop_latest.json"
DEFAULT_WATCH_TIER_SHADOW_EV = "data/research/watch_tier_shadow_ev_latest.json"
DEFAULT_READMISSION_RULINGS = "data/research/watch_tier_readmission_rulings.json"
DEFAULT_HOT_STANDBY_LIVENESS = "data/research/hot_standby_source_liveness_latest.json"
DEFAULT_COHORT_ADMISSION = "data/research/cohort_alive_admission_packets_latest.json"
DEFAULT_TEMPORAL_PROFITABILITY = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_VOLUME_PROMOTION_PACKET = "data/research/13e0_exact_policy_promotion_packet_latest.json"
DEFAULT_VOLUME_CLEARANCE_PACKET = "data/research/ranked_queue_clearance_packet_0x13e0d447520ebe7f8eeaf7817211201b2c585204.json"
DEFAULT_WIDE_EXACT_STATE = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_WIDE_STANDBY_BINDING = "data/research/82c8_wide_standby_binding_latest.json"
DEFAULT_WIDE_FINGERPRINT_EVIDENCE = (
    "data/research/wide_policy_fingerprint_evidence_latest.json"
)
DEFAULT_ORDER_FLOW_DEADMAN_STATE = "data/research/order_flow_deadman_state.json"
VOLUME_WALLET = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
PASS_POLICY = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
PROMOTION_MIN_COPYABLE_BUY_EVENTS = 20
WATCH_TIER_READMISSION_ROI_BAR_PCT = 13.7
WATCH_TIER_READMISSION_FRESH_FILL_GATE = 30
WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD = 0.087
BREADTH_TEMPORAL_REDECISION_FILL_GATE = 10
HOT_STANDBY_LIVENESS_MAX_AGE_H = 24.0
VOLUME_STANDBY_ADJUDICATION_HOURS = 72.0
DEFENSE_LATCH_RELEASE_AT = dt.datetime(2026, 7, 24, 0, 0, tzinfo=dt.timezone.utc)
HOT_STANDBY_RULINGS = {
    "HOT_STANDBY_PENDING_LIVENESS",
    "ADMITTED_TO_HOT_STANDBY_PENDING_LIVENESS",
}
A689_HOT_STANDBY_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
A689_HOT_STANDBY_EVIDENCE_HOURS = 48.0
RANK1_SUCCESSOR_WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
F1_SAMPLE_WALLET = "0x951bd740ef681d05891ca35440232488271d433e"
F1_SAMPLE_FINGERPRINT = (
    "dcef9b3028326682bf283dfc028a7225a64bc26b472960e1df1f98ed27d3c078"
)
F1_SAMPLE_RESOLVED_GATE = 200
RANK1_SUCCESSOR_SOURCE_BINDING = "FABLE_20260723_82C8_READY_SHADOW"
WIDE_STANDBY_SOURCE_BINDING = "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY"
REVOKED_OR_SUSPENDED_RULINGS = {
    "ADMISSION_REVOKED_PRE_LANE",
    "SUSPENDED_BELOW_BAR",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--watch-tier-shadow-ev", default=DEFAULT_WATCH_TIER_SHADOW_EV)
    parser.add_argument("--readmission-rulings", default=DEFAULT_READMISSION_RULINGS)
    parser.add_argument("--hot-standby-liveness", default=DEFAULT_HOT_STANDBY_LIVENESS)
    parser.add_argument("--cohort-admission", default=DEFAULT_COHORT_ADMISSION)
    parser.add_argument("--temporal-profitability", default=DEFAULT_TEMPORAL_PROFITABILITY)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--volume-promotion-packet", default=DEFAULT_VOLUME_PROMOTION_PACKET)
    parser.add_argument("--volume-clearance-packet", default=DEFAULT_VOLUME_CLEARANCE_PACKET)
    parser.add_argument("--wide-exact-state", default=DEFAULT_WIDE_EXACT_STATE)
    parser.add_argument("--wide-standby-binding", default=DEFAULT_WIDE_STANDBY_BINDING)
    parser.add_argument(
        "--wide-fingerprint-evidence",
        default=DEFAULT_WIDE_FINGERPRINT_EVIDENCE,
    )
    parser.add_argument(
        "--order-flow-deadman-state",
        default=DEFAULT_ORDER_FLOW_DEADMAN_STATE,
    )
    parser.add_argument("--watch-tier-readmission-roi-bar-pct", type=float, default=WATCH_TIER_READMISSION_ROI_BAR_PCT)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--promotion-resolved-fill-gate", type=int, default=50)
    return parser.parse_args()


def _wallet(value: Any) -> str:
    wallet = str(value or "").strip().lower()
    return wallet if wallet.startswith("0x") and len(wallet) == 42 else ""


def _parse_iso(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _temporal_by_wallet(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        wallet: row
        for row in payload.get("wallets") or []
        if isinstance(row, dict) and (wallet := _wallet(row.get("wallet")))
    }


def _defense_precondition(scorecard: dict[str, Any], *, now: dt.datetime) -> tuple[bool, dict[str, Any]]:
    since_topup = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    actual = since_topup.get("actual_delta_vs_baseline_usd")
    actual_value = num(actual, float("-inf")) if actual is not None else None
    crossed_floor = actual_value is not None and actual_value >= 20.0
    utc_release_due = now >= DEFENSE_LATCH_RELEASE_AT
    clear = bool(crossed_floor or utc_release_due)
    return clear, {
        "source": "wallet_copy_daily_scorecard_current.json since_topup_truth + preregistered UTC release",
        "scorecard_generated_at": scorecard.get("generated_at"),
        "since_topup_actual_usd": actual_value,
        "clear_floor_usd": 20.0,
        "crossed_floor": crossed_floor,
        "utc_release_at": DEFENSE_LATCH_RELEASE_AT.isoformat().replace("+00:00", "Z"),
        "utc_release_due": utc_release_due,
        "clear": clear,
    }


def _refresh_expiry_preconditions(
    lane: dict[str, Any],
    *,
    source_liveness: dict[str, Any],
    temporal_evidence: dict[str, Any],
    temporal_generated_at: Any,
    defense_clear: bool,
    defense_evidence: dict[str, Any],
) -> None:
    preconditions = lane.setdefault("live_canary_packet_preconditions", {})
    classification = str(temporal_evidence.get("classification") or "").upper()
    evidence_classifications = {
        "CONTINUOUS",
        "WEEKDAY-ONLY",
        "WEEKEND-ONLY",
        "BAND-SPECIALIST",
        "FADING",
    }
    preconditions["fading_clear"] = classification in evidence_classifications and classification != "FADING"
    preconditions["fresh_external_btc5m_lt_24h"] = bool(source_liveness.get("living_source")) and num(
        source_liveness.get("last_trade_age_h"), 999.0
    ) < HOT_STANDBY_LIVENESS_MAX_AGE_H
    preconditions["defense_not_in_triggered_rung"] = defense_clear
    preconditions["ready_shadow_full_utc_day"] = bool(lane.get("ready_shadow_full_utc_day"))
    lane["live_canary_precondition_evidence"] = {
        "fading": {
            "source": "wallet_temporal_profitability_latest.json",
            "generated_at": temporal_generated_at,
            "classification": classification or None,
            "clear": preconditions["fading_clear"],
        },
        "external_liveness": source_liveness,
        "defense": defense_evidence,
    }


def _arm_ready_shadow_canary(
    lane: dict[str, Any],
    *,
    queue_row: dict[str, Any],
    previous_lane: dict[str, Any] | None,
    source_liveness: dict[str, Any],
    temporal_evidence: dict[str, Any],
    temporal_generated_at: Any,
    defense_clear: bool,
    defense_evidence: dict[str, Any],
    now: dt.datetime,
) -> None:
    if not lane.get("ready_for_live"):
        return
    previous_lane = previous_lane or {}
    enrolled_at = previous_lane.get("paper_canary_enrolled_at") or now.isoformat().replace("+00:00", "Z")
    enrolled_dt = _parse_iso(enrolled_at) or now
    elapsed_h = max(0.0, (now - enrolled_dt).total_seconds() / 3600.0)
    lane["paper_canary_enrolled_at"] = enrolled_at
    lane["paper_canary_elapsed_h"] = round(elapsed_h, 6)
    lane["paper_canary_minimum_h"] = 24.0
    lane["ready_shadow_full_utc_day"] = elapsed_h >= 24.0
    lane["copyintent_parity_capture_armed"] = True
    lane["copyintent_parity_capture"] = {
        "status": "ARMED_PAPER_ONLY",
        "policy_id": lane.get("paper_policy_id"),
        "single_submitter_unchanged": True,
        "live_submit_disabled": True,
    }
    lane["canary_path"] = "CLEAR_TO_HOT_STANDBY_PAPER_CANARY"
    lane["paper_only"] = True
    lane["live_orders_allowed"] = False
    lane["source_liveness"] = source_liveness
    fresh_flow = queue_row.get("fresh_flow_rank") if isinstance(queue_row.get("fresh_flow_rank"), dict) else {}
    lane["live_canary_packet_preconditions"] = {
        "hot_standby_ready": bool(
            queue_row.get("ready_for_live")
            and (queue_row.get("bench_liveness") or {}).get("status") == "READY_AND_ALIVE"
        ),
        "fresh_source_active_window": bool(fresh_flow.get("fresh_flow")),
        "ready_shadow_full_utc_day": lane["ready_shadow_full_utc_day"],
    }
    _refresh_expiry_preconditions(
        lane,
        source_liveness=source_liveness,
        temporal_evidence=temporal_evidence,
        temporal_generated_at=temporal_generated_at,
        defense_clear=defense_clear,
        defense_evidence=defense_evidence,
    )
    lane["readiness_verdict"] = (
        "READY_SHADOW_CANARY_PREREQUISITES_DUE"
        if lane["ready_shadow_full_utc_day"]
        else "READY_SHADOW_24H_CANARY_ACCRUING"
    )
    lane["next"] = (
        "write Fable-audited plan only after every live-canary precondition passes; no live mutation"
        if lane["ready_shadow_full_utc_day"]
        else "capture paper CopyIntents for >=1 full UTC day; no live mutation"
    )


def _sticky_enrolled_standby_lanes(
    previous_lanes: dict[str, dict[str, Any]],
    *,
    current_wallets: set[str],
    now: dt.datetime,
) -> list[dict[str, Any]]:
    """Retain enrolled evidence clocks across ranked-queue membership churn."""
    retained: list[dict[str, Any]] = []
    terminal_tokens = ("ADJUDICATED", "RETIRED", "PROMOTED", "TOMBSTONED", "CLOSED", "FINAL_FAIL")
    for wallet, prior in previous_lanes.items():
        if wallet in current_wallets:
            continue
        clock_field = next(
            (
                field
                for field in (
                    "paper_canary_enrolled_at",
                    "standby_evidence_started_at",
                    "clock_start",
                    "decision_at",
                )
                if prior.get(field)
            ),
            None,
        )
        if clock_field is None:
            continue
        if any(prior.get(field) for field in ("paper_canary_adjudicated_at", "standby_clock_adjudicated_at", "clock_adjudicated_at")):
            continue
        status_text = " ".join(
            str(prior.get(field) or "").upper()
            for field in ("shadow_status", "readiness_verdict", "standby_clock_status", "status")
        )
        if any(token in status_text for token in terminal_tokens):
            continue
        lane = dict(prior)
        anchor = _parse_iso(prior.get(clock_field))
        if anchor is not None:
            elapsed_h = max(0.0, (now - anchor).total_seconds() / 3600.0)
            if clock_field == "paper_canary_enrolled_at":
                lane["paper_canary_elapsed_h"] = round(elapsed_h, 6)
                minimum_h = num(lane.get("paper_canary_minimum_h"), 24.0)
                lane["ready_shadow_full_utc_day"] = elapsed_h >= minimum_h
                lane["readiness_verdict"] = (
                    "READY_SHADOW_CANARY_PREREQUISITES_DUE"
                    if lane["ready_shadow_full_utc_day"]
                    else "READY_SHADOW_24H_CANARY_ACCRUING"
                )
                lane["next"] = (
                    "write Fable-audited plan only after every live-canary precondition passes; no live mutation"
                    if lane["ready_shadow_full_utc_day"]
                    else "capture paper CopyIntents for >=1 full UTC day; no live mutation"
                )
            elif clock_field == "standby_evidence_started_at":
                lane["standby_evidence_elapsed_h"] = round(elapsed_h, 6)
        lane["sticky_enrolled_standby"] = True
        lane["sticky_retention_reason"] = "enrolled evidence clock survives ranked-queue membership churn until adjudication"
        retained.append(lane)
    return retained


def _rulings_by_wallet(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("rulings") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
        if wallet:
            out[wallet] = row
    return out


def _liveness_by_wallet(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("rows") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet:
            out[wallet] = {
                **row,
                "_liveness_observed_ts": payload.get("observed_ts"),
            }
    return out


def _source_liveness(
    wallet: str,
    liveness_rows: dict[str, dict[str, Any]],
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    row = liveness_rows.get(wallet) if isinstance(liveness_rows.get(wallet), dict) else {}
    selection = row.get("address_selection") if isinstance(row.get("address_selection"), dict) else {}
    query_key = str(selection.get("recommended_query_key") or "")
    user_hot_path = bool(selection.get("user_only_hot_path_supported"))
    authoritative_query_key = "user" if user_hot_path else query_key
    age_value = selection.get("user_last_trade_age_h") if user_hot_path else None
    if age_value is None:
        age_value = selection.get("last_trade_age_h")
    try:
        age_h = float(age_value)
    except (TypeError, ValueError):
        age_h = None
    observed_ts = num(row.get("_liveness_observed_ts"), 0.0)
    if age_h is not None and observed_ts > 0.0:
        reference = now or dt.datetime.now(dt.timezone.utc)
        age_h = round(age_h + max(0.0, reference.timestamp() - observed_ts) / 3600.0, 6)
    living = bool(
        user_hot_path
        and age_h is not None
        and age_h <= HOT_STANDBY_LIVENESS_MAX_AGE_H
    )
    if living:
        status = "PASS"
    elif not row:
        status = "MISSING_LIVENESS_PROBE"
    elif not user_hot_path:
        status = "AUTHORITATIVE_USER_QUERY_NOT_CONFIRMED"
    elif age_h is None:
        status = "NO_MATCHING_TRADE_AGE"
    else:
        status = "STALE"
    return {
        "status": status,
        "living_source": living,
        "max_age_h": HOT_STANDBY_LIVENESS_MAX_AGE_H,
        "last_trade_age_h": age_h,
        "last_trade_iso": (
            selection.get("user_last_trade_iso") or selection.get("last_trade_iso")
            if user_hot_path
            else selection.get("last_trade_iso")
        ),
        "recommended_query_key": authoritative_query_key,
        "freshest_query_key": query_key,
        "user_only_hot_path_supported": user_hot_path,
        "selection_basis": selection.get("selection_basis"),
        "probe_observed_at": (
            dt.datetime.fromtimestamp(observed_ts, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
            if observed_ts > 0.0
            else None
        ),
    }


def _dead_source_reenrollment_state(
    previous_state: dict[str, Any] | None,
    *,
    liveness_rows: dict[str, dict[str, Any]],
    now: dt.datetime,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Carry dead-source rulings and reopen only after a newer BTC5m trade."""
    carried: list[dict[str, Any]] = []
    suppressed: set[str] = set()
    for raw in (previous_state or {}).get("standby_adjudications") or []:
        if not isinstance(raw, dict):
            continue
        ruling = dict(raw)
        wallet = _wallet(ruling.get("wallet"))
        if not wallet or ruling.get("status") != "NO_PROMOTE_DEAD_SOURCE":
            carried.append(ruling)
            continue
        current = _source_liveness(wallet, liveness_rows, now=now)
        ruled_last_trade = _parse_iso(ruling.get("source_last_trade_iso"))
        current_last_trade = _parse_iso(current.get("last_trade_iso"))
        resumed = bool(
            ruled_last_trade is not None
            and current_last_trade is not None
            and current_last_trade > ruled_last_trade
        )
        if resumed:
            ruling["status"] = "REENROLLMENT_RELEASED_FRESH_TRADE_OBSERVED"
            ruling["reenrollment_released_at"] = now.isoformat().replace("+00:00", "Z")
            ruling["fresh_trade_iso"] = current.get("last_trade_iso")
        else:
            suppressed.add(wallet)
        carried.append(ruling)
    return carried, suppressed


def _ingest_wide_standby_binding(
    previous_state: dict[str, Any] | None,
    binding_artifact: dict[str, Any] | None,
) -> dict[str, Any]:
    """Seed the reducer from the immutable, non-backdated WIDE binding."""

    state = dict(previous_state or {})
    binding = (
        binding_artifact.get("binding")
        if isinstance(binding_artifact, dict)
        and isinstance(binding_artifact.get("binding"), dict)
        else {}
    )
    if (
        not isinstance(binding_artifact, dict)
        or not binding_terminally_executed(binding_artifact)
        or binding.get("source_binding_status") != "WIRED"
        or _wallet(binding.get("wallet")) != RANK1_SUCCESSOR_WALLET
    ):
        return state
    binding_id = str(binding.get("source_binding_id") or "")
    if not binding_id:
        return state
    lanes = [
        dict(row)
        for row in state.get("lanes") or []
        if isinstance(row, dict)
    ]
    existing = next(
        (
            row
            for row in lanes
            if str(row.get("source_binding_id") or "") == binding_id
        ),
        {},
    )
    immutable_clock_start = (
        existing.get("standby_evidence_started_at")
        or binding.get("standby_evidence_started_at")
    )
    seeded_lane = {
        **binding,
        **existing,
        "wallet": RANK1_SUCCESSOR_WALLET,
        "source_binding": WIDE_STANDBY_SOURCE_BINDING,
        "source_binding_id": binding_id,
        "source_binding_status": "WIRED",
        "standby_evidence_started_at": immutable_clock_start,
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": False,
    }
    lanes = [
        row
        for row in lanes
        if _wallet(row.get("wallet")) != RANK1_SUCCESSOR_WALLET
        and str(row.get("source_binding_id") or "") != binding_id
    ]
    lanes.append(seeded_lane)
    adjudications = [
        dict(row)
        for row in state.get("standby_adjudications") or []
        if isinstance(row, dict)
        and str(row.get("source_binding_id") or "") != binding_id
    ]
    adjudications.append(
        {
            "wallet": RANK1_SUCCESSOR_WALLET,
            "status": "WIDE_RECEIPT_BOUND_FORWARD_READY_SHADOW",
            "source_binding": WIDE_STANDBY_SOURCE_BINDING,
            "source_binding_id": binding_id,
            "source_binding_status": "WIRED",
            "clock_start": immutable_clock_start,
            "paper_only": True,
            "live_orders_allowed": False,
            "authority": binding.get("source_binding_authority")
            or binding_artifact.get("authority"),
        }
    )
    state["lanes"] = lanes
    state["standby_adjudications"] = adjudications
    return state


def _adjudicate_mature_dead_source_lanes(
    lanes: list[dict[str, Any]],
    *,
    adjudications: list[dict[str, Any]],
    liveness_rows: dict[str, dict[str, Any]],
    now: dt.datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Release a mature standby slot when its BTC5m source remains stale."""
    retained: list[dict[str, Any]] = []
    ruled_wallets = {
        _wallet(row.get("wallet"))
        for row in adjudications
        if isinstance(row, dict) and row.get("status") == "NO_PROMOTE_DEAD_SOURCE"
    }
    for lane in lanes:
        wallet = _wallet(lane.get("wallet"))
        enrolled = _parse_iso(lane.get("paper_canary_enrolled_at"))
        if not wallet or enrolled is None:
            retained.append(lane)
            continue
        elapsed_h = max(0.0, (now - enrolled).total_seconds() / 3600.0)
        lane["paper_canary_elapsed_h"] = round(elapsed_h, 6)
        current_liveness = _source_liveness(wallet, liveness_rows, now=now)
        if current_liveness.get("status") != "MISSING_LIVENESS_PROBE":
            lane["source_liveness"] = current_liveness
        else:
            current_liveness = (
                lane.get("source_liveness")
                if isinstance(lane.get("source_liveness"), dict)
                else current_liveness
            )
        stale = bool(
            current_liveness.get("status") == "STALE"
            and num(current_liveness.get("last_trade_age_h"), 0.0) > HOT_STANDBY_LIVENESS_MAX_AGE_H
        )
        if elapsed_h < VOLUME_STANDBY_ADJUDICATION_HOURS or not stale:
            retained.append(lane)
            continue
        if wallet not in ruled_wallets:
            adjudications.append(
                {
                    "wallet": wallet,
                    "status": "NO_PROMOTE_DEAD_SOURCE",
                    "adjudicated_at": now.isoformat().replace("+00:00", "Z"),
                    "clock_enrolled_at": lane.get("paper_canary_enrolled_at"),
                    "clock_elapsed_h": round(elapsed_h, 6),
                    "required_h": VOLUME_STANDBY_ADJUDICATION_HOURS,
                    "source_liveness_status": current_liveness.get("status"),
                    "source_last_trade_age_h": current_liveness.get("last_trade_age_h"),
                    "source_last_trade_iso": current_liveness.get("last_trade_iso"),
                    "slot_action": "RELEASED_TO_NEXT_FULL_POOL_QUEUE_MEMBER",
                    "reenrollment_rule": "REQUIRES_FRESH_OBSERVED_BTC5M_TRADE_AFTER_DEAD_SOURCE_RULING",
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "authority": "fable DIRECTION 2026-07-22T20:05Z",
                }
            )
            ruled_wallets.add(wallet)
    return retained, adjudications


def _lane(row: dict[str, Any], *, gate: int) -> dict[str, Any]:
    replay = row.get("replay") if isinstance(row.get("replay"), dict) else {}
    breadth = row.get("breadth_disposition") if isinstance(row.get("breadth_disposition"), dict) else {}
    breadth_status = str(breadth.get("status") or "").upper()
    if breadth_status == "MEASUREMENT_ONLY_TEMPORAL_CLOSURE":
        resolved = int(breadth.get("resolved_simulated_copies") or replay.get("resolved_orders") or 0)
        paper_orders = int(breadth.get("simulated_copies") or replay.get("paper_orders") or 0)
        pnl = num(breadth.get("paper_pnl_usd"), num(replay.get("paper_pnl_usd"), num(row.get("resolved_pnl"), 0.0)))
        lane_gate = int(breadth.get("redecision_resolved_gate") or BREADTH_TEMPORAL_REDECISION_FILL_GATE)
        fee_estimate = round(resolved * WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD, 6)
        post_fee_pnl = round(pnl - fee_estimate, 6)
        hot_standby_ready = bool(post_fee_pnl > 0 and resolved >= lane_gate)
        if hot_standby_ready:
            readiness_verdict = "HOT_STANDBY_READY"
        elif resolved >= lane_gate and post_fee_pnl <= 0:
            readiness_verdict = "READINESS_DENIED_POST_FEE_NEGATIVE"
        else:
            readiness_verdict = "POST_FEE_BAR_PENDING"
        return {
            "queue_rank": int(row.get("queue_rank") or 0),
            "wallet": str(row.get("wallet") or "").lower(),
            "shadow_status": "BREADTH_TEMPORAL_MEASUREMENT_PENDING",
            "paper_policy_id": replay.get("policy_id") or PASS_POLICY,
            "copy_policy_family": PASS_POLICY,
            "paper_only": True,
            "live_orders_allowed": False,
            "ready_for_live": False,
            "resolved_paper_fills": resolved,
            "promotion_resolved_fill_gate": lane_gate,
            "resolved_fill_gap": max(0, lane_gate - resolved),
            "copyable_buy_events": int(replay.get("copyable_buy_events") or 0),
            "candidate_clob_backed_orders": int(replay.get("candidate_clob_backed_orders") or 0),
            "promotion_copyable_buy_gate": PROMOTION_MIN_COPYABLE_BUY_EVENTS,
            "copyable_buy_gap": 0,
            "paper_pnl_usd": pnl,
            "packet_paper_pnl_usd": pnl,
            "estimated_fee_per_simulated_fill_usd": WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD,
            "in_lane_gross_pnl_usd": pnl,
            "in_lane_fee_estimate_usd": fee_estimate,
            "in_lane_post_fee_pnl_usd": post_fee_pnl,
            "hot_standby_ready": hot_standby_ready,
            "succession_eligible": hot_standby_ready,
            "readiness_verdict": readiness_verdict,
            "paper_orders": paper_orders,
            "reject_ratio": replay.get("candidate_rejected_fill_ratio"),
            "unresolved_ratio": replay.get("unresolved_ratio"),
            "source": "Fable 2026-07-13T09:28Z BREADTH measurement-only temporal closure",
            "breadth_disposition": breadth,
            "next": (
                "eligible as hot standby by post-fee temporal rule; do not displace active producer"
                if hot_standby_ready
                else (
                    "readiness denied: remove from succession ordering and apply recorded negative_action at expiry"
                    if readiness_verdict == "READINESS_DENIED_POST_FEE_NEGATIVE"
                    else "continue paper/shadow temporal measurement until post-fee>0 and n>=10 resolved simulated copies"
                )
            ),
        }
    resolved = int(replay.get("resolved_orders") or 0)
    pnl = num(replay.get("paper_pnl_usd"), 0.0)
    copyable_buy_events = int(replay.get("copyable_buy_events") or 0)
    clob_backed_orders = int(replay.get("candidate_clob_backed_orders") or 0)
    replay_status = str(replay.get("status") or "").upper()
    ready_for_live = bool(row.get("ready_for_live"))
    copyable_watch = (
        replay_status == "PASS"
        and not ready_for_live
        and copyable_buy_events > 0
        and copyable_buy_events < PROMOTION_MIN_COPYABLE_BUY_EVENTS
    )
    if ready_for_live and resolved >= gate and pnl > 0:
        shadow_status = "GATE_CROSSED"
    elif copyable_watch:
        shadow_status = "COPYABLE_BUY_WATCH"
    else:
        shadow_status = "PAPER_SHADOW_ACCUMULATING"
    return {
        "queue_rank": int(row.get("queue_rank") or 0),
        "wallet": str(row.get("wallet") or "").lower(),
        "shadow_status": shadow_status,
        "paper_policy_id": replay.get("policy_id") or PASS_POLICY,
        "copy_policy_family": PASS_POLICY,
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": ready_for_live,
        "resolved_paper_fills": resolved,
        "promotion_resolved_fill_gate": int(gate),
        "resolved_fill_gap": max(0, int(gate) - resolved),
        "copyable_buy_events": copyable_buy_events,
        "candidate_clob_backed_orders": clob_backed_orders,
        "promotion_copyable_buy_gate": PROMOTION_MIN_COPYABLE_BUY_EVENTS,
        "copyable_buy_gap": max(0, PROMOTION_MIN_COPYABLE_BUY_EVENTS - min(copyable_buy_events, clob_backed_orders)),
        "paper_pnl_usd": pnl,
        "paper_orders": int(replay.get("paper_orders") or 0),
        "reject_ratio": replay.get("candidate_rejected_fill_ratio"),
        "unresolved_ratio": replay.get("unresolved_ratio"),
        "next": (
            "eligible for additive live allocation"
            if shadow_status == "GATE_CROSSED"
            else (
                "continue paper shadow until copyable/CLOB-backed BUYs >=20; no threshold cut"
                if shadow_status == "COPYABLE_BUY_WATCH"
                else "continue 1200-wide paper shadow until resolved fill gate crosses"
            )
        ),
    }


def _f1_sample_measurement_lane(
    evidence: dict[str, Any],
    *,
    previous_state: dict[str, Any] | None,
    order_flow_deadman_state: dict[str, Any] | None = None,
    now: dt.datetime,
) -> dict[str, Any] | None:
    cell = next(
        (
            row
            for row in evidence.get("cells") or []
            if isinstance(row, dict)
            and _wallet((row.get("identity") or {}).get("wallet"))
            == F1_SAMPLE_WALLET
            and str(
                (row.get("identity") or {}).get("wide_policy_fingerprint") or ""
            )
            == F1_SAMPLE_FINGERPRINT
        ),
        None,
    )
    if not isinstance(cell, dict):
        return None
    identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
    rescore = (
        cell.get("venue_executable_full_stream_rescore")
        if isinstance(cell.get("venue_executable_full_stream_rescore"), dict)
        else {}
    )
    resolved = int(rescore.get("resolved") or 0)
    unresolved = int(
        (cell.get("resolution_evidence_summary") or {}).get(
            "matured_unresolved_window_count"
        )
        or 0
    )
    prior = next(
        (
            row
            for row in (previous_state or {}).get("lanes") or []
            if isinstance(row, dict)
            and _wallet(row.get("wallet")) == F1_SAMPLE_WALLET
            and row.get("wide_policy_fingerprint") == F1_SAMPLE_FINGERPRINT
        ),
        {},
    )
    started_at = prior.get("measurement_started_at") or now.isoformat()
    baseline_resolved = int(
        prior.get("measurement_baseline_resolved_signals")
        if prior.get("measurement_baseline_resolved_signals") is not None
        else resolved
    )
    started = _parse_iso(started_at)
    elapsed_h = (
        max(0.0, (now - started).total_seconds() / 3600.0)
        if started is not None
        else 0.0
    )
    resolved_delta = max(0, resolved - baseline_resolved)
    rate_per_day = (
        round(24.0 * resolved_delta / elapsed_h, 6)
        if elapsed_h > 0.0 and resolved_delta > 0
        else None
    )
    gap = max(0, F1_SAMPLE_RESOLVED_GATE - resolved)
    estimated_days = (
        round(gap / rate_per_day, 6)
        if rate_per_day is not None and rate_per_day > 0
        else None
    )
    frontier_rows = (
        (((order_flow_deadman_state or {}).get("policy_choke") or {}).get(
            "source_roster_drought"
        ) or {}).get("candidate_evidence") or {}
    ).get("rows") or []
    f2_row = next(
        (
            row
            for row in frontier_rows
            if isinstance(row, dict)
            and _wallet(row.get("wallet")) == F1_SAMPLE_WALLET
            and str(row.get("wide_policy_fingerprint") or "")
            == F1_SAMPLE_FINGERPRINT
        ),
        {},
    )
    direct_source = (
        f2_row.get("direct_source")
        if isinstance(f2_row.get("direct_source"), dict)
        else {}
    )
    f2_fresh_rows = int(f2_row.get("fresh_own_source_buy_rows_30m") or 0)
    f2_policy_depth_pass = int(direct_source.get("policy_depth_pass") or 0)
    second_zero_cut_observed = bool(elapsed_h >= (10.0 / 60.0) and resolved_delta == 0)
    prior_projection = (
        prior.get("projected_resolved_signals_at_200")
        if isinstance(prior.get("projected_resolved_signals_at_200"), dict)
        else {}
    )
    accrual_cuts = [
        dict(row)
        for row in prior.get("f1_accrual_cuts") or []
        if isinstance(row, dict)
    ]
    if started_at == "2026-07-31T09:19:10.700641+00:00":
        ordered_stop = load_json(DEFAULT_F1_ACCRUAL_STOP, default={})
        ordered_cuts = ordered_stop.get("cuts") if isinstance(ordered_stop, dict) else []
        if len(ordered_cuts or []) >= 3:
            accrual_cuts = [
                dict(row) for row in ordered_cuts if isinstance(row, dict)
            ]
    if (
        not accrual_cuts
        and prior_projection.get("status") == "SECOND_CUT_OBSERVED_ZERO_ACCRUAL"
    ):
        prior_elapsed_h = num(prior_projection.get("measurement_elapsed_h"), 0.0)
        prior_cut_at = (
            started + dt.timedelta(hours=prior_elapsed_h)
            if started is not None and prior_elapsed_h > 0
            else None
        )
        accrual_cuts = [
            {
                "cut_at": started_at,
                "resolved_signals": baseline_resolved,
                "source": "measurement_baseline",
            },
            {
                "cut_at": prior_cut_at.isoformat() if prior_cut_at else None,
                "resolved_signals": int(
                    prior_projection.get("current_resolved_signals") or resolved
                ),
                "source": "prior_second_cut_observation",
            },
        ]
    current_cut = {
        "cut_at": now.isoformat(),
        "resolved_signals": resolved,
        "source": "wide_policy_fingerprint_evidence_latest.json",
    }
    if not accrual_cuts or accrual_cuts[-1] != current_cut:
        accrual_cuts.append(current_cut)
    accrual_cuts = accrual_cuts[-3:]
    third_zero_cut_observed = bool(
        len(accrual_cuts) == 3
        and len({int(row.get("resolved_signals") or 0) for row in accrual_cuts}) == 1
        and resolved_delta == 0
        and elapsed_h >= (20.0 / 60.0)
    )
    negative_cell = num(rescore.get("post_fee_pnl_usd"), 0.0) <= 0.0
    parked_negative_at_floor = bool(resolved >= F1_SAMPLE_RESOLVED_GATE and negative_cell)
    parked_zero_accrual = bool(
        third_zero_cut_observed and resolved < F1_SAMPLE_RESOLVED_GATE
    )
    return {
        "wallet": F1_SAMPLE_WALLET,
        "shadow_status": (
            "F1_SAMPLE_PARKED_NEGATIVE_AT_FLOOR"
            if parked_negative_at_floor
            else "PARKED_ZERO_F1_ACCRUAL"
            if parked_zero_accrual
            else "F1_SAMPLE_MEASUREMENT_ONLY"
        ),
        "paper_policy_id": identity.get("policy_id"),
        "copy_policy_family": identity.get("policy_id"),
        "wide_policy_fingerprint": F1_SAMPLE_FINGERPRINT,
        "move_slice_keys": identity.get("move_slice_keys") or [],
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": False,
        "resolved_paper_fills": resolved,
        "promotion_resolved_fill_gate": F1_SAMPLE_RESOLVED_GATE,
        "resolved_fill_gap": gap,
        "post_fee_pnl_usd": rescore.get("post_fee_pnl_usd"),
        "roi_pct": rescore.get("roi_pct"),
        "first_half_post_fee_pnl_usd": rescore.get(
            "first_half_post_fee_pnl_usd"
        ),
        "second_half_post_fee_pnl_usd": rescore.get(
            "second_half_post_fee_pnl_usd"
        ),
        "concentration_admissible": rescore.get("concentration_admissible"),
        "f1_walk_forward_admissible": rescore.get(
            "f1_walk_forward_admissible"
        ),
        "f1_sign_flip_required": negative_cell,
        "measurement_terminal": bool(parked_negative_at_floor or parked_zero_accrual),
        "f1_accrual_cuts": accrual_cuts,
        "f2_status": {
            "fresh_own_source_buy_rows_30m": f2_fresh_rows,
            "fresh_own_source_buy_rows_30m_required": 10,
            "policy_depth_pass": f2_policy_depth_pass,
            "policy_depth_pass_required": 1,
            "pass": bool(f2_fresh_rows >= 10 and f2_policy_depth_pass >= 1),
            "source": "order_flow_deadman_state.policy_choke.source_roster_drought.candidate_evidence.rows",
        },
        "measurement_started_at": started_at,
        "measurement_baseline_resolved_signals": baseline_resolved,
        "projected_resolved_signals_at_200": {
            "current_resolved_signals": resolved,
            "target_resolved_signals": F1_SAMPLE_RESOLVED_GATE,
            "resolved_signal_gap": gap,
            "matured_unresolved_windows": unresolved,
            "projected_after_matured_resolution": min(
                F1_SAMPLE_RESOLVED_GATE,
                resolved + unresolved,
            ),
            "resolved_since_seating": resolved_delta,
            "measurement_elapsed_h": round(elapsed_h, 6),
            "observed_resolved_signals_per_day": (
                0.0 if second_zero_cut_observed else rate_per_day
            ),
            "estimated_days_to_200": estimated_days,
            "status": (
                "PARKED_NEGATIVE_AT_F1_FLOOR"
                if parked_negative_at_floor
                else "PARKED_ZERO_F1_ACCRUAL"
                if parked_zero_accrual
                else "TARGET_REACHED"
                if gap == 0
                else "SECOND_CUT_OBSERVED_ZERO_ACCRUAL"
                if second_zero_cut_observed
                else "SAMPLE_ACCRUING_ON_NEGATIVE_CELL"
                if negative_cell
                else "PROJECTED_FROM_OBSERVED_ACCRUAL"
                if estimated_days is not None
                else "ACCRUAL_RATE_PENDING_SECOND_CUT"
            ),
        },
        "source": "wide_policy_fingerprint_evidence_latest.json",
        "next": (
            "none; negative cell parked at unchanged F1 floor with no extension"
            if parked_negative_at_floor
            else "none; third consecutive zero-rate cut parked the stalled F1 lane"
            if parked_zero_accrual
            else "accrue paper-only exact-fingerprint evidence to the unchanged F1 floor; no admission"
        ),
    }


def _watch_tier_readmission_lanes(
    watch_tier_shadow_ev: dict[str, Any],
    *,
    roi_bar_pct: float,
    previous_state: dict[str, Any] | None = None,
    readmission_rulings: dict[str, Any] | None = None,
    hot_standby_liveness: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows = watch_tier_shadow_ev.get("wallets") if isinstance(watch_tier_shadow_ev.get("wallets"), list) else []
    summary = watch_tier_shadow_ev.get("summary") if isinstance(watch_tier_shadow_ev.get("summary"), dict) else {}
    due_wallets = {
        str(wallet or "").lower()
        for wallet in summary.get("wallets_due") or []
        if str(wallet or "").strip()
    }
    due_wallets |= {
        str(wallet or "").lower()
        for wallet in summary.get("wallets_admitted_by_ruling") or []
        if str(wallet or "").strip()
    }
    previous_lanes = {
        str(row.get("wallet") or "").lower(): row
        for row in (previous_state or {}).get("lanes") or []
        if isinstance(row, dict) and str(row.get("wallet") or "").strip()
    }
    previous_readmission_wallets = {
        wallet
        for wallet, row in previous_lanes.items()
        if str(row.get("shadow_status") or "").upper() == "WATCH_TIER_READMITTED_POST_FEE_PENDING"
    }
    due_wallets |= previous_readmission_wallets
    rulings = _rulings_by_wallet(readmission_rulings)
    liveness_rows = _liveness_by_wallet(hot_standby_liveness)
    eligible = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            roi_pct = float(row.get("roi_pct"))
            resolved = int(row.get("resolved_signals") or 0)
        except (TypeError, ValueError):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        status = str(row.get("status") or "").upper()
        existing_readmission_lane = wallet in previous_readmission_wallets
        ruling = rulings.get(wallet)
        ruling_decision = str((ruling or {}).get("ruling") or (ruling or {}).get("decision") or "").upper()
        if (
            not wallet
            or (bool(due_wallets) and wallet not in due_wallets)
            or roi_pct < float(roi_bar_pct)
            or ruling_decision in REVOKED_OR_SUSPENDED_RULINGS
            or (
                not existing_readmission_lane
                and status not in {"READMISSION_RULING_DUE", "READMISSION_ADMITTED_MEASUREMENT_ONLY"}
            )
            or not bool(row.get("readmission_consideration_eligible"))
        ):
            continue
        eligible.append((roi_pct, resolved, wallet, status, row))
    eligible.sort(key=lambda item: (-item[0], -item[1], item[2]))

    lanes: list[dict[str, Any]] = []
    for rank, (roi_pct, resolved, wallet, status, row) in enumerate(eligible, start=1):
        previous = previous_lanes.get(wallet) if isinstance(previous_lanes.get(wallet), dict) else {}
        current_eligible = int(row.get("eligible_signals") or 0)
        current_pnl = num(row.get("pnl_usd"), 0.0)
        baseline_eligible = int(previous.get("feed_baseline_eligible_signals") or current_eligible)
        baseline_resolved = int(previous.get("feed_baseline_resolved_signals") or resolved)
        baseline_pnl = num(previous.get("feed_baseline_gross_pnl_usd"), current_pnl)
        fresh_orders = max(0, current_eligible - baseline_eligible)
        fresh_resolved = max(0, resolved - baseline_resolved)
        fresh_gross_pnl = round(current_pnl - baseline_pnl, 6) if fresh_orders else 0.0
        fresh_post_fee_pnl = round(
            fresh_gross_pnl - (fresh_resolved * WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD),
            6,
        )
        evidence_bar_crossed = bool(
            fresh_post_fee_pnl > 0.0 and fresh_resolved >= WATCH_TIER_READMISSION_FRESH_FILL_GATE
        )
        ruling = rulings.get(wallet) if isinstance(rulings.get(wallet), dict) else {}
        ruling_decision = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
        hot_standby_ruling = ruling_decision in HOT_STANDBY_RULINGS
        source_liveness = _source_liveness(wallet, liveness_rows)
        hot_standby_ready = bool(
            evidence_bar_crossed and hot_standby_ruling and source_liveness.get("living_source")
        )
        hot_standby_pending_liveness = bool(
            evidence_bar_crossed and hot_standby_ruling and not source_liveness.get("living_source")
        )
        page_fable_due = bool(evidence_bar_crossed and not hot_standby_ruling)
        if hot_standby_ready:
            readiness_verdict = "HOT_STANDBY_READY"
        elif hot_standby_pending_liveness:
            readiness_verdict = "HOT_STANDBY_PENDING_LIVENESS"
        elif page_fable_due:
            readiness_verdict = "PAGE_FABLE_DUE"
        elif fresh_resolved >= WATCH_TIER_READMISSION_FRESH_FILL_GATE and fresh_post_fee_pnl <= 0:
            readiness_verdict = "POST_FEE_BAR_FAILED"
        else:
            readiness_verdict = "POST_FEE_BAR_PENDING"
        lanes.append(
            {
                "queue_rank": rank,
                "wallet": wallet,
                "shadow_status": "WATCH_TIER_READMITTED_POST_FEE_PENDING",
                "paper_policy_id": PASS_POLICY,
                "copy_policy_family": PASS_POLICY,
                "paper_only": True,
                "live_orders_allowed": False,
                "ready_for_live": False,
                "resolved_paper_fills": fresh_resolved,
                "promotion_resolved_fill_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
                "resolved_fill_gap": max(0, WATCH_TIER_READMISSION_FRESH_FILL_GATE - fresh_resolved),
                "copyable_buy_events": 0,
                "candidate_clob_backed_orders": 0,
                "promotion_copyable_buy_gate": PROMOTION_MIN_COPYABLE_BUY_EVENTS,
                "copyable_buy_gap": PROMOTION_MIN_COPYABLE_BUY_EVENTS,
                "paper_pnl_usd": fresh_post_fee_pnl,
                "paper_orders": fresh_orders,
                "reject_ratio": None,
                "unresolved_ratio": None,
                "source": "Fable 2026-07-10T20:22Z watch-tier readmission ruling",
                "admission_status": status,
                "readmission_rank": rank,
                "readmission_roi_bar_pct": float(roi_bar_pct),
                "readmission_started_at": previous.get("readmission_started_at") or utc_now_iso(),
                "feed_source": "data/research/watch_tier_shadow_ev_latest.json",
                "feed_status": "FEED_ACCRUING" if fresh_orders > 0 else "BASELINED_NO_NEW_ORDERS",
                "feed_baseline_eligible_signals": baseline_eligible,
                "feed_baseline_resolved_signals": baseline_resolved,
                "feed_baseline_gross_pnl_usd": baseline_pnl,
                "retrospective_gross_roi_pct": roi_pct,
                "retrospective_gross_pnl_usd": current_pnl,
                "retrospective_resolved_signals": resolved,
                "retrospective_eligible_signals": current_eligible,
                "estimated_fee_per_fresh_fill_usd": WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD,
                "in_lane_gross_pnl_usd": fresh_gross_pnl,
                "in_lane_post_fee_pnl_usd": fresh_post_fee_pnl,
                "in_lane_fresh_resolved_signals": fresh_resolved,
                "post_fee_evidence_bar_crossed": evidence_bar_crossed,
                "page_fable_due": page_fable_due,
                "hot_standby_ruling": hot_standby_ruling,
                "hot_standby_ruling_id": ruling.get("ruling_id") or ruling.get("authority"),
                "hot_standby_pending_liveness": hot_standby_pending_liveness,
                "hot_standby_ready": hot_standby_ready,
                "succession_eligible": hot_standby_ready,
                "source_liveness": source_liveness,
                "readiness_verdict": readiness_verdict,
                "copyintent_parity_sanity": "PASS_SAME_POLICY_FAMILY_PAPER_ONLY_NO_LIVE_SUBMITTER",
                "reject_ratio_sanity": "PASS_NO_ATTRIBUTABLE_REJECT_STREAM_FOR_MEASURE_ONLY_WATCH_TIER_LANE",
                "next": (
                    "eligible hot standby; use only on recorded succession trigger"
                    if hot_standby_ready
                    else (
                        "source liveness probe required before hot-standby readiness"
                        if hot_standby_pending_liveness
                        else (
                            "PAGE_FABLE_DUE: post-fee>0 and n>=30 fresh resolved crossed"
                            if page_fable_due
                            else "run paper shadow with live CopyIntent parity; page Fable only after post-fee>0 and n>=30 fresh resolved"
                        )
                    )
                ),
            }
        )
    return lanes


def _explicit_a689_hot_standby_lane(
    watch_tier_shadow_ev: dict[str, Any],
    *,
    previous_state: dict[str, Any] | None,
    readmission_rulings: dict[str, Any],
    hot_standby_liveness: dict[str, Any],
    now: dt.datetime,
) -> dict[str, Any] | None:
    """Bind Fable's a689 standby ruling to the ready-shadow accumulator."""
    feed = next(
        (
            row
            for row in watch_tier_shadow_ev.get("wallets") or []
            if isinstance(row, dict) and _wallet(row.get("source_wallet")) == A689_HOT_STANDBY_WALLET
        ),
        None,
    )
    ruling = _rulings_by_wallet(readmission_rulings).get(A689_HOT_STANDBY_WALLET, {})
    ruling_decision = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
    if not isinstance(feed, dict) or ruling_decision not in HOT_STANDBY_RULINGS:
        return None
    previous = next(
        (
            row
            for row in (previous_state or {}).get("lanes") or []
            if isinstance(row, dict)
            and _wallet(row.get("wallet")) == A689_HOT_STANDBY_WALLET
            and row.get("source_binding") == "FABLE_20260721_A689_READY_SHADOW"
        ),
        {},
    )
    current_eligible = int(feed.get("eligible_signals") or 0)
    current_resolved = int(feed.get("resolved_signals") or 0)
    current_pnl = num(feed.get("pnl_usd"), 0.0)
    baseline_eligible = int(previous.get("feed_baseline_eligible_signals") or current_eligible)
    baseline_resolved = int(previous.get("feed_baseline_resolved_signals") or current_resolved)
    baseline_pnl = num(previous.get("feed_baseline_gross_pnl_usd"), current_pnl)
    fresh_orders = max(0, current_eligible - baseline_eligible)
    fresh_resolved = max(0, current_resolved - baseline_resolved)
    fresh_gross_pnl = round(current_pnl - baseline_pnl, 6) if fresh_orders else 0.0
    fresh_post_fee_pnl = round(
        fresh_gross_pnl - fresh_resolved * WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD,
        6,
    )
    started_at = previous.get("standby_evidence_started_at") or now.isoformat().replace("+00:00", "Z")
    started = _parse_iso(started_at) or now
    elapsed_h = max(0.0, (now - started).total_seconds() / 3600.0)
    liveness = _source_liveness(
        A689_HOT_STANDBY_WALLET,
        _liveness_by_wallet(hot_standby_liveness),
        now=now,
    )
    clock_complete = elapsed_h >= A689_HOT_STANDBY_EVIDENCE_HOURS
    evidence_pass = fresh_resolved >= WATCH_TIER_READMISSION_FRESH_FILL_GATE and fresh_post_fee_pnl > 0.0
    hot_ready = bool(clock_complete and evidence_pass and liveness.get("living_source"))
    return {
        "wallet": A689_HOT_STANDBY_WALLET,
        "source_binding": "FABLE_20260721_A689_READY_SHADOW",
        "source_binding_status": "WIRED",
        "source_binding_authority": "fable DIRECTION 2026-07-21T18:17Z R1",
        "shadow_status": "WATCH_TIER_READMITTED_POST_FEE_PENDING",
        "paper_policy_id": PASS_POLICY,
        "copy_policy_family": PASS_POLICY,
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": False,
        "standby_evidence_started_at": started_at,
        "standby_evidence_elapsed_h": round(elapsed_h, 6),
        "standby_evidence_minimum_h": A689_HOT_STANDBY_EVIDENCE_HOURS,
        "standby_evidence_clock_complete": clock_complete,
        "feed_baseline_eligible_signals": baseline_eligible,
        "feed_baseline_resolved_signals": baseline_resolved,
        "feed_baseline_gross_pnl_usd": baseline_pnl,
        "paper_orders": fresh_orders,
        "resolved_paper_fills": fresh_resolved,
        "in_lane_fresh_resolved_signals": fresh_resolved,
        "promotion_resolved_fill_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
        "resolved_fill_gap": max(0, WATCH_TIER_READMISSION_FRESH_FILL_GATE - fresh_resolved),
        "in_lane_gross_pnl_usd": fresh_gross_pnl,
        "in_lane_post_fee_pnl_usd": fresh_post_fee_pnl,
        "post_fee_evidence_bar_crossed": evidence_pass,
        "hot_standby_ruling": True,
        "hot_standby_ruling_id": ruling.get("ruling_id") or ruling.get("authority"),
        "source_liveness": liveness,
        "hot_standby_ready": hot_ready,
        "succession_eligible": hot_ready,
        "readiness_verdict": "HOT_STANDBY_READY" if hot_ready else "A689_48H_STANDBY_EVIDENCE_ACCRUING",
        "copyintent_parity_sanity": "PASS_SAME_POLICY_FAMILY_PAPER_ONLY_NO_LIVE_SUBMITTER",
        "next": "accrue 48h and >=30 fresh resolved with positive post-fee would-PnL; no live mutation",
    }


def _explicit_rank1_successor_lane(
    queue_rows: list[dict[str, Any]],
    *,
    previous_state: dict[str, Any] | None,
    hot_standby_liveness: dict[str, Any],
    wide_exact_state: dict[str, Any] | None,
    now: dt.datetime,
) -> dict[str, Any] | None:
    """Continue the preregistered 0x82c8 forward seat after the a689 cut."""
    previous = next(
        (
            row
            for row in (previous_state or {}).get("lanes") or []
            if isinstance(row, dict)
            and _wallet(row.get("wallet")) == RANK1_SUCCESSOR_WALLET
            and row.get("source_binding")
            in {RANK1_SUCCESSOR_SOURCE_BINDING, WIDE_STANDBY_SOURCE_BINDING}
        ),
        None,
    )
    if not isinstance(previous, dict):
        return None
    queue_row = next(
        (
            row
            for row in queue_rows
            if isinstance(row, dict) and _wallet(row.get("wallet")) == RANK1_SUCCESSOR_WALLET
        ),
        {},
    )
    replay = queue_row.get("replay") if isinstance(queue_row.get("replay"), dict) else {}
    started_at = previous.get("standby_evidence_started_at") or now.isoformat().replace("+00:00", "Z")
    started = _parse_iso(started_at) or now
    clock_end = started + dt.timedelta(hours=A689_HOT_STANDBY_EVIDENCE_HOURS)
    terminal_at = _parse_iso(previous.get("terminal_executed_at"))
    close_candidates = [clock_end] if now >= clock_end else []
    if terminal_at is not None and terminal_at <= now:
        close_candidates.append(terminal_at)
    evidence_window_closed_at = min(close_candidates) if close_candidates else None
    closed_window_metrics: dict[str, Any] = {}
    if previous.get("source_binding") == WIDE_STANDBY_SOURCE_BINDING:
        prospective = [
            row
            for row in (wide_exact_state or {}).get("orders") or []
            if isinstance(row, dict)
            and _wallet(row.get("wallet")) == RANK1_SUCCESSOR_WALLET
            and str((row.get("f1_f4_terminal") or {}).get("terminal") or "")
            == "COPYABLE_EXACT_POLICY_PAPER_FILL"
            and (_parse_iso(row.get("recorded_at")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc))
            >= started
        ]
        raw_prospective = prospective
        if evidence_window_closed_at is not None:
            prospective = [
                row
                for row in raw_prospective
                if (_parse_iso(row.get("recorded_at")) or now) < evidence_window_closed_at
            ]
            qualifying_resolved = []
            post_window_resolved = []
            missing_resolution_timestamp = []
            for row in raw_prospective:
                if not row.get("resolved"):
                    continue
                resolved_at = _parse_iso(
                    row.get("resolution_computed_at") or row.get("resolved_at")
                )
                if resolved_at is None:
                    missing_resolution_timestamp.append(row)
                elif row in prospective and resolved_at < evidence_window_closed_at:
                    qualifying_resolved.append(row)
                else:
                    post_window_resolved.append(row)
            closed_window_metrics = {
                "evidence_window_closed_at": evidence_window_closed_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "evidence_window_close_reason": (
                    "TERMINAL_COMMITTED"
                    if terminal_at is not None and evidence_window_closed_at == terminal_at
                    else "CLOCK_EXPIRED"
                ),
                "raw_paper_orders": len(raw_prospective),
                "raw_resolved_paper_fills": sum(
                    bool(row.get("resolved")) for row in raw_prospective
                ),
                "post_window_resolved_fills": len(post_window_resolved),
                "post_window_gross_pnl_usd": round(
                    sum(num(row.get("pre_fee_pnl_usd"), 0.0) for row in post_window_resolved),
                    6,
                ),
                "post_window_post_fee_pnl_usd": round(
                    sum(num(row.get("post_fee_pnl_usd"), 0.0) for row in post_window_resolved),
                    6,
                ),
                "resolution_timestamp_missing_fills": len(missing_resolution_timestamp),
            }
        else:
            qualifying_resolved = [row for row in prospective if row.get("resolved")]
        current_orders = len(prospective)
        current_resolved = len(qualifying_resolved)
        fresh_orders = current_orders
        fresh_resolved = current_resolved
        fresh_gross_pnl = round(
            sum(num(row.get("pre_fee_pnl_usd"), 0.0) for row in qualifying_resolved),
            6,
        )
        fresh_post_fee_pnl = round(
            sum(num(row.get("post_fee_pnl_usd"), 0.0) for row in qualifying_resolved),
            6,
        )
        baseline_orders = int(previous.get("feed_baseline_paper_orders") or 0)
        baseline_resolved = int(previous.get("feed_baseline_resolved_signals") or 0)
        baseline_pnl = num(previous.get("feed_baseline_gross_pnl_usd"), 0.0)
    else:
        current_orders = int(replay.get("paper_orders") or 0)
        current_resolved = int(replay.get("resolved_orders") or 0)
        current_pnl = num(replay.get("paper_pnl_usd"), 0.0)
        baseline_orders = int(previous.get("feed_baseline_paper_orders") or current_orders)
        baseline_resolved = int(previous.get("feed_baseline_resolved_signals") or current_resolved)
        baseline_pnl = num(previous.get("feed_baseline_gross_pnl_usd"), current_pnl)
        fresh_orders = max(0, current_orders - baseline_orders)
        fresh_resolved = max(0, current_resolved - baseline_resolved)
        fresh_gross_pnl = round(current_pnl - baseline_pnl, 6) if fresh_orders else 0.0
        fresh_post_fee_pnl = round(
            fresh_gross_pnl - fresh_resolved * WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD,
            6,
        )
    elapsed_h = max(0.0, (now - started).total_seconds() / 3600.0)
    liveness = _source_liveness(
        RANK1_SUCCESSOR_WALLET,
        _liveness_by_wallet(hot_standby_liveness),
        now=now,
    )
    if not liveness.get("recommended_query_key"):
        liveness = {
            **liveness,
            "status": "PROBE_WIRED_PENDING_FIRST_RESULT",
            "recommended_query_key": "user",
            "probe_command": (
                "python3 scripts/report_wallet_data_api_address_form.py "
                f"--include-wallet {RANK1_SUCCESSOR_WALLET} "
                "--output data/research/hot_standby_source_liveness_latest.json"
            ),
        }
    clock_complete = elapsed_h >= A689_HOT_STANDBY_EVIDENCE_HOURS
    evidence_pass = fresh_resolved >= WATCH_TIER_READMISSION_FRESH_FILL_GATE and fresh_post_fee_pnl > 0.0
    hot_ready = bool(clock_complete and evidence_pass and liveness.get("living_source"))
    return {
        **previous,
        "wallet": RANK1_SUCCESSOR_WALLET,
        "source_binding": previous.get("source_binding"),
        "source_binding_status": "WIRED",
        "source_binding_authority": previous.get("source_binding_authority"),
        "shadow_status": "RANK1_FORWARD_FILL_BACKED_READY_SHADOW",
        "paper_policy_id": PASS_POLICY,
        "copy_policy_family": PASS_POLICY,
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": False,
        "standby_evidence_started_at": started_at,
        "standby_evidence_elapsed_h": round(elapsed_h, 6),
        "standby_evidence_minimum_h": A689_HOT_STANDBY_EVIDENCE_HOURS,
        "standby_evidence_clock_complete": clock_complete,
        "feed_baseline_paper_orders": baseline_orders,
        "feed_baseline_resolved_signals": baseline_resolved,
        "feed_baseline_gross_pnl_usd": baseline_pnl,
        "paper_orders": fresh_orders,
        "resolved_paper_fills": fresh_resolved,
        "in_lane_fresh_resolved_signals": fresh_resolved,
        "promotion_resolved_fill_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
        "resolved_fill_gap": max(0, WATCH_TIER_READMISSION_FRESH_FILL_GATE - fresh_resolved),
        "in_lane_gross_pnl_usd": fresh_gross_pnl,
        "in_lane_post_fee_pnl_usd": fresh_post_fee_pnl,
        "post_fee_evidence_bar_crossed": evidence_pass,
        **closed_window_metrics,
        "source_liveness": liveness,
        "hot_standby_ready": hot_ready,
        "succession_eligible": hot_ready,
        "readiness_verdict": (
            "EVIDENCE_WINDOW_CLOSED_ACCRUAL_NON_QUALIFYING"
            if evidence_window_closed_at is not None
            and (
                closed_window_metrics.get("post_window_resolved_fills", 0) > 0
                or closed_window_metrics.get("resolution_timestamp_missing_fills", 0) > 0
            )
            else "HOT_STANDBY_READY" if hot_ready else "RANK1_48H_FORWARD_EVIDENCE_ACCRUING"
        ),
        "copyintent_parity_sanity": "PASS_SAME_POLICY_FAMILY_PAPER_ONLY_NO_LIVE_SUBMITTER",
        "next": "accrue 48h and >=30 fresh resolved with positive post-fee would-PnL; no live mutation",
    }


def _lane_resolved_count(row: dict[str, Any]) -> int:
    if row.get("shadow_status") == "WATCH_TIER_READMITTED_POST_FEE_PENDING":
        return int(row.get("in_lane_fresh_resolved_signals") or 0)
    return int(row.get("resolved_paper_fills") or 0)


def _cohort_admission_shadow_lane(
    payload: dict[str, Any] | None,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or payload.get("paper_only") is not True:
        return None
    candidate = payload.get("top_four_way_candidate")
    if not isinstance(candidate, dict) or candidate.get("recommendation") != "ADMISSION_PACKET_READY":
        return None
    wallet = _wallet(candidate.get("wallet") or candidate.get("source_wallet"))
    if not wallet or candidate.get("live_orders_allowed") is not False:
        return None
    previous = next(
        (
            row
            for row in (previous_state or {}).get("lanes") or []
            if isinstance(row, dict)
            and _wallet(row.get("wallet")) == wallet
            and row.get("shadow_status") == "COHORT_ADMISSION_READY_SHADOW"
        ),
        {},
    )
    resolved = int(candidate.get("resolved_copyable_events") or 0)
    pnl = num(candidate.get("paper_pnl_usd"), 0.0)
    baseline_resolved = int(previous.get("feed_baseline_resolved_signals") or resolved)
    baseline_pnl = num(previous.get("feed_baseline_gross_pnl_usd"), pnl)
    source_active = candidate.get("source_active") if isinstance(candidate.get("source_active"), dict) else {}
    external = candidate.get("external_liveness") if isinstance(candidate.get("external_liveness"), dict) else {}
    temporal = candidate.get("temporal_evidence") if isinstance(candidate.get("temporal_evidence"), dict) else {}
    return {
        "wallet": wallet,
        "candidate_id": candidate.get("candidate_id"),
        "shadow_status": "COHORT_ADMISSION_READY_SHADOW",
        "paper_policy_id": PASS_POLICY,
        "copy_policy_family": PASS_POLICY,
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": False,
        "admission_ruling": "Fable DIRECTION 2026-07-20T12:41Z",
        "readmission_started_at": previous.get("readmission_started_at") or utc_now_iso(),
        "ready_shadow_min_hours": 24.0,
        "feed_baseline_resolved_signals": baseline_resolved,
        "feed_baseline_gross_pnl_usd": baseline_pnl,
        "resolved_paper_fills": max(0, resolved - baseline_resolved),
        "in_lane_gross_pnl_usd": round(pnl - baseline_pnl, 6),
        "retrospective_resolved_signals": resolved,
        "retrospective_gross_pnl_usd": pnl,
        "retrospective_gross_roi_pct": candidate.get("roi_pct"),
        "history_completeness": candidate.get("history_completeness"),
        "temporal_classification": temporal.get("classification"),
        "source_active_status": source_active.get("status"),
        "source_active_windows": source_active.get("source_active_windows"),
        "external_liveness_status": external.get("status"),
        "external_latest_trade_age_h": candidate.get("latest_trade_age_h"),
        "fading_clear": str(temporal.get("classification") or "").upper()
        in {"CONTINUOUS", "WEEKDAY-ONLY", "WEEKEND-ONLY", "BAND-SPECIALIST"},
        "copyintent_parity_capture": {
            "status": "ARMED_PAPER_ONLY",
            "policy_id": PASS_POLICY,
            "single_submitter_unchanged": True,
            "live_submit_disabled": True,
        },
        "live_canary_packet_preconditions": {
            "fading_clear": False,
            "fresh_external_btc5m_lt_24h": external.get("status") == "PASS"
            and num(candidate.get("latest_trade_age_h"), 999.0) < 24.0,
            "fresh_source_active_window": source_active.get("source_active_tally_status") == "PASS",
            "ready_shadow_full_utc_day": False,
            "defense_not_in_triggered_rung": False,
        },
        "readiness_verdict": "READY_SHADOW_ACCRUING_FABLE_GATED",
        "next": "capture paper CopyIntents for >=1 full UTC day; packet Fable only after every live-canary precondition passes",
    }


def _rank_hot_standby_candidates(lanes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in lanes:
        if row.get("shadow_status") not in {
            "WATCH_TIER_READMITTED_POST_FEE_PENDING",
            "BREADTH_TEMPORAL_MEASUREMENT_PENDING",
        }:
            continue
        post_fee = num(row.get("in_lane_post_fee_pnl_usd"), 0.0)
        resolved = _lane_resolved_count(row)
        if post_fee <= 0.0 or resolved < 5:
            continue
        candidates.append(
            {
                "wallet": row.get("wallet"),
                "shadow_status": row.get("shadow_status"),
                "in_lane_post_fee_pnl_usd": post_fee,
                "resolved_count": resolved,
                "hot_standby_ready": bool(row.get("hot_standby_ready")),
                "pending_liveness": bool(row.get("hot_standby_pending_liveness")),
                "page_fable_due": bool(row.get("page_fable_due")),
                "readiness_verdict": row.get("readiness_verdict"),
            }
        )
    candidates.sort(
        key=lambda row: (
            -float(row.get("in_lane_post_fee_pnl_usd") or 0.0),
            -int(row.get("resolved_count") or 0),
            str(row.get("wallet") or ""),
        )
    )
    return candidates


def build_state(
    *,
    queue: dict[str, Any],
    limit: int,
    gate: int,
    watch_tier_shadow_ev: dict[str, Any] | None = None,
    watch_tier_readmission_roi_bar_pct: float = WATCH_TIER_READMISSION_ROI_BAR_PCT,
    previous_state: dict[str, Any] | None = None,
    readmission_rulings: dict[str, Any] | None = None,
    hot_standby_liveness: dict[str, Any] | None = None,
    cohort_admission: dict[str, Any] | None = None,
    temporal_profitability: dict[str, Any] | None = None,
    scorecard: dict[str, Any] | None = None,
    wide_exact_state: dict[str, Any] | None = None,
    wide_standby_binding: dict[str, Any] | None = None,
    wide_fingerprint_evidence: dict[str, Any] | None = None,
    order_flow_deadman_state: dict[str, Any] | None = None,
    volume_promotion_packet: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    canary_now = now or dt.datetime.now(dt.timezone.utc)
    previous_state = _ingest_wide_standby_binding(
        previous_state,
        wide_standby_binding,
    )
    temporal_profitability = temporal_profitability or {}
    temporal_rows = _temporal_by_wallet(temporal_profitability)
    defense_clear, defense_evidence = _defense_precondition(scorecard or {}, now=canary_now)
    liveness_rows = _liveness_by_wallet(hot_standby_liveness or {})
    standby_adjudications, dead_source_suppressed_wallets = _dead_source_reenrollment_state(
        previous_state,
        liveness_rows=liveness_rows,
        now=canary_now,
    )
    ranked = [row for row in queue.get("ranked_members") or [] if isinstance(row, dict)]
    ranked = [
        row
        for row in ranked
        if _wallet(row.get("wallet")) not in dead_source_suppressed_wallets
    ]
    ready = [
        row
        for row in ranked
        if bool(row.get("ready_for_live"))
    ]
    thin_pass_watch = [
        row
        for row in ranked
        if not bool(row.get("ready_for_live"))
        and isinstance(row.get("replay"), dict)
        and str(row["replay"].get("status") or "").upper() == "PASS"
        and 0 < int(row["replay"].get("copyable_buy_events") or 0) < PROMOTION_MIN_COPYABLE_BUY_EVENTS
    ]
    breadth_measurement = [
        row
        for row in ranked
        if isinstance(row.get("breadth_disposition"), dict)
        and str(row["breadth_disposition"].get("status") or "").upper() == "MEASUREMENT_ONLY_TEMPORAL_CLOSURE"
    ]
    selected = [*ready, *thin_pass_watch, *breadth_measurement]
    queue_lanes = [_lane(row, gate=int(gate)) for row in selected]
    previous_lanes = {
        _wallet(row.get("wallet")): row
        for row in (previous_state or {}).get("lanes") or []
        if isinstance(row, dict) and _wallet(row.get("wallet"))
    }
    queue_rows = {_wallet(row.get("wallet")): row for row in selected if _wallet(row.get("wallet"))}
    for lane in queue_lanes:
        wallet = _wallet(lane.get("wallet"))
        queue_row = queue_rows.get(wallet, {})
        queue_temporal = queue_row.get("temporal_evidence") if isinstance(queue_row.get("temporal_evidence"), dict) else {}
        _arm_ready_shadow_canary(
            lane,
            queue_row=queue_row,
            previous_lane=previous_lanes.get(wallet),
            source_liveness=_source_liveness(wallet, liveness_rows, now=canary_now),
            temporal_evidence=temporal_rows.get(wallet) or queue_temporal,
            temporal_generated_at=temporal_profitability.get("generated_at"),
            defense_clear=defense_clear,
            defense_evidence=defense_evidence,
            now=canary_now,
        )
    queue_lanes, standby_adjudications = _adjudicate_mature_dead_source_lanes(
        queue_lanes,
        adjudications=standby_adjudications,
        liveness_rows=liveness_rows,
        now=canary_now,
    )
    queue_lanes = queue_lanes[: max(1, int(limit))]
    readmission_lanes = _watch_tier_readmission_lanes(
        watch_tier_shadow_ev or {},
        roi_bar_pct=float(watch_tier_readmission_roi_bar_pct),
        previous_state=previous_state,
        readmission_rulings=readmission_rulings or {},
        hot_standby_liveness=hot_standby_liveness or {},
    )
    explicit_a689 = _explicit_a689_hot_standby_lane(
        watch_tier_shadow_ev or {},
        previous_state=previous_state,
        readmission_rulings=readmission_rulings or {},
        hot_standby_liveness=hot_standby_liveness or {},
        now=canary_now,
    )
    prior_cut = (
        (previous_state or {}).get("a689_82c8_cut")
        if isinstance((previous_state or {}).get("a689_82c8_cut"), dict)
        else {}
    )
    prior_successor_bound = any(
        isinstance(row, dict)
        and _wallet(row.get("wallet")) == RANK1_SUCCESSOR_WALLET
        and row.get("source_binding") == RANK1_SUCCESSOR_SOURCE_BINDING
        for row in (previous_state or {}).get("lanes") or []
    )
    a689_cut_terminalized = bool(
        prior_cut.get("status") == "EXECUTED_ATOMIC_STATE_REBIND" or prior_successor_bound
    )
    if a689_cut_terminalized:
        explicit_a689 = None
        readmission_lanes = [
            row
            for row in readmission_lanes
            if _wallet(row.get("wallet")) != A689_HOT_STANDBY_WALLET
        ]
    if explicit_a689 and all(row.get("wallet") != A689_HOT_STANDBY_WALLET for row in readmission_lanes):
        readmission_lanes.append(explicit_a689)
    explicit_rank1_successor = _explicit_rank1_successor_lane(
        ranked,
        previous_state=previous_state,
        hot_standby_liveness=hot_standby_liveness or {},
        wide_exact_state=wide_exact_state or {},
        now=canary_now,
    )
    cohort_admission_lane = _cohort_admission_shadow_lane(
        cohort_admission,
        previous_state=previous_state,
    )
    readmitted_wallets = {row["wallet"] for row in readmission_lanes}
    breadth_measurement_wallets = {
        row["wallet"]
        for row in queue_lanes
        if row.get("shadow_status") == "BREADTH_TEMPORAL_MEASUREMENT_PENDING"
    }
    lanes = [
        *([cohort_admission_lane] if cohort_admission_lane else []),
        *[row for row in readmission_lanes if row.get("wallet") not in breadth_measurement_wallets],
        *[
            row
            for row in queue_lanes
            if row.get("wallet") not in readmitted_wallets
            or row.get("wallet") in breadth_measurement_wallets
        ],
    ]
    if explicit_rank1_successor:
        lanes = [
            row for row in lanes if _wallet(row.get("wallet")) != RANK1_SUCCESSOR_WALLET
        ]
        lanes.append(explicit_rank1_successor)
    sticky_lanes = _sticky_enrolled_standby_lanes(
            previous_lanes,
            current_wallets={_wallet(row.get("wallet")) for row in lanes},
            now=canary_now,
        )
    for lane in sticky_lanes:
        wallet = _wallet(lane.get("wallet"))
        _refresh_expiry_preconditions(
            lane,
            source_liveness=_source_liveness(wallet, liveness_rows, now=canary_now),
            temporal_evidence=temporal_rows.get(wallet, {}),
            temporal_generated_at=temporal_profitability.get("generated_at"),
            defense_clear=defense_clear,
            defense_evidence=defense_evidence,
        )
    sticky_lanes, standby_adjudications = _adjudicate_mature_dead_source_lanes(
        sticky_lanes,
        adjudications=standby_adjudications,
        liveness_rows=liveness_rows,
        now=canary_now,
    )
    lanes.extend(sticky_lanes)
    prior_82c8_terminal = (
        (previous_state or {}).get("terminal_82c8_decision")
        if isinstance((previous_state or {}).get("terminal_82c8_decision"), dict)
        else {}
    )
    if (
        prior_82c8_terminal.get("status") in {
            "PARK_VOLUME_STANDBY_PAPER_ONLY",
            "PARK_READY_SHADOW_EVIDENCE_ABSENCE",
        }
        and (
            not explicit_rank1_successor
            or explicit_rank1_successor.get("source_binding")
            != WIDE_STANDBY_SOURCE_BINDING
        )
    ):
        lanes = [
            row
            for row in lanes
            if _wallet(row.get("wallet")) != RANK1_SUCCESSOR_WALLET
        ]
    f1_sample_lane = _f1_sample_measurement_lane(
        wide_fingerprint_evidence or {},
        previous_state=previous_state,
        order_flow_deadman_state=order_flow_deadman_state,
        now=canary_now,
    )
    volume_terminal_parked = bool(
        (volume_promotion_packet or {}).get("terminal_decision")
        == "PARK_VOLUME_STANDBY_PAPER_ONLY"
        or (
            (volume_promotion_packet or {}).get("prederived_decision_branches")
            or {}
        ).get("current_branch")
        == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    )
    if f1_sample_lane is not None:
        if volume_terminal_parked:
            lanes = [
                row
                for row in lanes
                if _wallet(row.get("wallet")) != VOLUME_WALLET
            ]
            f1_sample_lane["replaced_measurement_seat_wallet"] = VOLUME_WALLET
            f1_sample_lane["replacement_reason"] = (
                "terminal PARK_VOLUME_STANDBY_PAPER_ONLY cannot produce an "
                "admissible wallet"
            )
        lanes = [
            row
            for row in lanes
            if _wallet(row.get("wallet")) != F1_SAMPLE_WALLET
        ]
        lanes.append(f1_sample_lane)
    crossed = [row for row in lanes if row.get("shadow_status") == "GATE_CROSSED"]
    copyable_watch = [row for row in lanes if row.get("shadow_status") == "COPYABLE_BUY_WATCH"]
    readmitted = [
        row
        for row in lanes
        if row.get("shadow_status") == "WATCH_TIER_READMITTED_POST_FEE_PENDING"
    ]
    breadth_temporal = [
        row
        for row in lanes
        if row.get("shadow_status") == "BREADTH_TEMPORAL_MEASUREMENT_PENDING"
    ]
    hot_standby_ready = [row for row in breadth_temporal if bool(row.get("hot_standby_ready"))]
    watch_tier_hot_ready = [row for row in readmitted if bool(row.get("hot_standby_ready"))]
    watch_tier_pending_liveness = [row for row in readmitted if bool(row.get("hot_standby_pending_liveness"))]
    page_fable_due = [row for row in readmitted if bool(row.get("page_fable_due"))]
    ranked_hot_standby = _rank_hot_standby_candidates(lanes)
    ready_ranked = [row for row in ranked_hot_standby if bool(row.get("hot_standby_ready"))]
    return {
        "schema_version": 1,
        "kind": "wallet_copy_ready_shadow_lanes",
        "flow_stage": "PROMOTE/LEARN/ROTATE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "operator_decision": "OP-FASTPROOF-20260705-BELA",
        "source": "Fable 2026-07-05T18:45Z ready-5 paper shadow order",
        "watch_tier_readmission_source": (
            "Fable 2026-07-10T20:22Z fee-aware watch-tier readmission ruling"
            if readmitted
            else ""
        ),
        "a689_82c8_cut": prior_cut if a689_cut_terminalized else {},
        "summary": {
            "lane_count": len(lanes),
            "gate_crossed": len(crossed),
            "copyable_watch": len(copyable_watch),
            "watch_tier_readmitted": len(readmitted),
            "a689_ready_shadow_source_bound": bool(explicit_a689),
            "a689_cut_terminalized": a689_cut_terminalized,
            "cohort_admission_ready_shadow": 1 if cohort_admission_lane else 0,
            "breadth_temporal_measurement": len(breadth_temporal),
            "watch_tier_readmission_bar_pct": float(watch_tier_readmission_roi_bar_pct),
            "promotion_resolved_fill_gate": int(gate),
            "watch_tier_readmission_fresh_fill_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
            "watch_tier_readmission_total_fresh_orders": sum(int(row.get("paper_orders") or 0) for row in readmitted),
            "watch_tier_readmission_total_fresh_resolved": sum(
                int(row.get("in_lane_fresh_resolved_signals") or 0) for row in readmitted
            ),
            "watch_tier_hot_standby_ready": len(watch_tier_hot_ready),
            "watch_tier_hot_standby_pending_liveness": len(watch_tier_pending_liveness),
            "watch_tier_page_fable_due": len(page_fable_due),
            "page_fable_due_wallets": [row["wallet"] for row in page_fable_due],
            "breadth_temporal_redecision_fill_gate": BREADTH_TEMPORAL_REDECISION_FILL_GATE,
            "breadth_temporal_hot_standby_ready": len(hot_standby_ready),
            "breadth_temporal_fee_per_fill_usd": WATCH_TIER_READMISSION_ESTIMATED_FEE_PER_FILL_USD,
            "all_measurement_hot_standby_ready": len(ready_ranked),
            "all_measurement_hot_standby_candidates": len(ranked_hot_standby),
            "sos_scan_scope": "ALL_MEASUREMENT_LANES_WATCH_TIER_AND_BREADTH",
            "sos_top_standby_wallet": ready_ranked[0]["wallet"] if ready_ranked else "",
            "sos_top_candidate_wallet": ranked_hot_standby[0]["wallet"] if ranked_hot_standby else "",
            "promotion_copyable_buy_gate": PROMOTION_MIN_COPYABLE_BUY_EVENTS,
            "dead_source_adjudication_h": VOLUME_STANDBY_ADJUDICATION_HOURS,
            "dead_source_slots_released": sum(
                row.get("status") == "NO_PROMOTE_DEAD_SOURCE" for row in standby_adjudications
            ),
            "dead_source_reenrollment_suppressed_wallets": sorted(dead_source_suppressed_wallets),
            "next_gate_wallet": crossed[0]["wallet"] if crossed else "",
            "min_resolved_fill_gap": min((int(row.get("resolved_fill_gap") or 0) for row in lanes), default=0),
            "min_copyable_buy_gap": min((int(row.get("copyable_buy_gap") or 0) for row in lanes), default=0),
        },
        "standby_adjudications": standby_adjudications,
        "hot_standby_ranked_candidates": ranked_hot_standby,
        "lanes": lanes,
        "next": "promote the first gate-crossed wallet additively only when resolved fills >= gate and PnL stays positive",
    }


def main() -> int:
    args = parse_args()
    previous_state = load_json(args.output, default={})
    previous_volume_packet = load_json(args.volume_promotion_packet, default={})
    payload = build_state(
        queue=load_json(args.queue, default={}),
        limit=int(args.limit),
        gate=int(args.promotion_resolved_fill_gate),
        watch_tier_shadow_ev=load_json(args.watch_tier_shadow_ev, default={}),
        watch_tier_readmission_roi_bar_pct=float(args.watch_tier_readmission_roi_bar_pct),
        previous_state=previous_state if isinstance(previous_state, dict) else {},
        readmission_rulings=load_json(args.readmission_rulings, default={}),
        hot_standby_liveness=load_json(args.hot_standby_liveness, default={}),
        cohort_admission=load_json(args.cohort_admission, default={}),
        temporal_profitability=load_json(args.temporal_profitability, default={}),
        scorecard=load_fresh_scorecard(args.scorecard),
        wide_exact_state=load_json(args.wide_exact_state, default={}),
        wide_standby_binding=load_json(args.wide_standby_binding, default={}),
        wide_fingerprint_evidence=load_json(
            args.wide_fingerprint_evidence,
            default={},
        ),
        order_flow_deadman_state=load_json(
            args.order_flow_deadman_state,
            default={},
        ),
        volume_promotion_packet=previous_volume_packet,
    )
    atomic_write_json(args.output, payload)
    volume_packet = build_volume_promotion_packet(
        payload,
        wallet=VOLUME_WALLET,
        clearance_packet=load_json(args.volume_clearance_packet, default={}),
        previous_packet=previous_volume_packet,
        generated_at=payload.get("generated_at"),
    )
    atomic_write_json(args.volume_promotion_packet, volume_packet)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
