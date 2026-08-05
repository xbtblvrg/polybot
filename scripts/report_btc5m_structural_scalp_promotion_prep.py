#!/usr/bin/env python3
"""Build the fee-aware structural scalp PROMOTION-PREP packet."""

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

from src.wallet_copy.fees import POLYMARKET_EMBEDDED_FEE_RATE, expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_STATE = "data/research/btc5m_structural_scalp_paper_lane_state.json"
DEFAULT_EVENTS = "data/research/btc5m_structural_scalp_paper_lane_events.jsonl"
DEFAULT_OUTPUT = "data/research/btc5m_structural_scalp_promotion_prep_latest.json"
DECISION_AT = "2026-07-23T02:00:00Z"
RECONCILIATION_REFERENCE_AT = "2026-07-22T09:44:00Z"
REPORTED_REFERENCE_FILLS = 123
REPORTED_REFERENCE_PNL_USD = 1.485026
RECONCILIATION_LATER_AT = "2026-07-22T10:35:00Z"
REPORTED_LATER_FILLS = 76
REPORTED_LATER_PNL_USD = -1.0676


def _rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    output: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            output.append(row)
    return output


def _iso_epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _rolling_snapshot_as_of(
    fills: list[dict[str, Any]], *, as_of: str, window_hours: float
) -> dict[str, Any]:
    cutoff = _iso_epoch(as_of)
    if cutoff is None:
        return {"as_of": as_of, "fills": 0, "raw_pnl_usd": 0.0, "floor_window_s": None}
    latest_completed_window = int(cutoff // 300 * 300)
    floor = latest_completed_window - int(window_hours * 3600.0) + 300
    rows = [
        row
        for row in fills
        if float(row.get("event_ts") or 0.0) <= cutoff
        and int(row.get("window_start_s") or 0) >= floor
    ]
    return {
        "as_of": as_of,
        "basis": f"event_ts_lte_as_of_and_trailing_{window_hours:g}h_window",
        "floor_window_s": floor,
        "fills": len(rows),
        "raw_pnl_usd": round(sum(float(row.get("pnl_usd") or 0.0) for row in rows), 6),
    }


def build_packet(state: dict[str, Any], fills: list[dict[str, Any]], *, generated_at: str) -> dict[str, Any]:
    floor = int((state.get("forward_gate") or {}).get("forward_floor_window_start_s") or 0)
    forward = [row for row in fills if int(row.get("window_start_s") or 0) >= floor]
    raw_pnl = round(sum(float(row.get("pnl_usd") or 0.0) for row in forward), 6)
    fee = 0.0
    for row in forward:
        shares = float(row.get("matched_shares") or 0.0)
        fee += expected_polymarket_buy_fee_usd(shares=shares, price=row.get("entry_price"))
        fee += expected_polymarket_buy_fee_usd(shares=shares, price=row.get("exit_price"))
    fee = round(fee, 6)
    post_fee = round(raw_pnl - fee, 6)
    starts = [int(row.get("window_start_s") or 0) for row in forward]
    span_days = ((max(starts) - min(starts) + 300) / 86400.0) if starts else 0.0
    ev_per_day = round(post_fee / span_days, 6) if span_days > 0 else None
    evidence_sizes = [float(row.get("cost_usd") or 0.0) for row in forward]
    proven_size = max(evidence_sizes, default=0.0)
    parity = state.get("adapter_contract") or {}
    guard = state.get("single_guard_contract") or {}
    inputs = state.get("inputs") if isinstance(state.get("inputs"), dict) else {}
    newest_source_event_age_s = inputs.get("newest_source_event_age_s")
    source_fresh = bool(
        inputs.get("freshness_pass") is True
        and newest_source_event_age_s is not None
        and float(newest_source_event_age_s) <= 86400.0
    )
    daily_raw: dict[str, float] = {}
    buckets = {"loss": 0, "flat": 0, "win": 0}
    for row in forward:
        day = datetime.fromtimestamp(int(row.get("window_start_s") or 0), tz=UTC).date().isoformat()
        pnl = float(row.get("pnl_usd") or 0.0)
        daily_raw[day] = daily_raw.get(day, 0.0) + pnl
        buckets["win" if pnl > 0.0 else "loss" if pnl < 0.0 else "flat"] += 1
    daily_pnl = {day: round(value, 6) for day, value in sorted(daily_raw.items())}
    worst_day = min(daily_pnl.values()) if daily_pnl else None
    study_ev = (state.get("summary") or {}).get("study_ev_per_day_usd")
    study_ev = float(study_ev) if study_ev is not None else None
    divergence_ratio = abs(ev_per_day / study_ev) if ev_per_day is not None and study_ev not in (None, 0.0) else None
    divergence_review_required = divergence_ratio is None or divergence_ratio > 2.0
    seeded_at = state.get("seeded_at")
    seeded_s = _iso_epoch(seeded_at)
    generated_s = _iso_epoch(generated_at)
    decision_s = _iso_epoch(DECISION_AT)
    clock_elapsed_days = (
        max(0.0, (generated_s - seeded_s) / 86400.0)
        if generated_s is not None and seeded_s is not None
        else None
    )
    decision_due = bool(generated_s is not None and decision_s is not None and generated_s >= decision_s)
    reference = _rolling_snapshot_as_of(
        fills,
        as_of=RECONCILIATION_REFERENCE_AT,
        window_hours=float(inputs.get("gate_window_hours") or 24.0),
    )
    reference_reproduced = bool(
        reference["fills"] == REPORTED_REFERENCE_FILLS
        and abs(float(reference["raw_pnl_usd"]) - REPORTED_REFERENCE_PNL_USD) <= 0.000001
    )
    later = _rolling_snapshot_as_of(
        fills,
        as_of=RECONCILIATION_LATER_AT,
        window_hours=float(inputs.get("gate_window_hours") or 24.0),
    )
    later_reproduced = bool(
        later["fills"] == REPORTED_LATER_FILLS
        and abs(float(later["raw_pnl_usd"]) - REPORTED_LATER_PNL_USD) <= 0.000001
    )
    reconciliation_status = (
        "WINDOW_BASIS_DIFFERENCE_NOT_REBUILD_DRIFT"
        if reference_reproduced and later_reproduced
        else "ROLLING_WINDOW_PLUS_LATE_ARRIVAL_BACKFILL"
        if reference_reproduced
        else "RECONCILIATION_MISMATCH"
    )
    branch_inputs_pass = bool(
        source_fresh
        and len(forward) >= 30
        and post_fee > 0.0
        and proven_size >= 0.99
        and not divergence_review_required
        and clock_elapsed_days is not None
        and clock_elapsed_days >= 3.0
    )
    evidence_pass = decision_due and branch_inputs_pass
    current_branch = (
        "ACCRUE_PAPER_ONLY_NO_LIVE_MUTATION"
        if not decision_due
        else (
            "PROMOTE_PACKET_READY_FOR_FABLE_LIVE_DECISION"
            if branch_inputs_pass
            else "PARK_METHOD_LANE_PAPER_ONLY"
        )
    )
    return {
        "schema_version": 1,
        "kind": "btc5m_structural_scalp_promotion_prep",
        "flow_stage": "PROMOTE",
        "generated_at": generated_at,
        "status": (
            "PROMOTION_PREP_READY_FOR_FABLE_DECISION"
            if evidence_pass
            else "PROMOTION_PREP_PARK_BRANCH_READY"
            if decision_due
            else "PROMOTION_PREP_EVIDENCE_GATE_PENDING"
        ),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_owner": "Fable",
        "decision_clock": {
            "clock_start": seeded_at,
            "decision_at": DECISION_AT,
            "elapsed_days": round(clock_elapsed_days, 6) if clock_elapsed_days is not None else None,
            "due": decision_due,
            "basis": "fixed seeded_at clock; forward economics are cumulative from the first post-seed window",
        },
        "basis_reconciliation": {
            "status": reconciliation_status,
            "reported_reference": {
                "as_of": RECONCILIATION_REFERENCE_AT,
                "fills": REPORTED_REFERENCE_FILLS,
                "raw_pnl_usd": REPORTED_REFERENCE_PNL_USD,
            },
            "reproduced_reference": reference,
            "reported_later": {
                "as_of": RECONCILIATION_LATER_AT,
                "fills": REPORTED_LATER_FILLS,
                "raw_pnl_usd": REPORTED_LATER_PNL_USD,
            },
            "reproduced_later": later,
            "later_revision_delta": {
                "fills": int(later["fills"]) - REPORTED_LATER_FILLS,
                "raw_pnl_usd": round(float(later["raw_pnl_usd"]) - REPORTED_LATER_PNL_USD, 6),
                "cause": "late-arriving source rows with pre-cutoff event_ts are included by later deterministic rebuilds; historical paper snapshots are revisionable",
            },
            "current_rolling_24h": (state.get("metrics") or {}).get("gate_24h") or {},
            "decision_basis": "fixed_seeded_at_cumulative",
            "explanation": "the headline snapshots use different trailing-24h floors, and later rebuilds can backfill pre-cutoff event_ts rows; the fixed decision clock uses the current cumulative post-seed ledger, never a moving 24h floor",
        },
        "forward_window": {
            "start_window_s": min(starts) if starts else None,
            "end_window_s": max(starts) + 300 if starts else None,
            "measured_days": round(span_days, 6),
            "fills": len(forward),
            "newest_source_event_age_s": newest_source_event_age_s,
            "freshness_limit_s": inputs.get("freshness_limit_s", 86400.0),
            "freshness_pass": source_fresh,
        },
        "fee_aware_economics": {
            "raw_pnl_usd": raw_pnl,
            "expected_entry_and_exit_fee_usd": fee,
            "post_fee_pnl_usd": post_fee,
            "post_fee_ev_per_measured_day_usd": ev_per_day,
            "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "fee_basis": "embedded fee applied independently to entry and exit matched shares",
            "daily_raw_pnl_usd": daily_pnl,
            "worst_raw_day_pnl_usd": worst_day,
            "fill_pnl_bucket_counts": buckets,
            "study_oos_ev_per_day_usd": study_ev,
            "headline_to_study_abs_ratio": round(divergence_ratio, 6) if divergence_ratio is not None else None,
            "divergence_review_required": divergence_review_required,
            "divergence_rule": ">2x versus study OOS must be explained before promotion",
        },
        "capacity_at_live_size": {
            "evidence_order_size_usd": round(proven_size, 6),
            "validated_at_proposed_initial_size": proven_size >= 0.99,
            "max_guard_order_usd": 8.0,
            "max_member_order_usd": 2.5,
            "note": "$1 is observed; scaling above $1 requires a fresh capacity sample",
        },
        "proposed_initial_live_sizing": {
            "order_usd": 1.0,
            "within_guard_caps": True,
            "reason": "matches the forward evidence size and stays below existing 8/2.5 caps",
        },
        "copyintent_parity": {
            "status": "PASS" if parity.get("current_intents_field") == "current_intents" and parity.get("direct_submitter") is False else "FAIL",
            "adapter_contract": parity,
        },
        "single_guard_path": {
            "status": "PASS" if guard.get("direct_submitter") is False else "FAIL",
            "contract": guard,
        },
        "evidence_gate_pass": evidence_pass,
        "decision": (
            "FABLE_PROMOTION_DECISION_REQUIRED"
            if evidence_pass
            else "PARK_METHOD_LANE_PAPER_ONLY"
            if decision_due
            else "FRESH_FORWARD_EVIDENCE_REQUIRED"
        ),
        "prederived_decision_branches": {
            "before_decision_at": "ACCRUE_PAPER_ONLY_NO_LIVE_MUTATION",
            "at_or_after_decision_if_all_inputs_pass": "PROMOTE_PACKET_READY_FOR_FABLE_LIVE_DECISION",
            "at_or_after_decision_if_any_input_fails": "PARK_METHOD_LANE_PAPER_ONLY",
            "current_branch": current_branch,
            "mechanical_inputs": {
                "clock_elapsed_days_gte_3": bool(clock_elapsed_days is not None and clock_elapsed_days >= 3.0),
                "source_fresh": source_fresh,
                "forward_fills_gte_30": len(forward) >= 30,
                "cumulative_post_fee_pnl_positive": post_fee > 0.0,
                "one_dollar_capacity_validated": proven_size >= 0.99,
                "divergence_review_clear": not divergence_review_required,
                "copyintent_parity_pass": parity.get("current_intents_field") == "current_intents" and parity.get("direct_submitter") is False,
                "single_guard_path_pass": guard.get("direct_submitter") is False,
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--events", default=DEFAULT_EVENTS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    packet = build_packet(
        load_json(args.state, default={}) or {},
        _rows(Path(args.events)),
        generated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    atomic_write_json(args.output, packet)
    print(json.dumps({key: packet[key] for key in ("status", "evidence_gate_pass")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
