#!/usr/bin/env python3
"""Assemble counted E1 framework-audit inputs without mutating live trading."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_288_participation_map import build_map  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.gate_registry import PRE_SUBMIT_REFUSAL_CLASSES  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DATA_DIR = ROOT / "data/research"
DEFAULT_SCORECARD_CYCLE_S = 3600.0


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

TERMINAL_GATE_ORDER = (
    "not_selected_live_seat",
    "drift_buffer",
    "inventory_best_ask_gate",
    "live_hard_entry_cap",
    "live_hard_entry_floor",
    "entry_price_band_gate",
    "profit_latency_suppression",
    "toxicity_protection",
    "live_min_order_floor",
    "market_buy_precision_infeasible",
    "expected_fee_capture_gate",
    "window_fill_cap",
    "exchange_rejected",
)
REJECT_TAXONOMY = (
    "fak_no_match",
    "policy_cap_maker_fallback",
    "policy_cap_passive_at_source",
    "policy_cap_other",
    "venue_min_share_hard_ceiling_exceeded",
    "entry_price_band_closed_negative_holdout",
    "passive_at_source_lane_closed",
    "unknown",
)


def daily_gate_conversion(funnel: dict[str, Any]) -> dict[str, Any]:
    """Turn mutually exclusive terminal attribution into explicit gate survival math."""
    counts = Counter(
        {str(key): int(value or 0) for key, value in (funnel.get("terminal_stage_counts") or {}).items()}
    )
    inputs = int(funnel.get("policy_eligible_unique_intents") or sum(counts.values()))
    survivors = inputs
    rows: list[dict[str, Any]] = []
    for gate in TERMINAL_GATE_ORDER:
        rejected = counts.pop(gate, 0)
        if rejected == 0:
            continue
        output = max(0, survivors - rejected)
        rows.append(
            {
                "gate": gate,
                "signals_in": survivors,
                "rejected_at_gate": rejected,
                "survivors_out": output,
                "conversion_pct": round(100.0 * output / survivors, 6) if survivors else 0.0,
            }
        )
        survivors = output
    accepted = counts.pop("accepted_live_order", 0)
    for gate, rejected in sorted(counts.items()):
        output = max(0, survivors - rejected)
        rows.append(
            {
                "gate": gate,
                "signals_in": survivors,
                "rejected_at_gate": rejected,
                "survivors_out": output,
                "conversion_pct": round(100.0 * output / survivors, 6) if survivors else 0.0,
                "taxonomy_status": "UNORDERED_TERMINAL_STAGE",
            }
        )
        survivors = output
    accounted = sum(int(row["rejected_at_gate"]) for row in rows) + accepted
    return {
        "day_utc": funnel.get("day_utc"),
        "source_wallet": funnel.get("source_wallet"),
        "scope": "unique policy-eligible CopyIntents; each intent has exactly one terminal stage",
        "scope_exclusion": "upstream source signals rejected before policy eligibility are not present in this funnel",
        "policy_eligible_signals_in": inputs,
        "rows": rows,
        "accepted_live_orders_joined": accepted,
        "terminal_accounted_intents": accounted,
        "accounting_gap": inputs - accounted,
    }


def highest_rejection_gate_ev(
    conversion: dict[str, Any], funnel: dict[str, Any], routing_shadow: dict[str, Any]
) -> dict[str, Any]:
    rows = conversion.get("rows") or []
    highest = max(rows, key=lambda row: int(row.get("rejected_at_gate") or 0), default={})
    gate = str(highest.get("gate") or "")
    intent_ids = {
        str(row.get("intent_id") or "")
        for row in funnel.get("rows") or []
        if isinstance(row, dict) and str(row.get("terminal_stage") or "") == gate
    }
    paper_by_intent: dict[str, dict[str, Any]] = {}
    for row in routing_shadow.get("fee_gated_measurement_rows") or []:
        intent_id = str(row.get("intent_id") or "") if isinstance(row, dict) else ""
        if not isinstance(row, dict) or intent_id not in intent_ids:
            continue
        outcome = row.get("realized_paper_outcome") if isinstance(row.get("realized_paper_outcome"), dict) else {}
        if str(outcome.get("resolution_status") or outcome.get("status") or "").upper() != "RESOLVED":
            continue
        pnl = float(outcome.get("paper_pnl_usd") or 0.0)
        cost = float(row.get("shares") or 0.0) * float(row.get("limit_price") or 0.0)
        paper_by_intent.setdefault(
            intent_id,
            {"intent_id": intent_id, "paper_pnl_usd": pnl, "paper_cost_usd": cost},
        )
    paper_rows = list(paper_by_intent.values())
    pnl = sum(float(row["paper_pnl_usd"]) for row in paper_rows)
    cost = sum(float(row["paper_cost_usd"]) for row in paper_rows)
    return {
        "gate": gate or None,
        "rejected_intents": int(highest.get("rejected_at_gate") or 0),
        "resolved_counterfactual_intents": len(paper_rows),
        "unresolved_or_unjoined_intents": max(0, len(intent_ids) - len(paper_rows)),
        "paper_counterfactual_pnl_usd": round(pnl, 6),
        "paper_counterfactual_cost_usd": round(cost, 6),
        "paper_counterfactual_roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
        "decision": "MEASUREMENT_ONLY_NO_LIVE_GATE_CHANGE",
        "caveat": (
            "Paper outcome is valued at the observed source entry and does not prove executable "
            "fill price, liquidity, latency, or independence across intents."
        ),
    }


def multi_day_roi_distribution(scorecards: list[dict[str, Any]], *, since_day: str) -> dict[str, Any]:
    daily = []
    for scorecard in scorecards:
        day = str(scorecard.get("day_utc") or "")
        total = (scorecard.get("today") or {}).get("total") or {}
        cost = float(total.get("cost_usd") or 0.0)
        if day < since_day or cost <= 0:
            continue
        pnl = float(total.get("pnl_usd") or 0.0)
        daily.append(
            {
                "day_utc": day,
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
                "roi_pct": round(100.0 * pnl / cost, 6),
                "resolved_fills": int(total.get("resolved_fills") or 0),
            }
        )
    daily.sort(key=lambda row: row["day_utc"])
    rois = [float(row["roi_pct"]) for row in daily]
    aggregate_cost = sum(float(row["cost_usd"]) for row in daily)
    aggregate_pnl = sum(float(row["pnl_usd"]) for row in daily)
    size_steps = []
    for size in (4.0, 8.0):
        size_steps.append(
            {
                "base_size_usd": size,
                "full_288_window_expected_pnl_at_aggregate_roi_usd": round(
                    288.0 * size * aggregate_pnl / aggregate_cost, 6
                ) if aggregate_cost else None,
                "full_288_window_pnl_at_worst_daily_roi_usd": round(
                    288.0 * size * min(rois) / 100.0, 6
                ) if rois else None,
            }
        )
    return {
        "since_day_utc": since_day,
        "basis": "canonical end-of-day scorecards; partial current day included and labeled by day",
        "days_with_cost": len(daily),
        "daily": daily,
        "distribution": {
            "positive_days": sum(roi > 0 for roi in rois),
            "negative_days": sum(roi < 0 for roi in rois),
            "median_daily_roi_pct": round(statistics.median(rois), 6) if rois else None,
            "mean_daily_roi_pct": round(statistics.mean(rois), 6) if rois else None,
            "worst_daily_roi_pct": round(min(rois), 6) if rois else None,
            "best_daily_roi_pct": round(max(rois), 6) if rois else None,
            "aggregate_cost_usd": round(aggregate_cost, 6),
            "aggregate_pnl_usd": round(aggregate_pnl, 6),
            "aggregate_roi_pct": round(100.0 * aggregate_pnl / aggregate_cost, 6) if aggregate_cost else None,
        },
        "candidate_size_steps": size_steps,
        "projection_caveat": "Linear scaling assumes unchanged fill rate, liquidity, slippage, fees, and edge; it is not live authority.",
        "decision": "PREPARED_FOR_FRAMEWORK_AUDIT_NO_LIVE_SIZE_CHANGE",
    }


def _classify_reject(order: dict[str, Any]) -> str:
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    payload = lifecycle[-1].get("payload") if lifecycle and isinstance(lifecycle[-1], dict) else {}
    error = str(
        (payload.get("error") if isinstance(payload, dict) else "")
        or order.get("error")
        or (order.get("trade_result") or {}).get("error")
        or ""
    ).lower()
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    error_class = str(order.get("error_class") or trade_result.get("error_class") or "").lower()
    if error_class in PRE_SUBMIT_REFUSAL_CLASSES and error_class != "maker_min_share_bump_exceeds_policy_cap":
        return error_class
    if "no orders found to match with fak order" in error:
        return "fak_no_match"
    if "min-share bump would exceed wallet-copy policy cap" in error:
        decision = (
            order.get("trade_decision")
            if isinstance(order.get("trade_decision"), dict)
            else {}
        )
        strategy_reason = str(decision.get("strategy_reason") or "")
        if strategy_reason == "wallet_copy_fak_miss_maker_fallback":
            return "policy_cap_maker_fallback"
        if strategy_reason == "wallet_copy_passive_at_source":
            return "policy_cap_passive_at_source"
        return "policy_cap_other"
    if "clob five-share minimum would exceed the fable-ruled hard ceiling" in error:
        return "venue_min_share_hard_ceiling_exceeded"
    return "unknown"


def counted_reject_cluster(
    ledger: dict[str, Any], *, since: str, until: str = "", standard_drip_cap_usd: float = 2.5
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    taxonomy: Counter[str] = Counter()
    ruled_ceiling_refused = 0
    ledger_newest_submitted_at = max(
        (
            str(order.get("submitted_at") or "")
            for order in ledger.get("orders") or []
            if isinstance(order, dict) and order.get("submitted_at")
        ),
        default=None,
    )
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        submitted_at = str(order.get("submitted_at") or order.get("updated_at") or "")
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if submitted_at < since or (until and submitted_at >= until) or status != "REJECTED":
            continue
        category = _classify_reject(order)
        taxonomy[category] += 1
        limit_price = float(order.get("limit_price") or 0.0)
        fallback_notional = 5.0 * limit_price
        is_ruled_ceiling_refused = (
            category == "policy_cap_maker_fallback"
            and fallback_notional <= standard_drip_cap_usd
        )
        ruled_ceiling_refused += int(is_ruled_ceiling_refused)
        rows.append(
            {
                "submitted_at": submitted_at,
                "intent_id": order.get("intent_id"),
                "market_slug": order.get("market_slug"),
                "execution_role": order.get("execution_role"),
                "category": category,
                "limit_price": limit_price,
                "clob_min_share_notional_usd": round(fallback_notional, 6),
                "ruled_ceiling_refused_by_tighter_cap": is_ruled_ceiling_refused,
            }
        )
    return {
        "since": since,
        "until_exclusive": until or None,
        "ledger_newest_submitted_at": ledger_newest_submitted_at,
        "counting_basis": (
            "ledger orders; one row per recorded reject; policy-cap classes split "
            "by exact trade_decision.strategy_reason"
        ),
        "reject_rows": len(rows),
        "distinct_intents": len({row["intent_id"] for row in rows}),
        "taxonomy_counts": dict(sorted(taxonomy.items())),
        "ruled_ceiling_refused_by_tighter_cap_rejects": ruled_ceiling_refused,
        "standard_drip_cap_usd": standard_drip_cap_usd,
        "rows": rows,
    }


def maker_fallback_activation_evidence(
    ledger: dict[str, Any],
    scorecard: dict[str, Any],
    *,
    since: str,
    until: str,
) -> dict[str, Any]:
    """Measure only the fallback class whose cap behavior changed."""
    attempts: list[dict[str, Any]] = []
    window_orders: list[dict[str, Any]] = []
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        submitted_at = str(order.get("submitted_at") or "")
        decision = order.get("trade_decision") if isinstance(order.get("trade_decision"), dict) else {}
        in_window = submitted_at >= since and (not until or submitted_at < until)
        if in_window:
            window_orders.append(order)
        if in_window and str(decision.get("strategy_reason") or "") == (
            "wallet_copy_fak_miss_maker_fallback"
        ):
            attempts.append(order)

    submitted = [
        row
        for row in attempts
        if str(row.get("final_status") or row.get("status") or "").upper()
        in {"SUBMITTED", "FILLED"}
    ]
    rejected = [
        row
        for row in attempts
        if str(row.get("final_status") or row.get("status") or "").upper() == "REJECTED"
    ]
    skipped = [row for row in attempts if row not in submitted and row not in rejected]

    def _number(row: dict[str, Any], key: str) -> float | None:
        value = row.get(key)
        return (
            float(value)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else None
        )

    bumped_submitted = []
    for row in submitted:
        cap = _number(row, "maker_min_share_effective_cap_usd")
        cost = _number(row, "maker_min_share_bump_cost_usd")
        if cap is not None and cost is not None and cost <= cap + 1e-9:
            bumped_submitted.append(row)
    still_refused = []
    refused_inside_cap = []
    for row in rejected:
        result = row.get("trade_result") if isinstance(row.get("trade_result"), dict) else {}
        error_class = str(row.get("error_class") or result.get("error_class") or "")
        if error_class not in PRE_SUBMIT_REFUSAL_CLASSES:
            continue
        still_refused.append(row)
        cap = _number(row, "maker_min_share_effective_cap_usd")
        cost = _number(row, "maker_min_share_bump_cost_usd")
        if cap is not None and cost is not None and cost <= cap + 1e-9:
            refused_inside_cap.append(row)

    if not attempts:
        verdict = "INSUFFICIENT_SUPPLY"
    elif refused_inside_cap:
        verdict = "FAIL_REFUSED_INSIDE_EFFECTIVE_CAP"
    elif bumped_submitted and all(
        _number(row, "maker_min_share_bump_cost_usd") is not None
        and _number(row, "maker_min_share_effective_cap_usd") is not None
        and _number(row, "maker_min_share_bump_cost_usd")
        > _number(row, "maker_min_share_effective_cap_usd") + 1e-9
        for row in still_refused
    ):
        verdict = "PASS"
    else:
        verdict = "ACCRUING_NO_DECISIVE_ROW"

    events = (scorecard.get("canonical_pnl_truth") or {}).get("events") or []
    event_by_order = {
        str(row.get("order_id") or ""): row
        for row in events
        if isinstance(row, dict) and row.get("order_id")
    }
    bumped_fills = [
        row
        for row in bumped_submitted
        if str(row.get("final_status") or "").upper() == "FILLED"
    ]
    resolved_bumped = [
        (row, event_by_order.get(str(row.get("order_id") or "")))
        for row in bumped_fills
        if isinstance(event_by_order.get(str(row.get("order_id") or "")), dict)
        and bool(event_by_order[str(row.get("order_id") or "")].get("resolved"))
    ]
    effective_notional = sum(
        _number(row, "maker_min_share_bump_cost_usd")
        or (5.0 * float(row.get("limit_price") or 0.0))
        for row, _event in resolved_bumped
    )
    post_fee_pnl = sum(float(event.get("pnl_usd") or 0.0) for _row, event in resolved_bumped)
    reject_taxonomy = Counter({category: 0 for category in REJECT_TAXONOMY})
    reject_taxonomy.update(_classify_reject(row) for row in rejected)
    key_bearing_attempts = [
        row
        for row in attempts
        if _number(row, "maker_min_share_effective_cap_usd") is not None
        and _number(row, "maker_min_share_bump_cost_usd") is not None
    ]
    hard_ceiling_breaches = [
        row
        for row in attempts
        if (_number(row, "maker_min_share_bump_cost_usd") or 0.0) > 2.5 + 1e-9
    ]
    scope_leaks = [
        row
        for row in window_orders
        if str(row.get("final_status") or row.get("status") or "").upper()
        in {"SUBMITTED", "FILLED"}
        and _number(row, "maker_min_share_effective_cap_usd") is not None
        and _number(row, "maker_min_share_bump_cost_usd") is not None
        and _number(row, "maker_min_share_effective_cap_usd")
        >= 2.5 - 1e-9
        and str((row.get("trade_decision") or {}).get("strategy_reason") or "")
        != "wallet_copy_fak_miss_maker_fallback"
    ]
    submitted_by_hour = Counter(
        str(row.get("submitted_at") or "")[:13] + ":00Z"
        for row in bumped_submitted
    )
    since_topup = (
        scorecard.get("since_topup_truth")
        if isinstance(scorecard.get("since_topup_truth"), dict)
        else {}
    )
    since_topup_actual = since_topup.get("actual_delta_vs_baseline_usd")
    since_topup_actual = (
        float(since_topup_actual)
        if isinstance(since_topup_actual, (int, float))
        and not isinstance(since_topup_actual, bool)
        else None
    )
    conservation = {
        "submitted": len(submitted),
        "rejected": len(rejected),
        "skipped": len(skipped),
        "attempts": len(attempts),
    }
    conservation["gap"] = (
        conservation["attempts"]
        - conservation["submitted"]
        - conservation["rejected"]
        - conservation["skipped"]
    )
    return {
        "scope": "trade_decision.strategy_reason == wallet_copy_fak_miss_maker_fallback",
        "since_inclusive": since,
        "until_exclusive": until or None,
        "addressable_attempts": len(attempts),
        "bumped_submitted": len(bumped_submitted),
        "cap_keys_present_attempts": len(key_bearing_attempts),
        "cap_keys_missing_attempts": len(attempts) - len(key_bearing_attempts),
        "bumped_submitted_by_utc_hour": dict(sorted(submitted_by_hour.items())),
        "abort_a_threshold_per_utc_hour": 3,
        "abort_a_triggered_hours": sorted(
            hour for hour, count in submitted_by_hour.items() if count >= 3
        ),
        "abort_c_hard_ceiling_breaches": len(hard_ceiling_breaches),
        "abort_c_scope_leaks": len(scope_leaks),
        "bumped_filled": len(bumped_fills),
        "bumped_fill_rate_pct": (
            round(100.0 * len(bumped_fills) / len(bumped_submitted), 6)
            if bumped_submitted
            else None
        ),
        "still_refused": len(still_refused),
        "refused_inside_effective_cap": len(refused_inside_cap),
        "reject_taxonomy_counts": dict(sorted(reject_taxonomy.items())),
        "conservation": conservation,
        "resolved_bumped_fills": len(resolved_bumped),
        "unresolved_bumped_fills": len(bumped_fills) - len(resolved_bumped),
        "effective_notional_usd": round(effective_notional, 6),
        "post_fee_pnl_usd": round(post_fee_pnl, 6),
        "post_fee_roi_pct": (
            round(100.0 * post_fee_pnl / effective_notional, 6)
            if effective_notional
            else None
        ),
        "abort_b": {
            "since_topup_actual_usd": since_topup_actual,
            "probe_trigger_usd": -8.0,
            "probe_triggered": (
                since_topup_actual <= -8.0 if since_topup_actual is not None else None
            ),
            "kill_trigger_usd": -35.0,
            "kill_triggered": (
                since_topup_actual <= -35.0 if since_topup_actual is not None else None
            ),
            "resolved_bumped_fill_threshold": 30,
            "negative_bumped_class_triggered": (
                len(resolved_bumped) >= 30 and post_fee_pnl < 0.0
            ),
        },
        "verdict": verdict,
        "verdict_rule": (
            "PASS iff >=1 key-bearing bumped submit and every policy-cap refusal is above "
            "its emitted effective cap (the refusal condition is vacuously true when there "
            "are no such refusals); FAIL on any refusal inside cap; zero attempts is "
            "INSUFFICIENT_SUPPLY"
        ),
    }


def build_packet(
    scorecard: dict[str, Any],
    ledger: dict[str, Any],
    *,
    day: str,
    reject_since: str,
    reject_until: str = "",
    activation_since: str = "",
    acceptance_funnel: dict[str, Any] | None = None,
    routing_shadow: dict[str, Any] | None = None,
    scorecards: list[dict[str, Any]] | None = None,
    since_day: str = "2026-07-05",
    scorecard_input_path: str = "",
    scorecard_cycle_s: float = DEFAULT_SCORECARD_CYCLE_S,
) -> dict[str, Any]:
    participation_map = build_map(scorecard, day=day)
    day_start = f"{day}T00:00:00Z"
    day_end = f"{date.fromisoformat(day) + timedelta(days=1)}T00:00:00Z"
    generated_at = utc_now_iso()
    generated_dt = _parse_ts(generated_at)
    scorecard_generated_at = scorecard.get("generated_at")
    scorecard_generated_dt = _parse_ts(scorecard_generated_at)
    scorecard_lag_s = (
        max(0.0, (generated_dt - scorecard_generated_dt).total_seconds())
        if generated_dt is not None and scorecard_generated_dt is not None
        else None
    )
    scorecard_freshness_status = (
        "UNKNOWN_SCORECARD_GENERATED_AT"
        if scorecard_lag_s is None
        else "STALE_BEYOND_ONE_CYCLE"
        if scorecard_lag_s > float(scorecard_cycle_s)
        else "PASS_WITHIN_ONE_CYCLE"
    )
    packet = {
        "kind": "e1_framework_audit_inputs",
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN/DEFEND/SELF-DEV",
        "generated_at": generated_at,
        "scorecard_input_path": scorecard_input_path or None,
        "scorecard_input_generated_at": scorecard_generated_at,
        "e1_vs_scorecard_lag_s": scorecard_lag_s,
        "scorecard_cycle_s": float(scorecard_cycle_s),
        "scorecard_input_freshness_status": scorecard_freshness_status,
        "day_utc": day,
        "measurement_only": True,
        "live_mutation_allowed": False,
        "defense_regret": scorecard.get("defense_regret") or {},
        "reject_cluster": counted_reject_cluster(ledger, since=reject_since, until=reject_until),
        "full_utc_day_reject_cluster": counted_reject_cluster(
            ledger,
            since=day_start,
            until=day_end,
        ),
        "maker_fallback_activation_evidence": maker_fallback_activation_evidence(
            ledger,
            scorecard,
            since=activation_since or day_start,
            until=day_end,
        ),
        "participation_288_map_summary": participation_map["summary"],
        "participation_288_map": participation_map,
    }
    if acceptance_funnel is not None:
        conversion = daily_gate_conversion(acceptance_funnel)
        packet["daily_gate_conversion"] = conversion
        packet["highest_rejection_gate_ev"] = highest_rejection_gate_ev(
            conversion, acceptance_funnel, routing_shadow or {}
        )
    if scorecards is not None:
        packet["multi_day_roi_distribution"] = multi_day_roi_distribution(
            scorecards, since_day=since_day
        )
    return packet


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--reject-since", required=True)
    parser.add_argument("--reject-until", default="", help="Exclusive UTC bound that freezes the named cluster.")
    parser.add_argument(
        "--activation-since",
        default="",
        help="Inclusive UTC activation timestamp; defaults to the UTC day start.",
    )
    parser.add_argument("--scorecard", required=True)
    parser.add_argument("--ledger", default=str(DATA_DIR / "wallet_copy_live_execution_state.json"))
    parser.add_argument("--acceptance-funnel", default=str(DATA_DIR / "f418_acceptance_funnel_latest.json"))
    parser.add_argument("--routing-shadow", default=str(DATA_DIR / "routing_shadow_validation_latest.json"))
    parser.add_argument("--since-day", default="2026-07-05")
    parser.add_argument("--output", default="")
    parser.add_argument("--map-output", default="")
    args = parser.parse_args()
    scorecard = load_json(Path(args.scorecard), default={})
    ledger = load_json(Path(args.ledger), default={})
    acceptance_funnel = load_json(Path(args.acceptance_funnel), default={})
    routing_shadow = load_json(Path(args.routing_shadow), default={})
    scorecards = []
    for path in sorted(DATA_DIR.glob("wallet_copy_daily_scorecard_????-??-??.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            scorecards.append(value)
    packet = build_packet(
        scorecard,
        ledger,
        day=args.day,
        reject_since=args.reject_since,
        reject_until=args.reject_until,
        activation_since=args.activation_since,
        acceptance_funnel=acceptance_funnel,
        routing_shadow=routing_shadow,
        scorecards=scorecards,
        since_day=args.since_day,
        scorecard_input_path=args.scorecard,
    )
    output = Path(args.output) if args.output else DATA_DIR / f"e1_framework_audit_inputs_{args.day}.json"
    map_output = Path(args.map_output) if args.map_output else DATA_DIR / f"btc5m_288_participation_map_{args.day}.json"
    atomic_write_json(output, packet)
    atomic_write_json(map_output, packet["participation_288_map"])
    print(
        f"wrote {output}; map={map_output}; rejects={packet['reject_cluster']['taxonomy_counts']}; "
        "ruled_ceiling_refused="
        f"{packet['reject_cluster']['ruled_ceiling_refused_by_tighter_cap_rejects']}; "
        f"full_day_rejects={packet['full_utc_day_reject_cluster']['taxonomy_counts']}; "
        f"highest_gate={(packet.get('highest_rejection_gate_ev') or {}).get('gate')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
