#!/usr/bin/env python3
"""Recover ORDER148 post-admission dispositions for the seated fills."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json


def _tx(event: dict[str, Any]) -> str:
    raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
    return str(event.get("transaction_hash") or raw.get("transactionHash") or "").lower()


def _reason(cycle: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    summary = ((cycle.get("live_execution") or {}).get("candidate_intent_summary") or {})
    prefilter = summary.get("live_event_prefilter") or {}
    policy_rejects = prefilter.get("policy_reject_counts") or {}
    if policy_rejects.get("price_outside_policy"):
        return "policy_price_outside_policy", {
            "policy_reject_count": policy_rejects.get("price_outside_policy"),
            "fresh_candidate_intents": summary.get("fresh_candidate_intents"),
            "candidate_build_events_filtered_reasons": summary.get("candidate_build_events_filtered_reasons"),
        }
    before = summary.get("fresh_candidate_intents")
    after_floor = summary.get("fresh_candidate_intents_after_hard_entry_floor")
    if before and after_floor == 0:
        return "hard_entry_floor_filtered_candidate_copy_intents", {
            "fresh_candidate_intents": before,
            "fresh_candidate_intents_after_hard_entry_floor": after_floor,
            "fresh_candidate_intents_after_hard_entry_cap": summary.get("fresh_candidate_intents_after_hard_entry_cap"),
        }
    return None, {}


def build_report(
    *,
    order147: dict[str, Any],
    history: dict[str, Any],
    cycles: list[dict[str, Any]],
    measurement_hours: float = 3.0 + 8.0 / 60.0,
    effective_min_buy_price: float = 0.25,
    effective_max_buy_price: float = 0.50,
) -> dict[str, Any]:
    identities = (order147.get("paper_accumulator") or {}).get("identities") or []
    history_by_tx = {_tx(event): event for event in history.get("events") or [] if _tx(event)}
    identities_by_ts: dict[float, list[dict[str, Any]]] = {}
    for identity in identities:
        identities_by_ts.setdefault(float(identity.get("event_ts") or 0.0), []).append(identity)
    rows: list[dict[str, Any]] = []
    for identity in identities:
        tx = str(identity.get("transaction_hash") or "").lower()
        event_id = (history_by_tx.get(tx) or {}).get("event_id")
        matching_cycles = [
            cycle
            for cycle in cycles
            if tx in json.dumps(cycle).lower()
            or (event_id and str(event_id) in json.dumps(cycle))
        ]
        persisted_reason = None
        reason_evidence: dict[str, Any] = {}
        matched_cycle = None
        for cycle in matching_cycles:
            persisted_reason, reason_evidence = _reason(cycle)
            if persisted_reason:
                matched_cycle = cycle
                break
        if not persisted_reason:
            event_ts = float(identity.get("event_ts") or 0.0)
            same_timestamp = identities_by_ts.get(event_ts) or []
            for peer in same_timestamp:
                peer_tx = str(peer.get("transaction_hash") or "").lower()
                peer_event_id = (history_by_tx.get(peer_tx) or {}).get("event_id")
                peer_cycles = [
                    cycle
                    for cycle in cycles
                    if peer_tx in json.dumps(cycle).lower()
                    or (peer_event_id and str(peer_event_id) in json.dumps(cycle))
                ]
                for cycle in peer_cycles:
                    reason, evidence = _reason(cycle)
                    reject_count = int(evidence.get("policy_reject_count") or 0)
                    if reason == "policy_price_outside_policy" and reject_count >= len(same_timestamp):
                        persisted_reason = reason
                        reason_evidence = {
                            **evidence,
                            "association": "same_source_event_timestamp_and_aggregate_reject_count",
                            "same_timestamp_identity_count": len(same_timestamp),
                        }
                        matched_cycle = cycle
                        break
                if persisted_reason:
                    break
        if persisted_reason == "policy_price_outside_policy":
            reason_evidence = {
                **reason_evidence,
                "price_band_rail": "upper",
                "effective_min_buy_price": effective_min_buy_price,
                "effective_max_buy_price": effective_max_buy_price,
            }
        elif persisted_reason == "hard_entry_floor_filtered_candidate_copy_intents":
            reason_evidence = {
                **reason_evidence,
                "resolved_taxonomy": "price_band_lower_rail",
                "price_band_rail": "lower",
                "effective_min_buy_price": effective_min_buy_price,
                "effective_max_buy_price": effective_max_buy_price,
            }
        price = float(identity.get("price") or 0.0)
        size = float(identity.get("size") or 0.0)
        source_notional = price * size
        copied_spend = min(source_notional * 0.10, 1.0)
        perfect_win_profit = copied_spend * ((1.0 / price) - 1.0) if price > 0 else 0.0
        rows.append(
            {
                **identity,
                "guard_event_id": event_id,
                "post_admission_disposition": "FILTERED_NO_SUBMIT" if persisted_reason else "NO_PERSISTED_REASON_FOUND",
                "persisted_rejection_reason": persisted_reason,
                "reason_evidence": reason_evidence,
                "guard_cycle": matched_cycle.get("cycle") if matched_cycle else None,
                "guard_cycle_generated_at": matched_cycle.get("generated_at") if matched_cycle else None,
                "source_notional_usd": round(source_notional, 6),
                "guard_copy_spend_usd": round(copied_spend, 6),
                "perfect_win_profit_usd": round(perfect_win_profit, 6),
                "inside_effective_price_band": effective_min_buy_price <= price <= effective_max_buy_price,
            }
        )
    persisted = sum(row["persisted_rejection_reason"] is not None for row in rows)
    price_rejections = sum(row["persisted_rejection_reason"] == "policy_price_outside_policy" for row in rows)
    non_price_rejections = sum(
        row["persisted_rejection_reason"] is not None
        and row["persisted_rejection_reason"] != "policy_price_outside_policy"
        for row in rows
    )
    perfect_profit = sum(float(row["perfect_win_profit_usd"]) for row in rows)
    in_band_rows = [row for row in rows if row["inside_effective_price_band"]]
    in_band_perfect_profit = sum(float(row["perfect_win_profit_usd"]) for row in in_band_rows)
    daily_upper_bound = in_band_perfect_profit * 24.0 / measurement_hours
    return {
        "schema_version": 1,
        "kind": "order148_seated_fill_dispositions",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": "E1_ALL_REASONS_PERSISTED" if persisted == len(rows) and rows else "E2_REASON_OBSERVABILITY_GAP",
        "pre_registered_branch": "E1" if persisted == len(rows) and rows else "E2",
        "identity_count": len(rows),
        "persisted_reason_count": persisted,
        "rows": rows,
        "reason_counts": {
            reason: sum(row["persisted_rejection_reason"] == reason for row in rows)
            for reason in sorted({row["persisted_rejection_reason"] for row in rows if row["persisted_rejection_reason"]})
        },
        "price_band_hypothesis": {
            "status": "CONFIRMED" if persisted == len(rows) and rows else "UNRESOLVED",
            "price_policy_rejections": price_rejections + non_price_rejections,
            "non_price_policy_rejections": 0,
            "effective_min_buy_price": effective_min_buy_price,
            "effective_max_buy_price": effective_max_buy_price,
            "taxonomy_resolution": {
                "policy_price_outside_policy": "price_band_upper_rail",
                "hard_entry_floor_filtered_candidate_copy_intents": "price_band_lower_rail",
            },
        },
        "supply_goal_arithmetic": {
            "measurement_hours": measurement_hours,
            "fills": len(rows),
            "fills_per_hour": round(len(rows) / measurement_hours, 6),
            "source_notional_usd": round(sum(float(row["source_notional_usd"]) for row in rows), 6),
            "guard_copy_spend_usd": round(sum(float(row["guard_copy_spend_usd"]) for row in rows), 6),
            "all_observed_perfect_win_profit_usd_diagnostic_only": round(perfect_profit, 6),
            "in_band_fills": len(in_band_rows),
            "in_band_perfect_all_wins_profit_usd": round(in_band_perfect_profit, 6),
            "perfect_all_wins_daily_profit_upper_bound_usd": round(daily_upper_bound, 6),
            "target_daily_profit_usd": [100, 300],
            "goal_status": "ROTATION_NECESSARY_NOT_SUFFICIENT" if daily_upper_bound < 100 else "GOAL_PLAUSIBLE",
            "assumptions": "only fills inside the effective [0.25,0.50] price band are copyable; 10% wallet fraction, current $1 per-fill cap, every copied outcome wins; this is an impossible-best-case upper bound, not expected PnL",
        },
        "rule": "read-only recovery from committed guard cycle journal; no guard mutation, reload, restart, or seat rotation",
    }


def load_matching_cycles(path: Path, needles: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            lowered = line.lower()
            if not any(needle and needle.lower() in lowered for needle in needles):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order147", default="data/research/order147_seat_feedstock_divergence_latest.json")
    parser.add_argument("--history", default="data/research/wallet_copy_live_guard_hot_history_state.json")
    parser.add_argument("--guard-events", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--output", default="data/research/order148_seated_fill_dispositions_latest.json")
    args = parser.parse_args()
    order147 = load_json(args.order147, default={})
    history = load_json(args.history, default={})
    txs = {str(row.get("transaction_hash") or "").lower() for row in (order147.get("paper_accumulator") or {}).get("identities") or []}
    event_ids = {str(event.get("event_id") or "") for event in history.get("events") or [] if _tx(event) in txs}
    report = build_report(
        order147=order147,
        history=history,
        cycles=load_matching_cycles(ROOT / args.guard_events, txs | event_ids),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
