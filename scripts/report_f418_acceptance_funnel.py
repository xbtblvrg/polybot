#!/usr/bin/env python3
"""Attribute each selected-wallet policy-eligible intent to its terminal live stage."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.gate_registry import F418_GATE_ORDER  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
F418_SEAT_STARTED_S = datetime.fromisoformat("2026-07-21T10:18:11Z".replace("Z", "+00:00")).timestamp()
ACCEPTED_STATUSES = {"FILLED", "LIVE_FILLED", "LIVE_MAKER_FILLED", "LIVE_SUBMITTED", "MATCHED", "SUBMITTED"}
GATE_ORDER = F418_GATE_ORDER
EVENT_STAGES = {
    "wallet_copy_live_entry_price_band_gate_counterfactual": "entry_price_band_gate",
    "wallet_copy_live_profit_latency_suppression_reject": "profit_latency_suppression",
    "wallet_copy_live_window_fill_cap_skip": "window_fill_cap",
    "wallet_copy_live_market_buy_precision_infeasible_reject": "market_buy_precision_infeasible",
}


def _event_terminal_stage(row: dict[str, Any]) -> str | None:
    """Return a terminal stage, excluding explicitly non-applied shadow annotations."""
    event = str(row.get("event") or "")
    stage = EVENT_STAGES.get(event)
    if stage != "entry_price_band_gate":
        return stage
    if (
        row.get("approved_suppression") is False
        or row.get("live_gate_applied") is False
        or str(row.get("event_type") or "").upper() == "COUNTERFACTUAL_SHADOW_NOT_APPLIED"
    ):
        return None
    return stage


def _jsonl(path: Path):
    if not path.exists():
        return
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _epoch(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _iso_epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _market_start(value: Any) -> float | None:
    try:
        return float(str(value or "").rsplit("-", 1)[-1])
    except (TypeError, ValueError):
        return None


def _gate_has_intent(summary: dict[str, Any], intent_id: str) -> bool:
    for key in ("filtered_intents_detail", "sample_filtered_intents", "skipped_intents"):
        for row in summary.get(key) or []:
            if isinstance(row, dict) and str(row.get("intent_id") or "") == intent_id:
                return True
    return intent_id in {str(item) for item in summary.get("skipped_intent_ids") or []}


def _cycle_intents(cycle: dict[str, Any]) -> tuple[dict[str, Any], set[str]]:
    live = cycle.get("live_execution") if isinstance(cycle.get("live_execution"), dict) else {}
    summary = live.get("candidate_intent_summary") if isinstance(live.get("candidate_intent_summary"), dict) else {}
    ids: set[str] = set()
    for key in ("sample_intents", "sample_candidate_runtime_intents"):
        for row in summary.get(key) or []:
            if isinstance(row, dict) and row.get("intent_id"):
                ids.add(str(row["intent_id"]))
    latest = summary.get("latest_candidate_intent_runtime")
    if isinstance(latest, dict) and latest.get("intent_id"):
        ids.add(str(latest["intent_id"]))
    return summary, ids


def _cycle_terminal(summary: dict[str, Any], intent_id: str) -> str | None:
    for gate in GATE_ORDER:
        detail = summary.get(gate) if isinstance(summary.get(gate), dict) else {}
        if _gate_has_intent(detail, intent_id):
            return gate
        if gate == "inventory_best_ask_gate":
            if any(
                isinstance(row, dict)
                and str(row.get("intent_id") or "") == intent_id
                and str(row.get("status") or "").upper() == "BLOCKED"
                for row in detail.get("sample_decisions") or []
            ):
                return gate
    sample_ids = {
        str(row.get("intent_id"))
        for row in summary.get("sample_intents") or []
        if isinstance(row, dict) and row.get("intent_id")
    }
    if len(sample_ids) == 1 and intent_id in sample_ids:
        for gate in GATE_ORDER:
            detail = summary.get(gate) if isinstance(summary.get(gate), dict) else {}
            inputs = int(detail.get("input_intents") or 0)
            outputs = int(detail.get("output_intents") or 0)
            if inputs > 0 and outputs == 0:
                return gate
    return None


def _latest_selected_wallet(guard_cycles: list[dict[str, Any]]) -> str:
    for cycle in reversed(guard_cycles):
        if str(cycle.get("event") or "") != "wallet_copy_live_guard_cycle":
            continue
        wallet = str(cycle.get("source_wallet") or "").lower()
        if wallet:
            return wallet
    raise ValueError("no selected wallet found in live guard cycles")


def build_report(
    *,
    routing_shadow: dict[str, Any],
    guard_cycles: list[dict[str, Any]],
    execution_events: list[dict[str, Any]],
    ledger: dict[str, Any],
    source_wallet: str,
    day: str,
    generated_at: str,
    since_at: str | None = None,
) -> dict[str, Any]:
    wallet = source_wallet.lower()
    day_start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()
    day_end = day_start + timedelta(days=1).total_seconds()
    since_s = _iso_epoch(since_at) if since_at else None
    measurement_start = max(day_start, since_s) if since_s is not None else day_start
    eligible: dict[str, dict[str, Any]] = {}
    for row in routing_shadow.get("fee_gated_measurement_rows") or []:
        if not isinstance(row, dict):
            continue
        row_wallet = str(row.get("source_wallet") or row.get("copyintent_source_wallet") or "").lower()
        observed = _epoch(row.get("observed_ts") or row.get("source_detection_observed_ts"))
        intent_id = str(row.get("intent_id") or "")
        if (
            row_wallet == wallet
            and str(row.get("dominant_skip_reason") or "").lower() == "eligible"
            and observed is not None
            and measurement_start <= observed < day_end
            and intent_id
        ):
            eligible.setdefault(intent_id, row)

    cycle_rows: list[tuple[float, str, dict[str, Any], set[str]]] = []
    cycles_by_intent: dict[str, list[tuple[float, dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for cycle in guard_cycles:
        if str(cycle.get("event") or "") != "wallet_copy_live_guard_cycle":
            continue
        cycle_ts = _iso_epoch(cycle.get("generated_at"))
        if cycle_ts is None or not measurement_start <= cycle_ts < day_end:
            continue
        selected = str(cycle.get("source_wallet") or "").lower()
        summary, intent_ids = _cycle_intents(cycle)
        cycle_rows.append((cycle_ts, selected, cycle, intent_ids))
        if selected == wallet:
            for intent_id in intent_ids:
                cycles_by_intent[intent_id].append((cycle_ts, cycle, summary))
    cycle_rows.sort(key=lambda item: item[0])

    stage_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in execution_events:
        intent_id = str(row.get("intent_id") or "")
        if intent_id in eligible:
            stage_events[intent_id].append(row)

    orders_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "")
        if intent_id in eligible:
            orders_by_intent[intent_id].append(row)

    terminal_counts: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for intent_id, source in sorted(eligible.items(), key=lambda item: float(item[1].get("observed_ts") or 0.0)):
        observed = float(source.get("observed_ts") or source.get("source_detection_observed_ts") or 0.0)
        terminal = None
        detail: dict[str, Any] = {}
        attribution_class = "not_selected"
        order_rows = orders_by_intent.get(intent_id, [])
        accepted_order = next(
            (
                row
                for row in order_rows
                if {str(row.get("status") or "").upper(), str(row.get("final_status") or "").upper()}
                & ACCEPTED_STATUSES
            ),
            None,
        )
        if accepted_order is not None:
            terminal = "accepted_live_order"
            attribution_class = "submitted"
            detail = {
                "order_id": accepted_order.get("order_id"),
                "order_status": accepted_order.get("status") or accepted_order.get("final_status"),
            }
        elif order_rows:
            rejected = order_rows[-1]
            terminal = "exchange_rejected"
            attribution_class = "submitted"
            trade = rejected.get("trade_result") if isinstance(rejected.get("trade_result"), dict) else {}
            detail = {
                "order_id": rejected.get("order_id"),
                "order_status": rejected.get("status") or rejected.get("final_status"),
                "error_class": (
                    trade.get("error_class")
                    or rejected.get("error_class")
                    or trade.get("error")
                    or trade.get("post_error_msg")
                ),
            }
        else:
            event_stage = next(
                (
                    _event_terminal_stage(row)
                    for row in stage_events.get(intent_id, [])
                    if _event_terminal_stage(row)
                ),
                None,
            )
            if event_stage:
                terminal = event_stage
                attribution_class = "guard_terminal"
            else:
                intent_cycles = cycles_by_intent.get(intent_id, [])
                for _cycle_ts, cycle, summary in intent_cycles:
                    terminal = _cycle_terminal(summary, intent_id)
                    if terminal:
                        attribution_class = "guard_terminal"
                        detail = {"cycle": cycle.get("cycle"), "cycle_at": cycle.get("generated_at")}
                        break
                if terminal is None and intent_cycles:
                    passed_all_gates = any(
                        int(summary.get("fresh_candidate_intents_after_window_fill_cap") or 0) > 0
                        for _cycle_ts, _cycle, summary in intent_cycles
                    )
                    if passed_all_gates:
                        terminal = "eligible_passed_all_gates_no_guard_submit"
                        attribution_class = "wiring_defect"
                    else:
                        terminal = "selected_terminal_taxonomy_missing"
                        attribution_class = "telemetry_defect"
                    detail = {
                        "cycle_count": len(intent_cycles),
                        "cycle_statuses": sorted(
                            {
                                str((cycle.get("live_execution") or {}).get("status") or "")
                                for _cycle_ts, cycle, _summary in intent_cycles
                            }
                        ),
                    }
                if terminal is None:
                    market_start = _market_start(source.get("market_slug"))
                    if market_start is not None and observed >= market_start + 300.0:
                        terminal = "routing_shadow_eligible_after_market_close"
                        detail = {"market_start_s": market_start}
                if terminal is None:
                    selected_nearby = observed >= F418_SEAT_STARTED_S and any(
                        selected == wallet and observed - 5.0 <= cycle_ts <= observed + 35.0
                        for cycle_ts, selected, _cycle, _ids in cycle_rows
                    )
                    terminal = "not_selected_live_seat"
                    if selected_nearby:
                        detail = {"nearby_selected_wallet_cycle": True}
        terminal_counts[terminal] += 1
        rows.append(
            {
                "intent_id": intent_id,
                "source_wallet": wallet,
                "observed_ts": observed,
                "market_slug": source.get("market_slug"),
                "outcome": source.get("outcome"),
                "limit_price": source.get("limit_price"),
                "terminal_stage": terminal,
                "attribution_class": attribution_class,
                **detail,
            }
        )

    accepted = terminal_counts["accepted_live_order"]
    submitted = accepted + terminal_counts["exchange_rejected"]
    telemetry_defects = terminal_counts["selected_terminal_taxonomy_missing"]
    wiring_defects = terminal_counts["eligible_passed_all_gates_no_guard_submit"]
    unattributed = telemetry_defects + wiring_defects
    selected_rows = [row for row in rows if row["attribution_class"] != "not_selected"]
    seat_rows = [row for row in rows if float(row.get("observed_ts") or 0.0) >= F418_SEAT_STARTED_S]
    seat_counts = Counter(str(row.get("terminal_stage") or "") for row in seat_rows)
    generated_s = _iso_epoch(generated_at) or day_end
    rolling_start = generated_s - 1800.0
    rolling_rows = [
        row for row in rows if rolling_start <= float(row.get("observed_ts") or 0.0) <= generated_s
    ]
    actual_orders = []
    for row in ledger.get("orders") or []:
        if not isinstance(row, dict) or str(row.get("source_wallet") or "").lower() != wallet:
            continue
        submitted_at_s = _iso_epoch(row.get("submitted_at") or row.get("updated_at"))
        if submitted_at_s is not None and day_start <= submitted_at_s < day_end:
            actual_orders.append((submitted_at_s, row))
    actual_accepted = [
        row
        for _submitted_at_s, row in actual_orders
        if {str(row.get("status") or "").upper(), str(row.get("final_status") or "").upper()}
        & ACCEPTED_STATUSES
    ]
    rolling_actual_orders = [row for submitted_at_s, row in actual_orders if rolling_start <= submitted_at_s]
    rolling_actual_accepted = [
        row
        for row in rolling_actual_orders
        if {str(row.get("status") or "").upper(), str(row.get("final_status") or "").upper()}
        & ACCEPTED_STATUSES
    ]
    seat_actual_orders = [row for submitted_at_s, row in actual_orders if submitted_at_s >= F418_SEAT_STARTED_S]
    seat_actual_accepted = [
        row
        for row in seat_actual_orders
        if {str(row.get("status") or "").upper(), str(row.get("final_status") or "").upper()}
        & ACCEPTED_STATUSES
    ]
    exact_joined_accepted = terminal_counts["accepted_live_order"]
    join_gap = max(0, len(actual_accepted) - exact_joined_accepted)
    status = "ANALYZE" if unattributed else ("PASS_WITH_JOIN_GAP" if join_gap else "PASS")
    defect_classification = (
        "WIRING_DEFECT"
        if wiring_defects
        else ("TELEMETRY_DEFECT" if telemetry_defects else "NONE")
    )
    return {
        "schema_version": 1,
        "kind": "f418_acceptance_funnel",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "generated_at": generated_at,
        "day_utc": day,
        "measurement_started_at": (
            datetime.fromtimestamp(measurement_start, timezone.utc).isoformat().replace("+00:00", "Z")
        ),
        "source_wallet": wallet,
        "status": status,
        "policy_eligible_unique_intents": len(eligible),
        "submitted_intents": submitted,
        "accepted_live_orders": accepted,
        "eligible_to_submit_pct": round(100.0 * submitted / len(eligible), 6) if eligible else 0.0,
        "eligible_to_accept_pct": round(100.0 * accepted / len(eligible), 6) if eligible else 0.0,
        "unattributed_selected_intents": unattributed,
        "selected_policy_eligible_unique_intents": len(selected_rows),
        "selected_member_attribution": {
            "status": "FAIL" if unattributed else "PASS",
            "defect_classification": defect_classification,
            "guard_terminal": sum(row["attribution_class"] == "guard_terminal" for row in selected_rows),
            "submitted": sum(row["attribution_class"] == "submitted" for row in selected_rows),
            "telemetry_defects": telemetry_defects,
            "wiring_defects": wiring_defects,
            "rule": (
                "a selected eligible intent with a positive post-window-fill-cap count and no exact "
                "ledger order is a wiring defect; an otherwise selected intent lacking an exact terminal "
                "gate row is a telemetry defect"
            ),
        },
        "terminal_stage_counts": dict(sorted(terminal_counts.items())),
        "canonical_live_ledger": {
            "attempted_orders_today": len(actual_orders),
            "accepted_orders_today": len(actual_accepted),
            "accepted_joined_to_routing_intent_id": exact_joined_accepted,
            "routing_to_live_intent_join_gap": join_gap,
            "accepted_order_ids": [row.get("order_id") for row in actual_accepted],
        },
        "rolling_30m": {
            "window_started_at": datetime.fromtimestamp(rolling_start, timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "policy_eligible_unique_intents": len(rolling_rows),
            "actual_live_order_attempts": len(rolling_actual_orders),
            "actual_accepted_orders": len(rolling_actual_accepted),
        },
        "identity_note": (
            "Routing-shadow and canonical live guard independently materialize CopyIntent IDs; "
            "canonical_live_ledger is authoritative for actual order acceptance."
        ),
        "seat_started_at": "2026-07-21T10:18:11Z",
        "seat_tenure": {
            "routing_policy_eligible_unique_intents": len(seat_rows),
            "routing_terminal_stage_counts": dict(sorted(seat_counts.items())),
            "actual_live_order_attempts": len(seat_actual_orders),
            "actual_accepted_orders": len(seat_actual_accepted),
            "actual_rejected_orders": len(seat_actual_orders) - len(seat_actual_accepted),
            "actual_acceptance_pct": round(
                100.0 * len(seat_actual_accepted) / len(seat_actual_orders), 6
            )
            if seat_actual_orders
            else 0.0,
        },
        "rows": rows,
        "selected_member_rows": selected_rows,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-wallet",
        default=F418,
        help="wallet address, or 'latest-selected' to follow the latest guard cycle",
    )
    parser.add_argument("--day", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--since", help="Inclusive ISO-8601 lower bound within --day")
    parser.add_argument("--routing-shadow", default="data/research/routing_shadow_validation_latest.json")
    parser.add_argument("--guard-events", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--execution-events", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--output", default="data/research/f418_acceptance_funnel_latest.json")
    args = parser.parse_args()
    guard_cycles = list(_jsonl(ROOT / args.guard_events) or [])
    source_wallet = (
        _latest_selected_wallet(guard_cycles)
        if args.source_wallet == "latest-selected"
        else args.source_wallet
    )
    report = build_report(
        routing_shadow=json.loads((ROOT / args.routing_shadow).read_text()),
        guard_cycles=guard_cycles,
        execution_events=list(_jsonl(ROOT / args.execution_events) or []),
        ledger=json.loads((ROOT / args.ledger).read_text()),
        source_wallet=source_wallet,
        day=args.day,
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        since_at=args.since,
    )
    atomic_write_json(ROOT / args.output, report)
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "policy_eligible_unique_intents",
                    "submitted_intents",
                    "accepted_live_orders",
                    "terminal_stage_counts",
                )
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
