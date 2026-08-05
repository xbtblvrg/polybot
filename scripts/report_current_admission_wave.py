#!/usr/bin/env python3
"""Emit a fresh, measurement-only admission wave from the current qualified queue."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

FRESHNESS_DEADMAN_S = 180.0
INPUT_FRESHNESS_LIMIT_S = 900.0
FORWARD_CLOCK_NOT_BEFORE = datetime(2026, 7, 24, 4, 17, 22, tzinfo=UTC).timestamp()


def _parse(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _age_s(value: Any, now: datetime) -> float | None:
    parsed = _parse(value)
    return round(max(0.0, (now - parsed).total_seconds()), 6) if parsed else None


def _wallet_from_order(order: dict[str, Any]) -> str:
    decision = order.get("trade_decision") if isinstance(order.get("trade_decision"), dict) else {}
    wallet_copy = decision.get("wallet_copy") if isinstance(decision.get("wallet_copy"), dict) else {}
    return str(wallet_copy.get("source_wallet") or "").lower()


def _order_ts(order: dict[str, Any]) -> datetime | None:
    return _parse(order.get("submitted_at") or order.get("updated_at") or order.get("created_at"))


def build_wave(
    *,
    queue: dict[str, Any],
    guard: dict[str, Any],
    ledger: dict[str, Any],
    previous_wave: dict[str, Any] | None = None,
    forward_927f: dict[str, Any] | None = None,
    forward_a689: dict[str, Any] | None = None,
    generated_at: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    generated_at = generated_at or utc_now_iso()
    now = _parse(generated_at) or datetime.now(UTC)
    cycle_started = _parse((previous_wave or {}).get("generated_at"))
    qualified = [
        row
        for row in (queue.get("ranked_members") or [])
        if isinstance(row, dict)
        and row.get("clearance_ready") is True
        and row.get("ready_for_live") is True
        and str(row.get("external_liveness_status") or "") == "PASS"
    ][: max(1, limit)]
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    runtime_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower()
        for row in (runtime.get("members") or [])
        if isinstance(row, dict)
    }
    orders_by_wallet: dict[str, list[dict[str, Any]]] = {}
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        wallet = _wallet_from_order(order)
        order_ts = _order_ts(order)
        if wallet and (cycle_started is None or (order_ts is not None and order_ts > cycle_started)):
            orders_by_wallet.setdefault(wallet, []).append(order)

    members = []
    for row in qualified:
        wallet = str(row.get("wallet") or "").lower()
        wallet_orders = orders_by_wallet.get(wallet, [])
        submitted = [
            order
            for order in wallet_orders
            if str(order.get("final_status") or "").upper() in {"SUBMITTED", "FILLED", "CANCELLED"}
        ]
        filled = [
            order
            for order in wallet_orders
            if str(order.get("final_status") or "").upper() == "FILLED"
        ]
        members.append(
            {
                "queue_rank": row.get("queue_rank"),
                "wallet": wallet,
                "candidate_id": row.get("candidate_id"),
                "policy_id": row.get("paper_policy_id") or row.get("copy_policy_family"),
                "runtime_loaded": wallet in runtime_wallets,
                "own_source_rows_30m": int(row.get("fresh_own_source_buy_rows_30m") or 0),
                "intents": len(wallet_orders),
                "submits": len(submitted),
                "fills": len(filled),
                "latest_intent_id": wallet_orders[-1].get("intent_id") if wallet_orders else None,
                "latest_order_id": wallet_orders[-1].get("order_id") if wallet_orders else None,
            }
        )
    input_timestamps = {
        "queue_generated_at": queue.get("generated_at"),
        "guard_generated_at": guard.get("generated_at"),
    }
    input_ages_s = {key: _age_s(value, now) for key, value in input_timestamps.items()}
    inputs_fresh = all(age is not None and age <= INPUT_FRESHNESS_LIMIT_S for age in input_ages_s.values())
    def _forward_seat(
        forward: dict[str, Any] | None,
        *,
        wallet: str,
        key: str,
        required_h: float,
        required_resolved: int,
    ) -> dict[str, Any]:
        state = forward or {}
        summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        diagnostics = (
            summary.get("current_poll_diagnostics")
            if isinstance(summary.get("current_poll_diagnostics"), dict)
            else {}
        )
        ladder = diagnostics.get("current_poll_ladder") if isinstance(diagnostics.get("current_poll_ladder"), dict) else {}
        eligible = int(summary.get("new_unique_buy_copy_intents") or 0)
        previous_seats = (previous_wave or {}).get("forward_seats")
        previous_seats = previous_seats if isinstance(previous_seats, dict) else {}
        prior = previous_seats.get(key) if isinstance(previous_seats.get(key), dict) else {}
        if not prior and key == "927f":
            prior = (previous_wave or {}).get("forward_927f") or {}
        clock_start = prior.get("clock_start")
        binding = prior.get("source_binding")
        max_event_age_s = float(
            ((state.get("config") or {}).get("max_copyability_event_age_s"))
            or 10.0
        )
        allowed_routes = {
            "activity:user",
            "trade:user",
            "trade:proxyWallet",
            "rtds_activity",
        }

        def _move_route(move: dict[str, Any]) -> str:
            wallet_event = move.get("wallet_event") if isinstance(move.get("wallet_event"), dict) else {}
            raw = wallet_event.get("raw") if isinstance(wallet_event.get("raw"), dict) else {}
            efficiency = move.get("copy_efficiency") if isinstance(move.get("copy_efficiency"), dict) else {}
            return str(
                raw.get("_walletCopySource")
                or wallet_event.get("source")
                or ((efficiency.get("copyability_details") or {}).get("wallet_data_api_source"))
                or ""
            )

        if clock_start and (
            not isinstance(binding, dict)
            or str(binding.get("route") or "") not in allowed_routes
            or not binding.get("source_event_id")
            or not binding.get("intent_id")
            or binding.get("event_ts") is None
            or binding.get("observed_ts") is None
            or float(binding.get("event_ts")) < FORWARD_CLOCK_NOT_BEFORE
            or float(binding.get("observed_ts")) < FORWARD_CLOCK_NOT_BEFORE
            or binding.get("age_s") is None
            or float(binding.get("age_s")) > max_event_age_s
        ):
            clock_start = None
            binding = None
        eligible_move = next(
            (
                move for move in reversed(state.get("last_moves") or [])
                if isinstance(move, dict)
                and bool(((move.get("copyability") or {}).get("accepted")))
                and bool(((move.get("copy_efficiency") or {}).get("profit_policy_accepted")))
                and bool(((move.get("copy_efficiency") or {}).get("intent_id")))
                and bool(((move.get("wallet_event") or {}).get("event_id")))
                and bool(
                    ((move.get("wallet_event") or {}).get("transaction_hash"))
                    or ((move.get("wallet_event") or {}).get("source_fingerprint"))
                )
                and (move.get("wallet_event") or {}).get("event_ts") is not None
                and (move.get("wallet_event") or {}).get("observed_ts") is not None
                and float((move.get("wallet_event") or {}).get("event_ts")) >= FORWARD_CLOCK_NOT_BEFORE
                and float((move.get("wallet_event") or {}).get("observed_ts")) >= FORWARD_CLOCK_NOT_BEFORE
                and ((move.get("copy_efficiency") or {}).get("event_age_s")) is not None
                and float((move.get("copy_efficiency") or {}).get("event_age_s")) <= max_event_age_s
                and _move_route(move) in allowed_routes
            ),
            None,
        )
        if not clock_start and eligible_move:
            clock_start = generated_at
            efficiency = eligible_move.get("copy_efficiency") or {}
            wallet_event = eligible_move.get("wallet_event") or {}
            binding = {
                "source_event_id": efficiency.get("source_event_id") or wallet_event.get("event_id"),
                "stable_identity": {
                    "market": wallet_event.get("condition_id") or wallet_event.get("market_slug"),
                    "token": wallet_event.get("token_id") or wallet_event.get("outcome"),
                    "side": wallet_event.get("action"),
                    "transaction": wallet_event.get("transaction_hash") or wallet_event.get("event_id"),
                },
                "intent_id": efficiency.get("intent_id"),
                "event_ts": wallet_event.get("event_ts"),
                "observed_ts": wallet_event.get("observed_ts"),
                "route": _move_route(eligible_move),
                "age_s": efficiency.get("event_age_s"),
                "policy_id": efficiency.get("profit_policy_id") or efficiency.get("policy_id"),
            }
        wallet_reports = summary.get("wallet_reports") if isinstance(summary.get("wallet_reports"), list) else []
        wallet_report = wallet_reports[0] if wallet_reports and isinstance(wallet_reports[0], dict) else {}
        return {
            "wallet": wallet,
            "generated_at": state.get("generated_at"),
            "paper_only": state.get("paper_only"),
            "live_orders_allowed": state.get("live_orders_allowed"),
            "policy_id": ((summary.get("profit_policy") or {}).get("policy") or {}).get("policy_id"),
            "clock_start": clock_start,
            "clock_rule": "start once, on first post-direction policy-eligible CopyIntent; never backdate",
            "required_h": required_h,
            "required_resolved": required_resolved,
            "source_binding": binding,
            "funnel": {
                "raw_rows": int(ladder.get("raw_rows") or diagnostics.get("raw_source_rows_seen") or 0),
                "profit_policy_buy_rows": int(ladder.get("profit_policy_buy_rows") or 0),
                "eligible_copyintents": eligible,
                "would_submit": int(ladder.get("paper_orders") or 0),
                "resolved_post_fee": int((summary.get("paper_summary") or {}).get("resolved_orders") or 0),
                "post_fee_pnl_usd": (summary.get("paper_summary") or {}).get("post_fee_pnl_usd"),
            },
            "skip_taxonomy": diagnostics.get("current_poll_copyability_reason_counts") or {},
            "source_latency_by_route": diagnostics.get("current_poll_source_freshness_by_source") or {},
            "source_mux": {
                **(
                    wallet_report.get("source_mux")
                    if isinstance(wallet_report.get("source_mux"), dict)
                    else {}
                ),
                "rtds_cursor": state.get("rtds_source_cursor") or {},
                "rtds_poll": state.get("source_mux") or {},
            },
            "clock_binding_status": (
                "BOUND_ACCEPTED_EXACT_POLICY_EVENT"
                if clock_start and binding
                else "FAIL_CLOSED_NO_EXPLICIT_ACCEPTED_EVENT"
            ),
        }
    seat_927f = _forward_seat(
        forward_927f,
        wallet="0x927f7694de44d19a72bce76254e628d1c141d215",
        key="927f",
        required_h=24.0,
        required_resolved=50,
    )
    seat_a689 = _forward_seat(
        forward_a689,
        wallet="0xa6896d11f76dfa2820662c1f441496f51553559b",
        key="a689",
        required_h=48.0,
        required_resolved=30,
    )
    return {
        "schema_version": 1,
        "kind": "current_qualified_admission_wave",
        "flow_stage": "DISCOVER/OBSERVE",
        "generated_at": generated_at,
        "direction_id": "2026-07-24T02:52:21Z-fable-current-fresh-output-wave",
        "paper_only": True,
        "live_mutation_allowed": False,
        "selection_rule": "current queue clearance_ready + ready_for_live + external liveness PASS",
        "input_timestamps": input_timestamps,
        "input_ages_s": input_ages_s,
        "raw_output_timestamps": {
            "cycle_started_at": (previous_wave or {}).get("generated_at"),
            "cycle_completed_at": generated_at,
        },
        "freshness_deadman": {
            "limit_s": FRESHNESS_DEADMAN_S,
            "last_output_at": generated_at,
            "status": "CLEAR",
            "firing": False,
            "rule": "consumer fires when current time minus last_output_at exceeds limit_s",
        },
        "input_freshness": {
            "limit_s": INPUT_FRESHNESS_LIMIT_S,
            "status": "CLEAR" if inputs_fresh else "STALE_INPUT_FAIL_CLOSED",
            "firing": not inputs_fresh,
        },
        "picked_count": len(members),
        "picked_wallets": [row["wallet"] for row in members],
        "runtime_loaded_count": sum(bool(row["runtime_loaded"]) for row in members),
        "own_source_rows_30m": sum(int(row["own_source_rows_30m"]) for row in members),
        "intent_count": sum(int(row["intents"]) for row in members),
        "submit_count": sum(int(row["submits"]) for row in members),
        "fill_count": sum(int(row["fills"]) for row in members),
        "forward_927f": seat_927f,
        "forward_seats": {"927f": seat_927f, "a689": seat_a689},
        "status": "FRESH_OUTPUT_WAVE_ACTIVE" if inputs_fresh else "STALE_INPUT_FAIL_CLOSED",
        "members": members,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default="data/research/wallet_copy_full_pool_member_queue.json")
    parser.add_argument("--guard", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--output", default="data/research/current_admission_wave_latest.json")
    parser.add_argument(
        "--forward-927f",
        default="data/research/927f_forward_live_tracking_state.json",
    )
    parser.add_argument("--forward-a689", default="data/research/a689_forward_live_tracking_state.json")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    output_path = ROOT / args.output
    payload = build_wave(
        queue=load_json(ROOT / args.queue, default={}) or {},
        guard=load_json(ROOT / args.guard, default={}) or {},
        ledger=load_json(ROOT / args.ledger, default={}) or {},
        previous_wave=load_json(output_path, default={}) or {},
        forward_927f=load_json(ROOT / args.forward_927f, default={}) or {},
        forward_a689=load_json(ROOT / args.forward_a689, default={}) or {},
        limit=args.limit,
    )
    atomic_write_json(output_path, payload)
    print(
        f"wave={payload['status']} picked={payload['picked_count']} "
        f"runtime={payload['runtime_loaded_count']} source={payload['own_source_rows_30m']} "
        f"intents={payload['intent_count']} submits={payload['submit_count']} fills={payload['fill_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
