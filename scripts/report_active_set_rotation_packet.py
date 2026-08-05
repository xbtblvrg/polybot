#!/usr/bin/env python3
"""Pre-stage the active-set liveness rotation packet without live mutation."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_HISTORY = ROOT / "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_ROUTING = ROOT / "data/research/routing_shadow_validation_latest.json"
DEFAULT_READY_SHADOW = ROOT / "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_LIVE_EXECUTION = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/active_set_rotation_packet_latest.json"
DEFAULT_CANDIDATES = (
    "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
    "0xa6896d11f76dfa2820662c1f441496f51553559b",
    "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
)
# Fable QC-1 recompute (2026-07-18T05:56Z): selected chatter is not tenure,
# and the current rotation clock is anchored no later than the last accepted
# live submit at 2026-07-17T23:55:03.914090Z.
DEFAULT_QUIET_ANCHOR_TS = 1784332503.91409
DEFAULT_QUIET_HOURS = 4.0
QUIET_RESET_PREDICATE = "selected_submit_eligible_copyintent"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") else ""


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _iso_from_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _timestamp_candidates(row: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for key in ("submitted_at", "accepted_at", "updated_at", "created_at", "timestamp", "ts"):
        parsed = _parse_ts(row.get(key))
        if parsed is not None:
            values.append(parsed.timestamp())
        numeric = _as_float(row.get(key))
        if numeric is not None:
            values.append(numeric)
    result = row.get("trade_result") if isinstance(row.get("trade_result"), dict) else {}
    parsed = _parse_ts(result.get("timestamp"))
    if parsed is not None:
        values.append(parsed.timestamp())
    lifecycle = row.get("lifecycle") if isinstance(row.get("lifecycle"), list) else []
    for event in lifecycle:
        if not isinstance(event, dict):
            continue
        parsed = _parse_ts(event.get("ts"))
        if parsed is not None:
            values.append(parsed.timestamp())
    return values


def _latest_selected_submit_attempt(live_execution: dict[str, Any], selected_wallet: str) -> dict[str, Any]:
    selected_wallet = _wallet(selected_wallet)
    latest_ts: float | None = None
    latest_row: dict[str, Any] = {}
    orders = live_execution.get("orders") if isinstance(live_execution.get("orders"), list) else []
    for row in orders:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet"))
        if not wallet:
            intent = row.get("intent") if isinstance(row.get("intent"), dict) else {}
            wallet = _wallet(intent.get("source_wallet"))
        if wallet != selected_wallet:
            continue
        status_values = {
            str(row.get("status") or "").upper(),
            str(row.get("final_status") or "").upper(),
        }
        lifecycle = row.get("lifecycle") if isinstance(row.get("lifecycle"), list) else []
        status_values.update(str(event.get("status") or "").upper() for event in lifecycle if isinstance(event, dict))
        if not any(status for status in status_values if "SUBMIT" in status or "FILL" in status or "ACCEPT" in status):
            continue
        row_ts = max(_timestamp_candidates(row), default=None)
        if row_ts is None:
            continue
        if latest_ts is None or row_ts > latest_ts:
            latest_ts = row_ts
            latest_row = row
    if latest_ts is None:
        return {}
    return {
        "ts": latest_ts,
        "iso": _iso_from_ts(latest_ts),
        "order_id": latest_row.get("order_id") or latest_row.get("clob_order_id"),
        "intent_id": latest_row.get("intent_id") or latest_row.get("copy_intent_id"),
        "status": latest_row.get("status"),
        "final_status": latest_row.get("final_status"),
        "market_slug": latest_row.get("market_slug"),
    }


def _now_ts(now_iso: str | None) -> tuple[str, float]:
    if now_iso:
        parsed = _parse_ts(now_iso)
        if parsed is not None:
            return parsed.isoformat().replace("+00:00", "Z"), parsed.timestamp()
    now = _parse_ts(utc_now_iso())
    if now is None:
        now = datetime.now(timezone.utc)
    return now.isoformat().replace("+00:00", "Z"), now.timestamp()


def _live_execution_active_set(live_execution: dict[str, Any] | None) -> dict[str, Any]:
    live_execution = live_execution if isinstance(live_execution, dict) else {}
    return _dict(_dict(_dict(live_execution.get("runtime_permission")).get("details")).get("active_set"))


def _current_member(active_set: dict[str, Any]) -> dict[str, Any]:
    selected = _dict(active_set.get("selected_member"))
    if selected:
        return selected
    current = [row for row in _list(active_set.get("members")) if isinstance(row, dict) and row.get("is_current_cycle_member")]
    return current[0] if current else {}


def _selected_member(guard: dict[str, Any], live_execution: dict[str, Any] | None = None) -> dict[str, Any]:
    selected = _current_member(_live_execution_active_set(live_execution))
    if selected:
        return selected
    runtime = _dict(guard.get("active_set_runtime"))
    selected = _current_member(runtime)
    if selected:
        return selected
    active_set = _dict(guard.get("active_set"))
    return _current_member(active_set)


def _active_runtime_members(
    guard: dict[str, Any],
    live_execution: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    runtime = _dict(guard.get("active_set_runtime"))
    members = _list(runtime.get("members"))
    if not members:
        members = _list(_dict(guard.get("active_set")).get("members"))
    live_members = _list(_live_execution_active_set(live_execution).get("members"))
    if live_members:
        members = [*members, *live_members]
    out: dict[str, dict[str, Any]] = {}
    for row in members:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
        if wallet:
            out[wallet] = row
    return out


def _history_events(history: dict[str, Any], wallet: str, *, since_ts: float, now_ts: float) -> list[dict[str, Any]]:
    rows = []
    for row in _list(history.get("events")):
        if not isinstance(row, dict):
            continue
        if _wallet(row.get("source_wallet")) != wallet:
            continue
        event_ts = _as_float(row.get("event_ts"))
        if event_ts is None or event_ts < since_ts or event_ts > now_ts + 300:
            continue
        if str(row.get("action") or "").upper() != "BUY":
            continue
        if str(row.get("asset") or "").upper() != "BTC":
            continue
        if str(row.get("duration") or "").lower() not in {"5m", "5min", "5"}:
            continue
        rows.append(row)
    return rows


def _latency_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values: list[float] = []
    for row in rows:
        event_ts = _as_float(row.get("event_ts"))
        observed_ts = _as_float(row.get("observed_ts"))
        if event_ts is None or observed_ts is None:
            continue
        values.append(max(0.0, observed_ts - event_ts))
    values.sort()

    def percentile(pct: float) -> float | None:
        if not values:
            return None
        if len(values) == 1:
            return round(values[0], 6)
        rank = (len(values) - 1) * pct
        lo = int(rank)
        hi = min(lo + 1, len(values) - 1)
        weight = rank - lo
        return round(values[lo] * (1 - weight) + values[hi] * weight, 6)

    return {
        "local_entry_latency_count": len(values),
        "local_entry_latency_p50_s": percentile(0.5),
        "local_entry_latency_p90_s": percentile(0.9),
        "local_entry_latency_max_s": round(values[-1], 6) if values else None,
    }


def _premerge_index(guard: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = _list(_dict(guard.get("active_set_rtds_premerge")).get("rows"))
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet"))
        if wallet:
            out[wallet] = row
    return out


def _routing_post_fee_index(routing: dict[str, Any]) -> dict[str, dict[str, Any]]:
    retained = _dict(_dict(_dict(routing.get("summary")).get("fee_gate_calibration_retained")).get("by_member"))
    return {_wallet(wallet): _dict(row) for wallet, row in retained.items() if _wallet(wallet)}


def _ready_shadow_index(ready_shadow: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _list(ready_shadow.get("lanes")):
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet:
            out[wallet] = row
    return out


def _candidate_row(
    *,
    wallet: str,
    member: dict[str, Any],
    history_rows: list[dict[str, Any]],
    premerge: dict[str, Any],
    routing_fee: dict[str, Any],
    ready_shadow: dict[str, Any],
    trailing_hours: float,
) -> dict[str, Any]:
    latest_event_ts = max((_as_float(row.get("event_ts")) or 0.0 for row in history_rows), default=0.0) or None
    latest_observed_ts = max((_as_float(row.get("observed_ts")) or 0.0 for row in history_rows), default=0.0) or None
    latest_premerge_event_ts = _as_float(premerge.get("latest_event_ts"))
    latest_premerge_observed_ts = _as_float(premerge.get("latest_observed_ts"))
    latest_premerge_delta = None
    if latest_premerge_event_ts is not None and latest_premerge_observed_ts is not None:
        latest_premerge_delta = round(max(0.0, latest_premerge_observed_ts - latest_premerge_event_ts), 6)
    stats = _latency_stats(history_rows)
    post_fee = _as_float(routing_fee.get("post_fee_pnl_usd"))
    edge_status = "PRESENT_IN_ROUTING_SHADOW_RETAINED" if post_fee is not None else "MISSING_ROUTING_SHADOW_RETAINED"
    fallback_paper_pnl = _as_float(ready_shadow.get("paper_pnl_usd"))
    count = len(history_rows)
    return {
        "source_wallet": wallet,
        "candidate_id": member.get("candidate_id"),
        "policy_id": member.get("policy_id"),
        "fresh_matching_events_4h": count,
        "fresh_matching_event_rate_per_hour": round(count / trailing_hours, 6) if trailing_hours > 0 else None,
        "latest_matching_event_ts": latest_event_ts,
        "latest_matching_event_iso": _iso_from_ts(latest_event_ts),
        "latest_matching_observed_ts": latest_observed_ts,
        "latest_matching_observed_iso": _iso_from_ts(latest_observed_ts),
        "premerge_new_matching_events": int(premerge.get("new_matching_events") or 0),
        "premerge_retained_matching_rows": int(premerge.get("retained_matching_rows") or 0),
        "premerge_latest_event_ts": latest_premerge_event_ts,
        "premerge_latest_event_iso": _iso_from_ts(latest_premerge_event_ts),
        "premerge_latest_observed_ts": latest_premerge_observed_ts,
        "premerge_latest_observed_iso": _iso_from_ts(latest_premerge_observed_ts),
        "premerge_latest_observed_delta_s": latest_premerge_delta,
        "premerge_rtds_catchup_lag_s": _as_float(premerge.get("rtds_catchup_lag_s")),
        **stats,
        "routing_shadow_edge_status": edge_status,
        "routing_shadow_post_fee_pnl_usd": post_fee,
        "routing_shadow_measured_unique_windows": routing_fee.get("measured_unique_windows"),
        "routing_shadow_fee_gated_intents": routing_fee.get("fee_gated_intents"),
        "ready_shadow_paper_pnl_usd": fallback_paper_pnl,
        "ready_shadow_resolved_paper_fills": ready_shadow.get("resolved_paper_fills"),
        "ranking_basis": [
            "fresh_matching_events_4h_desc",
            "local_entry_latency_p50_s_asc",
            "routing_shadow_post_fee_pnl_usd_desc",
        ],
    }


def build_packet(
    *,
    guard: dict[str, Any],
    history: dict[str, Any],
    routing: dict[str, Any],
    ready_shadow: dict[str, Any],
    live_execution: dict[str, Any] | None = None,
    candidate_wallets: list[str],
    now_iso: str | None = None,
    quiet_anchor_ts: float = DEFAULT_QUIET_ANCHOR_TS,
    quiet_hours: float = DEFAULT_QUIET_HOURS,
    prior_packet: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at, now_ts = _now_ts(now_iso)
    trailing_hours = 4.0
    since_ts = now_ts - trailing_hours * 3600.0
    selected = _selected_member(guard, live_execution)
    selected_wallet = _wallet(selected.get("source_wallet"))
    members = _active_runtime_members(guard, live_execution)
    premerge = _premerge_index(guard)
    routing_fee = _routing_post_fee_index(routing)
    ready = _ready_shadow_index(ready_shadow)
    rows: list[dict[str, Any]] = []
    for wallet in [_wallet(item) for item in candidate_wallets]:
        if not wallet:
            continue
        row = _candidate_row(
            wallet=wallet,
            member=members.get(wallet, {}),
            history_rows=_history_events(history, wallet, since_ts=since_ts, now_ts=now_ts),
            premerge=premerge.get(wallet, {}),
            routing_fee=routing_fee.get(wallet, {}),
            ready_shadow=ready.get(wallet, {}),
            trailing_hours=trailing_hours,
        )
        rows.append(row)

    tie_order = {wallet: index for index, wallet in enumerate([_wallet(item) for item in DEFAULT_CANDIDATES])}
    rows.sort(
        key=lambda row: (
            -int(row.get("fresh_matching_events_4h") or 0),
            float("inf")
            if row.get("local_entry_latency_p50_s") is None
            else float(row.get("local_entry_latency_p50_s")),
            -(
                float(row.get("routing_shadow_post_fee_pnl_usd"))
                if row.get("routing_shadow_post_fee_pnl_usd") is not None
                else -1_000_000.0
            ),
            tie_order.get(str(row.get("source_wallet")), 999),
        )
    )
    for idx, row in enumerate(rows, start=1):
        row["packet_rank"] = idx
        row["presumptive_rotation_target"] = idx == 1

    selected_premerge = premerge.get(selected_wallet, {})
    selected_history_rows = _history_events(history, selected_wallet, since_ts=since_ts, now_ts=now_ts) if selected_wallet else []
    latest_selected_retained = max((_as_float(row.get("event_ts")) or 0.0 for row in selected_history_rows), default=0.0) or None
    selected_new_matching = int(selected_premerge.get("new_matching_events") or 0) if selected_premerge else 0
    submit_attempt = _latest_selected_submit_attempt(live_execution or {}, selected_wallet) if selected_wallet else {}
    submit_attempt_ts = _as_float(submit_attempt.get("ts"))
    effective_submit_attempt = submit_attempt or None
    effective_submit_attempt_ts = submit_attempt_ts
    quiet_anchor = float(quiet_anchor_ts)
    quiet_anchor_source = "selection_or_rotation_ts"
    if submit_attempt_ts is not None and submit_attempt_ts > quiet_anchor:
        quiet_anchor = submit_attempt_ts
        quiet_anchor_source = "selected_submit_attempt"
    prior = _dict(prior_packet)
    prior_quiet = _dict(prior.get("quiet_clock"))
    prior_anchor = _as_float(prior_quiet.get("anchor_ts"))
    prior_submit_attempt = _dict(prior_quiet.get("selected_submit_attempt"))
    prior_submit_attempt_ts = _as_float(prior_quiet.get("selected_submit_attempt_ts"))
    if prior_submit_attempt_ts is None:
        prior_submit_attempt_ts = _as_float(prior_submit_attempt.get("ts"))
    prior_anchor_floor = _as_float(prior_quiet.get("anchor_floor_ts"))
    if (
        _wallet(prior.get("selected_wallet")) == selected_wallet
        and prior_quiet.get("reset_predicate") == QUIET_RESET_PREDICATE
        and prior_anchor_floor == quiet_anchor_ts
        and prior_submit_attempt_ts is not None
        and prior_anchor is not None
    ):
        quiet_anchor = max(quiet_anchor, prior_anchor)
        if quiet_anchor == prior_anchor:
            quiet_anchor_source = "prior_same_selected_submit_predicate_anchor"
            effective_submit_attempt_ts = prior_submit_attempt_ts
            effective_submit_attempt = prior_submit_attempt or {
                "ts": prior_submit_attempt_ts,
                "iso": _iso_from_ts(prior_submit_attempt_ts),
            }
    earliest_fire = quiet_anchor + quiet_hours * 3600.0
    return {
        "kind": "active_set_rotation_packet",
        "schema_version": 1,
        "generated_at": generated_at,
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "status": "PRESTAGED_NO_LIVE_CHANGE",
        "live_path_mutated": False,
        "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the only live order submitter",
        "direction_id": "2026-07-18T03:24Z-fable-rotation-packet",
        "ranking_rule": "fresh_matching_events_4h desc, local_entry_latency_p50_s asc, routing_shadow_post_fee_pnl_usd desc; tie=f418>a689>c539",
        "candidate_wallets": [_wallet(item) for item in candidate_wallets],
        "selected_wallet": selected_wallet or None,
        "selected_candidate_id": selected.get("candidate_id"),
        "selected_premerge_new_matching_events": selected_new_matching,
        "selected_latest_retained_matching_event_ts": latest_selected_retained,
        "selected_latest_retained_matching_event_iso": _iso_from_ts(latest_selected_retained),
        "quiet_clock": {
            "rule": "trailing 4h selected-wallet quiet; only selected submit-eligible CopyIntent activity extends tenure",
            "reset_predicate": QUIET_RESET_PREDICATE,
            "ignored_selected_premerge_new_matching_events": selected_new_matching,
            "anchor_ts": quiet_anchor,
            "anchor_iso": _iso_from_ts(quiet_anchor),
            "anchor_floor_ts": quiet_anchor_ts,
            "anchor_floor_iso": _iso_from_ts(quiet_anchor_ts),
            "anchor_source": quiet_anchor_source,
            "selected_submit_attempt_ts": effective_submit_attempt_ts,
            "selected_submit_attempt_iso": _iso_from_ts(effective_submit_attempt_ts),
            "selected_submit_attempt": effective_submit_attempt,
            "quiet_hours": quiet_hours,
            "earliest_fire_ts": earliest_fire,
            "earliest_fire_iso": _iso_from_ts(earliest_fire),
            "fires_now": now_ts >= earliest_fire,
            "now_ts": now_ts,
            "now_iso": generated_at,
        },
        "presumptive_target": rows[0]["source_wallet"] if rows else None,
        "presumptive_candidate_id": rows[0].get("candidate_id") if rows else None,
        "ranked_candidates": rows,
        "next_action": (
            "if quiet_clock.fires_now at the first heartbeat after earliest_fire_iso, "
            "rotate to presumptive_target under the standing Fable rule"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD))
    parser.add_argument("--history-state", default=str(DEFAULT_HISTORY))
    parser.add_argument("--routing-shadow", default=str(DEFAULT_ROUTING))
    parser.add_argument("--ready-shadow", default=str(DEFAULT_READY_SHADOW))
    parser.add_argument("--live-execution-state", default=str(DEFAULT_LIVE_EXECUTION))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--candidate-wallet", action="append", dest="candidate_wallets")
    parser.add_argument("--now")
    parser.add_argument("--quiet-anchor-ts", type=float, default=DEFAULT_QUIET_ANCHOR_TS)
    parser.add_argument("--quiet-hours", type=float, default=DEFAULT_QUIET_HOURS)
    args = parser.parse_args()

    output = Path(args.output)
    packet = build_packet(
        guard=load_json(args.guard_state, default={}) or {},
        history=load_json(args.history_state, default={}) or {},
        routing=load_json(args.routing_shadow, default={}) or {},
        ready_shadow=load_json(args.ready_shadow, default={}) or {},
        live_execution=load_json(args.live_execution_state, default={}) or {},
        candidate_wallets=args.candidate_wallets or list(DEFAULT_CANDIDATES),
        now_iso=args.now,
        quiet_anchor_ts=args.quiet_anchor_ts,
        quiet_hours=args.quiet_hours,
        prior_packet=load_json(output, default={}) or {},
    )
    atomic_write_json(output, packet)
    print(json.dumps({"status": packet["status"], "output": str(output), "presumptive_target": packet["presumptive_target"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
