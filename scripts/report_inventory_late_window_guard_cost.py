#!/usr/bin/env python3
"""Measure the counterfactual cost of inventory_late_window_guard.

Flow stage: LIVE/MEASURE. This script is evidence-only: it reads the live
active-set contract, RTDS wallet history, live ledger, and BTC resolutions, then
writes a JSON report. It does not mutate live guard state or policy.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.mission import mission_contract  # noqa: E402
from src.wallet_copy.models import WalletEvent, num, parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.profit_engine import CandidatePolicy, policy_accepts_event  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/inventory_late_window_guard_cost_full_latest.json"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--late-window-stop-s", type=float, default=60.0)
    parser.add_argument("--decision-min-pnl-usd", type=float, default=5.0)
    parser.add_argument("--decision-min-resolved-would-be-fills", type=int, default=20)
    parser.add_argument("--since", default="", help="Optional lower UTC timestamp bound for observed source events.")
    parser.add_argument("--until", default="", help="Optional upper UTC timestamp bound for observed source events.")
    parser.add_argument(
        "--include-unresolved",
        action="store_true",
        help="Include unresolved groups in output rows. Summaries always split resolved/unresolved.",
    )
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text[:42] if text.startswith("0x") and len(text) >= 42 else ""


def _active_members(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    rows = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    if rows:
        return [row for row in rows if isinstance(row, dict) and row.get("enabled") is not False]
    phase = (mission_contract().get("current_runtime_phase_contract") or {})
    contract = phase.get("active_live_set") if isinstance(phase.get("active_live_set"), dict) else {}
    rows = contract.get("members") if isinstance(contract.get("members"), list) else []
    return [row for row in rows if isinstance(row, dict) and row.get("enabled") is not False]


def _policy_from_member(member: dict[str, Any]) -> CandidatePolicy:
    policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
    return CandidatePolicy(
        policy_id=str(policy.get("policy_id") or member.get("policy_id") or ""),
        min_price=num(policy.get("min_price"), 0.01),
        max_price=num(policy.get("max_price"), num(member.get("max_price"), 1.0)),
        min_wallet_usdc=num(policy.get("min_wallet_usdc"), 0.0),
        max_wallet_usdc=num(policy.get("max_wallet_usdc"), 0.0),
        min_seconds_from_open=(
            None if policy.get("min_seconds_from_open") is None else num(policy.get("min_seconds_from_open"))
        ),
        max_seconds_from_open=(
            None if policy.get("max_seconds_from_open") is None else num(policy.get("max_seconds_from_open"))
        ),
        wallet_fraction=num(policy.get("wallet_fraction"), num(member.get("wallet_fraction"), 0.10)),
        max_order_usd=num(policy.get("max_order_usd"), num(member.get("max_order_usd"), 8.0)),
        min_order_usd=num(policy.get("min_order_usd"), 1.0),
    )


def _order_ts(order: dict[str, Any]) -> float | None:
    for key in ("submitted_at", "updated_at", "created_at"):
        ts = parse_ts(order.get(key))
        if ts is not None:
            return ts
    return None


def _source_wallet_for_order(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    return _norm_wallet(order.get("source_wallet") or source_intent.get("source_wallet"))


def _first_live_order_ts_by_wallet(ledger: dict[str, Any]) -> dict[str, float]:
    first: dict[str, float] = {}
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        wallet = _source_wallet_for_order(order)
        ts = _order_ts(order)
        if not wallet or ts is None:
            continue
        first[wallet] = min(first.get(wallet, ts), ts)
    return first


def _timestamp_from_text(value: Any) -> float | None:
    match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", str(value or ""))
    return parse_ts(match.group(0)) if match else None


def _activation_ts(member: dict[str, Any], first_live_order_ts: dict[str, float]) -> float | None:
    wallet = _norm_wallet(member.get("source_wallet"))
    candidates: list[float] = []
    if wallet and wallet in first_live_order_ts:
        candidates.append(first_live_order_ts[wallet])
    summary = member.get("summary") if isinstance(member.get("summary"), dict) else {}
    for key in ("direction_id", "activation_id", "as_of"):
        ts = _timestamp_from_text(summary.get(key) or member.get(key))
        if ts is not None:
            candidates.append(ts)
    return min(candidates) if candidates else None


def _wallet_event(row: dict[str, Any]) -> WalletEvent | None:
    wallet = _norm_wallet(row.get("source_wallet") or row.get("proxyWallet"))
    if not wallet:
        return None
    return WalletEvent(
        source_wallet=wallet,
        wallet_name=str(row.get("wallet_name") or ""),
        row_type=str(row.get("row_type") or "trade"),
        action=str(row.get("action") or row.get("side") or ""),
        condition_id=str(row.get("condition_id") or row.get("conditionId") or ""),
        market_slug=str(row.get("market_slug") or row.get("event_slug") or ""),
        outcome=str(row.get("outcome") or ""),
        price=num(row.get("price")),
        size=num(row.get("size")),
        usdc_size=num(row.get("usdc_size"), num(row.get("price")) * num(row.get("size"))),
        event_ts=parse_ts(row.get("event_ts")),
        observed_ts=num(row.get("observed_ts")),
        event_id=str(row.get("event_id") or ""),
        source=str(row.get("source") or ""),
        market_id=str(row.get("market_id") or ""),
        event_slug=str(row.get("event_slug") or ""),
        title=str(row.get("title") or ""),
        asset=str(row.get("asset") or ""),
        duration=str(row.get("duration") or ""),
        window_start_s=(None if row.get("window_start_s") is None else int(num(row.get("window_start_s")))),
        token_id=str(row.get("token_id") or row.get("asset") or ""),
        outcome_index=None if row.get("outcome_index") is None else int(num(row.get("outcome_index"))),
        transaction_hash=str(row.get("transaction_hash") or ""),
        api_latency_s=None if row.get("api_latency_s") is None else num(row.get("api_latency_s")),
        raw=row.get("raw") if isinstance(row.get("raw"), dict) else {},
    )


def _btc_5m_window_start_s(event: WalletEvent) -> float | None:
    slug = str(event.market_slug or event.event_slug or "")
    if slug.startswith("btc-updown-5m-"):
        marker = slug.rsplit("-", 1)[-1]
        if marker.isdigit():
            return float(marker)
    if event.window_start_s is not None and float(event.window_start_s) > 0:
        return float(event.window_start_s)
    if event.event_ts is not None and float(event.event_ts) > 0 and "bitcoin-up-or-down" in slug:
        return float(int(float(event.event_ts) // 300.0) * 300)
    return None


def _resolution_direction(resolutions: dict[str, dict[str, Any]], event: WalletEvent) -> str:
    keys = [str(event.condition_id or ""), str(event.token_id or "")]
    start = _btc_5m_window_start_s(event)
    if start is not None:
        keys.append(f"slug_start:{int(start)}")
    for key in keys:
        row = resolutions.get(key)
        if row:
            direction = str(row.get("direction") or "").strip().upper()
            if direction.startswith("UP"):
                return "Up"
            if direction.startswith("DOWN"):
                return "Down"
    return ""


def _seconds_to_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    value = max(0.0, float(seconds))
    if value <= 5:
        return "000_005"
    if value <= 15:
        return "006_015"
    if value <= 30:
        return "016_030"
    if value <= 45:
        return "031_045"
    if value <= 60:
        return "046_060"
    return "gt_060"


def _canonical_slug(event: WalletEvent, window_start_s: float | None) -> str:
    if window_start_s is None:
        return str(event.market_slug or event.condition_id or "")
    return f"btc-updown-5m-{int(window_start_s)}"


def _event_key(event: WalletEvent) -> tuple[str, str, str, str]:
    start = _btc_5m_window_start_s(event)
    return (
        event.source_wallet.lower(),
        _canonical_slug(event, start),
        str(event.condition_id or ""),
        str(event.outcome or ""),
    )


def _summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    eligible = [row for row in rows if row.get("counterfactual_eligible_min_order_submitted_basis")]
    resolved_eligible = [row for row in eligible if row.get("resolved")]
    return {
        "groups": len(rows),
        "wallet_orders_skipped": int(sum(int(row.get("wallet_orders_skipped_submitted_basis") or 0) for row in rows)),
        "wallet_orders_in_guard_groups": int(sum(int(row.get("wallet_orders_in_group") or 0) for row in rows)),
        "direct_late_source_orders": int(sum(int(row.get("direct_late_source_orders") or 0) for row in rows)),
        "source_usd": round(sum(num(row.get("source_usd")) for row in rows), 6),
        "target_copy_usd": round(sum(num(row.get("target_copy_usd")) for row in rows), 6),
        "residual_after_submitted_usd": round(sum(num(row.get("residual_after_submitted_usd")) for row in rows), 6),
        "residual_after_filled_usd": round(sum(num(row.get("residual_after_filled_usd")) for row in rows), 6),
        "would_be_fill_groups": len(eligible),
        "resolved_groups": len(resolved),
        "resolved_would_be_fills": len(resolved_eligible),
        "counterfactual_pnl_usd": round(sum(num(row.get("counterfactual_pnl_usd")) for row in resolved_eligible), 6),
        "unresolved_would_be_fills": len(eligible) - len(resolved_eligible),
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    guard_state = load_json(args.guard_state, default={})
    ledger = load_json(args.ledger, default={})
    history = load_json(args.history, default={})
    resolutions = load_resolutions(args.resolutions)
    since_ts = parse_ts(args.since) if str(args.since or "").strip() else None
    until_ts = parse_ts(args.until) if str(args.until or "").strip() else None
    first_live = _first_live_order_ts_by_wallet(ledger if isinstance(ledger, dict) else {})
    submitted_by_group: dict[tuple[str, str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"submitted_usd": 0.0, "filled_usd": 0.0, "orders": 0, "fills": 0}
    )
    for order in (ledger.get("orders") if isinstance(ledger, dict) else []) or []:
        if not isinstance(order, dict):
            continue
        source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        wallet = _source_wallet_for_order(order)
        market_slug = str(order.get("market_slug") or source_intent.get("market_slug") or "")
        condition_id = str(order.get("condition_id") or source_intent.get("condition_id") or "")
        outcome = str(order.get("outcome") or source_intent.get("outcome") or "")
        if not wallet or not market_slug or not outcome:
            continue
        key = (wallet, market_slug, condition_id, outcome)
        submitted_by_group[key]["orders"] += 1
        submitted_by_group[key]["submitted_usd"] = round(
            float(submitted_by_group[key]["submitted_usd"]) + num(order.get("requested_size_usd")),
            6,
        )
        if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED":
            submitted_by_group[key]["fills"] += 1
            submitted_by_group[key]["filled_usd"] = round(
                float(submitted_by_group[key]["filled_usd"]) + num(order.get("requested_size_usd")),
                6,
            )

    members: dict[str, dict[str, Any]] = {}
    policies: dict[str, CandidatePolicy] = {}
    activation: dict[str, float | None] = {}
    for member in _active_members(guard_state if isinstance(guard_state, dict) else {}):
        wallet = _norm_wallet(member.get("source_wallet"))
        if not wallet:
            continue
        members[wallet] = member
        policies[wallet] = _policy_from_member(member)
        activation[wallet] = _activation_ts(member, first_live)

    diagnostics: Counter[str] = Counter()
    grouped: dict[tuple[str, str, str, str], list[WalletEvent]] = defaultdict(list)
    for row in history.get("events") or []:
        if not isinstance(row, dict):
            diagnostics["history_non_object"] += 1
            continue
        event = _wallet_event(row)
        if event is None:
            diagnostics["event_parse_reject"] += 1
            continue
        wallet = event.source_wallet.lower()
        policy = policies.get(wallet)
        if policy is None:
            diagnostics["wallet_not_active_member"] += 1
            continue
        if str(event.source or "") != "rtds_activity":
            diagnostics["not_rtds_activity"] += 1
            continue
        start = _btc_5m_window_start_s(event)
        if start is None:
            diagnostics["not_btc_5m"] += 1
            continue
        observed = float(event.observed_ts or 0.0)
        if observed <= 0:
            diagnostics["missing_observed_ts"] += 1
            continue
        if since_ts is not None and observed < float(since_ts):
            diagnostics["before_since"] += 1
            continue
        if until_ts is not None and observed >= float(until_ts):
            diagnostics["after_until"] += 1
            continue
        activated = activation.get(wallet)
        if activated is not None and observed < float(activated):
            diagnostics["before_member_activation"] += 1
            continue
        if observed > start + 300.0:
            diagnostics["observed_after_close"] += 1
            continue
        accepted, reason = policy_accepts_event(policy, event)
        if not accepted:
            diagnostics[f"policy_{reason}"] += 1
            continue
        time_to_close_s = start + 300.0 - observed
        if time_to_close_s <= float(args.late_window_stop_s):
            diagnostics["accepted_direct_late_source_event"] += 1
        else:
            diagnostics["accepted_source_event_before_late_window"] += 1
        diagnostics["accepted_policy_events"] += 1
        grouped[_event_key(event)].append(event)

    rows: list[dict[str, Any]] = []
    for key, events in grouped.items():
        wallet, market_slug, condition_id, outcome = key
        policy = policies[wallet]
        events = sorted(events, key=lambda item: (item.observed_ts or 0.0, item.event_id))
        source_usd = sum(max(0.0, float(event.usdc_size)) for event in events)
        source_shares = sum(max(0.0, float(event.size)) for event in events)
        source_vwap = source_usd / source_shares if source_shares > 0 else 0.0
        target_copy_usd = min(max(0.0, float(policy.max_order_usd)), source_usd * float(policy.wallet_fraction))
        submitted = submitted_by_group.get(key, {})
        submitted_usd = num(submitted.get("submitted_usd"))
        filled_usd = num(submitted.get("filled_usd"))
        residual_after_submitted = max(0.0, target_copy_usd - submitted_usd)
        residual_after_filled = max(0.0, target_copy_usd - filled_usd)
        eligible_min_submitted = bool(residual_after_submitted >= float(policy.min_order_usd) and source_vwap > 0)
        eligible_min_filled = bool(residual_after_filled >= float(policy.min_order_usd) and source_vwap > 0)
        direction = _resolution_direction(resolutions, events[-1])
        resolved = bool(direction)
        pnl: float | None = None
        if eligible_min_submitted and resolved:
            candidate_shares = residual_after_submitted / source_vwap
            pnl = (
                candidate_shares - residual_after_submitted
                if outcome.lower() == direction.lower()
                else -residual_after_submitted
            )
        latest_observed_ts = max(float(event.observed_ts or 0.0) for event in events)
        window_start_s = _btc_5m_window_start_s(events[-1])
        time_to_close_latest = None if window_start_s is None else window_start_s + 300.0 - latest_observed_ts
        direct_late_source_orders = sum(
            1
            for event in events
            if (
                (window_start := _btc_5m_window_start_s(event)) is not None
                and window_start + 300.0 - float(event.observed_ts or 0.0) <= float(args.late_window_stop_s)
            )
        )
        row = {
            "source_wallet": wallet,
            "policy_id": policy.policy_id,
            "market_slug": market_slug,
            "condition_id": condition_id,
            "outcome": outcome,
            "wallet_orders_in_group": len(events),
            "wallet_orders_skipped_submitted_basis": len(events) if residual_after_submitted > 0 else 0,
            "direct_late_source_orders": direct_late_source_orders,
            "source_usd": round(source_usd, 6),
            "source_shares": round(source_shares, 6),
            "source_vwap": round(source_vwap, 6),
            "target_copy_usd": round(target_copy_usd, 6),
            "actual_submitted_orders": int(submitted.get("orders") or 0),
            "actual_fills": int(submitted.get("fills") or 0),
            "actual_submitted_usd": round(submitted_usd, 6),
            "actual_filled_usd": round(filled_usd, 6),
            "residual_after_submitted_usd": round(residual_after_submitted, 6),
            "residual_after_filled_usd": round(residual_after_filled, 6),
            "counterfactual_eligible_min_order_submitted_basis": eligible_min_submitted,
            "counterfactual_eligible_min_order_filled_basis": eligible_min_filled,
            "resolution_direction": direction,
            "resolved": resolved,
            "counterfactual_pnl_usd": None if pnl is None else round(pnl, 6),
            "first_observed_ts": round(min(float(event.observed_ts or 0.0) for event in events), 6),
            "latest_observed_ts": round(latest_observed_ts, 6),
            "window_start_s": None if window_start_s is None else round(float(window_start_s), 6),
            "source_time_to_close_s_latest": (
                None if time_to_close_latest is None else round(float(time_to_close_latest), 6)
            ),
            "source_time_to_close_bucket": _seconds_to_bucket(time_to_close_latest),
            "modeled_guard_time_to_close_bucket": "000_060",
            "event_ids": [event.event_id for event in events[:20]],
        }
        if args.include_unresolved or residual_after_submitted > 0 or residual_after_filled > 0:
            rows.append(row)

    rows.sort(key=lambda row: (str(row["source_wallet"]), float(row.get("window_start_s") or 0), str(row["outcome"])))
    by_wallet = {wallet: _summarize_rows([row for row in rows if row["source_wallet"] == wallet]) for wallet in sorted(members)}
    by_time_to_close = {
        bucket: _summarize_rows([row for row in rows if row["source_time_to_close_bucket"] == bucket])
        for bucket in sorted({str(row["source_time_to_close_bucket"]) for row in rows})
    }
    summary = _summarize_rows(rows)
    min_resolved = int(args.decision_min_resolved_would_be_fills)
    min_pnl = float(args.decision_min_pnl_usd)
    if summary["resolved_would_be_fills"] >= min_resolved and summary["counterfactual_pnl_usd"] >= min_pnl:
        verdict = "RELAX_GUARD_EVIDENCE_POSITIVE"
    elif summary["resolved_would_be_fills"] >= min_resolved:
        verdict = "KEEP_GUARD_WORKING_AS_INTENDED_NEGATIVE"
    else:
        verdict = "KEEP_GUARD_MEASURING_UNDER_SAMPLED"

    return {
        "schema_version": 1,
        "kind": "inventory_late_window_guard_cost_report",
        "flow_stage": "LIVE/MEASURE",
        "generated_at": utc_now_iso(),
        "scope": {
            "history": args.history,
            "ledger": args.ledger,
            "guard_state": args.guard_state,
            "resolutions": args.resolutions,
            "source_filter": "rtds_activity",
            "active_member_count": len(members),
            "late_window_stop_s": float(args.late_window_stop_s),
            "since": args.since,
            "until": args.until,
            "member_activation": {
                wallet: (
                    None
                    if activation.get(wallet) is None
                    else datetime.fromtimestamp(float(activation[wallet]), tz=UTC).isoformat().replace("+00:00", "Z")
                )
                for wallet in sorted(members)
            },
        },
        "assumptions": {
            "copy_surface": "BUY BTC-5m RTDS events accepted by the live member CandidatePolicy",
            "late_guard_model": (
                "model residual inventory still unsubmitted when the window enters the final "
                "late_window_stop_s; strict source-event-in-final-minute counts are reported separately"
            ),
            "counterfactual_price": "source_vwap of policy-accepted events grouped by wallet/market/outcome",
            "target_copy_usd": "min(member.max_order_usd, grouped_source_usd * member.wallet_fraction)",
            "residual_basis": "target_copy_usd minus actual submitted live usd for the same wallet/market/outcome",
            "would_be_fill": "residual_after_submitted_usd >= member.min_order_usd and resolution available",
            "pnl_model": "BUY payout at 1.0/share if grouped outcome matches resolved direction, else lose copy_usd",
            "live_path_mutated": False,
        },
        "decision_rule": {
            "relax_if_counterfactual_pnl_usd_gte": min_pnl,
            "and_resolved_would_be_fills_gte": min_resolved,
        },
        "diagnostics": dict(sorted(diagnostics.items())),
        "summary": summary,
        "by_wallet": by_wallet,
        "by_time_to_close": by_time_to_close,
        "sample_status": "FULL_HISTORY_ACTIVE_SET_ACTIVATION_SCOPED",
        "verdict": verdict,
        "rows": rows,
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    summary = report["summary"]
    print(
        "inventory_late_window_guard_cost",
        f"wallet_orders_skipped={summary['wallet_orders_skipped']}",
        f"would_be_fills={summary['would_be_fill_groups']}",
        f"resolved_would_be_fills={summary['resolved_would_be_fills']}",
        f"cf_pnl={summary['counterfactual_pnl_usd']:+.6f}",
        f"verdict={report['verdict']}",
        f"output={args.output}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
