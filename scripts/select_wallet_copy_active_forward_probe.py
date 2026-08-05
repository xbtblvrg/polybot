#!/usr/bin/env python3
"""Select an active hot-lane candidate for paper-only forward probing.

This script deliberately does not promote a strategy. It builds a small
profit-state-shaped file whose best_candidate is a currently active hot-lane
single-wallet candidate, so the live tracker can measure fresh CLOB-backed
copyability without pretending that this evidence satisfies canonical live
admission for the historical best candidate.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


MISSION_ANCHOR_TAGS = {
    "mission_anchor",
    "operator_mission_anchor",
    "current_poll_anchor",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument("--output", default="data/research/wallet_copy_active_forward_probe_profit_state.json")
    parser.add_argument(
        "--runtime-tracker-state",
        action="append",
        default=[],
        help=(
            "Optional prior paper live-tracker state(s) used only to rank the next probe toward "
            "candidate/policy pairs that already produced required CLOB-backed BUY evidence."
        ),
    )
    parser.add_argument("--max-probe-candidates", type=int, default=10)
    return parser.parse_args()


def _wallet(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return str(row.get("source_wallet") or metadata.get("source_wallet") or "").lower()


def _summary(row: dict[str, Any]) -> dict[str, Any]:
    if isinstance(row.get("summary"), dict):
        return row["summary"]
    summary: dict[str, Any] = {}
    for source_key, target_key in (
        ("roi_pct", "roi_pct"),
        ("resolved_orders", "resolved_orders"),
        ("wr_pct", "wr_pct"),
    ):
        if source_key in row:
            summary[target_key] = row.get(source_key)
    window_metrics: dict[str, Any] = {}
    for source_key, target_key in (
        ("unique_windows", "unique_windows"),
        ("max_orders_per_window_ratio", "max_orders_per_window_ratio"),
    ):
        if source_key in row:
            window_metrics[target_key] = row.get(source_key)
    if window_metrics:
        summary["window_metrics"] = window_metrics
    return summary


def _validation(row: dict[str, Any]) -> dict[str, Any]:
    if isinstance(row.get("validation_summary"), dict):
        return row["validation_summary"]
    validation: dict[str, Any] = {}
    for source_key, target_key in (
        ("validation_roi_pct", "roi_pct"),
        ("validation_unique_windows", "unique_windows"),
    ):
        if source_key in row:
            validation[target_key] = row.get(source_key)
    return validation


def _window_metrics(row: dict[str, Any]) -> dict[str, Any]:
    return _summary(row).get("window_metrics") if isinstance(_summary(row).get("window_metrics"), dict) else {}


def _selected_active_wallets(active_hotlane: dict[str, Any]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    rows = active_hotlane.get("selected_wallets") if isinstance(active_hotlane.get("selected_wallets"), list) else []
    ordered: list[str] = []
    by_wallet: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("address") or row.get("wallet") or "").lower()
        if not wallet or wallet in by_wallet:
            continue
        ordered.append(wallet)
        by_wallet[wallet] = row
    return ordered, by_wallet


def _candidate_key(row: dict[str, Any]) -> tuple[str, str, str]:
    policy = row.get("policy") if isinstance(row.get("policy"), dict) else {}
    return (
        str(row.get("candidate_id") or ""),
        _wallet(row),
        str(row.get("policy_id") or policy.get("policy_id") or ""),
    )


def _policy_id(row: dict[str, Any]) -> str:
    policy = row.get("policy") if isinstance(row.get("policy"), dict) else {}
    return str(row.get("policy_id") or policy.get("policy_id") or "")


def _candidate_pool(profit: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add(row: Any) -> None:
        if not isinstance(row, dict):
            return
        key = _candidate_key(row)
        if key in seen:
            return
        seen.add(key)
        rows.append(row)

    add(profit.get("forward_candidate"))
    queue = profit.get("forward_tracking_queue") if isinstance(profit.get("forward_tracking_queue"), list) else []
    for row in queue:
        add(row)
    ranked = profit.get("ranked_candidates") if isinstance(profit.get("ranked_candidates"), list) else []
    for row in ranked:
        add(row)
    return rows


def _active_row_tags(row: dict[str, Any]) -> set[str]:
    tags = row.get("tags") if isinstance(row.get("tags"), list) else []
    mission_tags = (
        row.get("mission_anchor_tags")
        if isinstance(row.get("mission_anchor_tags"), list)
        else []
    )
    return {
        str(tag)
        for tag in [*tags, *mission_tags]
        if str(tag or "").strip()
    }


def _is_mission_anchor_active_row(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    if bool(row.get("mission_anchor")):
        return True
    if MISSION_ANCHOR_TAGS & _active_row_tags(row):
        return True
    reason = str(row.get("mission_anchor_reason") or "")
    return bool(reason)


def _mission_anchor_candidate_id(wallet: str) -> str:
    normalized = wallet.lower()
    if normalized.startswith("0x"):
        normalized = normalized[2:]
    return f"mission_anchor_{normalized[:12]}"


def _mission_anchor_policy(wallet: str) -> dict[str, Any]:
    suffix = wallet[2:10] if wallet.startswith("0x") else wallet[:8]
    return {
        "policy_id": f"mission_anchor_fast_wf_0.10_cap_4_all_prices_minusd_0_all_window_{suffix}",
        "min_price": 0.01,
        "max_price": 1.0,
        "min_wallet_usdc": 0.0,
        "max_wallet_usdc": 0.0,
        "min_seconds_from_open": None,
        "max_seconds_from_open": None,
        "wallet_fraction": 0.10,
        "max_order_usd": 4.0,
        "min_order_usd": 0.0,
    }


def _mission_anchor_probe_candidates(
    ranked: list[dict[str, Any]],
    selected_by_wallet: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    existing_trackable_wallets = {
        _wallet(row)
        for row in ranked
        if isinstance(row, dict)
        and row.get("candidate_type") == "SINGLE_WALLET"
        and isinstance(row.get("policy"), dict)
        and _wallet(row)
    }
    synthetic: list[dict[str, Any]] = []
    for wallet, active_row in selected_by_wallet.items():
        if wallet in existing_trackable_wallets or not _is_mission_anchor_active_row(active_row):
            continue
        policy = _mission_anchor_policy(wallet)
        synthetic.append(
            {
                "status": "BLOCKED",
                "candidate_id": _mission_anchor_candidate_id(wallet),
                "candidate_type": "SINGLE_WALLET",
                "source_wallet": wallet,
                "wallet_name": active_row.get("name") or active_row.get("wallet_name"),
                "policy_id": policy["policy_id"],
                "policy": policy,
                "mission_anchor_probe": True,
                "active_hotlane": active_row,
                "metadata": {
                    "source_wallet": wallet,
                    "wallet_name": active_row.get("name") or active_row.get("wallet_name"),
                    "mission_anchor": True,
                    "mission_anchor_reason": active_row.get("mission_anchor_reason"),
                    "mission_anchor_tags": sorted(_active_row_tags(active_row) & MISSION_ANCHOR_TAGS),
                    "active_hotlane_score": active_row.get("score"),
                    "probe_origin": "active_hotlane_operator_mission_anchor_without_profit_candidate",
                },
                "blockers": [
                    "mission_anchor_needs_profit_engine_candidate_proof",
                    "mission_anchor_needs_current_poll_clob_truth",
                    "active_forward_probe_not_live_admission_truth",
                ],
                "summary": {
                    "orders": 0,
                    "resolved_orders": 0,
                    "roi_pct": 0.0,
                    "wr_pct": 0.0,
                    "window_metrics": {
                        "unique_windows": 0,
                        "avg_orders_per_window": 0.0,
                        "max_orders_per_window_ratio": 1.0,
                    },
                },
                "validation_summary": {
                    "orders": 0,
                    "resolved_orders": 0,
                    "roi_pct": 0.0,
                    "wr_pct": 0.0,
                    "unique_windows": 0,
                },
            }
        )
    return synthetic


def _freshness_bucket(active_row: dict[str, Any]) -> tuple[int, float]:
    try:
        lag_s = float(active_row.get("latest_live_event_lag_s"))
    except (TypeError, ValueError):
        lag_s = float("inf")
    if lag_s <= 30.0:
        bucket = 0
    elif lag_s <= 60.0:
        bucket = 1
    elif lag_s <= 180.0:
        bucket = 2
    else:
        bucket = 3
    return bucket, lag_s


def _active_signal_summary(active_row: dict[str, Any]) -> dict[str, Any]:
    def as_int(key: str) -> int:
        try:
            return int(active_row.get(key) or 0)
        except (TypeError, ValueError):
            return 0

    def as_float(key: str) -> float | None:
        try:
            return float(active_row.get(key))
        except (TypeError, ValueError):
            return None

    return {
        "latest_live_event_lag_s": as_float("latest_live_event_lag_s"),
        "live_btc5m_buys": as_int("live_btc5m_buys"),
        "live_clob_ok": as_int("live_clob_ok"),
        "live_copyability_accepted": as_int("live_copyability_accepted"),
    }


def _candidate_active_row(row: dict[str, Any], selected_by_wallet: dict[str, dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    wallet = _wallet(row)
    if wallet in selected_by_wallet:
        merged.update(selected_by_wallet[wallet])
    row_active = row.get("active_hotlane") if isinstance(row.get("active_hotlane"), dict) else {}
    forward_viability = row.get("forward_viability") if isinstance(row.get("forward_viability"), dict) else {}
    forward_active = (
        forward_viability.get("active_hotlane")
        if isinstance(forward_viability.get("active_hotlane"), dict)
        else {}
    )
    merged.update(row_active)
    merged.update(forward_active)
    return merged


def _active_signal_rank(active_row: dict[str, Any]) -> tuple[int, int, int, int, float]:
    summary = _active_signal_summary(active_row)
    lag_s = summary["latest_live_event_lag_s"]
    lag_rank = lag_s if lag_s is not None else float("inf")
    accepted = int(summary["live_copyability_accepted"])
    clob_ok = int(summary["live_clob_ok"])
    buys = int(summary["live_btc5m_buys"])
    fresh = lag_s is not None and lag_s <= 10.0
    if accepted > 0 and fresh:
        bucket = 0
    elif clob_ok > 0 and fresh:
        bucket = 1
    elif buys > 0 and fresh:
        bucket = 2
    elif accepted > 0:
        bucket = 3
    elif clob_ok > 0:
        bucket = 4
    elif buys > 0:
        bucket = 5
    else:
        bucket = 6
    return (bucket, -accepted, -clob_ok, -buys, lag_rank)


def _active_copyability_rank(active_row: dict[str, Any]) -> tuple[int, int, int, float]:
    summary = _active_signal_summary(active_row)
    lag_s = summary["latest_live_event_lag_s"]
    lag_rank = lag_s if lag_s is not None else float("inf")
    accepted = int(summary["live_copyability_accepted"])
    clob_ok = int(summary["live_clob_ok"])
    fresh = lag_s is not None and lag_s <= 10.0
    if accepted > 0 and fresh:
        bucket = 0
    elif clob_ok > 0 and fresh:
        bucket = 1
    elif accepted > 0:
        bucket = 2
    elif clob_ok > 0:
        bucket = 3
    else:
        bucket = 4
        lag_rank = float("inf")
    return (bucket, -accepted, -clob_ok, lag_rank)


def _near_admission_blockers(row: dict[str, Any]) -> set[str]:
    blockers = {str(blocker) for blocker in row.get("blockers") or []}
    measurement_only = {
        "candidate_missing_clob_fill_evidence",
        "candidate_scored_on_limited_history",
    }
    return blockers - measurement_only


def _validation_priority(row: dict[str, Any]) -> tuple[int, float, float]:
    validation = _validation(row)
    try:
        roi = float(validation.get("roi_pct"))
    except (TypeError, ValueError):
        roi = float("-inf")
    try:
        wr = float(validation.get("wr_pct"))
    except (TypeError, ValueError):
        wr = float("-inf")
    return (0 if roi > 0.0 and wr >= 50.0 else 1, -roi, -wr)


def _forward_queue_rank(row: dict[str, Any]) -> int:
    try:
        return int(row.get("forward_queue_rank"))
    except (TypeError, ValueError):
        return 9999


def _probe_evidence_bucket(row: dict[str, Any], hard_blocker_count: int) -> int:
    if hard_blocker_count > 0:
        return 3
    if _forward_queue_rank(row) < 9999:
        return 0
    metrics = _window_metrics(row)
    validation = _validation(row)
    try:
        unique_windows = int(metrics.get("unique_windows") or 0)
    except (TypeError, ValueError):
        unique_windows = 0
    try:
        validation_unique_windows = int(validation.get("unique_windows") or 0)
    except (TypeError, ValueError):
        validation_unique_windows = 0
    if unique_windows >= 8 and validation_unique_windows >= 3:
        return 1
    return 2


def _runtime_tracker_context(state: dict[str, Any]) -> dict[str, Any] | None:
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    profit_policy = summary.get("profit_policy") if isinstance(summary.get("profit_policy"), dict) else {}
    copy_efficiency = (
        summary.get("copy_efficiency") if isinstance(summary.get("copy_efficiency"), dict) else {}
    )
    copy_summary = (
        copy_efficiency.get("summary")
        if isinstance(copy_efficiency.get("summary"), dict)
        else {}
    )
    current_poll = (
        copy_efficiency.get("current_poll")
        if isinstance(copy_efficiency.get("current_poll"), dict)
        else {}
    )
    current_summary = (
        current_poll.get("summary")
        if isinstance(current_poll.get("summary"), dict)
        else {}
    )
    if not profit_policy and not copy_summary:
        return None
    policy = profit_policy.get("policy") if isinstance(profit_policy.get("policy"), dict) else {}
    wallet = str(profit_policy.get("candidate_source_wallet") or "").lower()
    policy_id = str(policy.get("policy_id") or profit_policy.get("candidate_policy_id") or "")
    try:
        required = int(copy_summary.get("required_buy_copy_events") or 0)
    except (TypeError, ValueError):
        required = 0
    try:
        clob = int(copy_summary.get("clob_filled_buy_copy_events") or 0)
    except (TypeError, ValueError):
        clob = 0
    try:
        admission_rows = int(copy_summary.get("admission_relevant_buy_rows") or 0)
    except (TypeError, ValueError):
        admission_rows = 0
    try:
        source_buy_events = int(copy_summary.get("source_buy_events") or 0)
    except (TypeError, ValueError):
        source_buy_events = 0
    try:
        current_required = int(current_summary.get("required_buy_copy_events") or 0)
    except (TypeError, ValueError):
        current_required = 0
    try:
        current_clob = int(current_summary.get("clob_filled_buy_copy_events") or 0)
    except (TypeError, ValueError):
        current_clob = 0
    try:
        current_admission_rows = int(current_summary.get("admission_relevant_buy_rows") or 0)
    except (TypeError, ValueError):
        current_admission_rows = 0
    try:
        current_fresh_buy_rows = int(current_summary.get("source_fresh_buy_events_le_10s") or 0)
    except (TypeError, ValueError):
        current_fresh_buy_rows = 0
    try:
        current_latest_buy_lag_s = float(current_summary.get("latest_buy_event_lag_s"))
    except (TypeError, ValueError):
        current_latest_buy_lag_s = None
    return {
        "state_generated_at": state.get("generated_at"),
        "candidate_id": str(profit_policy.get("candidate_id") or ""),
        "source_wallet": wallet,
        "policy_id": policy_id,
        "copy_efficiency_status": copy_efficiency.get("status"),
        "copy_efficiency_blockers": copy_efficiency.get("blockers") or [],
        "mirror_coverage_status": summary.get("mirror_coverage_status"),
        "current_poll_status": current_poll.get("status"),
        "current_poll_required_buy_copy_events": current_required,
        "current_poll_clob_filled_buy_copy_events": current_clob,
        "current_poll_admission_relevant_buy_rows": current_admission_rows,
        "current_poll_source_fresh_buy_events_le_10s": current_fresh_buy_rows,
        "current_poll_latest_buy_event_lag_s": current_latest_buy_lag_s,
        "required_buy_copy_events": required,
        "clob_filled_buy_copy_events": clob,
        "fallback_filled_buy_copy_events": int(copy_summary.get("fallback_filled_buy_copy_events") or 0),
        "rejected_buy_copy_events": int(copy_summary.get("rejected_buy_copy_events") or 0),
        "missed_buy_copy_events": int(copy_summary.get("missed_buy_copy_events") or 0),
        "admission_relevant_buy_rows": admission_rows,
        "source_buy_events": source_buy_events,
    }


def _runtime_tracker_contexts(states: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for state in states or []:
        if not isinstance(state, dict):
            continue
        context = _runtime_tracker_context(state)
        if context:
            contexts.append(context)
    return contexts


def _runtime_context_rank(context: dict[str, Any] | None) -> tuple[int, int, int]:
    if not context:
        return (5, 0, 0)
    required = int(context.get("required_buy_copy_events") or 0)
    clob = int(context.get("clob_filled_buy_copy_events") or 0)
    fallback = int(context.get("fallback_filled_buy_copy_events") or 0)
    rejected = int(context.get("rejected_buy_copy_events") or 0)
    missed = int(context.get("missed_buy_copy_events") or 0)
    admission_rows = int(context.get("admission_relevant_buy_rows") or 0)
    source_buy_events = int(context.get("source_buy_events") or 0)
    status = str(context.get("copy_efficiency_status") or "")
    current_required = int(context.get("current_poll_required_buy_copy_events") or 0)
    current_clob = int(context.get("current_poll_clob_filled_buy_copy_events") or 0)
    current_admission_rows = int(context.get("current_poll_admission_relevant_buy_rows") or 0)
    current_fresh_buy_rows = int(context.get("current_poll_source_fresh_buy_events_le_10s") or 0)
    current_status = str(context.get("current_poll_status") or "")
    current_is_clean_pass = (
        current_required > 0
        and current_clob >= current_required
        and fallback == 0
        and rejected == 0
        and missed == 0
        and current_status == "PASS"
    )
    rolling_is_clean_pass = (
        required > 0
        and clob >= required
        and fallback == 0
        and rejected == 0
        and missed == 0
        and status == "PASS"
    )
    if current_is_clean_pass:
        bucket = 0
    elif current_required > 0 and current_clob > 0:
        bucket = 1
    elif current_admission_rows > 0 or current_fresh_buy_rows > 0:
        bucket = 2
    elif rolling_is_clean_pass:
        bucket = 3
    elif required > 0 and clob > 0:
        bucket = 4
    elif required > 0:
        bucket = 5
    elif admission_rows > 0:
        bucket = 6
    elif source_buy_events > 0:
        bucket = 7
    else:
        bucket = 8
    return (bucket, -required, -clob)


def _matching_runtime_context(
    row: dict[str, Any],
    runtime_contexts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    candidate_id = str(row.get("candidate_id") or "")
    wallet = _wallet(row)
    policy_id = _policy_id(row)
    matches: list[dict[str, Any]] = []
    for context in runtime_contexts:
        context_candidate = str(context.get("candidate_id") or "")
        context_wallet = str(context.get("source_wallet") or "").lower()
        context_policy = str(context.get("policy_id") or "")
        if candidate_id and context_candidate == candidate_id:
            matches.append(context)
            continue
        if wallet and policy_id and context_wallet == wallet and context_policy == policy_id:
            matches.append(context)
    if not matches:
        return None
    return sorted(matches, key=_runtime_context_rank)[0]


def _probe_rank(
    row: dict[str, Any],
    active_rank: dict[str, int],
    selected_by_wallet: dict[str, dict[str, Any]],
    runtime_contexts: list[dict[str, Any]] | None = None,
) -> tuple[Any, ...]:
    wallet = _wallet(row)
    summary = _summary(row)
    validation = _validation(row)
    metrics = _window_metrics(row)
    blockers = {str(blocker) for blocker in row.get("blockers") or []}
    near_admission_blockers = _near_admission_blockers(row)
    freshness_bucket, latest_lag_s = _freshness_bucket(selected_by_wallet.get(wallet, {}))
    validation_priority = _validation_priority(row)
    runtime_context = _matching_runtime_context(row, runtime_contexts or [])
    runtime_rank = _runtime_context_rank(runtime_context)
    hard_blocker_count = len(near_admission_blockers)
    # Runtime copy proof is useful only for candidates that are otherwise close
    # to admission. A hard-blocked wallet must not monopolize the proof lane just
    # because it happened to trade more recently.
    effective_runtime_rank = runtime_rank if hard_blocker_count == 0 else (9, 0, 0)
    active_row = _candidate_active_row(row, selected_by_wallet)
    active_signal_rank = _active_signal_rank(active_row)
    active_copyability_rank = _active_copyability_rank(active_row)
    mission_anchor_rank = (
        0
        if row.get("mission_anchor_probe")
        or _is_mission_anchor_active_row(active_row)
        else 1
    )
    return (
        0 if str(row.get("status") or "") == "BLOCKED" else 1,
        active_copyability_rank,
        mission_anchor_rank,
        0 if hard_blocker_count == 0 else 1,
        _probe_evidence_bucket(row, hard_blocker_count),
        _forward_queue_rank(row),
        effective_runtime_rank,
        active_signal_rank,
        freshness_bucket,
        hard_blocker_count,
        validation_priority,
        len(blockers),
        latest_lag_s,
        0 if "candidate_missing_clob_fill_evidence" in blockers else 1,
        0 if "candidate_research_only_resolution_evidence" not in blockers else 1,
        -int(validation.get("unique_windows") or 0),
        -int(metrics.get("unique_windows") or 0),
        -int(summary.get("resolved_orders") or 0),
        float(metrics.get("max_orders_per_window_ratio") or 1.0),
        active_rank.get(wallet, 9999),
        -float(row.get("profit_score") or 0.0),
        -float(summary.get("roi_pct") or 0.0),
    )


def _selection_reasons(
    row: dict[str, Any],
    active_row: dict[str, Any],
    runtime_context: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    active_summary = _active_signal_summary(active_row)
    if row.get("mission_anchor_probe") or _is_mission_anchor_active_row(active_row):
        reasons.append("operator_mission_anchor_probe")
    lag_s = active_summary.get("latest_live_event_lag_s")
    if lag_s is not None and lag_s <= 10.0:
        reasons.append("active_hotlane_fresh_lag_le_10s")
    elif lag_s is not None and lag_s <= 30.0:
        reasons.append("active_hotlane_fresh_lag_le_30s")
    if int(active_summary.get("live_copyability_accepted") or 0) > 0:
        reasons.append("active_hotlane_live_copyability_accepted")
    if int(active_summary.get("live_clob_ok") or 0) > 0:
        reasons.append("active_hotlane_live_clob_ok")
    if int(active_summary.get("live_btc5m_buys") or 0) > 0:
        reasons.append("active_hotlane_live_btc5m_buys")
    if _forward_queue_rank(row) < 9999:
        reasons.append("forward_queue_candidate")
    if not _near_admission_blockers(row):
        reasons.append("measurement_only_blockers")
    if runtime_context:
        if int(runtime_context.get("current_poll_source_fresh_buy_events_le_10s") or 0) > 0:
            reasons.append("runtime_current_poll_fresh_buy_rows")
        if int(runtime_context.get("current_poll_clob_filled_buy_copy_events") or 0) > 0:
            reasons.append("runtime_current_poll_clob_fills")
        if int(runtime_context.get("clob_filled_buy_copy_events") or 0) > 0:
            reasons.append("runtime_rolling_clob_fills")
    return reasons


def select_active_forward_probe(
    profit: dict[str, Any],
    active_hotlane: dict[str, Any],
    *,
    max_probe_candidates: int = 10,
    runtime_tracker_states: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected_wallets, selected_by_wallet = _selected_active_wallets(active_hotlane)
    active_rank = {wallet: index for index, wallet in enumerate(selected_wallets)}
    runtime_contexts = _runtime_tracker_contexts(runtime_tracker_states)
    ranked = _candidate_pool(profit)
    mission_anchor_candidates = _mission_anchor_probe_candidates(ranked, selected_by_wallet)
    ranked = [*mission_anchor_candidates, *ranked]
    probe_candidates = [
        row
        for row in ranked
        if isinstance(row, dict)
        and row.get("candidate_type") == "SINGLE_WALLET"
        and _wallet(row) in selected_by_wallet
        and isinstance(row.get("policy"), dict)
        and str(row.get("status") or "") == "BLOCKED"
    ]
    sorted_candidates = sorted(
        probe_candidates,
        key=lambda row: _probe_rank(row, active_rank, selected_by_wallet, runtime_contexts),
    )[
        : max(1, int(max_probe_candidates))
    ]
    probe_candidates = []
    for index, row in enumerate(sorted_candidates):
        wallet = _wallet(row)
        active_row = _candidate_active_row(row, selected_by_wallet)
        runtime_context = _matching_runtime_context(row, runtime_contexts)
        annotated = dict(row)
        annotated["active_wallet_rank"] = active_rank.get(wallet)
        annotated["probe_rank"] = index + 1
        annotated["active_signal_summary"] = _active_signal_summary(active_row)
        annotated["runtime_tracker_context"] = runtime_context
        annotated["probe_selection_reasons"] = _selection_reasons(row, active_row, runtime_context)
        probe_candidates.append(annotated)
    selected = probe_candidates[0] if probe_candidates else None
    now = utc_now_iso()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "wallet_copy_active_forward_probe_profit_state",
        "generated_at": now,
        "paper_only": True,
        "live_orders_allowed": False,
        "source_profit_state_generated_at": profit.get("generated_at"),
        "source_active_hotlane_generated_at": active_hotlane.get("generated_at"),
        "probe_role": "paper_only_active_candidate_forward_probe_not_live_admission_truth",
        "active_hotlane_wallets": selected_wallets,
        "mission_anchor_probe_candidates_added": len(mission_anchor_candidates),
        "mission_anchor_wallets": [
            wallet
            for wallet, active_row in selected_by_wallet.items()
            if _is_mission_anchor_active_row(active_row)
        ],
        "operator_mission_anchor_wallets": [
            wallet
            for wallet, active_row in selected_by_wallet.items()
            if "operator_mission_anchor" in _active_row_tags(active_row)
        ],
        "runtime_tracker_contexts": runtime_contexts,
        "ranked_probe_candidates": probe_candidates,
        "best_candidate": selected or {},
        "decision": {
            "status": "ANALYZE",
            "candidate_selection_status": "ANALYZE",
            "live_admission_status": "ANALYZE",
            "live_orders_allowed": False,
            "paper_only": True,
            "reason": (
                "operator_mission_anchor_forward_probe_selected"
                if selected
                and (
                    "operator_mission_anchor_probe"
                    in (selected.get("probe_selection_reasons") or [])
                )
                else
                "active_forward_probe_selected_non_admission_candidate"
                if selected
                else "no_active_hotlane_single_wallet_candidate_available"
            ),
            "live_admission_blockers": [
                "active_forward_probe_not_live_admission_truth",
                *(
                    [
                        f"candidate_blocker:{blocker}"
                        for blocker in (selected.get("blockers") or [])
                    ]
                    if selected
                    else []
                ),
            ],
        },
    }
    if selected:
        payload["selected_probe"] = {
            "candidate_id": selected.get("candidate_id"),
            "source_wallet": _wallet(selected),
            "wallet_name": (selected.get("metadata") or {}).get("wallet_name")
            if isinstance(selected.get("metadata"), dict)
            else None,
            "policy_id": (selected.get("policy") or {}).get("policy_id")
            if isinstance(selected.get("policy"), dict)
            else None,
            "status": selected.get("status"),
            "blockers": selected.get("blockers") or [],
            "active_hotlane": selected_by_wallet.get(_wallet(selected), {}),
            "summary": selected.get("summary") if isinstance(selected.get("summary"), dict) else {},
            "validation_summary": selected.get("validation_summary")
            if isinstance(selected.get("validation_summary"), dict)
            else {},
            "active_signal_summary": selected.get("active_signal_summary") or {},
            "probe_rank": selected.get("probe_rank"),
            "probe_selection_reasons": selected.get("probe_selection_reasons") or [],
            "runtime_tracker_context": selected.get("runtime_tracker_context"),
        }
    return payload


def main() -> int:
    args = parse_args()
    profit = load_json(args.profit_state, default={})
    active_hotlane = load_json(args.active_hotlane_state, default={})
    if not isinstance(profit, dict):
        profit = {}
    if not isinstance(active_hotlane, dict):
        active_hotlane = {}
    runtime_states: list[dict[str, Any]] = []
    for state_path in args.runtime_tracker_state or []:
        state = load_json(state_path, default={})
        if isinstance(state, dict):
            runtime_states.append(state)
    payload = select_active_forward_probe(
        profit,
        active_hotlane,
        max_probe_candidates=int(args.max_probe_candidates),
        runtime_tracker_states=runtime_states,
    )
    atomic_write_json(args.output, payload)
    print(
        json.dumps(
            {
                "status": "PASS" if payload.get("selected_probe") else "WATCH",
                "output": args.output,
                "selected_probe": payload.get("selected_probe"),
                "probe_candidates": len(payload.get("ranked_probe_candidates") or []),
                "paper_only": True,
                "live_orders_allowed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if payload.get("selected_probe") else 2


if __name__ == "__main__":
    raise SystemExit(main())
