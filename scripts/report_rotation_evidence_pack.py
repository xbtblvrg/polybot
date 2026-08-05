#!/usr/bin/env python3
"""Build the M4 rotation evidence pack without mutating live configuration."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json


def _load_json(path: str | Path, default: Any) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        return json.loads(target.read_text())
    except Exception:
        return default


def _active_members(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    return [member for member in members if isinstance(member, dict)]


def _live_member_pnl_index(state_digest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    active_set = state_digest.get("active_set") if isinstance(state_digest.get("active_set"), dict) else {}
    rows = active_set.get("live_members_today") if isinstance(active_set.get("live_members_today"), list) else []
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, dict) and row.get("wallet"):
            index[str(row["wallet"]).lower()] = row
    return index


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _last_observed_by_wallet(guard_state: dict[str, Any]) -> dict[str, float]:
    participation = (
        guard_state.get("window_participation")
        if isinstance(guard_state.get("window_participation"), dict)
        else {}
    )
    rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    latest: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if not wallet:
            continue
        observed = (
            _as_float(row.get("effective_latest_observed_ts"))
            or _as_float(row.get("latest_observed_ts"))
            or _as_float(row.get("freshness_watermark_ts"))
        )
        if observed is None:
            continue
        latest[wallet] = max(latest.get(wallet, 0.0), observed)
    return latest


def _hours_since(last_observed_ts: float | None, *, now_ts: float) -> float | None:
    if last_observed_ts is None or last_observed_ts <= 0:
        return None
    return round(max(0.0, now_ts - last_observed_ts) / 3600.0, 6)


def _member_signal_rows(
    *,
    members: list[dict[str, Any]],
    deadman_state: dict[str, Any],
    pnl_index: dict[str, dict[str, Any]],
    last_observed_by_wallet: dict[str, float],
    now_ts: float,
) -> list[dict[str, Any]]:
    signal_age = (
        deadman_state.get("member_signal_age") if isinstance(deadman_state.get("member_signal_age"), dict) else {}
    )
    rows: list[dict[str, Any]] = []
    for member in members:
        wallet = str(member.get("source_wallet") or "").lower()
        age = signal_age.get(wallet) if isinstance(signal_age.get(wallet), dict) else {}
        pnl = pnl_index.get(wallet, {})
        suppressed = int(age.get("suppressed_intents") or 0)
        eligible = int(age.get("eligible_intents") or 0)
        pnl_usd = float(pnl.get("pnl_usd") or 0.0)
        today_orders = int(pnl.get("orders") or 0)
        today_fills = int(pnl.get("fills") or 0)
        today_rejects = int(pnl.get("rejects") or 0)
        if eligible > 0:
            recommendation = "HOLD_HAS_ELIGIBLE_FLOW_WAIT_FOR_FAST_FEED_RULING"
        elif suppressed > 0:
            recommendation = "ROTATE_REVIEW_STALE_SUPPRESSED_FLOW_AT_0139Z_RULING"
        elif today_orders > 0 or today_fills > 0:
            recommendation = "HOLD_ACTIVE_LIVE_FLOW_WAIT_FOR_RAMP_RULING"
        elif pnl_usd < -8.0:
            recommendation = "ROTATE_REVIEW_NEGATIVE_MEMBER_AT_0139Z_RULING"
        else:
            recommendation = "HOLD_NO_FRESH_ROTATION_EVIDENCE"
        rows.append(
            {
                "candidate_id": member.get("candidate_id"),
                "source_wallet": wallet,
                "policy_id": member.get("policy_id"),
                "is_current_cycle_member": bool(member.get("is_current_cycle_member")),
                "eligible_intents": eligible,
                "suppressed_intents": suppressed,
                "signal_age_count": int(age.get("signal_age_count") or 0),
                "signal_age_p50_s": age.get("signal_age_p50_s"),
                "signal_age_p90_s": age.get("signal_age_p90_s"),
                "today_orders": today_orders,
                "today_fills": today_fills,
                "today_rejects": today_rejects,
                "today_pnl_usd": pnl_usd,
                "hours_since_last_observed_source_trade": _hours_since(
                    last_observed_by_wallet.get(wallet),
                    now_ts=now_ts,
                ),
                "recommendation": recommendation,
                "evidence_line": (
                    f"eligible={eligible} suppressed={suppressed} "
                    f"p90={age.get('signal_age_p90_s')} pnl={pnl_usd}"
                ),
            }
        )
    return rows


def _flow_truth_consistency(
    deadman_state: dict[str, Any],
    flow_truth_state: dict[str, Any],
    *,
    tolerance_s: float,
) -> dict[str, Any]:
    drought = _as_float(deadman_state.get("eligible_drought_s"))
    flow_truth_drought = _as_float(flow_truth_state.get("eligible_drought_s"))
    delta = (
        round(abs(drought - flow_truth_drought), 6)
        if drought is not None and flow_truth_drought is not None
        else None
    )
    status_match = deadman_state.get("eligible_drought_status") == flow_truth_state.get("eligible_drought_status")
    latest_order_match = deadman_state.get("latest_order_ts") == flow_truth_state.get("latest_order_ts")
    passed = (
        drought is not None
        and flow_truth_drought is not None
        and delta is not None
        and delta <= tolerance_s
        and status_match
        and latest_order_match
    )
    latest_order = deadman_state.get("latest_order_ts")
    checked_at = deadman_state.get("checked_at")
    return {
        "status": "PASS" if passed else "FAIL",
        "eligible_drought_s": drought,
        "flow_truth_eligible_drought_s": flow_truth_drought,
        "eligible_drought_delta_s": delta,
        "eligible_drought_status": deadman_state.get("eligible_drought_status"),
        "flow_truth_eligible_drought_status": flow_truth_state.get("eligible_drought_status"),
        "eligible_drought_status_match": status_match,
        "latest_order_ts": latest_order,
        "flow_truth_latest_order_ts": flow_truth_state.get("latest_order_ts"),
        "latest_order_ts_match": latest_order_match,
        "checked_at": checked_at,
        "tolerance_s": float(tolerance_s),
        "source": "scripts/order_flow_deadman.py refreshed state row compared to state_digest.order_flow_deadman",
    }


def _queue_rows(queue_state: dict[str, Any], active_wallets: set[str], *, limit: int) -> list[dict[str, Any]]:
    ranked = queue_state.get("ranked_members") if isinstance(queue_state.get("ranked_members"), list) else []
    rows: list[dict[str, Any]] = []
    for row in ranked:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").lower()
        if wallet in active_wallets:
            continue
        rows.append(
            {
                "queue_rank": row.get("queue_rank"),
                "wallet": wallet,
                "name": row.get("name"),
                "ready_for_live": bool(row.get("ready_for_live")),
                "clearance_ready": bool(row.get("clearance_ready")),
                "queue_source": row.get("queue_source"),
                "resolved_pnl": row.get("resolved_pnl"),
                "recent_fill_windows": row.get("recent_fill_windows"),
                "next_action": row.get("next_action"),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _cycle_payload(row: dict[str, Any]) -> dict[str, Any]:
    live_execution = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
    return live_execution or row


def _cycle_ts(row: dict[str, Any]) -> str:
    return str(row.get("generated_at") or row.get("ts") or row.get("checked_at") or "")


def _load_recent_guard_cycles(path: str | Path | None, *, tail_bytes: int) -> list[dict[str, Any]]:
    if not path:
        return []
    target = Path(path)
    if not target.exists():
        return []
    size = target.stat().st_size
    offset = max(0, size - max(1, int(tail_bytes)))
    with target.open("rb") as handle:
        handle.seek(offset)
        if offset:
            handle.readline()
        raw_lines = handle.readlines()
    rows: list[dict[str, Any]] = []
    for raw in raw_lines:
        if b"wallet_copy_live_guard_cycle" not in raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            continue
        if isinstance(row, dict) and row.get("event") == "wallet_copy_live_guard_cycle":
            rows.append(row)
    return rows


def _int_from(mapping: dict[str, Any], key: str) -> int:
    try:
        return int(mapping.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _reject_delta(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, int]:
    current_counts = current.get("reject_taxonomy_counts")
    previous_counts = previous.get("reject_taxonomy_counts")
    if not isinstance(current_counts, dict):
        current_counts = {}
    if not isinstance(previous_counts, dict):
        previous_counts = {}
    keys = set(current_counts) | set(previous_counts)
    delta: dict[str, int] = {}
    for key in sorted(keys):
        change = _int_from(current_counts, key) - _int_from(previous_counts, key)
        if change:
            delta[str(key)] = change
    return delta


def _format_delta(delta: dict[str, int]) -> str:
    if not delta:
        return "none"
    return ",".join(f"{key}:{value:+d}" for key, value in sorted(delta.items()))


def _eligible_but_unsubmitted_attribution(
    guard_event_log_path: str | Path | None,
    *,
    target_ts: str | None,
    tail_bytes: int,
) -> dict[str, Any]:
    cycles = _load_recent_guard_cycles(guard_event_log_path, tail_bytes=tail_bytes)
    if not cycles:
        return {
            "status": "MISSING",
            "line": "eligible_but_unsubmitted_attribution=MISSING: no recent guard cycle rows found",
        }

    selected_idx: int | None = None
    if target_ts:
        for idx, row in enumerate(cycles):
            if _cycle_ts(row) == target_ts:
                selected_idx = idx
                break
    if selected_idx is None:
        for idx in range(len(cycles) - 1, -1, -1):
            payload = _cycle_payload(cycles[idx])
            drought = payload.get("drought_funnel") if isinstance(payload.get("drought_funnel"), dict) else {}
            profit = (
                payload.get("profit_latency_suppression")
                if isinstance(payload.get("profit_latency_suppression"), dict)
                else {}
            )
            if (
                _int_from(drought, "fresh_candidate_intents") > 0
                and _int_from(profit, "output_intents") > 0
                and _int_from(drought, "orders_submitted") == 0
            ):
                selected_idx = idx
                break

    if selected_idx is None:
        return {
            "status": "NO_MATCH",
            "line": "eligible_but_unsubmitted_attribution=NO_MATCH: no post-filter zero-submit guard cycle in recent tail",
        }

    row = cycles[selected_idx]
    previous_row = cycles[selected_idx - 1] if selected_idx > 0 else {}
    payload = _cycle_payload(row)
    previous_payload = _cycle_payload(previous_row) if previous_row else {}
    drought = payload.get("drought_funnel") if isinstance(payload.get("drought_funnel"), dict) else {}
    previous_drought = (
        previous_payload.get("drought_funnel") if isinstance(previous_payload.get("drought_funnel"), dict) else {}
    )
    profit = (
        payload.get("profit_latency_suppression")
        if isinstance(payload.get("profit_latency_suppression"), dict)
        else {}
    )
    toxicity = payload.get("toxicity_protection") if isinstance(payload.get("toxicity_protection"), dict) else {}
    selected = payload.get("selected_candidate") if isinstance(payload.get("selected_candidate"), dict) else {}
    sample_filtered = toxicity.get("sample_filtered_intents")
    if not isinstance(sample_filtered, list):
        sample_filtered = []

    post_filter = _int_from(profit, "output_intents")
    orders_submitted = _int_from(drought, "orders_submitted")
    toxicity_blocked = _int_from(toxicity, "blocked_intents")
    profit_filtered = _int_from(profit, "filtered_intents")
    if post_filter > 0 and toxicity_blocked >= post_filter and _int_from(toxicity, "output_intents") == 0:
        consuming_gate = "toxicity_protection"
        consuming_detail = f"blocked={toxicity_blocked}/{post_filter}"
    elif profit_filtered > 0 and post_filter == 0:
        consuming_gate = "profit_latency_suppression"
        consuming_detail = f"filtered={profit_filtered}"
    elif orders_submitted > 0:
        consuming_gate = "none_submitted"
        consuming_detail = f"orders_submitted={orders_submitted}"
    else:
        consuming_gate = "unknown_post_filter_gate"
        consuming_detail = f"post_filter={post_filter},toxicity_blocked={toxicity_blocked}"

    intent_ids = [str(item.get("intent_id")) for item in sample_filtered if isinstance(item, dict) and item.get("intent_id")]
    price_buckets = sorted(
        {
            str(item.get("price_bucket"))
            for item in sample_filtered
            if isinstance(item, dict) and item.get("price_bucket")
        }
    )
    delta = _reject_delta(drought, previous_drought)
    line = (
        "eligible_but_unsubmitted_attribution="
        f"{_cycle_ts(row)} candidate={selected.get('candidate_id')} "
        f"wallet={selected.get('source_wallet')} post_filter={post_filter} "
        f"orders={orders_submitted} consumed_by={consuming_gate}({consuming_detail}) "
        f"market={sample_filtered[0].get('market_slug') if sample_filtered else ''} "
        f"buckets={','.join(price_buckets) or 'unknown'} "
        f"intents={','.join(intent_ids) or 'none'} "
        f"reject_delta_vs_prior={_format_delta(delta)}"
    )
    return {
        "status": "PASS" if consuming_gate != "unknown_post_filter_gate" else "ANALYZE",
        "line": line,
        "cycle_ts": _cycle_ts(row),
        "previous_cycle_ts": _cycle_ts(previous_row) if previous_row else None,
        "candidate_id": selected.get("candidate_id"),
        "source_wallet": selected.get("source_wallet"),
        "post_filter_intents": post_filter,
        "orders_submitted": orders_submitted,
        "consuming_gate": consuming_gate,
        "consuming_detail": consuming_detail,
        "reject_delta_vs_prior": delta,
        "sample_filtered_intents": sample_filtered,
        "source": "wallet_copy_live_guard_events.jsonl wallet_copy_live_guard_cycle drought_funnel/profit/toxicity delta",
    }


def build_pack(
    *,
    guard_state: dict[str, Any],
    deadman_state: dict[str, Any],
    queue_state: dict[str, Any],
    leaderboard_state: dict[str, Any],
    state_digest: dict[str, Any],
    top_n: int,
    guard_event_log_path: str | Path | None = None,
    eligible_target_ts: str | None = "2026-07-08T00:01:34.038125+00:00",
    guard_event_tail_bytes: int = 64 * 1024 * 1024,
) -> dict[str, Any]:
    members = _active_members(guard_state)
    active_wallets = {str(member.get("source_wallet") or "").lower() for member in members}
    now_ts = time.time()
    member_rows = _member_signal_rows(
        members=members,
        deadman_state=deadman_state,
        pnl_index=_live_member_pnl_index(state_digest),
        last_observed_by_wallet=_last_observed_by_wallet(guard_state),
        now_ts=now_ts,
    )
    queue_rows = _queue_rows(queue_state, active_wallets, limit=top_n)
    rotate_reviews = [row for row in member_rows if str(row.get("recommendation") or "").startswith("ROTATE_REVIEW")]
    flow_truth_state = (
        state_digest.get("order_flow_deadman") if isinstance(state_digest.get("order_flow_deadman"), dict) else {}
    )
    eligible_attribution = _eligible_but_unsubmitted_attribution(
        guard_event_log_path,
        target_ts=eligible_target_ts,
        tail_bytes=guard_event_tail_bytes,
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_rotation_evidence_pack",
        "flow_stage": "LIVE/ROTATE/LEARN",
        "generated_at": utc_now_iso(),
        "artifact_places_no_orders": True,
        "guard_live_orders_allowed": bool(guard_state.get("live_orders_allowed")),
        "guard_status": guard_state.get("status"),
        "no_rotation_executed": True,
        "direction_id": "2026-07-07T23:58Z-fable-M5",
        "deadman": {
            "status": deadman_state.get("status"),
            "checked_at": deadman_state.get("checked_at"),
            "latest_order_ts": deadman_state.get("latest_order_ts"),
            "eligible_drought_s": deadman_state.get("eligible_drought_s"),
            "eligible_drought_status": deadman_state.get("eligible_drought_status"),
            "fresh_stale_signal_rows": deadman_state.get("fresh_stale_signal_rows"),
        },
        "flow_truth_consistency": _flow_truth_consistency(
            deadman_state,
            flow_truth_state,
            tolerance_s=5.0,
        ),
        "eligible_but_unsubmitted_attribution": eligible_attribution.get("line"),
        "eligible_but_unsubmitted_attribution_details": eligible_attribution,
        "active_set": {
            "generated_at": (guard_state.get("active_set") or {}).get("generated_at")
            if isinstance(guard_state.get("active_set"), dict)
            else guard_state.get("generated_at"),
            "members": len(members),
            "current_candidate_id": guard_state.get("candidate_id"),
            "current_wallet": guard_state.get("source_wallet"),
            "current_policy_id": guard_state.get("policy_id"),
        },
        "member_signal_age_table": member_rows,
        "candidate_ranking_delta": {
            "queue_generated_at": queue_state.get("generated_at"),
            "queue_summary": queue_state.get("summary") if isinstance(queue_state.get("summary"), dict) else {},
            "leaderboard_generated_at": leaderboard_state.get("generated_at") or leaderboard_state.get("updated_at"),
            "leaderboard_summary": leaderboard_state.get("summary")
            if isinstance(leaderboard_state.get("summary"), dict)
            else {},
            "active_wallets_excluded": sorted(active_wallets),
            "top_non_active_candidates": queue_rows,
        },
        "recommendation_summary": {
            "members": len(member_rows),
            "rotate_review_members": len(rotate_reviews),
            "hold_members": len(member_rows) - len(rotate_reviews),
            "ready_non_active_candidates": sum(1 for row in queue_rows if row.get("ready_for_live")),
            "decision": "PACKAGE_FOR_0139Z_FABLE_RULING_NO_MECHANICAL_ROTATION_NOW",
            "next": "send with fast-feed scorecard after 2026-07-08T01:39:16Z",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--deadman-state", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--queue-state", default="data/research/wallet_copy_full_pool_member_queue.json")
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--state-digest", default="data/research/state_digest.json")
    parser.add_argument("--guard-event-log", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--output", default="data/research/wallet_copy_rotation_evidence_pack_latest.json")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--refresh-deadman", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--eligible-attribution-target-ts", default="2026-07-08T00:01:34.038125+00:00")
    parser.add_argument("--guard-event-tail-bytes", type=int, default=64 * 1024 * 1024)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.refresh_deadman:
        subprocess.run(
            [sys.executable, "scripts/order_flow_deadman.py"],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        subprocess.run(
            [sys.executable, "scripts/update_state_digest.py"],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    payload = build_pack(
        guard_state=_load_json(args.guard_state, {}),
        deadman_state=_load_json(args.deadman_state, {}),
        queue_state=_load_json(args.queue_state, {}),
        leaderboard_state=_load_json(args.leaderboard_state, {}),
        state_digest=_load_json(args.state_digest, {}),
        top_n=max(1, int(args.top_n)),
        guard_event_log_path=args.guard_event_log,
        eligible_target_ts=args.eligible_attribution_target_ts,
        guard_event_tail_bytes=max(1, int(args.guard_event_tail_bytes)),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
