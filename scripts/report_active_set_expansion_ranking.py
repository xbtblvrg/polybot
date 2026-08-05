#!/usr/bin/env python3
"""Rank active-set expansion candidates against the current Fable gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_FULL_POOL_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_MEMBER_BAR_REPLAY = "data/research/wallet_copy_active_set_member_bar_replay.json"
DEFAULT_SLOW_MARKET_RANKING = "data/research/slow_market_candidate_ranking_20260705.json"
DEFAULT_SLOW_MARKET_MEASUREMENT = "data/research/slow_market_paper_measurement_state.json"
DEFAULT_SLOW_MARKET_STATUS = "data/research/slow_market_paper_qualification_status.json"
DEFAULT_LIVE_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_ABANDONED = "data/research/slow_market_abandoned_wallets.json"
DEFAULT_OUTPUT = "data/research/active_set_expansion_fable_ranking.json"
FULL_POOL_PROMOTION_POLICY_ID = "protection_refill_0.10_cap_8_auto_degrade_le_50"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-pool-replay", default=DEFAULT_FULL_POOL_REPLAY)
    parser.add_argument("--member-bar-replay", default=DEFAULT_MEMBER_BAR_REPLAY)
    parser.add_argument("--slow-market-ranking", default=DEFAULT_SLOW_MARKET_RANKING)
    parser.add_argument("--slow-market-measurement", default=DEFAULT_SLOW_MARKET_MEASUREMENT)
    parser.add_argument("--slow-market-status", default=DEFAULT_SLOW_MARKET_STATUS)
    parser.add_argument("--live-guard-state", default=DEFAULT_LIVE_GUARD_STATE)
    parser.add_argument("--abandoned", default=DEFAULT_ABANDONED)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=100)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _active_wallets(payload: dict[str, Any]) -> set[str]:
    active_set = payload.get("active_set") if isinstance(payload.get("active_set"), dict) else {}
    return {
        wallet
        for member in active_set.get("members") or []
        if isinstance(member, dict)
        for wallet in [_norm_wallet(member.get("source_wallet") or member.get("wallet"))]
        if wallet
    }


def _active_members(payload: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = payload.get("active_set") if isinstance(payload.get("active_set"), dict) else {}
    return [member for member in active_set.get("members") or [] if isinstance(member, dict)]


def _abandoned_wallets(payload: Any) -> set[str]:
    rows = payload.get("wallets") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return set()
    return {
        wallet
        for row in rows
        if isinstance(row, dict)
        for wallet in [_norm_wallet(row.get("wallet"))]
        if wallet
    }


def _policy_family_allowed(*, source: str, candidate_id: str, policy_id: str) -> bool:
    policy = str(policy_id or "")
    candidate = str(candidate_id or "")
    return (
        policy.startswith("protection_refill_")
        or source == "full_pool_replay"
        or source == "member_bar_replay"
        or candidate.startswith("active_set_member_bar_")
    )


def _resolved_paper_fills(replay: dict[str, Any]) -> int:
    clob_resolved = replay.get("candidate_clob_backed_resolved_orders")
    if clob_resolved is not None:
        return int(clob_resolved or 0)
    return int(replay.get("resolved_orders") or 0)


def _ranked_replay_rows(
    *,
    source: str,
    replay_payload: dict[str, Any],
    active_wallets: set[str],
    abandoned_wallets: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in replay_payload.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        if not wallet:
            continue
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        candidate_id = str(candidate.get("candidate_id") or "")
        evidence_policy_id = str(replay.get("policy_id") or candidate.get("policy_id") or "")
        policy_id = FULL_POOL_PROMOTION_POLICY_ID if source == "full_pool_replay" else evidence_policy_id
        resolved_fills = _resolved_paper_fills(replay)
        pnl = num(replay.get("paper_pnl_usd"), None)
        copyable = int(replay.get("copyable_buy_events") or 0)
        clob_backed = int(replay.get("candidate_clob_backed_orders") or 0)
        allowed_policy = _policy_family_allowed(source=source, candidate_id=candidate_id, policy_id=policy_id)
        gate_reasons: list[str] = []
        if wallet in active_wallets:
            gate_reasons.append("already_active")
        if wallet in abandoned_wallets:
            gate_reasons.append("abandoned_listed")
        if not allowed_policy:
            gate_reasons.append("policy_family_not_allowed")
        if resolved_fills < 12:
            gate_reasons.append("resolved_paper_fills_below_12")
        if pnl is None or pnl <= 0:
            gate_reasons.append("paper_pnl_not_positive")
        ready = not gate_reasons
        rows.append(
            {
                "source": source,
                "wallet": wallet,
                "candidate_id": candidate_id,
                "policy_id": policy_id,
                "evidence_policy_id": evidence_policy_id,
                "promotion_ready": ready,
                "gate_reasons": gate_reasons,
                "resolved_paper_fills_at_our_prices": resolved_fills,
                "paper_pnl_usd": pnl,
                "copyable_buy_events": copyable,
                "candidate_clob_backed_orders": clob_backed,
                "paper_orders": int(replay.get("paper_orders") or 0),
                "replay_status": str(replay.get("eligibility_status") or replay.get("status") or ""),
                "replay_failure_reasons": [
                    str(reason) for reason in (replay.get("failure_reasons") or []) if str(reason or "")
                ],
                "unresolved_ratio": replay.get("unresolved_ratio"),
            }
        )
    return rows


def _slow_market_rows(payload: dict[str, Any], *, active_wallets: set[str], abandoned_wallets: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in payload.get("ranked_wallets") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet"))
        if not wallet:
            continue
        gate_reasons = ["missing_resolved_paper_replay"]
        if wallet in active_wallets:
            gate_reasons.append("already_active")
        if wallet in abandoned_wallets:
            gate_reasons.append("abandoned_listed")
        rows.append(
            {
                "source": "slow_market_ranking",
                "wallet": wallet,
                "candidate_id": candidate.get("name") or f"slow_market_{wallet[-12:]}",
                "policy_id": "",
                "promotion_ready": False,
                "gate_reasons": gate_reasons,
                "resolved_paper_fills_at_our_prices": int(candidate.get("paper_resolved_orders") or 0),
                "paper_pnl_usd": candidate.get("paper_pnl_usd"),
                "copyable_buy_events": None,
                "candidate_clob_backed_orders": None,
                "paper_orders": int(candidate.get("paper_orders") or 0),
                "replay_status": "SLOW_MARKET_RANKED_NOT_REPLAYED",
                "replay_failure_reasons": [],
                "unresolved_ratio": None,
                "slow_market_rank": candidate.get("rank"),
            }
        )
    return rows


def _sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def has_positive_12(row: dict[str, Any]) -> bool:
        return int(row.get("resolved_paper_fills_at_our_prices") or 0) >= 12 and num(
            row.get("paper_pnl_usd"), 0.0
        ) > 0.0

    return sorted(
        rows,
        key=lambda row: (
            0 if row.get("promotion_ready") else 1,
            0 if has_positive_12(row) else 1,
            0 if row.get("source") != "slow_market_ranking" else 1,
            0 if "policy_family_not_allowed" not in row.get("gate_reasons", []) else 1,
            0 if "paper_pnl_not_positive" not in row.get("gate_reasons", []) else 1,
            0 if "resolved_paper_fills_below_12" not in row.get("gate_reasons", []) else 1,
            -num(row.get("paper_pnl_usd"), -1_000_000.0),
            -int(row.get("resolved_paper_fills_at_our_prices") or 0),
            str(row.get("wallet") or ""),
        ),
    )


def _reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for reason in row.get("gate_reasons") or []:
            counts[str(reason)] = counts.get(str(reason), 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def build_report(
    *,
    full_pool_replay: dict[str, Any],
    member_bar_replay: dict[str, Any],
    slow_market_ranking: dict[str, Any],
    slow_market_measurement: dict[str, Any],
    slow_market_status: dict[str, Any],
    live_guard_state: dict[str, Any],
    abandoned_payload: Any,
    limit: int,
) -> dict[str, Any]:
    active = _active_wallets(live_guard_state)
    active_members = _active_members(live_guard_state)
    abandoned = _abandoned_wallets(abandoned_payload)
    rows = []
    rows.extend(
        _ranked_replay_rows(
            source="full_pool_replay",
            replay_payload=full_pool_replay,
            active_wallets=active,
            abandoned_wallets=abandoned,
        )
    )
    rows.extend(
        _ranked_replay_rows(
            source="member_bar_replay",
            replay_payload=member_bar_replay,
            active_wallets=active,
            abandoned_wallets=abandoned,
        )
    )
    rows.extend(_slow_market_rows(slow_market_ranking, active_wallets=active, abandoned_wallets=abandoned))
    ranked = _sort_rows(rows)
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    ready = [row for row in ranked if row.get("promotion_ready")]
    active_set_payload = live_guard_state.get("active_set") if isinstance(live_guard_state.get("active_set"), dict) else {}
    target_active_max = int(active_set_payload.get("target_member_count_max") or 8)
    open_active_slots = max(0, target_active_max - len(active))
    ready_now = ready[: min(2, open_active_slots)]
    active_expansion_members = [
        {
            "candidate_id": member.get("candidate_id"),
            "source_wallet": member.get("source_wallet"),
            "policy_id": member.get("policy_id"),
            "status": member.get("status"),
        }
        for member in active_members
        if str(member.get("candidate_id") or "").startswith("expansion_rank_")
    ]
    full_pool_positive_12 = [
        row
        for row in rows
        if row.get("source") == "full_pool_replay"
        and int(row.get("resolved_paper_fills_at_our_prices") or 0) >= 12
        and num(row.get("paper_pnl_usd"), 0.0) > 0.0
    ]
    member_bar_positive_12 = [
        row
        for row in rows
        if row.get("source") == "member_bar_replay"
        and int(row.get("resolved_paper_fills_at_our_prices") or 0) >= 12
        and num(row.get("paper_pnl_usd"), 0.0) > 0.0
    ]
    return {
        "schema_version": 1,
        "kind": "active_set_expansion_fable_ranking",
        "flow_stage": "PROMOTE/LEARN/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "fable_direction": "2026-07-06T15:45Z active-set expansion ranking NOW",
        "promotion_gate": {
            "max_promotions": 2,
            "requires_resolved_paper_fills_at_our_prices_gte": 12,
            "requires_positive_paper_pnl": True,
            "requires_not_abandoned_listed": True,
            "requires_existing_conservative_policy_family": "protection_refill/member_bar",
        },
        "summary": {
            "active_wallets": len(active),
            "abandoned_wallets": len(abandoned),
            "full_pool_candidates": len(full_pool_replay.get("candidates") or []),
            "full_pool_positive_resolved12": len(full_pool_positive_12),
            "member_bar_candidates": len(member_bar_replay.get("candidates") or []),
            "member_bar_positive_resolved12": len(member_bar_positive_12),
            "slow_market_ranked_wallets": len(slow_market_ranking.get("ranked_wallets") or []),
            "slow_market_measurement_pnl_usd": (slow_market_measurement.get("summary") or {}).get("paper_pnl_usd"),
            "slow_market_measurement_copyable_buy_events": (slow_market_measurement.get("summary") or {}).get(
                "copyable_buy_events"
            ),
            "target_active_max": target_active_max,
            "open_active_slots": open_active_slots,
            "active_expansion_members": len(active_expansion_members),
            "promotion_ready_backlog_count": len(ready),
            "promotions_allowed": len(ready_now),
            "decision": "PROMOTE" if ready_now else "ACTIVE_SET_AT_MAX_RANKING_ONLY"
            if open_active_slots == 0
            else "NO_PROMOTION_RANKING_ONLY",
            "gate_reason_counts": _reason_counts(rows),
        },
        "source_status": {
            "full_pool_replay_summary": full_pool_replay.get("replay_summary") or {},
            "member_bar_replay_summary": member_bar_replay.get("replay_summary") or {},
            "slow_market_ranking_status": slow_market_ranking.get("status"),
            "slow_market_measurement_status": slow_market_measurement.get("status"),
            "slow_market_status": slow_market_status.get("status"),
            "slow_market_next_action": slow_market_status.get("next_action"),
        },
        "active_expansion_members": active_expansion_members,
        "ready_candidates": ready_now,
        "ready_backlog_candidates": ready[:2],
        "ranked_candidates": ranked[: max(1, int(limit))],
    }


def main() -> int:
    args = parse_args()
    payload = build_report(
        full_pool_replay=load_json(args.full_pool_replay, default={}),
        member_bar_replay=load_json(args.member_bar_replay, default={}),
        slow_market_ranking=load_json(args.slow_market_ranking, default={}),
        slow_market_measurement=load_json(args.slow_market_measurement, default={}),
        slow_market_status=load_json(args.slow_market_status, default={}),
        live_guard_state=load_json(args.live_guard_state, default={}),
        abandoned_payload=load_json(args.abandoned, default={}),
        limit=int(args.limit),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
