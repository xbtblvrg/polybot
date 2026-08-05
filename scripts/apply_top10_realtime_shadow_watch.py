#!/usr/bin/env python3
"""Apply Fable's top-10 realtime-shadow watch reclassification."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_CLEARANCE = "data/research/wallet_copy_top10_watch_clearance_summary.json"
DEFAULT_PARITY = "data/research/wallet_copy_top10_guard_parity_rescore.json"
DEFAULT_SHADOW_STATE = "data/research/wallet_copy_realtime_shadow_watch_state.json"
DEFAULT_LANE_STATE = "data/research/wallet_copy_realtime_shadow_watch_lane_state.json"
DEFAULT_MEMBER_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"

WATCH_STATUS = "WATCH_REALTIME_SHADOW_REQUIRED"
FABLE_EXTRA_SHADOW_WATCH_WALLETS = {
    "0xad825954d08beba32f74b594821f4251460c3df1",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clearance-summary", default=DEFAULT_CLEARANCE)
    parser.add_argument("--parity-rescore", default=DEFAULT_PARITY)
    parser.add_argument("--output-clearance-summary", default=DEFAULT_CLEARANCE)
    parser.add_argument("--shadow-state", default=DEFAULT_SHADOW_STATE)
    parser.add_argument("--lane-state", default=DEFAULT_LANE_STATE)
    parser.add_argument("--member-queue", default=DEFAULT_MEMBER_QUEUE)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _parity_by_wallet(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("rows") if isinstance(payload.get("rows"), list) else []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if wallet:
            out[wallet] = row
    return out


def _should_reclassify(row: dict[str, Any], parity: dict[str, Any]) -> bool:
    status = str(row.get("clearance_status") or "")
    if status == WATCH_STATUS:
        return True
    if status == "ANALYZE_PARTIAL_SAMPLE":
        return True
    if status == "FAIL_LANE_COVERAGE":
        return True
    return False


def _reclass_reason(row: dict[str, Any], parity: dict[str, Any]) -> str:
    status = str(row.get("clearance_status") or "")
    prior = str(row.get("prior_clearance_status") or "")
    if status == WATCH_STATUS:
        if prior == "ANALYZE_PARTIAL_SAMPLE":
            return "decisive_replay_parity_estimate_requires_realtime_taker_shadow"
        if prior == "FAIL_LANE_COVERAGE":
            return "lane_coverage_fail_voided_by_source_live_replay_artifact_forensics"
        return str(row.get("clearance_reason") or "realtime_shadow_required")
    if status == "ANALYZE_PARTIAL_SAMPLE":
        return "decisive_replay_parity_estimate_requires_realtime_taker_shadow"
    if status == "FAIL_LANE_COVERAGE":
        return "lane_coverage_fail_voided_by_source_live_replay_artifact_forensics"
    return "unchanged"


def _source_live_counts(parity_row: dict[str, Any]) -> dict[str, int]:
    rejects = parity_row.get("rejects") if isinstance(parity_row.get("rejects"), list) else []
    source_live = 0
    dead_at_source = 0
    replay_artifacts = 0
    for reject in rejects:
        if not isinstance(reject, dict):
            continue
        if reject.get("live_at_source_plus_latency"):
            source_live += 1
        else:
            dead_at_source += 1
        if reject.get("replay_artifact_book_expired"):
            replay_artifacts += 1
    return {
        "source_live_rejects": source_live,
        "dead_at_source_rejects": dead_at_source,
        "replay_artifact_book_expired_rejects": replay_artifacts,
    }


def _lane_state_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "wallet": row.get("wallet"),
        "candidate_id": row.get("candidate_id"),
        "clearance_status": row.get("clearance_status"),
        "prior_clearance_status": row.get("prior_clearance_status"),
        "primary_market_category": "btc_5m",
        "tags": ["WATCH_REALTIME_SHADOW_REQUIRED", "paper_only", "btc_5m"],
        "realtime_shadow_required": True,
        "promotion_arithmetic": "realtime_shadow_taker_fills_only",
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _extra_shadow_watch_rows_from_queue(queue: dict[str, Any], wallets: set[str]) -> list[dict[str, Any]]:
    requested = {_norm_wallet(wallet) for wallet in wallets}
    requested.discard("")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in queue.get("ranked_members") if isinstance(queue.get("ranked_members"), list) else []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if wallet not in requested or wallet in seen:
            continue
        replay = row.get("replay") if isinstance(row.get("replay"), dict) else {}
        rows.append(
            {
                "wallet": wallet,
                "candidate_id": replay.get("candidate_id") or row.get("name") or f"shadow_watch_{wallet[-12:]}",
                "clearance_status": WATCH_STATUS,
                "prior_clearance_status": row.get("queue_source") or "member_queue",
                "clearance_reason": (
                    "fable_shadow_watch_until_20_copyable_clob_backed_buys;"
                    f"current_copyable_buy_events={int(replay.get('copyable_buy_events') or 0)}"
                ),
                "ready_for_live": False,
                "realtime_shadow_required": True,
                "promotion_arithmetic": "realtime_shadow_taker_fills_only",
                "parity_prior": {
                    "source": "member_queue_shadow_override",
                    "queue_rank": row.get("queue_rank"),
                    "queue_source": row.get("queue_source"),
                    "copyable_buy_events": int(replay.get("copyable_buy_events") or 0),
                    "candidate_clob_backed_orders": int(replay.get("candidate_clob_backed_orders") or 0),
                    "paper_pnl_usd": replay.get("paper_pnl_usd"),
                    "promotion_min_copyable_buy_events": 20,
                },
            }
        )
        seen.add(wallet)
    for wallet in sorted(requested - seen):
        rows.append(
            {
                "wallet": wallet,
                "candidate_id": f"shadow_watch_{wallet[-12:]}",
                "clearance_status": WATCH_STATUS,
                "prior_clearance_status": "missing_member_queue_row",
                "clearance_reason": "fable_shadow_watch_requested_but_member_queue_row_missing",
                "ready_for_live": False,
                "realtime_shadow_required": True,
                "promotion_arithmetic": "realtime_shadow_taker_fills_only",
                "parity_prior": {
                    "source": "member_queue_shadow_override_missing",
                    "promotion_min_copyable_buy_events": 20,
                },
            }
        )
    return rows


def apply_reclassification(
    *,
    clearance_summary: dict[str, Any],
    parity_rescore: dict[str, Any],
    extra_watch_rows: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    parity = _parity_by_wallet(parity_rescore)
    rows = []
    watch_rows = []
    passive_rows = []
    for row in clearance_summary.get("rows") if isinstance(clearance_summary.get("rows"), list) else []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        parity_row = parity.get(wallet, {})
        new_row = dict(row)
        old_status = str(row.get("clearance_status") or "")
        if _should_reclassify(row, parity_row):
            new_row["prior_clearance_status"] = row.get("prior_clearance_status") or old_status
            new_row["clearance_status"] = WATCH_STATUS
            new_row["clearance_reason"] = _reclass_reason(row, parity_row)
            new_row["ready_for_live"] = False
            new_row["realtime_shadow_required"] = True
            new_row["promotion_arithmetic"] = "realtime_shadow_taker_fills_only"
            new_row["parity_prior"] = {
                "parity_taker_fillable_orders": parity_row.get("parity_taker_fillable_orders"),
                "parity_maker_maybe_fill_orders": parity_row.get("parity_maker_maybe_fill_orders"),
                "copyable_rate_pct_parity_taker": parity_row.get("copyable_rate_pct_parity_taker"),
                "copyable_rate_pct_parity_taker_plus_maker_maybe": parity_row.get(
                    "copyable_rate_pct_parity_taker_plus_maker_maybe"
                ),
                "reject_category_counts": parity_row.get("reject_category_counts"),
                "source_live_at_trade_time_rejects": parity_row.get("source_live_at_trade_time_rejects"),
                "replay_artifact_book_expired_rejects": parity_row.get("replay_artifact_book_expired_rejects"),
                "source_live_forensics": _source_live_counts(parity_row),
            }
            watch_rows.append(new_row)
        elif old_status == "ANALYZE_NO_RECENT_BUY_SAMPLE":
            new_row["realtime_shadow_required"] = True
            new_row["shadow_coverage_mode"] = "passive_if_wallet_trades"
            passive_rows.append(new_row)
        rows.append(new_row)

    existing_watch_wallets = {_norm_wallet(row.get("wallet")) for row in [*watch_rows, *passive_rows]}
    extra_watch_rows = extra_watch_rows or []
    for row in extra_watch_rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet or wallet in existing_watch_wallets:
            continue
        new_row = dict(row)
        new_row["clearance_status"] = WATCH_STATUS
        new_row["ready_for_live"] = False
        new_row["realtime_shadow_required"] = True
        new_row["promotion_arithmetic"] = "realtime_shadow_taker_fills_only"
        watch_rows.append(new_row)
        existing_watch_wallets.add(wallet)

    status_counts = Counter(str(row.get("clearance_status") or "") for row in rows)
    summary = dict(clearance_summary.get("summary") if isinstance(clearance_summary.get("summary"), dict) else {})
    summary.update(
        {
            "clearance_status_counts": dict(sorted(status_counts.items())),
            "watch_realtime_shadow_required_count": int(status_counts.get(WATCH_STATUS) or 0),
            "fail_lane_coverage_count": int(status_counts.get("FAIL_LANE_COVERAGE") or 0),
            "analyze_count": sum(
                count for status, count in status_counts.items() if str(status).startswith("ANALYZE_")
            ),
            "definitive_count": sum(
                count for status, count in status_counts.items() if str(status).startswith("DEFINITIVE_")
            ),
            "unmeasurable_count": sum(
                count for status, count in status_counts.items() if str(status).startswith("UNMEASURABLE_")
            ),
            "ready_for_live": 0,
            "extra_shadow_watch_wallets": len(extra_watch_rows),
        }
    )
    updated = {
        **clearance_summary,
        "generated_at": utc_now_iso(),
        "summary": summary,
        "rows": rows,
        "next": "score reclassified wallets with realtime paper-only shadow; promotion arithmetic uses realtime taker fills only",
    }
    shadow_state = {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_shadow_watch_state",
        "flow_stage": "PROMOTE/LEARN/OBSERVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": "ARMED",
        "generated_at": utc_now_iso(),
        "source_artifacts": {
            "clearance_summary": DEFAULT_CLEARANCE,
            "parity_rescore": DEFAULT_PARITY,
            "lane_state": DEFAULT_LANE_STATE,
            "guard_shadow_lanes_state": "data/research/wallet_copy_guard_shadow_lanes_state.json",
            "guard_shadow_lanes_events": "data/research/wallet_copy_guard_shadow_lanes_events.jsonl",
        },
        "scoring_contract": {
            "max_book_age_s": 5.0,
            "persist_required_fields": [
                "source_ts",
                "book_ts",
                "book_age_s",
                "taker_fillable",
                "parity_fillable",
                "needed_bps",
            ],
            "promotion_arithmetic": "realtime_shadow_taker_fills_only",
            "maker_maybe_counts_as_admission_fill": False,
            "shadow_must_submit_orders": False,
        },
        "summary": {
            "registered_wallets": len(watch_rows),
            "passive_wallets": len(passive_rows),
            "ready_for_live": 0,
        },
        "wallets": [
            {
                "wallet": row.get("wallet"),
                "candidate_id": row.get("candidate_id"),
                "prior_clearance_status": row.get("prior_clearance_status"),
                "status": WATCH_STATUS,
                "reason": row.get("clearance_reason"),
                "parity_prior": row.get("parity_prior"),
            }
            for row in watch_rows
        ],
        "passive_wallets": [
            {
                "wallet": row.get("wallet"),
                "candidate_id": row.get("candidate_id"),
                "status": row.get("clearance_status"),
                "shadow_coverage_mode": row.get("shadow_coverage_mode"),
            }
            for row in passive_rows
        ],
        "next": "existing paper/shadow capture must score live source buys for these wallets within 60s and persist book_age_s",
    }
    lane_state = {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_shadow_watch_lane_state",
        "flow_stage": "OBSERVE/PROMOTE/LEARN",
        "status": "ARMED",
        "paper_only": True,
        "live_orders_allowed": False,
        "updated_at": utc_now_iso(),
        "summary": {
            "selected_wallets": len(watch_rows) + len(passive_rows),
            "primary_shadow_wallets": len(watch_rows),
            "passive_wallets": len(passive_rows),
            "ready_for_live": 0,
        },
        "ranked_wallets": [_lane_state_row(row) for row in [*watch_rows, *passive_rows]],
        "next": "run_top10_broad_paper_lane.py consumes this lane-state with max_receipt_to_fetch_age_s<=60",
    }
    return updated, shadow_state, lane_state


def main() -> int:
    args = parse_args()
    updated, shadow_state, lane_state = apply_reclassification(
        clearance_summary=load_json(args.clearance_summary, default={}),
        parity_rescore=load_json(args.parity_rescore, default={}),
        extra_watch_rows=_extra_shadow_watch_rows_from_queue(
            load_json(args.member_queue, default={}),
            FABLE_EXTRA_SHADOW_WATCH_WALLETS,
        ),
    )
    atomic_write_json(args.output_clearance_summary, updated)
    atomic_write_json(args.shadow_state, shadow_state)
    atomic_write_json(args.lane_state, lane_state)
    print(
        json.dumps(
            {"summary": updated.get("summary"), "shadow": shadow_state.get("summary"), "lane": lane_state.get("summary")},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
