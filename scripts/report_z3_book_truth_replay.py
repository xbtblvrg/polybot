#!/usr/bin/env python3
"""Report Z3 eligible wallets against captured alpha-decay book truth.

This is paper-only evidence plumbing for the autonomous heartbeat lane. It
does not fetch network data and does not touch live guard state.
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


DEFAULT_ALPHA_REPORT = "data/research/alpha_decay_report_20260706T0230Z.json"
DEFAULT_LANE_STATE = "data/research/wallet_copy_z3_eligible_profile_lane_state_20260706T0314Z.json"
DEFAULT_BOOK_TRUTH_REPLAY = "data/research/wallet_copy_z3_eligible_profile_book_truth_replay_20260706T0334Z.json"
DEFAULT_MEMBER_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_z3_book_truth_replay.json"
DEFAULT_FORWARD_OUTPUT = "data/research/wallet_copy_z3_eligible_forward_lane_state.json"
PROMOTION_MIN_COPYABLE_BUY_EVENTS = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alpha-report", default=DEFAULT_ALPHA_REPORT)
    parser.add_argument("--lane-state", default=DEFAULT_LANE_STATE)
    parser.add_argument("--book-truth-replay", default=DEFAULT_BOOK_TRUTH_REPLAY)
    parser.add_argument("--member-queue", default=DEFAULT_MEMBER_QUEUE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--forward-output", default=DEFAULT_FORWARD_OUTPUT)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _wallets_from_lane(state: dict[str, Any]) -> list[str]:
    wallets: list[str] = []
    for key in ("wallets", "selected_wallets", "ranked_wallets", "candidates", "rows", "top_candidates"):
        rows = state.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            wallet = ""
            if isinstance(row, dict):
                wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet") or row.get("wallet_address"))
            else:
                wallet = _norm_wallet(row)
            if wallet and wallet not in wallets:
                wallets.append(wallet)
    return wallets


def _row_blockers_from_replay(row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    copyable = int(_num(row.get("copyable_buy_events")))
    pnl = _num(row.get("paper_pnl_usd"))
    if copyable < PROMOTION_MIN_COPYABLE_BUY_EVENTS:
        blockers.append("copyable_buy_events_below_20_floor")
    if pnl <= 0.0:
        blockers.append("paper_pnl_not_positive")
    return blockers


def _build_report_from_book_truth_replay(
    *,
    source_replay: dict[str, Any],
    alpha_report: dict[str, Any],
    lane_state: dict[str, Any],
) -> dict[str, Any]:
    source_rows = source_replay.get("wallets") if isinstance(source_replay.get("wallets"), list) else []
    replay_by_wallet = {
        _norm_wallet(row.get("wallet")): row
        for row in source_rows
        if isinstance(row, dict) and _norm_wallet(row.get("wallet"))
    }
    lane_wallets = _wallets_from_lane(lane_state)
    wallets = lane_wallets or list(replay_by_wallet)
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    promotable_wallets = 0
    for wallet in wallets:
        replay = replay_by_wallet.get(wallet, {})
        if not replay:
            row_blockers = ["z3_wallet_missing_from_book_truth_replay"]
        else:
            row_blockers = _row_blockers_from_replay(replay)
        primary_promotable = bool(replay.get("primary_promotable"))
        if primary_promotable:
            promotable_wallets += 1
        rows.append(
            {
                "wallet": wallet,
                "status": "PROMOTABLE" if primary_promotable else ("MISSING" if not replay else "ANALYZE"),
                "book_truth_source": source_replay.get("kind") or "wallet_copy_z3_eligible_profile_book_truth_replay",
                "buy_events": int(_num(replay.get("buy_events"))),
                "book_covered_events": int(_num(replay.get("book_covered_events"))),
                "copyable_buy_events": int(_num(replay.get("copyable_buy_events"))),
                "rejected_buy_events": int(_num(replay.get("rejected_buy_events"))),
                "resolved_copyable_events": int(_num(replay.get("resolved_copyable_events"))),
                "paper_pnl_usd": replay.get("paper_pnl_usd"),
                "paper_pnl_basis": "settlement_backed_book_truth_replay",
                "roi_pct": replay.get("roi_pct"),
                "win_rate_pct": replay.get("win_rate_pct"),
                "strict_source_price_copyable_buy_events": int(_num(replay.get("strict_source_price_copyable_buy_events"))),
                "strict_source_price_pnl_usd": replay.get("strict_source_price_pnl_usd"),
                "eligible_copyable_rate_pct": replay.get("eligible_copyable_rate_pct"),
                "eligible_fill_sample": int(_num(replay.get("eligible_fill_sample"))),
                "eligible_mean_edge": replay.get("eligible_mean_edge"),
                "eligible_median_edge": replay.get("eligible_median_edge"),
                "observation_lag_s": replay.get("observation_lag_s") if isinstance(replay.get("observation_lag_s"), dict) else {},
                "reject_reasons": replay.get("reject_reasons") if isinstance(replay.get("reject_reasons"), dict) else {},
                "eligible_for_forward_paper_lane": bool(replay),
                "eligible_for_selector": primary_promotable,
                "blockers": row_blockers,
            }
        )
        for blocker in row_blockers:
            if blocker not in blockers:
                blockers.append(blocker)
    summary = source_replay.get("summary") if isinstance(source_replay.get("summary"), dict) else {}
    if promotable_wallets == 0 and "no_wallet_cleared_20_copyable_positive_pnl" not in blockers:
        blockers.append("no_wallet_cleared_20_copyable_positive_pnl")
    return {
        "schema_version": 1,
        "kind": "wallet_copy_z3_book_truth_replay",
        "flow_stage": "LEARN/PROMOTE/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "inputs": {
            "alpha_report_updated_at": alpha_report.get("updated_at"),
            "alpha_report_polygon_jsonl": alpha_report.get("polygon_jsonl"),
            "alpha_report_clob_jsonl": alpha_report.get("clob_jsonl"),
            "lane_status": lane_state.get("status"),
            "source_replay_generated_at": source_replay.get("generated_at"),
            "source_replay_inputs": source_replay.get("inputs") if isinstance(source_replay.get("inputs"), dict) else {},
        },
        "verdict": {
            "status": "PASS" if promotable_wallets > 0 else "ANALYZE",
            "z3_wallet_count": len(wallets),
            "book_truth_profile_wallets": len(replay_by_wallet),
            "forward_paper_lane_wallets": sum(1 for row in rows if row.get("eligible_for_forward_paper_lane")),
            "positive_latency_edge_proxy_wallets": sum(1 for row in rows if _num(row.get("eligible_mean_edge")) > 0.0),
            "promotable_wallets": promotable_wallets,
            "copyable_buy_events": int(_num(summary.get("copyable_buy_events"))),
            "book_covered_events": int(_num(summary.get("book_covered_events"))),
            "paper_pnl_usd": summary.get("paper_pnl_usd"),
            "strict_source_price_copyable_buy_events": int(_num(summary.get("strict_source_price_copyable_buy_events"))),
            "strict_source_price_pnl_usd": summary.get("strict_source_price_pnl_usd"),
            "settlement_pnl_available": True,
            "blockers": blockers,
            "next": (
                "send promotable Z3 wallets to promotion selector"
                if promotable_wallets > 0
                else "keep all Z3 wallets attached to forward paper lane until live-time evidence clears >=20 copyable + positive PnL"
            ),
        },
        "rows": rows,
    }


def build_report(
    *,
    alpha_report: dict[str, Any],
    lane_state: dict[str, Any],
    source_replay: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(source_replay, dict) and source_replay.get("wallets"):
        return _build_report_from_book_truth_replay(
            source_replay=source_replay,
            alpha_report=alpha_report,
            lane_state=lane_state,
        )
    wallets = _wallets_from_lane(lane_state)
    execution = alpha_report.get("execution_profiles") if isinstance(alpha_report.get("execution_profiles"), dict) else {}
    profiles = execution.get("profiles_by_wallet") if isinstance(execution.get("profiles_by_wallet"), dict) else {}
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for wallet in wallets:
        profile = profiles.get(wallet) if isinstance(profiles.get(wallet), dict) else {}
        fill_sample = int(_num(profile.get("fill_sample")))
        mean_edge = _num(profile.get("mean_edge"))
        copyable_rate = _num(profile.get("copyable_rate_pct"))
        settlement_pnl = profile.get("paper_pnl_usd")
        settlement_missing = settlement_pnl is None
        row_blockers = list(profile.get("blockers") or [])
        if settlement_missing:
            row_blockers.append("settlement_pnl_missing_for_book_truth_replay")
        if not profile:
            row_blockers.append("z3_wallet_missing_from_alpha_execution_profiles")
        rows.append(
            {
                "wallet": wallet,
                "status": "PASS" if profile.get("eligible") is True else ("MISSING" if not profile else "ANALYZE"),
                "book_truth_source": "alpha_decay_2s_execution_profile",
                "fill_sample": fill_sample,
                "raw_fill_coverage": int(_num(profile.get("raw_fill_coverage"))),
                "copyable_rate_pct": profile.get("copyable_rate_pct"),
                "mean_edge": profile.get("mean_edge"),
                "median_edge": profile.get("median_edge"),
                "stale_or_missing_book_observations": int(_num(profile.get("stale_or_missing_book_observations"))),
                "latency_edge_pnl_proxy_usd": round(mean_edge * fill_sample, 6),
                "paper_pnl_usd": settlement_pnl,
                "paper_pnl_basis": "missing_settlement_resolution" if settlement_missing else "profile",
                "eligible_for_forward_paper_lane": profile.get("eligible") is True,
                "blockers": row_blockers,
            }
        )
        for blocker in row_blockers:
            if blocker not in blockers:
                blockers.append(blocker)
    eligible_rows = [row for row in rows if row.get("eligible_for_forward_paper_lane")]
    positive_proxy_rows = [row for row in rows if _num(row.get("latency_edge_pnl_proxy_usd")) > 0.0]
    return {
        "schema_version": 1,
        "kind": "wallet_copy_z3_book_truth_replay",
        "flow_stage": "LEARN/PROMOTE/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "inputs": {
            "alpha_report_updated_at": alpha_report.get("updated_at"),
            "alpha_report_polygon_jsonl": alpha_report.get("polygon_jsonl"),
            "alpha_report_clob_jsonl": alpha_report.get("clob_jsonl"),
            "lane_status": lane_state.get("status"),
        },
        "verdict": {
            "status": "ANALYZE" if "settlement_pnl_missing_for_book_truth_replay" in blockers else "PASS",
            "z3_wallet_count": len(wallets),
            "book_truth_profile_wallets": sum(1 for row in rows if row.get("status") != "MISSING"),
            "forward_paper_lane_wallets": len(eligible_rows),
            "positive_latency_edge_proxy_wallets": len(positive_proxy_rows),
            "settlement_pnl_available": "settlement_pnl_missing_for_book_truth_replay" not in blockers,
            "blockers": blockers,
            "next": (
                "attach all eligible Z3 wallets to forward paper lane; collect settlement-backed paper PnL before promotion"
                if blockers
                else "eligible for promotion selector input"
            ),
        },
        "rows": rows,
    }


def _below_floor_queue_wallets(queue: dict[str, Any]) -> list[str]:
    wallets: list[str] = []
    for row in queue.get("ranked_members") or []:
        if not isinstance(row, dict):
            continue
        replay = row.get("replay") if isinstance(row.get("replay"), dict) else {}
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet or wallet in wallets:
            continue
        if row.get("ready_for_live") is True:
            continue
        if str(replay.get("status") or "").upper() != "PASS":
            continue
        if int(_num(replay.get("copyable_buy_events"))) >= PROMOTION_MIN_COPYABLE_BUY_EVENTS:
            continue
        wallets.append(wallet)
    return wallets


def build_forward_lane(report: dict[str, Any], *, member_queue: dict[str, Any] | None = None) -> dict[str, Any]:
    wallets = [row["wallet"] for row in report.get("rows", []) if row.get("eligible_for_forward_paper_lane")]
    extra_wallets = _below_floor_queue_wallets(member_queue or {})
    for wallet in extra_wallets:
        if wallet not in wallets:
            wallets.append(wallet)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_z3_eligible_forward_lane_state",
        "flow_stage": "LEARN/FORWARD_PAPER",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": report.get("generated_at") or utc_now_iso(),
        "wallets": wallets,
        "wallet_count": len(wallets),
        "extra_wallets": extra_wallets,
        "extra_wallet_source": "below_floor_full_pool_strict_replay_pass",
        "source_report": report.get("kind"),
        "source_report_status": (report.get("verdict") or {}).get("status"),
        "next": "accrue live-time forward evidence for Z3 and below-floor replay-pass wallets without steering live rotation",
    }


def main() -> int:
    args = parse_args()
    alpha_report = load_json(args.alpha_report, default={})
    lane_state = load_json(args.lane_state, default={})
    source_replay = load_json(args.book_truth_replay, default={})
    member_queue = load_json(args.member_queue, default={})
    if not isinstance(alpha_report, dict) or not alpha_report:
        raise SystemExit(f"alpha report missing or invalid: {args.alpha_report}")
    if not isinstance(lane_state, dict) or not lane_state:
        raise SystemExit(f"lane state missing or invalid: {args.lane_state}")
    report = build_report(
        alpha_report=alpha_report,
        lane_state=lane_state,
        source_replay=source_replay if isinstance(source_replay, dict) else None,
    )
    report["inputs"].update(
        {
            "alpha_report": args.alpha_report,
            "lane_state": args.lane_state,
            "book_truth_replay": args.book_truth_replay if isinstance(source_replay, dict) and source_replay else None,
        }
    )
    atomic_write_json(args.output, report)
    forward = build_forward_lane(report, member_queue=member_queue if isinstance(member_queue, dict) else {})
    forward["source_report_path"] = args.output
    forward["member_queue_path"] = args.member_queue if isinstance(member_queue, dict) and member_queue else None
    atomic_write_json(args.forward_output, forward)
    verdict = report["verdict"]
    print(
        "z3_book_truth_replay",
        f"status={verdict['status']}",
        f"wallets={verdict['z3_wallet_count']}",
        f"forward_wallets={verdict['forward_paper_lane_wallets']}",
        f"forward_lane_wallets={forward['wallet_count']}",
        f"positive_proxy_wallets={verdict['positive_latency_edge_proxy_wallets']}",
        f"output={args.output}",
        f"forward_output={args.forward_output}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
