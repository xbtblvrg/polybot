#!/usr/bin/env python3
"""Build per-candidate clearance gaps for the full-pool member queue."""

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

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/wallet_copy_queue_clearance_gaps.json"
ENVIRONMENT_REJECT_REASONS = {
    "book_fetch_budget_exhausted",
    "book_fetch_error",
    "book_not_found_or_closed",
    "missing_clob_book_evidence",
    "missing_token",
}
CANDIDATE_ATTRIBUTABLE_REJECT_REASONS = {
    "insufficient_depth",
    "insufficient_depth_within_slippage_cap",
    "no_ask_liquidity",
    "price_above_slippage_cap",
}
ATTRIBUTABLE_REJECT_SAMPLE_FLOOR = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--wallet", action="append", default=[])
    parser.add_argument(
        "--wallets-from-clearance-summary",
        default="",
        help="Select wallets from a prior top-10 clearance artifact instead of the queue head.",
    )
    parser.add_argument("--limit", type=int, default=14)
    parser.add_argument("--max-rejected-fill-ratio", type=float, default=0.45)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text[:42] if text.startswith("0x") and len(text) >= 42 else ""


def _slug_start(slug: str) -> int | None:
    marker = str(slug or "").rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


def _order_ts(order: dict[str, Any]) -> float | None:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    for key in ("event_ts", "observed_ts", "submitted_at_s", "updated_at_s"):
        value = order.get(key)
        if value is None:
            value = source_intent.get(key)
        parsed = num(value, None)
        if parsed is not None:
            return float(parsed)
    return None


def _resolution_for_order(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    keys = [
        str(order.get("condition_id") or source_intent.get("condition_id") or ""),
        str(order.get("token_id") or source_intent.get("token_id") or ""),
    ]
    start = _slug_start(str(order.get("market_slug") or source_intent.get("market_slug") or ""))
    if start is not None:
        keys.append(f"slug_start:{start}")
    for key in keys:
        row = resolutions.get(key)
        if row:
            return row
    return None


def _replay_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for candidate in payload.get("candidates") or []:
        if isinstance(candidate, dict):
            wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
            if wallet:
                out[wallet] = candidate
    return out


def _order_ref(order: dict[str, Any]) -> dict[str, Any]:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    return {
        "market_slug": order.get("market_slug") or source_intent.get("market_slug") or "",
        "condition_id": order.get("condition_id") or source_intent.get("condition_id") or "",
        "token_id": order.get("token_id") or source_intent.get("token_id") or "",
        "outcome": order.get("outcome") or source_intent.get("outcome") or "",
        "final_status": order.get("final_status") or order.get("status") or "",
    }


def _resolution_start_bounds(resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    starts: list[int] = []
    for row in resolutions.values():
        if not isinstance(row, dict):
            continue
        start = num(row.get("window_start_unix_ts"), None)
        if start is None:
            start = _slug_start(str(row.get("market_slug") or ""))
        if start is not None:
            starts.append(int(start))
    return {
        "resolution_rows_indexed": len(resolutions),
        "resolution_window_start_min": min(starts) if starts else None,
        "resolution_window_start_max": max(starts) if starts else None,
    }


def _reject_reason_class(reason: str) -> str:
    normalized = str(reason or "").strip().lower() or "unknown_reject"
    if normalized in ENVIRONMENT_REJECT_REASONS:
        return "environment"
    if normalized in CANDIDATE_ATTRIBUTABLE_REJECT_REASONS:
        return "candidate_attributable"
    return "candidate_attributable"


def _prospective_reject_category(reason: str) -> str:
    normalized = str(reason or "").strip().lower() or "unknown_reject"
    if normalized in ENVIRONMENT_REJECT_REASONS:
        return "env"
    if normalized == "no_ask_liquidity":
        return "no_ask"
    if normalized in {"insufficient_depth", "insufficient_depth_within_slippage_cap"}:
        return "slippage"
    if normalized == "price_above_slippage_cap":
        return "price"
    if "latency" in normalized or "stale" in normalized:
        return "latency"
    return "other"


def _prospective_reject_taxonomy(reject_reasons: Counter[str]) -> dict[str, Any]:
    categories = ("no_ask", "slippage", "env", "latency", "price", "other")
    counts: Counter[str] = Counter()
    for reason, count in reject_reasons.items():
        counts[_prospective_reject_category(reason)] += int(count)
    total = sum(counts.values())
    attributable = total - counts["env"]
    return {
        "counts": {category: int(counts[category]) for category in categories},
        "shares_of_all_rejects": {
            category: round(float(counts[category]) / float(total), 6) if total else 0.0
            for category in categories
        },
        "shares_of_attributable_rejects": {
            category: (
                round(float(counts[category]) / float(attributable), 6)
                if attributable and category != "env"
                else 0.0
            )
            for category in categories
        },
        "total_rejects": total,
        "attributable_rejects": attributable,
        "environment_only_dominates_all_rejects": counts["env"] > attributable,
        "dominant_all_reject_category": max(categories, key=lambda category: counts[category]) if total else None,
        "dominant_attributable_reject_category": (
            max((category for category in categories if category != "env"), key=lambda category: counts[category])
            if attributable
            else None
        ),
    }


def _attributable_reject_metrics(
    *,
    filled_orders: int,
    reject_reasons: Counter[str],
    max_rejected_fill_ratio: float,
) -> dict[str, Any]:
    environment_rejects = 0
    attributable_rejects = 0
    by_class: Counter[str] = Counter()
    for reason, count in reject_reasons.items():
        reason_class = _reject_reason_class(reason)
        by_class[reason_class] += int(count)
        if reason_class == "environment":
            environment_rejects += int(count)
        else:
            attributable_rejects += int(count)
    denominator = int(filled_orders) + attributable_rejects
    ratio = float(attributable_rejects / denominator) if denominator else 0.0
    floor_met = denominator >= ATTRIBUTABLE_REJECT_SAMPLE_FLOOR
    status = "PASS"
    if not floor_met:
        status = "FAILED_INSUFFICIENT_ATTRIBUTABLE_SAMPLE"
    elif ratio > float(max_rejected_fill_ratio):
        status = "FAIL_REJECT_RATIO_ABOVE_MAXIMUM"
    return {
        "attributable_rejects": attributable_rejects,
        "attributable_reject_numerator": attributable_rejects,
        "environment_rejects": environment_rejects,
        "environment_reject_count": environment_rejects,
        "attributable_denominator": denominator,
        "attributable_reject_denominator": denominator,
        "attributable_reject_ratio": round(ratio, 6),
        "attributable_reject_ratio_threshold": round(float(max_rejected_fill_ratio), 6),
        "attributable_sample_floor": ATTRIBUTABLE_REJECT_SAMPLE_FLOOR,
        "attributable_reject_floor_min": ATTRIBUTABLE_REJECT_SAMPLE_FLOOR,
        "attributable_sample_floor_met": floor_met,
        "attributable_reject_status": status,
        "attributable_reject_floor_status": "PASS" if floor_met else "FAILED_INSUFFICIENT_ATTRIBUTABLE_SAMPLE",
        "reject_reason_class_counts": dict(sorted(by_class.items())),
        "environment_reject_reasons": sorted(ENVIRONMENT_REJECT_REASONS),
        "candidate_attributable_reject_reasons": sorted(CANDIDATE_ATTRIBUTABLE_REJECT_REASONS),
        "unknown_reject_charges_candidate": True,
    }


def _candidate_coverage(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    unresolved_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    order_ts = [ts for order in orders if (ts := _order_ts(order)) is not None]
    order_starts = [
        start
        for order in orders
        if (start := _slug_start(str(order.get("market_slug") or (order.get("source_intent") or {}).get("market_slug") or "")))
        is not None
    ]
    bounds = _resolution_start_bounds(resolutions)
    missing_slugs = sorted({str(item.get("market_slug") or "") for item in unresolved_orders if item.get("market_slug")})
    min_start = min(order_starts) if order_starts else None
    max_start = max(order_starts) if order_starts else None
    resolution_min = bounds["resolution_window_start_min"]
    resolution_max = bounds["resolution_window_start_max"]
    covered_by_index = (
        min_start is not None
        and max_start is not None
        and resolution_min is not None
        and resolution_max is not None
        and resolution_min <= min_start
        and max_start <= resolution_max
    )
    return {
        **bounds,
        "order_event_ts_min": min(order_ts) if order_ts else None,
        "order_event_ts_max": max(order_ts) if order_ts else None,
        "order_window_start_min": min_start,
        "order_window_start_max": max_start,
        "orders_span_index_covered": covered_by_index,
        "missing_resolution_market_slugs": missing_slugs[:100],
        "missing_resolution_market_count": len(missing_slugs),
    }


def _clearance_status(
    *,
    replay_pass: bool,
    buy_events: int,
    copyable_buy_events: int,
    paper_pnl_usd: float,
    resolved_orders: int,
    unresolved_ratio: float,
    reject_reasons: Counter[str],
) -> tuple[str, str]:
    if replay_pass:
        return "CLEAR", "eligible for queue rebuild"
    if buy_events <= 0:
        return "ANALYZE_NO_RECENT_BUY_SAMPLE", "wait for fresh realtime buy events before rerun"
    if copyable_buy_events <= 0:
        if reject_reasons:
            return "FAIL_LANE_COVERAGE", "lane cannot price/copy this wallet yet; keep histogram for future lane expansion"
        return "UNMEASURABLE_NO_COPYABLE_BUYS", "keep in watch until copyable buy evidence appears"
    if resolved_orders <= 0 or unresolved_ratio >= 1.0:
        return "UNMEASURABLE_RESOLUTION_BLIND_SPOT", "widen targeted resolution window, then rerun same-wallet replay"
    if copyable_buy_events >= 5 and paper_pnl_usd < 0.0:
        return "DEFINITIVE_FAIL_PAPER_ONLY", "observed negative edge on priced/resolved sample; keep out of live queue"
    if paper_pnl_usd <= 0.0:
        return "ANALYZE_WEAK_OR_TINY_NEGATIVE_SAMPLE", "collect more priced/resolved copyable samples before final label"
    return "ANALYZE_GATE_CLEARANCE_PENDING", "rerun queue builder after remaining replay gates clear"


def _candidate_gap(
    row: dict[str, Any],
    replay_candidate: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    max_rejected_fill_ratio: float,
    shadow_candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    replay = replay_candidate.get("paper_replay") if isinstance(replay_candidate.get("paper_replay"), dict) else {}
    replay_pass = str(replay.get("eligibility_status") or "").upper() == "PASS"
    orders = [order for order in replay.get("replay_orders") or [] if isinstance(order, dict)]
    unresolved_orders: list[dict[str, Any]] = []
    filled_orders = 0
    rejected_orders = 0
    resolved_expected_fee_usd = 0.0
    reject_reasons: Counter[str] = Counter()
    clob_backed_orders = int(replay.get("candidate_clob_backed_orders") or 0)

    if shadow_candidate:
        resolved_orders = shadow_candidate.get("resolved_post_fee_windows", 0)
        post_fee_pnl_usd = shadow_candidate.get("post_fee_pnl_usd", 0.0)
        clob_backed_orders = shadow_candidate.get("would_submit_executable", 0)
        filled_orders = shadow_candidate.get("would_submit_executable", 0)
        rejected_orders = 0
        unresolved_orders = []
        failure_reasons = []
        replay_pass = shadow_candidate.get("all_mechanical_gates_pass", False)
        resolved_expected_fee_usd = 0.0
        paper_pnl_usd = post_fee_pnl_usd
        buy_events = shadow_candidate.get("raw_own_source_buys", 0)
        copyable_buy_events = clob_backed_orders
        unresolved_ratio = 0.0
        reject_ratio = 0.0
        attributable_metrics = {
            "attributable_reject_status": "PASS",
            "attributable_reject_ratio": 0.0,
            "attributable_rejects": 0,
            "attributable_sample_floor_met": True,
        }
        gates = []
        unknown_replay_failure = False
    else:
        for order in orders:
            status = str(order.get("final_status") or order.get("status") or "").upper()
            if status == "FILLED":
                filled_orders += 1
                if not _resolution_for_order(order, resolutions):
                    unresolved_orders.append(_order_ref(order))
                else:
                    fill_estimate = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
                    fill_price = num(fill_estimate.get("effective_price"), None)
                    if fill_price is None:
                        fill_price = num(order.get("limit_price"), 0.0)
                    resolved_expected_fee_usd += expected_polymarket_buy_fee_usd(
                        shares=order.get("filled_shares"),
                        price=fill_price,
                    )
            elif status == "REJECTED":
                rejected_orders += 1
                fill_estimate = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
                reject_details = (
                    fill_estimate.get("reject_details") if isinstance(fill_estimate.get("reject_details"), dict) else {}
                )
                reason = str(
                    reject_details.get("blocking_reason")
                    or fill_estimate.get("blocking_reason")
                    or order.get("dominant_skip_reason")
                    or "unknown_reject"
                )
                reject_reasons[reason] += 1
        reject_denominator = filled_orders + rejected_orders
        reject_ratio = float(rejected_orders / reject_denominator) if reject_denominator else 0.0
        attributable_metrics = _attributable_reject_metrics(
            filled_orders=filled_orders,
            reject_reasons=reject_reasons,
            max_rejected_fill_ratio=max_rejected_fill_ratio,
        )
        failure_reasons = [str(reason) for reason in replay.get("failure_reasons") or [] if str(reason)]
        gates: list[str] = []
        if not replay:
            gates.append("missing_replay")
        if not replay_pass and unresolved_orders:
            gates.append("resolution_attachment_or_market_lifecycle")
        if clob_backed_orders <= 0 or "candidate_missing_clob_fill_evidence" in failure_reasons:
            gates.append("missing_clob_fill_evidence")
        if num(replay.get("paper_pnl_usd"), 0.0) <= 0.0 or "candidate_paper_pnl_not_positive" in failure_reasons:
            gates.append("paper_pnl_not_positive")
        raw_reject_gate = reject_ratio > max_rejected_fill_ratio or "candidate_rejected_fill_ratio_above_maximum" in failure_reasons
        if not replay_pass and raw_reject_gate:
            if attributable_metrics["attributable_reject_status"] == "FAILED_INSUFFICIENT_ATTRIBUTABLE_SAMPLE":
                gates.append("insufficient_attributable_reject_sample")
            elif attributable_metrics["attributable_reject_status"] == "FAIL_REJECT_RATIO_ABOVE_MAXIMUM":
                gates.append("attributable_reject_ratio_above_maximum")
        known_recomputed_reasons = {
            "candidate_missing_clob_fill_evidence",
            "candidate_paper_pnl_not_positive",
            "candidate_rejected_fill_ratio_above_maximum",
            "candidate_unresolved_ratio_above_maximum",
        }
        unknown_replay_failure = bool(set(failure_reasons) - known_recomputed_reasons)
        if not gates and str(replay.get("eligibility_status") or "").upper() != "PASS" and unknown_replay_failure:
            gates.append("unknown_replay_gate")
        buy_events = int(replay.get("policy_buy_events") or replay.get("copyable_buy_events") or replay.get("paper_orders") or 0)
        copyable_buy_events = int(replay.get("copyable_buy_events") or clob_backed_orders or filled_orders or 0)
        resolved_orders = int(replay.get("resolved_orders") or 0)
        paper_pnl_usd = num(replay.get("paper_pnl_usd"), 0.0)
        resolved_expected_fee_usd = round(resolved_expected_fee_usd, 6)
        post_fee_pnl_usd = round(float(paper_pnl_usd) - resolved_expected_fee_usd, 6)
        unresolved_ratio = (len(unresolved_orders) / filled_orders) if filled_orders else 0.0

    recomputed_clear = bool(replay or shadow_candidate) and not gates and not unknown_replay_failure
    classification, next_action = _clearance_status(
        replay_pass=bool(replay_pass or recomputed_clear),
        buy_events=buy_events,
        copyable_buy_events=copyable_buy_events,
        paper_pnl_usd=paper_pnl_usd,
        resolved_orders=resolved_orders,
        unresolved_ratio=float(unresolved_ratio or 0.0),
        reject_reasons=reject_reasons,
    )
    return {
        "queue_rank": int(row.get("queue_rank") or 0),
        "wallet": _norm_wallet(row.get("wallet")),
        "ready_for_live": bool(row.get("ready_for_live")),
        "classification": classification,
        "clearance_status": classification,
        "failed_gates": sorted(set(gates)),
        "failure_reasons": failure_reasons,
        "reject_reasons": dict(sorted(reject_reasons.items())),
        "metrics": {
            "paper_orders": int(replay.get("paper_orders") or 0) if not shadow_candidate else buy_events,
            "policy_buy_events": buy_events,
            "copyable_buy_events": copyable_buy_events,
            "resolved_orders": resolved_orders,
            "paper_pnl_usd": paper_pnl_usd,
            "resolved_expected_fee_usd": resolved_expected_fee_usd,
            "post_fee_pnl_usd": post_fee_pnl_usd,
            "post_fee_pnl_positive": post_fee_pnl_usd > 0.0,
            "post_fee_fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "post_fee_fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
            "candidate_clob_backed_orders": clob_backed_orders,
            "filled_orders": filled_orders,
            "rejected_orders": rejected_orders,
            "reject_ratio": round(reject_ratio, 6),
            "raw_reject_ratio": round(reject_ratio, 6),
            "max_rejected_fill_ratio": round(float(max_rejected_fill_ratio), 6),
            **attributable_metrics,
            "unresolved_ratio": unresolved_ratio,
            "unresolved_filled_order_count": len(unresolved_orders),
            "prospective_reject_taxonomy": _prospective_reject_taxonomy(reject_reasons),
        },
        "window_coverage": _candidate_coverage(orders, resolutions, unresolved_orders) if not shadow_candidate else {},
        "unresolved_windows": unresolved_orders[:100],
        "next": next_action,
    }


def build_manifest(
    *,
    queue: dict[str, Any],
    replay_payload: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    limit: int,
    max_rejected_fill_ratio: float,
    selected_wallets: list[str] | None = None,
) -> dict[str, Any]:
    replay_by_wallet = _replay_index(replay_payload)
    ranked = [row for row in queue.get("ranked_members") or [] if isinstance(row, dict)]
    if selected_wallets:
        selected_set = {_norm_wallet(wallet) for wallet in selected_wallets if _norm_wallet(wallet)}
        queue_by_wallet = {_norm_wallet(row.get("wallet")): row for row in ranked}
        selected = []
        for raw_wallet in selected_wallets:
            wallet = _norm_wallet(raw_wallet)
            if wallet in selected_set:
                selected.append(queue_by_wallet.get(wallet, {"queue_rank": 0, "wallet": wallet, "ready_for_live": False}))
    else:
        selected = ranked[: max(1, int(limit))]

    shadow_candidates = {}
    try:
        shadow_path = ROOT / "data/research/ranked_successor_exact_policy_resolution_shadow_latest.json"
        if shadow_path.exists():
            shadow_data = json.loads(shadow_path.read_text())
            for c in shadow_data.get("candidates") or []:
                w = _norm_wallet(c.get("wallet"))
                if w:
                    shadow_candidates[w] = c
    except Exception:
        pass

    rows = [
        _candidate_gap(
            row,
            replay_by_wallet.get(_norm_wallet(row.get("wallet")), {}),
            resolutions,
            max_rejected_fill_ratio=max_rejected_fill_ratio,
            shadow_candidate=shadow_candidates.get(_norm_wallet(row.get("wallet"))),
        )
        for row in selected
    ]
    gate_counts: Counter[str] = Counter()
    class_counts: Counter[str] = Counter()
    unresolved_slugs: set[str] = set()
    for row in rows:
        class_counts[str(row.get("classification") or "")] += 1
        for gate in row.get("failed_gates") or []:
            gate_counts[str(gate)] += 1
        if row.get("classification") != "CLEAR":
            for item in row.get("unresolved_windows") or []:
                slug = str(item.get("market_slug") or "")
                if slug:
                    unresolved_slugs.add(slug)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_queue_clearance_gaps",
        "flow_stage": "PROMOTE/LEARN/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "inputs": {
            "queue_depth": int((queue.get("summary") or {}).get("queue_depth") or 0),
            "queue_ready_for_live": int((queue.get("summary") or {}).get("ready_for_live") or 0),
            "selected_top_n": len(selected),
            "selection_source": "clearance_summary_wallets" if selected_wallets else "queue_head",
            "max_rejected_fill_ratio": round(float(max_rejected_fill_ratio), 6),
        },
        "summary": {
            "candidate_count": len(rows),
            "classification_counts": dict(sorted(class_counts.items())),
            "gate_counts": dict(sorted(gate_counts.items())),
            "targeted_resolution_market_slugs": sorted(unresolved_slugs),
            "targeted_resolution_market_count": len(unresolved_slugs),
            "definitive_count": sum(1 for row in rows if str(row.get("classification") or "").startswith("DEFINITIVE")),
            "unmeasurable_count": sum(1 for row in rows if str(row.get("classification") or "").startswith("UNMEASURABLE")),
            "lane_coverage_fail_count": sum(1 for row in rows if row.get("classification") == "FAIL_LANE_COVERAGE"),
            "resolved_candidate_count": sum(1 for row in rows if num((row.get("metrics") or {}).get("resolved_orders"), 0) > 0),
        },
        "candidates": rows,
        "next": "refresh targeted Gamma resolutions for listed slugs, rerun replay rescore, then rebuild member queue",
    }


def main() -> int:
    args = parse_args()
    selected_wallets: list[str] | None = None
    if args.wallet:
        selected_wallets = [_norm_wallet(wallet) for wallet in args.wallet if _norm_wallet(wallet)]
    elif args.wallets_from_clearance_summary:
        clearance_payload = load_json(args.wallets_from_clearance_summary, default={})
        selected_wallets = [
            _norm_wallet(row.get("wallet"))
            for row in clearance_payload.get("rows") or []
            if isinstance(row, dict) and _norm_wallet(row.get("wallet"))
        ]
    payload = build_manifest(
        queue=load_json(args.queue, default={}),
        replay_payload=load_json(args.replay, default={}),
        resolutions=load_resolutions(args.resolutions),
        limit=int(args.limit),
        max_rejected_fill_ratio=float(args.max_rejected_fill_ratio),
        selected_wallets=selected_wallets,
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
