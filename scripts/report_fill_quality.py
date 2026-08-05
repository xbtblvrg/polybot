#!/usr/bin/env python3
"""Report live wallet-copy fill quality and chase-vs-skip calibration."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.pnl_truth import (  # noqa: E402
    build_pnl_truth,
    order_cost as _canonical_order_cost,
    order_shares as _canonical_order_shares,
    order_source_wallet as _canonical_order_source_wallet,
    order_ts as _canonical_order_ts,
    price_bucket as _canonical_price_bucket,
    resolution_for_order as _canonical_resolution_for_order,
    resolved_pnl as _canonical_resolved_pnl,
    winner_from_resolution as _canonical_winner_from_resolution,
)
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.store import atomic_write_json, json_file_lock, load_json  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--cash-diff", default="data/research/wallet_copy_today_fill_cash_diff_latest.json")
    parser.add_argument("--output", default="data/research/wallet_copy_live_fill_quality_report.json")
    parser.add_argument(
        "--post-fix-since",
        default="",
        help="Optional ISO/epoch cutoff for post-fix precision-reject verification.",
    )
    parser.add_argument(
        "--price-band-decision-since",
        default="",
        help="Optional ISO/epoch cutoff for the live <=max-price PnL decision window.",
    )
    parser.add_argument("--price-band-decision-max-price", type=float, default=0.50)
    parser.add_argument("--price-band-decision-min-resolved", type=int, default=10)
    parser.add_argument("--price-band-decision-min-fill-rate-pct", type=float, default=40.0)
    parser.add_argument(
        "--price-band-decision-wallet",
        default="",
        help="Optional source wallet scope for the live price-band decision window.",
    )
    parser.add_argument(
        "--write-ledger-resolutions",
        action="store_true",
        help="Persist resolved filled-order PnL fields back into the live ledger.",
    )
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return ""


def _order_source_wallet(order: dict[str, Any]) -> str:
    return _canonical_order_source_wallet(order)


def _price_bucket(price: float) -> str:
    return _canonical_price_bucket(price)


def _order_cost(order: dict[str, Any]) -> float:
    return _canonical_order_cost(order)


def _order_shares(order: dict[str, Any]) -> float:
    return _canonical_order_shares(order)


def _execution_role(order: dict[str, Any]) -> str:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    role = str(order.get("execution_role") or result.get("execution_role") or "").lower()
    if role in {"maker", "taker"}:
        return role
    maker_value = order.get("maker", result.get("maker"))
    if maker_value is True:
        return "maker"
    if maker_value is False:
        return "taker"
    return "unknown"


def _resolution_for_order(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    return _canonical_resolution_for_order(order, resolutions)


def _winner_from_resolution(row: dict[str, Any] | None) -> str:
    return _canonical_winner_from_resolution(row)


def _resolved_pnl(*, side: str, shares: float, cost: float, resolution: dict[str, Any] | None) -> tuple[bool, float]:
    return _canonical_resolved_pnl(side=side, shares=shares, cost=cost, resolution=resolution)


def _ledger_resolution_payload(order: dict[str, Any], resolution: dict[str, Any], pnl: float) -> dict[str, Any]:
    payload = {
        "direction": str(resolution.get("direction") or ""),
        "winning_side": _winner_from_resolution(resolution),
        "pnl_usd": round(float(pnl), 6),
        "source": str(resolution.get("source") or resolution.get("resolution_precision") or ""),
        "research_only": bool(resolution.get("research_only")),
    }
    for key in ("condition_id", "expiry_unix_ts", "window_type", "market_slug", "yes_token", "no_token"):
        if resolution.get(key) not in (None, ""):
            payload[key] = resolution.get(key)
    return payload


def annotate_ledger_resolutions(
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    resolved_at: str | None = None,
) -> dict[str, Any]:
    """Write resolved filled-order PnL fields into an in-memory live ledger."""

    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    now = resolved_at or utc_now_iso()
    inspected = 0
    resolved_orders = 0
    updated_orders = 0
    pnl_total = 0.0
    per_slug: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"orders": 0, "pnl_usd": 0.0, "yes_pnl_usd": 0.0, "no_pnl_usd": 0.0}
    )
    for order in orders:
        if not isinstance(order, dict):
            continue
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if status != "FILLED":
            continue
        inspected += 1
        resolution = _resolution_for_order(order, resolutions)
        resolved, pnl = _resolved_pnl(
            side=str(order.get("side") or ""),
            shares=_order_shares(order),
            cost=_order_cost(order),
            resolution=resolution,
        )
        if not resolved or resolution is None:
            continue
        resolved_orders += 1
        pnl_total = round(pnl_total + pnl, 6)
        slug = str(order.get("market_slug") or _as_source_intent(order).get("market_slug") or "unknown")
        side = str(order.get("side") or "").upper()
        slug_row = per_slug[slug]
        slug_row["orders"] += 1
        slug_row["pnl_usd"] = round(float(slug_row["pnl_usd"]) + pnl, 6)
        if side == "YES":
            slug_row["yes_pnl_usd"] = round(float(slug_row["yes_pnl_usd"]) + pnl, 6)
        elif side == "NO":
            slug_row["no_pnl_usd"] = round(float(slug_row["no_pnl_usd"]) + pnl, 6)

        payload = _ledger_resolution_payload(order, resolution, pnl)
        before = (order.get("resolved"), order.get("pnl_usd"), order.get("resolution"))
        order["resolved"] = True
        order["pnl_usd"] = round(float(pnl), 6)
        order["resolution"] = payload
        order["resolution_updated_at"] = now
        after = (order.get("resolved"), order.get("pnl_usd"), order.get("resolution"))
        if before != after:
            updated_orders += 1

    summary = {
        "updated_at": now,
        "inspected_filled_orders": inspected,
        "resolved_filled_orders": resolved_orders,
        "updated_orders": updated_orders,
        "pnl_usd": round(pnl_total, 6),
        "per_market_slug": dict(sorted((slug, dict(row)) for slug, row in per_slug.items())),
    }
    ledger["resolution_writeback"] = summary
    return summary


def _as_source_intent(order: dict[str, Any]) -> dict[str, Any]:
    return order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}


def _order_ts(order: dict[str, Any]) -> float | None:
    return _canonical_order_ts(order)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * pct))))
    return round(float(ordered[idx]), 6)


def _summary_stats(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "p50": _percentile(values, 0.5),
        "p90": _percentile(values, 0.9),
        "max": round(max(values), 6) if values else 0.0,
    }


def _reconciled_pnl_by_order_id(cash_diff_report: dict[str, Any]) -> dict[str, float]:
    rows = cash_diff_report.get("rows") if isinstance(cash_diff_report.get("rows"), list) else []
    out: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        order_ids = [str(item) for item in (row.get("order_ids") or []) if str(item or "")]
        if len(order_ids) != 1:
            continue
        if "actual_cost_usd" not in row or "actual_payout_usd" not in row:
            continue
        out[order_ids[0]] = round(num(row.get("actual_payout_usd")) - num(row.get("actual_cost_usd")), 6)
    return out


def classify_reject_reason(order: dict[str, Any]) -> dict[str, Any]:
    """Classify why a live wallet-copy order rejected using persisted evidence only."""

    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    snapshot = (
        result.get("wallet_copy_post_submit_book")
        if isinstance(result.get("wallet_copy_post_submit_book"), dict)
        else {}
    )
    limit_price = num(order.get("limit_price"))
    requested_usd = num(order.get("requested_size_usd"))
    best_ask = num(snapshot.get("best_ask")) if snapshot.get("status") == "OK" else 0.0
    limit_depth_usd = (
        num(snapshot.get("ask_depth_at_or_below_limit_usd")) if snapshot.get("status") == "OK" else 0.0
    )
    error_class = str(result.get("error_class") or "")
    error_text = str(result.get("error") or "")

    category = "unknown_reject"
    evidence = "no_structured_reject_evidence"
    if snapshot.get("status") == "OK" and best_ask > 0 and limit_price > 0 and best_ask > limit_price + 1e-9:
        category = "best_ask_above_copied_limit"
        evidence = "post_submit_book_best_ask_exceeded_copied_limit"
    elif snapshot.get("status") == "OK" and requested_usd > 0 and limit_depth_usd + 1e-9 < requested_usd:
        category = "insufficient_depth_at_copied_limit"
        evidence = "post_submit_book_limit_depth_below_requested_usd"
    elif error_class == "market_buy_precision_below_min_order":
        category = "market_buy_precision_below_min_order"
        evidence = "precision_safe_market_buy_amount_below_clob_min"
    elif error_class == "fak_no_match":
        category = "fak_no_match_without_book_evidence"
        evidence = "clob_returned_no_orders_found_to_match_fak"
    elif error_class == "fok_not_filled":
        category = "fok_not_filled"
        evidence = "clob_returned_fok_not_filled"
    elif error_class == "clob_request_exception":
        category = "clob_request_exception"
        evidence = "clob_request_exception"
    elif "invalid amount for a marketable BUY order" in error_text:
        category = "legacy_market_buy_amount_below_min_order"
        evidence = "pre_precision_guard_clob_min_order_reject"
    elif "invalid order version" in error_text:
        category = "legacy_clob_client_order_version"
        evidence = "pre_reload_clob_client_version_reject"
    elif error_class:
        category = f"executor_error:{error_class}"
        evidence = "executor_error_class"

    return {
        "category": category,
        "evidence": evidence,
        "error_class": error_class,
        "error": error_text[:240],
        "snapshot_status": str(snapshot.get("status") or ""),
        "best_ask": round(best_ask, 6),
        "copied_limit": round(limit_price, 6),
        "executable_delta": round(best_ask - limit_price, 6) if best_ask > 0 and limit_price > 0 else 0.0,
        "ask_depth_at_or_below_limit_usd": round(limit_depth_usd, 6),
        "requested_size_usd": round(requested_usd, 6),
    }


def _reject_sample(order: dict[str, Any], classification: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": str(order.get("order_id") or ""),
        "market_slug": str(order.get("market_slug") or ""),
        "side": str(order.get("side") or ""),
        "bucket": _price_bucket(num(order.get("limit_price"))),
        "limit_price": round(num(order.get("limit_price")), 6),
        "requested_size_usd": round(num(order.get("requested_size_usd")), 6),
        "category": classification["category"],
        "evidence": classification["evidence"],
        "error_class": classification["error_class"],
        "best_ask": classification["best_ask"],
        "executable_delta": classification["executable_delta"],
        "ask_depth_at_or_below_limit_usd": classification["ask_depth_at_or_below_limit_usd"],
        "error": classification["error"],
    }


def _reject_group(category: str) -> str:
    if category in {"best_ask_above_copied_limit", "insufficient_depth_at_copied_limit"}:
        return "genuine_slippage_or_liquidity"
    if category in {"market_buy_precision_below_min_order", "legacy_market_buy_amount_below_min_order"}:
        return "sizing_or_precision"
    if category.startswith("legacy_"):
        return "legacy_infrastructure"
    if category in {"fak_no_match_without_book_evidence", "fok_not_filled"}:
        return "unclassified_clob_no_fill"
    if category.startswith("clob_") or category.startswith("executor_error:"):
        return "execution_error"
    return "unknown"


def _post_fix_precision_verification(orders: list[dict[str, Any]], since: str) -> dict[str, Any]:
    cutoff = parse_ts(since)
    if cutoff is None:
        return {
            "enabled": False,
            "status": "SKIPPED",
            "since": since,
            "orders": 0,
            "precision_rejects": 0,
        }
    post_orders = [order for order in orders if (_order_ts(order) or 0.0) >= cutoff]
    taxonomy = Counter()
    samples: list[dict[str, Any]] = []
    for order in post_orders:
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if status != "REJECTED":
            continue
        classification = classify_reject_reason(order)
        category = str(classification.get("category") or "")
        taxonomy[category] += 1
        if category in {"market_buy_precision_below_min_order", "legacy_market_buy_amount_below_min_order"}:
            samples.append(_reject_sample(order, classification))
            samples = samples[-10:]
    precision_rejects = sum(
        int(taxonomy.get(category) or 0)
        for category in ("market_buy_precision_below_min_order", "legacy_market_buy_amount_below_min_order")
    )
    low_price_orders = [
        order
        for order in post_orders
        if num(order.get("limit_price")) <= 0.70 and str(order.get("final_status") or order.get("status") or "").upper()
    ]
    low_price_filled = sum(
        1
        for order in low_price_orders
        if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
    )
    low_price_chase_enabled = 0
    for order in low_price_orders:
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        chase = result.get("wallet_copy_chase") if isinstance(result.get("wallet_copy_chase"), dict) else {}
        if chase.get("chase_enabled") is True:
            low_price_chase_enabled += 1
    status = "WATCH" if not post_orders else "PASS" if precision_rejects == 0 else "CORRECTION"
    return {
        "enabled": True,
        "status": status,
        "since": since,
        "since_ts": cutoff,
        "orders": len(post_orders),
        "rejects": sum(taxonomy.values()),
        "taxonomy_counts": dict(sorted(taxonomy.items())),
        "precision_rejects": precision_rejects,
        "sampled_precision_rejects": samples,
        "latest_order_ts": max((order.get("submitted_at") or order.get("updated_at") or "" for order in post_orders), default=""),
        "low_price_le_70": {
            "orders": len(low_price_orders),
            "filled": low_price_filled,
            "fill_rate_pct": round(100.0 * low_price_filled / len(low_price_orders), 6) if low_price_orders else 0.0,
            "chase_enabled_rows": low_price_chase_enabled,
            "target_fill_rate_pct": 60.0,
        },
    }


def _price_band_decision_window(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    reconciled_pnl_by_order_id: dict[str, float] | None = None,
    since: str,
    max_price: float,
    min_resolved_filled: int,
    min_fill_rate_pct: float,
    target_wallet: str = "",
) -> dict[str, Any]:
    cutoff = parse_ts(since)
    if cutoff is None:
        return {
            "enabled": False,
            "status": "SKIPPED",
            "since": since,
            "max_price": round(float(max_price), 6),
            "orders": 0,
            "filled": 0,
            "resolved_filled": 0,
            "realized_pnl_usd": 0.0,
            "rotation_triggered": False,
            "next_action": "pass --price-band-decision-since to evaluate the live price-band rotation rule",
        }

    max_price = max(0.0, float(max_price))
    min_resolved_filled = max(0, int(min_resolved_filled))
    min_fill_rate_pct = max(0.0, float(min_fill_rate_pct))
    wallet_scope = _norm_wallet(target_wallet)
    window_orders = [
        order
        for order in orders
        if (_order_ts(order) or 0.0) >= cutoff and num(order.get("limit_price")) <= max_price + 1e-9
    ]
    unscoped_orders = len(window_orders)
    if wallet_scope:
        window_orders = [order for order in window_orders if _order_source_wallet(order) == wallet_scope]
    filled = 0
    rejected = 0
    resolved_filled = 0
    realized_pnl = 0.0
    resolved_rows: list[dict[str, Any]] = []
    chase_enabled_rows = 0
    taxonomy: Counter[str] = Counter()
    error_classes: Counter[str] = Counter()
    latest_order_ts = ""
    reconciled_map = reconciled_pnl_by_order_id or {}
    for order in window_orders:
        latest_order_ts = max(latest_order_ts, str(order.get("submitted_at") or order.get("updated_at") or ""))
        status = str(order.get("final_status") or order.get("status") or "").upper()
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        chase = result.get("wallet_copy_chase") if isinstance(result.get("wallet_copy_chase"), dict) else {}
        if chase.get("chase_enabled") is True:
            chase_enabled_rows += 1
        if status == "FILLED":
            filled += 1
            resolved, pnl = _resolved_pnl(
                side=str(order.get("side") or ""),
                shares=_order_shares(order),
                cost=_order_cost(order),
                resolution=_resolution_for_order(order, resolutions),
            )
            if resolved:
                order_id = str(order.get("order_id") or "")
                reconciled_pnl = reconciled_map.get(order_id)
                resolved_filled += 1
                realized_pnl = round(realized_pnl + pnl, 6)
                resolved_rows.append(
                    {
                        "order_id": order_id,
                        "submitted_at": str(order.get("submitted_at") or order.get("updated_at") or ""),
                        "ts": _order_ts(order) or 0.0,
                        "pnl_usd": round(float(pnl), 6),
                        "reconciled_pnl_usd": (
                            round(float(reconciled_pnl), 6) if reconciled_pnl is not None else None
                        ),
                        "limit_price": round(num(order.get("limit_price")), 6),
                        "side": str(order.get("side") or ""),
                        "market_slug": str(order.get("market_slug") or _as_source_intent(order).get("market_slug") or ""),
                    }
                )
        elif status == "REJECTED":
            rejected += 1
            classification = classify_reject_reason(order)
            taxonomy[str(classification.get("category") or "unknown_reject")] += 1
            error_classes[str(classification.get("error_class") or "none")] += 1

    fill_rate = round(100.0 * filled / len(window_orders), 6) if window_orders else 0.0
    sample_ready = resolved_filled >= min_resolved_filled
    rolling_window_size = 20
    rolling_loss_threshold_usd = -8.0
    rolling_rows = sorted(resolved_rows, key=lambda row: (float(row.get("ts") or 0.0), str(row.get("order_id") or "")), reverse=True)[
        :rolling_window_size
    ]
    rolling_resolved_filled = len(rolling_rows)
    rolling_realized_pnl = round(sum(num(row.get("pnl_usd")) for row in rolling_rows), 6)
    rolling_sample_ready = rolling_resolved_filled >= rolling_window_size
    reconciled_rows = [row for row in rolling_rows if row.get("reconciled_pnl_usd") is not None]
    reconciled_rolling_ready = rolling_sample_ready and len(reconciled_rows) == rolling_window_size
    reconciled_rolling_pnl = round(sum(num(row.get("reconciled_pnl_usd")) for row in reconciled_rows), 6)
    rolling_rotation_triggered = rolling_sample_ready and rolling_realized_pnl <= rolling_loss_threshold_usd
    pnl_negative = sample_ready and realized_pnl < 0.0
    fill_rate_ready = fill_rate >= min_fill_rate_pct
    blockers: list[str] = []
    if not sample_ready:
        blockers.append("price_band_resolved_sample_below_threshold")
    if pnl_negative:
        blockers.append("price_band_realized_pnl_negative")
    if rolling_rotation_triggered:
        blockers.append("rolling_20_resolved_pnl_below_loss_threshold")
    if window_orders and not fill_rate_ready:
        blockers.append("price_band_fill_rate_below_target")
    if pnl_negative or rolling_rotation_triggered:
        status = "CORRECTION"
        next_action = "mechanically rotate live wallet from the broad paper candidate lane; do not keep tuning this price band"
    elif sample_ready and fill_rate_ready:
        status = "PASS"
        next_action = "keep <=max-price live band active and continue rolling measurement"
    else:
        status = "WATCH"
        next_action = "continue collecting <=max-price live band fills until the decision thresholds are met"
    return {
        "enabled": True,
        "status": status,
        "since": since,
        "since_ts": cutoff,
        "max_price": round(max_price, 6),
        "target_wallet": wallet_scope,
        "wallet_scoped": bool(wallet_scope),
        "unscoped_orders": unscoped_orders,
        "excluded_by_wallet_scope": max(0, unscoped_orders - len(window_orders)),
        "orders": len(window_orders),
        "filled": filled,
        "rejected": rejected,
        "fill_rate_pct": fill_rate,
        "target_fill_rate_pct": round(min_fill_rate_pct, 6),
        "resolved_filled": resolved_filled,
        "min_resolved_filled": min_resolved_filled,
        "realized_pnl_usd": round(realized_pnl, 6),
        "min_realized_pnl_usd": 0.0,
        "resolution_sample_ready": sample_ready,
        "rotation_triggered": pnl_negative or rolling_rotation_triggered,
        "rolling_rotation_trigger": {
            "enabled": True,
            "flow_stage": "ROTATE",
            "window_size": rolling_window_size,
            "resolved_filled": rolling_resolved_filled,
            "sample_ready": rolling_sample_ready,
            "realized_pnl_usd": rolling_realized_pnl,
            "pnl_basis": "canonical_response_or_receipt_cost",
            "loss_threshold_usd": rolling_loss_threshold_usd,
            "rotation_triggered": rolling_rotation_triggered,
            "sample_orders": [
                {key: value for key, value in row.items() if key not in {"ts", "reconciled_pnl_usd"}}
                for row in rolling_rows[:5]
            ],
            "bases": {
                "canonical": {
                    "enabled": True,
                    "flow_stage": "ROTATE",
                    "window_size": rolling_window_size,
                    "resolved_filled": rolling_resolved_filled,
                    "sample_ready": rolling_sample_ready,
                    "realized_pnl_usd": rolling_realized_pnl,
                    "pnl_basis": "canonical_response_or_receipt_cost",
                    "sample_orders": [
                        {key: value for key, value in row.items() if key not in {"ts", "reconciled_pnl_usd"}}
                        for row in rolling_rows[:5]
                    ],
                },
                "reconciled": {
                    "enabled": bool(reconciled_map),
                    "flow_stage": "ROTATE",
                    "window_size": rolling_window_size,
                    "resolved_filled": len(reconciled_rows),
                    "sample_ready": reconciled_rolling_ready,
                    "realized_pnl_usd": reconciled_rolling_pnl,
                    "pnl_basis": "order_level_actual_cash_diff",
                    "missing_order_level_reconciled_rows": max(0, rolling_resolved_filled - len(reconciled_rows)),
                    "sample_orders": [
                        {
                            key: (row.get("reconciled_pnl_usd") if key == "pnl_usd" else value)
                            for key, value in row.items()
                            if key != "ts"
                        }
                        for row in reconciled_rows[:5]
                    ],
                },
            },
        },
        "chase_enabled_rows": chase_enabled_rows,
        "taxonomy_counts": dict(sorted(taxonomy.items())),
        "error_class_counts": dict(sorted(error_classes.items())),
        "blockers": blockers,
        "latest_order_ts": latest_order_ts,
        "next_action": next_action,
    }


def build_fill_quality_report(
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    cash_diff_report: dict[str, Any] | None = None,
    post_fix_since: str = "",
    price_band_decision_since: str = "",
    price_band_decision_max_price: float = 0.50,
    price_band_decision_min_resolved: int = 10,
    price_band_decision_min_fill_rate_pct: float = 40.0,
    price_band_decision_wallet: str = "",
) -> dict[str, Any]:
    orders = [row for row in ledger.get("orders") or [] if isinstance(row, dict)]
    reconciled_pnl = _reconciled_pnl_by_order_id(cash_diff_report or {})
    canonical_truth = build_pnl_truth({"orders": orders}, resolutions)
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "orders": 0,
            "filled": 0,
            "rejected": 0,
            "resolved_filled": 0,
            "realized_pnl_usd": 0.0,
            "snapshot_rows": 0,
            "snapshot_status_counts": {},
            "snapshot_fetch_ms": [],
            "chase_rows": 0,
            "depth_gated_chase_rows": 0,
            "resolved_chase_rows": 0,
            "would_have_chase_pnl_usd": 0.0,
            "skip_vs_chase_pnl_usd": 0.0,
            "reject_taxonomy_counts": {},
            "reject_group_counts": {},
            "reject_error_class_counts": {},
        }
    )
    measured_snapshots = 0
    snapshot_status_counts: Counter[str] = Counter()
    snapshot_fetch_ms: list[float] = []
    reject_taxonomy_counts: Counter[str] = Counter()
    reject_group_counts: Counter[str] = Counter()
    reject_error_class_counts: Counter[str] = Counter()
    high_price_reject_samples: list[dict[str, Any]] = []
    latency_hops: dict[str, list[float]] = defaultdict(list)
    latency_instrumented_orders = 0
    latency_missing_orders = 0
    role_summary: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "orders": 0,
            "filled": 0,
            "rejected": 0,
            "submitted": 0,
            "resolved_filled": 0,
            "realized_pnl_usd": 0.0,
        }
    )
    for order in orders:
        price = num(order.get("limit_price"))
        bucket = _price_bucket(price)
        row = buckets[bucket]
        status = str(order.get("final_status") or order.get("status") or "").upper()
        role = _execution_role(order)
        role_row = role_summary[role]
        role_row["orders"] += 1
        row["orders"] += 1
        if status == "FILLED":
            row["filled"] += 1
            role_row["filled"] += 1
            resolution = _resolution_for_order(order, resolutions)
            resolved, pnl = _resolved_pnl(
                side=str(order.get("side") or ""),
                shares=_order_shares(order),
                cost=_order_cost(order),
                resolution=resolution,
            )
            if resolved:
                row["resolved_filled"] += 1
                row["realized_pnl_usd"] = round(float(row["realized_pnl_usd"]) + pnl, 6)
                role_row["resolved_filled"] += 1
                role_row["realized_pnl_usd"] = round(float(role_row["realized_pnl_usd"]) + pnl, 6)
        elif status == "REJECTED":
            row["rejected"] += 1
            role_row["rejected"] += 1
            classification = classify_reject_reason(order)
            category = str(classification["category"])
            group = _reject_group(category)
            error_class = str(classification["error_class"] or "none")
            reject_taxonomy_counts[category] += 1
            reject_group_counts[group] += 1
            reject_error_class_counts[error_class] += 1
            row_taxonomy = Counter(row.get("reject_taxonomy_counts") or {})
            row_taxonomy[category] += 1
            row["reject_taxonomy_counts"] = dict(row_taxonomy)
            row_groups = Counter(row.get("reject_group_counts") or {})
            row_groups[group] += 1
            row["reject_group_counts"] = dict(row_groups)
            row_errors = Counter(row.get("reject_error_class_counts") or {})
            row_errors[error_class] += 1
            row["reject_error_class_counts"] = dict(row_errors)
            if bucket == "04_85_100":
                high_price_reject_samples.append(_reject_sample(order, classification))
                high_price_reject_samples = high_price_reject_samples[-10:]
        elif status == "SUBMITTED":
            role_row["submitted"] += 1

        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        latency_budget = order.get("latency_budget") if isinstance(order.get("latency_budget"), dict) else {}
        if not latency_budget:
            latency_budget = (
                result.get("wallet_copy_latency_budget")
                if isinstance(result.get("wallet_copy_latency_budget"), dict)
                else {}
            )
        hops = latency_budget.get("hops") if isinstance(latency_budget.get("hops"), dict) else {}
        if hops:
            latency_instrumented_orders += 1
            for key, value in hops.items():
                if value is not None:
                    latency_hops[str(key)].append(num(value))
        else:
            latency_missing_orders += 1
        snapshot = result.get("wallet_copy_post_submit_book") if isinstance(result.get("wallet_copy_post_submit_book"), dict) else {}
        if snapshot:
            measured_snapshots += 1
            row["snapshot_rows"] += 1
            snapshot_status = str(snapshot.get("status") or "UNKNOWN")
            row_status_counts = Counter(row.get("snapshot_status_counts") or {})
            row_status_counts[snapshot_status] += 1
            row["snapshot_status_counts"] = dict(row_status_counts)
            snapshot_status_counts[snapshot_status] += 1
            fetch_ms = num(snapshot.get("fetch_ms"), -1.0)
            if fetch_ms >= 0:
                row["snapshot_fetch_ms"].append(fetch_ms)
                snapshot_fetch_ms.append(fetch_ms)
        best_ask = num(snapshot.get("best_ask")) if snapshot.get("status") == "OK" else 0.0
        if status == "REJECTED" and best_ask > 0:
            row["chase_rows"] += 1
            requested_usd = num(order.get("requested_size_usd"))
            chase_depth_usd = num(snapshot.get("ask_depth_at_or_below_best_ask_usd"))
            if chase_depth_usd <= 0:
                chase_depth_usd = num(snapshot.get("ask_depth_at_or_below_limit_usd"))
            if chase_depth_usd + 1e-9 < requested_usd:
                continue
            row["depth_gated_chase_rows"] += 1
            resolution = _resolution_for_order(order, resolutions)
            chase_shares = requested_usd / best_ask if best_ask > 0 else 0.0
            resolved, chase_pnl = _resolved_pnl(
                side=str(order.get("side") or ""),
                shares=chase_shares,
                cost=requested_usd,
                resolution=resolution,
            )
            if resolved:
                row["resolved_chase_rows"] += 1
                row["would_have_chase_pnl_usd"] = round(float(row["would_have_chase_pnl_usd"]) + chase_pnl, 6)
                row["skip_vs_chase_pnl_usd"] = round(float(row["skip_vs_chase_pnl_usd"]) - chase_pnl, 6)

    out_buckets = {}
    for bucket, row in sorted(buckets.items()):
        orders_count = int(row["orders"])
        filled = int(row["filled"])
        fetch_values = [float(value) for value in row.pop("snapshot_fetch_ms", [])]
        row["snapshot_fetch_ms_summary"] = {
            "count": len(fetch_values),
            "p50": _percentile(fetch_values, 0.5),
            "p90": _percentile(fetch_values, 0.9),
            "max": round(max(fetch_values), 6) if fetch_values else 0.0,
        }
        row["fill_rate_pct"] = round(100.0 * filled / orders_count, 6) if orders_count else 0.0
        out_buckets[bucket] = dict(row)
    filled_total = sum(int(row["filled"]) for row in out_buckets.values())
    high_price_groups = out_buckets.get("04_85_100", {}).get("reject_group_counts") or {}
    high_price_genuine_rejects = int(high_price_groups.get("genuine_slippage_or_liquidity") or 0)
    high_price_precision_rejects = int(high_price_groups.get("sizing_or_precision") or 0)
    high_price_measured_skip_candidate = high_price_genuine_rejects > high_price_precision_rejects
    return {
        "schema_version": 1,
        "kind": "wallet_copy_live_fill_quality_report",
        "flow_stage": "LIVE",
        "updated_at": utc_now_iso(),
        "ledger_resolution_writeback": (
            dict(ledger.get("resolution_writeback"))
            if isinstance(ledger.get("resolution_writeback"), dict)
            else {}
        ),
        "canonical_pnl_truth": {
            "source": canonical_truth.get("source"),
            "total": canonical_truth.get("total", {}),
            "by_price_band": canonical_truth.get("by_price_band", {}),
        },
        "orders": len(orders),
        "filled": filled_total,
        "rejected": sum(int(row["rejected"]) for row in out_buckets.values()),
        "fill_rate_pct": round(100.0 * filled_total / len(orders), 6) if orders else 0.0,
        "post_submit_book_snapshots": measured_snapshots,
        "snapshot_status_counts": dict(sorted(snapshot_status_counts.items())),
        "snapshot_fetch_ms_summary": {
            "count": len(snapshot_fetch_ms),
            "p50": _percentile(snapshot_fetch_ms, 0.5),
            "p90": _percentile(snapshot_fetch_ms, 0.9),
            "max": round(max(snapshot_fetch_ms), 6) if snapshot_fetch_ms else 0.0,
        },
        "latency_budget": {
            "instrumented_orders": latency_instrumented_orders,
            "missing_orders": latency_missing_orders,
            "hop_summaries": {
                key: _summary_stats(values)
                for key, values in sorted(latency_hops.items())
            },
            "next_action": (
                "use p50/p90 hop timings to decide whether source detection, intent build, submit path, or exchange ack dominates BTC-5m edge loss"
                if latency_instrumented_orders
                else "reload the live guard with latency instrumentation and collect the next live order"
            ),
        },
        "execution_role_summary": dict(sorted((role, dict(row)) for role, row in role_summary.items())),
        "reject_classification": {
            "rejects": sum(reject_taxonomy_counts.values()),
            "taxonomy_counts": dict(sorted(reject_taxonomy_counts.items())),
            "group_counts": dict(sorted(reject_group_counts.items())),
            "error_class_counts": dict(sorted(reject_error_class_counts.items())),
            "high_price_85_100": {
                "rejects": int(out_buckets.get("04_85_100", {}).get("rejected") or 0),
                "group_counts": dict(
                    sorted((out_buckets.get("04_85_100", {}).get("reject_group_counts") or {}).items())
                ),
                "taxonomy_counts": dict(
                    sorted((out_buckets.get("04_85_100", {}).get("reject_taxonomy_counts") or {}).items())
                ),
                "genuine_slippage_or_liquidity_rejects": high_price_genuine_rejects,
                "sizing_or_precision_rejects": high_price_precision_rejects,
                "measured_skip_candidate": high_price_measured_skip_candidate,
                "sampled_rejects": high_price_reject_samples,
                "decision_threshold": (
                    "treat as measured-skip candidate when genuine_slippage_or_liquidity "
                    "dominates sizing_or_precision"
                ),
            },
        },
        "post_fix_precision_verification": _post_fix_precision_verification(orders, post_fix_since),
        "price_band_decision_window": _price_band_decision_window(
            orders,
            resolutions,
            reconciled_pnl_by_order_id=reconciled_pnl,
            since=price_band_decision_since,
            max_price=price_band_decision_max_price,
            min_resolved_filled=price_band_decision_min_resolved,
            min_fill_rate_pct=price_band_decision_min_fill_rate_pct,
            target_wallet=price_band_decision_wallet,
        ),
        "buckets": out_buckets,
        "next_action": (
            "collect post-submit book snapshots before enabling chase"
            if measured_snapshots < 50
            else "review bucket chase PnL and set max_chase_ticks only where positive"
        ),
    }


def main() -> int:
    args = parse_args()
    resolutions = load_resolutions(args.resolutions)
    writeback_summary: dict[str, Any] | None = None
    if bool(args.write_ledger_resolutions):
        with json_file_lock(args.ledger):
            ledger = load_json(args.ledger, default={})
            if not isinstance(ledger, dict):
                ledger = {}
            writeback_summary = annotate_ledger_resolutions(ledger, resolutions)
            atomic_write_json(args.ledger, ledger)
    else:
        ledger = load_json(args.ledger, default={})
        if not isinstance(ledger, dict):
            ledger = {}
    cash_diff_report = load_json(args.cash_diff, default={}) if args.cash_diff else {}
    if not isinstance(cash_diff_report, dict):
        cash_diff_report = {}
    report = build_fill_quality_report(
        ledger,
        resolutions,
        cash_diff_report=cash_diff_report,
        post_fix_since=str(args.post_fix_since or ""),
        price_band_decision_since=str(args.price_band_decision_since or ""),
        price_band_decision_max_price=float(args.price_band_decision_max_price),
        price_band_decision_min_resolved=int(args.price_band_decision_min_resolved),
        price_band_decision_min_fill_rate_pct=float(args.price_band_decision_min_fill_rate_pct),
        price_band_decision_wallet=str(args.price_band_decision_wallet or ""),
    )
    if writeback_summary is not None:
        report["ledger_resolution_writeback"] = writeback_summary
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
