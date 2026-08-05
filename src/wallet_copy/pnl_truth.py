"""Canonical realized PnL accounting for live wallet-copy ledgers.

This module is the single accounting surface for live realized PnL. Consumers
may slice its output by day, member, lane, or band, but should not reimplement
the fill-resolution join.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import Config
from src.wallet_copy.mission import (
    BEST_ASK_FLOOR_COVERED_COPY_MODELS,
    RULED_01A_ENTRY_PRICE_MAX_EXCLUSIVE,
    RULED_01A_ENTRY_PRICE_MIN,
)
from src.wallet_copy.models import num, parse_ts, utc_now_iso


def norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return ""


def order_source_wallet(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    return norm_wallet(order.get("source_wallet") or source_intent.get("source_wallet")) or "unknown"


def order_lane(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    policy = metadata.get("wallet_copy_policy") if isinstance(metadata.get("wallet_copy_policy"), dict) else {}
    for value in (
        order.get("policy_id"),
        source_intent.get("policy_id"),
        policy.get("policy_id"),
        metadata.get("copy_model"),
        order.get("copy_model"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return "unknown"


def order_copy_model(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    return str(
        order.get("copy_model")
        or source_intent.get("copy_model")
        or metadata.get("copy_model")
        or "unknown"
    ).strip().lower()


def order_ts(order: dict[str, Any]) -> float | None:
    for key in ("submitted_at", "updated_at", "created_at"):
        value = parse_ts(order.get(key))
        if value is not None:
            return value
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    values = [parse_ts(row.get("ts")) for row in lifecycle if isinstance(row, dict)]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    if price < 0.85:
        return "03_70_85"
    return "04_85_100"


def price_subbucket(price: float) -> str:
    if price < 0.25 or price >= 0.50:
        return price_bucket(price)
    if price < 0.32:
        return "01a_25_32"
    if price < 0.40:
        return "01b_32_40"
    return "01c_40_50"


def _receipt_cost_keys(order: dict[str, Any]) -> list[str]:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    details = result.get("details") if isinstance(result.get("details"), dict) else {}
    values: list[Any] = [
        order.get("order_id"),
        result.get("order_id"),
        details.get("orderID"),
        order.get("tx_hash"),
        order.get("transaction_hash"),
    ]
    for container in (order, result, details):
        for key in ("tx_hashes", "transactionsHashes"):
            raw = container.get(key) if isinstance(container, dict) else None
            if isinstance(raw, list):
                values.extend(raw)
            elif raw:
                values.append(raw)
    keys: list[str] = []
    for value in values:
        text = str(value or "").strip().lower()
        if text:
            keys.append(text)
    return list(dict.fromkeys(keys))


def _cost_record_amount(value: Any) -> float:
    if isinstance(value, dict):
        for key in ("actual_cost_usd", "cost_usd", "amount_usd"):
            amount = num(value.get(key))
            if amount > 0:
                return amount
        return 0.0
    return num(value)


def _order_actual_trade_cost(order: dict[str, Any], actual_trade_costs: dict[str, Any] | None = None) -> tuple[float, str]:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for container in (order, result):
        amount = _cost_record_amount(container.get("actual_trade_cost_usd") if isinstance(container, dict) else None)
        if amount > 0:
            key = str(
                (container.get("actual_trade_cost_key") if isinstance(container, dict) else "")
                or (container.get("actual_trade_cost_tx") if isinstance(container, dict) else "")
                or ""
            ).strip().lower()
            return round(amount, 6), key
    costs = actual_trade_costs or {}
    for key in _receipt_cost_keys(order):
        amount = _cost_record_amount(costs.get(key))
        if amount > 0:
            return round(amount, 6), key
    return 0.0, ""


def order_intended_cost(order: dict[str, Any]) -> float:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for value in (
        result.get("response_filled_size_usd"),
        result.get("filled_size_usd"),
        order.get("filled_size_usd"),
        result.get("making_amount"),
        order.get("requested_size_usd"),
    ):
        amount = num(value)
        if amount > 0:
            adjustment = num(result.get("market_order_amount_adjustment_usd"))
            return round(amount + adjustment, 6)
    return 0.0


def order_response_cost(order: dict[str, Any]) -> float:
    # Actual cash principal debited by the fill; the market-order precision
    # adjustment converts this back to the intended/requested amount and must
    # never be applied to a realized-PnL cost basis.
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for value in (
        result.get("response_filled_size_usd"),
        result.get("filled_size_usd"),
        order.get("filled_size_usd"),
        result.get("making_amount"),
        order.get("requested_size_usd"),
    ):
        amount = num(value)
        if amount > 0:
            return round(amount, 6)
    return 0.0


def order_cost_basis(
    order: dict[str, Any],
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> tuple[float, str, str]:
    actual_cost, actual_key = _order_actual_trade_cost(order, actual_trade_costs=actual_trade_costs)
    if actual_cost > 0:
        return actual_cost, "actual_trade_record", actual_key
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    if order.get("actual_trade_cost_rejected_reason") or result.get("actual_trade_cost_rejected_reason"):
        response = order_response_cost(order)
        if response > 0:
            return response, "response_filled_size_usd", "actual_trade_rejected_fallback"
    # A receipt observed after settlement is a separate measurement. It must
    # never retroactively rewrite the principal banked from the fill response.
    response = order_response_cost(order)
    if response > 0:
        return response, "response_filled_size_usd", ""
    return 0.0, "response_filled_size_usd", ""


def order_cost(order: dict[str, Any]) -> float:
    return order_cost_basis(order)[0]


def order_shares(order: dict[str, Any]) -> float:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for value in (
        result.get("response_fill_size_shares"),
        result.get("filled_shares"),
        order.get("filled_shares"),
        result.get("taking_amount"),
        order.get("requested_shares"),
    ):
        shares = num(value)
        if shares > 0:
            return shares
    return 0.0


def realized_entry_facts(order: dict[str, Any]) -> tuple[float | None, str, bool]:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    cost = num(result.get("response_filled_size_usd") or order.get("response_filled_size_usd"))
    shares = num(result.get("response_fill_size_shares") or order.get("response_fill_size_shares"))
    if cost <= 0 or shares <= 0:
        return None, "UNMEASURED", False
    price = cost / shares
    return (
        round(price, 9),
        price_subbucket(price),
        not (
            RULED_01A_ENTRY_PRICE_MIN
            <= price
            < RULED_01A_ENTRY_PRICE_MAX_EXCLUSIVE
        ),
    )


def resolution_for_order(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    keys = [
        str(order.get("condition_id") or ""),
        str(order.get("token_id") or source_intent.get("token_id") or ""),
    ]
    slug = str(order.get("market_slug") or source_intent.get("market_slug") or "")
    start = slug.rsplit("-", 1)[-1]
    if start.isdigit():
        keys.append(f"slug_start:{start}")
    for key in keys:
        row = resolutions.get(key)
        if row:
            return row
    cached = order.get("resolution") if isinstance(order.get("resolution"), dict) else {}
    if winner_from_resolution(cached):
        return cached
    return None


def winner_from_resolution(row: dict[str, Any] | None) -> str:
    direction = str((row or {}).get("direction") or "").upper()
    if direction.startswith("UP"):
        return "YES"
    if direction.startswith("DOWN"):
        return "NO"
    return ""


def resolved_pnl(*, side: str, shares: float, cost: float, resolution: dict[str, Any] | None) -> tuple[bool, float]:
    winner = winner_from_resolution(resolution)
    if not winner:
        return False, 0.0
    payout = shares if str(side or "").upper() == winner else 0.0
    return True, round(payout - cost, 6)


def _metric() -> dict[str, Any]:
    return {
        "orders": 0,
        "fills": 0,
        "rejects": 0,
        "resolved_fills": 0,
        "unresolved_fills": 0,
        "cost_usd": 0.0,
        "payout_usd": 0.0,
        "payout_fill_count": 0,
        "pnl_usd": 0.0,
        "realized_entry_measured_fills": 0,
        "in_band_fill_count": 0,
        "in_band_resolved_fill_count": 0,
        "in_band_payout_fill_count": 0,
        "out_of_band_fill_count": 0,
        "out_of_band_cost_usd": 0.0,
        "ruled_floor_breach_count": 0,
        "ruled_floor_breach_cost_usd": 0.0,
        "floor_gate_copy_model_in_coverage_set_fill_count": 0,
        "floor_gate_enforced_fill_count": 0,
        "stale_book_at_gate_abstain_count": 0,
        "_book_age_at_submit_sent_samples": [],
        "_gate_to_submit_sent_samples": [],
        "_threshold_independent_flags": [],
        "_downward_slippage_from_limit_samples": [],
    }


def _add_metric(row: dict[str, Any], event: dict[str, Any]) -> None:
    row["orders"] += 1
    if event.get("abstain_reason") == "stale_book_at_gate":
        row["stale_book_at_gate_abstain_count"] += 1
    status = str(event.get("status") or "")
    if status == "FILLED":
        for event_key, sample_key in (
            ("book_age_at_submit_sent_s", "_book_age_at_submit_sent_samples"),
            ("gate_to_submit_sent_s", "_gate_to_submit_sent_samples"),
        ):
            value = event.get(event_key)
            if isinstance(value, (int, float)):
                row[sample_key].append(float(value))
        independent = event.get("threshold_independent_of_cache_ttl")
        if isinstance(independent, bool):
            row["_threshold_independent_flags"].append(independent)
        realized_minus_limit = event.get("realized_minus_limit_price")
        if isinstance(realized_minus_limit, (int, float)):
            row["_downward_slippage_from_limit_samples"].append(
                max(0.0, -float(realized_minus_limit))
            )
        row["fills"] += 1
        if event.get("floor_gate_copy_model_in_coverage_set"):
            row["floor_gate_copy_model_in_coverage_set_fill_count"] += 1
        if event.get("floor_gate_enforced"):
            row["floor_gate_enforced_fill_count"] += 1
        if event.get("realized_entry_price") is not None:
            row["realized_entry_measured_fills"] += 1
            if event.get("ruled_floor_breach"):
                row["ruled_floor_breach_count"] += 1
                row["ruled_floor_breach_cost_usd"] = round(
                    float(row["ruled_floor_breach_cost_usd"])
                    + float(event.get("cost_usd") or 0.0),
                    6,
                )
            if event.get("out_of_band_fill"):
                row["out_of_band_fill_count"] += 1
                row["out_of_band_cost_usd"] = round(
                    float(row["out_of_band_cost_usd"])
                    + float(event.get("cost_usd") or 0.0),
                    6,
                )
            else:
                row["in_band_fill_count"] += 1
        if event.get("resolved"):
            row["resolved_fills"] += 1
            payout_usd = float(event.get("payout_usd") or 0.0)
            if payout_usd > 0.0:
                row["payout_fill_count"] += 1
            if (
                event.get("realized_entry_price") is not None
                and not event.get("out_of_band_fill")
            ):
                row["in_band_resolved_fill_count"] += 1
                if payout_usd > 0.0:
                    row["in_band_payout_fill_count"] += 1
            row["cost_usd"] = round(float(row["cost_usd"]) + float(event.get("cost_usd") or 0.0), 6)
            row["payout_usd"] = round(float(row["payout_usd"]) + payout_usd, 6)
            row["pnl_usd"] = round(float(row["pnl_usd"]) + float(event.get("pnl_usd") or 0.0), 6)
        else:
            row["unresolved_fills"] += 1
    elif status == "REJECTED":
        row["rejects"] += 1


def finalize_metric(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key, output_key in (
        ("_book_age_at_submit_sent_samples", "book_age_at_submit_sent_s"),
        ("_gate_to_submit_sent_samples", "gate_to_submit_sent_s"),
    ):
        samples = sorted(float(value) for value in out.pop(key, []))
        out[output_key] = {
            "count": len(samples),
            "min": round(samples[0], 6) if samples else None,
            "median": round(statistics.median(samples), 6) if samples else None,
            "max": round(samples[-1], 6) if samples else None,
        }
    independence_flags = out.pop("_threshold_independent_flags", [])
    out["threshold_independent_of_cache_ttl"] = (
        all(independence_flags) if independence_flags else None
    )
    slippage = sorted(
        float(value)
        for value in out.pop("_downward_slippage_from_limit_samples", [])
    )
    p95_index = max(0, math.ceil(0.95 * len(slippage)) - 1) if slippage else 0
    out["downward_slippage_from_limit"] = {
        "count": len(slippage),
        "min": round(slippage[0], 9) if slippage else None,
        "median": round(statistics.median(slippage), 9) if slippage else None,
        "p95": round(slippage[p95_index], 9) if slippage else None,
        "max": round(slippage[-1], 9) if slippage else None,
    }
    for key in (
        "cost_usd",
        "payout_usd",
        "pnl_usd",
        "out_of_band_cost_usd",
        "ruled_floor_breach_cost_usd",
    ):
        out[key] = round(float(out.get(key) or 0.0), 6)
    fills = int(out.get("fills") or 0)
    resolved = int(out.get("resolved_fills") or 0)
    out["resolved_fill_rate_pct"] = round((100.0 * resolved / fills), 6) if fills else 0.0
    out["roi_pct"] = round((100.0 * float(out["pnl_usd"]) / float(out["cost_usd"])), 6) if out["cost_usd"] else 0.0
    measured = int(out.get("realized_entry_measured_fills") or 0)
    in_band = int(out.get("in_band_fill_count") or 0)
    out["in_band_fill_rate"] = round(in_band / measured, 9) if measured else None
    out["in_band_fill_rate_pct"] = round(100.0 * in_band / measured, 6) if measured else None
    holdout_win_rate = 0.29787234
    in_band_resolved = int(out.get("in_band_resolved_fill_count") or 0)
    in_band_winners = int(out.get("in_band_payout_fill_count") or 0)
    lower_tail_p_value = None
    if in_band_resolved:
        lower_tail_p_value = sum(
            math.comb(in_band_resolved, winner_count)
            * (holdout_win_rate ** winner_count)
            * ((1.0 - holdout_win_rate) ** (in_band_resolved - winner_count))
            for winner_count in range(in_band_winners + 1)
        )
    zero_winner_tripwire_target = 15
    zero_winner_falsified = (
        in_band_resolved >= zero_winner_tripwire_target and in_band_winners == 0
    )
    out["in_band_holdout_win_rate"] = holdout_win_rate
    out["in_band_winner_binomial_lower_tail_p_value"] = (
        round(lower_tail_p_value, 9) if lower_tail_p_value is not None else None
    )
    out["in_band_zero_winner_tripwire_target"] = zero_winner_tripwire_target
    out["in_band_holdout_live_falsified"] = zero_winner_falsified
    out["in_band_holdout_live_status"] = (
        "FALSIFIED"
        if zero_winner_falsified
        else "WATCH"
        if in_band_resolved
        else "PENDING"
    )
    coverage_set = int(
        out.get("floor_gate_copy_model_in_coverage_set_fill_count") or 0
    )
    enforced = int(out.get("floor_gate_enforced_fill_count") or 0)
    out["floor_gate_copy_model_in_coverage_set_fill_rate"] = (
        round(coverage_set / fills, 9) if fills else None
    )
    out["floor_gate_copy_model_in_coverage_set_fill_rate_pct"] = (
        round(100.0 * coverage_set / fills, 6) if fills else None
    )
    out["floor_gate_enforced_fill_rate"] = (
        round(enforced / fills, 9) if fills else None
    )
    out["floor_gate_enforced_fill_rate_pct"] = (
        round(100.0 * enforced / fills, 6) if fills else None
    )
    return out


def score_order(
    order: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status = str(order.get("final_status") or order.get("status") or "").upper()
    ts = order_ts(order)
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    realized_entry_price, realized_entry_band, out_of_band_fill = realized_entry_facts(order)
    copy_model = order_copy_model(order)
    trade_decision = (
        order.get("trade_decision")
        if isinstance(order.get("trade_decision"), dict)
        else {}
    )
    trade_result = (
        order.get("trade_result")
        if isinstance(order.get("trade_result"), dict)
        else {}
    )
    floor_evidence = (
        trade_result.get("submit_best_ask_evidence")
        if isinstance(trade_result.get("submit_best_ask_evidence"), dict)
        else {}
    )
    inventory_gate = (
        trade_result.get("inventory_best_ask_gate")
        if isinstance(trade_result.get("inventory_best_ask_gate"), dict)
        else floor_evidence
    )
    floor_gate_enforced = bool(
        inventory_gate.get("max_gate_probe_best_ask_age_at_gate_s") is not None
        and str(inventory_gate.get("generation_sha256") or "").strip()
    )
    gate_probe_best_ask = num(
        trade_result.get("gate_probe_best_ask"),
        floor_evidence.get("gate_probe_best_ask"),
    ) or 0.0
    limit_price = num(order.get("limit_price")) or 0.0
    event = {
        "order_id": str(order.get("order_id") or ""),
        "intent_id": str(order.get("intent_id") or ""),
        "submitted_at": str(order.get("submitted_at") or order.get("updated_at") or ""),
        "ts": ts,
        "day_utc": datetime.fromtimestamp(ts, tz=UTC).date().isoformat() if ts is not None else "",
        "status": status,
        "abstain_reason": str(
            trade_decision.get("reason")
            or trade_decision.get("strategy_reason")
            or order.get("reason")
            or ""
        ),
        "source_wallet": order_source_wallet(order),
        "lane": order_lane(order),
        "copy_model": copy_model,
        "floor_gate_copy_model_in_coverage_set": (
            copy_model in BEST_ASK_FLOOR_COVERED_COPY_MODELS
        ),
        "floor_gate_enforced": floor_gate_enforced,
        "floor_gate_generation_sha256": (
            str(inventory_gate.get("generation_sha256") or "").strip() or None
        ),
        "wallet_name": str(order.get("wallet_name") or ""),
        "condition_id": str(order.get("condition_id") or ""),
        "market_slug": str(order.get("market_slug") or source_intent.get("market_slug") or ""),
        "side": str(order.get("side") or ""),
        "limit_price": round(limit_price, 6),
        "price_bucket": price_bucket(limit_price),
        "price_subbucket": price_subbucket(limit_price),
        "realized_entry_price": realized_entry_price,
        "realized_entry_band": realized_entry_band,
        "out_of_band_fill": out_of_band_fill,
        "realized_entry_classification": (
            "RULED_FLOOR_BREACH"
            if realized_entry_price is not None
            and realized_entry_price < RULED_01A_ENTRY_PRICE_MIN
            else "RULED_FLOOR_PASS"
            if realized_entry_price is not None
            else "UNMEASURED"
        ),
        "ruled_floor_breach": bool(
            realized_entry_price is not None
            and realized_entry_price < RULED_01A_ENTRY_PRICE_MIN
        ),
        "gate_probe_best_ask": gate_probe_best_ask or None,
        "gate_probe_best_ask_age_at_gate_s": (
            order.get("trade_result", {})
            .get("submit_best_ask_evidence", {})
            .get("gate_probe_best_ask_age_at_gate_s")
            if isinstance(order.get("trade_result"), dict)
            else None
        ),
        "book_age_at_submit_sent_s": (
            order.get("trade_result", {})
            .get("submit_best_ask_evidence", {})
            .get("book_age_at_submit_sent_s")
            if isinstance(order.get("trade_result"), dict)
            else None
        ),
        "gate_to_submit_sent_s": (
            order.get("trade_result", {})
            .get("submit_best_ask_evidence", {})
            .get("gate_to_submit_sent_s")
            if isinstance(order.get("trade_result"), dict)
            else None
        ),
        "threshold_independent_of_cache_ttl": (
            order.get("trade_result", {})
            .get("submit_best_ask_evidence", {})
            .get("threshold_independent_of_cache_ttl")
            if isinstance(order.get("trade_result"), dict)
            else None
        ),
        "event_age_s": (
            order.get("trade_result", {})
            .get("submit_best_ask_evidence", {})
            .get("event_age_s")
            if isinstance(order.get("trade_result"), dict)
            else None
        ),
        "realized_minus_limit_price": round(
            realized_entry_price - limit_price, 9
        )
        if realized_entry_price is not None and limit_price > 0.0
        else None,
        "realized_minus_gate_probe_best_ask": round(
            realized_entry_price - gate_probe_best_ask, 9
        )
        if realized_entry_price is not None and gate_probe_best_ask > 0.0
        else None,
        "cost_usd": 0.0,
        "intended_cost_usd": 0.0,
        "price_improvement_usd": 0.0,
        "shares": 0.0,
        "payout_usd": 0.0,
        "pnl_usd": 0.0,
        "cost_basis_source": "response_filled_size_usd",
        "cost_basis_key": "",
        "resolved": False,
        "winner": "",
        "source": "pnl_truth_fill_resolution_join",
    }
    if status != "FILLED":
        return event
    intended_cost = order_intended_cost(order)
    cost, cost_basis_source, cost_basis_key = order_cost_basis(
        order,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    shares = order_shares(order)
    resolution = resolution_for_order(order, resolutions)
    resolved, pnl = resolved_pnl(side=event["side"], shares=shares, cost=cost, resolution=resolution)
    winner = winner_from_resolution(resolution)
    payout = shares if resolved and str(event["side"]).upper() == winner else 0.0
    event.update(
        {
            "cost_usd": round(cost, 6),
            "intended_cost_usd": round(intended_cost, 6),
            "price_improvement_usd": round(intended_cost - cost, 6)
            if cost_basis_source == "actual_trade_record" and intended_cost > 0
            else 0.0,
            "cost_basis_source": cost_basis_source,
            "cost_basis_key": cost_basis_key,
            "shares": round(shares, 6),
            "payout_usd": round(payout, 6),
            "pnl_usd": round(pnl, 6) if resolved else 0.0,
            "resolved": bool(resolved),
            "winner": winner,
            "resolution_source": str((resolution or {}).get("source") or (resolution or {}).get("resolution_precision") or ""),
        }
    )
    return event


def build_pnl_truth(
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    start_ts: float | None = None,
    end_ts: float | None = None,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    orders = [row for row in ledger.get("orders") or [] if isinstance(row, dict)]
    events = [
        score_order(
            order,
            resolutions,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
        for order in orders
    ]
    scoped = [
        event
        for event in events
        if event.get("ts") is not None
        and (start_ts is None or float(event["ts"]) >= start_ts)
        and (end_ts is None or float(event["ts"]) < end_ts)
    ]
    total = _metric()
    by_day: dict[str, dict[str, Any]] = defaultdict(_metric)
    by_member: dict[str, dict[str, Any]] = defaultdict(_metric)
    by_lane: dict[str, dict[str, Any]] = defaultdict(_metric)
    by_band: dict[str, dict[str, Any]] = defaultdict(_metric)
    by_subband: dict[str, dict[str, Any]] = defaultdict(_metric)
    by_realized_entry_band: dict[str, dict[str, Any]] = defaultdict(_metric)
    status_counts: Counter[str] = Counter()
    cost_basis_counts: Counter[str] = Counter()
    latest_order_ts = ""
    for event in scoped:
        status_counts[str(event.get("status") or "")] += 1
        if str(event.get("status") or "") == "FILLED":
            cost_basis_counts[str(event.get("cost_basis_source") or "unknown")] += 1
        latest_order_ts = max(latest_order_ts, str(event.get("submitted_at") or ""))
        _add_metric(total, event)
        _add_metric(by_day[str(event.get("day_utc") or "unknown")], event)
        _add_metric(by_member[str(event.get("source_wallet") or "unknown")], event)
        _add_metric(by_lane[str(event.get("lane") or "unknown")], event)
        _add_metric(by_band[str(event.get("price_bucket") or "unknown")], event)
        _add_metric(by_subband[str(event.get("price_subbucket") or "unknown")], event)
        if (
            str(event.get("status") or "") == "FILLED"
            and str(event.get("realized_entry_band") or "")
        ):
            _add_metric(
                by_realized_entry_band[str(event["realized_entry_band"])],
                event,
            )
    if cost_basis_counts.get("actual_trade_record"):
        primary_cost_basis = "actual_trade_record"
    else:
        primary_cost_basis = "response_filled_size_usd"
    return {
        "schema_version": 1,
        "kind": "wallet_copy_pnl_truth",
        "generated_at": utc_now_iso(),
        "source": "live_execution_ledger_plus_canonical_resolutions",
        "cost_basis_source": primary_cost_basis,
        "fallback_cost_basis_source": "response_filled_size_usd",
        "scope": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "orders_in_scope": len(scoped),
            "latest_order_ts": latest_order_ts,
            "status_counts": dict(sorted(status_counts.items())),
            "cost_basis_counts": dict(sorted(cost_basis_counts.items())),
        },
        "events": scoped,
        "total": finalize_metric(total),
        "by_day": {key: finalize_metric(value) for key, value in sorted(by_day.items())},
        "by_member": {key: finalize_metric(value) for key, value in sorted(by_member.items())},
        "by_lane": {key: finalize_metric(value) for key, value in sorted(by_lane.items())},
        "by_price_band": {key: finalize_metric(value) for key, value in sorted(by_band.items())},
        "by_price_subband": {
            key: {**finalize_metric(value), "keyed_on": "decision_price"}
            for key, value in sorted(by_subband.items())
        },
        "by_realized_entry_band": {
            key: {**finalize_metric(value), "keyed_on": "realized_entry_price"}
            for key, value in sorted(by_realized_entry_band.items())
        },
    }


def filled_order_count(ledger: dict[str, Any]) -> int:
    return sum(
        1
        for order in ledger.get("orders") or []
        if isinstance(order, dict)
        and str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
    )


def validate_resolutions_nonempty_for_fills(
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    resolutions_path: str | Path = "",
) -> None:
    fills = filled_order_count(ledger)
    if fills > 0 and not resolutions:
        path = f" path={resolutions_path}" if resolutions_path else ""
        raise RuntimeError(f"resolution snapshot loaded zero rows while ledger has {fills} filled orders;{path}")


def resolution_snapshot_status(
    resolutions_path: str | Path,
    resolutions: dict[str, dict[str, Any]],
    *,
    max_age_s: float = 7200.0,
) -> dict[str, Any]:
    path = Path(resolutions_path)
    row_count = len(resolutions)
    if not path.exists():
        return {
            "path": str(path),
            "status": "MISSING",
            "rows": row_count,
            "age_s": None,
            "max_age_s": float(max_age_s),
            "pnl_citable": False,
            "reason": "resolutions_path_missing",
        }
    mtime = path.stat().st_mtime
    now = datetime.now(tz=UTC).timestamp()
    age_s = max(0.0, now - mtime)
    stale = age_s > float(max_age_s)
    zero = row_count == 0
    return {
        "path": str(path),
        "status": "STALE" if stale else "ZERO_ROWS" if zero else "FRESH",
        "rows": row_count,
        "mtime": datetime.fromtimestamp(mtime, tz=UTC).isoformat().replace("+00:00", "Z"),
        "age_s": round(age_s, 3),
        "max_age_s": float(max_age_s),
        "pnl_citable": bool(row_count > 0 and not stale),
        "reason": "snapshot_older_than_max_age" if stale else "snapshot_loaded_zero_rows" if zero else "",
    }


def _legacy_max_fill_pnl(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> tuple[bool, float]:
    if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
        return False, 0.0
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    cost = max(num(result.get("response_filled_size_usd")), num(result.get("making_amount")), num(order.get("requested_size_usd")))
    shares = max(num(result.get("response_fill_size_shares")), num(result.get("taking_amount")), num(order.get("requested_shares")))
    return resolved_pnl(side=str(order.get("side") or ""), shares=shares, cost=cost, resolution=resolution_for_order(order, resolutions))


def _order_event_key(row: dict[str, Any]) -> str:
    for key in ("order_id", "intent_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return "|".join(
        [
            str(row.get("submitted_at") or ""),
            str(row.get("condition_id") or ""),
            str(row.get("side") or ""),
            str(row.get("market_slug") or ""),
        ]
    )


def discrepancy_report(ledger: dict[str, Any], truth: dict[str, Any]) -> dict[str, Any]:
    writeback = ledger.get("resolution_writeback") if isinstance(ledger.get("resolution_writeback"), dict) else {}
    truth_total = truth.get("total") if isinstance(truth.get("total"), dict) else {}
    writeback_pnl = float(writeback.get("pnl_usd") or 0.0)
    truth_pnl = float(truth_total.get("pnl_usd") or 0.0)
    writeback_resolved = int(writeback.get("resolved_filled_orders") or 0)
    truth_resolved = int(truth_total.get("resolved_fills") or 0)
    rows: list[dict[str, Any]] = []
    if writeback:
        rows.append(
            {
                "source": "ledger.resolution_writeback",
                "pnl_usd": round(writeback_pnl, 6),
                "resolved_fills": writeback_resolved,
                "delta_vs_canonical_pnl_usd": round(writeback_pnl - truth_pnl, 6),
                "delta_vs_canonical_resolved_fills": writeback_resolved - truth_resolved,
            }
        )
    ledger_order_pnl = 0.0
    ledger_order_resolved = 0
    unresolved_with_cached_pnl = 0
    legacy_max_pnl = 0.0
    legacy_max_resolved = 0
    actual_fill_pnl = 0.0
    actual_fill_resolved = 0
    amount_method_delta_rows = 0
    amount_method_delta_pnl = 0.0
    unresolved_excluded = 0
    itemized_delta_orders: list[dict[str, Any]] = []
    resolutions_by_condition: dict[str, dict[str, Any]] = {}
    truth_events_by_key: dict[str, dict[str, Any]] = {}
    for event in truth.get("events") or []:
        if not isinstance(event, dict):
            continue
        truth_events_by_key[_order_event_key(event)] = event
        if event.get("condition_id") and event.get("winner"):
            resolutions_by_condition[str(event["condition_id"])] = {
                "direction": "UP" if event.get("winner") == "YES" else "DOWN",
            }
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        event = truth_events_by_key.get(_order_event_key(order)) or score_order(order, resolutions_by_condition)
        if event.get("resolved"):
            actual_fill_pnl = round(actual_fill_pnl + num(event.get("pnl_usd")), 6)
            actual_fill_resolved += 1
        else:
            unresolved_excluded += 1
        legacy_resolved, legacy_pnl = _legacy_max_fill_pnl(order, resolutions_by_condition)
        if legacy_resolved:
            legacy_max_pnl = round(legacy_max_pnl + legacy_pnl, 6)
            legacy_max_resolved += 1
            if event.get("resolved") and abs(legacy_pnl - num(event.get("pnl_usd"))) > 0.000001:
                amount_method_delta_rows += 1
                amount_method_delta_pnl = round(amount_method_delta_pnl + legacy_pnl - num(event.get("pnl_usd")), 6)
        if order.get("pnl_usd") is not None:
            ledger_order_pnl = round(ledger_order_pnl + num(order.get("pnl_usd")), 6)
            ledger_order_resolved += 1
            if order.get("resolved") is not True:
                unresolved_with_cached_pnl += 1
        cached_pnl = num(order.get("pnl_usd")) if order.get("pnl_usd") is not None else None
        canonical_pnl = num(event.get("pnl_usd")) if event.get("resolved") else None
        legacy_value = legacy_pnl if legacy_resolved else None
        cached_delta = round((cached_pnl or 0.0) - (canonical_pnl or 0.0), 6) if cached_pnl is not None and canonical_pnl is not None else None
        legacy_delta = round((legacy_value or 0.0) - (canonical_pnl or 0.0), 6) if legacy_value is not None and canonical_pnl is not None else None
        cause = ""
        if canonical_pnl is None:
            cause = "unresolved_fill_excluded_from_canonical_realized_pnl"
        elif legacy_delta is not None and abs(legacy_delta) > 0.000001:
            cause = "legacy_requested_or_max_fill_amount_differs_from_actual_exchange_fill"
        elif cached_delta is not None and abs(cached_delta) > 0.000001:
            cause = "cached_order_pnl_differs_from_canonical_recompute"
        elif order.get("pnl_usd") is not None and order.get("resolved") is not True:
            cause = "cached_pnl_present_on_unresolved_row"
        if cause:
            itemized_delta_orders.append(
                {
                    "order_id": str(order.get("order_id") or ""),
                    "intent_id": str(order.get("intent_id") or ""),
                    "submitted_at": str(order.get("submitted_at") or ""),
                    "market_slug": str(order.get("market_slug") or ""),
                    "condition_id": str(order.get("condition_id") or ""),
                    "side": str(order.get("side") or ""),
                    "cached_order_pnl_usd": cached_pnl,
                    "canonical_pnl_usd": canonical_pnl,
                    "delta_cached_vs_canonical_usd": cached_delta,
                    "legacy_requested_or_max_fill_pnl_usd": legacy_value,
                    "delta_legacy_vs_canonical_usd": legacy_delta,
                    "cause": cause,
                }
            )
    rows.append(
        {
            "source": "order.cached_pnl_usd_sum",
            "pnl_usd": round(ledger_order_pnl, 6),
            "resolved_fills": ledger_order_resolved,
            "delta_vs_canonical_pnl_usd": round(ledger_order_pnl - truth_pnl, 6),
            "delta_vs_canonical_resolved_fills": ledger_order_resolved - truth_resolved,
            "unresolved_rows_with_cached_pnl": unresolved_with_cached_pnl,
        }
    )
    rows.append(
        {
            "source": "legacy_requested_or_max_fill_formula",
            "pnl_usd": round(legacy_max_pnl, 6),
            "resolved_fills": legacy_max_resolved,
            "delta_vs_canonical_pnl_usd": round(legacy_max_pnl - truth_pnl, 6),
            "delta_vs_canonical_resolved_fills": legacy_max_resolved - truth_resolved,
            "affected_rows_vs_actual_fill_formula": amount_method_delta_rows,
        }
    )
    return {
        "canonical_source": "pnl_truth.total",
        "canonical_formula": "receipt pUSD debit is cost of record when available; otherwise response_filled_size_usd plus recorded adjustment, joined to winning_side",
        "canonical_pnl_usd": round(truth_pnl, 6),
        "canonical_resolved_fills": truth_resolved,
        "comparisons": rows,
        "itemized_delta_pnl_usd": round(writeback_pnl - truth_pnl, 6) if writeback else 0.0,
        "itemized_delta_orders": itemized_delta_orders,
        "itemized_causes": [
            {
                "cause": "legacy_writeback_used_requested_or_max_fill_amount_instead_of_actual_exchange_fill",
                "delta_pnl_usd": round(amount_method_delta_pnl, 6),
                "affected_rows": amount_method_delta_rows,
                "explanation": "Partial fills were charged at requested/max notional while canonical PnL uses response_filled_size_usd and response_fill_size_shares.",
            },
            {
                "cause": "unresolved_fills_excluded_from_realized_pnl",
                "delta_pnl_usd": 0.0,
                "affected_rows": unresolved_excluded,
                "explanation": "Open or not-yet-resolved fills stay out of realized PnL until a winning side is known.",
            },
        ],
    }


def unresolved_position_bounds(truth: dict[str, Any]) -> dict[str, Any]:
    unresolved = [
        event
        for event in truth.get("events") or []
        if isinstance(event, dict) and str(event.get("status") or "") == "FILLED" and not event.get("resolved")
    ]
    open_cost = round(sum(num(event.get("cost_usd")) for event in unresolved), 6)
    max_payout = round(sum(num(event.get("shares")) for event in unresolved), 6)
    return {
        "unresolved_fills": len(unresolved),
        "open_cost_usd": open_cost,
        "max_payout_usd": max_payout,
        "position_value_bounds_usd": [0.0, max_payout],
        "basis": "unresolved canonical fill shares; lower bound assumes all lose, upper bound assumes all win",
    }


def estimate_cached_open_position_value(positions_path: str | Path) -> dict[str, Any]:
    path = Path(positions_path)
    if not path.exists():
        return {"status": "UNAVAILABLE", "path": str(path), "reason": "positions_cache_missing", "open_position_value_usd": 0.0}
    try:
        rows = [row for row in __import__("json").loads(path.read_text()).copy() if isinstance(row, dict)]
    except Exception as exc:
        return {"status": "UNAVAILABLE", "path": str(path), "reason": f"positions_cache_read_error:{type(exc).__name__}", "open_position_value_usd": 0.0}
    open_rows = [row for row in rows if str(row.get("position_closed") or "").lower() not in {"true", "1"}]
    value = sum(max(num(row.get("response_filled_size_usd")), num(row.get("making_amount")), num(row.get("size_usd"))) for row in open_rows)
    return {
        "status": "CACHE_ESTIMATE",
        "path": str(path),
        "rows": len(rows),
        "open_rows": len(open_rows),
        "open_position_value_usd": round(value, 6),
        "valuation_basis": "local filled-cost cache, not live mark",
    }


def fetch_live_position_value(*, timeout_s: float = 8.0) -> dict[str, Any]:
    user = os.getenv("POLYMARKET_PROXY", "").strip()
    if not user:
        return {"status": "UNAVAILABLE", "reason": "POLYMARKET_PROXY_missing", "open_position_value_usd": 0.0}
    configured_base = os.getenv("POLYMARKET_DATA_API_BASE_URL", "").rstrip("/")
    bases = [
        base
        for base in (
            configured_base,
            "https://data-api.polymarket.com",
            "http://127.0.0.1:8787/data-api",
        )
        if base
    ]
    deduped_bases = list(dict.fromkeys(bases))
    page_limit = 500
    max_pages = 50
    errors: list[str] = []
    rows: list[dict[str, Any]] | None = None
    winner_url = ""
    final_truncated = True
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://polymarket.com",
        "Referer": "https://polymarket.com/",
    }
    for base in deduped_bases:
        fetched_rows: list[dict[str, Any]] = []
        page_errors_before = len(errors)
        truncated = True
        for _attempt in range(3):
            try:
                fetched_rows = []
                for page in range(max_pages):
                    query = urllib.parse.urlencode(
                        {"user": user, "sizeThreshold": ".001", "limit": page_limit, "offset": page * page_limit}
                    )
                    url = f"{base}/positions?{query}"
                    request = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                    page_rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
                    fetched_rows.extend(page_rows)
                    winner_url = url
                    if len(page_rows) < page_limit:
                        truncated = False
                        break
                rows = fetched_rows
                final_truncated = truncated
                break
            except Exception as exc:
                errors.append(f"{base}:{type(exc).__name__}:{str(exc)[:160]}")
        if rows is not None:
            break
        if len(errors) == page_errors_before:
            errors.append(f"{base}:positions_pagination_returned_no_rows")
    if rows is None:
        return {
            "status": "UNAVAILABLE",
            "reason": "positions_fetch_error",
            "errors": errors,
            "urls": [
                f"{base}/positions?{urllib.parse.urlencode({'user': user, 'sizeThreshold': '.001', 'limit': page_limit, 'offset': 0})}"
                for base in deduped_bases
            ],
            "open_position_value_usd": 0.0,
        }
    current_value = sum(num(row.get("currentValue")) for row in rows if isinstance(row, dict))
    initial_value = sum(num(row.get("initialValue")) for row in rows if isinstance(row, dict))
    cash_pnl = sum(num(row.get("cashPnl")) for row in rows if isinstance(row, dict))
    return {
        "status": "LIVE_MARK",
        "url": winner_url,
        "rows": len(rows),
        "rows_fetched": len(rows),
        "page_limit": page_limit,
        "truncated": final_truncated,
        "open_position_value_usd": round(current_value, 6),
        "initial_value_usd": round(initial_value, 6),
        "cash_pnl_usd": round(cash_pnl, 6),
        "valuation_basis": "data-api positions currentValue",
    }


def _derive_scorecard_api_creds(client: Any) -> Any:
    if hasattr(client, "derive_api_key"):
        return client.derive_api_key()
    if hasattr(client, "create_or_derive_api_key"):
        return client.create_or_derive_api_key()
    if hasattr(client, "create_or_derive_api_creds"):
        return client.create_or_derive_api_creds()
    raise RuntimeError("ClobClient has no supported API credential derivation method")


async def _fetch_live_balance(config: Config) -> float:
    if not config.private_key:
        return -1.0

    def _fetch_sync() -> float:
        try:
            try:
                from py_clob_client_v2.client import ClobClient
                from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
            except ImportError:
                from py_clob_client.client import ClobClient
                from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            funder = config.polymarket_proxy if config.polymarket_proxy else None
            signature_type = 1 if funder else 0
            client = ClobClient(
                host=config.clob_host,
                key=config.private_key,
                chain_id=config.chain_id,
                funder=funder,
                signature_type=signature_type,
            )
            client.set_api_creds(_derive_scorecard_api_creds(client))
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=signature_type,
            )
            balance = client.get_balance_allowance(params=params)
            return int(balance.get("balance", 0)) / 1e6
        except Exception:
            return -1.0

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_sync)


def _balance_sample_config(
    count: int | None = None,
    interval_s: float | None = None,
) -> tuple[int, float]:
    raw_count = os.getenv("WALLET_COPY_BALANCE_SAMPLE_COUNT", "1") if count is None else count
    raw_interval = os.getenv("WALLET_COPY_BALANCE_SAMPLE_INTERVAL_S", "0") if interval_s is None else interval_s
    sample_count = int(num(raw_count, 1))
    sample_interval_s = float(num(raw_interval, 0.0))
    return max(1, min(sample_count, 3)), max(0.0, sample_interval_s)


def _fetch_live_balance_samples(config: Config, *, count: int, interval_s: float) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    for idx in range(int(count)):
        sample: dict[str, Any] = {
            "idx": idx,
            "ts": utc_now_iso(),
            "status": "ERROR",
            "balance_usd": None,
            "reason": "",
        }
        try:
            balance = float(asyncio.run(_fetch_live_balance(config)))
            sample["balance_usd"] = round(balance, 6)
            if balance >= 0:
                sample["status"] = "OK"
            else:
                sample["status"] = "ERROR"
                sample["reason"] = "clob_balance_fetch_returned_negative"
        except Exception as exc:
            sample["reason"] = f"clob_balance_fetch_error:{type(exc).__name__}"
        samples.append(sample)
        if idx + 1 < int(count) and interval_s > 0:
            time.sleep(interval_s)
    return _balance_sample_selection(samples, sample_count=int(count), sample_interval_s=float(interval_s))


def _balance_sample_selection(
    samples: list[dict[str, Any]],
    *,
    sample_count: int,
    sample_interval_s: float,
    escalation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    valid_entries = [
        (idx, float(sample["balance_usd"]))
        for idx, sample in enumerate(samples)
        if sample.get("status") == "OK" and sample.get("balance_usd") is not None
    ]
    valid = [balance for _, balance in valid_entries]
    valid_sorted = sorted(valid)
    consistent_pair_tolerance_usd = max(0.0, float(num(os.getenv("WALLET_COPY_BALANCE_CONSISTENT_PAIR_TOLERANCE_USD", "0.05"), 0.05)))
    consistent_pair: list[float] = []
    closest_pair_gap: float | None = None
    if len(valid_entries) >= 2:
        consecutive_pairs = [
            (abs(valid_entries[idx + 1][1] - valid_entries[idx][1]), valid_entries[idx][0], valid_entries[idx][1], valid_entries[idx + 1][1])
            for idx in range(len(valid_entries) - 1)
        ]
        closest_pair_gap = min(pair[0] for pair in consecutive_pairs)
        consistent_pairs = [pair for pair in consecutive_pairs if pair[0] <= consistent_pair_tolerance_usd]
        if consistent_pairs:
            _, _, left, right = consistent_pairs[-1]
            consistent_pair = [left, right]
    if consistent_pair:
        selected = sum(consistent_pair) / len(consistent_pair)
        selection_method = "consistent_pair"
    elif valid_sorted:
        selected = valid_sorted[len(valid_sorted) // 2]
        selection_method = "median_valid_samples"
    else:
        selected = -1.0
        selection_method = "no_valid_samples"
    if valid:
        status = "OK"
        reason = ""
    else:
        status = "UNAVAILABLE"
        reason = next((str(sample.get("reason") or "") for sample in samples if sample.get("reason")), "no_valid_balance_samples")
    return {
        "status": status,
        "reason": reason,
        "samples": samples,
        "valid_sample_count": len(valid),
        "sample_count": int(sample_count),
        "sample_interval_s": float(sample_interval_s),
        "strategy": "consistent_pair_then_median_valid_balance_samples",
        "selection_method": selection_method,
        "consistent_pair_tolerance_usd": round(consistent_pair_tolerance_usd, 6),
        "consistent_pair_gap_usd": round(float(closest_pair_gap), 6) if closest_pair_gap is not None else None,
        "consistent_pair_balance_usd": [round(value, 6) for value in consistent_pair],
        "selected_balance_usd": round(selected, 6) if selected >= 0 else None,
        "escalation": escalation or {"triggered": False},
    }


def chain_reconciliation(
    *,
    baseline_usd: float,
    canonical_pnl_usd: float,
    unresolved_open_cost_usd: float = 0.0,
    unresolved_max_payout_usd: float = 0.0,
    positions_path: str | Path = "data/wallet_copy_live_positions.json",
    balance_sample_count: int | None = None,
    balance_sample_interval_s: float | None = None,
    unavailable_resample_count: int | None = None,
    unavailable_resample_interval_s: float | None = None,
    mismatch_resample_count: int | None = None,
    mismatch_resample_interval_s: float | None = None,
) -> dict[str, Any]:
    config = Config()
    sample_count, sample_interval_s = _balance_sample_config(balance_sample_count, balance_sample_interval_s)
    balance_sampling = _fetch_live_balance_samples(config, count=sample_count, interval_s=sample_interval_s)
    position_value = fetch_live_position_value()
    if position_value.get("status") != "LIVE_MARK":
        position_value = {
            **position_value,
            "cache_fallback": estimate_cached_open_position_value(positions_path),
        }
    open_value = float(position_value.get("open_position_value_usd") or 0.0)
    expected = float(baseline_usd) + float(canonical_pnl_usd)
    unresolved_open_cost = float(unresolved_open_cost_usd)
    unresolved_max_payout = float(unresolved_max_payout_usd)
    tolerance = 2.0
    expected_cash = expected - unresolved_open_cost
    lower_bound = expected - tolerance
    upper_bound = expected + unresolved_max_payout + tolerance
    cash_lower_bound = -tolerance
    cash_upper_bound = unresolved_max_payout + tolerance

    def evaluate(sampling: dict[str, Any]) -> tuple[str, float, float | None, float | None, float | None, float | None]:
        selected_balance = float(sampling.get("selected_balance_usd") or -1.0)
        live_mark_account_value = (
            selected_balance + open_value if selected_balance >= 0 and position_value.get("status") == "LIVE_MARK" else None
        )
        selected_account_value = selected_balance + unresolved_open_cost if selected_balance >= 0 else None
        selected_delta = (selected_account_value - expected) if selected_account_value is not None else None
        selected_cash_delta = (selected_balance - expected_cash) if selected_balance >= 0 else None
        selected_cash_ok = (
            selected_cash_delta is not None and cash_lower_bound <= selected_cash_delta <= cash_upper_bound
        )
        selected_account_ok = (
            selected_account_value is not None and lower_bound <= selected_account_value <= upper_bound
        )
        if selected_account_value is None:
            selected_status = "UNAVAILABLE"
        elif selected_cash_ok and selected_account_ok:
            selected_status = "PASS"
        else:
            selected_status = "MISMATCH"
        return selected_status, selected_balance, selected_account_value, selected_delta, selected_cash_delta, live_mark_account_value

    status, balance, account_value, delta, cash_delta, live_mark_account_value = evaluate(balance_sampling)
    if status == "UNAVAILABLE":
        retry_count = max(
            0,
            min(
                int(
                    num(
                        os.getenv("WALLET_COPY_BALANCE_UNAVAILABLE_RESAMPLE_COUNT", "1")
                        if unavailable_resample_count is None
                        else unavailable_resample_count,
                        1,
                    )
                ),
                2,
            ),
        )
        retry_interval_s = max(
            0.0,
            float(
                num(
                    os.getenv("WALLET_COPY_BALANCE_UNAVAILABLE_RESAMPLE_INTERVAL_S", "45")
                    if unavailable_resample_interval_s is None
                    else unavailable_resample_interval_s,
                    45.0,
                )
            ),
        )
        if retry_count > 0:
            retry_sampling = _fetch_live_balance_samples(config, count=retry_count, interval_s=retry_interval_s)
            combined_samples = list(balance_sampling.get("samples") or [])
            for sample in retry_sampling.get("samples") or []:
                if isinstance(sample, dict):
                    combined_samples.append({**sample, "idx": len(combined_samples), "unavailable_retry_sample": True})
            balance_sampling = _balance_sample_selection(
                combined_samples,
                sample_count=len(combined_samples),
                sample_interval_s=retry_interval_s,
                escalation={
                    "triggered": True,
                    "reason": "unavailable_balance_fetch",
                    "initial_status": status,
                    "initial_selected_balance_usd": balance,
                    "resample_count": retry_count,
                    "resample_interval_s": retry_interval_s,
                },
            )
            status, balance, account_value, delta, cash_delta, live_mark_account_value = evaluate(balance_sampling)
    pair_gap = balance_sampling.get("consistent_pair_gap_usd")
    pair_tolerance = float(balance_sampling.get("consistent_pair_tolerance_usd") or 0.0)
    samples_disagree = pair_gap is not None and float(pair_gap) > pair_tolerance
    if status == "MISMATCH" or samples_disagree:
        slow_count = max(
            0,
            min(
                int(
                    num(
                        os.getenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_COUNT", "2")
                        if mismatch_resample_count is None
                        else mismatch_resample_count,
                        2,
                    )
                ),
                3,
            ),
        )
        slow_interval_s = max(
            0.0,
            float(
                num(
                    os.getenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S", "25")
                    if mismatch_resample_interval_s is None
                    else mismatch_resample_interval_s,
                    25.0,
                )
            ),
        )
        if slow_count > 0:
            slow_sampling = _fetch_live_balance_samples(config, count=slow_count, interval_s=slow_interval_s)
            combined_samples = list(balance_sampling.get("samples") or [])
            for sample in slow_sampling.get("samples") or []:
                if isinstance(sample, dict):
                    combined_samples.append({**sample, "idx": len(combined_samples), "escalation_sample": True})
            balance_sampling = _balance_sample_selection(
                combined_samples,
                sample_count=len(combined_samples),
                sample_interval_s=sample_interval_s,
                escalation={
                    "triggered": True,
                    "reason": "would_be_mismatch" if status == "MISMATCH" else "fast_samples_disagreed_beyond_tolerance",
                    "initial_status": status,
                    "initial_selected_balance_usd": balance,
                    "resample_count": slow_count,
                    "resample_interval_s": slow_interval_s,
                },
            )
            status, balance, account_value, delta, cash_delta, live_mark_account_value = evaluate(balance_sampling)
    balance_status = str(balance_sampling.get("status") or "UNAVAILABLE")
    balance_reason = str(balance_sampling.get("reason") or "")
    return {
        "status": status,
        "status_basis": "cash_identity_with_unresolved_bounds_v2",
        "account_value_basis": "live_cash_balance_plus_unresolved_open_cost",
        "baseline_usd": round(float(baseline_usd), 6),
        "canonical_pnl_usd": round(float(canonical_pnl_usd), 6),
        "expected_value_usd": round(expected, 6),
        "expected_cash_identity_usd": round(expected_cash, 6),
        "unresolved_open_cost_usd": round(unresolved_open_cost, 6),
        "unresolved_position_value_bounds_usd": [0.0, round(unresolved_max_payout, 6)],
        "live_cash_balance_usd": round(balance, 6) if balance >= 0 else None,
        "cash_delta_vs_expected_identity_usd": round(cash_delta, 6) if cash_delta is not None else None,
        "balance_status": balance_status,
        "balance_reason": balance_reason,
        "balance_sampling": balance_sampling,
        "open_position_value": position_value,
        "account_value_usd": round(account_value, 6) if account_value is not None else None,
        "live_mark_account_value_usd": round(live_mark_account_value, 6) if live_mark_account_value is not None else None,
        "valuation_basis_adjustment_usd": round(open_value - unresolved_open_cost, 6),
        "point_in_time_adjustments": {
            "basis_unification_usd": round(open_value - unresolved_open_cost, 6),
            "pending_redemption_usd": 0.0,
            "in_flight_fill_usd": 0.0,
            "unjoined_actual_gap_usd": 0.0,
            "adjusted_delta_vs_expected_usd": round(delta, 6) if delta is not None else None,
            "status_basis": "cash_plus_canonical_open_cost; live_open_mark_is_diagnostic",
        },
        "delta_vs_expected_usd": round(delta, 6) if delta is not None else None,
        "tolerance_usd": tolerance,
    }
