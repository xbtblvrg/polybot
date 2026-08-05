"""Actionable copy-execution diagnostics for wallet-copy paper evidence.

This module turns paper rejections and lifecycle misses into concrete next
corrections. It deliberately stays advisory: it never relaxes live gates and it
never creates orders. The goal is to keep the development loop moving from
failed copy evidence to measurable execution improvements.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import re
from typing import Any

from src.wallet_copy.models import num
from src.wallet_copy.status import ANALYZE, CORRECTION, PASS, active_status_from_blockers


@dataclass(frozen=True)
class CopyTacticsConfig:
    min_order_usd: float = 1.0
    micro_aggregation_window_s: float = 1.0
    aggressive_slippage_cap_bps: float = 750.0
    max_retry_slices: int = 4
    require_position_delta_reconciliation: bool = True

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _rounded(value: Any, default: float = 0.0) -> float:
    return round(num(value, default), 6)


def _optional_rounded(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _ranked_counts(counts: dict[str, int], *, limit: int = 5) -> list[dict[str, Any]]:
    return [
        {"id": key, "count": int(value)}
        for key, value in sorted(counts.items(), key=lambda item: (-int(item[1]), item[0]))[:limit]
    ]


def _fill_estimate(order: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(_as_dict(order.get("source_intent")).get("fill_estimate"))


def _required_slippage_bps(source_price: float, best_ask: float) -> float | None:
    if source_price <= 0 or best_ask <= 0:
        return None
    return round(max(0.0, (best_ask / source_price - 1.0) * 10_000.0), 6)


def _event_key(order: dict[str, Any]) -> str:
    intent = _as_dict(order.get("source_intent"))
    return str(intent.get("source_event_id") or order.get("intent_id") or order.get("order_id") or "unknown")


def _copy_reject_taxonomy(counts: dict[str, int]) -> dict[str, Any]:
    """Group low-level correction ids into repair lanes without changing gates."""

    taxonomy_map = {
        "hotlane_latency_reduction": "stale_or_latency",
        "book_closed_or_no_liquidity_analysis": "no_liquidity",
        "depth_slicing_or_copy_size_reduction": "depth_or_size",
        "aggressive_limit_with_measured_slippage_cap": "recoverable_slippage",
        "skip_or_delay_high_slippage_copy": "adverse_slippage",
        "micro_aggregate_below_min_order": "micro_or_min_order",
        "micro_aggregate_sub_min_filled_copy": "micro_or_min_order",
        "position_delta_reconciliation": "position_lifecycle",
        "inspect_fill_model_rejection": "unknown_fill_reject",
    }
    taxonomy_counts: Counter[str] = Counter()
    unknown_ids: Counter[str] = Counter()
    for correction_id, count in counts.items():
        count = int(count)
        if count <= 0:
            continue
        category = taxonomy_map.get(str(correction_id))
        if category:
            taxonomy_counts[category] += count
        else:
            taxonomy_counts["unknown"] += count
            unknown_ids[str(correction_id)] += count

    if not taxonomy_counts:
        dominant = None
        recommended_lane = "collect_current_poll_copyability_reject_taxonomy"
    else:
        category_priority = {
            "stale_or_latency": 0,
            "no_liquidity": 1,
            "depth_or_size": 2,
            "recoverable_slippage": 3,
            "adverse_slippage": 4,
            "micro_or_min_order": 5,
            "position_lifecycle": 6,
            "unknown_fill_reject": 7,
            "unknown": 8,
        }
        dominant = sorted(
            taxonomy_counts.items(),
            key=lambda item: (-int(item[1]), category_priority.get(item[0], 99), item[0]),
        )[0][0]
        lane_by_category = {
            "stale_or_latency": "reduce_source_latency_before_gate_changes",
            "no_liquidity": "verify_source_trade_book_timing_before_copy",
            "depth_or_size": "test_depth_slicing_or_smaller_wallet_fraction_in_paper",
            "recoverable_slippage": "paper_test_measured_aggressive_limit_with_pnl_attribution",
            "adverse_slippage": "keep_live_blocked_and_shadow_test_slippage_pnl",
            "micro_or_min_order": "paper_test_micro_aggregation_without_live_admission",
            "position_lifecycle": "reconcile_position_delta_lifecycle_before_live",
            "unknown_fill_reject": "inspect_fill_model_reject_details",
            "unknown": "inspect_unclassified_copy_rejects",
        }
        recommended_lane = lane_by_category.get(dominant, "inspect_unclassified_copy_rejects")

    return {
        "status": PASS if not taxonomy_counts else CORRECTION,
        "role": "paper_only_copy_reject_taxonomy_not_live_admission",
        "counts": dict(sorted(taxonomy_counts.items())),
        "dominant_category": dominant,
        "recommended_repair_lane": recommended_lane,
        "top_raw_correction_ids": _ranked_counts(counts),
        "unknown_correction_ids": dict(sorted(unknown_ids.items())),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _is_buy_order(order: dict[str, Any]) -> bool:
    intent = _as_dict(order.get("source_intent"))
    action = str(order.get("action") or intent.get("action") or "").upper()
    if action:
        return action == "BUY"
    side = str(order.get("side") or intent.get("side") or "").upper()
    return side in {"BUY", "YES", "NO"}


def _aggregation_key(order: dict[str, Any]) -> str:
    intent = _as_dict(order.get("source_intent"))
    token_id = str(intent.get("token_id") or order.get("token_id") or "")
    return "|".join(
        [
            str(order.get("source_wallet") or "").lower(),
            str(order.get("condition_id") or ""),
            token_id,
            str(order.get("side") or "BUY").upper(),
        ]
    )


def _base_correction(order: dict[str, Any], fill: dict[str, Any]) -> dict[str, Any]:
    details = _as_dict(fill.get("reject_details"))
    book = _as_dict(fill.get("book"))
    source_price = num(details.get("source_price"), num(book.get("source_price"), num(order.get("limit_price"))))
    best_ask = num(details.get("best_ask"), num(book.get("best_ask")))
    fill_blockers = [str(item) for item in _as_list(fill.get("blockers")) if str(item)]
    return {
        "source_event_id": _event_key(order),
        "order_id": order.get("order_id"),
        "intent_id": order.get("intent_id"),
        "source_wallet": str(order.get("source_wallet") or "").lower(),
        "wallet_name": order.get("wallet_name"),
        "condition_id": order.get("condition_id"),
        "market_slug": order.get("market_slug"),
        "outcome": order.get("outcome"),
        "reject_reason": fill.get("reject_reason") or "unknown_rejection",
        "reject_stage": fill.get("reject_stage"),
        "fill_source": fill.get("source"),
        "requested_size_usd": _rounded(fill.get("requested_size_usd"), num(order.get("requested_size_usd"))),
        "source_price": _rounded(source_price),
        "best_ask": _rounded(best_ask) if best_ask > 0 else None,
        "fill_ratio": _rounded(details.get("fill_ratio"), num(fill.get("fill_ratio"))),
        "fillable_usd": _rounded(details.get("fillable_usd"), num(book.get("fillable_usd"))),
        "blocking_reason": details.get("blocking_reason") or book.get("blocking_reason"),
        "required_slippage_bps": _required_slippage_bps(source_price, best_ask),
        "fill_blockers": fill_blockers,
        "event_age_s": _optional_rounded(details.get("event_age_s")),
        "event_age_at_source_fetch_start_s": _optional_rounded(
            details.get("event_age_at_source_fetch_start_s")
            or details.get("age_at_wallet_fetch_start_s")
        ),
        "event_age_over_cap_s": _optional_rounded(details.get("event_age_over_cap_s")),
        "max_event_age_s": _optional_rounded(details.get("max_event_age_s")),
        "source_fetch_duration_s": _optional_rounded(details.get("source_fetch_duration_s")),
        "wallet_fetch_duration_s": _optional_rounded(details.get("wallet_fetch_duration_s")),
    }


def _filled_sub_min_correction(order: dict[str, Any], config: CopyTacticsConfig) -> dict[str, Any]:
    fill = _fill_estimate(order)
    requested = num(fill.get("requested_size_usd"), num(order.get("requested_size_usd")))
    return {
        "source_event_id": _event_key(order),
        "order_id": order.get("order_id"),
        "intent_id": order.get("intent_id"),
        "source_wallet": str(order.get("source_wallet") or "").lower(),
        "wallet_name": order.get("wallet_name"),
        "condition_id": order.get("condition_id"),
        "market_slug": order.get("market_slug"),
        "outcome": order.get("outcome"),
        "fill_source": fill.get("source"),
        "requested_size_usd": _rounded(requested),
        "minimum_order_usd": float(config.min_order_usd),
        "aggregation_key": _aggregation_key(order),
        "correction_id": "micro_aggregate_sub_min_filled_copy",
        "tactic_class": "micro_overcopy_research_only",
        "status": CORRECTION,
        "recommended_next_step": (
            "treat filled sub-minimum paper BUYs as aggregation candidates before live readiness, "
            "then replay them as batched CopyIntents with CLOB fillability evidence"
        ),
        "recommended_config": {
            "aggregation_key": "source_wallet|condition_id|token_id|side",
            "aggregation_window_s": config.micro_aggregation_window_s,
            "minimum_batch_usd": config.min_order_usd,
        },
        "external_patterns_applied": ["micro_trade_aggregation_buffer"],
    }


def _rejected_order_correction(order: dict[str, Any], config: CopyTacticsConfig) -> dict[str, Any]:
    fill = _fill_estimate(order)
    base = _base_correction(order, fill)
    reason_text = " ".join(
        str(item).lower()
        for item in [
            base.get("reject_reason"),
            base.get("reject_stage"),
            base.get("blocking_reason"),
            fill.get("source"),
            *_as_list(fill.get("blockers")),
        ]
        if item
    )
    requested = num(base.get("requested_size_usd"))
    required_slippage = base.get("required_slippage_bps")

    correction_id = "inspect_fill_model_rejection"
    status = ANALYZE
    next_step = (
        "inspect fill_model reject_details, copyability decision, and CLOB book summary for this intent"
    )
    external_patterns = ["error_taxonomy_and_processed_flag_discipline"]
    recommended_config: dict[str, Any] = {}
    secondary_correction_ids: list[str] = []
    secondary_external_patterns: list[str] = []

    if 0.0 < requested < float(config.min_order_usd):
        correction_id = "micro_aggregate_below_min_order"
        status = CORRECTION
        next_step = (
            "aggregate same wallet/condition/token/side micro BUYs for a short paper-only window "
            "and compare CLOB fillability before creating CopyIntent batches"
        )
        external_patterns = ["micro_trade_aggregation_buffer"]
        recommended_config = {
            "aggregation_key": "source_wallet|condition_id|token_id|side",
            "aggregation_window_s": config.micro_aggregation_window_s,
            "minimum_batch_usd": config.min_order_usd,
        }
        tactic_class = "micro_overcopy_research_only"
    elif "missing_clob_book_evidence" in reason_text or "no_book_rejected" in reason_text:
        correction_id = "hotlane_clob_book_refresh"
        status = CORRECTION
        next_step = (
            "fetch CLOB book immediately on fresh wallet event, persist book hash, and rerun strict paper fill"
        )
        external_patterns = ["data_api_activity_plus_clob_book_evidence"]
        tactic_class = "delay_recheck_candidate"
    elif "api_latency_above" in reason_text:
        correction_id = "hotlane_latency_reduction"
        status = CORRECTION
        next_step = "reduce wallet poll scope, raise hot-lane cadence, and separate historical age from live fetch latency"
        external_patterns = ["sub_second_wallet_polling_guardrail"]
        tactic_class = "delay_recheck_candidate"
    elif "no_ask" in reason_text or "no_liquidity" in reason_text:
        correction_id = "book_closed_or_no_liquidity_analysis"
        status = ANALYZE
        next_step = "keep order out of live readiness and measure whether source wallet filled before our book snapshot"
        external_patterns = ["book_state_at_source_trade_verification"]
        tactic_class = "delay_recheck_candidate"
    elif "price_above_slippage" in reason_text or "above_slippage_cap" in reason_text:
        if required_slippage is not None and required_slippage <= float(config.aggressive_slippage_cap_bps):
            correction_id = "aggressive_limit_with_measured_slippage_cap"
            status = CORRECTION
            next_step = (
                "paper-test a more aggressive IOC/FOK limit at best ask with explicit slippage cap and PnL attribution"
            )
            recommended_config = {
                "max_test_slippage_bps": config.aggressive_slippage_cap_bps,
                "observed_required_slippage_bps": required_slippage,
            }
            tactic_class = "aggressive_cap_recoverable"
        else:
            correction_id = "skip_or_delay_high_slippage_copy"
            status = ANALYZE
            next_step = "classify as adverse-selection risk unless later book snapshots show repeatable recovery"
            recommended_config = {
                "observed_required_slippage_bps": required_slippage,
                "cap_bps": config.aggressive_slippage_cap_bps,
            }
            tactic_class = "adverse_selection_skip"
        external_patterns = ["best_ask_slippage_cap_before_submit"]
    elif "insufficient_depth" in reason_text or "fill_ratio" in reason_text or "depth" in reason_text:
        correction_id = "depth_slicing_or_copy_size_reduction"
        status = CORRECTION
        next_step = (
            "split paper CopyIntent into book-depth slices or reduce wallet fraction until min fill ratio passes"
        )
        external_patterns = ["depth_guard_and_order_slicing"]
        recommended_config = {
            "max_retry_slices": config.max_retry_slices,
            "target_fill_ratio": 0.999,
        }
        tactic_class = "delay_recheck_candidate"
    else:
        tactic_class = "delay_recheck_candidate"

    def add_secondary(correction: str, pattern: str | None = None) -> None:
        if correction != correction_id and correction not in secondary_correction_ids:
            secondary_correction_ids.append(correction)
        if pattern and pattern not in secondary_external_patterns and pattern not in external_patterns:
            secondary_external_patterns.append(pattern)

    if "price_above_slippage" in reason_text or "above_slippage_cap" in reason_text:
        if required_slippage is not None and required_slippage <= float(config.aggressive_slippage_cap_bps):
            add_secondary("aggressive_limit_with_measured_slippage_cap", "best_ask_slippage_cap_before_submit")
        else:
            add_secondary("skip_or_delay_high_slippage_copy", "best_ask_slippage_cap_before_submit")
    if "no_ask" in reason_text or "no_liquidity" in reason_text:
        add_secondary("book_closed_or_no_liquidity_analysis", "book_state_at_source_trade_verification")
    if "insufficient_depth" in reason_text or "fill_ratio" in reason_text or "depth" in reason_text:
        add_secondary("depth_slicing_or_copy_size_reduction", "depth_guard_and_order_slicing")

    return {
        **base,
        "correction_id": correction_id,
        "secondary_correction_ids": secondary_correction_ids,
        "all_correction_ids": [correction_id, *secondary_correction_ids],
        "tactic_class": tactic_class,
        "status": status,
        "recommended_next_step": next_step,
        "recommended_config": recommended_config,
        "external_patterns_applied": [*external_patterns, *secondary_external_patterns],
    }


def _lifecycle_correction(row: dict[str, Any], config: CopyTacticsConfig) -> dict[str, Any] | None:
    if not config.require_position_delta_reconciliation:
        return None
    reduction = _as_dict(row.get("position_reduction"))
    status = str(reduction.get("status") or "")
    if status not in {"NO_MATCHING_POSITION", "NO_PAIRED_POSITION"}:
        return None
    return {
        "source_event_id": row.get("source_event_id"),
        "source_wallet": str(row.get("source_wallet") or "").lower(),
        "wallet_name": row.get("wallet_name"),
        "condition_id": row.get("condition_id"),
        "outcome": row.get("outcome"),
        "wallet_action": row.get("wallet_action"),
        "position_reduction_status": status,
        "correction_id": "position_delta_reconciliation",
        "status": CORRECTION,
        "recommended_next_step": (
            "reconcile Data API positions delta against copied paper lots before treating lifecycle coverage as complete"
        ),
        "recommended_config": {
            "position_delta_source": "data_api_positions",
            "reconciliation_key": "source_wallet|condition_id|outcome",
        },
        "external_patterns_applied": ["positions_delta_reconciliation"],
    }


def build_copy_execution_corrections(
    orders: list[dict[str, Any]] | None,
    lifecycle_events: list[dict[str, Any]] | None,
    *,
    config: CopyTacticsConfig | None = None,
) -> dict[str, Any]:
    """Summarize next corrections from paper order and lifecycle evidence."""

    cfg = config or CopyTacticsConfig()
    safe_orders = [row for row in (orders or []) if isinstance(row, dict)]
    safe_lifecycle = [row for row in (lifecycle_events or []) if isinstance(row, dict)]
    rejected = [row for row in safe_orders if str(row.get("final_status") or row.get("status") or "") == "REJECTED"]
    sub_min_filled = [
        row
        for row in safe_orders
        if str(row.get("final_status") or row.get("status") or "") == "FILLED"
        and _is_buy_order(row)
        and 0.0 < num(_fill_estimate(row).get("requested_size_usd"), num(row.get("requested_size_usd"))) < float(cfg.min_order_usd)
    ]
    order_corrections = [_rejected_order_correction(row, cfg) for row in rejected]
    sub_min_corrections = [_filled_sub_min_correction(row, cfg) for row in sub_min_filled]
    lifecycle_corrections = [
        correction
        for correction in (_lifecycle_correction(row, cfg) for row in safe_lifecycle)
        if correction is not None
    ]
    corrections = order_corrections + sub_min_corrections + lifecycle_corrections
    correction_counts = Counter(str(row.get("correction_id") or "unknown") for row in corrections)
    secondary_correction_counts = Counter(
        str(correction_id)
        for row in corrections
        for correction_id in _as_list(row.get("secondary_correction_ids"))
        if correction_id
    )
    all_correction_counts = Counter(correction_counts)
    all_correction_counts.update(secondary_correction_counts)
    reason_counts = Counter(str(row.get("reject_reason") or row.get("position_reduction_status") or "unknown") for row in corrections)
    status_counts = Counter(str(row.get("status") or ANALYZE) for row in corrections)
    sub_min_notional_by_key: dict[str, float] = {}
    for row in sub_min_filled:
        key = _aggregation_key(row)
        requested = num(_fill_estimate(row).get("requested_size_usd"), num(row.get("requested_size_usd")))
        sub_min_notional_by_key[key] = round(float(sub_min_notional_by_key.get(key, 0.0)) + requested, 6)
    blockers = sorted(correction_counts) + sorted(reason_counts)
    if not corrections:
        status = PASS
    elif status_counts.get(CORRECTION):
        status = CORRECTION
    else:
        status = active_status_from_blockers(blockers, default=ANALYZE)
    next_steps: list[str] = []
    for correction in corrections:
        step = str(correction.get("recommended_next_step") or "")
        if step and step not in next_steps:
            next_steps.append(step)
    patterns: list[str] = []
    for correction in corrections:
        for pattern in _as_list(correction.get("external_patterns_applied")):
            pattern_text = str(pattern)
            if pattern_text and pattern_text not in patterns:
                patterns.append(pattern_text)

    return {
        "status": status,
        "role": "paper_only_copy_execution_correction_advisory",
        "source": "paper_order_fill_estimates_and_lifecycle_reductions",
        "paper_only": True,
        "live_orders_allowed": False,
        "config": cfg.asdict(),
        "orders_seen": len(safe_orders),
        "rejected_orders": len(rejected),
        "sub_min_filled_buy_copy_events": len(sub_min_filled),
        "sub_min_filled_notional_by_aggregation_key": dict(sorted(sub_min_notional_by_key.items())),
        "lifecycle_events_seen": len(safe_lifecycle),
        "lifecycle_misses": len(lifecycle_corrections),
        "correction_count": len(corrections),
        "correction_counts": dict(sorted(correction_counts.items())),
        "secondary_correction_counts": dict(sorted(secondary_correction_counts.items())),
        "all_correction_counts": dict(sorted(all_correction_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "external_patterns_applied": patterns,
        "recommended_next_steps": next_steps,
        "corrections": corrections[:50],
    }


def _int_counter(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, int] = {}
    for key, count in value.items():
        try:
            out[str(key)] = int(count or 0)
        except (TypeError, ValueError):
            continue
    return out


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(100.0 * int(numerator) / int(denominator), 6)


def _build_size_reduction_probe(correction_report: dict[str, Any], *, min_fill_ratio: float = 0.999) -> dict[str, Any]:
    """Measure whether strict rejects are recoverable by smaller paper copy size.

    The probe is intentionally advisory. Reducing size is no longer exact wallet
    copy for the rejected event, so this can guide paper experiments but cannot
    satisfy all-order live readiness by itself.
    """

    corrections = [
        row
        for row in _as_list(correction_report.get("corrections"))
        if isinstance(row, dict)
    ]
    depth_rows = []
    actionable_rows = []
    unfillable_rows = 0
    zero_liquidity_rows = 0
    total_requested = 0.0
    actionable_requested = 0.0
    actionable_max_copy = 0.0
    for row in corrections:
        correction_ids = {
            str(item)
            for item in [
                row.get("correction_id"),
                *_as_list(row.get("secondary_correction_ids")),
                *_as_list(row.get("all_correction_ids")),
            ]
            if item
        }
        reject_text = " ".join(
            str(item).lower()
            for item in [
                row.get("reject_reason"),
                row.get("blocking_reason"),
                *_as_list(row.get("secondary_correction_ids")),
                *_as_list(row.get("all_correction_ids")),
            ]
            if item
        )
        requested = num(row.get("requested_size_usd"))
        fillable = num(row.get("fillable_usd"))
        fill_ratio = num(row.get("fill_ratio"))
        is_zero_liquidity = (
            "book_closed_or_no_liquidity_analysis" in correction_ids
            or "no_ask" in reject_text
            or "no_liquidity" in reject_text
        ) and fillable <= 0
        has_depth_signal = (
            "depth_slicing_or_copy_size_reduction" in correction_ids
            or str(row.get("reject_reason") or "") == "clob_fill_ratio_below_minimum"
            or str(row.get("blocking_reason") or "") == "insufficient_depth_within_slippage_cap"
            or (requested > 0 and 0.0 <= fill_ratio < float(min_fill_ratio))
        )
        if not has_depth_signal:
            continue
        total_requested += requested
        max_copy_usd = min(requested, fillable) if requested > 0 and fillable > 0 else 0.0
        size_multiplier = round(max_copy_usd / requested, 6) if requested > 0 and max_copy_usd > 0 else 0.0
        probe_row = {
            "source_event_id": row.get("source_event_id"),
            "order_id": row.get("order_id"),
            "intent_id": row.get("intent_id"),
            "source_wallet": row.get("source_wallet"),
            "condition_id": row.get("condition_id"),
            "market_slug": row.get("market_slug"),
            "outcome": row.get("outcome"),
            "requested_size_usd": _rounded(requested),
            "fillable_usd": _rounded(fillable),
            "strict_fill_ratio": _rounded(fill_ratio),
            "max_paper_copy_size_usd": _rounded(max_copy_usd),
            "recommended_wallet_fraction_multiplier": size_multiplier,
            "would_pass_size_reduction_probe": bool(0.0 < max_copy_usd < requested),
            "reject_reason": row.get("reject_reason"),
            "blocking_reason": row.get("blocking_reason"),
        }
        depth_rows.append(probe_row)
        if is_zero_liquidity:
            zero_liquidity_rows += 1
        if probe_row["would_pass_size_reduction_probe"]:
            actionable_rows.append(probe_row)
            actionable_requested += requested
            actionable_max_copy += max_copy_usd
        else:
            unfillable_rows += 1

    blockers: list[str] = []
    if not depth_rows:
        blockers.append("no_depth_or_size_rejects_measured")
    if depth_rows and not actionable_rows:
        blockers.append("no_reject_has_positive_fillable_depth_for_size_reduction")
    if actionable_rows:
        blockers.append("size_reduction_changes_exact_copy_not_live_admission")
    if unfillable_rows:
        blockers.append("some_depth_rejects_have_zero_fillable_depth")
    if zero_liquidity_rows:
        blockers.append("zero_liquidity_requires_book_timing_probe")
    if depth_rows and zero_liquidity_rows == len(depth_rows) and not actionable_rows:
        blockers.append("all_depth_rejects_zero_liquidity")

    min_multiplier = (
        min(float(row["recommended_wallet_fraction_multiplier"]) for row in actionable_rows)
        if actionable_rows
        else None
    )
    max_multiplier = (
        max(float(row["recommended_wallet_fraction_multiplier"]) for row in actionable_rows)
        if actionable_rows
        else None
    )
    return {
        "status": CORRECTION if actionable_rows else ANALYZE,
        "role": "paper_only_depth_size_reduction_probe_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "min_fill_ratio": float(min_fill_ratio),
        "depth_or_size_reject_events": len(depth_rows),
        "actionable_size_reduction_events": len(actionable_rows),
        "unfillable_depth_events": unfillable_rows,
        "zero_liquidity_reject_events": zero_liquidity_rows,
        "total_requested_usd": _rounded(total_requested),
        "actionable_requested_usd": _rounded(actionable_requested),
        "actionable_max_copy_size_usd": _rounded(actionable_max_copy),
        "min_recommended_wallet_fraction_multiplier": min_multiplier,
        "max_recommended_wallet_fraction_multiplier": max_multiplier,
        "blockers": blockers,
        "recommended_next_step": (
            "paper-replay these events with reduced wallet_fraction/depth-capped sizing and resolved PnL "
            "attribution; do not promote until ordinary all-order CopyIntent truth later has zero rejects"
            if actionable_rows
            else (
                "verify source trade book timing and no-liquidity rows before changing copy size"
                if zero_liquidity_rows
                else "separate zero-liquidity timing from depth sizing before changing copy policy"
            )
        ),
        "sample_events": depth_rows[:20],
    }


def _reject_bucket_diagnostics(
    correction_report: dict[str, Any],
    *,
    micro_batch_summary: dict[str, Any],
) -> dict[str, Any]:
    """Split rejected BUY evidence into diagnostic buckets before tactic tuning."""

    corrections = [
        row
        for row in _as_list(correction_report.get("corrections"))
        if isinstance(row, dict)
    ]
    bucket_counts: Counter[str] = Counter()
    bucket_samples: dict[str, list[dict[str, Any]]] = {}
    depth_repair_actionable_events = 0
    depth_repair_zero_fillable_events = 0

    def add_bucket(bucket: str, row: dict[str, Any]) -> None:
        bucket_counts[bucket] += 1
        samples = bucket_samples.setdefault(bucket, [])
        if len(samples) >= 10:
            return
        samples.append(
            {
                "source_event_id": row.get("source_event_id"),
                "order_id": row.get("order_id"),
                "intent_id": row.get("intent_id"),
                "source_wallet": row.get("source_wallet"),
                "condition_id": row.get("condition_id"),
                "market_slug": row.get("market_slug"),
                "reject_reason": row.get("reject_reason"),
                "blocking_reason": row.get("blocking_reason"),
                "correction_id": row.get("correction_id"),
                "secondary_correction_ids": _as_list(row.get("secondary_correction_ids")),
                "requested_size_usd": row.get("requested_size_usd"),
                "fillable_usd": row.get("fillable_usd"),
                "fill_ratio": row.get("fill_ratio"),
                "required_slippage_bps": row.get("required_slippage_bps"),
                "fill_blockers": _as_list(row.get("fill_blockers")),
                "event_age_s": row.get("event_age_s"),
                "event_age_at_source_fetch_start_s": row.get("event_age_at_source_fetch_start_s"),
                "event_age_over_cap_s": row.get("event_age_over_cap_s"),
                "source_fetch_duration_s": row.get("source_fetch_duration_s"),
            }
        )

    for row in corrections:
        correction_ids = {
            str(item)
            for item in [
                row.get("correction_id"),
                *_as_list(row.get("secondary_correction_ids")),
                *_as_list(row.get("all_correction_ids")),
            ]
            if item
        }
        text = " ".join(
            str(item).lower()
            for item in [
                row.get("reject_reason"),
                row.get("reject_stage"),
                row.get("blocking_reason"),
                row.get("fill_source"),
                *_as_list(row.get("fill_blockers")),
                *correction_ids,
            ]
            if item
        )
        is_stale = (
            "event_age_above_cap" in text
            or "source_feed_pre_fetch_stale" in text
            or num(row.get("event_age_over_cap_s")) > 0
        )
        is_route_degraded = (
            "source_route" in text
            or "route_degraded" in text
            or "source_feed_pre_fetch_stale" in text
            or "fetch_duration_above_cap" in text
        )
        is_no_liquidity = (
            "book_closed_or_no_liquidity_analysis" in correction_ids
            or "no_ask" in text
            or "no_liquidity" in text
        )
        is_depth = (
            "depth_slicing_or_copy_size_reduction" in correction_ids
            or "insufficient_depth" in text
            or "fill_ratio" in text
            or "depth" in text
        )
        is_slippage = (
            "skip_or_delay_high_slippage_copy" in correction_ids
            or "aggressive_limit_with_measured_slippage_cap" in correction_ids
            or "price_above_slippage" in text
            or "above_slippage_cap" in text
        )
        is_overcopy = (
            "micro_aggregate_below_min_order" in correction_ids
            or "micro_aggregate_sub_min_filled_copy" in correction_ids
        )

        if is_stale:
            add_bucket("stale_source_event", row)
        if is_route_degraded:
            add_bucket("route_degraded_or_prefetch_stale", row)
        if is_no_liquidity:
            add_bucket("no_liquidity_unfillable", row)
        if is_depth:
            add_bucket("depth_unfillable", row)
            if num(row.get("fillable_usd")) > 0:
                depth_repair_actionable_events += 1
            else:
                depth_repair_zero_fillable_events += 1
        if is_slippage:
            add_bucket("slippage_unfillable", row)
        if is_overcopy:
            add_bucket("overcopy_required", row)

    if num(micro_batch_summary.get("overcopy_usd")) > 0 and not bucket_counts.get("overcopy_required"):
        bucket_counts["overcopy_required"] += 1
    if micro_batch_summary.get("requires_overcopy") and "overcopy_required" not in bucket_samples:
        bucket_samples["overcopy_required"] = bucket_samples.get("overcopy_required", [])[:10]

    ordered_counts = dict(sorted(bucket_counts.items()))
    dominant_bucket = None
    raw_dominant_bucket = None
    if bucket_counts:
        bucket_priority = {
            "route_degraded_or_prefetch_stale": 0,
            "stale_source_event": 1,
            "no_liquidity_unfillable": 2,
            "depth_unfillable": 3,
            "slippage_unfillable": 4,
            "overcopy_required": 5,
        }
        raw_dominant_bucket = sorted(
            bucket_counts.items(),
            key=lambda item: (-int(item[1]), bucket_priority.get(item[0], 99), item[0]),
        )[0][0]
        dominant_bucket = raw_dominant_bucket
        if raw_dominant_bucket == "depth_unfillable" and depth_repair_actionable_events <= 0:
            alternative_counts = Counter(bucket_counts)
            alternative_counts.pop("depth_unfillable", None)
            if alternative_counts:
                dominant_bucket = sorted(
                    alternative_counts.items(),
                    key=lambda item: (-int(item[1]), bucket_priority.get(item[0], 99), item[0]),
                )[0][0]
    recommended_next_by_bucket = {
        "stale_source_event": "measure source freshness and event age before changing copy tactics",
        "route_degraded_or_prefetch_stale": "stabilize or fail-fast the source route before tactic tuning",
        "no_liquidity_unfillable": "verify source trade book timing before size or slippage tactic changes",
        "depth_unfillable": "paper-test depth slicing or smaller wallet fraction with resolved PnL attribution",
        "slippage_unfillable": "shadow-test slippage recovery in paper and keep live blocked",
        "overcopy_required": "treat micro aggregation as paper-only until exact CopyIntent truth passes without overcopy",
    }
    return {
        "status": CORRECTION if bucket_counts else PASS,
        "role": "paper_only_reject_bucket_diagnostics_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "counts": ordered_counts,
        "dominant_bucket": dominant_bucket,
        "raw_dominant_bucket": raw_dominant_bucket,
        "depth_repair_actionable_events": depth_repair_actionable_events,
        "depth_repair_zero_fillable_events": depth_repair_zero_fillable_events,
        "top_buckets": _ranked_counts(ordered_counts),
        "sample_events_by_bucket": {key: bucket_samples[key] for key in sorted(bucket_samples)},
        "recommended_next_measurement": recommended_next_by_bucket.get(
            dominant_bucket,
            "collect rejected BUY evidence before tactic tuning",
        ),
        "live_admission_note": (
            "diagnostic buckets do not relax CopyIntent, source-route, CLOB-fill, fallback, reject, miss, "
            "or live lifecycle gates"
        ),
    }


def _bucket_repair_lane(bucket: str | None) -> tuple[str | None, str | None]:
    if bucket == "route_degraded_or_prefetch_stale":
        return "stale_or_latency", "reduce_source_latency_before_gate_changes"
    if bucket == "stale_source_event":
        return "stale_or_latency", "reduce_source_latency_before_gate_changes"
    if bucket == "no_liquidity_unfillable":
        return "no_liquidity", "verify_source_trade_book_timing_before_copy"
    if bucket == "slippage_unfillable":
        return "adverse_slippage", "keep_live_blocked_and_shadow_test_slippage_pnl"
    if bucket == "overcopy_required":
        return "micro_or_min_order", "diagnose_exact_no_overcopy_micro_copy_before_batching"
    return None, None


def _dominant_paper_tactic_lane(
    *,
    reject_taxonomy: dict[str, Any],
    reject_bucket_diagnostics: dict[str, Any],
    flow_control_plan: dict[str, Any],
    micro_min_order_actionability: dict[str, Any],
    size_reduction_probe: dict[str, Any],
    flow_control_shadow_measurement_plan: dict[str, Any],
) -> dict[str, Any]:
    """Persist the next paper-only tactic lane as a compact routing object."""

    dominant_category = str(reject_taxonomy.get("dominant_category") or "")
    dominant_bucket = str(reject_bucket_diagnostics.get("dominant_bucket") or "")
    recommended_lane = str(reject_taxonomy.get("recommended_repair_lane") or "")
    skip_current_event_replay = bool(flow_control_plan.get("skip_current_event_replay"))
    allow_aggressive_profile_replay = bool(flow_control_plan.get("allow_aggressive_profile_replay", True))
    micro_actionable = bool(micro_min_order_actionability.get("micro_batch_actionable_for_paper_replay"))
    size_actionable = int(size_reduction_probe.get("actionable_size_reduction_events") or 0)
    shadow_profile_id = flow_control_shadow_measurement_plan.get("profile_id")

    if dominant_category == "micro_or_min_order":
        lane_id = "micro_min_order_exact_no_overcopy"
        next_measurement = (
            "measure exact-no-overcopy micro grouping and source-wallet fraction floors before any replay"
            if not micro_actionable
            else "paper-replay exact micro grouping with resolved PnL attribution"
        )
        actionable = micro_actionable
    elif dominant_category == "depth_or_size" or dominant_bucket == "depth_unfillable":
        lane_id = "depth_sized_copy_probe"
        next_measurement = (
            "paper-replay depth-sized copies with resolved PnL attribution"
            if size_actionable
            else "separate zero-liquidity timing from positive-depth sizing before changing copy size"
        )
        actionable = size_actionable > 0
    elif dominant_category == "no_liquidity" or dominant_bucket == "no_liquidity_unfillable":
        lane_id = "source_trade_book_timing"
        next_measurement = "verify source trade book timing before size or slippage tactic changes"
        actionable = False
    elif dominant_category == "stale_or_latency" or dominant_bucket in {
        "route_degraded_or_prefetch_stale",
        "stale_source_event",
    }:
        lane_id = "source_freshness_or_route"
        next_measurement = "refresh current-poll source freshness or rotate wallet/window before tactic replay"
        actionable = False
    elif dominant_category == "adverse_slippage" or dominant_bucket == "slippage_unfillable":
        lane_id = "slippage_shadow_pnl"
        next_measurement = (
            "queue blocked aggressive profile for paper-only shadow PnL attribution"
            if shadow_profile_id
            else "shadow-test slippage recovery in paper and keep live blocked"
        )
        actionable = bool(shadow_profile_id) and not skip_current_event_replay
    else:
        lane_id = "collect_reject_taxonomy"
        next_measurement = "collect rejected BUY evidence before tactic tuning"
        actionable = False

    blockers: list[str] = []
    if skip_current_event_replay:
        blockers.append("flow_control_skips_current_event_replay")
    if not allow_aggressive_profile_replay:
        blockers.append("flow_control_blocks_aggressive_profile_replay")
    if dominant_category == "micro_or_min_order" and not micro_actionable:
        blockers.append("micro_min_order_probe_not_actionable_for_repair")
    if lane_id == "depth_sized_copy_probe" and not size_actionable:
        blockers.append("no_reject_has_positive_fillable_depth_for_size_reduction")

    return {
        "status": CORRECTION if blockers or not actionable else PASS,
        "role": "paper_only_dominant_tactic_lane_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "lane_id": lane_id,
        "dominant_reject_category": dominant_category or None,
        "dominant_reject_bucket": dominant_bucket or None,
        "recommended_repair_lane": recommended_lane or None,
        "actionable_for_paper_replay": actionable,
        "skip_current_event_replay": skip_current_event_replay,
        "allow_aggressive_profile_replay": allow_aggressive_profile_replay,
        "shadow_profile_id": shadow_profile_id,
        "next_measurement": next_measurement,
        "blockers": blockers,
    }


def _reject_flow_control_plan(
    reject_bucket_diagnostics: dict[str, Any],
    *,
    size_reduction_probe: dict[str, Any],
) -> dict[str, Any]:
    counts = _int_counter(_as_dict(reject_bucket_diagnostics.get("counts")))
    dominant_bucket = str(reject_bucket_diagnostics.get("dominant_bucket") or "")
    stale_or_route_rows = int(counts.get("route_degraded_or_prefetch_stale") or 0) + int(
        counts.get("stale_source_event") or 0
    )
    no_liquidity_rows = int(counts.get("no_liquidity_unfillable") or 0)
    depth_rows = int(counts.get("depth_unfillable") or 0)
    actionable_depth_rows = int(size_reduction_probe.get("actionable_size_reduction_events") or 0)
    zero_liquidity_rows = int(size_reduction_probe.get("zero_liquidity_reject_events") or 0)

    blockers: list[str] = []
    next_action = "strict_copy_or_selected_paper_tactic"
    skip_current_event_replay = False
    allow_aggressive_profile_replay = True
    delay_recheck_required = False

    if stale_or_route_rows:
        next_action = "fail_fast_refresh_current_poll_or_rotate_wallet_window_before_tactic_replay"
        blockers.append("source_freshness_or_route_not_current")
        skip_current_event_replay = True
        allow_aggressive_profile_replay = False
        delay_recheck_required = True
    if no_liquidity_rows and (stale_or_route_rows or zero_liquidity_rows):
        next_action = "delay_recheck_or_rotate_no_liquidity_events_before_size_or_slippage_tuning"
        blockers.append("no_liquidity_requires_book_timing_recheck")
        skip_current_event_replay = True
        allow_aggressive_profile_replay = False
        delay_recheck_required = True
    elif no_liquidity_rows:
        next_action = "verify_source_trade_book_timing_before_copy"
        blockers.append("no_liquidity_requires_book_timing_probe")
        allow_aggressive_profile_replay = False
        delay_recheck_required = True
    elif depth_rows and actionable_depth_rows <= 0:
        next_action = "skip_unfillable_depth_events_until_positive_book_depth_appears"
        blockers.append("depth_rows_have_no_positive_fillable_liquidity")
        skip_current_event_replay = True
        allow_aggressive_profile_replay = False
    elif depth_rows and actionable_depth_rows > 0:
        next_action = "paper_depth_sized_replay_with_pnl_attribution"
        blockers.append("depth_sizing_changes_exact_copy_truth")

    status = PASS if not blockers else CORRECTION
    return {
        "status": status,
        "role": "paper_only_reject_flow_control_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "dominant_bucket": dominant_bucket or None,
        "counts": dict(sorted(counts.items())),
        "next_action": next_action,
        "skip_current_event_replay": skip_current_event_replay,
        "allow_aggressive_profile_replay": allow_aggressive_profile_replay,
        "delay_recheck_required": delay_recheck_required,
        "blockers": blockers,
        "verify": (
            "python3 scripts/run_wallet_live_tracker.py --registry "
            "data/research/wallet_copy_active_hotlane_registry.json --iterations 1 --limit 20 "
            "--pages 1 --enable-clob-books --strict-mirror-coverage"
        ),
    }


def _profile_meta(profile_id: str) -> dict[str, Any]:
    text = str(profile_id)
    research_only = "micro_batch_min" in text or "research" in text
    slippage_bps: float | None = None
    match = re.search(r"(\d+(?:\.\d+)?)bps", text)
    if match:
        slippage_bps = float(match.group(1))
    if text == "strict_current":
        kind = "strict"
        slippage_bps = 0.0
    elif "aggressive" in text:
        kind = "aggressive_best_ask"
    elif "micro" in text:
        kind = "micro_batch"
    else:
        kind = "unknown"
    return {
        "kind": kind,
        "research_only": research_only,
        "slippage_bps": slippage_bps,
    }


def build_copy_execution_tactic_plan(
    orders: list[dict[str, Any]] | None,
    *,
    tactic_profile_status_counts: dict[str, Any] | None = None,
    tactic_profile_pass_events: dict[str, Any] | None = None,
    micro_batch_probe: dict[str, Any] | None = None,
    correction_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a paper-only execution improvement plan from measured tactic evidence.

    This is deliberately not an admission gate. It converts all-order strict-copy
    failures into an auditable paper plan that says which already-measured tactic
    improved fillability, what cost/coverage tradeoff it introduced, and what
    verification must pass before the tactic can be considered for the normal
    CopyIntent lifecycle.
    """

    safe_orders = [row for row in (orders or []) if isinstance(row, dict)]
    buy_orders = [row for row in safe_orders if _is_buy_order(row)]
    filled_orders = [row for row in buy_orders if str(row.get("final_status") or row.get("status") or "") == "FILLED"]
    rejected_orders = [
        row for row in buy_orders if str(row.get("final_status") or row.get("status") or "") == "REJECTED"
    ]
    total_buy_orders = len(buy_orders)
    strict_filled = len(filled_orders)
    strict_rejected = len(rejected_orders)
    strict_fill_rate_pct = _rate(strict_filled, total_buy_orders)
    strict_copy_all_current_buys_filled = bool(
        total_buy_orders > 0 and strict_filled == total_buy_orders and strict_rejected == 0
    )
    strict_unfilled_buy_orders = max(0, total_buy_orders - strict_filled - strict_rejected)
    tactic_repair_required = not strict_copy_all_current_buys_filled

    profile_status_counts = {
        str(profile_id): _int_counter(counts)
        for profile_id, counts in (tactic_profile_status_counts or {}).items()
        if isinstance(counts, dict)
    }
    profile_pass_events = _int_counter(tactic_profile_pass_events or {})
    profile_summaries: dict[str, dict[str, Any]] = {}
    for profile_id, counts in profile_status_counts.items():
        pass_events = int(profile_pass_events.get(profile_id, counts.get(PASS, 0)))
        watch_events = int(counts.get("WATCH", 0))
        blocked_events = int(counts.get("BLOCKED", 0))
        meta = _profile_meta(profile_id)
        profile_summaries[profile_id] = {
            **meta,
            "pass_events": pass_events,
            "watch_events": watch_events,
            "blocked_events": blocked_events,
            "status_counts": counts,
            "fill_rate_pct_of_current_buys": _rate(pass_events, total_buy_orders),
            "incremental_pass_events_vs_strict": max(0, pass_events - strict_filled),
        }

    actionable_profiles: list[str] = []
    best_profile_id: str | None = None
    if profile_summaries:
        actionable_profiles = [
            item
            for item, summary in profile_summaries.items()
            if not summary.get("research_only")
            and item != "strict_current"
            and int(summary.get("incremental_pass_events_vs_strict") or 0) > 0
        ]
        profile_pool = actionable_profiles or list(profile_summaries)
        best_profile_id = sorted(
            profile_pool,
            key=lambda item: (
                -int(profile_summaries[item].get("incremental_pass_events_vs_strict") or 0),
                -int(profile_summaries[item].get("pass_events") or 0),
                float(profile_summaries[item].get("slippage_bps") or 999999.0),
                1 if profile_summaries[item].get("research_only") else 0,
                item,
            ),
        )[0]
    best_profile = profile_summaries.get(best_profile_id or "", {})
    best_profile_incremental = int(best_profile.get("incremental_pass_events_vs_strict") or 0)

    micro = micro_batch_probe if isinstance(micro_batch_probe, dict) else {}
    micro_covered = int(micro.get("covered_child_events") or 0)
    micro_filled = int(micro.get("filled_child_events") or 0)
    micro_rejected = int(micro.get("rejected_child_events") or 0)
    micro_improvement = max(0, micro_filled - strict_filled)
    micro_overcopy_usd = _rounded(micro.get("overcopy_usd"), 0.0)
    micro_exact_total_usd = _rounded(micro.get("exact_total_usd"), 0.0)
    micro_submit_total_usd = _rounded(micro.get("submit_total_usd"), 0.0)
    micro_overcopy_pct_of_exact_total = (
        _rounded(micro.get("overcopy_pct_of_exact_total"), 0.0)
        if micro.get("overcopy_pct_of_exact_total") is not None
        else (round(100.0 * micro_overcopy_usd / micro_exact_total_usd, 6) if micro_exact_total_usd > 0 else None)
    )
    micro_overcopy_per_filled_child_event_usd = (
        _rounded(micro.get("overcopy_per_filled_child_event_usd"), 0.0)
        if micro.get("overcopy_per_filled_child_event_usd") is not None
        else (round(micro_overcopy_usd / micro_filled, 6) if micro_filled > 0 else None)
    )
    micro_batch_actionability_blockers: list[str] = []
    if micro:
        if micro_improvement <= 0:
            micro_batch_actionability_blockers.append("micro_batch_does_not_improve_strict_fill_count")
        if micro_rejected:
            micro_batch_actionability_blockers.append("micro_batch_rejected_child_events_present")
        if micro_overcopy_usd > 0:
            micro_batch_actionability_blockers.append("micro_batch_requires_overcopy_not_live_admission")
    micro_improves_strict_fill_count = micro_improvement > 0
    micro_actionable_for_paper_replay = bool(micro and micro_filled > 0 and micro_improves_strict_fill_count)
    micro_research_only_not_live_admission = bool(
        micro and (micro_overcopy_usd > 0 or micro_rejected > 0 or not micro_improves_strict_fill_count)
    )
    micro_summary = {
        "status": micro.get("status"),
        "source_buy_intents": int(micro.get("source_buy_intents") or 0),
        "probe_groups": int(micro.get("probe_groups") or 0),
        "pass_groups": int(micro.get("pass_groups") or 0),
        "rejected_groups": int(micro.get("rejected_groups") or 0),
        "covered_child_events": micro_covered,
        "filled_child_events": micro_filled,
        "rejected_child_events": micro_rejected,
        "child_fill_rate_pct": _rate(micro_filled, micro_covered),
        "incremental_filled_child_events_vs_strict": micro_improvement,
        "improves_strict_fill_count": micro_improves_strict_fill_count,
        "actionable_for_paper_replay": micro_actionable_for_paper_replay,
        "research_only_not_live_admission": micro_research_only_not_live_admission,
        "exact_total_usd": micro_exact_total_usd,
        "submit_total_usd": micro_submit_total_usd,
        "overcopy_usd": micro_overcopy_usd,
        "requires_overcopy": micro_overcopy_usd > 0,
        "overcopy_pct_of_exact_total": micro_overcopy_pct_of_exact_total,
        "overcopy_per_filled_child_event_usd": micro_overcopy_per_filled_child_event_usd,
        "source_wallet_fraction_floor_summary": micro.get("source_wallet_fraction_floor_summary")
        if isinstance(micro.get("source_wallet_fraction_floor_summary"), dict)
        else {},
        "actionability_blockers": list(micro_batch_actionability_blockers),
        "blockers": _as_list(micro.get("blockers")),
    }
    measured_micro_exact_no_overcopy = (
        micro.get("micro_batch_exact_no_overcopy")
        if isinstance(micro.get("micro_batch_exact_no_overcopy"), dict)
        else {}
    )
    measured_micro_min_order_research = (
        micro.get("micro_batch_min_order_research")
        if isinstance(micro.get("micro_batch_min_order_research"), dict)
        else {}
    )
    micro_exact_no_overcopy = {
        "status": PASS if micro and micro_rejected == 0 and micro_overcopy_usd <= 0 and micro_filled > 0 else ANALYZE,
        "filled_child_events": micro_filled if micro_overcopy_usd <= 0 else 0,
        "rejected_child_events": micro_rejected,
        "overcopy_usd": micro_overcopy_usd,
        "research_only": False,
        **measured_micro_exact_no_overcopy,
    }
    micro_min_order_research = {
        "status": PASS if micro and micro_filled > 0 else ANALYZE,
        "filled_child_events": micro_filled,
        "rejected_child_events": micro_rejected,
        "overcopy_usd": micro_overcopy_usd,
        "overcopy_pct_of_exact_total": micro_overcopy_pct_of_exact_total,
        "overcopy_per_filled_child_event_usd": micro_overcopy_per_filled_child_event_usd,
        "incremental_filled_child_events_vs_strict": micro_improvement,
        "improves_strict_fill_count": micro_improves_strict_fill_count,
        "actionable_for_paper_replay": micro_actionable_for_paper_replay,
        "research_only_not_live_admission": micro_research_only_not_live_admission,
        "requires_overcopy": micro_overcopy_usd > 0,
        "actionability_blockers": list(micro_batch_actionability_blockers),
        "research_only": True,
        **measured_micro_min_order_research,
    }
    exact_no_overcopy_probe_groups = int(micro_exact_no_overcopy.get("probe_groups") or 0)
    exact_no_overcopy_pass_groups = int(micro_exact_no_overcopy.get("pass_groups") or 0)
    exact_no_overcopy_covered = int(micro_exact_no_overcopy.get("covered_child_events") or 0)
    exact_no_overcopy_filled = int(micro_exact_no_overcopy.get("filled_child_events") or 0)
    exact_no_overcopy_rejected = int(micro_exact_no_overcopy.get("rejected_child_events") or 0)
    exact_no_overcopy_source_intents = int(micro_summary.get("source_buy_intents") or 0)
    exact_no_overcopy_coverage_gap = max(0, exact_no_overcopy_source_intents - exact_no_overcopy_covered)
    exact_no_overcopy_incremental = max(0, exact_no_overcopy_filled - strict_filled)
    exact_no_overcopy_blockers: list[str] = []
    if micro and exact_no_overcopy_probe_groups <= 0:
        exact_no_overcopy_blockers.append("no_exact_no_overcopy_probe_groups")
    if micro and exact_no_overcopy_pass_groups <= 0:
        exact_no_overcopy_blockers.append("no_exact_no_overcopy_pass_groups")
    if micro and exact_no_overcopy_incremental <= 0:
        exact_no_overcopy_blockers.append("exact_no_overcopy_does_not_improve_strict_fill_count")
    if exact_no_overcopy_rejected > 0:
        exact_no_overcopy_blockers.append("exact_no_overcopy_rejected_child_events_present")
    if exact_no_overcopy_coverage_gap > 0:
        exact_no_overcopy_blockers.append("exact_no_overcopy_coverage_gap_child_events")
    micro_exact_no_overcopy_diagnostics = {
        "status": PASS
        if micro
        and str(micro_exact_no_overcopy.get("status") or "").upper() == PASS
        and not exact_no_overcopy_blockers
        else (CORRECTION if exact_no_overcopy_blockers else ANALYZE),
        "role": "paper_only_exact_no_overcopy_micro_gap_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "source_buy_intents": exact_no_overcopy_source_intents,
        "probe_groups": exact_no_overcopy_probe_groups,
        "pass_groups": exact_no_overcopy_pass_groups,
        "covered_child_events": exact_no_overcopy_covered,
        "coverage_gap_child_events": exact_no_overcopy_coverage_gap,
        "filled_child_events": exact_no_overcopy_filled,
        "rejected_child_events": exact_no_overcopy_rejected,
        "incremental_filled_child_events_vs_strict": exact_no_overcopy_incremental,
        "improves_strict_fill_count": exact_no_overcopy_incremental > 0,
        "source_wallet_fraction_floor_summary": micro_exact_no_overcopy.get("source_wallet_fraction_floor_summary")
        if isinstance(micro_exact_no_overcopy.get("source_wallet_fraction_floor_summary"), dict)
        else {},
        "missing_exact_no_overcopy_reason": micro_exact_no_overcopy.get("missing_exact_no_overcopy_reason"),
        "blockers": exact_no_overcopy_blockers,
        "recommended_next_measurement": (
            "collect exact-no-overcopy micro groups that improve strict fill count without rejected child events "
            "before treating micro/min-order batching as a paper replay repair"
        ),
    }

    correction = correction_report if isinstance(correction_report, dict) else {}
    correction_counts = _int_counter(correction.get("correction_counts") or {})
    secondary_correction_counts = _int_counter(correction.get("secondary_correction_counts") or {})
    all_correction_counts = _int_counter(correction.get("all_correction_counts") or correction_counts)
    size_reduction_probe = _build_size_reduction_probe(correction)
    size_reduction_actionable = int(size_reduction_probe.get("actionable_size_reduction_events") or 0)

    blockers: list[str] = []
    if total_buy_orders == 0:
        blockers.append("no_current_buy_orders_for_tactic_plan")
    if strict_rejected:
        blockers.append("strict_all_order_rejected_orders_present")
    if tactic_repair_required and profile_summaries and best_profile_incremental <= 0:
        blockers.append("no_tactic_profile_improves_strict_fill_count")
    if tactic_repair_required and micro and micro_improvement <= 0:
        blockers.append("micro_batch_does_not_improve_strict_fill_count")
    if tactic_repair_required and micro_overcopy_usd > 0:
        blockers.append("micro_batch_requires_overcopy_not_live_admission")
    if tactic_repair_required and micro_rejected:
        blockers.append("micro_batch_rejected_child_events_present")
    if tactic_repair_required and not profile_summaries and not micro:
        blockers.append("no_measured_tactic_evidence")
    if all_correction_counts.get("skip_or_delay_high_slippage_copy"):
        blockers.append("high_slippage_copy_not_live_admissible")
    if size_reduction_actionable:
        blockers.append("depth_size_reduction_requires_pnl_validation")
        blockers.append("size_reduction_changes_exact_copy_not_live_admission")

    recommended_tactic = "strict_copy"
    recommended_reason = (
        "strict CopyIntent execution fills every current BUY; remaining live-readiness work is "
        "source freshness, copyability truth, resolved paper PnL, or parity proof"
        if strict_copy_all_current_buys_filled
        else "strict all-order paper proof has no rejected current BUYs yet still needs fill coverage"
    )
    if strict_rejected:
        if micro_improvement > 0 and micro_filled >= int(best_profile.get("pass_events") or 0):
            recommended_tactic = "paper_micro_batch_replay_with_pnl_attribution"
            recommended_reason = "micro-batch probe improved measured child fill count versus strict all-order copy"
        elif best_profile_incremental > 0 and best_profile_id and not best_profile.get("research_only"):
            recommended_tactic = f"paper_profile_replay:{best_profile_id}"
            recommended_reason = "CLOB tactic profile improved measured pass count versus strict all-order copy"
        elif size_reduction_actionable > 0:
            recommended_tactic = "paper_depth_sized_replay_with_pnl_attribution"
            recommended_reason = (
                "depth probe found rejected CopyIntents with positive fillable CLOB depth at a smaller "
                "wallet fraction; validate only in paper because sizing changes exact-copy truth"
            )
        elif best_profile_incremental > 0 and best_profile_id:
            recommended_tactic = "paper_research_only_profile_not_promotable"
            recommended_reason = "best measured profile requires research-only sizing or overcopy and cannot fix exact-copy proof"
        else:
            recommended_tactic = "paper_diagnose_unfillable_or_stale_copy_events"
            recommended_reason = "no measured tactic improved strict all-order copyability"

    reject_taxonomy = _copy_reject_taxonomy(all_correction_counts)
    reject_bucket_diagnostics = _reject_bucket_diagnostics(
        correction,
        micro_batch_summary=micro_summary,
    )
    flow_control_plan = _reject_flow_control_plan(
        reject_bucket_diagnostics,
        size_reduction_probe=size_reduction_probe,
    )
    pre_flow_control_recommended_tactic = recommended_tactic
    pre_flow_control_recommended_reason = recommended_reason
    flow_blocks_aggressive_profile_replay = bool(
        strict_rejected
        and recommended_tactic.startswith("paper_profile_replay:")
        and not bool(flow_control_plan.get("allow_aggressive_profile_replay", True))
    )
    flow_control_shadow_measurement_plan: dict[str, Any] = {}
    if flow_blocks_aggressive_profile_replay:
        flow_control_shadow_measurement_plan = {
            "status": CORRECTION if best_profile_incremental > 0 else ANALYZE,
            "role": "paper_only_flow_control_shadow_measurement_not_live_admission",
            "paper_only": True,
            "live_orders_allowed": False,
            "profile_id": best_profile_id,
            "profile": best_profile,
            "blocked_recommended_tactic": pre_flow_control_recommended_tactic,
            "blocked_recommended_reason": pre_flow_control_recommended_reason,
            "strict_buy_orders": total_buy_orders,
            "strict_filled_buy_orders": strict_filled,
            "strict_rejected_buy_orders": strict_rejected,
            "best_profile_pass_events": int(best_profile.get("pass_events") or 0),
            "incremental_pass_events_vs_strict": best_profile_incremental,
            "flow_control_next_action": flow_control_plan.get("next_action"),
            "flow_control_dominant_bucket": flow_control_plan.get("dominant_bucket"),
            "flow_control_blockers": list(_as_list(flow_control_plan.get("blockers"))),
            "skip_current_event_replay": bool(flow_control_plan.get("skip_current_event_replay")),
            "allow_aggressive_profile_replay": bool(
                flow_control_plan.get("allow_aggressive_profile_replay", True)
            ),
            "blockers": sorted(
                set(
                    [
                        "blocked_by_flow_control_not_live_admission",
                        "paper_shadow_profile_needs_resolved_pnl_attribution",
                        *_as_list(flow_control_plan.get("blockers")),
                    ]
                )
            ),
            "next_measurement": (
                "persist the blocked profile as a paper-only shadow PnL measurement queue and compare resolved "
                "WR/ROI/PnL against strict CopyIntent truth; keep live blocked until fresh current-poll strict "
                "truth later has zero rejects, misses, and fallback fills"
            ),
            "verification_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900",
        }
        recommended_tactic = "paper_diagnose_unfillable_or_stale_copy_events"
        recommended_reason = (
            "reject flow-control requires "
            f"{flow_control_plan.get('next_action') or 'fresh reject measurement'} before aggressive profile replay"
        )
        blockers.append("flow_control_blocks_aggressive_profile_replay")
        blockers.append("flow_control_shadow_pnl_measurement_required")
    if strict_rejected and bool(flow_control_plan.get("skip_current_event_replay")):
        blockers.append("flow_control_skips_current_event_replay")
    if (
        reject_taxonomy.get("dominant_category") == "depth_or_size"
        and size_reduction_actionable <= 0
        and "no_reject_has_positive_fillable_depth_for_size_reduction"
        in _as_list(size_reduction_probe.get("blockers"))
    ):
        bucket_category, bucket_lane = _bucket_repair_lane(str(reject_bucket_diagnostics.get("dominant_bucket") or ""))
        if bucket_category and bucket_lane:
            reject_taxonomy = {
                **reject_taxonomy,
                "raw_dominant_category": reject_taxonomy.get("dominant_category"),
                "raw_recommended_repair_lane": reject_taxonomy.get("recommended_repair_lane"),
                "dominant_category": bucket_category,
                "recommended_repair_lane": bucket_lane,
                "dominant_category_override_reason": (
                    "depth_size_reduction_probe_found_no_positive_fillable_depth"
                ),
                "effective_reject_bucket": reject_bucket_diagnostics.get("dominant_bucket"),
            }
    micro_dominant_not_actionable = bool(
        micro
        and reject_taxonomy.get("dominant_category") == "micro_or_min_order"
        and not micro_actionable_for_paper_replay
    )
    micro_min_order_actionability = {
        "status": CORRECTION if micro_dominant_not_actionable else (PASS if micro_actionable_for_paper_replay else ANALYZE),
        "role": "paper_only_micro_min_order_actionability_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "dominant_reject_category_is_micro_or_min_order": reject_taxonomy.get("dominant_category") == "micro_or_min_order",
        "micro_batch_actionable_for_paper_replay": micro_actionable_for_paper_replay,
        "micro_batch_research_only_not_live_admission": micro_research_only_not_live_admission,
        "micro_batch_actionability_blockers": list(micro_batch_actionability_blockers),
        "micro_batch_exact_no_overcopy_status": micro_exact_no_overcopy.get("status"),
        "micro_batch_min_order_research_status": micro_min_order_research.get("status"),
        "micro_batch_exact_no_overcopy_diagnostics": micro_exact_no_overcopy_diagnostics,
        "source_wallet_fraction_floor_summary": micro_summary.get("source_wallet_fraction_floor_summary"),
        "micro_batch_exact_no_overcopy_source_wallet_fraction_floor_summary": micro_exact_no_overcopy.get(
            "source_wallet_fraction_floor_summary"
        ),
        "micro_batch_min_order_source_wallet_fraction_floor_summary": micro_min_order_research.get(
            "source_wallet_fraction_floor_summary"
        ),
        "recommended_next_measurement": (
            "measure exact-no-overcopy micro grouping and source wallet fraction floor; do not replay "
            "overcopying or rejected micro batches as a repair"
            if micro_dominant_not_actionable
            else (
                "paper-replay exact micro grouping with resolved PnL attribution before any live gate change"
                if micro_actionable_for_paper_replay
                else "collect a micro-batch probe before treating micro/min-order evidence as actionable"
            )
        ),
    }
    if micro_dominant_not_actionable:
        blockers.append("micro_min_order_probe_not_actionable_for_repair")
        reject_taxonomy = {
            **reject_taxonomy,
            "recommended_repair_lane": "diagnose_exact_no_overcopy_micro_copy_before_batching",
            "micro_batch_not_actionable": True,
            "micro_batch_actionability_blockers": list(micro_batch_actionability_blockers),
        }
    dominant_paper_tactic_lane = _dominant_paper_tactic_lane(
        reject_taxonomy=reject_taxonomy,
        reject_bucket_diagnostics=reject_bucket_diagnostics,
        flow_control_plan=flow_control_plan,
        micro_min_order_actionability=micro_min_order_actionability,
        size_reduction_probe=size_reduction_probe,
        flow_control_shadow_measurement_plan=flow_control_shadow_measurement_plan,
    )
    no_actionable_status = (
        CORRECTION
        if flow_control_shadow_measurement_plan
        else (ANALYZE if recommended_tactic == "paper_diagnose_unfillable_or_stale_copy_events" else PASS)
    )
    no_actionable_tactic_diagnostics = {
        "status": no_actionable_status,
        "role": "paper_only_no_actionable_tactic_gap_diagnostics",
        "strict_buy_orders": total_buy_orders,
        "strict_filled_buy_orders": strict_filled,
        "strict_rejected_buy_orders": strict_rejected,
        "strict_unfilled_buy_orders": strict_unfilled_buy_orders,
        "strict_copy_all_current_buys_filled": strict_copy_all_current_buys_filled,
        "tactic_repair_required": tactic_repair_required,
        "best_profile_id": best_profile_id,
        "best_profile_pass_events": int(best_profile.get("pass_events") or 0),
        "best_profile_incremental_pass_events_vs_strict": best_profile_incremental,
        "actionable_profile_ids": sorted(actionable_profiles),
        "micro_batch_filled_child_events": micro_filled,
        "micro_batch_rejected_child_events": micro_rejected,
        "micro_batch_incremental_filled_child_events_vs_strict": micro_improvement,
        "micro_batch_overcopy_usd": micro_overcopy_usd,
        "micro_batch_overcopy_pct_of_exact_total": micro_overcopy_pct_of_exact_total,
        "micro_batch_overcopy_per_filled_child_event_usd": micro_overcopy_per_filled_child_event_usd,
        "micro_batch_improves_strict_fill_count": micro_improves_strict_fill_count,
        "micro_batch_actionable_for_paper_replay": micro_actionable_for_paper_replay,
        "micro_batch_research_only_not_live_admission": micro_research_only_not_live_admission,
        "micro_batch_actionability_blockers": list(micro_batch_actionability_blockers),
        "micro_batch_exact_no_overcopy_diagnostics": micro_exact_no_overcopy_diagnostics,
        "top_correction_ids": _ranked_counts(correction_counts),
        "top_all_correction_ids": _ranked_counts(all_correction_counts),
        "top_secondary_correction_ids": _ranked_counts(secondary_correction_counts),
        "size_reduction_probe": size_reduction_probe,
        "micro_min_order_actionability": micro_min_order_actionability,
        "reject_bucket_diagnostics": reject_bucket_diagnostics,
        "flow_control_plan": flow_control_plan,
        "flow_control_shadow_measurement_plan": flow_control_shadow_measurement_plan,
        "dominant_paper_tactic_lane": dominant_paper_tactic_lane,
        "reject_taxonomy": reject_taxonomy,
        "dominant_reject_category": reject_taxonomy.get("dominant_category"),
        "recommended_repair_lane": reject_taxonomy.get("recommended_repair_lane"),
        "recommended_next_measurement": (
            (
                "refresh current-poll source freshness and copyability truth; do not chase "
                "micro/depth tactic replay while strict CopyIntent fills every current BUY"
            )
            if strict_copy_all_current_buys_filled
            else (
                (
                    "queue the blocked paper profile for shadow PnL attribution while flow-control keeps live "
                    "and current-event replay blocked"
                )
                if flow_control_shadow_measurement_plan
                else "split stale, no-liquidity, depth, and high-slippage rejects before changing live gates"
                if recommended_tactic == "paper_diagnose_unfillable_or_stale_copy_events"
                else "run selected paper tactic replay and compare resolved PnL before any live gate change"
            )
        ),
        "paper_only": True,
        "live_orders_allowed": False,
    }

    if total_buy_orders == 0:
        status = "NO_EVENTS"
    elif strict_copy_all_current_buys_filled:
        status = PASS
    elif micro_improvement > 0 or best_profile_incremental > 0 or size_reduction_actionable > 0:
        status = CORRECTION
    else:
        status = active_status_from_blockers(blockers, default=ANALYZE)

    verification_commands = [
        "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
        "python3 scripts/audit_wallet_copy_learning_logs.py --no-append-feedback-log",
        "python3 -m pytest -q tests/test_wallet_copy_core.py",
    ]
    if recommended_tactic.startswith("paper_") and recommended_tactic != "paper_diagnose_unfillable_or_stale_copy_events":
        verification_commands.insert(
            1,
            "python3 scripts/run_wallet_live_tracker.py --registry data/research/wallet_copy_active_hotlane_registry.json "
            "--iterations 1 --limit 20 --pages 1 --enable-clob-books --strict-mirror-coverage",
        )
    shadow_verify = str(flow_control_shadow_measurement_plan.get("verification_command") or "")
    if shadow_verify and shadow_verify not in verification_commands:
        verification_commands.insert(1, shadow_verify)

    return {
        "status": status,
        "role": "paper_only_all_order_execution_tactic_plan_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "strict_buy_orders": total_buy_orders,
        "strict_filled_buy_orders": strict_filled,
        "strict_rejected_buy_orders": strict_rejected,
        "strict_unfilled_buy_orders": strict_unfilled_buy_orders,
        "strict_copy_all_current_buys_filled": strict_copy_all_current_buys_filled,
        "tactic_repair_required": tactic_repair_required,
        "strict_fill_rate_pct": strict_fill_rate_pct,
        "profile_summaries": profile_summaries,
        "best_profile_id": best_profile_id,
        "best_profile": best_profile,
        "micro_batch_summary": micro_summary,
        "micro_batch_exact_no_overcopy": micro_exact_no_overcopy,
        "micro_batch_min_order_research": micro_min_order_research,
        "micro_batch_exact_no_overcopy_diagnostics": micro_exact_no_overcopy_diagnostics,
        "micro_min_order_actionability": micro_min_order_actionability,
        "size_reduction_probe": size_reduction_probe,
        "correction_counts": correction_counts,
        "secondary_correction_counts": secondary_correction_counts,
        "all_correction_counts": all_correction_counts,
        "reject_taxonomy": reject_taxonomy,
        "reject_bucket_diagnostics": reject_bucket_diagnostics,
        "flow_control_plan": flow_control_plan,
        "flow_control_shadow_measurement_plan": flow_control_shadow_measurement_plan,
        "dominant_paper_tactic_lane": dominant_paper_tactic_lane,
        "recommended_tactic": recommended_tactic,
        "recommended_reason": recommended_reason,
        "no_actionable_tactic_diagnostics": no_actionable_tactic_diagnostics,
        "blockers": sorted(set(blockers)),
        "live_admission_note": (
            "research_only_tactic_plan; live-ready requires a later ordinary CopyIntent lifecycle "
            "with non-seed CLOB fills, zero fallback, zero rejects, zero misses, and PnL attribution"
        ),
        "pnl_attribution_plan": {
            "required": True,
            "cost_delta_fields": ["strict_cost_usd", "tactic_cost_usd", "overcopy_usd", "effective_price_delta_bps"],
            "resolution_fields": ["resolved_orders", "tactic_roi_pct", "tactic_pnl_usd", "max_drawdown_usd"],
            "minimum_evidence": "one_week_or_hundreds_of_resolved_paper_markets_before_live_gate",
        },
        "verification_commands": verification_commands,
    }
