"""Event-level copy efficiency scoring for wallet-copy tracking.

This module links the source wallet move to our generated CopyIntent and the
paper order outcome. It deliberately measures execution quality separately from
strategy profitability: a losing wallet can still be copied perfectly, while a
profitable wallet is not live-admissible if our copy layer is slow or misses
fills.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from src.wallet_copy.models import CopyIntent, WalletEvent, num


DEFAULT_MAX_API_LATENCY_S = 10.0
DEFAULT_MAX_AVG_WORSE_SLIPPAGE_BPS = 500.0


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * max(0.0, min(100.0, pct)) / 100.0))
    return round(ordered[index], 6)


def _fill_estimate(order: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(order, dict):
        return {}
    source_intent = order.get("source_intent")
    if not isinstance(source_intent, dict):
        return {}
    fill = source_intent.get("fill_estimate")
    return fill if isinstance(fill, dict) else {}


def _tracking_evidence(intent: CopyIntent | None) -> dict[str, Any]:
    if intent is None or not isinstance(intent.metadata, dict):
        return {}
    evidence = intent.metadata.get("live_tracking_evidence")
    return evidence if isinstance(evidence, dict) else {}


def _detail_fallback(primary: Any, details: dict[str, Any], key: str) -> Any:
    return details.get(key) if primary is None and isinstance(details, dict) else primary


def _score_profit_policy_accepted(row: dict[str, Any]) -> bool:
    if row.get("profit_policy_accepted") is True:
        return True
    if row.get("profit_policy_accepted") is False:
        return False
    if str(row.get("filter_policy") or "") == "profit_policy":
        return False
    return True


def _counter(values: list[Any]) -> dict[str, int]:
    return dict(Counter(str(value) for value in values if value not in (None, "")))


def _age_bucket_counts(values: list[float]) -> dict[str, int]:
    buckets = {
        "le_1s": 0,
        "le_2s": 0,
        "le_5s": 0,
        "le_10s": 0,
        "le_30s": 0,
        "le_60s": 0,
        "le_300s": 0,
        "gt_300s": 0,
    }
    for value in values:
        if value <= 1.0:
            buckets["le_1s"] += 1
        elif value <= 2.0:
            buckets["le_2s"] += 1
        elif value <= 5.0:
            buckets["le_5s"] += 1
        elif value <= 10.0:
            buckets["le_10s"] += 1
        elif value <= 30.0:
            buckets["le_30s"] += 1
        elif value <= 60.0:
            buckets["le_60s"] += 1
        elif value <= 300.0:
            buckets["le_300s"] += 1
        else:
            buckets["gt_300s"] += 1
    return buckets


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            return ts / 1000.0
        if ts > 1e10:
            return ts / 1000.0
        return ts
    text = str(value).strip()
    if not text:
        return None
    try:
        return _parse_ts(float(text))
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _missed_reason(
    *,
    event: WalletEvent,
    intent: CopyIntent | None,
    order: dict[str, Any] | None,
    mirror_result: dict[str, Any],
    profit_filter_decision: tuple[bool, str] | None,
) -> str:
    action = event.action.upper()
    coverage_status = str(mirror_result.get("coverage_status") or "")
    if coverage_status == "FILTERED":
        if str(mirror_result.get("filter_policy") or "") == "copyability":
            return str(mirror_result.get("reason") or "copyability_filtered")
        if profit_filter_decision is not None and not profit_filter_decision[0]:
            return profit_filter_decision[1]
        return str(mirror_result.get("reason") or "filtered")
    if coverage_status == "VIOLATION":
        return str(mirror_result.get("reason") or "coverage_violation")
    if action != "BUY":
        return "lifecycle_mirrored" if coverage_status == "MIRRORED" else "lifecycle_not_mirrored"
    if intent is None:
        return "no_copy_intent"
    if not isinstance(order, dict):
        return "missing_paper_order"
    if str(order.get("final_status") or "") == "REJECTED":
        fill = _fill_estimate(order)
        blockers = fill.get("blockers")
        if isinstance(blockers, list) and blockers:
            return ",".join(str(item) for item in blockers)
        return "paper_order_rejected"
    return "copied"


def score_copy_event(
    event: WalletEvent,
    *,
    intent: CopyIntent | None,
    paper_order: dict[str, Any] | None,
    mirror_result: dict[str, Any] | None,
    copyability_decision: dict[str, Any] | None = None,
    profit_filter_decision: tuple[bool, str] | None = None,
) -> dict[str, Any]:
    """Score one source wallet event against our copy/paper lifecycle."""

    mirror = mirror_result if isinstance(mirror_result, dict) else {}
    action = event.action.upper()
    order = paper_order if isinstance(paper_order, dict) else None
    fill = _fill_estimate(order)
    evidence = _tracking_evidence(intent)
    wallet_api = evidence.get("wallet_api") if isinstance(evidence.get("wallet_api"), dict) else {}
    clob_book = evidence.get("clob_book") if isinstance(evidence.get("clob_book"), dict) else {}
    book_ts = _parse_ts(clob_book.get("book_timestamp") if isinstance(clob_book, dict) else None)
    clob_admission_relevant = clob_book.get("admission_relevant") if isinstance(clob_book, dict) else None
    source_price = max(0.0, float(event.price))
    fill_price = num(order.get("limit_price") if order else fill.get("effective_price"), 0.0)
    filled_shares = num(order.get("filled_shares") if order else None, 0.0)
    requested_shares = num(order.get("requested_shares") if order else (intent.shares if intent else None), 0.0)
    filled_usd = num(order.get("filled_size_usd") if order else None, 0.0)
    requested_usd = num(order.get("requested_size_usd") if order else (intent.copy_size_usd if intent else None), 0.0)
    fill_ratio = num(fill.get("fill_ratio"), 0.0)
    if fill_ratio <= 0.0 and requested_usd > 0:
        fill_ratio = min(1.0, filled_usd / requested_usd)
    share_fill_ratio = min(1.0, filled_shares / requested_shares) if requested_shares > 0 else 0.0
    price_delta = (fill_price - source_price) if source_price > 0 and fill_price > 0 else None
    slippage_bps = (price_delta / source_price * 10_000.0) if price_delta is not None else None
    event_age_s = event.age_s
    latency_s = event.api_latency_s if event.api_latency_s is not None else event_age_s
    coverage_status = str(mirror.get("coverage_status") or "UNKNOWN")
    mirror_copyability = (
        mirror.get("copyability_decision") if isinstance(mirror.get("copyability_decision"), dict) else {}
    )
    explicit_copyability = copyability_decision if isinstance(copyability_decision, dict) else {}
    copyability = mirror_copyability or explicit_copyability
    copyability_details = copyability.get("details") if isinstance(copyability.get("details"), dict) else {}
    clob_book_status = (
        clob_book.get("status")
        if isinstance(clob_book, dict) and clob_book.get("status") is not None
        else copyability_details.get("clob_book_status")
    )
    if clob_admission_relevant is None and action == "BUY":
        detail_age = copyability_details.get("event_age_s")
        detail_max_age = copyability_details.get("max_event_age_s")
        if detail_age is not None and detail_max_age is not None:
            clob_admission_relevant = float(detail_age) <= float(detail_max_age)
    final_status = str(order.get("final_status") or "") if order else ""
    if profit_filter_decision is None:
        profit_policy_accepted = True
        profit_policy_reason = "accepted"
    else:
        profit_policy_accepted = bool(profit_filter_decision[0])
        profit_policy_reason = str(profit_filter_decision[1] or "accepted")
    missed_reason = _missed_reason(
        event=event,
        intent=intent,
        order=order,
        mirror_result=mirror,
        profit_filter_decision=profit_filter_decision,
    )

    if coverage_status == "FILTERED":
        copy_status = "FILTERED"
    elif action in {"SELL", "MERGE", "REDEEM"}:
        copy_status = "LIFECYCLE_MIRRORED" if coverage_status == "MIRRORED" else "MISSED"
    elif final_status == "FILLED":
        copy_status = "COPIED_FILLED"
    elif final_status == "REJECTED":
        copy_status = "COPY_REJECTED"
    else:
        copy_status = "MISSED"

    fill_blockers = fill.get("blockers") if isinstance(fill.get("blockers"), list) else []
    reject_details = fill.get("reject_details") if isinstance(fill.get("reject_details"), dict) else {}
    reject_reason = fill.get("reject_reason") if final_status == "REJECTED" else None
    if final_status == "REJECTED" and not reject_reason:
        reject_reason = missed_reason

    return {
        "schema_version": 1,
        "source_event_id": event.event_id,
        "source_fingerprint": event.source_fingerprint,
        "source_wallet": event.source_wallet.lower(),
        "wallet_name": event.wallet_name,
        "wallet_action": action,
        "condition_id": event.condition_id,
        "market_slug": event.market_slug,
        "outcome": event.outcome,
        "token_id": event.token_id,
        "source_price": round(source_price, 6),
        "source_shares": round(float(event.size), 6),
        "source_usdc_size": round(float(event.usdc_size), 6),
        "source_event_ts": event.event_ts,
        "observed_ts": event.observed_ts,
        "api_latency_s": _round(latency_s),
        "api_latency_basis": wallet_api.get("api_latency_basis") or "legacy_wallet_data_api_event_age_s",
        "event_age_s": _round(event_age_s if event_age_s is not None else copyability_details.get("event_age_s")),
        "wallet_api_observed_ts": wallet_api.get("observed_ts"),
        "data_api_trade_query_key": wallet_api.get("data_api_query_param"),
        "data_api_trade_query_keys": wallet_api.get("data_api_trade_query_keys"),
        "data_api_trade_query_scope": wallet_api.get("data_api_trade_query_scope"),
        "wallet_api_fetch_duration_s": _round(wallet_api.get("fetch_duration_s") or copyability_details.get("wallet_fetch_duration_s")),
        "wallet_api_fetch_duration_basis": (
            wallet_api.get("fetch_duration_basis") or copyability_details.get("wallet_fetch_duration_basis")
        ),
        "wallet_api_source_fetch_duration_s": _round(
            wallet_api.get("source_fetch_duration_s") or copyability_details.get("source_fetch_duration_s")
        ),
        "wallet_api_batch_fetch_duration_s": _round(
            wallet_api.get("wallet_batch_fetch_duration_s") or copyability_details.get("wallet_batch_fetch_duration_s")
        ),
        "route_status": wallet_api.get("route_status"),
        "route_class": wallet_api.get("route_class"),
        "route_report_id": wallet_api.get("route_report_id"),
        "routed_host": wallet_api.get("routed_host"),
        "source_base_override_configured": wallet_api.get("source_base_override_configured"),
        "source_base_override_env_var": wallet_api.get("source_base_override_env_var"),
        "request_fingerprint": wallet_api.get("request_fingerprint"),
        "request_role": wallet_api.get("request_role"),
        "wallet_route_status": wallet_api.get("route_status"),
        "wallet_route_class": wallet_api.get("route_class"),
        "wallet_route_report_id": wallet_api.get("route_report_id"),
        "wallet_routed_host": wallet_api.get("routed_host"),
        "wallet_source_base_override_configured": wallet_api.get("source_base_override_configured"),
        "wallet_request_fingerprint": wallet_api.get("request_fingerprint"),
        "copy_status": copy_status,
        "coverage_status": coverage_status,
        "mirror_status": mirror.get("mirror_status"),
        "filter_policy": mirror.get("filter_policy"),
        "missed_copy_reason": missed_reason,
        "profit_policy_accepted": profit_policy_accepted,
        "profit_policy_reason": profit_policy_reason,
        "profit_policy_id": mirror.get("profit_policy_id"),
        "profit_policy_context_id": mirror.get("profit_policy_context_id") or mirror.get("profit_policy_id"),
        "profit_policy_candidate_id": mirror.get("profit_policy_candidate_id"),
        "copyability_policy_id": copyability.get("policy_id"),
        "copyability_accepted": copyability.get("accepted"),
        "copyability_reason": copyability.get("reason"),
        "copyability_details": copyability_details,
        "reject_reason": reject_reason,
        "reject_stage": fill.get("reject_stage"),
        "reject_details": reject_details,
        "intent_id": intent.intent_id if intent else None,
        "candidate_id": mirror.get("profit_policy_candidate_id"),
        "policy_id": intent.policy_id if intent else (
            mirror.get("profit_policy_context_id") or mirror.get("profit_policy_id") or mirror.get("copy_policy_id")
        ),
        "sizing_policy_id": intent.sizing_policy_id if intent else None,
        "copy_size_usd": _round(intent.copy_size_usd if intent else None),
        "copy_shares_requested": _round(intent.shares if intent else None),
        "order_id": order.get("order_id") if order else None,
        "paper_final_status": final_status or None,
        "fill_model": order.get("fill_model") if order else None,
        "fill_source": fill.get("source"),
        "filled_size_usd": round(filled_usd, 6),
        "filled_shares": round(filled_shares, 6),
        "fill_ratio": round(fill_ratio, 6),
        "share_fill_ratio": round(share_fill_ratio, 6),
        "fill_price": _round(fill_price if fill_price > 0 else None),
        "price_delta_abs": _round(price_delta),
        "slippage_bps": _round(slippage_bps),
        "worse_slippage_bps": _round(max(0.0, slippage_bps) if slippage_bps is not None else None),
        "clob_book_status": clob_book_status,
        "clob_book_admission_relevant": clob_admission_relevant,
        "clob_route_status": clob_book.get("route_status") if isinstance(clob_book, dict) else None,
        "clob_route_class": clob_book.get("route_class") if isinstance(clob_book, dict) else None,
        "clob_route_report_id": clob_book.get("route_report_id") if isinstance(clob_book, dict) else None,
        "clob_routed_host": clob_book.get("routed_host") if isinstance(clob_book, dict) else None,
        "clob_source_base_override_configured": (
            clob_book.get("source_base_override_configured") if isinstance(clob_book, dict) else None
        ),
        "clob_request_fingerprint": clob_book.get("request_fingerprint") if isinstance(clob_book, dict) else None,
        "clob_instant_fill_status": _detail_fallback(
            clob_book.get("instant_fill_status") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_instant_fill_status",
        ),
        "clob_best_bid": _detail_fallback(
            clob_book.get("best_bid") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_best_bid",
        ),
        "clob_best_ask": _detail_fallback(
            clob_book.get("best_ask") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_best_ask",
        ),
        "clob_spread": _detail_fallback(
            clob_book.get("spread") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_spread",
        ),
        "clob_fillable_usd": _detail_fallback(
            clob_book.get("fillable_usd") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_fillable_usd",
        ),
        "clob_remaining_usd": _detail_fallback(
            clob_book.get("remaining_usd") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_remaining_usd",
        ),
        "clob_fill_ratio": _detail_fallback(
            clob_book.get("fill_ratio") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_fill_ratio",
        ),
        "clob_max_copy_price": _detail_fallback(
            clob_book.get("max_copy_price") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_max_copy_price",
        ),
        "clob_blocking_reason": _detail_fallback(
            clob_book.get("blocking_reason") if isinstance(clob_book, dict) else None,
            copyability_details,
            "clob_blocking_reason",
        ),
        "book_timestamp": clob_book.get("book_timestamp") if isinstance(clob_book, dict) else None,
        "book_age_s": _round(max(0.0, event.observed_ts - book_ts) if book_ts is not None else None),
        "fill_blockers": fill_blockers,
    }


def build_copy_efficiency_report(
    events: list[WalletEvent],
    *,
    intent_by_event_id: dict[str, CopyIntent],
    paper_order_by_intent_id: dict[str, dict[str, Any]],
    mirror_result_by_event_id: dict[str, dict[str, Any]],
    profit_filter_decisions: dict[str, tuple[bool, str]] | None = None,
    copyability_decisions: dict[str, dict[str, Any]] | None = None,
    max_api_latency_s: float = DEFAULT_MAX_API_LATENCY_S,
    max_avg_worse_slippage_bps: float = DEFAULT_MAX_AVG_WORSE_SLIPPAGE_BPS,
    require_clob_book_evidence: bool = True,
) -> dict[str, Any]:
    """Build an auditable copy-quality report for the current tracker poll."""

    decisions = profit_filter_decisions or {}
    copyability = copyability_decisions or {}
    scores: list[dict[str, Any]] = []
    for event in events:
        intent = intent_by_event_id.get(event.event_id)
        paper_order = paper_order_by_intent_id.get(intent.intent_id) if intent is not None else None
        scores.append(
            score_copy_event(
                event,
                intent=intent,
                paper_order=paper_order,
                mirror_result=mirror_result_by_event_id.get(event.event_id),
                copyability_decision=copyability.get(event.event_id),
                profit_filter_decision=decisions.get(event.event_id),
            )
        )

    return build_copy_efficiency_report_from_scores(
        scores,
        max_api_latency_s=max_api_latency_s,
        max_avg_worse_slippage_bps=max_avg_worse_slippage_bps,
        require_clob_book_evidence=require_clob_book_evidence,
    )


def build_copy_efficiency_report_from_scores(
    scores: list[dict[str, Any]],
    *,
    max_api_latency_s: float = DEFAULT_MAX_API_LATENCY_S,
    max_avg_worse_slippage_bps: float = DEFAULT_MAX_AVG_WORSE_SLIPPAGE_BPS,
    require_clob_book_evidence: bool = True,
) -> dict[str, Any]:
    """Build an auditable copy-quality report from persisted event scores."""

    scores = [row for row in scores if isinstance(row, dict)]
    buy_scores = [row for row in scores if row.get("wallet_action") == "BUY"]
    fresh_buy_scores = [
        row
        for row in buy_scores
        if row.get("event_age_s") is not None and float(row["event_age_s"]) <= float(max_api_latency_s)
    ]
    stale_buy_scores = [
        row
        for row in buy_scores
        if row.get("event_age_s") is not None and float(row["event_age_s"]) > float(max_api_latency_s)
    ]
    filtered_buy_scores = [row for row in buy_scores if row.get("copy_status") == "FILTERED"]
    copyability_filtered_buy_scores = [
        row
        for row in filtered_buy_scores
        if str(row.get("filter_policy") or "") == "copyability"
    ]
    profit_filtered_buy_scores = [
        row
        for row in filtered_buy_scores
        if str(row.get("filter_policy") or "") != "copyability"
    ]
    profit_policy_accepted_buy_scores = [row for row in buy_scores if _score_profit_policy_accepted(row)]
    profit_policy_rejected_buy_scores = [row for row in buy_scores if not _score_profit_policy_accepted(row)]
    profit_policy_accepted_copyability_filtered_buy_scores = [
        row
        for row in profit_policy_accepted_buy_scores
        if row.get("copy_status") == "FILTERED" and str(row.get("filter_policy") or "") == "copyability"
    ]
    profit_policy_accepted_copyability_accepted_buy_scores = [
        row
        for row in profit_policy_accepted_buy_scores
        if row.get("copyability_accepted") is True
    ]
    required_buy_scores = [row for row in buy_scores if row.get("copy_status") != "FILTERED"]
    filled_buy_scores = [row for row in required_buy_scores if row.get("copy_status") == "COPIED_FILLED"]
    rejected_buy_scores = [row for row in required_buy_scores if row.get("copy_status") == "COPY_REJECTED"]
    missed_buy_scores = [row for row in required_buy_scores if row.get("copy_status") == "MISSED"]
    fallback_filled_buy_scores = [
        row
        for row in filled_buy_scores
        if str(row.get("fill_source") or "") != "clob_book_evidence"
    ]
    clob_filled_buy_scores = [
        row
        for row in filled_buy_scores
        if str(row.get("fill_source") or "") == "clob_book_evidence"
    ]
    lifecycle_scores = [row for row in scores if row.get("wallet_action") in {"SELL", "MERGE", "REDEEM"}]
    lifecycle_filtered = [row for row in lifecycle_scores if row.get("copy_status") == "FILTERED"]
    lifecycle_missed = [row for row in lifecycle_scores if row.get("copy_status") == "MISSED"]
    latencies = [
        float(row["api_latency_s"])
        for row in scores
        if row.get("api_latency_s") is not None
    ]
    event_ages = [
        float(row["event_age_s"])
        for row in scores
        if row.get("event_age_s") is not None
    ]
    required_event_ages = [
        float(row["event_age_s"])
        for row in required_buy_scores
        if row.get("event_age_s") is not None
    ]
    required_latencies = [
        float(row["api_latency_s"])
        for row in required_buy_scores
        if row.get("api_latency_s") is not None
    ]
    book_ages = [
        float(row["book_age_s"])
        for row in scores
        if row.get("book_age_s") is not None
    ]
    fetch_durations = [
        float(row["wallet_api_fetch_duration_s"])
        for row in scores
        if row.get("wallet_api_fetch_duration_s") is not None
    ]
    worse_slippage = [
        float(row["worse_slippage_bps"])
        for row in filled_buy_scores
        if row.get("worse_slippage_bps") is not None
    ]
    fill_ratios = [float(row.get("fill_ratio") or 0.0) for row in required_buy_scores]
    required_buy_count = len(required_buy_scores)
    filled_buy_count = len(filled_buy_scores)
    blockers: list[str] = []
    buy_execution_blockers: list[str] = []
    if missed_buy_scores:
        buy_execution_blockers.append("buy_copy_missed")
    if rejected_buy_scores:
        buy_execution_blockers.append("paper_order_rejected")
    if lifecycle_missed:
        blockers.append("lifecycle_copy_missed")
    if required_buy_scores and len(filled_buy_scores) < len(required_buy_scores):
        buy_execution_blockers.append("buy_copy_fill_rate_below_100pct")
    if require_clob_book_evidence and fallback_filled_buy_scores:
        buy_execution_blockers.append("fallback_fill_not_live_admissible")
    if required_event_ages and max(required_event_ages) > float(max_api_latency_s):
        buy_execution_blockers.append("wallet_event_age_above_copy_efficiency_cap")
    if fetch_durations and max(fetch_durations) > float(max_api_latency_s):
        buy_execution_blockers.append("wallet_fetch_duration_above_copy_efficiency_cap")
    if scores and required_buy_count == 0:
        buy_execution_blockers.append("no_required_buy_copy_evidence")
    if copyability_filtered_buy_scores and not required_buy_scores:
        buy_execution_blockers.append("copyability_filter_no_required_buy_evidence")
    if profit_policy_accepted_copyability_filtered_buy_scores and not required_buy_scores:
        buy_execution_blockers.append("policy_compatible_copyability_rejected_no_required_buy_evidence")
    avg_worse_slippage = _mean(worse_slippage)
    if avg_worse_slippage is not None and avg_worse_slippage > float(max_avg_worse_slippage_bps):
        buy_execution_blockers.append("avg_worse_slippage_above_copy_efficiency_cap")
    blockers.extend(buy_execution_blockers)

    executable_fill_rate_pct = (
        None if required_buy_count == 0 else round(100.0 * filled_buy_count / required_buy_count, 6)
    )
    requested_notional = sum(float(row.get("copy_size_usd") or 0.0) for row in required_buy_scores)
    filled_notional = sum(float(row.get("filled_size_usd") or 0.0) for row in filled_buy_scores)
    clob_filled_notional = sum(float(row.get("filled_size_usd") or 0.0) for row in clob_filled_buy_scores)
    fallback_filled_notional = sum(float(row.get("filled_size_usd") or 0.0) for row in fallback_filled_buy_scores)
    fill_blockers = [
        blocker
        for row in required_buy_scores
        for blocker in (row.get("fill_blockers") if isinstance(row.get("fill_blockers"), list) else [])
    ]
    missed_reasons = [row.get("missed_copy_reason") for row in required_buy_scores if row.get("copy_status") != "COPIED_FILLED"]
    rejection_reasons = [row.get("reject_reason") for row in rejected_buy_scores]
    clob_statuses = [row.get("clob_book_status") for row in buy_scores]
    fresh_clob_statuses = [row.get("clob_book_status") for row in fresh_buy_scores]
    stale_clob_statuses = [row.get("clob_book_status") for row in stale_buy_scores]
    admission_relevant_clob_statuses = [
        row.get("clob_book_status")
        for row in buy_scores
        if row.get("clob_book_admission_relevant") is True
    ]
    non_admission_clob_statuses = [
        row.get("clob_book_status")
        for row in buy_scores
        if row.get("clob_book_admission_relevant") is False
    ]
    clob_blocking_reasons = [row.get("clob_blocking_reason") for row in required_buy_scores]
    copyability_reasons = [row.get("copyability_reason") for row in copyability_filtered_buy_scores]
    profit_policy_rejection_reasons = [
        row.get("profit_policy_reason") or row.get("missed_copy_reason")
        for row in profit_policy_rejected_buy_scores
    ]
    profit_policy_accepted_copyability_reasons = [
        row.get("copyability_reason")
        for row in profit_policy_accepted_copyability_filtered_buy_scores
    ]
    clob_ok_copyability_filtered_by_fetch_duration = [
        row
        for row in copyability_filtered_buy_scores
        if str(row.get("copyability_reason") or "") == "fetch_duration_above_cap"
        and str(row.get("clob_book_status") or "") == "OK"
    ]
    profit_policy_accepted_clob_ok_copyability_filtered_by_fetch_duration = [
        row
        for row in profit_policy_accepted_copyability_filtered_buy_scores
        if str(row.get("copyability_reason") or "") == "fetch_duration_above_cap"
        and str(row.get("clob_book_status") or "") == "OK"
    ]
    profit_policy_accepted_event_ages = [
        float(row["event_age_s"])
        for row in profit_policy_accepted_buy_scores
        if row.get("event_age_s") is not None
    ]
    profit_policy_accepted_copyability_rejected_slippage_to_fill_bps = [
        float(details.get("min_slippage_to_fill_bps"))
        for row in profit_policy_accepted_copyability_filtered_buy_scores
        for details in [row.get("copyability_details") if isinstance(row.get("copyability_details"), dict) else {}]
        if details.get("min_slippage_to_fill_bps") is not None
    ]
    lifecycle_filter_reasons = [row.get("missed_copy_reason") for row in lifecycle_filtered]
    min_slippage_to_fill_bps = [
        float(details.get("min_slippage_to_fill_bps"))
        for row in buy_scores
        for details in [row.get("copyability_details") if isinstance(row.get("copyability_details"), dict) else {}]
        if details.get("min_slippage_to_fill_bps") is not None
    ]
    if not scores:
        blockers.append("no_wallet_events_observed")
        buy_execution_status = "WATCH"
        status = "WATCH"
    elif required_buy_count == 0:
        buy_execution_status = "WATCH"
        status = "WATCH"
    else:
        buy_execution_status = "PASS" if not buy_execution_blockers else "FAIL"
        status = "PASS" if not blockers else "FAIL"

    return {
        "status": status,
        "blockers": blockers,
        "thresholds": {
            "max_api_latency_s": float(max_api_latency_s),
            "max_event_age_s": float(max_api_latency_s),
            "max_avg_worse_slippage_bps": float(max_avg_worse_slippage_bps),
            "require_clob_book_evidence": bool(require_clob_book_evidence),
        },
        "summary": {
            "latency_basis": "api_latency_s is kept for compatibility and currently equals wallet Data API event age when no lower-level transport timing exists",
            "total_wallet_events": len(scores),
            "source_buy_events": len(buy_scores),
            "source_lifecycle_events": len(lifecycle_scores),
            "filtered_buy_events": len(filtered_buy_scores),
            "copyability_filtered_buy_events": len(copyability_filtered_buy_scores),
            "profit_filtered_buy_events": len(profit_filtered_buy_scores),
            "profit_policy_accepted_buy_events": len(profit_policy_accepted_buy_scores),
            "profit_policy_rejected_buy_events": len(profit_policy_rejected_buy_scores),
            "profit_policy_accepted_copyability_filtered_buy_events": len(
                profit_policy_accepted_copyability_filtered_buy_scores
            ),
            "profit_policy_accepted_copyability_accepted_buy_events": len(
                profit_policy_accepted_copyability_accepted_buy_scores
            ),
            "profit_policy_accepted_fresh_buy_events_le_10s": sum(
                1
                for row in profit_policy_accepted_buy_scores
                if row.get("event_age_s") is not None and float(row["event_age_s"]) <= 10.0
            ),
            "profit_policy_accepted_fresh_buy_events_le_30s": sum(
                1
                for row in profit_policy_accepted_buy_scores
                if row.get("event_age_s") is not None and float(row["event_age_s"]) <= 30.0
            ),
            "required_buy_copy_events": required_buy_count,
            "filled_buy_copy_events": filled_buy_count,
            "clob_filled_buy_copy_events": len(clob_filled_buy_scores),
            "fallback_filled_buy_copy_events": len(fallback_filled_buy_scores),
            "rejected_buy_copy_events": len(rejected_buy_scores),
            "missed_buy_copy_events": len(missed_buy_scores),
            "buy_execution_status": buy_execution_status,
            "buy_execution_blockers": buy_execution_blockers,
            "source_fresh_buy_events_le_10s": sum(
                1 for row in buy_scores if row.get("event_age_s") is not None and float(row["event_age_s"]) <= 10.0
            ),
            "source_fresh_buy_events_le_30s": sum(
                1 for row in buy_scores if row.get("event_age_s") is not None and float(row["event_age_s"]) <= 30.0
            ),
            "stale_buy_rows_gt_300s": sum(
                1 for row in buy_scores if row.get("event_age_s") is not None and float(row["event_age_s"]) > 300.0
            ),
            "admission_relevant_buy_rows": len(fresh_buy_scores),
            "latest_buy_event_ts": max(
                (float(row["source_event_ts"]) for row in buy_scores if row.get("source_event_ts") is not None),
                default=None,
            ),
            "latest_buy_event_lag_s": min(
                (float(row["event_age_s"]) for row in buy_scores if row.get("event_age_s") is not None),
                default=None,
            ),
            "required_fresh_buy_events_le_10s": sum(
                1
                for row in required_buy_scores
                if row.get("event_age_s") is not None and float(row["event_age_s"]) <= 10.0
            ),
            "lifecycle_mirrored_events": sum(
                1 for row in lifecycle_scores if str(row.get("coverage_status") or "") == "MIRRORED"
            ),
            "lifecycle_filtered_events": len(lifecycle_filtered),
            "lifecycle_missed_events": len(lifecycle_missed),
            "executable_fill_rate_pct": executable_fill_rate_pct,
            "clob_admission_fill_rate_pct": (
                None if required_buy_count == 0 else round(100.0 * len(clob_filled_buy_scores) / required_buy_count, 6)
            ),
            "requested_copy_notional_usd": round(requested_notional, 6),
            "filled_copy_notional_usd": round(filled_notional, 6),
            "clob_filled_copy_notional_usd": round(clob_filled_notional, 6),
            "fallback_filled_copy_notional_usd": round(fallback_filled_notional, 6),
            "notional_fill_rate_pct": (
                None if requested_notional <= 0 else round(100.0 * filled_notional / requested_notional, 6)
            ),
            "clob_admission_notional_fill_rate_pct": (
                None if requested_notional <= 0 else round(100.0 * clob_filled_notional / requested_notional, 6)
            ),
            "fill_ratio_avg": _mean(fill_ratios),
            "api_latency_avg_s": _mean(latencies),
            "api_latency_p50_s": _percentile(latencies, 50.0),
            "api_latency_p95_s": _percentile(latencies, 95.0),
            "api_latency_max_s": _round(max(latencies) if latencies else None),
            "required_api_latency_avg_s": _mean(required_latencies),
            "required_api_latency_p50_s": _percentile(required_latencies, 50.0),
            "required_api_latency_p95_s": _percentile(required_latencies, 95.0),
            "required_api_latency_max_s": _round(max(required_latencies) if required_latencies else None),
            "wallet_api_fetch_duration_avg_s": _mean(fetch_durations),
            "wallet_api_fetch_duration_p50_s": _percentile(fetch_durations, 50.0),
            "wallet_api_fetch_duration_p95_s": _percentile(fetch_durations, 95.0),
            "wallet_api_fetch_duration_max_s": _round(max(fetch_durations) if fetch_durations else None),
            "event_age_avg_s": _mean(event_ages),
            "observed_event_age_p95_s": _percentile(event_ages, 95.0),
            "required_event_age_avg_s": _mean(required_event_ages),
            "required_event_age_p50_s": _percentile(required_event_ages, 50.0),
            "required_event_age_p95_s": _percentile(required_event_ages, 95.0),
            "required_event_age_max_s": _round(max(required_event_ages) if required_event_ages else None),
            "event_age_p50_s": _percentile(event_ages, 50.0),
            "event_age_p95_s": _percentile(event_ages, 95.0),
            "event_age_max_s": _round(max(event_ages) if event_ages else None),
            "event_age_bucket_counts": _age_bucket_counts(event_ages),
            "required_event_age_bucket_counts": _age_bucket_counts(required_event_ages),
            "book_age_avg_s": _mean(book_ages),
            "book_age_p95_s": _percentile(book_ages, 95.0),
            "book_age_max_s": _round(max(book_ages) if book_ages else None),
            "avg_worse_slippage_bps": avg_worse_slippage,
            "p95_worse_slippage_bps": _percentile(worse_slippage, 95.0),
            "max_worse_slippage_bps": _round(max(worse_slippage) if worse_slippage else None),
            "missed_copy_reason_counts": _counter(missed_reasons),
            "rejection_reason_counts": _counter(rejection_reasons),
            "fill_blocker_counts": _counter(fill_blockers),
            "clob_book_status_counts": _counter(clob_statuses),
            "clob_book_status_counts_for_fresh_buys": _counter(fresh_clob_statuses),
            "clob_book_status_counts_for_stale_buys": _counter(stale_clob_statuses),
            "clob_book_status_counts_for_admission_relevant_buys": _counter(admission_relevant_clob_statuses),
            "clob_book_status_counts_for_non_admission_buys": _counter(non_admission_clob_statuses),
            "clob_blocking_reason_counts": _counter(clob_blocking_reasons),
            "copyability_reason_counts": _counter(copyability_reasons),
            "clob_ok_but_copyability_filtered_by_fetch_duration": len(
                clob_ok_copyability_filtered_by_fetch_duration
            ),
            "profit_policy_accepted_clob_ok_but_copyability_filtered_by_fetch_duration": len(
                profit_policy_accepted_clob_ok_copyability_filtered_by_fetch_duration
            ),
            "profit_policy_rejection_reason_counts": _counter(profit_policy_rejection_reasons),
            "profit_policy_accepted_copyability_reason_counts": _counter(
                profit_policy_accepted_copyability_reasons
            ),
            "profit_policy_accepted_event_age_bucket_counts": _age_bucket_counts(
                profit_policy_accepted_event_ages
            ),
            "profit_policy_accepted_copyability_rejected_min_slippage_to_fill_bps_p95": _percentile(
                profit_policy_accepted_copyability_rejected_slippage_to_fill_bps,
                95.0,
            ),
            "profit_policy_accepted_copyability_rejected_min_slippage_to_fill_bps_max": _round(
                max(profit_policy_accepted_copyability_rejected_slippage_to_fill_bps)
                if profit_policy_accepted_copyability_rejected_slippage_to_fill_bps
                else None
            ),
            "lifecycle_filter_reason_counts": _counter(lifecycle_filter_reasons),
            "min_slippage_to_fill_bps_p95": _percentile(min_slippage_to_fill_bps, 95.0),
            "min_slippage_to_fill_bps_max": _round(max(min_slippage_to_fill_bps) if min_slippage_to_fill_bps else None),
        },
        "event_scores": scores,
    }
