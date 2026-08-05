"""PnL attribution for paper-only wallet-copy execution tactics."""

from __future__ import annotations

from typing import Any

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.performance import score_order, summarize_scores


def _source_event_id(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    return str(source_intent.get("source_event_id") or "")


def _fill_estimate(order: dict[str, Any]) -> dict[str, Any]:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    fill = source_intent.get("fill_estimate") if isinstance(source_intent.get("fill_estimate"), dict) else {}
    return fill


def _effective_price(order: dict[str, Any]) -> float | None:
    fill = _fill_estimate(order)
    value = fill.get("effective_price", order.get("limit_price"))
    if value is None:
        return None
    return num(value)


def _source_price(order: dict[str, Any]) -> float | None:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    value = source_intent.get("limit_price")
    if value is None:
        return None
    return num(value)


def _price_delta_bps(order: dict[str, Any]) -> float | None:
    source = _source_price(order)
    effective = _effective_price(order)
    if source is None or effective is None or source <= 0:
        return None
    return (effective - source) / source * 10_000.0


def _canonical_rows(scored_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in scored_rows:
        if not row.get("resolved"):
            continue
        resolution = row.get("resolution") if isinstance(row.get("resolution"), dict) else {}
        if resolution.get("research_only") is True:
            continue
        out.append(row)
    return out


def _research_only_rows(scored_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in scored_rows:
        if not row.get("resolved"):
            continue
        resolution = row.get("resolution") if isinstance(row.get("resolution"), dict) else {}
        if resolution.get("research_only") is True:
            out.append(row)
    return out


def _max_drawdown(rows: list[dict[str, Any]]) -> float:
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for row in rows:
        if not row.get("resolved"):
            continue
        running += num(row.get("pnl_usd"))
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
    return round(max_drawdown, 6)


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize_scores(rows)
    summary["max_drawdown_usd"] = _max_drawdown(rows)
    return summary


def score_tactic_replay_pnl(
    strict_orders: list[dict[str, Any]],
    tactic_orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Compare strict exact-copy paper orders with a paper-only tactic replay.

    The tactic can improve CLOB fillability, but it is not useful unless the
    higher execution price still leaves resolved PnL. This function keeps that
    attribution source-event scoped, so a tactic cannot become live-ready merely
    because it filled more orders.
    """

    strict_by_source_event = {
        source_event_id: order
        for order in strict_orders
        for source_event_id in [_source_event_id(order)]
        if source_event_id
    }
    tactic_by_source_event = {
        source_event_id: order
        for order in tactic_orders
        for source_event_id in [_source_event_id(order)]
        if source_event_id
    }
    strict_scores_by_source_event = {
        source_event_id: score_order(order, resolutions)
        for source_event_id, order in strict_by_source_event.items()
    }
    tactic_scores_by_source_event = {
        source_event_id: score_order(order, resolutions)
        for source_event_id, order in tactic_by_source_event.items()
    }
    tactic_scores = list(tactic_scores_by_source_event.values())
    strict_scores = list(strict_scores_by_source_event.values())
    canonical_tactic_scores = _canonical_rows(tactic_scores)
    canonical_strict_scores = _canonical_rows(strict_scores)
    research_only_tactic_scores = _research_only_rows(tactic_scores)

    event_rows: list[dict[str, Any]] = []
    cost_delta_usd = 0.0
    effective_price_delta_bps_values: list[float] = []
    for source_event_id in sorted(set(strict_by_source_event) | set(tactic_by_source_event)):
        strict_order = strict_by_source_event.get(source_event_id) or {}
        tactic_order = tactic_by_source_event.get(source_event_id) or {}
        strict_score = strict_scores_by_source_event.get(source_event_id) or {}
        tactic_score = tactic_scores_by_source_event.get(source_event_id) or {}
        strict_cost = num(strict_order.get("filled_size_usd"))
        tactic_cost = num(tactic_order.get("filled_size_usd"))
        cost_delta = tactic_cost - strict_cost
        cost_delta_usd += cost_delta
        price_delta_bps = _price_delta_bps(tactic_order) if tactic_order else None
        if price_delta_bps is not None:
            effective_price_delta_bps_values.append(float(price_delta_bps))
        event_rows.append(
            {
                "source_event_id": source_event_id,
                "strict_order_id": strict_order.get("order_id"),
                "tactic_order_id": tactic_order.get("order_id"),
                "strict_status": strict_order.get("final_status") or strict_order.get("status"),
                "tactic_status": tactic_order.get("final_status") or tactic_order.get("status"),
                "strict_pnl_usd": strict_score.get("pnl_usd"),
                "tactic_pnl_usd": tactic_score.get("pnl_usd"),
                "strict_resolved": bool(strict_score.get("resolved")),
                "tactic_resolved": bool(tactic_score.get("resolved")),
                "strict_win": strict_score.get("win"),
                "tactic_win": tactic_score.get("win"),
                "strict_cost_usd": round(strict_cost, 6),
                "tactic_cost_usd": round(tactic_cost, 6),
                "cost_delta_usd": round(cost_delta, 6),
                "effective_price_delta_bps": round(price_delta_bps, 6) if price_delta_bps is not None else None,
            }
        )

    fallback_filled_orders = sum(
        1
        for row in tactic_orders
        if str(row.get("final_status") or row.get("status") or "").upper() == "FILLED"
        and str(_fill_estimate(row).get("source") or "") == "source_price_plus_slippage_fallback"
    )
    rejected_orders = sum(
        1 for row in tactic_orders if str(row.get("final_status") or row.get("status") or "").upper() == "REJECTED"
    )
    canonical_summary = _summary(canonical_tactic_scores)
    strict_canonical_summary = _summary(canonical_strict_scores)
    pnl_delta_vs_strict = num(canonical_summary.get("pnl_usd")) - num(strict_canonical_summary.get("pnl_usd"))
    blockers: list[str] = []
    if not tactic_orders:
        blockers.append("tactic_replay_missing_orders")
    if rejected_orders > 0:
        blockers.append("tactic_replay_rejected_orders_present")
    if fallback_filled_orders > 0:
        blockers.append("tactic_replay_fallback_fills_present")
    if int(canonical_summary.get("resolved_orders") or 0) <= 0:
        blockers.append("tactic_replay_pnl_attribution_missing")
    if num(canonical_summary.get("pnl_usd")) <= 0:
        blockers.append("tactic_replay_pnl_not_positive")
    if num(canonical_summary.get("roi_pct")) <= 0:
        blockers.append("tactic_replay_roi_not_positive")

    return {
        "schema_version": 1,
        "kind": "wallet_copy_tactic_replay_pnl_attribution",
        "generated_at": utc_now_iso(),
        "status": "PASS" if not blockers else "ANALYZE",
        "blockers": blockers,
        "orders": len(tactic_orders),
        "strict_orders": len(strict_orders),
        "fallback_filled_orders": fallback_filled_orders,
        "rejected_orders": rejected_orders,
        "canonical_resolved_orders": int(canonical_summary.get("resolved_orders") or 0),
        "research_only_resolved_orders": len(research_only_tactic_scores),
        "unresolved_ratio": canonical_summary.get("unresolved_ratio"),
        "tactic_pnl_usd": canonical_summary.get("pnl_usd"),
        "tactic_roi_pct": canonical_summary.get("roi_pct"),
        "tactic_wr_pct": canonical_summary.get("wr_pct"),
        "strict_pnl_usd": strict_canonical_summary.get("pnl_usd"),
        "strict_roi_pct": strict_canonical_summary.get("roi_pct"),
        "strict_wr_pct": strict_canonical_summary.get("wr_pct"),
        "pnl_delta_vs_strict_usd": round(pnl_delta_vs_strict, 6),
        "cost_delta_usd": round(cost_delta_usd, 6),
        "avg_effective_price_delta_bps": (
            round(sum(effective_price_delta_bps_values) / len(effective_price_delta_bps_values), 6)
            if effective_price_delta_bps_values
            else None
        ),
        "p95_effective_price_delta_bps": _percentile(effective_price_delta_bps_values, 95.0),
        "max_drawdown_usd": canonical_summary.get("max_drawdown_usd"),
        "canonical_summary": canonical_summary,
        "strict_canonical_summary": strict_canonical_summary,
        "research_only_summary": _summary(research_only_tactic_scores),
        "event_attribution_rows": event_rows[:100],
    }


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * max(0.0, min(100.0, float(pct))) / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return round(ordered[lower] * (1.0 - weight) + ordered[upper] * weight, 6)
