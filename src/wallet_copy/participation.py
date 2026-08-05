"""Window participation taxonomy for wallet-copy live coverage."""

from __future__ import annotations

from collections import Counter
from typing import Any


CORRECT_SKIP_REASONS = frozenset(
    {
        "inventory_target_already_met",
        "inventory_late_window_guard",
        "inventory_residual_gap_below_min_order",
        "filtered_after_inventory_build",
        "inventory_confirmed_unchanged_no_edge",
        "already_submitted_intent",
    }
)
NO_SIGNAL_REASONS = frozenset(
    {
        "event_prefilter_or_policy_gate",
        "no_fresh_live_tradeable_intents",
        "no_fresh_live_tradeable_rows",
        "no_fresh_policy_compatible_source_rows",
        "no_inventory_window_participation_rows",
    }
)
PROTECTED_SKIP_TOKENS = (
    "toxicity",
    "above_vwap",
    "shadow_ev",
    "shadow-ev",
    "negative_shadow",
    "hard_entry_cap",
    "hard_entry_floor",
    "entry_price_band_gate",
    "profit_latency",
    "window_fill_cap",
    "window_time_gte",
    "signal_age_gte",
)
SUBMITTED_REASONS = frozenset({"filled", "submitted", "live_order_rejected"})
FLOOR_BLOCKED_MISS_REASONS = frozenset({"drip_min_tranche_exceeds_window_budget"})
PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS = 6


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _reason(row: dict[str, Any]) -> str:
    return str(row.get("dominant_skip_reason") or row.get("reason") or "unknown").strip()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def floor_blocked_miss(row: dict[str, Any]) -> bool:
    reason_lc = _reason(row).lower()
    if reason_lc not in FLOOR_BLOCKED_MISS_REASONS:
        return False
    window_budget_usd = _as_float(row.get("window_budget_usd"), 0.0)
    # Effective floor is the max of the process $1 min and the per-token
    # drip/CLOB 5-share floor persisted on the row by live execution;
    # a $1 budget against a $2.35 share floor is an honest blocked skip.
    effective_min_submit_usd = max(
        _as_float(row.get("process_min_live_order_usd"), 0.0),
        _as_float(row.get("effective_min_tranche_usd"), 0.0),
        _as_float(row.get("drip_min_tranche_usd"), 0.0),
    )
    if effective_min_submit_usd <= 0 and row.get("probe_cap_min_order_floor_blocked"):
        effective_min_submit_usd = _as_float(row.get("would_floor_min_order_usd"), 0.0)
    return effective_min_submit_usd > 0 and 0 <= window_budget_usd < effective_min_submit_usd


def participation_skip_category(row: dict[str, Any]) -> str:
    reason = _reason(row)
    reason_lc = reason.lower()
    if _as_int(row.get("our_submits")) > 0 or _as_int(row.get("our_fills")) > 0:
        return "SUBMITTED"
    if reason_lc in SUBMITTED_REASONS:
        return "SUBMITTED"
    if floor_blocked_miss(row):
        return "FLOOR_BLOCKED_MISS"
    if reason_lc in CORRECT_SKIP_REASONS:
        return "CORRECT_SKIP"
    if reason_lc in NO_SIGNAL_REASONS or reason_lc.startswith("no_fresh_"):
        return "NO_SIGNAL"
    if any(token in reason_lc for token in PROTECTED_SKIP_TOKENS):
        return "PROTECTED_SKIP"
    return "MEASURED_SKIP"


def annotate_participation_item(row: dict[str, Any], *, category: str | None = None) -> dict[str, Any]:
    category = category or participation_skip_category(row)
    eligible = _as_int(row.get("wallet_eligible_orders"))
    pending = bool(row.get("miss_pending_market_lifecycle"))
    missed = bool(row.get("missed_active_window"))
    denominator = eligible > 0 and not pending and category != "NO_SIGNAL"
    equivalent = denominator and category in {"SUBMITTED", "CORRECT_SKIP"}
    floor_blocked = bool(denominator and missed and category == "FLOOR_BLOCKED_MISS")
    adjusted_missed = bool(denominator and missed and not equivalent and not floor_blocked)
    measured_skip = bool(denominator and missed and category in {"PROTECTED_SKIP", "MEASURED_SKIP"})
    row["participation_skip_category"] = category
    row["participation_adjusted_denominator"] = bool(denominator)
    row["participation_equivalent"] = bool(equivalent)
    row["floor_blocked_miss"] = floor_blocked
    row["adjusted_missed_active_window"] = adjusted_missed
    row["measured_skip_window"] = measured_skip
    row["source_coverage_window"] = bool(denominator)
    return row


def participation_window_category(window: dict[str, Any], child_rows: list[dict[str, Any]]) -> str:
    if _as_int(window.get("our_submits")) > 0 or _as_int(window.get("our_fills")) > 0:
        return "SUBMITTED"
    counts = Counter(
        str(row.get("participation_skip_category") or participation_skip_category(row))
        for row in child_rows
        if isinstance(row, dict)
    )
    if counts.get("PROTECTED_SKIP"):
        return "PROTECTED_SKIP"
    if counts.get("MEASURED_SKIP"):
        return "MEASURED_SKIP"
    if counts.get("FLOOR_BLOCKED_MISS"):
        return "FLOOR_BLOCKED_MISS"
    if counts.get("CORRECT_SKIP"):
        return "CORRECT_SKIP"
    if counts.get("NO_SIGNAL"):
        return "NO_SIGNAL"
    return participation_skip_category(window)


def annotate_participation_window(window: dict[str, Any], child_rows: list[dict[str, Any]]) -> dict[str, Any]:
    category = participation_window_category(window, child_rows)
    counts = Counter(
        str(row.get("participation_skip_category") or participation_skip_category(row))
        for row in child_rows
        if isinstance(row, dict)
    )
    annotate_participation_item(window, category=category)
    window["participation_skip_category_counts"] = dict(sorted(counts.items()))
    return window


def summarize_adjusted_participation(
    windows: list[dict[str, Any]],
    *,
    incident_threshold_windows: int = PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
) -> dict[str, Any]:
    threshold = max(1, int(incident_threshold_windows or PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS))
    active = [row for row in windows if isinstance(row, dict) and row.get("participation_adjusted_denominator")]
    missed = [row for row in active if row.get("adjusted_missed_active_window")]
    consecutive = 0
    for row in active:
        if row.get("adjusted_missed_active_window"):
            consecutive += 1
            continue
        break
    category_counts = Counter(
        str(row.get("participation_skip_category") or participation_skip_category(row))
        for row in windows
        if isinstance(row, dict)
    )
    protected_reason_counts = Counter(
        str(row.get("dominant_skip_reason") or "unknown")
        for row in windows
        if isinstance(row, dict) and row.get("participation_skip_category") == "PROTECTED_SKIP"
    )
    source_coverage_windows = sum(1 for row in windows if row.get("source_coverage_window"))
    no_signal_windows = int(category_counts.get("NO_SIGNAL") or 0)
    source_coverage_denominator = source_coverage_windows + no_signal_windows
    source_coverage_rate_pct = (
        round(100.0 * source_coverage_windows / source_coverage_denominator, 6)
        if source_coverage_denominator > 0
        else None
    )
    return {
        "basis": "skip_taxonomy_current_generation",
        "incident_threshold_windows": threshold,
        "adjusted_active_windows": len(active),
        "adjusted_missed_active_windows": len(missed),
        "adjusted_consecutive_missed_active_windows": consecutive,
        "adjusted_incident_triggered": consecutive >= threshold,
        "participation_equivalent_windows": sum(1 for row in active if row.get("participation_equivalent")),
        "floor_blocked_miss_windows": sum(1 for row in active if row.get("floor_blocked_miss")),
        "correct_skip_windows": int(category_counts.get("CORRECT_SKIP") or 0),
        "protected_skip_windows": int(category_counts.get("PROTECTED_SKIP") or 0),
        "measured_skip_windows": int(category_counts.get("MEASURED_SKIP") or 0),
        "no_signal_windows": no_signal_windows,
        "source_coverage_windows": source_coverage_windows,
        "source_coverage_denominator_windows": source_coverage_denominator,
        "source_coverage_rate_pct": source_coverage_rate_pct,
        "skip_taxonomy_counts": dict(sorted(category_counts.items())),
        "protected_skip_reason_counts": dict(sorted(protected_reason_counts.items())),
    }
