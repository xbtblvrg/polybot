"""Canonical live-guard gate registry shared by runtime diagnostics."""

from __future__ import annotations


LIVE_DROUGHT_FUNNEL_GATE_KEYS = (
    "drift_buffer",
    "inventory_best_ask_gate",
    "live_hard_entry_cap",
    "profit_latency_suppression",
    "toxicity_protection",
    "live_min_order_floor",
    "expected_fee_capture_gate",
    "entry_price_band_gate",
)

F418_GATE_ORDER = (
    "drift_buffer",
    "inventory_best_ask_gate",
    "live_hard_entry_cap",
    "live_hard_entry_floor",
    "entry_price_band_gate",
    "profit_latency_suppression",
    "toxicity_protection",
    "live_min_order_floor",
    "market_buy_precision_preflight",
    "expected_fee_capture_gate",
    "window_fill_cap",
)

GUARD_AUTHORED_GATE_CLASSES = frozenset(
    (*LIVE_DROUGHT_FUNNEL_GATE_KEYS, *F418_GATE_ORDER)
)

# Empty venue order_id is the defining invariant: these outcomes are authored
# before a CLOB interaction and must never be counted as venue rejects/submits.
PRE_SUBMIT_REFUSAL_CLASSES = frozenset(
    {
        "entry_price_band_closed_negative_holdout",
        "maker_min_share_bump_exceeds_policy_cap",
        "passive_at_source_lane_closed",
    }
)
