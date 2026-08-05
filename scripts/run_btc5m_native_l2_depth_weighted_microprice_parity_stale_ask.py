#!/usr/bin/env python3
"""BTC-native top-3 depth-weighted cross-outcome parity paper lane."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_btc5m_native_l2_cross_outcome_parity_stale_ask as base
from scripts.run_btc5m_book_shock_reversion import _levels


FORWARD_START_S = 1_785_011_700  # 2026-07-25T20:35:00Z
TOP_N_LEVELS = 3
STATE = "data/research/btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_state.json"
CACHE = "data/research/btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_raw_cache.json"
TERMINALS = "data/research/btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_terminals.jsonl"
EVENTS = "data/research/btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_events.jsonl"
SELECTOR = "data/research/btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_selector.json"

CONFIG = {
    **base.CONFIG,
    "method": "btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_v1",
    "economic_edge": "native_receipt_clock_cross_outcome_top3_depth_weighted_parity_fair_vs_same_outcome_ask",
    "calibration_method": "complement_top3_depth_weighted_microprice_parity_to_executable_same_outcome_ask",
    "training_cutoff_s": FORWARD_START_S - 600,
    "forward_start_s": FORWARD_START_S,
    "features": {
        **base.CONFIG["features"],
        "top_n_levels": TOP_N_LEVELS,
    },
    "signal_rule": "fair_outcome_equals_one_minus_complement_top3_depth_weighted_microprice; strictly_positive_post_cost_edge; higher_net_edge_wins; exact_equal_edge_abstains",
    "terminal_clock": {
        **base.CONFIG["terminal_clock"],
        "sixth_terminal_deadline_s": FORWARD_START_S + 5 * 300 + 270,
    },
}
CHECKSUM = hashlib.sha256(
    json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
CELL_ID = f"l2_depth_weighted_cross_parity_{CHECKSUM[:12]}"
PREREG = (
    "data/research/"
    f"btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_preregistration_{CHECKSUM[:12]}.json"
)

_base_summarize_l2 = base.summarize_l2
_base_choose_signal = base.choose_signal


def summarize_l2(raw: Any, *, token_id: str, observed_at_s: float) -> dict[str, Any]:
    summary = _base_summarize_l2(
        raw, token_id=token_id, observed_at_s=observed_at_s
    )
    bids = _levels(raw, "bids")[:TOP_N_LEVELS]
    asks = _levels(raw, "asks")[:TOP_N_LEVELS]
    if summary.get("status") != "PASS" or not bids or not asks:
        return summary
    bid_depth = sum(size for _, size in bids)
    ask_depth = sum(size for _, size in asks)
    bid_vwap = sum(price * size for price, size in bids) / bid_depth
    ask_vwap = sum(price * size for price, size in asks) / ask_depth
    denominator = bid_depth + ask_depth
    depth_weighted_microprice = (
        ask_vwap * bid_depth + bid_vwap * ask_depth
    ) / denominator
    return {
        **summary,
        "top_n_levels": TOP_N_LEVELS,
        "top_n_bids": [[price, size] for price, size in bids],
        "top_n_asks": [[price, size] for price, size in asks],
        "top_n_bid_depth": round(bid_depth, 6),
        "top_n_ask_depth": round(ask_depth, 6),
        "top_n_bid_vwap": round(bid_vwap, 6),
        "top_n_ask_vwap": round(ask_vwap, 6),
        "depth_weighted_microprice": round(depth_weighted_microprice, 6),
    }


def choose_signal(**kwargs: Any) -> tuple[dict[str, Any] | None, list[str]]:
    books = kwargs["books"]
    projected: dict[str, dict[str, dict[str, Any]]] = {}
    for asset, outcomes in books.items():
        projected[asset] = {}
        for outcome, book in outcomes.items():
            projected[asset][outcome] = {
                **book,
                "microprice": book.get("depth_weighted_microprice"),
            }
    signal, blockers = _base_choose_signal(**{**kwargs, "books": projected})
    if signal:
        complement_book = books["BTC"][signal["complement"]]
        signal["fair_basis"] = "complement_top3_depth_weighted_microprice"
        signal["complement_depth_weighted_microprice"] = complement_book.get(
            "depth_weighted_microprice"
        )
        signal["top_n_levels"] = TOP_N_LEVELS
    return signal, blockers


def configure_base() -> None:
    base.FORWARD_START_S = FORWARD_START_S
    base.STATE = STATE
    base.CACHE = CACHE
    base.TERMINALS = TERMINALS
    base.EVENTS = EVENTS
    base.SELECTOR = SELECTOR
    base.CONFIG = CONFIG
    base.CHECKSUM = CHECKSUM
    base.CELL_ID = CELL_ID
    base.PREREG = PREREG
    base.summarize_l2 = summarize_l2
    base.choose_signal = choose_signal
    base.ARBITER_SOURCES = [
        source
        for source in base.ARBITER_SOURCES
        if "cross_outcome_parity_stale_ask_selector" not in source
    ] + [SELECTOR]


def main() -> int:
    configure_base()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
