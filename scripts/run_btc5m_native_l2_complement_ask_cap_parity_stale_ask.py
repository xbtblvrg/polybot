#!/usr/bin/env python3
"""BTC-native complement best-ask cap parity into a stale ask."""

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


FORWARD_START_S = 1_785_014_100  # 2026-07-25T21:15:00Z
STATE = "data/research/btc5m_native_l2_complement_ask_cap_parity_stale_ask_state.json"
CACHE = "data/research/btc5m_native_l2_complement_ask_cap_parity_stale_ask_raw_cache.json"
TERMINALS = "data/research/btc5m_native_l2_complement_ask_cap_parity_stale_ask_terminals.jsonl"
EVENTS = "data/research/btc5m_native_l2_complement_ask_cap_parity_stale_ask_events.jsonl"
SELECTOR = "data/research/btc5m_native_l2_complement_ask_cap_parity_stale_ask_selector.json"

CONFIG = {
    **base.CONFIG,
    "method": "btc5m_native_l2_complement_ask_cap_parity_stale_ask_v1",
    "economic_edge": "native_receipt_clock_complement_ask_cap_fair_vs_same_outcome_ask",
    "calibration_method": "one_minus_complement_best_ask_to_executable_same_outcome_ask",
    "training_cutoff_s": FORWARD_START_S - 600,
    "forward_start_s": FORWARD_START_S,
    "signal_rule": "fair_outcome_equals_one_minus_complement_best_ask; strictly_positive_post_cost_edge; higher_net_edge_wins; exact_equal_edge_abstains",
    "terminal_clock": {
        **base.CONFIG["terminal_clock"],
        "sixth_terminal_deadline_s": FORWARD_START_S + 5 * 300 + 270,
        "negative_rolling_pnl_park": True,
        "integrity_disagreement_park": True,
    },
}
CHECKSUM = hashlib.sha256(
    json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
CELL_ID = f"l2_complement_ask_cap_{CHECKSUM[:12]}"
PREREG = (
    "data/research/"
    f"btc5m_native_l2_complement_ask_cap_parity_stale_ask_preregistration_{CHECKSUM[:12]}.json"
)

_base_choose_signal = base.choose_signal
_base_build_intent = base.build_intent
_base_reduce_generation = base.reduce_generation


def choose_signal(**kwargs: Any) -> tuple[dict[str, Any] | None, list[str]]:
    books = kwargs["books"]
    projected: dict[str, dict[str, dict[str, Any]]] = {}
    for asset, outcomes in books.items():
        projected[asset] = {
            outcome: {**book, "microprice": book.get("best_ask")}
            for outcome, book in outcomes.items()
        }
    signal, blockers = _base_choose_signal(**{**kwargs, "books": projected})
    if signal:
        complement_book = books["BTC"][signal["complement"]]
        signal["fair_basis"] = "one_minus_complement_best_ask"
        signal["complement_best_ask"] = complement_book.get("best_ask")
    return signal, blockers


def build_intent(signal: dict[str, Any], market: dict[str, Any], now: float) -> dict[str, Any]:
    intent = _base_build_intent(signal, market, now)
    intent["policy_id"] = "native_l2_complement_ask_cap_parity_stale_ask_costed_cap1"
    intent["reason"] = "native receipt-clock complement ask cap fair before ask reprice"
    return intent


def reduce_generation(
    terminals: list[dict[str, Any]],
    events: list[dict[str, Any]],
    resolutions: dict[str, str],
) -> dict[str, Any]:
    prior_checksum, prior_forward = base.CHECKSUM, base.FORWARD_START_S
    base.CHECKSUM, base.FORWARD_START_S = CHECKSUM, FORWARD_START_S
    try:
        reduced = _base_reduce_generation(terminals, events, resolutions)
    finally:
        base.CHECKSUM, base.FORWARD_START_S = prior_checksum, prior_forward
    integrity = reduced.get("measured_integrity") or {}
    if any(int(value or 0) > 0 for value in integrity.values()):
        reduced["status"] = "PARK_IRRECOVERABLE_INTEGRITY_DISAGREEMENT"
        reduced["stop_writer"] = True
    return reduced


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
    base.choose_signal = choose_signal
    base.build_intent = build_intent
    base.reduce_generation = reduce_generation
    base.ARBITER_SOURCES = [
        source
        for source in base.ARBITER_SOURCES
        if "cross_outcome_parity_stale_ask_selector" not in source
        and "depth_weighted_microprice_parity_stale_ask_selector" not in source
        and "complement_bid_support_parity_stale_ask_selector" not in source
    ] + [SELECTOR]


def main() -> int:
    configure_base()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
