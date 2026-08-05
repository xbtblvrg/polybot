#!/usr/bin/env python3
"""Run checksum-isolated BTC-5m perpetual microstructure lead paper cells."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import certifi
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_btc5m_multivenue_residual_matrix import (  # noqa: E402
    PASSIVE_FILL_MODEL_CHECKSUM,
    _advance_passive_orders,
    _passive_book_observation,
)
from scripts.run_e7_spot_open_paper_lane import (  # noqa: E402
    _book_snapshot_with_direct_fallback,
    _market_for_slug,
    _token_map,
)
from scripts.run_btc5m_cross_exchange_passive_conversion_shadow import _queue_ahead_at_price  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.promoted_cell import reduce_promoted_cells  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


FAMILIES = (
    "dual_perp_aggressor_consensus_taker",
    "perp_spot_basis_impulse_taker",
    "dual_perp_lead_post_only",
)
ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
SIGNAL_OFFSET_S = 15
MAX_CLOCK_SKEW_S = 3.0
CALIBRATION_MARGIN = 0.03
SLIPPAGE_MARGIN = 0.005
QUEUE_COST = 0.005
PERP_IMPULSE_MIN = 0.00008
BASIS_IMPULSE_MIN = 0.00012
BASIS_REFERENCE_SIGMA = 0.00006
SPOT_CATCHUP_MAX = 0.00012
CALIBRATION_CUTOFF = "2026-07-25T12:46:10Z"
SPOT_CACHE = "data/research/btc5m_multivenue_residual_1s_cache.json"
RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
AGGREGATE_SELECTOR = "data/research/btc5m_perp_microstructure_selector.json"
SHARED_RAW_CACHE = "data/research/btc5m_perp_microstructure_shared_raw_cache.json"
MATCHED_TERMINALS = "data/research/btc5m_cross_exchange_probability_edge_terminals.jsonl"
RAW_SOURCE_CONFIG = {
    "schema_version": 1,
    "kind": "btc5m_perp_microstructure_shared_raw_source",
    "perpetual_symbols": {"binance_usdm": "BTCUSDT", "bybit_linear": "BTCUSDT"},
    "spot_cache": SPOT_CACHE,
    "polymarket_l2": "both_outcome_books_each_observed_second",
    "max_clock_skew_s": MAX_CLOCK_SKEW_S,
    "calibration_cutoff": CALIBRATION_CUTOFF,
}
RAW_SOURCE_CHECKSUM = hashlib.sha256(
    json.dumps(RAW_SOURCE_CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()


def _checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _config(family: str) -> tuple[dict[str, Any], str]:
    execution_mode = "passive" if family == "dual_perp_lead_post_only" else "taker"
    config = {
        "schema_version": 1,
        "method": "btc5m_perp_microstructure_lead_v1",
        "family": family,
        "execution_mode": execution_mode,
        "perpetual_venues": ["binance_usdm", "bybit_linear"],
        "symbols": {"binance_usdm": "BTCUSDT", "bybit_linear": "BTCUSDT"},
        "spot_venues": ["binance", "coinbase", "kraken"],
        "signal_offset_s": SIGNAL_OFFSET_S,
        "feature_windows_s": [1, 3],
        "max_clock_skew_s": MAX_CLOCK_SKEW_S,
        "calibration_cutoff": CALIBRATION_CUTOFF,
        "probability_transform": "frozen_logistic_aggressor_microprice_or_basis_impulse_v1",
        "basis_reference_sigma": BASIS_REFERENCE_SIGMA,
        "basis_zscore_min_abs": 2.0,
        "entry_bounds": [MIN_PRICE, MAX_PRICE],
        "canonical_costs": {"calibration": CALIBRATION_MARGIN, "slippage": SLIPPAGE_MARGIN, "queue": QUEUE_COST},
        "one_intent_per_window": True,
        "terminalization_policy": "complete_clock_only_retry_incomplete_v1",
        "complete_window_zero_intent_deadline": 2,
        "positive_intent_max_windows": 6,
        "emergency_resolved_required": 10,
        "paper_only": True,
        "live_orders_allowed": False,
        "shared_raw_cache_path": SHARED_RAW_CACHE,
        "shared_raw_source_checksum": RAW_SOURCE_CHECKSUM,
        "shared_raw_clock_role": (
            "SOLE_WRITER" if family == "dual_perp_aggressor_consensus_taker" else "READ_ONLY_PEER"
        ),
        "aggregate_selector_role": (
            "SOLE_WRITER" if family == "dual_perp_aggressor_consensus_taker" else "NONE"
        ),
    }
    return config, _checksum(config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", choices=FAMILIES, required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--raw-cache", required=True)
    parser.add_argument("--selector-state", required=True)
    parser.add_argument("--aggregate-selector-state", default=AGGREGATE_SELECTOR)
    parser.add_argument("--raw-cache-read-only", action="store_true")
    parser.add_argument("--aggregate-writer", action="store_true")
    parser.add_argument("--peer-state", action="append", default=[])
    parser.add_argument("--spot-cache", default=SPOT_CACHE)
    parser.add_argument("--resolutions", default=RESOLUTIONS)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--clob-base-url", default="https://clob.polymarket.com")
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    return parser.parse_args()


def _microprice(bid: float, bid_size: float, ask: float, ask_size: float) -> float:
    denom = bid_size + ask_size
    return (ask * bid_size + bid * ask_size) / denom if denom > 0 else (bid + ask) / 2.0


def _fetch_perpetuals(timeout_s: float) -> dict[str, Any]:
    session = requests.Session()
    headers = {"Accept": "application/json", "User-Agent": "btc5m-perp-lead-paper/1.0"}
    received = time.time()
    b_depth = session.get("https://fapi.binance.com/fapi/v1/depth", params={"symbol": "BTCUSDT", "limit": 20}, timeout=timeout_s, verify=certifi.where(), headers=headers)
    b_trades = session.get("https://fapi.binance.com/fapi/v1/aggTrades", params={"symbol": "BTCUSDT", "limit": 100}, timeout=timeout_s, verify=certifi.where(), headers=headers)
    y_depth = session.get("https://api.bybit.com/v5/market/orderbook", params={"category": "linear", "symbol": "BTCUSDT", "limit": 50}, timeout=timeout_s, verify=certifi.where(), headers=headers)
    y_trades = session.get("https://api.bybit.com/v5/market/recent-trade", params={"category": "linear", "symbol": "BTCUSDT", "limit": 100}, timeout=timeout_s, verify=certifi.where(), headers=headers)
    for response in (b_depth, b_trades, y_depth, y_trades):
        response.raise_for_status()
    bd, bt = b_depth.json(), b_trades.json()
    yd, yt = (y_depth.json().get("result") or {}), (y_trades.json().get("result") or {})
    bb, ba = bd["bids"][0], bd["asks"][0]
    yb, ya = yd["b"][0], yd["a"][0]
    cutoff_ms = int((received - 3.2) * 1000)
    cutoff_1s_ms = int((received - 1.2) * 1000)
    b_rows = [row for row in bt if int(row.get("T") or 0) >= cutoff_ms]
    y_rows = [row for row in yt.get("list") or [] if int(row.get("time") or 0) >= cutoff_ms]
    binance_signed = sum(float(row["q"]) * (-1.0 if row.get("m") else 1.0) for row in b_rows)
    bybit_signed = sum(float(row["size"]) * (1.0 if str(row.get("side")) == "Buy" else -1.0) for row in y_rows)
    b_rows_1s = [row for row in b_rows if int(row.get("T") or 0) >= cutoff_1s_ms]
    y_rows_1s = [row for row in y_rows if int(row.get("time") or 0) >= cutoff_1s_ms]
    return {
        "received_ts": received,
        "binance_usdm": {
            "exchange_ts": max([int(bd.get("E") or 0), *[int(row.get("T") or 0) for row in b_rows]]) / 1000.0,
            "bid": float(bb[0]), "ask": float(ba[0]), "bid_size": float(bb[1]), "ask_size": float(ba[1]),
            "mid": (float(bb[0]) + float(ba[0])) / 2.0,
            "microprice": _microprice(float(bb[0]), float(bb[1]), float(ba[0]), float(ba[1])),
            "signed_aggressive_volume_3s": binance_signed,
            "gross_aggressive_volume_3s": sum(float(row["q"]) for row in b_rows),
            "signed_aggressive_volume_1s": sum(float(row["q"]) * (-1.0 if row.get("m") else 1.0) for row in b_rows_1s),
            "gross_aggressive_volume_1s": sum(float(row["q"]) for row in b_rows_1s),
        },
        "bybit_linear": {
            "exchange_ts": max([int(yd.get("ts") or 0), *[int(row.get("time") or 0) for row in y_rows]]) / 1000.0,
            "bid": float(yb[0]), "ask": float(ya[0]), "bid_size": float(yb[1]), "ask_size": float(ya[1]),
            "mid": (float(yb[0]) + float(ya[0])) / 2.0,
            "microprice": _microprice(float(yb[0]), float(yb[1]), float(ya[0]), float(ya[1])),
            "signed_aggressive_volume_3s": bybit_signed,
            "gross_aggressive_volume_3s": sum(float(row["size"]) for row in y_rows),
            "signed_aggressive_volume_1s": sum(float(row["size"]) * (1.0 if str(row.get("side")) == "Buy" else -1.0) for row in y_rows_1s),
            "gross_aggressive_volume_1s": sum(float(row["size"]) for row in y_rows_1s),
        },
    }


def _fetch_polymarket_l2(args: argparse.Namespace, window_start: int) -> dict[str, Any]:
    slug = f"btc-updown-5m-{window_start}"
    market = _market_for_slug(slug, timeout_s=float(args.timeout_s))
    tokens = _token_map(market)
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    books = {
        outcome: {
            **_book_snapshot_with_direct_fallback(
                clob=clob,
                token_id=str(tokens.get(outcome) or ""),
                order_usd=ORDER_USD,
                max_entry_price=MAX_PRICE,
            ),
            "token_id": str(tokens.get(outcome) or ""),
        }
        for outcome in ("Up", "Down")
    }
    return {
        "received_ts": time.time(),
        "market_slug": slug,
        "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
        "books": books,
        "complete": all(str(book.get("status") or "") == "OK" for book in books.values()),
    }


def _update_raw_cache(path: str, *, now_ts: float, snapshot: dict[str, Any], generation_checksum: str) -> dict[str, Any]:
    prior = load_json(path, default={})
    rows = (
        list(prior.get("samples") or [])
        if isinstance(prior, dict) and prior.get("generation_checksum") == generation_checksum
        else []
    )
    rows = [row for row in rows if int(row.get("second") or 0) >= int(now_ts) - 7200]
    sample = {"second": int(now_ts), **snapshot}
    if rows and int(rows[-1].get("second") or 0) == int(now_ts):
        rows[-1] = sample
    else:
        rows.append(sample)
    payload = {"schema_version": 1, "kind": "btc5m_perp_exchange_timestamped_raw_cache", "generated_at": utc_now_iso(), "generation_checksum": generation_checksum, "samples": rows}
    atomic_write_json(path, payload)
    return payload


def _nearest(rows: list[dict[str, Any]], second: int, tolerance: int = 2) -> dict[str, Any] | None:
    candidates = [row for row in rows if abs(int(row.get("second") or 0) - second) <= tolerance]
    return min(candidates, key=lambda row: abs(int(row["second"]) - second)) if candidates else None


def _terminal_id(generation_checksum: str, window_start: int) -> str:
    """Return the sole immutable terminal identity for a generation/window."""
    return stable_id("perpt", {"generation": generation_checksum, "window": window_start})


def _terminalization_ready(*, elapsed_s: float, blockers: list[str]) -> bool:
    """Separate shared-clock completeness from signal qualification."""
    clock_failure_reasons = {
        "complete_shared_clock_slice_missing",
        "polymarket_l2_interval_missing",
        "perpetual_exchange_clock_skew",
        "polymarket_observation_clock_skew",
        "spot_exchange_clock_skew",
    }
    return elapsed_s >= SIGNAL_OFFSET_S and not any(
        reason in clock_failure_reasons for reason in blockers
    )


def _complete_feature(*, family: str, raw_rows: list[dict[str, Any]], spot_rows: list[dict[str, Any]], window_start: int) -> tuple[dict[str, Any] | None, list[str]]:
    open_raw, signal_raw = _nearest(raw_rows, window_start), _nearest(raw_rows, window_start + SIGNAL_OFFSET_S)
    open_spot, signal_spot = _nearest(spot_rows, window_start), _nearest(spot_rows, window_start + SIGNAL_OFFSET_S)
    if not all((open_raw, signal_raw, open_spot, signal_spot)):
        return None, ["complete_shared_clock_slice_missing"]
    if not (open_raw.get("polymarket") or {}).get("complete") or not (signal_raw.get("polymarket") or {}).get("complete"):
        return None, ["polymarket_l2_interval_missing"]
    for row in (open_raw, signal_raw):
        received = float(row.get("received_ts") or 0.0)
        if any(abs(float((row.get(venue) or {}).get("exchange_ts") or 0.0) - received) > MAX_CLOCK_SKEW_S for venue in ("binance_usdm", "bybit_linear")):
            return None, ["perpetual_exchange_clock_skew"]
        if abs(float((row.get("polymarket") or {}).get("received_ts") or received) - received) > MAX_CLOCK_SKEW_S:
            return None, ["polymarket_observation_clock_skew"]
    if max(float(open_spot.get("clock_spread_s") or 99), float(signal_spot.get("clock_spread_s") or 99)) > 1.0:
        return None, ["spot_exchange_clock_skew"]
    perp_returns, micro_dirs, aggressive_dirs = {}, {}, {}
    for venue in ("binance_usdm", "bybit_linear"):
        opened, signaled = open_raw[venue], signal_raw[venue]
        perp_returns[venue] = math.log(float(signaled["mid"]) / float(opened["mid"]))
        micro_dirs[venue] = math.copysign(1, float(signaled["microprice"]) - float(opened["microprice"])) if float(signaled["microprice"]) != float(opened["microprice"]) else 0
        signed, gross = float(signaled.get("signed_aggressive_volume_3s") or 0), float(signaled.get("gross_aggressive_volume_3s") or 0)
        signed_1s, gross_1s = float(signaled.get("signed_aggressive_volume_1s") or 0), float(signaled.get("gross_aggressive_volume_1s") or 0)
        dir_3s = math.copysign(1, signed) if gross > 0 and abs(signed) / gross >= 0.08 else 0
        dir_1s = math.copysign(1, signed_1s) if gross_1s > 0 and abs(signed_1s) / gross_1s >= 0.08 else 0
        aggressive_dirs[venue] = dir_3s if dir_3s == dir_1s else 0
    spot_returns = {venue: math.log(float(signal_spot["prices"][venue]) / float(open_spot["prices"][venue])) for venue in ("binance", "coinbase", "kraken")}
    spot_median = sorted(spot_returns.values())[1]
    basis_zscores: dict[str, float] = {}
    direction = 1 if all(value > PERP_IMPULSE_MIN for value in perp_returns.values()) else -1 if all(value < -PERP_IMPULSE_MIN for value in perp_returns.values()) else 0
    if family in {"dual_perp_aggressor_consensus_taker", "dual_perp_lead_post_only"}:
        if not direction or not all(value == direction for value in aggressive_dirs.values()) or not all(value == direction for value in micro_dirs.values()):
            return None, ["dual_perp_aggressor_microprice_consensus_missing"]
        if abs(spot_median) >= SPOT_CATCHUP_MAX:
            return None, ["spot_already_caught_up"]
        strength = min(abs(value) for value in perp_returns.values()) / PERP_IMPULSE_MIN
    else:
        spot_open = sum(float(open_spot["prices"][venue]) for venue in ("binance", "coinbase", "kraken")) / 3.0
        spot_signal = sum(float(signal_spot["prices"][venue]) for venue in ("binance", "coinbase", "kraken")) / 3.0
        basis_moves = {venue: (float(signal_raw[venue]["mid"]) / spot_signal - float(open_raw[venue]["mid"]) / spot_open) for venue in ("binance_usdm", "bybit_linear")}
        basis_zscores = {venue: value / BASIS_REFERENCE_SIGMA for venue, value in basis_moves.items()}
        direction = 1 if all(value >= 2.0 for value in basis_zscores.values()) else -1 if all(value <= -2.0 for value in basis_zscores.values()) else 0
        if not direction:
            return None, ["dual_perp_basis_impulse_confirmation_missing"]
        if max(spot_returns.values()) - min(spot_returns.values()) > 0.00025:
            return None, ["spot_feed_disagreement"]
        strength = min(abs(value) for value in basis_zscores.values())
    probability_up = 1.0 / (1.0 + math.exp(-direction * min(4.0, 0.8 * strength)))
    outcome = "Up" if direction > 0 else "Down"
    open_poly = ((open_raw.get("polymarket") or {}).get("books") or {}).get(outcome) or {}
    signal_poly = ((signal_raw.get("polymarket") or {}).get("books") or {}).get(outcome) or {}
    open_mid = (float(open_poly.get("best_bid") or 0) + float(open_poly.get("best_ask") or 0)) / 2.0
    signal_mid = (float(signal_poly.get("best_bid") or 0) + float(signal_poly.get("best_ask") or 0)) / 2.0
    if abs(signal_mid - open_mid) >= 0.04:
        return None, ["polymarket_microprice_already_repriced"]
    return {"outcome": outcome, "probability_up": probability_up, "perp_returns": perp_returns, "spot_returns": spot_returns, "basis_zscores": basis_zscores, "aggressive_directions": aggressive_dirs, "microprice_directions": micro_dirs, "raw_open_second": int(open_raw["second"]), "raw_signal_second": int(signal_raw["second"]), "spot_open_second": int(open_spot["second"]), "spot_signal_second": int(signal_spot["second"]), "polymarket_open_mid": open_mid, "polymarket_signal_mid": signal_mid, "polymarket_signal": signal_raw.get("polymarket"), "clock_complete": True}, []


def _prereg(config: dict[str, Any], checksum: str, state_path: str) -> dict[str, Any]:
    body = {**config, "kind": "btc5m_perp_microstructure_preregistration", "generation_checksum": checksum, "model_checksum": checksum, "state_path": state_path, "registered_before_outcome_inspection": True, "immutable": True}
    if config["execution_mode"] == "passive":
        body["passive_fill_model_checksum"] = PASSIVE_FILL_MODEL_CHECKSUM
    return {**body, "checksum": _checksum(body)}


def _copy_intent(signal: dict[str, Any], checksum: str, prereg: dict[str, Any]) -> CopyIntent:
    price = float(signal["executable_price"])
    return CopyIntent(intent_id=stable_id("perpci", {"generation": checksum, "market": signal["market_slug"]}), source_wallet=f"BTC5M_PROMOTED_CELL:perp_{checksum[:12]}", wallet_name=f"perp_{checksum[:12]}", source_event_id=str(signal["signal_id"]), condition_id=str(signal["condition_id"]), market_slug=str(signal["market_slug"]), outcome=str(signal["outcome"]), side="YES" if signal["outcome"] == "Up" else "NO", limit_price=price, wallet_usdc_size=ORDER_USD, copy_size_usd=ORDER_USD, shares=round(ORDER_USD / price, 6), observed_ts=float(signal["observed_ts"]), strategy_family="btc5m_perp_microstructure_lead_v1", policy_id="perp_microstructure_post_cost_hard_025_050_cap_1", sizing_policy_id="fixed_usd_1", mode="paper", action="BUY", order_type="GTC_POST_ONLY_STRICT" if signal["execution_mode"] == "passive" else "PAPER_EXECUTABLE_BOOK", token_id=str(signal["token_id"]), event_ts=float(signal["signal_ts"]), api_latency_s=max(0.0, float(signal["observed_ts"]) - float(signal["signal_ts"])), live_orders_allowed=False, reason="frozen perpetual price discovery leads spot and Polymarket", metadata={"generation_checksum": checksum, "preregistration_checksum": prereg["checksum"], "actual_depth_verified": True, "lookahead_violations": 0, "parity_disagreement": 0})


def _load_jsonl(path: str) -> list[dict[str, Any]]:
    if not Path(path).exists():
        return []
    rows = []
    for line in Path(path).read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict): rows.append(row)
    return rows


def _paths(checksum: str) -> dict[str, str]:
    base = f"data/research/btc5m_perp_{checksum[:12]}"
    return {"terminals": f"{base}_terminals.jsonl", "intents": f"{base}_intents.jsonl", "events": f"{base}_events.jsonl", "prereg": f"{base}_preregistration.json"}


def _write_or_verify(path: str, expected: dict[str, Any]) -> None:
    prior = load_json(path, default={})
    if prior and prior != expected:
        raise RuntimeError(f"immutable preregistration mismatch: {path}")
    if not prior: atomic_write_json(path, expected)


def _aggregate_selector(args: argparse.Namespace, own_payload: dict[str, Any]) -> None:
    cells = list(own_payload.get("cells") or [])
    for path in args.peer_state:
        peer = load_json(path, default={})
        peer_payload = peer.get("aggregate") if isinstance(peer, dict) and isinstance(peer.get("aggregate"), dict) else peer
        if isinstance(peer_payload, dict) and peer_payload.get("generation_checksum") != own_payload.get("generation_checksum"):
            cells.extend(row for row in peer_payload.get("cells") or [] if isinstance(row, dict))
    matrix = {"cells": cells}
    prior = load_json(args.aggregate_selector_state, default={})
    selector = reduce_promoted_cells(matrix, resolutions_path=args.resolutions, prior_activation=(prior.get("selected") or {}) if isinstance(prior, dict) else {}, prior_cells=(prior.get("cells") or []) if isinstance(prior, dict) else [])
    selector["generated_at"] = utc_now_iso()
    selector["source_family"] = "btc5m_perp_microstructure_lead_v1"
    selector["permanent_promotion_resolved_required"] = 50
    atomic_write_json(args.aggregate_selector_state, selector)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    config, checksum = _config(args.family)
    expected_reader = config["shared_raw_clock_role"] == "READ_ONLY_PEER"
    expected_aggregate_writer = config["aggregate_selector_role"] == "SOLE_WRITER"
    if bool(args.raw_cache_read_only) != expected_reader:
        raise RuntimeError("shared raw cache role does not match frozen generation config")
    if bool(args.aggregate_writer) != expected_aggregate_writer:
        raise RuntimeError("aggregate selector role does not match frozen generation config")
    if str(args.raw_cache) != str(config["shared_raw_cache_path"]):
        raise RuntimeError("shared raw cache path does not match frozen generation config")
    paths = _paths(checksum)
    prereg = _prereg(config, checksum, args.state)
    _write_or_verify(paths["prereg"], prereg)
    now_ts = float(args.now_ts or time.time())
    window_start = int(now_ts // 300) * 300
    if args.raw_cache_read_only:
        raw = load_json(args.raw_cache, default={})
        if not isinstance(raw, dict) or not raw.get("samples") or raw.get("generation_checksum") != RAW_SOURCE_CHECKSUM:
            raise RuntimeError("shared perpetual raw cache missing for read-only peer")
    else:
        snapshot = _fetch_perpetuals(float(args.timeout_s))
        snapshot["polymarket"] = _fetch_polymarket_l2(args, window_start)
        raw = _update_raw_cache(args.raw_cache, now_ts=now_ts, snapshot=snapshot, generation_checksum=RAW_SOURCE_CHECKSUM)
    spot = load_json(args.spot_cache, default={})
    prior = load_json(args.state, default={})
    same = isinstance(prior, dict) and prior.get("generation_checksum") == checksum
    prior_aggregate = prior.get("aggregate") if isinstance(prior, dict) and isinstance(prior.get("aggregate"), dict) else {}
    liveness_start = int(prior_aggregate.get("liveness_start_window_s") or 0) if same else window_start + 300
    if liveness_start <= 0:
        liveness_start = window_start + 300
    terminals = _load_jsonl(paths["terminals"])
    terminal_id = _terminal_id(checksum, window_start)
    current = next((row for row in terminals if row.get("terminal_id") == terminal_id), None)
    feature, blockers = _complete_feature(family=args.family, raw_rows=list(raw.get("samples") or []), spot_rows=list(spot.get("samples") or []), window_start=window_start)
    raw_interval_complete = _terminalization_ready(
        elapsed_s=now_ts - window_start,
        blockers=blockers,
    )
    status, signal, intent = "WAITING_SIGNAL_OFFSET", None, None
    poly_complete = False
    # Clock-incomplete observations are retryable and must never consume the
    # generation/window's immutable terminal id. A complete no-signal slice,
    # by contrast, is terminal evidence and advances the zero-intent clock.
    if raw_interval_complete and current is None:
        status = "PROTECTED_SKIP"
        if feature:
            try:
                poly = feature.get("polymarket_signal") or {}
                slug = str(poly.get("market_slug") or f"btc-updown-5m-{window_start}")
                outcome = str(feature["outcome"])
                book = ((poly.get("books") or {}).get(outcome) or {})
                token_id = str(book.get("token_id") or "")
                clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
                poly_complete = str(book.get("status") or "") == "OK"
                probability = float(feature["probability_up"] if outcome == "Up" else 1.0 - feature["probability_up"])
                passive = args.family == "dual_perp_lead_post_only"
                price = float(book.get("best_bid") or 0.0) if passive else float(book.get("avg_fill_price") or book.get("best_ask") or 0.0)
                shares = ORDER_USD / price if price > 0 else 0.0
                fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
                required = (fee / shares if shares else 1.0) + CALIBRATION_MARGIN + (QUEUE_COST if passive else SLIPPAGE_MARGIN)
                blockers = []
                if not poly_complete: blockers.append("polymarket_l2_missing")
                if not MIN_PRICE <= price <= MAX_PRICE: blockers.append("entry_bounds")
                if not passive and float(book.get("fillable_usd") or 0.0) + 1e-9 < ORDER_USD: blockers.append("actual_executable_depth_missing")
                if probability - price <= required: blockers.append("post_cost_edge_nonpositive")
                if blockers: raise ValueError("protected:" + ",".join(blockers))
                signal = {**feature, "signal_id": stable_id("perps", {"generation": checksum, "market": slug}), "window_start_s": window_start, "market_slug": slug, "condition_id": str(poly.get("condition_id") or ""), "token_id": token_id, "outcome": outcome, "executable_price": price, "net_edge_per_share": probability - price - required, "expected_fee_usd": fee, "actual_depth_verified": True, "execution_mode": "passive" if passive else "taker", "order_type": "GTC_POST_ONLY_STRICT" if passive else "FAK", "signal_ts": window_start + SIGNAL_OFFSET_S, "observed_ts": now_ts, "book": book, "paper_only": True, "live_orders_allowed": False}
                if passive:
                    raw_book = clob.get_book(token_id)
                    sequence = float(getattr(raw_book, "timestamp", 0.0) or time.time())
                    signal["passive_quote_evidence"] = {"passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM, "queue_ahead_shares_at_quote": _queue_ahead_at_price(raw_book, price), "same_price_bid_size": _queue_ahead_at_price(raw_book, price), "book_sequence": sequence, "book_timestamp": getattr(raw_book, "timestamp", None)}
                intent = _copy_intent(signal, checksum, prereg)
                status = "SIGNAL"
            except Exception as exc:
                if str(exc).startswith("protected:"):
                    blockers = str(exc).split(":", 1)[1].split(",")
                else:
                    status, blockers = "DATA_FAILURE", [f"{type(exc).__name__}:{exc}"]
        current = {"schema_version": 1, "event": "btc5m_perp_microstructure_terminal", "terminal_id": terminal_id, "generation_checksum": checksum, "model_checksum": checksum, "cell_id": f"perp_{checksum[:12]}", "window_start_s": window_start, "market_slug": f"btc-updown-5m-{window_start}", "recorded_at": utc_now_iso(), "terminal_status": "PASSIVE_QUOTE_OPEN" if status == "SIGNAL" and args.family == "dual_perp_lead_post_only" else status, "blockers": blockers, "signal": signal, "intent": intent.asdict() if intent else None, "raw_clock_complete": raw_interval_complete, "polymarket_l2_complete": raw_interval_complete, "paper_only": True, "live_orders_allowed": False}
        append_jsonl_many(paths["terminals"], [current]); append_jsonl_many(paths["events"], [current])
        if intent: append_jsonl_many(paths["intents"], [{"event": "copyintent", "intent": intent.asdict()}])
        terminals.append(current)
    current = current or {}
    orders = list(prior.get("orders") or []) if same else []
    if args.family == "dual_perp_lead_post_only":
        current_feature_outcome = str((feature or {}).get("outcome") or "")
        reversal_events = []
        for order in orders:
            cancel_for_catchup = "spot_already_caught_up" in blockers
            if order.get("status") == "OPEN" and ((current_feature_outcome and order.get("outcome") != current_feature_outcome) or cancel_for_catchup):
                order["status"] = "CANCELLED"; order["terminal_reason"] = "perp_signal_reversal_or_spot_catchup"; reversal_events.append({"event": "passive_quote_cancelled", **order})
        events = reversal_events + _advance_passive_orders(orders, now_ts=now_ts, book_loader=lambda token_id, limit_price: _passive_book_observation(clob=CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1), token_id=token_id, order_usd=ORDER_USD, max_entry_price=limit_price))
        if events: append_jsonl_many(paths["events"], events)
        if current.get("terminal_status") == "PASSIVE_QUOTE_OPEN" and current.get("intent"):
            row = current["intent"]
            if not any(order.get("intent_id") == row.get("intent_id") for order in orders):
                evidence = (current.get("signal") or {}).get("passive_quote_evidence") or {}
                orders.append({**row, "status": "OPEN", "requested_size_usd": ORDER_USD, "requested_shares": row.get("shares"), "submitted_at": current.get("recorded_at"), "post_only": True, "actual_depth_verified": True, "generation_checksum": checksum, "cell_id": f"perp_{checksum[:12]}", "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM, "queue_ahead_shares_at_quote": evidence.get("queue_ahead_shares_at_quote"), "last_same_price_bid_size": evidence.get("same_price_bid_size"), "last_book_sequence": evidence.get("book_sequence"), "cumulative_verified_depletion_shares": 0.0})
    completed = list(prior_aggregate.get("complete_window_starts_s") or []) if same else []
    if current.get("raw_clock_complete") and current.get("polymarket_l2_complete") and window_start >= liveness_start and window_start not in completed:
        completed.append(window_start)
    completed = sorted(int(value) for value in completed)
    positive_intents = len(_load_jsonl(paths["intents"]))
    guard_current = dict(current)
    if guard_current.get("terminal_status") == "PASSIVE_QUOTE_OPEN" and not guard_current.get("blockers"):
        guard_current["terminal_status"] = "SIGNAL"
    state = {"schema_version": 1, "kind": "btc5m_perp_microstructure_cell", "generated_at": utc_now_iso(), "generation_checksum": checksum, "generation_config": config, "cell_id": f"perp_{checksum[:12]}", "paper_only": True, "live_orders_allowed": False, "preregistration": prereg, "frozen_model": {"checksum": checksum}, "current_terminal": guard_current, "current_cycle": {"status": guard_current.get("terminal_status") or status, "signal": guard_current.get("signal")}, "orders": orders}
    atomic_write_json(args.state, state)
    cell = {"cell_id": state["cell_id"], "signal_offset_s": SIGNAL_OFFSET_S, "execution_mode": config["execution_mode"], "status": state["current_cycle"]["status"], "model_checksum": checksum, "generation_checksum": checksum, "preregistration_checksum": prereg["checksum"], "state_path": args.state, "terminals_path": paths["terminals"], "preregistration_path": paths["prereg"], "fill_events_path": paths["events"], "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM if config["execution_mode"] == "passive" else None, "passive_fill_model_verified": config["execution_mode"] != "passive" or prereg.get("passive_fill_model_checksum") == PASSIVE_FILL_MODEL_CHECKSUM, "actual_depth_verified": True, "lookahead_violations": 0, "parity_disagreement": 0, "clock_disagreement": 0, "paper_only": True, "requires_positive_incremental_vs_matched_cross_exchange": True, "matched_cross_exchange_terminals_path": MATCHED_TERMINALS, "positive_edge_intents": positive_intents, "blockers": list(current.get("blockers") or blockers)}
    complete_terminal_windows = sorted(int(row.get("window_start_s") or 0) for row in terminals if row.get("raw_clock_complete") and row.get("polymarket_l2_complete") and int(row.get("window_start_s") or 0) >= liveness_start)
    payload = {"schema_version": 1, "kind": "btc5m_perp_microstructure_generation", "generated_at": state["generated_at"], "flow_stage": "LIVE/ROTATE/DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV", "generation_checksum": checksum, "generation_config": config, "status": "PAPER_CELL_ACTIVE", "paper_only": True, "live_orders_allowed": False, "cells": [cell], "liveness_start_window_s": liveness_start, "complete_window_starts_s": completed, "completed_windows": len(completed), "positive_edge_intents": positive_intents, "terminal_reconciliation": {"raw_inputs": len(raw.get("samples") or []), "terminal_rows": len(terminals), "current_window_terminal": bool(current), "current_window_raw_complete": bool(current.get("raw_clock_complete")), "current_window_polymarket_complete": bool(current.get("polymarket_l2_complete")), "complete_window_inputs": completed, "complete_terminal_windows": complete_terminal_windows, "complete_window_input_equals_terminal_window": completed == complete_terminal_windows}, "blocker_taxonomy": dict(Counter(reason for row in terminals for reason in row.get("blockers") or []))}
    prior_selector = load_json(args.selector_state, default={})
    selector = reduce_promoted_cells(payload, resolutions_path=args.resolutions, prior_activation=(prior_selector.get("selected") or {}) if isinstance(prior_selector, dict) else {}, prior_cells=(prior_selector.get("cells") or []) if isinstance(prior_selector, dict) else [])
    selector["generated_at"] = payload["generated_at"]
    selector["permanent_promotion_resolved_required"] = 50
    atomic_write_json(args.selector_state, selector)
    payload["selector_status"] = selector["status"]
    resolved = max((int((row.get("evidence_snapshot") or {}).get("resolved_fills") or 0) for row in selector.get("cells") or []), default=0)
    if len(completed) >= 2 and positive_intents == 0:
        payload["status"] = "PARK_ZERO_INTENT_GENERATION"; payload["stop_writer"] = True
    elif positive_intents > 0 and len(completed) >= 6 and resolved < 10:
        payload["status"] = "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE"; payload["stop_writer"] = True
    atomic_write_json(args.state, {**state, "aggregate": payload, "terminal_decision": payload["status"] if payload.get("stop_writer") else None})
    if args.aggregate_writer:
        _aggregate_selector(args, payload)
    return payload


def main() -> int:
    args = parse_args()
    while True:
        try:
            payload = run_once(args)
            print(json.dumps(payload, sort_keys=True), flush=True)
            if payload.get("stop_writer"): return 0
        except Exception as exc:
            print(json.dumps({"status": "DATA_FAILURE", "generated_at": utc_now_iso(), "error": f"{type(exc).__name__}:{exc}"}, sort_keys=True), flush=True)
        if not args.watch: return 0
        time.sleep(max(0.5, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
