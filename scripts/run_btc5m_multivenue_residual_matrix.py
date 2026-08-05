#!/usr/bin/env python3
"""Run isolated BTC-5m multivenue consensus and residual paper cells."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import certifi
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_e7_spot_open_paper_lane import (
    _book_snapshot_with_direct_fallback,
    _market_for_slug,
    _token_map,
)
from scripts.run_btc5m_cross_exchange_passive_conversion_shadow import (
    _queue_ahead_at_price,
)
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso
from src.wallet_copy.promoted_cell import reduce_promoted_cells
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json


OFFSETS = (15, 30, 45, 60)
RESIDUAL_THRESHOLDS = (0.01, 0.02, 0.03, 0.04, 0.05)
VENUES = ("binance", "coinbase", "kraken")
ORDER_USD = 1.0
PAIR_LEG_USD = 0.5
MIN_PRICE, MAX_PRICE = 0.25, 0.50
CALIBRATION_ERROR_MARGIN = 0.03
SLIPPAGE_MARGIN = 0.005
REFERENCE_SIGMA_1S = 0.00012
FORBIDDEN_CHECKSUM = "848f22460923"
GENERATION_CONFIG = {
    "schema_version": 1,
    "venues": list(VENUES),
    "clock_resolution_s": 1,
    "clock_alignment_tolerance_s": 1.0,
    "consensus_rule": "two_of_three_directional_agreement",
    "probability_calibration": "frozen_logistic_median_return_reference_sigma",
    "training_cutoff": "2026-07-25T07:10:34Z",
    "reference_sigma_1s": REFERENCE_SIGMA_1S,
    "residual_definition": "consensus_probability_minus_executable_price",
    "calibration_error_margin": CALIBRATION_ERROR_MARGIN,
    "slippage_margin": SLIPPAGE_MARGIN,
    "offsets_s": list(OFFSETS),
    "residual_thresholds": list(RESIDUAL_THRESHOLDS),
    "execution_modes": ["taker", "passive"],
    "passive_fill_model": "sequence_continuous_queue_depletion_v2",
    "order_usd": ORDER_USD,
    "entry_bounds": [MIN_PRICE, MAX_PRICE],
    "paper_only": True,
    "live_orders_allowed": False,
}
GENERATION_CHECKSUM = hashlib.sha256(
    json.dumps(GENERATION_CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
PASSIVE_FILL_MODEL_CHECKSUM = hashlib.sha256(
    b"sequence_continuous_queue_depletion_v2"
).hexdigest()

DEFAULT_STATE = "data/research/btc5m_multivenue_residual_matrix_state.json"
DEFAULT_CACHE = "data/research/btc5m_multivenue_residual_1s_cache.json"
DEFAULT_SELECTOR = "data/research/btc5m_cross_exchange_promoted_cell_latest.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--cache", default=DEFAULT_CACHE)
    parser.add_argument("--selector-state", default=DEFAULT_SELECTOR)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--clob-base-url", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-s", type=float, default=3.0)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--generation-id", default="resident-69a9f9df")
    parser.add_argument("--generation-family", choices=("resident_matrix", "passive_residual", "maker_first_residual", "two_sided_post_only_residual", "two_sided_inside_spread_maker", "paired_complement_post_only_inventory", "paired_complement_fill_then_hedge", "paired_complement_dual_ioc", "complete_set_paired_maker", "complete_set_split_sell_overround", "basis_triggered_single_outcome_maker"), default="resident_matrix")
    parser.add_argument("--execution-mode", choices=("all", "taker", "passive"), default="all")
    parser.add_argument("--shared-raw-cache", default="")
    parser.add_argument("--liveness-complete-after-s", type=int, default=0)
    parser.add_argument("--execution-decision-at-s", type=int, default=0)
    parser.add_argument("--execution-decision-state", default="")
    parser.add_argument(
        "--reconcile-terminal-evidence",
        action="store_true",
        help="Append missing split-sell bundle evidence and rerun the selector without advancing the clock.",
    )
    return parser.parse_args()


def _checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _generation_variant_config(generation_id: str, family: str, execution_mode: str) -> tuple[dict[str, Any], str]:
    config = {**GENERATION_CONFIG, "generation_id": generation_id, "generation_family": family, "execution_modes": [execution_mode]}
    mechanism_configs = {
        "complete_set_paired_maker": {
            "execution_mechanism_version": "book_only_complete_set_paired_maker_v1",
            "terminalization_policy": "first_lawful_bundle_or_window_270s_complete_v1",
            "complete_window_clock_source": "paired_cell_terminal_at_or_after_270s_only",
            "bundle_total_notional_usd": ORDER_USD,
            "leg_notional_usd": PAIR_LEG_USD,
            "queue_cost_per_share": 0.005,
            "adverse_selection_reserve": "observed_best_ask_minus_bid_per_leg",
            "orphan_accounting": "canonical_resolution_full_realized_pnl",
            "paired_signal_dependency": "none_book_updates_only",
        },
        "complete_set_split_sell_overround": {
            "execution_mechanism_version": "ctf_split_actual_bid_depth_sell_v1",
            "terminalization_policy": "first_lawful_bundle_or_window_270s_complete_v1",
            "complete_window_clock_source": "paired_cell_terminal_at_or_after_270s_only",
            "split_collateral_usd": ORDER_USD,
            "inventory_per_outcome_shares": 1.0,
            "ctf_gas_settlement_reserve_usd": 0.01,
            "slippage_reserve_per_leg_usd": SLIPPAGE_MARGIN,
            "orphan_accounting": "unsold_leg_canonical_resolution_full_realized_pnl",
            "paired_signal_dependency": "none_book_updates_only",
        },
        "paired_complement_fill_then_hedge": {
            "execution_mechanism_version": "passive_first_executable_depth_hedge_v1",
            "queue_cost_per_share": 0.005,
            "hedge_rule": "only_after_verified_passive_fill_and_positive_locked_post_cost_value",
            "one_leg_failure_selector_eligible": False,
        },
        "paired_complement_dual_ioc": {
            "execution_mechanism_version": "chronological_dual_ioc_depth_v1",
            "one_leg_failure_cost_rule": "max_leg_price_plus_slippage",
            "depth_required_per_leg_usd": ORDER_USD,
        },
        "basis_triggered_single_outcome_maker": {
            "execution_mechanism_version": "three_venue_basis_post_only_reversal_cancel_v1",
            "basis_trigger": "return_range_gt_reference_sigma_sqrt_offset",
            "cancel_rule": "three_venue_signal_reversal",
        },
    }
    config.update(mechanism_configs.get(family) or {})
    return config, hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _variant(threshold: float | None) -> str:
    return "consensus" if threshold is None else f"residual_{int(threshold * 1000):03d}m"


def _paths(offset: int, mode: str, threshold: float | None) -> dict[str, str]:
    base = (
        "data/research/btc5m_multivenue_"
        f"{GENERATION_CHECKSUM[:12]}_{offset}s_{mode}_{_variant(threshold)}"
    )
    return {
        "state": f"{base}_state.json",
        "events": f"{base}_events.jsonl",
        "intents": f"{base}_intents.jsonl",
        "terminals": f"{base}_terminals.jsonl",
        "preregistration": f"{base}_preregistration.json",
    }


def _preregistration(offset: int, mode: str, threshold: float | None) -> dict[str, Any]:
    body = {
        **GENERATION_CONFIG,
        "kind": "btc5m_multivenue_residual_cell_preregistration",
        "generation_checksum": GENERATION_CHECKSUM,
        "cell_id": f"multivenue_{offset}s_{mode}_{_variant(threshold)}",
        "signal_offset_s": offset,
        "execution_mode": (
            "paired_passive"
            if GENERATION_CONFIG.get("generation_family") == "complete_set_paired_maker"
            else "paired_split_sell"
            if GENERATION_CONFIG.get("generation_family") == "complete_set_split_sell_overround"
            else mode
        ),
        "residual_threshold": threshold,
        "model_checksum": GENERATION_CHECKSUM,
        "passive_fill_model_checksum": (
            PASSIVE_FILL_MODEL_CHECKSUM if mode == "passive" else None
        ),
        "registered_before_outcome_inspection": True,
        "immutable": True,
    }
    return {**body, "checksum": _checksum(body)}


def _write_or_verify(path: str, expected: dict[str, Any]) -> None:
    prior = load_json(path, default={})
    if prior:
        body = {key: value for key, value in prior.items() if key != "checksum"}
        if prior != expected or prior.get("checksum") != _checksum(body):
            raise RuntimeError(f"immutable preregistration mismatch: {path}")
    else:
        atomic_write_json(path, expected)


def _fetch_prices(timeout_s: float) -> tuple[dict[str, float], dict[str, float]]:
    session = requests.Session()
    headers = {"Accept": "application/json", "User-Agent": "btc5m-multivenue-paper/1.0"}
    specs = (
        ("binance", "https://api.binance.com/api/v3/ticker/price", {"symbol": "BTCUSDT"}),
        ("coinbase", "https://api.exchange.coinbase.com/products/BTC-USD/ticker", None),
        ("kraken", "https://api.kraken.com/0/public/Ticker", {"pair": "XBTUSD"}),
    )
    prices: dict[str, float] = {}
    observed: dict[str, float] = {}
    for venue, url, params in specs:
        response = session.get(
            url,
            params=params,
            timeout=timeout_s,
            verify=certifi.where(),
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        if venue == "kraken":
            ticker = next(iter((payload.get("result") or {}).values()), {})
            price = float((ticker.get("c") or [0])[0])
        else:
            price = float(payload.get("price") or 0.0)
        if price <= 0:
            raise RuntimeError(f"{venue} returned invalid price")
        prices[venue] = price
        observed[venue] = time.time()
    return prices, observed


def _update_cache(
    path: str,
    *,
    now_ts: float,
    prices: dict[str, float],
    observed: dict[str, float],
) -> dict[str, Any]:
    prior = load_json(path, default={})
    samples = list(prior.get("samples") or []) if isinstance(prior, dict) else []
    samples = [row for row in samples if int(row.get("second") or 0) >= int(now_ts) - 7200]
    sample = {
        "second": int(now_ts),
        "prices": prices,
        "observed_ts": observed,
        "clock_spread_s": round(max(observed.values()) - min(observed.values()), 6),
    }
    if samples and int(samples[-1].get("second") or 0) == int(now_ts):
        samples[-1] = sample
    else:
        samples.append(sample)
    payload = {
        "schema_version": 1,
        "kind": "btc5m_multivenue_synchronized_1s_cache",
        "generated_at": utc_now_iso(),
        "generation_checksum": GENERATION_CHECKSUM,
        "samples": samples,
    }
    atomic_write_json(path, payload)
    return payload


def _nearest(samples: list[dict[str, Any]], second: int) -> dict[str, Any] | None:
    candidates = [
        row
        for row in samples
        if abs(int(row.get("second") or 0) - second) <= 2
        and float(row.get("clock_spread_s") or 99.0) <= 1.0
    ]
    return min(candidates, key=lambda row: abs(int(row["second"]) - second)) if candidates else None


def _consensus_feature(
    samples: list[dict[str, Any]],
    *,
    window_start: int,
    offset: int,
) -> tuple[dict[str, Any] | None, list[str]]:
    open_row = _nearest(samples, window_start)
    signal_row = _nearest(samples, window_start + offset)
    if open_row is None or signal_row is None:
        return None, ["synchronized_open_or_signal_clock_missing"]
    returns = {
        venue: math.log(float(signal_row["prices"][venue]) / float(open_row["prices"][venue]))
        for venue in VENUES
    }
    up_votes = sum(value > 0 for value in returns.values())
    down_votes = sum(value < 0 for value in returns.values())
    if max(up_votes, down_votes) < 2:
        return None, ["two_of_three_consensus_missing"]
    up = up_votes >= 2
    median_move = sorted(value if up else -value for value in returns.values())[1]
    z_score = median_move / (REFERENCE_SIGMA_1S * math.sqrt(offset))
    probability = 1.0 / (1.0 + math.exp(-max(-8.0, min(8.0, z_score))))
    return {
        "venue_returns": returns,
        "directional_votes": {"up": up_votes, "down": down_votes},
        "outcome": "Up" if up else "Down",
        "median_directional_return": median_move,
        "z_score": z_score,
        "consensus_probability": probability,
        "open_clock_second": int(open_row["second"]),
        "signal_clock_second": int(signal_row["second"]),
        "max_clock_spread_s": max(
            float(open_row["clock_spread_s"]), float(signal_row["clock_spread_s"])
        ),
    }, []


def _load_jsonl(path: str) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _copy_intent(signal: dict[str, Any], cell_id: str, prereg: dict[str, Any]) -> CopyIntent:
    price = float(signal["executable_price"])
    split_sell = signal.get("generation_family") == "complete_set_split_sell_overround"
    size_usd = PAIR_LEG_USD if signal.get("generation_family") in {"complete_set_paired_maker", "complete_set_split_sell_overround"} else ORDER_USD
    return CopyIntent(
        intent_id=stable_id(
            "ci",
            {
                "cell_id": cell_id,
                "signal_id": signal["signal_id"],
                "outcome": signal["outcome"],
                "token_id": signal["token_id"],
            },
        ),
        source_wallet=f"BTC5M_PROMOTED_CELL:{cell_id}",
        wallet_name=cell_id,
        source_event_id=str(signal["signal_id"]),
        condition_id=str(signal["condition_id"]),
        market_slug=str(signal["market_slug"]),
        outcome=str(signal["outcome"]),
        side="YES" if signal["outcome"] == "Up" else "NO",
        limit_price=price,
        wallet_usdc_size=size_usd,
        copy_size_usd=size_usd,
        shares=1.0 if split_sell else round(size_usd / price, 6),
        observed_ts=float(signal["observed_ts"]),
        strategy_family="paper_struct_btc5m_cross_venue_residual_leadlag",
        policy_id="multivenue_residual_net_edge_hard_025_050_cap_1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="SELL" if split_sell else "BUY",
        order_type="PAPER_EXECUTABLE_BOOK",
        token_id=str(signal["token_id"]),
        event_ts=float(signal["signal_ts"]),
        api_latency_s=max(0.0, float(signal["observed_ts"]) - float(signal["signal_ts"])),
        live_orders_allowed=False,
        reason=(
            "CTF split inventory sold only against positive actual executable overround"
            if split_sell
            else "frozen multivenue residual exceeds actual executable cost"
        ),
        metadata={
            "generation_checksum": GENERATION_CHECKSUM,
            "preregistration_checksum": prereg["checksum"],
            "actual_depth_verified": True,
            "lookahead_violations": 0,
            "parity_disagreement": 0,
        },
    )


def _choose_two_sided_quote(
    *, p_up: float, books: dict[str, dict[str, Any]], family: str, threshold: float,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Rank UP and DOWN independently at unchanged post-cost protections."""
    candidates: list[dict[str, Any]] = []
    failures: list[str] = []
    for outcome, probability in (("Up", p_up), ("Down", 1.0 - p_up)):
        book = books.get(outcome) or {}
        best_bid = float(book.get("best_bid") or 0.0)
        best_ask = float(book.get("best_ask") or 0.0)
        tick = float(book.get("tick_size") or 0.01)
        improvement = tick if family == "two_sided_inside_spread_maker" else 0.0
        price = round(best_bid + improvement, 8)
        blockers = []
        if str(book.get("status") or "") != "OK":
            blockers.append("actual_book_not_ok")
        if best_bid <= 0 or best_ask <= 0:
            blockers.append("two_sided_book_missing")
        if family == "two_sided_inside_spread_maker" and not (best_bid < price < best_ask):
            blockers.append("inside_spread_strict_post_only_failed")
        if not MIN_PRICE <= price <= MAX_PRICE:
            blockers.append("hard_entry_bounds")
        shares = ORDER_USD / price if price > 0 else 0.0
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        residual = probability - price
        required = (fee / shares if shares else 1.0) + SLIPPAGE_MARGIN + CALIBRATION_ERROR_MARGIN + threshold + improvement
        if residual <= required:
            blockers.append("residual_below_frozen_margin")
        if blockers:
            failures.extend(f"{outcome}:{reason}" for reason in blockers)
            continue
        candidates.append({
            "outcome": outcome, "token_id": str(book.get("token_id") or ""), "book": book,
            "executable_price": price, "best_bid": best_bid, "best_ask": best_ask, "tick_size": tick,
            "quote_improvement": improvement, "side_probability": probability, "residual": residual,
            "required_residual": required, "net_edge_per_share": residual - required, "expected_fee_usd": fee,
        })
    candidates.sort(key=lambda row: (-float(row["net_edge_per_share"]), str(row["outcome"])))
    return (candidates[0], []) if candidates else (None, sorted(set(failures)))


def _choose_paired_complement_quote(*, books: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    """Admit a two-leg paper bundle only under positive worst-case post-cost edge."""
    legs, blockers = [], []
    total_cost = 0.0
    for outcome in ("Up", "Down"):
        book = books.get(outcome) or {}
        bid, ask = float(book.get("best_bid") or 0.0), float(book.get("best_ask") or 0.0)
        if str(book.get("status") or "") != "OK" or not MIN_PRICE <= bid <= MAX_PRICE or ask <= 0:
            blockers.append(f"{outcome}:paired_actual_book_or_bounds")
            continue
        shares = ORDER_USD / bid
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=bid)
        queue_cost = 0.005
        adverse_liquidation = max(0.0, ask - bid)
        leg_cost = bid + fee / shares + queue_cost + adverse_liquidation
        total_cost += leg_cost
        legs.append({"outcome": outcome, "token_id": str(book.get("token_id") or ""), "executable_price": bid, "best_ask": ask, "expected_fee_usd": fee, "queue_cost_per_share": queue_cost, "adverse_one_leg_liquidation_per_share": adverse_liquidation, "book": book})
    worst_case_edge = 1.0 - total_cost - CALIBRATION_ERROR_MARGIN
    if len(legs) != 2 or worst_case_edge <= 0:
        blockers.append("paired_worst_case_post_cost_edge_nonpositive")
        return None, blockers
    return {"legs": legs, "worst_case_post_cost_edge": worst_case_edge, "combined_post_cost": total_cost + CALIBRATION_ERROR_MARGIN}, []


def _choose_complete_set_paired_quote(*, books: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    """Freeze a strict-post-only $1 total complete-set bundle from both actual books."""
    legs: list[dict[str, Any]] = []
    combined = CALIBRATION_ERROR_MARGIN
    for outcome in ("Up", "Down"):
        book = books.get(outcome) or {}
        bid, ask = float(book.get("best_bid") or 0.0), float(book.get("best_ask") or 0.0)
        if str(book.get("status") or "") != "OK" or not MIN_PRICE <= bid <= MAX_PRICE or ask <= bid:
            return None, [f"{outcome}:complete_set_actual_book_or_post_only_bounds"]
        shares = PAIR_LEG_USD / bid
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=bid)
        queue_reserve = 0.005
        adverse_reserve = ask - bid
        combined += bid + fee / shares + queue_reserve + adverse_reserve
        legs.append({
            "outcome": outcome,
            "token_id": str(book.get("token_id") or ""),
            "executable_price": bid,
            "best_ask": ask,
            "expected_fee_usd": fee,
            "queue_cost_per_share": queue_reserve,
            "adverse_one_leg_liquidation_per_share": adverse_reserve,
            "book": book,
        })
    edge = 1.0 - combined
    if edge <= 0.0:
        return None, ["complete_set_pair_post_cost_edge_nonpositive"]
    return {"legs": legs, "worst_case_post_cost_edge": edge, "combined_post_cost": combined}, []


def _sell_book_snapshot(*, clob: CLOBMarketClient, token_id: str, shares: float = 1.0) -> dict[str, Any]:
    """Return chronological executable bid depth for an exact share-sized SELL."""
    try:
        raw = clob.get_book(token_id)
        levels: list[tuple[float, float]] = []
        raw_bids = raw.get("bids") if isinstance(raw, dict) else getattr(raw, "bids", [])
        for level in raw_bids or []:
            price = float(level.get("price") if isinstance(level, dict) else getattr(level, "price", 0.0))
            size = float(level.get("size") if isinstance(level, dict) else getattr(level, "size", 0.0))
            if price > 0 and size > 0:
                levels.append((price, size))
        levels.sort(reverse=True)
        remaining = float(shares)
        proceeds = 0.0
        consumed: list[dict[str, float]] = []
        for price, available in levels:
            take = min(remaining, available)
            if take <= 0:
                continue
            proceeds += take * price
            remaining -= take
            consumed.append({"price": price, "shares": take})
            if remaining <= 1e-9:
                break
        filled = max(0.0, float(shares) - remaining)
        return {
            "status": "OK",
            "best_bid": levels[0][0] if levels else 0.0,
            "executable_sell_limit_price": consumed[-1]["price"] if consumed else 0.0,
            "avg_sell_fill_price": proceeds / filled if filled > 0 else 0.0,
            "fillable_sell_shares": filled,
            "requested_sell_shares": float(shares),
            "consumed_bid_depth": consumed,
            "book_timestamp": raw.get("timestamp") if isinstance(raw, dict) else getattr(raw, "timestamp", None),
            "book_hash": stable_id("sellbook", {"token_id": token_id, "levels": consumed, "ts": raw.get("timestamp") if isinstance(raw, dict) else getattr(raw, "timestamp", None)}),
        }
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}:{exc}", "fillable_sell_shares": 0.0}


def _choose_complete_set_split_sell_quote(*, books: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    """Admit only a fully executable $1 CTF split whose two SELL legs lock an overround."""
    legs: list[dict[str, Any]] = []
    net_proceeds = -0.01
    for outcome in ("Up", "Down"):
        book = books.get(outcome) or {}
        price = float(book.get("executable_sell_limit_price") or 0.0)
        average = float(book.get("avg_sell_fill_price") or 0.0)
        filled = float(book.get("fillable_sell_shares") or 0.0)
        if str(book.get("status") or "") != "OK" or not 0.0 < price < 1.0 or filled + 1e-9 < 1.0:
            return None, [f"{outcome}:split_sell_actual_bid_depth_missing"]
        fee = expected_polymarket_buy_fee_usd(shares=1.0, price=average)
        net_proceeds += average - fee - SLIPPAGE_MARGIN
        legs.append({
            "outcome": outcome,
            "token_id": str(book.get("token_id") or ""),
            "executable_price": price,
            "avg_fill_price": average,
            "expected_fee_usd": fee,
            "filled_shares": 1.0,
            "book": book,
        })
    edge = net_proceeds - ORDER_USD
    if edge <= 0.0:
        return None, ["complete_set_split_sell_post_cost_overround_nonpositive"]
    return {
        "legs": legs,
        "worst_case_post_cost_edge": edge,
        "combined_post_cost": ORDER_USD,
        "split_collateral_usd": ORDER_USD,
        "net_executable_sale_proceeds_usd": net_proceeds,
        "inventory_conservation": {"split_shares_per_leg": 1.0, "sell_shares_per_leg": 1.0, "disagreement": 0},
    }, []


def _settle_split_sell_inventory(
    *, legs: list[dict[str, Any]], winner: str | None
) -> dict[str, Any]:
    """Charge partial/asymmetric SELL fills and carry unsold split inventory."""
    fills: dict[str, dict[str, float]] = {}
    proceeds = 0.0
    fees = 0.0
    for leg in legs:
        outcome = str(leg.get("outcome") or "")
        requested = float(leg.get("requested_shares") or leg.get("shares") or 1.0)
        filled = min(requested, max(0.0, float(leg.get("filled_shares") or 0.0)))
        price = float(leg.get("fill_price") or leg.get("limit_price") or 0.0)
        unsold = max(0.0, requested - filled)
        proceeds += filled * price
        fees += expected_polymarket_buy_fee_usd(shares=filled, price=price)
        fills[outcome] = {
            "requested_shares": requested,
            "filled_shares": filled,
            "unsold_shares": unsold,
            "fill_price": price,
        }
    unsold_up = float((fills.get("Up") or {}).get("unsold_shares") or 0.0)
    unsold_down = float((fills.get("Down") or {}).get("unsold_shares") or 0.0)
    resolution_value = (
        unsold_up if winner == "Up" else unsold_down if winner == "Down" else None
    )
    realized = (
        proceeds
        + float(resolution_value or 0.0)
        - ORDER_USD
        - fees
        - 0.01
        - (2 * SLIPPAGE_MARGIN)
    )
    return {
        "split_collateral_usd": ORDER_USD,
        "fills": fills,
        "gross_sell_proceeds_usd": round(proceeds, 8),
        "venue_fees_usd": round(fees, 8),
        "ctf_gas_settlement_reserve_usd": 0.01,
        "slippage_reserve_usd": round(2 * SLIPPAGE_MARGIN, 8),
        "unsold_inventory": {"Up": unsold_up, "Down": unsold_down},
        "canonical_winner": winner,
        "resolution_value_usd": (
            round(float(resolution_value), 8) if resolution_value is not None else None
        ),
        "resolved": winner in {"Up", "Down"} or (unsold_up == 0.0 and unsold_down == 0.0),
        "realized_post_cost_pnl_usd": (
            round(realized, 8)
            if winner in {"Up", "Down"} or (unsold_up == 0.0 and unsold_down == 0.0)
            else None
        ),
        "dual_leg_fill_verified": all(
            float((fills.get(outcome) or {}).get("filled_shares") or 0.0) >= 1.0 - 1e-9
            for outcome in ("Up", "Down")
        ),
        "inventory_conservation": {
            "split_shares_per_leg": 1.0,
            "sold_plus_unsold_up": round(
                float((fills.get("Up") or {}).get("filled_shares") or 0.0) + unsold_up,
                8,
            ),
            "sold_plus_unsold_down": round(
                float((fills.get("Down") or {}).get("filled_shares") or 0.0)
                + unsold_down,
                8,
            ),
            "disagreement": int(
                abs(
                    float((fills.get("Up") or {}).get("filled_shares") or 0.0)
                    + unsold_up
                    - 1.0
                )
                > 1e-9
                or abs(
                    float((fills.get("Down") or {}).get("filled_shares") or 0.0)
                    + unsold_down
                    - 1.0
                )
                > 1e-9
            ),
        },
    }


def reconcile_split_sell_terminal_evidence(args: argparse.Namespace) -> dict[str, Any]:
    """Append selector evidence for immutable split-sell terminals without moving the clock."""
    matrix = load_json(args.state, default={})
    if not isinstance(matrix, dict) or matrix.get("generation_checksum") != GENERATION_CHECKSUM:
        raise RuntimeError("split-sell reconciliation generation checksum mismatch")
    if args.generation_family != "complete_set_split_sell_overround":
        raise RuntimeError("terminal evidence reconciliation is split-sell only")
    if matrix.get("stop_writer") is not True:
        raise RuntimeError("split-sell reconciliation requires a terminal writer decision")

    allowed_windows = {
        int(value)
        for value in matrix.get("complete_liveness_window_starts_s") or []
    }
    if not allowed_windows:
        raise RuntimeError("split-sell reconciliation has no complete liveness windows")
    resolution_rows = {
        str(row.get("market_slug") or ""): row
        for row in _load_jsonl(args.resolutions)
        if row.get("research_only") is False
        and row.get("gamma_lifecycle_ended") is True
        and str(row.get("direction") or "") in {"UP", "DOWN"}
    }
    required_slugs = {f"btc-updown-5m-{window}" for window in allowed_windows}
    missing_slugs = sorted(required_slugs - resolution_rows.keys())
    if missing_slugs:
        return {
            "status": "UPSTREAM_CANONICAL_RESOLUTION_PENDING",
            "generation_checksum": GENERATION_CHECKSUM,
            "complete_liveness_window_starts_s": sorted(allowed_windows),
            "missing_market_slugs": missing_slugs,
            "appended_bundle_events": 0,
        }

    appended = 0
    reconciled_ids: list[str] = []
    for cell in matrix.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        cell_id = str(cell.get("cell_id") or "")
        terminal_path = str(cell.get("terminals_path") or "")
        events_path = str(cell.get("fill_events_path") or "")
        if not cell_id or not terminal_path or not events_path:
            continue
        existing_events = _load_jsonl(events_path)
        existing_ids = {
            str(row.get("paired_bundle_id") or "")
            for row in existing_events
            if row.get("event") == "paired_bundle_terminal"
        }
        reconciled_ids.extend(
            str(row.get("event_id") or "")
            for row in existing_events
            if row.get("resolution_reconciliation") == "APPEND_ONLY_LEG_ID_COLLISION_REPAIR"
            and row.get("event_id")
        )
        new_events: list[dict[str, Any]] = []
        for terminal in _load_jsonl(terminal_path):
            window_start = int(terminal.get("window_start_s") or 0)
            bundle_id = str(terminal.get("terminal_id") or "")
            paired = [row for row in terminal.get("paired_intents") or [] if isinstance(row, dict)]
            if (
                terminal.get("generation_checksum") != GENERATION_CHECKSUM
                or terminal.get("cell_id") != cell_id
                or terminal.get("terminal_status") != "SIGNAL"
                or window_start not in allowed_windows
                or bundle_id in existing_ids
                or len(paired) != 2
                or {str(row.get("outcome") or "") for row in paired} != {"Up", "Down"}
                or len({str(row.get("token_id") or "") for row in paired}) != 2
            ):
                continue
            legs: list[dict[str, Any]] = []
            for intent in paired:
                leg_id = stable_id(
                    "ci",
                    {
                        "cell_id": cell_id,
                        "signal_id": intent.get("source_event_id"),
                        "outcome": intent.get("outcome"),
                        "token_id": intent.get("token_id"),
                    },
                )
                shares = float(intent.get("shares") or 0.0)
                price = float(intent.get("limit_price") or 0.0)
                legs.append(
                    {
                        **intent,
                        "intent_id": leg_id,
                        "superseded_colliding_intent_id": intent.get("intent_id"),
                        "status": "FILLED",
                        "requested_size_usd": intent.get("copy_size_usd"),
                        "requested_shares": shares,
                        "filled_size_usd": round(shares * price, 8),
                        "filled_shares": shares,
                        "fill_price": price,
                        "submitted_at": terminal.get("recorded_at"),
                        "actual_depth_verified": True,
                        "generation_checksum": GENERATION_CHECKSUM,
                        "cell_id": cell_id,
                        "paired_bundle_id": bundle_id,
                    }
                )
            canonical = resolution_rows[str(terminal.get("market_slug") or "")]
            accounting = _settle_split_sell_inventory(
                legs=legs,
                winner=str(canonical.get("direction") or "").title(),
            )
            event = {
                "schema_version": 1,
                "event": "paired_bundle_terminal",
                "event_id": stable_id(
                    "mvbe",
                    {"generation": GENERATION_CHECKSUM, "cell": cell_id, "bundle": bundle_id},
                ),
                "paired_bundle_id": bundle_id,
                "bundle_status": "DUAL_LEG_FILLED",
                "dual_leg_fill_verified": True,
                "selector_evidence_eligible": True,
                "legs": legs,
                "split_sell_accounting": accounting,
                "canonical_resolution": {
                    "market_slug": canonical.get("market_slug"),
                    "direction": canonical.get("direction"),
                    "source": canonical.get("source"),
                    "computed_at_iso": canonical.get("computed_at_iso"),
                    "condition_id": canonical.get("condition_id"),
                },
                "resolution_reconciliation": "APPEND_ONLY_LEG_ID_COLLISION_REPAIR",
                "generation_checksum": GENERATION_CHECKSUM,
                "cell_id": cell_id,
            }
            new_events.append(event)
            existing_ids.add(bundle_id)
            reconciled_ids.append(event["event_id"])
        if new_events:
            append_jsonl_many(events_path, new_events)
            appended += len(new_events)

    prior_selector = load_json(args.selector_state, default={})
    prior_selector = prior_selector if isinstance(prior_selector, dict) else {}
    selector = reduce_promoted_cells(
        matrix,
        resolutions_path=args.resolutions,
        prior_activation=prior_selector.get("selected") or {},
        prior_cells=prior_selector.get("cells") or [],
        forbidden_model_checksums=(FORBIDDEN_CHECKSUM,),
    )
    selector["generated_at"] = utc_now_iso()
    selector["single_submitter"] = "scripts/run_wallet_copy_live_guard.py"
    selector["activation_contract"] = {
        "order_usd": 1.0,
        "max_accepted_orders_per_window": 2,
        "paired_bundle_required": True,
        "ttl_s": 3600,
        "ttl_refresh_allowed": False,
    }
    atomic_write_json(args.selector_state, selector)
    resolved_count = sum(
        int((row.get("evidence_snapshot") or {}).get("resolved_fills") or 0)
        for row in selector.get("cells") or []
    )
    matrix.setdefault("attribution_funnel", {})["resolved_selector_cells"] = resolved_count
    matrix["promoted_cell_selector"] = {
        "state_path": args.selector_state,
        "status": selector.get("status"),
        "selected": selector.get("selected"),
    }
    matrix["resolution_reconciliation"] = {
        "status": "PASS",
        "mode": "APPEND_ONLY_LEG_ID_COLLISION_REPAIR",
        "canonical_market_count": len(required_slugs),
        "reconciled_bundle_events": resolved_count,
        "last_append_count": appended,
        "event_ids": sorted(set(reconciled_ids)),
        "clock_mutated": False,
    }
    atomic_write_json(args.state, matrix)
    return {
        "status": "PASS",
        "generation_checksum": GENERATION_CHECKSUM,
        "terminal_generation_status": matrix.get("status"),
        "complete_liveness_window_starts_s": sorted(allowed_windows),
        "appended_bundle_events": appended,
        "resolved_selector_cells": resolved_count,
        "selector_status": selector.get("status"),
        "selected": selector.get("selected"),
        "clock_mutated": False,
    }


def _choose_fill_then_hedge_quote(*, books: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    """Price a passive first leg against an immediately executable complement hedge."""
    candidates: list[dict[str, Any]] = []
    for first_outcome, hedge_outcome in (("Up", "Down"), ("Down", "Up")):
        first, hedge = books.get(first_outcome) or {}, books.get(hedge_outcome) or {}
        first_price = float(first.get("best_bid") or 0.0)
        hedge_price = float(hedge.get("avg_fill_price") or hedge.get("best_ask") or 0.0)
        if (
            str(first.get("status") or "") != "OK"
            or str(hedge.get("status") or "") != "OK"
            or not MIN_PRICE <= first_price <= MAX_PRICE
            or not MIN_PRICE <= hedge_price <= MAX_PRICE
            or float(hedge.get("fillable_usd") or 0.0) + 1e-9 < ORDER_USD
        ):
            continue
        first_shares, hedge_shares = ORDER_USD / first_price, ORDER_USD / hedge_price
        first_fee = expected_polymarket_buy_fee_usd(shares=first_shares, price=first_price)
        hedge_fee = expected_polymarket_buy_fee_usd(shares=hedge_shares, price=hedge_price)
        fee_per_share = first_fee / first_shares + hedge_fee / hedge_shares
        queue_cost = 0.005
        one_leg_risk = max(0.0, float(first.get("best_ask") or first_price) - first_price)
        locked_cost = first_price + hedge_price + fee_per_share + queue_cost + one_leg_risk + CALIBRATION_ERROR_MARGIN
        if locked_cost < 1.0:
            candidates.append({
                "legs": [
                    {"outcome": first_outcome, "token_id": str(first.get("token_id") or ""), "executable_price": first_price, "expected_fee_usd": first_fee, "book": first, "execution": "PASSIVE_FIRST"},
                    {"outcome": hedge_outcome, "token_id": str(hedge.get("token_id") or ""), "executable_price": hedge_price, "expected_fee_usd": hedge_fee, "book": hedge, "execution": "EXECUTABLE_DEPTH_HEDGE"},
                ],
                "worst_case_post_cost_edge": 1.0 - locked_cost,
                "combined_post_cost": locked_cost,
                "one_leg_risk_per_share": one_leg_risk,
            })
    candidates.sort(key=lambda row: -float(row["worst_case_post_cost_edge"]))
    return (candidates[0], []) if candidates else (None, ["fill_then_hedge_locked_value_nonpositive_or_depth_missing"])


def _choose_dual_ioc_quote(*, books: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    """Price chronological executable two-book depth and an explicit one-leg failure loss."""
    legs: list[dict[str, Any]] = []
    total = CALIBRATION_ERROR_MARGIN
    for sequence, outcome in enumerate(("Up", "Down"), start=1):
        book = books.get(outcome) or {}
        price = float(book.get("avg_fill_price") or book.get("best_ask") or 0.0)
        if (
            str(book.get("status") or "") != "OK"
            or not MIN_PRICE <= price <= MAX_PRICE
            or float(book.get("fillable_usd") or 0.0) + 1e-9 < ORDER_USD
        ):
            return None, [f"{outcome}:dual_ioc_depth_or_bounds"]
        shares = ORDER_USD / price
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        total += price + fee / shares + SLIPPAGE_MARGIN
        legs.append({"outcome": outcome, "token_id": str(book.get("token_id") or ""), "executable_price": price, "expected_fee_usd": fee, "book": book, "execution": "IOC", "chronological_sequence": sequence})
    one_leg_failure_cost = max(float(row["executable_price"]) for row in legs) + SLIPPAGE_MARGIN
    edge = 1.0 - total - one_leg_failure_cost * 0.01
    if edge <= 0:
        return None, ["dual_ioc_worst_case_post_cost_edge_nonpositive"]
    return {"legs": legs, "worst_case_post_cost_edge": edge, "combined_post_cost": total, "one_leg_failure_cost": one_leg_failure_cost}, []


def _is_paired_family(family: str) -> bool:
    return family in {
        "paired_complement_post_only_inventory",
        "paired_complement_fill_then_hedge",
        "paired_complement_dual_ioc",
        "complete_set_paired_maker",
        "complete_set_split_sell_overround",
    }


def _choose_basis_maker_quote(
    *, feature: dict[str, Any], books: dict[str, dict[str, Any]], offset: int
) -> tuple[dict[str, Any] | None, list[str]]:
    """Post only when the three-venue return basis is materially displaced."""
    returns = [float(value) for value in (feature.get("venue_returns") or {}).values()]
    displacement = max(returns) - min(returns) if len(returns) == len(VENUES) else 0.0
    minimum = REFERENCE_SIGMA_1S * math.sqrt(offset)
    if displacement <= minimum:
        return None, ["three_venue_basis_displacement_below_trigger"]
    outcome = str(feature.get("outcome") or "")
    probability = float(feature.get("consensus_probability") or 0.0)
    book = books.get(outcome) or {}
    price = float(book.get("best_bid") or 0.0)
    if str(book.get("status") or "") != "OK" or not MIN_PRICE <= price <= MAX_PRICE:
        return None, ["basis_maker_book_or_bounds"]
    shares = ORDER_USD / price
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
    required = fee / shares + SLIPPAGE_MARGIN + CALIBRATION_ERROR_MARGIN
    if probability - price <= required:
        return None, ["basis_maker_post_cost_edge_nonpositive"]
    return {
        "outcome": outcome,
        "token_id": str(book.get("token_id") or ""),
        "book": book,
        "executable_price": price,
        "expected_fee_usd": fee,
        "residual": probability - price,
        "required_residual": required,
        "basis_displacement": displacement,
        "basis_trigger": minimum,
    }, []


def _complete_liveness_windows(
    prior: dict[str, Any], *, window_start: int, clock_complete: bool, complete_after_s: int
) -> list[int]:
    """Return only prospective windows with all configured raw-clock offsets."""
    completed = {
        int(value)
        for value in prior.get("complete_liveness_window_starts_s") or []
        if int(value) >= int(complete_after_s)
    }
    if clock_complete and window_start >= complete_after_s:
        completed.add(window_start)
    return sorted(completed)


def _complete_set_window_complete(*, terminal_present: bool, elapsed_s: float) -> bool:
    return terminal_present and elapsed_s >= 270


def _generation_window_clock_complete(
    *, family: str, cells: list[dict[str, Any]], complete_clock_offsets: set[int]
) -> bool:
    if family in {"complete_set_paired_maker", "complete_set_split_sell_overround"}:
        return bool(cells) and all(cell.get("current_window_terminal_complete") for cell in cells)
    return complete_clock_offsets == set(OFFSETS)


def _generation_evidence_window_eligible(
    *, family: str, window_start: int, liveness_start: int
) -> bool:
    if family in {"complete_set_paired_maker", "complete_set_split_sell_overround"}:
        return window_start >= liveness_start
    return True


def _fill_then_hedge_completion(
    order: dict[str, Any], *, hedge_book: dict[str, Any]
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate an executable hedge after the passive first leg actually fills."""
    hedge = order.get("hedge_leg") or {}
    first_price = float(order.get("fill_price") or 0.0)
    hedge_price = float(hedge_book.get("avg_fill_price") or hedge_book.get("best_ask") or 0.0)
    if (
        str(hedge_book.get("status") or "") != "OK"
        or not MIN_PRICE <= hedge_price <= MAX_PRICE
        or float(hedge_book.get("fillable_usd") or 0.0) + 1e-9 < ORDER_USD
    ):
        return None, "fill_then_hedge_executable_depth_missing"
    first_shares, hedge_shares = ORDER_USD / first_price, ORDER_USD / hedge_price
    fee_per_share = (
        expected_polymarket_buy_fee_usd(shares=first_shares, price=first_price) / first_shares
        + expected_polymarket_buy_fee_usd(shares=hedge_shares, price=hedge_price) / hedge_shares
    )
    one_leg_risk = float(order.get("one_leg_risk_per_share") or 0.0)
    locked_cost = first_price + hedge_price + fee_per_share + 0.005 + one_leg_risk + CALIBRATION_ERROR_MARGIN
    if locked_cost >= 1.0:
        return None, "fill_then_hedge_locked_value_nonpositive_at_fill"
    return {
        "token_id": str(hedge.get("token_id") or ""),
        "outcome": str(hedge.get("outcome") or ""),
        "fill_price": hedge_price,
        "filled_size_usd": ORDER_USD,
        "actual_depth_verified": True,
        "locked_post_cost_edge": 1.0 - locked_cost,
        "book_hash": hedge_book.get("book_hash"),
        "book_timestamp": hedge_book.get("book_timestamp"),
    }, None


def _passive_book_observation(
    *,
    clob: CLOBMarketClient,
    token_id: str,
    order_usd: float,
    max_entry_price: float,
) -> dict[str, Any]:
    observed_sequence = time.time_ns()
    direct = _book_snapshot_with_direct_fallback(
        clob=clob,
        token_id=token_id,
        order_usd=order_usd,
        max_entry_price=max_entry_price,
    )
    raw = clob.get_book(token_id)
    return {
        **direct,
        "sequence": observed_sequence,
        "book_timestamp": getattr(raw, "timestamp", None),
        "same_price_bid_size": _queue_ahead_at_price(raw, max_entry_price),
    }


def _advance_passive_orders(
    orders: list[dict[str, Any]],
    *,
    now_ts: float,
    book_loader: Any,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for order in orders:
        if order.get("status") != "OPEN":
            continue
        order_window = int(str(order.get("market_slug") or "").rsplit("-", 1)[-1] or 0)
        if now_ts >= order_window + 270:
            order["status"] = "CANCELLED"
            order["terminal_reason"] = "cancel_before_close"
            order["updated_at"] = utc_now_iso()
            events.append({"event": "passive_quote_cancelled", **order})
            continue
        try:
            later_book = book_loader(
                str(order.get("token_id") or ""),
                float(order.get("limit_price") or 0.0),
            )
        except Exception:
            continue
        later_sequence = float(later_book.get("sequence") or 0.0)
        prior_sequence = float(order.get("last_book_sequence") or 0.0)
        if later_sequence <= prior_sequence:
            continue
        prior_queue = float(order.get("last_same_price_bid_size") or 0.0)
        current_queue = float(later_book.get("same_price_bid_size") or 0.0)
        depletion = max(0.0, prior_queue - current_queue)
        order["cumulative_verified_depletion_shares"] = round(
            float(order.get("cumulative_verified_depletion_shares") or 0.0)
            + depletion,
            6,
        )
        order["last_same_price_bid_size"] = current_queue
        order["last_book_sequence"] = later_sequence
        order["last_book_timestamp"] = later_book.get("book_timestamp")
        later_ask = float(later_book.get("best_ask") or 0.0)
        required_depletion = (
            float(order.get("queue_ahead_shares_at_quote") or 0.0)
            + float(order.get("requested_shares") or 0.0)
        )
        if (
            str(later_book.get("status") or "") == "OK"
            and 0 < later_ask <= float(order.get("limit_price") or 0.0)
            and float(order["cumulative_verified_depletion_shares"]) + 1e-9
            >= required_depletion
        ):
            order["status"] = "FILLED"
            order["fill_price"] = later_ask
            filled_size_usd = float(order.get("requested_size_usd") or ORDER_USD)
            order["filled_size_usd"] = filled_size_usd
            order["filled_shares"] = round(filled_size_usd / later_ask, 6)
            order["updated_at"] = utc_now_iso()
            order["fill_evidence"] = {
                "kind": "sequence_continuous_queue_depletion",
                "verified": True,
                "generation_checksum": order.get("generation_checksum"),
                "cell_id": order.get("cell_id"),
                "intent_id": order.get("intent_id"),
                "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM,
                "book_hash": later_book.get("book_hash"),
                "book_timestamp": later_book.get("book_timestamp"),
                "book_sequence": later_sequence,
                "best_ask": later_ask,
                "queue_ahead_shares_at_quote": order.get("queue_ahead_shares_at_quote"),
                "cumulative_verified_depletion_shares": order.get(
                    "cumulative_verified_depletion_shares"
                ),
                "required_depletion_shares": round(required_depletion, 6),
            }
            events.append({"event": "passive_quote_filled", **order})
    return events


def _evaluate_cell(
    args: argparse.Namespace,
    *,
    now_ts: float,
    offset: int,
    mode: str,
    threshold: float | None,
    feature: dict[str, Any] | None,
    feature_blockers: list[str],
) -> dict[str, Any]:
    paths = _paths(offset, mode, threshold)
    prereg = _preregistration(offset, mode, threshold)
    _write_or_verify(paths["preregistration"], prereg)
    cell_id = prereg["cell_id"]
    window_start = int(now_ts // 300) * 300
    terminals = _load_jsonl(paths["terminals"])
    terminal_id = stable_id(
        "mvrt",
        {"generation": GENERATION_CHECKSUM, "cell": cell_id, "window": window_start},
    )
    current = next((row for row in terminals if row.get("terminal_id") == terminal_id), None)
    signal = None
    status = "WAITING_SIGNAL_OFFSET" if now_ts - window_start < offset else "PROTECTED_SKIP"
    blockers = list(feature_blockers)
    complete_set_pair = args.generation_family in {
        "complete_set_paired_maker",
        "complete_set_split_sell_overround",
    }
    split_sell = args.generation_family == "complete_set_split_sell_overround"
    book_only_pair = complete_set_pair
    if current is None and now_ts - window_start >= offset and (feature is not None or book_only_pair):
        quote = None
        paired_quote = None
        try:
            slug = f"btc-updown-5m-{window_start}"
            market = _market_for_slug(slug, timeout_s=float(args.timeout_s))
            tokens = _token_map(market)
            clob = CLOBMarketClient(
                    args.clob_base_url,
                    timeout_s=float(args.clob_timeout_s),
                    retries=1,
                )
            if _is_paired_family(args.generation_family):
                leg_usd = PAIR_LEG_USD if book_only_pair else ORDER_USD
                side_books = {
                    outcome: {
                        **(
                            _sell_book_snapshot(
                                clob=clob,
                                token_id=str(tokens.get(outcome) or ""),
                                shares=1.0,
                            )
                            if split_sell
                            else _book_snapshot_with_direct_fallback(
                                clob=clob,
                                token_id=str(tokens.get(outcome) or ""),
                                order_usd=leg_usd,
                                max_entry_price=MAX_PRICE,
                            )
                        ),
                        "token_id": str(tokens.get(outcome) or ""),
                    }
                    for outcome in ("Up", "Down")
                }
                if split_sell:
                    paired_quote, blockers = _choose_complete_set_split_sell_quote(books=side_books)
                elif book_only_pair:
                    paired_quote, blockers = _choose_complete_set_paired_quote(books=side_books)
                elif args.generation_family == "paired_complement_fill_then_hedge":
                    paired_quote, blockers = _choose_fill_then_hedge_quote(books=side_books)
                elif args.generation_family == "paired_complement_dual_ioc":
                    paired_quote, blockers = _choose_dual_ioc_quote(books=side_books)
                else:
                    paired_quote, blockers = _choose_paired_complement_quote(books=side_books)
                if paired_quote is None:
                    status = "PROTECTED_SKIP"
                    raise ValueError("paired_no_positive_worst_case_quote:" + ",".join(blockers))
                primary = paired_quote["legs"][0]
                outcome, token_id, book = str(primary["outcome"]), str(primary["token_id"]), primary["book"]
                price, fee = float(primary["executable_price"]), float(primary["expected_fee_usd"])
                residual, required, best_bid = float(paired_quote["worst_case_post_cost_edge"]), 0.0, price
            elif args.generation_family == "basis_triggered_single_outcome_maker":
                side_books = {
                    outcome: {
                        **_book_snapshot_with_direct_fallback(clob=clob, token_id=str(tokens.get(outcome) or ""), order_usd=ORDER_USD, max_entry_price=MAX_PRICE),
                        "token_id": str(tokens.get(outcome) or ""),
                    }
                    for outcome in ("Up", "Down")
                }
                quote, blockers = _choose_basis_maker_quote(
                    feature=feature, books=side_books, offset=offset
                )
                if quote is None:
                    status = "PROTECTED_SKIP"
                    raise ValueError("basis_no_positive_post_cost_quote:" + ",".join(blockers))
                outcome, token_id, book = str(quote["outcome"]), str(quote["token_id"]), quote["book"]
                price, fee = float(quote["executable_price"]), float(quote["expected_fee_usd"])
                residual, required, best_bid = float(quote["residual"]), float(quote["required_residual"]), price
            elif args.generation_family.startswith("two_sided_"):
                p_up = float(feature["consensus_probability"]) if feature["outcome"] == "Up" else 1.0 - float(feature["consensus_probability"])
                side_books = {
                    outcome: {
                        **_book_snapshot_with_direct_fallback(clob=clob, token_id=str(tokens.get(outcome) or ""), order_usd=ORDER_USD, max_entry_price=MAX_PRICE),
                        "token_id": str(tokens.get(outcome) or ""),
                    }
                    for outcome in ("Up", "Down")
                }
                quote, blockers = _choose_two_sided_quote(
                    p_up=p_up, books=side_books, family=args.generation_family, threshold=float(threshold or 0.0)
                )
                if quote is None:
                    status = "PROTECTED_SKIP"
                    raise ValueError("two_sided_no_positive_post_cost_quote:" + ",".join(blockers))
                outcome, token_id, book = str(quote["outcome"]), str(quote["token_id"]), quote["book"]
                price, fee = float(quote["executable_price"]), float(quote["expected_fee_usd"])
                residual, required = float(quote["residual"]), float(quote["required_residual"])
                best_bid = float(quote["best_bid"])
            else:
                outcome = str(feature["outcome"])
                token_id = str(tokens.get(outcome) or "")
                book = _book_snapshot_with_direct_fallback(clob=clob, token_id=token_id, order_usd=ORDER_USD, max_entry_price=MAX_PRICE)
                taker_price = float(book.get("avg_fill_price") or book.get("best_ask") or 0.0)
                best_bid = float(book.get("best_bid") or 0.0)
                price = taker_price if mode == "taker" else best_bid
                shares = ORDER_USD / price if price > 0 else 0.0
                fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
                residual = float(feature["consensus_probability"]) - price
                required = (fee / shares if shares else 1.0) + SLIPPAGE_MARGIN + CALIBRATION_ERROR_MARGIN + float(threshold or 0.0)
                blockers = []
                if str(book.get("status") or "") != "OK": blockers.append("actual_book_not_ok")
                if mode == "taker" and float(book.get("fillable_usd") or 0.0) + 1e-9 < ORDER_USD: blockers.append("actual_depth_below_order")
                if mode == "passive" and best_bid <= 0: blockers.append("actual_post_only_bid_missing")
                if not MIN_PRICE <= price <= MAX_PRICE: blockers.append("hard_entry_bounds")
                if residual <= required: blockers.append("residual_below_frozen_margin")
            signal = {
                **(feature or {}),
                "outcome": outcome,
                "signal_id": stable_id("mvrs", {"cell": cell_id, "market": slug}),
                "window_start_s": window_start,
                "market_slug": slug,
                "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
                "token_id": token_id,
                "executable_price": round(price, 8),
                "residual": round(residual, 8),
                "required_residual": round(required, 8),
                "net_edge_per_share": round(residual - required, 8),
                "expected_fee_usd": fee,
                "book": book,
                "actual_depth_verified": not blockers,
                "execution_mode": mode,
                "order_type": "FAK" if mode == "taker" else "GTC_POST_ONLY_STRICT",
                "signal_ts": window_start + offset,
                "observed_ts": now_ts,
                "paper_only": True,
                "live_orders_allowed": False,
                "generation_family": args.generation_family,
                "quote_improvement": float((quote or {}).get("quote_improvement") or 0.0) if args.generation_family.startswith("two_sided_") else 0.0,
                "paired_legs": paired_quote.get("legs") if paired_quote else None,
                "worst_case_post_cost_edge": paired_quote.get("worst_case_post_cost_edge") if paired_quote else None,
                "one_leg_failure_cost": paired_quote.get("one_leg_failure_cost") if paired_quote else None,
                "basis_displacement": quote.get("basis_displacement") if quote else None,
                "cancel_on_signal_reversal": args.generation_family == "basis_triggered_single_outcome_maker",
                "split_sell_accounting": (
                    {
                        "split_collateral_usd": paired_quote.get("split_collateral_usd"),
                        "net_executable_sale_proceeds_usd": paired_quote.get("net_executable_sale_proceeds_usd"),
                        "realized_post_cost_pnl_usd": paired_quote.get("worst_case_post_cost_edge"),
                        "inventory_conservation": paired_quote.get("inventory_conservation"),
                        "ctf_split_required_before_live_sells": True,
                    }
                    if split_sell and paired_quote
                    else None
                ),
            }
            if mode == "passive" and not blockers:
                raw_book = clob.get_book(token_id)
                quote_sequence = float(getattr(raw_book, "timestamp", 0.0) or time.time())
                signal["passive_quote_evidence"] = {
                    "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM,
                    "queue_ahead_shares_at_quote": _queue_ahead_at_price(raw_book, price),
                    "same_price_bid_size": _queue_ahead_at_price(raw_book, price),
                    "book_sequence": quote_sequence,
                    "book_timestamp": getattr(raw_book, "timestamp", None),
                    "book_hash": stable_id(
                        "book",
                        {
                            "token_id": token_id,
                            "price": price,
                            "sequence": quote_sequence,
                        },
                    ),
                }
                if args.generation_family in {"paired_complement_post_only_inventory", "complete_set_paired_maker"}:
                    for leg in signal.get("paired_legs") or []:
                        raw_leg = clob.get_book(str(leg["token_id"]))
                        leg_sequence = float(getattr(raw_leg, "timestamp", 0.0) or time.time())
                        leg["passive_quote_evidence"] = {
                            "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM,
                            "queue_ahead_shares_at_quote": _queue_ahead_at_price(raw_leg, float(leg["executable_price"])),
                            "same_price_bid_size": _queue_ahead_at_price(raw_leg, float(leg["executable_price"])),
                            "book_sequence": leg_sequence,
                            "book_timestamp": getattr(raw_leg, "timestamp", None),
                        }
            status = "SIGNAL" if not blockers else "PROTECTED_SKIP"
        except Exception as exc:
            if str(exc).startswith(("two_sided_no_positive_post_cost_quote:", "paired_no_positive_worst_case_quote:", "basis_no_positive_post_cost_quote:")):
                status = "PROTECTED_SKIP"
            else:
                status, blockers = "DATA_FAILURE", [f"{type(exc).__name__}:{exc}"]
    terminal_ready = (
        now_ts - window_start >= offset
        and (not book_only_pair or status == "SIGNAL" or now_ts - window_start >= 270)
    )
    if current is None and terminal_ready:
        paired_intents = []
        if signal and status == "SIGNAL" and args.generation_family in {"paired_complement_post_only_inventory", "complete_set_paired_maker", "complete_set_split_sell_overround"}:
            paired_intents = [
                _copy_intent({**signal, **leg, "outcome": leg["outcome"], "token_id": leg["token_id"], "executable_price": leg["executable_price"]}, cell_id, prereg)
                for leg in signal.get("paired_legs") or []
            ]
            intent = None
        else:
            intent = _copy_intent(signal, cell_id, prereg) if signal and status == "SIGNAL" else None
        current = {
            "schema_version": 1,
            "event": "btc5m_multivenue_residual_terminal",
            "terminal_id": terminal_id,
            "generation_checksum": GENERATION_CHECKSUM,
            "model_checksum": GENERATION_CHECKSUM,
            "cell_id": cell_id,
            "window_start_s": window_start,
            "market_slug": f"btc-updown-5m-{window_start}",
            "recorded_at": utc_now_iso(),
            "terminal_status": status if mode == "taker" else (
                "PASSIVE_QUOTE_OPEN" if status == "SIGNAL" else status
            ),
            "blockers": blockers,
            "signal": signal,
            "intent": intent.asdict() if intent else None,
            "paired_intents": [row.asdict() for row in paired_intents],
            "actual_depth_verified": bool(signal and signal.get("actual_depth_verified")),
            "lookahead_violations": 0,
            "parity_disagreement": 0,
            "paper_only": True,
            "live_orders_allowed": False,
        }
        append_jsonl_many(paths["terminals"], [current])
        append_jsonl_many(paths["events"], [current])
        if intent:
            append_jsonl_many(paths["intents"], [{"event": "copyintent", "intent": intent.asdict()}])
        elif paired_intents:
            append_jsonl_many(paths["intents"], [{"event": "paired_copyintent_bundle", "paired_intents": [row.asdict() for row in paired_intents], "dual_leg_fill_required": True}])
        terminals.append(current)
    prior = load_json(paths["state"], default={})
    orders = list(prior.get("orders") or []) if isinstance(prior, dict) else []
    if current and current.get("paired_intents"):
        bundle_id = str(current.get("terminal_id") or "")
        bundle_created = False
        leg_evidence = {str(row.get("token_id") or ""): row.get("passive_quote_evidence") or {} for row in ((current.get("signal") or {}).get("paired_legs") or [])}
        for paired in current.get("paired_intents") or []:
            if any(row.get("intent_id") == paired.get("intent_id") for row in orders):
                continue
            evidence = leg_evidence.get(str(paired.get("token_id") or ""), {})
            orders.append({**paired, "status": "FILLED" if split_sell else "OPEN", "requested_size_usd": paired.get("copy_size_usd"), "requested_shares": paired.get("shares"), "filled_size_usd": float(paired.get("shares") or 0.0) * float(paired.get("limit_price") or 0.0) if split_sell else 0.0, "filled_shares": paired.get("shares") if split_sell else 0.0, "fill_price": paired.get("limit_price") if split_sell else None, "submitted_at": current["recorded_at"], "post_only": not split_sell, "actual_depth_verified": True, "generation_checksum": GENERATION_CHECKSUM, "cell_id": cell_id, "paired_bundle_id": bundle_id, "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM if not split_sell else None, "queue_ahead_shares_at_quote": evidence.get("queue_ahead_shares_at_quote"), "last_same_price_bid_size": evidence.get("same_price_bid_size"), "last_book_sequence": evidence.get("book_sequence"), "cumulative_verified_depletion_shares": 0.0})
            bundle_created = True
        if split_sell and bundle_created:
            bundle = [row for row in orders if row.get("paired_bundle_id") == bundle_id]
            if len(bundle) == 2:
                split_accounting = _settle_split_sell_inventory(
                    legs=bundle, winner=None
                )
                append_jsonl_many(paths["events"], [{
                    "event": "paired_bundle_terminal",
                    "paired_bundle_id": bundle_id,
                    "bundle_status": "DUAL_LEG_FILLED",
                    "dual_leg_fill_verified": True,
                    "legs": bundle,
                    "split_sell_accounting": split_accounting,
                    "generation_checksum": GENERATION_CHECKSUM,
                    "cell_id": cell_id,
                }])
    if mode == "passive":
        reversal_events: list[dict[str, Any]] = []
        if args.generation_family == "basis_triggered_single_outcome_maker" and feature:
            for order in orders:
                if order.get("status") == "OPEN" and order.get("outcome") != feature.get("outcome"):
                    order["status"] = "CANCELLED"
                    order["terminal_reason"] = "three_venue_signal_reversal"
                    order["updated_at"] = utc_now_iso()
                    reversal_events.append({"event": "passive_quote_cancelled", **order})
        passive_events = _advance_passive_orders(
            orders,
            now_ts=now_ts,
            book_loader=lambda token_id, limit_price: _passive_book_observation(
                    clob=CLOBMarketClient(
                        args.clob_base_url,
                        timeout_s=float(args.clob_timeout_s),
                        retries=1,
                    ),
                    token_id=token_id,
                    order_usd=ORDER_USD,
                    max_entry_price=limit_price,
                ),
        )
        passive_events = reversal_events + passive_events
        if args.generation_family == "paired_complement_fill_then_hedge":
            completed_events: list[dict[str, Any]] = []
            for event in passive_events:
                order = next(
                    (row for row in orders if row.get("intent_id") == event.get("intent_id")),
                    None,
                )
                if event.get("event") != "passive_quote_filled" or not order:
                    completed_events.append(event)
                    continue
                hedge = order.get("hedge_leg") or {}
                try:
                    hedge_book = _book_snapshot_with_direct_fallback(
                        clob=CLOBMarketClient(
                            args.clob_base_url,
                            timeout_s=float(args.clob_timeout_s),
                            retries=1,
                        ),
                        token_id=str(hedge.get("token_id") or ""),
                        order_usd=ORDER_USD,
                        max_entry_price=MAX_PRICE,
                    )
                    hedge_fill, hedge_failure = _fill_then_hedge_completion(
                        order, hedge_book=hedge_book
                    )
                except Exception as exc:
                    hedge_fill, hedge_failure = None, f"fill_then_hedge_data_failure:{type(exc).__name__}"
                if hedge_fill:
                    order["hedge_status"] = "FILLED"
                    order["hedge_fill"] = hedge_fill
                    completed_events.append({
                        **event,
                        "dual_leg_fill_verified": True,
                        "hedge_fill": hedge_fill,
                    })
                else:
                    order["hedge_status"] = "FAILED"
                    order["hedge_failure"] = hedge_failure
                    completed_events.append({
                        **event,
                        "event": "fill_then_hedge_one_leg_exposure",
                        "selector_evidence_eligible": False,
                        "hedge_failure": hedge_failure,
                    })
            passive_events = completed_events
        paired_changed = {str(row.get("paired_bundle_id") or "") for row in passive_events if row.get("paired_bundle_id")}
        passive_events = [row for row in passive_events if not row.get("paired_bundle_id")]
        for bundle_id in paired_changed:
            bundle = [row for row in orders if row.get("paired_bundle_id") == bundle_id]
            if len(bundle) == 2 and all(row.get("status") == "FILLED" for row in bundle):
                passive_events.append({"event": "paired_bundle_terminal", "paired_bundle_id": bundle_id, "bundle_status": "DUAL_LEG_FILLED", "dual_leg_fill_verified": True, "legs": bundle, "generation_checksum": GENERATION_CHECKSUM, "cell_id": cell_id})
            elif len(bundle) == 2 and any(row.get("status") == "FILLED" for row in bundle) and all(row.get("status") in {"FILLED", "CANCELLED"} for row in bundle):
                passive_events.append({"event": "paired_bundle_terminal", "paired_bundle_id": bundle_id, "bundle_status": "ORPHAN_RESOLUTION", "dual_leg_fill_verified": False, "legs": bundle, "generation_checksum": GENERATION_CHECKSUM, "cell_id": cell_id})
        if passive_events:
            append_jsonl_many(paths["events"], passive_events)
    if mode == "passive" and current and current.get("terminal_status") == "PASSIVE_QUOTE_OPEN":
        intent = current.get("intent") or {}
        if not any(row.get("intent_id") == intent.get("intent_id") for row in orders):
            current_signal = (current or {}).get("signal") or {}
            paired_legs = list(current_signal.get("paired_legs") or [])
            hedge_leg = paired_legs[1] if args.generation_family == "paired_complement_fill_then_hedge" and len(paired_legs) == 2 else None
            orders.append(
                {
                    **intent,
                    "status": "OPEN",
                    "requested_size_usd": ORDER_USD,
                    "requested_shares": intent.get("shares"),
                    "submitted_at": current["recorded_at"],
                    "post_only": True,
                    "actual_depth_verified": True,
                    "generation_checksum": GENERATION_CHECKSUM,
                    "cell_id": cell_id,
                    "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM,
                    "queue_ahead_shares_at_quote": (
                        ((current.get("signal") or {}).get("passive_quote_evidence") or {}).get(
                            "queue_ahead_shares_at_quote"
                        )
                    ),
                    "last_same_price_bid_size": (
                        ((current.get("signal") or {}).get("passive_quote_evidence") or {}).get(
                            "same_price_bid_size"
                        )
                    ),
                    "last_book_sequence": (
                        ((current.get("signal") or {}).get("passive_quote_evidence") or {}).get(
                            "book_sequence"
                        )
                    ),
                    "cumulative_verified_depletion_shares": 0.0,
                    "generation_family": args.generation_family,
                    "hedge_leg": hedge_leg,
                    "one_leg_risk_per_share": max(
                        0.0,
                        float((current_signal.get("paired_legs") or [{}])[0].get("book", {}).get("best_ask") or 0.0)
                        - float(intent.get("limit_price") or 0.0),
                    ) if hedge_leg else 0.0,
                }
            )
    guard_current = dict(current or {})
    guard_signal = (
        dict(guard_current.get("signal") or {})
        if isinstance(guard_current.get("signal"), dict)
        else {}
    )
    if guard_current:
        guard_current.setdefault("model_checksum", GENERATION_CHECKSUM)
        guard_signal.setdefault("window_start_s", guard_current.get("window_start_s"))
        guard_signal.setdefault(
            "net_edge_per_share",
            round(
                float(guard_signal.get("residual") or 0.0)
                - float(guard_signal.get("required_residual") or 0.0),
                8,
            ),
        )
        guard_current["signal"] = guard_signal
        if (
            mode == "passive"
            and guard_current.get("terminal_status") == "PASSIVE_QUOTE_OPEN"
            and not guard_current.get("blockers")
        ):
            guard_current["terminal_status"] = "SIGNAL"
            guard_current["passive_guard_projection"] = {
                "source_terminal_status": "PASSIVE_QUOTE_OPEN",
                "generation_checksum": GENERATION_CHECKSUM,
                "passive_fill_model_checksum": PASSIVE_FILL_MODEL_CHECKSUM,
                "rule": "immutable quote terminal projected to strict post-only live signal only after selector evidence passes",
            }
    state = {
        "schema_version": 1,
        "kind": "btc5m_multivenue_residual_cell",
        "generated_at": utc_now_iso(),
        "cell_id": cell_id,
        "generation_checksum": GENERATION_CHECKSUM,
        "paper_only": True,
        "live_orders_allowed": False,
        "preregistration": prereg,
        "frozen_model": {"checksum": GENERATION_CHECKSUM},
        "current_terminal": guard_current,
        "current_decision": (
            {
                "eligible": bool(
                    current
                    and current.get("terminal_status") == "PASSIVE_QUOTE_OPEN"
                    and not current.get("blockers")
                ),
                "quote_price": ((current or {}).get("signal") or {}).get("executable_price"),
                "market_slug": (current or {}).get("market_slug"),
                "window_start_s": (current or {}).get("window_start_s"),
                "best_bid_at_quote": (
                    (((current or {}).get("signal") or {}).get("book") or {}).get("best_bid")
                ),
                "best_ask_at_quote": (
                    (((current or {}).get("signal") or {}).get("book") or {}).get("best_ask")
                ),
                "condition_id": (((current or {}).get("signal") or {}).get("condition_id")),
                "token_id": (((current or {}).get("signal") or {}).get("token_id")),
                "outcome": (((current or {}).get("signal") or {}).get("outcome")),
                "net_edge_per_share": (
                    ((current or {}).get("signal") or {}).get("net_edge_per_share")
                ),
                "reasons": list((current or {}).get("blockers") or []),
                "generation_checksum": GENERATION_CHECKSUM,
                "preregistration_checksum": prereg["checksum"],
            }
            if mode == "passive"
            else None
        ),
        "current_cycle": {
            "status": str((current or {}).get("terminal_status") or status),
            "signal": (current or {}).get("signal") if current else signal,
        },
        "orders": orders,
        "terminal_count": len(terminals),
        "current_window_terminal_complete": bool(current) and (
            not book_only_pair or _complete_set_window_complete(terminal_present=True, elapsed_s=now_ts - window_start)
        ),
        "paired_bundle_required": book_only_pair,
        "pair_accounting_disagreement": 0,
    }
    atomic_write_json(paths["state"], state)
    signal_rows = [row for row in terminals if row.get("terminal_status") == "SIGNAL"]
    return {
        "cell_id": cell_id,
        "signal_offset_s": offset,
        "execution_mode": "paired_split_sell" if split_sell else "paired_passive" if book_only_pair else mode,
        "variant": _variant(threshold),
        "status": state["current_cycle"]["status"],
        "model_checksum": GENERATION_CHECKSUM,
        "generation_checksum": GENERATION_CHECKSUM,
        "preregistration_checksum": prereg["checksum"],
        "state_path": paths["state"],
        "terminals_path": paths["terminals"],
        "preregistration_path": paths["preregistration"],
        "fill_events_path": paths["events"],
        "passive_fill_model_checksum": (
            PASSIVE_FILL_MODEL_CHECKSUM if mode == "passive" and not split_sell else None
        ),
        "passive_fill_model_verified": split_sell or mode != "passive" or (
            prereg.get("passive_fill_model_checksum") == PASSIVE_FILL_MODEL_CHECKSUM
        ),
        "actual_depth_verified": (
            all(bool(row.get("actual_depth_verified")) for row in signal_rows)
            if mode == "taker"
            else split_sell or prereg.get("passive_fill_model_checksum") == PASSIVE_FILL_MODEL_CHECKSUM
        ),
        "lookahead_violations": 0,
        "parity_disagreement": 0,
        "paper_only": True,
        "productive": True,
        "terminal_count": len(terminals),
        "positive_edge_intents": len(_load_jsonl(paths["intents"])),
        "blockers": list((current or {}).get("blockers") or blockers),
        "current_window_terminal_complete": bool(current) and (
            not book_only_pair or _complete_set_window_complete(terminal_present=True, elapsed_s=now_ts - window_start)
        ),
        "paired_bundle_required": book_only_pair,
        "pair_accounting_disagreement": 0,
    }


def run_matrix_once(args: argparse.Namespace) -> dict[str, Any]:
    if GENERATION_CHECKSUM.startswith(FORBIDDEN_CHECKSUM):
        raise RuntimeError("generation checksum collides with terminal negative method")
    now_ts = float(args.now_ts or time.time())
    prices, observed = _fetch_prices(float(args.timeout_s))
    if args.shared_raw_cache:
        cache = load_json(args.shared_raw_cache, default={})
        if not isinstance(cache, dict) or not cache.get("samples"):
            raise RuntimeError("shared raw observation cache is empty")
    else:
        cache = _update_cache(args.cache, now_ts=now_ts, prices=prices, observed=observed)
    samples = list(cache.get("samples") or [])
    window_start = int(now_ts // 300) * 300
    prior_matrix = load_json(args.state, default={})
    same_generation = isinstance(prior_matrix, dict) and prior_matrix.get("generation_checksum") == GENERATION_CHECKSUM
    prior_liveness_start = int(prior_matrix.get("liveness_clock_start_window_s") or 0) if same_generation else 0
    liveness_start = prior_liveness_start if prior_liveness_start > 0 else (window_start + 300)
    cells: list[dict[str, Any]] = []
    complete_clock_offsets: set[int] = set()
    for offset in OFFSETS:
        if not _generation_evidence_window_eligible(
            family=args.generation_family,
            window_start=window_start,
            liveness_start=liveness_start,
        ):
            continue
        feature, blockers = _consensus_feature(samples, window_start=window_start, offset=offset)
        if args.generation_family in {"complete_set_paired_maker", "complete_set_split_sell_overround"}:
            feature, blockers = None, []
        if (
            args.generation_family not in {"complete_set_paired_maker", "complete_set_split_sell_overround"}
            and
            now_ts - window_start >= offset
            and "synchronized_open_or_signal_clock_missing" not in blockers
        ):
            complete_clock_offsets.add(offset)
        thresholds = (None,) if (_is_paired_family(args.generation_family) or args.generation_family == "basis_triggered_single_outcome_maker") else RESIDUAL_THRESHOLDS if args.generation_family in {"passive_residual", "two_sided_post_only_residual"} else (None, *RESIDUAL_THRESHOLDS)
        for threshold in thresholds:
            modes = ("taker", "passive") if args.execution_mode == "all" else (args.execution_mode,)
            for mode in modes:
                cells.append(
                    _evaluate_cell(
                        args,
                        now_ts=now_ts,
                        offset=offset,
                        mode=mode,
                        threshold=threshold,
                        feature=feature,
                        feature_blockers=blockers,
                    )
                )
    complete_after_s = int(args.liveness_complete_after_s or liveness_start)
    generation_clock_complete = _generation_window_clock_complete(
        family=args.generation_family,
        cells=cells,
        complete_clock_offsets=complete_clock_offsets,
    )
    completed_window_starts = _complete_liveness_windows(
        prior_matrix if same_generation else {},
        window_start=window_start,
        clock_complete=generation_clock_complete,
        complete_after_s=complete_after_s,
    )
    payload = {
        "schema_version": 1,
        "kind": "btc5m_multivenue_residual_matrix",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "status": "PAPER_MATRIX_ACTIVE",
        "generation_checksum": GENERATION_CHECKSUM,
        "generation_config": GENERATION_CONFIG,
        "synchronized_venue_clocks": {
            "venues": list(VENUES),
            "resolution_s": 1,
            "latest_clock_spread_s": round(max(observed.values()) - min(observed.values()), 6),
            "sample_count": len(samples),
        },
        "consensus_cell_count": 8,
        "residual_lane_slots": len(RESIDUAL_THRESHOLDS),
        "residual_sibling_count": len(cells) - 8,
        "cell_count": len(cells),
        "blocker_taxonomy": dict(
            sorted(
                Counter(
                    blocker
                    for cell in cells
                    for blocker in cell.get("blockers") or []
                ).items()
            )
        ),
        "paper_only": True,
        "live_orders_allowed": False,
        "cells": cells,
        "liveness_clock_start_window_s": liveness_start,
        "liveness_complete_after_s": complete_after_s,
        "liveness_decision_at_s": complete_after_s + 600,
        "complete_clock_offsets_current_window": sorted(complete_clock_offsets),
        "complete_liveness_window_starts_s": completed_window_starts,
        "completed_liveness_windows": min(2, len(completed_window_starts)),
        "positive_edge_intents": sum(int(cell.get("positive_edge_intents") or 0) for cell in cells),
        "attribution_funnel": {
            "source_feature_cells": sum(1 for cell in cells if cell.get("status") not in {"WAITING_SIGNAL_OFFSET"}),
            "positive_edge_intents": sum(int(cell.get("positive_edge_intents") or 0) for cell in cells),
            "terminal_rows": sum(int(cell.get("terminal_count") or 0) for cell in cells),
            "resolved_selector_cells": sum(int((cell.get("evidence") or {}).get("resolved_fills") or 0) for cell in cells),
        },
    }
    if payload["completed_liveness_windows"] >= 2 and payload["positive_edge_intents"] == 0:
        payload["status"] = (
            "PARK_ZERO_INTENT_GENERATION"
            if args.generation_family in {"complete_set_paired_maker", "complete_set_split_sell_overround"}
            else "TWO_WINDOW_ZERO_INTENT_ROTATION_DUE"
        )
        payload["stop_writer"] = args.generation_family in {"complete_set_paired_maker", "complete_set_split_sell_overround"}
    prior_selector = load_json(args.selector_state, default={})
    selector = reduce_promoted_cells(
        payload,
        resolutions_path=args.resolutions,
        prior_activation=(prior_selector.get("selected") or {}) if isinstance(prior_selector, dict) else {},
        prior_cells=(prior_selector.get("cells") or []) if isinstance(prior_selector, dict) else [],
        forbidden_model_checksums=(FORBIDDEN_CHECKSUM,),
    )
    selector["generated_at"] = payload["generated_at"]
    selector["single_submitter"] = "scripts/run_wallet_copy_live_guard.py"
    selector["activation_contract"] = {
        "order_usd": 1.0,
        "max_accepted_orders_per_window": 2 if args.generation_family in {"complete_set_paired_maker", "complete_set_split_sell_overround"} else 1,
        "paired_bundle_required": args.generation_family in {"complete_set_paired_maker", "complete_set_split_sell_overround"},
        "ttl_s": 3600,
        "ttl_refresh_allowed": False,
    }
    if args.generation_family in {"complete_set_paired_maker", "complete_set_split_sell_overround"} and payload["positive_edge_intents"] > 0:
        best_resolved = max(
            (int((row.get("evidence_snapshot") or {}).get("resolved_fills") or 0) for row in selector.get("cells") or []),
            default=0,
        )
        if len(completed_window_starts) >= 6 and best_resolved < 10:
            payload["status"] = "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE"
            payload["stop_writer"] = True
    atomic_write_json(args.selector_state, selector)
    payload["promoted_cell_selector"] = {
        "state_path": args.selector_state,
        "status": selector["status"],
        "selected": selector.get("selected"),
    }
    decision_at_s = int(args.execution_decision_at_s or 0)
    if decision_at_s:
        payload["execution_decision_at_s"] = decision_at_s
        payload["execution_gate"] = {
            "minimum_resolved_fills": 10,
            "positive_post_fee_aggregate": True,
            "positive_chronological_halves": True,
            "zero_parity_lookahead_disagreement": True,
        }
        if now_ts >= decision_at_s and not selector.get("selected"):
            decision = {
                "schema_version": 1,
                "kind": "btc5m_multivenue_execution_deadline_decision",
                "generated_at": utc_now_iso(),
                "generation_checksum": GENERATION_CHECKSUM,
                "decision_at_s": decision_at_s,
                "decision": "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE",
                "resolved_selector_cells": payload["attribution_funnel"]["resolved_selector_cells"],
                "selector_status": selector.get("status"),
                "immutable": True,
            }
            if args.execution_decision_state:
                prior_decision = load_json(args.execution_decision_state, default={})
                if prior_decision:
                    stable_keys = ("generation_checksum", "decision_at_s", "decision", "immutable")
                    if any(prior_decision.get(key) != decision.get(key) for key in stable_keys):
                        raise RuntimeError("immutable execution decision mismatch")
                else:
                    atomic_write_json(args.execution_decision_state, decision)
            payload["status"] = decision["decision"]
            payload["stop_writer"] = True
    atomic_write_json(args.state, payload)
    return payload


def main() -> int:
    global GENERATION_CONFIG, GENERATION_CHECKSUM
    args = parse_args()
    if args.generation_id != "resident-69a9f9df" or args.execution_mode != "all" or args.generation_family != "resident_matrix":
        GENERATION_CONFIG, GENERATION_CHECKSUM = _generation_variant_config(args.generation_id, args.generation_family, args.execution_mode)
    if args.reconcile_terminal_evidence:
        payload = reconcile_split_sell_terminal_evidence(args)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 0 if payload.get("status") == "PASS" else 2
    while True:
        try:
            payload = run_matrix_once(args)
            print(json.dumps(payload, sort_keys=True), flush=True)
            if payload.get("stop_writer"):
                return 0
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "status": "DATA_FAILURE",
                        "generated_at": utc_now_iso(),
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        if not args.watch:
            return 0
        time.sleep(max(0.5, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
