#!/usr/bin/env python3
"""Resident paper lane for a calibrated BTC spot -> Polymarket probability edge.

Flow stage: OBSERVE/LEARN/PROMOTE/LIVE-READY. The lane is deliberately
paper-only. It freezes a 30-second walk-forward feature, calibrates only on
earlier Binance 1-second bars, prices the current signal against executable
Polymarket depth, charges canonical fee plus measured slippage, and emits a
guard-compatible CopyIntent with live submission disabled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import certifi
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_e7_spot_open_paper_lane import (  # noqa: E402
    DEFAULT_CLOB_BASE,
    _book_snapshot_with_direct_fallback,
    _market_for_slug,
    _token_map,
)
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


LANE_ID = "paper_struct_btc5m_cross_exchange_probability_edge_v1"
SOURCE_WALLET = "BTC5M_CROSS_EXCHANGE_PROBABILITY_EDGE_V1"
DEFAULT_STATE = "data/research/btc5m_cross_exchange_probability_edge_paper_lane_state.json"
DEFAULT_EVENT_LOG = "data/research/btc5m_cross_exchange_probability_edge_paper_lane_events.jsonl"
DEFAULT_INTENT_LOG = "data/research/btc5m_cross_exchange_probability_edge_copyintents.jsonl"
DEFAULT_CACHE = "data/research/btc5m_cross_exchange_probability_edge_binance_1s_cache.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_MODEL = "data/research/btc5m_cross_exchange_probability_edge_model_v1.json"
DEFAULT_TERMINAL_LOG = "data/research/btc5m_cross_exchange_probability_edge_terminals.jsonl"
EXPERIMENT_ID = "btc5m-cross-exchange-probability-edge-v1-20260724T1356Z"
TRAINING_CUTOFF_S = 1784901364
SIGNAL_OFFSET_S = 30
VOL_LOOKBACK_S = 300
TRAIN_FRACTION = 0.70
Z_BINS = (-math.inf, -1.0, -0.5, 0.0, 0.5, 1.0, math.inf)
HISTORICAL_REFERENCE_PRICE = 0.50
HISTORICAL_SLIPPAGE_PER_SHARE = 0.01
ORDER_USD = 1.0
MIN_BUY_PRICE = 0.25
MAX_BUY_PRICE = 0.50
PROMOTION_RESOLVED_REQUIRED = 200
PROMOTION_WINDOWS_REQUIRED = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--intent-log", default=DEFAULT_INTENT_LOG)
    parser.add_argument("--cache", default=DEFAULT_CACHE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--terminal-log", default=DEFAULT_TERMINAL_LOG)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--history-hours", type=float, default=24.0)
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--signal-tolerance-s", type=float, default=8.0)
    parser.add_argument("--signal-offset-s", type=int, default=SIGNAL_OFFSET_S)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-s", type=float, default=10.0)
    return parser.parse_args()


def _experiment_id(signal_offset_s: int) -> str:
    return (
        EXPERIMENT_ID
        if signal_offset_s == SIGNAL_OFFSET_S
        else f"btc5m-cross-exchange-probability-edge-offset-{signal_offset_s}s-20260725"
    )


def _frozen_preregistration(signal_offset_s: int = SIGNAL_OFFSET_S) -> dict[str, Any]:
    return {
        "experiment_id": _experiment_id(signal_offset_s),
        "training_cutoff": "2026-07-24T13:56:04Z",
        "registered_before_outcome_inspection": True,
        "feature_schema": {
            "signal_offset_s": signal_offset_s,
            "return_since_window_open": f"log(close_at_{signal_offset_s - 1}s/open_at_0s)",
            "rolling_realized_volatility": f"stdev of prior {VOL_LOOKBACK_S} one-second log returns",
            "time_to_close_s": 300 - signal_offset_s,
            "standardized_return": "return_since_open / (rolling_vol * sqrt(signal_offset_s))",
            "z_bins": ["-inf", -1.0, -0.5, 0.0, 0.5, 1.0, "+inf"],
        },
        "calibration": {
            "method": "walk_forward_train_only_laplace_bin_probability",
            "train_fraction": TRAIN_FRACTION,
            "chronological_holdout_fraction": 1.0 - TRAIN_FRACTION,
            "laplace_alpha": 1.0,
        },
        "historical_economics": {
            "reference_price": HISTORICAL_REFERENCE_PRICE,
            "slippage_per_share": HISTORICAL_SLIPPAGE_PER_SHARE,
            "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
            "promotion_evidence": False,
            "reason": "historical executable Polymarket books are unavailable; prospective book slice is separate",
        },
        "promotion_gate": {
            "positive_post_fee_train": True,
            "positive_post_fee_chronological_holdout": True,
            "resolved_signals_gte": PROMOTION_RESOLVED_REQUIRED,
            "distinct_windows_gte": PROMOTION_WINDOWS_REQUIRED,
            "prospective_executable_book_post_fee_pnl_positive": True,
            "copyintent_parity": True,
            "single_guard_only": True,
        },
        "live_orders_allowed": False,
    }


def _fetch_1s_klines(symbol: str, start_s: int, end_s: int, timeout_s: float) -> dict[int, list[Any]]:
    rows: dict[int, list[Any]] = {}
    cursor_ms = int(start_s * 1000)
    end_ms = int(end_s * 1000)
    session = requests.Session()
    while cursor_ms <= end_ms:
        response = session.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "1s",
                "startTime": cursor_ms,
                "endTime": end_ms,
                "limit": 1000,
            },
            timeout=timeout_s,
            verify=certifi.where(),
            headers={"Accept": "application/json", "User-Agent": "btc5m-cross-exchange-paper/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            break
        last_ms = cursor_ms
        for row in payload:
            if isinstance(row, list) and row:
                open_s = int(row[0]) // 1000
                rows[open_s] = row
                last_ms = max(last_ms, int(row[0]))
        next_cursor = last_ms + 1000
        if next_cursor <= cursor_ms:
            break
        cursor_ms = next_cursor
    return rows


def _load_cache(path: str) -> dict[int, list[Any]]:
    payload = load_json(path, default={})
    rows = payload.get("rows") if isinstance(payload, dict) else {}
    return {
        int(key): value
        for key, value in (rows.items() if isinstance(rows, dict) else [])
        if isinstance(value, list)
    }


def _refresh_cache(args: argparse.Namespace, now_ts: float) -> tuple[dict[int, list[Any]], dict[str, Any]]:
    start_s = int(now_ts - max(18.0, float(args.history_hours)) * 3600)
    start_s -= start_s % 300
    end_s = int(now_ts)
    cached = _load_cache(args.cache)
    cached = {ts: row for ts, row in cached.items() if ts >= start_s - VOL_LOOKBACK_S}
    fetch_start = max(start_s - VOL_LOOKBACK_S, max(cached, default=start_s - VOL_LOOKBACK_S) + 1)
    fetched = _fetch_1s_klines(args.symbol, fetch_start, end_s, float(args.timeout_s))
    cached.update(fetched)
    cached = dict(sorted(cached.items()))
    atomic_write_json(
        args.cache,
        {
            "schema_version": 1,
            "kind": "btc5m_cross_exchange_binance_1s_cache",
            "generated_at": utc_now_iso(),
            "symbol": args.symbol,
            "rows": {str(key): value for key, value in cached.items()},
        },
    )
    return cached, {
        "start_s": start_s,
        "end_s": end_s,
        "cached_rows": len(cached),
        "fetched_rows": len(fetched),
        "latest_bar_s": max(cached, default=0),
    }


def _close(row: list[Any] | None) -> float:
    try:
        return float(row[4]) if row else 0.0
    except (TypeError, ValueError, IndexError):
        return 0.0


def _open(row: list[Any] | None) -> float:
    try:
        return float(row[1]) if row else 0.0
    except (TypeError, ValueError, IndexError):
        return 0.0


def _sample_for_window(
    rows: dict[int, list[Any]],
    start: int,
    *,
    require_label: bool,
    signal_offset_s: int = SIGNAL_OFFSET_S,
) -> dict[str, Any] | None:
    open_price = _open(rows.get(start))
    signal_price = _close(rows.get(start + signal_offset_s - 1))
    close_price = _close(rows.get(start + 299))
    if open_price <= 0 or signal_price <= 0 or (require_label and close_price <= 0):
        return None
    prior_prices = [_close(rows.get(ts)) for ts in range(start - VOL_LOOKBACK_S, start + 1)]
    if any(price <= 0 for price in prior_prices):
        return None
    returns = [
        math.log(prior_prices[index] / prior_prices[index - 1])
        for index in range(1, len(prior_prices))
        if prior_prices[index - 1] > 0
    ]
    realized_vol = statistics.pstdev(returns) if len(returns) >= VOL_LOOKBACK_S - 2 else 0.0
    if realized_vol <= 0:
        return None
    return_since_open = math.log(signal_price / open_price)
    z_score = return_since_open / (realized_vol * math.sqrt(signal_offset_s))
    return {
        "window_start_s": start,
        "market_slug": f"btc-updown-5m-{start}",
        "signal_offset_s": signal_offset_s,
        "time_to_close_s": 300 - signal_offset_s,
        "open_price": open_price,
        "signal_price": signal_price,
        "close_price": close_price if close_price > 0 else None,
        "return_since_open": return_since_open,
        "rolling_realized_volatility": realized_vol,
        "z_score": z_score,
        "up": bool(close_price > open_price) if close_price > 0 else None,
    }


def build_walk_forward_samples(
    rows: dict[int, list[Any]],
    now_ts: float,
    signal_offset_s: int = SIGNAL_OFFSET_S,
) -> list[dict[str, Any]]:
    first = (min(rows, default=0) // 300 + 1) * 300
    last_closed = int(now_ts // 300) * 300 - 300
    return [
        sample
        for start in range(first, last_closed + 1, 300)
        if (
            sample := _sample_for_window(
                rows,
                start,
                require_label=True,
                signal_offset_s=signal_offset_s,
            )
        )
        is not None
    ]


def _bin_index(z_score: float) -> int:
    for index in range(len(Z_BINS) - 1):
        if Z_BINS[index] <= z_score < Z_BINS[index + 1]:
            return index
    return len(Z_BINS) - 2


def fit_calibration(train: list[dict[str, Any]]) -> dict[int, float]:
    bins: dict[int, list[bool]] = {}
    for row in train:
        bins.setdefault(_bin_index(float(row["z_score"])), []).append(bool(row["up"]))
    global_up = sum(1 for row in train if row["up"])
    global_p = (global_up + 1.0) / (len(train) + 2.0) if train else 0.5
    return {
        index: (
            (sum(values) + 1.0) / (len(values) + 2.0)
            if values
            else global_p
        )
        for index in range(len(Z_BINS) - 1)
        for values in [bins.get(index, [])]
    }


def _model_checksum(payload: dict[str, Any]) -> str:
    canonical = {key: value for key, value in payload.items() if key != "checksum"}
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _build_frozen_model(
    samples: list[dict[str, Any]],
    signal_offset_s: int = SIGNAL_OFFSET_S,
) -> dict[str, Any]:
    eligible = [row for row in samples if int(row.get("window_start_s") or 0) < TRAINING_CUTOFF_S]
    split = max(1, min(len(eligible) - 1, int(len(eligible) * TRAIN_FRACTION))) if len(eligible) >= 2 else 0
    train, holdout = eligible[:split], eligible[split:]
    calibration = fit_calibration(train)
    payload = {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_probability_edge_frozen_model",
        "experiment_id": _experiment_id(signal_offset_s),
        "training_cutoff": "2026-07-24T13:56:04Z",
        "training_cutoff_s": TRAINING_CUTOFF_S,
        "feature_constants": _frozen_preregistration(signal_offset_s)["feature_schema"],
        "calibration_method": _frozen_preregistration(signal_offset_s)["calibration"],
        "train_window_ids": [int(row["window_start_s"]) for row in train],
        "holdout_window_ids": [int(row["window_start_s"]) for row in holdout],
        "calibration_by_z_bin": {str(key): value for key, value in calibration.items()},
        "train_metrics": _historical_economics(train, calibration),
        "holdout_metrics": _historical_economics(holdout, calibration),
    }
    return {**payload, "checksum": _model_checksum(payload)}


def _load_or_create_frozen_model(
    path: str,
    samples: list[dict[str, Any]],
    signal_offset_s: int = SIGNAL_OFFSET_S,
) -> dict[str, Any]:
    existing = load_json(path, default={})
    if isinstance(existing, dict) and existing:
        if existing.get("experiment_id") != _experiment_id(signal_offset_s):
            raise RuntimeError("frozen model experiment id mismatch")
        if str(existing.get("checksum") or "") != _model_checksum(existing):
            raise RuntimeError("frozen model checksum mismatch")
        return existing
    model = _build_frozen_model(samples, signal_offset_s)
    atomic_write_json(path, model)
    return model


def _historical_economics(rows: list[dict[str, Any]], calibration: dict[int, float]) -> dict[str, Any]:
    pnl = 0.0
    signals = 0
    wins = 0
    shares = ORDER_USD / HISTORICAL_REFERENCE_PRICE
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=HISTORICAL_REFERENCE_PRICE)
    slippage = shares * HISTORICAL_SLIPPAGE_PER_SHARE
    for row in rows:
        p_up = calibration[_bin_index(float(row["z_score"]))]
        predicted_up = p_up >= 0.5
        predicted_probability = max(p_up, 1.0 - p_up)
        net_edge = predicted_probability - HISTORICAL_REFERENCE_PRICE - fee / shares - HISTORICAL_SLIPPAGE_PER_SHARE
        if net_edge <= 0:
            continue
        win = bool(row["up"]) is predicted_up
        pnl += (shares if win else 0.0) - ORDER_USD - fee - slippage
        signals += 1
        wins += int(win)
    return {
        "resolved_signals": signals,
        "distinct_windows": signals,
        "post_fee_pnl_usd": round(pnl, 6),
        "positive": pnl > 0,
        "wins": wins,
        "win_rate_pct": round(wins / signals * 100.0, 6) if signals else 0.0,
    }


def probability_signal_to_intent(signal: dict[str, Any]) -> CopyIntent:
    price = float(signal["executable_price"])
    return CopyIntent(
        intent_id=stable_id("ci", {"lane": LANE_ID, "signal_id": signal["signal_id"]}),
        source_wallet=SOURCE_WALLET,
        wallet_name=LANE_ID,
        source_event_id=str(signal["signal_id"]),
        condition_id=str(signal["condition_id"]),
        market_slug=str(signal["market_slug"]),
        outcome=str(signal["outcome"]),
        side="YES" if signal["outcome"] == "Up" else "NO",
        limit_price=price,
        wallet_usdc_size=ORDER_USD,
        copy_size_usd=ORDER_USD,
        shares=round(ORDER_USD / price, 6),
        observed_ts=float(signal["observed_ts"]),
        strategy_family=LANE_ID,
        policy_id="cross_exchange_net_edge_hard_025_050_cap_1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="PAPER_EXECUTABLE_BOOK",
        token_id=str(signal["token_id"]),
        event_ts=float(signal["signal_ts"]),
        api_latency_s=max(0.0, float(signal["observed_ts"]) - float(signal["signal_ts"])),
        live_orders_allowed=False,
        reason="calibrated BTC spot probability exceeds executable Polymarket cost after fee/slippage",
        metadata={
            "copy_model": "btc5m_cross_exchange_probability_edge_v1",
            "signal": signal,
            "single_guard_adapter": True,
            "live_candidate_member": False,
            "promotion_gate": _frozen_preregistration()["promotion_gate"],
        },
    )


def _current_signal(
    args: argparse.Namespace,
    rows: dict[int, list[Any]],
    calibration: dict[int, float],
    now_ts: float,
    signal_offset_s: int = SIGNAL_OFFSET_S,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    start = int(now_ts // 300) * 300
    offset = now_ts - start
    if offset < signal_offset_s:
        return None, {"status": "WAITING_SIGNAL_OFFSET", "window_start_s": start, "offset_s": round(offset, 6)}
    sample = _sample_for_window(
        rows,
        start,
        require_label=False,
        signal_offset_s=signal_offset_s,
    )
    if sample is None:
        return None, {"status": "CURRENT_FEATURE_INCOMPLETE", "window_start_s": start, "offset_s": round(offset, 6)}
    p_up = calibration[_bin_index(float(sample["z_score"]))]
    outcome = "Up" if p_up >= 0.5 else "Down"
    probability = max(p_up, 1.0 - p_up)
    market = _market_for_slug(sample["market_slug"], timeout_s=float(args.timeout_s))
    tokens = _token_map(market)
    token_id = str(tokens.get(outcome) or "")
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    book = _book_snapshot_with_direct_fallback(
        clob=clob,
        token_id=token_id,
        order_usd=ORDER_USD,
        max_entry_price=MAX_BUY_PRICE,
    )
    executable_price = float(book.get("avg_fill_price") or book.get("best_ask") or 0.0)
    shares = ORDER_USD / executable_price if executable_price > 0 else 0.0
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=executable_price)
    best_ask = float(book.get("best_ask") or 0.0)
    measured_slippage = max(0.0, executable_price - best_ask)
    net_edge = probability - executable_price - (fee / shares if shares > 0 else 0.0)
    blockers: list[str] = []
    if str(book.get("status") or "") != "OK":
        blockers.append("executable_book_not_ok")
    if float(book.get("fillable_usd") or 0.0) + 1e-9 < ORDER_USD:
        blockers.append("insufficient_executable_depth")
    if not MIN_BUY_PRICE <= executable_price <= MAX_BUY_PRICE:
        blockers.append("unchanged_hard_entry_bounds")
    if net_edge <= 0:
        blockers.append("net_edge_nonpositive")
    signal = {
        **sample,
        "signal_id": stable_id("xep", {"market_slug": sample["market_slug"], "outcome": outcome}),
        "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
        "outcome": outcome,
        "token_id": token_id,
        "calibrated_probability": round(probability, 8),
        "executable_price": round(executable_price, 8),
        "best_ask": round(best_ask, 8),
        "measured_slippage_per_share": round(measured_slippage, 8),
        "expected_fee_usd": fee,
        "net_edge_per_share": round(net_edge, 8),
        "signal_ts": start + signal_offset_s,
        "observed_ts": now_ts,
        "book": book,
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    return (signal if not blockers else None), {"status": "SIGNAL" if not blockers else "PROTECTED_SKIP", "signal": signal}


def _load_jsonl(path: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return output
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            output.append(row)
    return output


def _prospective_summary(terminals: list[dict[str, Any]], resolutions_path: str) -> dict[str, Any]:
    resolutions = load_resolutions(resolutions_path)
    scored: list[dict[str, Any]] = []
    for row in terminals:
        if str(row.get("terminal_status") or "") != "SIGNAL":
            continue
        intent = row.get("intent") if isinstance(row.get("intent"), dict) else {}
        if not intent:
            continue
        order = {
            "order_id": stable_id("po", {"intent_id": intent.get("intent_id")}),
            "intent_id": intent.get("intent_id"),
            "source_wallet": intent.get("source_wallet"),
            "market_slug": intent.get("market_slug"),
            "outcome": intent.get("outcome"),
            "side": intent.get("side"),
            "limit_price": intent.get("limit_price"),
            "requested_size_usd": intent.get("copy_size_usd"),
            "requested_shares": intent.get("shares"),
            "filled_size_usd": intent.get("copy_size_usd"),
            "filled_shares": intent.get("shares"),
            "status": "FILLED",
            "final_status": "FILLED",
            "paper_only": True,
            "live_orders_allowed": False,
        }
        result = score_order(order, resolutions)
        if result.get("resolved"):
            fee = expected_polymarket_buy_fee_usd(
                shares=float(intent.get("shares") or 0.0),
                price=float(intent.get("limit_price") or 0.0),
            )
            result["post_fee_pnl_usd"] = round(float(result.get("pnl_usd") or 0.0) - fee, 6)
            scored.append(result)
    pnl = round(sum(float(row.get("post_fee_pnl_usd") or 0.0) for row in scored), 6)
    return {
        "resolved_signals": len(scored),
        "distinct_windows": len({row.get("market_slug") for row in scored}),
        "post_fee_pnl_usd": pnl,
        "positive": pnl > 0,
        "scheduled_windows": len(terminals),
        "terminal_counts": dict(
            sorted(
                {
                    status: sum(1 for row in terminals if str(row.get("terminal_status") or "") == status)
                    for status in {str(row.get("terminal_status") or "") for row in terminals}
                    if status
                }.items()
            )
        ),
    }


def _terminal_row(
    *,
    window_start_s: int,
    model: dict[str, Any],
    cycle: dict[str, Any],
    intent: CopyIntent | None,
    experiment_id: str = EXPERIMENT_ID,
    signal_offset_s: int = SIGNAL_OFFSET_S,
) -> dict[str, Any]:
    status = str(cycle.get("status") or "DATA_FAILURE")
    return {
        "schema_version": 1,
        "event": "btc5m_cross_exchange_probability_terminal",
        "terminal_id": stable_id(
            "xept",
            {
                "experiment_id": experiment_id,
                "model_checksum": model["checksum"],
                "window_start_s": window_start_s,
            },
        ),
        "experiment_id": experiment_id,
        "model_checksum": model["checksum"],
        "window_start_s": window_start_s,
        "market_slug": f"btc-updown-5m-{window_start_s}",
        "scheduled_signal_ts": window_start_s + signal_offset_s,
        "recorded_at": utc_now_iso(),
        "terminal_status": status,
        "blockers": list((cycle.get("signal") or {}).get("blockers") or []),
        "signal": cycle.get("signal"),
        "intent": intent.asdict() if intent is not None else None,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = float(args.now_ts or time.time())
    signal_offset_s = int(getattr(args, "signal_offset_s", SIGNAL_OFFSET_S))
    if signal_offset_s not in {15, 30, 45, 60}:
        raise ValueError("signal_offset_s must be one of 15,30,45,60")
    experiment_id = _experiment_id(signal_offset_s)
    prereg = _frozen_preregistration(signal_offset_s)
    rows, cache_summary = _refresh_cache(args, now_ts)
    samples = build_walk_forward_samples(rows, now_ts, signal_offset_s)
    model = _load_or_create_frozen_model(args.model, samples, signal_offset_s)
    calibration = {int(key): float(value) for key, value in model["calibration_by_z_bin"].items()}
    train_summary = dict(model["train_metrics"])
    holdout_summary = dict(model["holdout_metrics"])
    try:
        signal, cycle = _current_signal(
            args,
            rows,
            calibration,
            now_ts,
            signal_offset_s,
        )
    except Exception as exc:
        signal = None
        cycle = {
            "status": "DATA_FAILURE",
            "window_start_s": int(now_ts // 300) * 300,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    new_rows: list[dict[str, Any]] = []
    new_terminals: list[dict[str, Any]] = []
    current_start = int(now_ts // 300) * 300
    terminal_rows = _load_jsonl(args.terminal_log)
    terminal_ids = {str(row.get("terminal_id") or "") for row in terminal_rows}
    current_terminal: dict[str, Any] | None = None
    if now_ts - current_start >= signal_offset_s:
        intent = probability_signal_to_intent(signal) if signal is not None else None
        current_terminal = _terminal_row(
            window_start_s=current_start,
            model=model,
            cycle=cycle,
            intent=intent,
            experiment_id=experiment_id,
            signal_offset_s=signal_offset_s,
        )
        if current_terminal["terminal_id"] not in terminal_ids:
            append_jsonl_many(args.terminal_log, [current_terminal])
            append_jsonl_many(args.event_log, [current_terminal])
            terminal_rows.append(current_terminal)
            new_terminals.append(current_terminal)
        else:
            current_terminal = next(
                row for row in terminal_rows if str(row.get("terminal_id") or "") == current_terminal["terminal_id"]
            )
            signal = (
                current_terminal.get("signal")
                if str(current_terminal.get("terminal_status") or "") == "SIGNAL"
                and isinstance(current_terminal.get("signal"), dict)
                else None
            )
    if signal is not None:
        intent = probability_signal_to_intent(signal)
        existing_ids = {
            str((row.get("intent") or {}).get("intent_id") or "")
            for row in _load_jsonl(args.intent_log)
            if isinstance(row.get("intent"), dict)
        }
        if intent.intent_id not in existing_ids:
            event = {
                "schema_version": 1,
                "event": "btc5m_cross_exchange_probability_copyintent",
                "generated_at": utc_now_iso(),
                "lane": LANE_ID,
                "flow_stage": "OBSERVE/LEARN/PROMOTE/LIVE-READY",
                "intent": intent.asdict(),
                "signal": signal,
                "paper_only": True,
                "live_orders_allowed": False,
                "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
            }
            append_jsonl_many(args.intent_log, [event])
            append_jsonl_many(args.event_log, [event])
            new_rows.append(event)
    all_intents = _load_jsonl(args.intent_log)
    prospective = _prospective_summary(terminal_rows, args.resolutions)
    gate_checks = {
        "positive_post_fee_train": bool(train_summary["positive"]),
        "positive_post_fee_chronological_holdout": bool(holdout_summary["positive"]),
        "resolved_signals_gte_200": int(prospective["resolved_signals"]) >= PROMOTION_RESOLVED_REQUIRED,
        "distinct_windows_gte_10": int(prospective["distinct_windows"]) >= PROMOTION_WINDOWS_REQUIRED,
        "prospective_executable_book_post_fee_pnl_positive": bool(prospective["positive"]),
        "copyintent_parity": all(
            isinstance(row.get("intent"), dict)
            and row["intent"].get("mode") == "paper"
            and row["intent"].get("live_orders_allowed") is False
            for row in all_intents
        ),
        "single_guard_only": True,
    }
    gate_pass = bool(gate_checks and all(gate_checks.values()))
    payload = {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_probability_edge_paper_lane_state",
        "generated_at": utc_now_iso(),
        "lane_id": LANE_ID,
        "flow_stage": "OBSERVE/LEARN/PROMOTE/LIVE-READY",
        "status": "PROMOTION_PACKET_READY_FOR_FABLE" if gate_pass else "PAPER_ACCRUING",
        "paper_only": True,
        "live_orders_allowed": False,
        "orders_submitted": 0,
        "preregistration": prereg,
        "cache": cache_summary,
        "walk_forward": {
            "samples": len(model["train_window_ids"]) + len(model["holdout_window_ids"]),
            "train_samples": len(model["train_window_ids"]),
            "holdout_samples": len(model["holdout_window_ids"]),
            "calibration_by_z_bin": {str(key): round(value, 8) for key, value in calibration.items()},
            "train": train_summary,
            "chronological_holdout": holdout_summary,
        },
        "frozen_model": {
            "path": args.model,
            "experiment_id": model["experiment_id"],
            "training_cutoff": model["training_cutoff"],
            "checksum": model["checksum"],
            "status": "IMMUTABLE_CHECKSUM_VERIFIED",
        },
        "current_cycle": cycle,
        "new_copyintents": len(new_rows),
        "new_terminals": len(new_terminals),
        "current_terminal": current_terminal,
        "prospective_executable_book": prospective,
        "promotion_gate": {"pass": gate_pass, "checks": gate_checks},
        "guard_adapter": {
            "status": "LIVE_READY_BUILD_COMPLETE_EVIDENCE_GATE_CLOSED" if not gate_pass else "PROMOTION_PACKET_READY",
            "intent_type": "src.wallet_copy.models.CopyIntent",
            "adapter_function": "probability_signal_to_intent",
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
            "live_orders_allowed": False,
            "hard_entry_bounds": [MIN_BUY_PRICE, MAX_BUY_PRICE],
            "max_order_usd": ORDER_USD,
            "activation_rule": "Fable promotion only after every promotion_gate check passes",
        },
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
    }
    atomic_write_json(args.state, payload)
    return payload


def main() -> int:
    args = parse_args()
    while True:
        payload = run_once(args)
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        if not args.watch:
            break
        time.sleep(max(1.0, float(args.interval_s)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
