"""Paper-only 1:1 Weird-Peak wallet copy flow.

This module is intentionally narrower than the inventory/consensus
Weird-Peak paper flow: one confirmed Weird-Peak wallet BUY trade becomes one
paper copy order with the same condition, token, outcome, and limit price.
Confirmed wallet copy also mirrors the wallet's USDC order size by default.
There is no execution path, and confirmed wallet copy is not capped by 5m
window.

Wallet lifecycle events are tracked in a separate paper-only ledger. That keeps
BUY order mirroring auditable while letting the copy model account for Weird-Peak
MERGE/REDEEM activity and future SELL rows without double-counting expiry PnL.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from src.weird_peak_paper_flow import (
    UTC,
    _event_age_s,
    _inventory_from_orders,
    _resolve_order,
    _update_window_resolution,
    event_canonical_key,
    load_json,
    load_resolutions,
    num,
    resolution_for_order,
    utc_now_iso,
)
from src.weird_peak_fast_inferred_paper_flow import build_token_outcome_map
from src.weird_peak_wallet_tracker import MarketWsEventLogBuffer, _row_ts_s, _ws_items, append_jsonl, atomic_write_json

MIRROR_PRICE_TOLERANCE = 0.000001


@dataclass(frozen=True)
class WeirdPeakExactCopyConfig:
    target_wallet: str
    state_path: str = "data/research/weird_peak_exact_copy_paper_flow_state.json"
    event_log_path: str = "data/research/weird_peak_exact_copy_paper_flow_events.jsonl"
    resolutions_path: str = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
    raw_pm_events_path: str = "data/lead_lag_raw_pm_events.jsonl"
    tracker_state_path: str = "data/research/weird_peak_exact_copy_wallet_tracker_state.json"
    confirmed_paper_state_path: str = "data/research/weird_peak_paper_flow_state.json"
    wallet_trades_path: str = "data/wallet_analysis/trades_30d.json"
    wallet_activity_path: str = "data/wallet_analysis/activity_30d.json"
    gamma_events_api: str = "https://gamma-api.polymarket.com/events"
    order_usd: float = 1.0
    wallet_size_fraction: float = 1.0
    max_order_usd: float = 0.0
    max_orders_per_window: int = 12
    max_window_usd: float = 12.0
    max_confirmed_age_s: float = 300.0
    hard_price_cap: float = 0.98
    min_price: float = 0.01
    require_market_ws_match_for_copy: bool = False
    enable_fast_preconfirm: bool = True
    require_fast_preconfirm_wallet_attribution: bool = True
    allow_pending_wallet_attribution_preconfirm: bool = False
    enable_wallet_api_preconfirm: bool = True
    wallet_api_preconfirm_max_latency_s: float = 120.0
    fast_preconfirm_lookback_s: float = 10.0
    fast_preconfirm_min_ws_size: float = 1.0
    max_fast_preconfirm_per_poll: int = 60
    fast_confirm_window_s: float = 240.0
    fast_confirm_timeout_s: float = 900.0
    fast_confirm_price_tolerance: float = 0.000001
    fast_confirm_min_resolved_for_rate_gate: int = 10
    fast_confirm_min_confirmation_rate_pct: float = 60.0
    wallet_history_confirm_refresh_s: float = 60.0
    wallet_history_confirm_lookback_s: float = 3600.0
    wallet_history_confirm_max_rows_per_file: int = 20_000
    enable_wallet_history_replay: bool = True
    wallet_history_replay_lookback_s: float = 86_400.0
    wallet_history_replay_max_rows_per_file: int = 60_000
    wallet_history_replay_max_events_per_poll: int = 50_000
    enable_gamma_token_map: bool = True
    gamma_token_map_windows: int = 6
    gamma_token_map_refresh_s: float = 20.0
    gamma_timeout_s: float = 3.0
    market_ws_tail_lines: int = 150_000
    market_ws_tail_max_bytes: int = 128 * 1024 * 1024
    retain_processed_keys: int = 100_000
    retain_processed_signal_keys: int = 50_000
    retain_orders: int = 100_000


def exact_copy_order_id(canonical_key: str) -> str:
    return hashlib.sha256(f"weird_peak_exact_copy|{canonical_key}".encode()).hexdigest()[:24]


def fast_preconfirm_order_id(signal_key: str) -> str:
    return hashlib.sha256(f"weird_peak_exact_fast_preconfirm|{signal_key}".encode()).hexdigest()[:24]


def lifecycle_event_id(canonical_key: str) -> str:
    return hashlib.sha256(f"weird_peak_exact_lifecycle|{canonical_key}".encode()).hexdigest()[:24]


def fast_signal_key(item: dict[str, Any], row_ts: float | None) -> str:
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    return "|".join(
        [
            str(item.get("market") or ""),
            str(item.get("token_id") or ""),
            f"{num(item.get('price')):.10f}",
            f"{num(item.get('size')):.10f}",
            str(item.get("book_hash") or ""),
            str(raw.get("timestamp") or ""),
            f"{float(row_ts or 0.0):.6f}",
        ]
    )


def wallet_api_preconfirm_signal_key(event: dict[str, Any]) -> str:
    return "wallet_api_preconfirm|" + event_canonical_key(event)


def _parse_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _window_start_from_slug(slug: Any) -> int | None:
    parts = str(slug or "").rsplit("-", 1)
    if len(parts) != 2:
        return None
    try:
        value = int(parts[1])
    except ValueError:
        return None
    return value if value > 0 else None


def _active_btc_gamma_token_map(
    state: dict[str, Any],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    existing = state.get("active_market_token_outcome_map")
    if not isinstance(existing, dict):
        existing = {}
    meta = state.get("active_market_token_outcome_map_meta")
    if not isinstance(meta, dict):
        meta = {}
    if not config.enable_gamma_token_map:
        return existing, {"status": "DISABLED", "enabled": False, "token_count": len(existing)}
    generated_at_s = num(meta.get("generated_at_s"), 0.0)
    if generated_at_s > 0 and now_ts - generated_at_s < float(config.gamma_token_map_refresh_s):
        return existing, {**meta, "status": meta.get("status") or "CACHED", "token_count": len(existing)}

    current_5m = (int(now_ts) // 300) * 300
    start_5m = current_5m - 300
    token_map: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    fetched_slugs: list[str] = []
    for offset in range(max(1, int(config.gamma_token_map_windows)) + 2):
        window_start = start_5m + offset * 300
        slug = f"btc-updown-5m-{window_start}"
        try:
            response = requests.get(
                str(config.gamma_events_api),
                params={"slug": slug},
                timeout=float(config.gamma_timeout_s),
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            errors.append(f"{slug}:{type(exc).__name__}")
            continue
        events = payload if isinstance(payload, list) else []
        if not events:
            continue
        event = events[0] if isinstance(events[0], dict) else {}
        markets = event.get("markets") if isinstance(event.get("markets"), list) else []
        if not markets or not isinstance(markets[0], dict):
            continue
        market = markets[0]
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
        outcomes = [str(item) for item in _parse_list(market.get("outcomes"))]
        token_ids = [str(item) for item in _parse_list(market.get("clobTokenIds") or market.get("clob_token_ids"))]
        if not condition_id or len(outcomes) < 2 or len(token_ids) < 2:
            errors.append(f"{slug}:missing_condition_or_tokens")
            continue
        fetched_slugs.append(slug)
        for idx, token_id in enumerate(token_ids):
            outcome = outcomes[idx] if idx < len(outcomes) else ""
            if token_id and outcome in {"Up", "Down"}:
                token_map[token_id] = {
                    "condition_id": condition_id,
                    "token_id": token_id,
                    "outcome": outcome,
                    "side": "YES" if outcome == "Up" else "NO",
                    "market_slug": slug,
                    "window_start_s": window_start,
                    "title": event.get("title") or event.get("slug") or market.get("question"),
                    "source": "gamma_active_btc_5m_market_metadata",
                }

    status = "PASS" if token_map else "WATCH"
    meta = {
        "enabled": True,
        "status": status,
        "generated_at": utc_now_iso(),
        "generated_at_s": now_ts,
        "token_count": len(token_map),
        "market_count": len(fetched_slugs),
        "fetched_slugs": fetched_slugs[-10:],
        "errors": errors[-10:],
        "refresh_s": float(config.gamma_token_map_refresh_s),
    }
    state["active_market_token_outcome_map"] = token_map
    state["active_market_token_outcome_map_meta"] = meta
    return token_map, meta


def _initial_state(config: WeirdPeakExactCopyConfig) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "btc_weird_peak_exact_copy_paper_flow",
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "target_wallet": config.target_wallet.lower(),
        "strategy_families": [
            "btc_weird_peak_confirmed_exact_copy_v1",
            "btc_weird_peak_latency_first_exact_copy_v1",
        ],
        "processed_trade_keys": [],
        "processed_signal_keys": [],
        "paper_orders": [],
        "wallet_lifecycle_events": [],
        "windows": {},
        "resolved_order_ids": [],
        "token_outcome_map": {},
        "copy_contract": _copy_contract([], [], config),
    }


def _skip_record(reason: str, event: dict[str, Any], *, canonical_key: str, now_ts: float) -> dict[str, Any]:
    ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
    return {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_exact_copy_skip",
        "reason": reason,
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "candidate_family": "btc_weird_peak_confirmed_exact_copy_v1",
        "canonical_key": canonical_key,
        "target_wallet": str(event.get("target_wallet") or "").lower(),
        "condition_id": str(event.get("condition_id") or ""),
        "market_slug": str(event.get("market_slug") or ""),
        "token_id": str(event.get("token_id") or ""),
        "wallet_action": str(event.get("side") or "").upper(),
        "wallet_outcome": str(event.get("outcome") or ""),
        "wallet_price": num(event.get("price")),
        "wallet_size": num(event.get("size")),
        "wallet_usdc_size": round(num(event.get("usdc_size")), 6),
        "event_ts": event.get("event_ts"),
        "observed_ts": event.get("observed_ts"),
        "api_latency_s": event.get("api_latency_s"),
        "market_ws_matched": bool(ws.get("matched")),
        "ws_to_api_observed_latency_s": ws.get("ws_to_api_observed_latency_s"),
        "event_age_s": _event_age_s(event, now_ts),
    }


def _fast_skip_record(
    reason: str,
    item: dict[str, Any],
    *,
    signal_key: str,
    row_ts: float | None,
    now_ts: float,
) -> dict[str, Any]:
    return {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_exact_fast_preconfirm_skip",
        "reason": reason,
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "candidate_family": "btc_weird_peak_latency_first_exact_copy_v1",
        "signal_key": signal_key,
        "condition_id": str(item.get("market") or ""),
        "token_id": str(item.get("token_id") or ""),
        "ws_event_type": item.get("event_type"),
        "ws_side": str(item.get("side") or "").upper(),
        "ws_price": num(item.get("price")),
        "ws_size": num(item.get("size")),
        "ws_recv_ts": row_ts,
        "signal_age_s": round(max(0.0, now_ts - row_ts), 6) if row_ts is not None else None,
    }


def _copy_size_usd(event: dict[str, Any], config: WeirdPeakExactCopyConfig, remaining_window_usd: float) -> float:
    wallet_usdc = max(0.0, num(event.get("usdc_size")))
    base = max(0.0, float(config.order_usd))
    if config.wallet_size_fraction > 0.0 and wallet_usdc > 0.0:
        base = wallet_usdc * float(config.wallet_size_fraction)
    max_order_usd = max(0.0, float(config.max_order_usd))
    if max_order_usd > 0:
        base = min(base, max_order_usd)
    return round(max(0.0, min(base, remaining_window_usd)), 6)


def _sizing_policy_type(config: WeirdPeakExactCopyConfig) -> str:
    wallet_fraction = float(config.wallet_size_fraction)
    max_order_usd = max(0.0, float(config.max_order_usd))
    if wallet_fraction == 1.0 and max_order_usd <= 0.0:
        return "full_wallet_size_clone"
    if wallet_fraction > 0.0:
        return "wallet_fraction_capped_size" if max_order_usd > 0.0 else "wallet_fraction_size"
    return "own_capped_size" if max_order_usd > 0.0 else "own_fixed_size"


def _wallet_lifecycle_action(event: dict[str, Any]) -> str:
    row_type = str(event.get("row_type") or "").upper()
    side = str(event.get("side") or "").upper()
    if row_type == "TRADE" and side == "SELL":
        return "SELL"
    if row_type in {"MERGE", "REDEEM"}:
        return row_type
    return ""


def _lifecycle_record(
    action: str,
    event: dict[str, Any],
    *,
    canonical_key: str,
    now_ts: float,
) -> dict[str, Any]:
    ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
    condition_id = str(event.get("condition_id") or "")
    outcome = str(event.get("outcome") or "")
    return {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_exact_copy_lifecycle_event",
        "paper_lifecycle_event_id": lifecycle_event_id(canonical_key),
        "canonical_key": canonical_key,
        "candidate_family": "btc_weird_peak_confirmed_exact_copy_lifecycle_v1",
        "inventory_family": "btc_weird_peak_exact_copy_inventory_v1",
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "target_wallet": str(event.get("target_wallet") or "").lower(),
        "condition_id": condition_id,
        "market_slug": str(event.get("market_slug") or ""),
        "window_start_s": event.get("window_start_s"),
        "transaction_hash": event.get("transaction_hash"),
        "row_type": str(event.get("row_type") or "").upper(),
        "wallet_action": action,
        "action": action,
        "side": str(event.get("side") or "").upper(),
        "wallet_outcome": outcome,
        "outcome": outcome,
        "wallet_token_id": str(event.get("token_id") or ""),
        "token_id": str(event.get("token_id") or ""),
        "wallet_price": num(event.get("price")),
        "wallet_size": num(event.get("size")),
        "wallet_usdc_size": round(num(event.get("usdc_size")), 6),
        "wallet_event_ts": event.get("event_ts"),
        "wallet_observed_ts": event.get("observed_ts"),
        "api_latency_s": event.get("api_latency_s"),
        "market_ws_corroboration": ws or None,
        "market_ws_matched": bool(ws.get("matched")),
        "ws_to_api_observed_latency_s": ws.get("ws_to_api_observed_latency_s"),
        "event_age_s": _event_age_s(event, now_ts),
        "paper_lifecycle_policy": "mirror_wallet_lifecycle_action_against_own_scaled_inventory",
        "reason": f"confirmed Weird-Peak wallet {action} lifecycle action recorded in paper-only ledger",
    }


def _window(state: dict[str, Any], condition_id: str, event: dict[str, Any]) -> dict[str, Any]:
    windows = state.setdefault("windows", {})
    window = windows.setdefault(
        condition_id,
        {
            "condition_id": condition_id,
            "market_slug": event.get("market_slug"),
            "title": event.get("title"),
            "window_start_s": event.get("window_start_s"),
            "paper_order_count": 0,
            "copy_order_count": 0,
            "cost_usd": 0.0,
            "orders": [],
            "wallet_lifecycle_event_ids": [],
            "inventory": {},
            "wallet_lifecycle": {},
            "latest_wallet_event_ts": None,
            "latest_copy_ts": None,
            "resolved": None,
        },
    )
    window["market_slug"] = window.get("market_slug") or event.get("market_slug")
    window["title"] = window.get("title") or event.get("title")
    window["window_start_s"] = window.get("window_start_s") or event.get("window_start_s")
    return window


def _is_confirmed_wallet_buy(event: dict[str, Any]) -> tuple[bool, str]:
    if str(event.get("row_type") or "").upper() != "TRADE":
        return False, "not_trade_row"
    if str(event.get("side") or "").upper() != "BUY":
        return False, "wallet_side_not_buy"
    if str(event.get("outcome") or "") not in {"Up", "Down"}:
        return False, "missing_or_unknown_outcome"
    if not str(event.get("condition_id") or ""):
        return False, "missing_condition_id"
    if not str(event.get("token_id") or ""):
        return False, "missing_token_id"
    if num(event.get("price")) <= 0:
        return False, "missing_wallet_price"
    return True, ""


def _candidate_orders(orders_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [order for order in orders_by_id.values() if isinstance(order, dict)]


def _is_fast_preconfirm_order(order: dict[str, Any]) -> bool:
    return str(order.get("source_mode") or "") in {
        "market_ws_preconfirm",
        "market_ws_preconfirm_confirmed",
        "wallet_api_preconfirm",
        "wallet_api_preconfirm_confirmed",
    }


def _is_pending_fast_preconfirm_order(order: dict[str, Any]) -> bool:
    return str(order.get("source_mode") or "") in {"market_ws_preconfirm", "wallet_api_preconfirm"}


def _is_confirmed_fast_preconfirm_order(order: dict[str, Any]) -> bool:
    return str(order.get("source_mode") or "") in {
        "market_ws_preconfirm_confirmed",
        "wallet_api_preconfirm_confirmed",
    }


def _is_invalidated_fast_preconfirm_order(order: dict[str, Any]) -> bool:
    return str(order.get("source_mode") or "") == "market_ws_preconfirm_invalidated" or str(
        order.get("confirmation_status") or ""
    ).startswith("INVALIDATED_")


def _active_inventory_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [order for order in orders if not _is_invalidated_fast_preconfirm_order(order)]


def _existing_order_for_canonical_key(
    orders_by_id: dict[str, dict[str, Any]],
    canonical_key: str,
) -> dict[str, Any] | None:
    exact_id = exact_copy_order_id(canonical_key)
    if exact_id in orders_by_id:
        return orders_by_id[exact_id]
    for order in _candidate_orders(orders_by_id):
        if str(order.get("canonical_key") or "") == canonical_key:
            return order
        if str(order.get("source_wallet_canonical_key") or "") == canonical_key:
            return order
        if str(order.get("confirmed_wallet_canonical_key") or "") == canonical_key:
            return order
    return None


def _matches_confirmed_event(
    order: dict[str, Any],
    event: dict[str, Any],
    config: WeirdPeakExactCopyConfig,
) -> bool:
    if order.get("action") != "BUY":
        return False
    if str(order.get("condition_id") or "").lower() != str(event.get("condition_id") or "").lower():
        return False
    if str(order.get("token_id") or "") != str(event.get("token_id") or ""):
        return False
    if str(order.get("outcome") or "") != str(event.get("outcome") or ""):
        return False
    if abs(num(order.get("limit_price")) - num(event.get("price"))) > float(config.fast_confirm_price_tolerance):
        return False
    order_tx_hash = str(order.get("preconfirm_wallet_tx_hash") or order.get("transaction_hash") or "").lower()
    event_tx_hash = str(event.get("transaction_hash") or "").lower()
    if order_tx_hash and event_tx_hash and order_tx_hash == event_tx_hash:
        return True
    observed_ts = num(event.get("observed_ts"), 0.0)
    event_ts = num(event.get("event_ts"), 0.0)
    preconfirm_ts = num(order.get("preconfirm_observed_ts"), 0.0) or num(order.get("ws_recv_ts"), 0.0)
    if preconfirm_ts <= 0:
        return False
    deltas = [abs(ts - preconfirm_ts) for ts in (observed_ts, event_ts) if ts > 0]
    return bool(deltas) and min(deltas) <= float(config.fast_confirm_window_s)


def _find_matching_preconfirm_order(
    orders_by_id: dict[str, dict[str, Any]],
    event: dict[str, Any],
    config: WeirdPeakExactCopyConfig,
) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_delta: float | None = None
    observed_ts = num(event.get("observed_ts"), 0.0)
    event_ts = num(event.get("event_ts"), 0.0)
    for order in _candidate_orders(orders_by_id):
        if not _is_pending_fast_preconfirm_order(order):
            continue
        if order.get("confirmation_status") == "CONFIRMED":
            continue
        if not _matches_confirmed_event(order, event, config):
            continue
        preconfirm_ts = num(order.get("preconfirm_observed_ts"), 0.0) or num(order.get("ws_recv_ts"), 0.0)
        deltas = [abs(ts - preconfirm_ts) for ts in (observed_ts, event_ts) if ts > 0]
        delta = min(deltas) if deltas else 0.0
        if best is None or best_delta is None or delta < best_delta:
            best = order
            best_delta = delta
    return best


def _find_same_tx_pending_market_ws_preconfirm_orders(
    orders_by_id: dict[str, dict[str, Any]],
    event: dict[str, Any],
) -> list[dict[str, Any]]:
    tx_hash = str(event.get("transaction_hash") or "").lower()
    if not tx_hash.startswith("0x"):
        return []
    return [
        order
        for order in _candidate_orders(orders_by_id)
        if str(order.get("source_mode") or "") == "market_ws_preconfirm"
        and order.get("confirmation_status") == "PENDING_CONFIRMATION"
        and str(order.get("preconfirm_wallet_tx_hash") or "").lower() == tx_hash
    ]


def _invalidate_mismatched_market_ws_preconfirm_orders(
    orders_by_id: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
    new_records: list[dict[str, Any]],
) -> dict[str, Any]:
    invalidated = 0
    matched_same_side = 0
    for event in events:
        is_buy, _reason = _is_confirmed_wallet_buy(event)
        if not is_buy:
            continue
        key = event_canonical_key(event)
        same_tx_orders = _find_same_tx_pending_market_ws_preconfirm_orders(orders_by_id, event)
        for order in same_tx_orders:
            if _matches_confirmed_event(order, event, config):
                matched_same_side += 1
                continue
            previous_status = str(order.get("wallet_attribution_status") or "")
            order.update(
                {
                    "source_mode": "market_ws_preconfirm_invalidated",
                    "confirmation_status": "INVALIDATED_BY_CONFIRMED_WALLET_API",
                    "confirmed_wallet_match": False,
                    "confirmed_wallet_canonical_key": key,
                    "invalidated_at": datetime.fromtimestamp(now_ts, UTC).isoformat(),
                    "invalidation_reason": "same_tx_hash_confirmed_wallet_api_different_token_outcome_or_price",
                    "invalidated_by_wallet_condition_id": event.get("condition_id"),
                    "invalidated_by_wallet_token_id": event.get("token_id"),
                    "invalidated_by_wallet_outcome": event.get("outcome"),
                    "invalidated_by_wallet_price": num(event.get("price")),
                    "invalidated_by_wallet_size": num(event.get("size")),
                    "invalidated_by_wallet_usdc_size": round(num(event.get("usdc_size")), 6),
                    "invalidated_by_wallet_trade_ts": event.get("event_ts"),
                    "invalidated_by_wallet_observed_ts": event.get("observed_ts"),
                    "wallet_attribution_preconfirm_status": previous_status or None,
                    "wallet_attribution_status": "SUPERSEDED_BY_CONFIRMED_WALLET_API",
                    "wallet_attribution_confirmed": False,
                    "wallet_attribution_live_admissible": False,
                    "wallet_attribution_confirmation_basis": (
                        "same transaction hash was confirmed by Weird-Peak wallet API on a different token/outcome/price"
                    ),
                    "paper_active": False,
                    "excluded_from_live_admission": True,
                    "excluded_from_inventory": True,
                }
            )
            new_records.append(
                {
                    "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
                    "event": "weird_peak_exact_fast_preconfirm_invalidated",
                    "paper_order_id": order.get("paper_order_id"),
                    "candidate_family": order.get("candidate_family"),
                    "read_only": True,
                    "paper_only": True,
                    "can_trade": False,
                    "live_orders_allowed": False,
                    "condition_id": order.get("condition_id"),
                    "token_id": order.get("token_id"),
                    "outcome": order.get("outcome"),
                    "limit_price": order.get("limit_price"),
                    "preconfirm_wallet_tx_hash": order.get("preconfirm_wallet_tx_hash"),
                    "confirmed_wallet_canonical_key": key,
                    "invalidated_by_wallet_token_id": event.get("token_id"),
                    "invalidated_by_wallet_outcome": event.get("outcome"),
                    "invalidated_by_wallet_price": num(event.get("price")),
                    "wallet_attribution_preconfirm_status": previous_status or None,
                    "wallet_attribution_status": order.get("wallet_attribution_status"),
                    "invalidation_reason": order.get("invalidation_reason"),
                }
            )
            invalidated += 1
    return {
        "enabled": True,
        "invalidated": invalidated,
        "matched_same_side_pending": matched_same_side,
        "mode": "same_tx_hash_market_ws_preconfirm_truth_loop_invalidation",
    }


def _copy_lifecycle_order(order: dict[str, Any]) -> bool:
    return (
        order.get("candidate_family") == "btc_weird_peak_confirmed_exact_copy_v1"
        or _is_confirmed_fast_preconfirm_order(order)
    )


def _empty_position() -> dict[str, dict[str, float]]:
    return {
        "Up": {"shares": 0.0, "cost_usd": 0.0, "wallet_shares": 0.0, "wallet_cost_usd": 0.0},
        "Down": {"shares": 0.0, "cost_usd": 0.0, "wallet_shares": 0.0, "wallet_cost_usd": 0.0},
    }


def _reduce_position(position: dict[str, float], shares: float) -> float:
    available = max(0.0, num(position.get("shares")))
    if shares <= 0 or available <= 0:
        return 0.0
    take = min(available, shares)
    avg_cost = num(position.get("cost_usd")) / available if available > 0 else 0.0
    cost = take * avg_cost
    position["shares"] = max(0.0, available - take)
    position["cost_usd"] = max(0.0, num(position.get("cost_usd")) - cost)
    return cost


def _reduce_wallet_position(position: dict[str, float], shares: float) -> float:
    available = max(0.0, num(position.get("wallet_shares")))
    if shares <= 0 or available <= 0:
        return 0.0
    take = min(available, shares)
    avg_cost = num(position.get("wallet_cost_usd")) / available if available > 0 else 0.0
    cost = take * avg_cost
    position["wallet_shares"] = max(0.0, available - take)
    position["wallet_cost_usd"] = max(0.0, num(position.get("wallet_cost_usd")) - cost)
    return cost


def _snapshot_position(positions: dict[str, dict[str, float]]) -> dict[str, Any]:
    up = positions["Up"]
    down = positions["Down"]
    paired_shares = min(num(up.get("shares")), num(down.get("shares")))
    paired_cost = 0.0
    if paired_shares > 0:
        up_avg = num(up.get("cost_usd")) / num(up.get("shares")) if num(up.get("shares")) > 0 else 0.0
        down_avg = num(down.get("cost_usd")) / num(down.get("shares")) if num(down.get("shares")) > 0 else 0.0
        paired_cost = paired_shares * (up_avg + down_avg)
    residual_outcome = ""
    residual_shares = 0.0
    if num(up.get("shares")) > num(down.get("shares")):
        residual_outcome = "Up"
        residual_shares = num(up.get("shares")) - num(down.get("shares"))
    elif num(down.get("shares")) > num(up.get("shares")):
        residual_outcome = "Down"
        residual_shares = num(down.get("shares")) - num(up.get("shares"))
    return {
        "up": {
            "shares": round(num(up.get("shares")), 6),
            "cost_usd": round(num(up.get("cost_usd")), 6),
            "wallet_shares": round(num(up.get("wallet_shares")), 6),
            "wallet_cost_usd": round(num(up.get("wallet_cost_usd")), 6),
        },
        "down": {
            "shares": round(num(down.get("shares")), 6),
            "cost_usd": round(num(down.get("cost_usd")), 6),
            "wallet_shares": round(num(down.get("wallet_shares")), 6),
            "wallet_cost_usd": round(num(down.get("wallet_cost_usd")), 6),
        },
        "total_cost_usd": round(num(up.get("cost_usd")) + num(down.get("cost_usd")), 6),
        "both_sides": num(up.get("shares")) > 0 and num(down.get("shares")) > 0,
        "paired_shares": round(paired_shares, 6),
        "paired_cost_usd": round(paired_cost, 6),
        "paired_edge_usd": round(paired_shares - paired_cost, 6),
        "residual_outcome": residual_outcome,
        "residual_shares": round(residual_shares, 6),
    }


def _winner_for_orders(orders: list[dict[str, Any]]) -> str:
    for order in orders:
        resolution = order.get("resolution") if isinstance(order.get("resolution"), dict) else {}
        winner = str(resolution.get("winner") or "").upper()
        if winner in {"UP", "DOWN"}:
            return "Up" if winner == "UP" else "Down"
    return ""


def _window_lifecycle_model(
    window: dict[str, Any],
    orders_by_id: dict[str, dict[str, Any]],
    lifecycle_events_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    order_ids = [str(item) for item in window.get("orders") or []]
    orders = [
        orders_by_id[order_id]
        for order_id in order_ids
        if order_id in orders_by_id and _copy_lifecycle_order(orders_by_id[order_id])
    ]
    lifecycle_ids = [str(item) for item in window.get("wallet_lifecycle_event_ids") or []]
    lifecycle_events = [lifecycle_events_by_id[event_id] for event_id in lifecycle_ids if event_id in lifecycle_events_by_id]
    timeline: list[tuple[float, int, str, dict[str, Any]]] = []
    for idx, order in enumerate(orders):
        timeline.append((num(order.get("wallet_trade_ts"), 0.0), idx, "BUY", order))
    offset = len(timeline)
    for idx, event in enumerate(lifecycle_events):
        timeline.append((num(event.get("wallet_event_ts"), 0.0), offset + idx, str(event.get("action") or ""), event))
    timeline.sort(key=lambda item: (item[0], item[1]))

    positions = _empty_position()
    realized_pnl = 0.0
    realized_cash = 0.0
    realized_cost = 0.0
    action_counts: dict[str, int] = {}
    applied_events: list[dict[str, Any]] = []
    ignored_events: list[dict[str, Any]] = []

    for _ts, _idx, action, row in timeline:
        action_counts[action] = action_counts.get(action, 0) + 1
        if action == "BUY":
            outcome = str(row.get("outcome") or "")
            if outcome not in positions:
                continue
            positions[outcome]["shares"] += num(row.get("shares"))
            positions[outcome]["cost_usd"] += num(row.get("size_usd"))
            positions[outcome]["wallet_shares"] += num(row.get("wallet_size"))
            positions[outcome]["wallet_cost_usd"] += num(row.get("wallet_usdc_size"))
            continue

        if action == "MERGE":
            paper_pair = min(num(positions["Up"].get("shares")), num(positions["Down"].get("shares")))
            wallet_pair = min(num(positions["Up"].get("wallet_shares")), num(positions["Down"].get("wallet_shares")))
            wallet_merge_shares = max(0.0, num(row.get("wallet_size") or row.get("wallet_usdc_size")))
            ratio = min(1.0, wallet_merge_shares / wallet_pair) if wallet_pair > 0 else (1.0 if paper_pair > 0 else 0.0)
            paper_merge_shares = min(paper_pair, paper_pair * ratio)
            if paper_merge_shares <= 0:
                ignored_events.append({"action": action, "reason": "no_wallet_copy_inventory_available", "wallet_event_ts": row.get("wallet_event_ts")})
                continue
            up_cost = _reduce_position(positions["Up"], paper_merge_shares)
            down_cost = _reduce_position(positions["Down"], paper_merge_shares)
            if wallet_pair > 0:
                _reduce_wallet_position(positions["Up"], min(wallet_pair, wallet_merge_shares))
                _reduce_wallet_position(positions["Down"], min(wallet_pair, wallet_merge_shares))
            cost = up_cost + down_cost
            cash = paper_merge_shares
            realized_cash += cash
            realized_cost += cost
            realized_pnl += cash - cost
            applied_events.append(
                {
                    "action": action,
                    "paper_shares": round(paper_merge_shares, 6),
                    "wallet_shares": round(wallet_merge_shares, 6),
                    "paper_cash_usd": round(cash, 6),
                    "paper_cost_basis_usd": round(cost, 6),
                    "paper_realized_pnl_usd": round(cash - cost, 6),
                    "wallet_event_ts": row.get("wallet_event_ts"),
                }
            )
            continue

        if action == "SELL":
            outcome = str(row.get("outcome") or "")
            if outcome not in positions:
                ignored_events.append({"action": action, "reason": "missing_or_unknown_outcome", "wallet_event_ts": row.get("wallet_event_ts")})
                continue
            wallet_available = num(positions[outcome].get("wallet_shares"))
            paper_available = num(positions[outcome].get("shares"))
            wallet_sell_shares = max(0.0, num(row.get("wallet_size")))
            ratio = min(1.0, wallet_sell_shares / wallet_available) if wallet_available > 0 else (1.0 if paper_available > 0 else 0.0)
            paper_sell_shares = min(paper_available, paper_available * ratio)
            if paper_sell_shares <= 0:
                ignored_events.append({"action": action, "reason": "no_inventory_available", "wallet_event_ts": row.get("wallet_event_ts")})
                continue
            cost = _reduce_position(positions[outcome], paper_sell_shares)
            if wallet_available > 0:
                _reduce_wallet_position(positions[outcome], min(wallet_available, wallet_sell_shares))
            cash = paper_sell_shares * num(row.get("wallet_price"))
            realized_cash += cash
            realized_cost += cost
            realized_pnl += cash - cost
            applied_events.append(
                {
                    "action": action,
                    "outcome": outcome,
                    "paper_shares": round(paper_sell_shares, 6),
                    "wallet_shares": round(wallet_sell_shares, 6),
                    "paper_cash_usd": round(cash, 6),
                    "paper_cost_basis_usd": round(cost, 6),
                    "paper_realized_pnl_usd": round(cash - cost, 6),
                    "wallet_event_ts": row.get("wallet_event_ts"),
                }
            )
            continue

        if action == "REDEEM":
            applied_events.append(
                {
                    "action": action,
                    "wallet_shares": round(max(0.0, num(row.get("wallet_size") or row.get("wallet_usdc_size"))), 6),
                    "wallet_event_ts": row.get("wallet_event_ts"),
                    "paper_effect": "claim_residual_winning_inventory_after_resolution",
                }
            )

    final_inventory = _snapshot_position(positions)
    winner = _winner_for_orders(orders)
    resolved: dict[str, Any] | None = None
    if orders and winner:
        residual_cost = final_inventory["total_cost_usd"]
        residual_payout = num(positions[winner].get("shares")) if winner in positions else 0.0
        total_cost = realized_cost + residual_cost
        total_pnl = realized_pnl + residual_payout - residual_cost
        resolved = {
            "winner": winner.upper(),
            "buy_order_count": len(orders),
            "lifecycle_event_count": len(lifecycle_events),
            "realized_cash_usd": round(realized_cash, 6),
            "realized_cost_basis_usd": round(realized_cost, 6),
            "realized_pnl_usd": round(realized_pnl, 6),
            "residual_cost_usd": round(residual_cost, 6),
            "residual_payout_usd": round(residual_payout, 6),
            "total_cost_basis_usd": round(total_cost, 6),
            "total_pnl_usd": round(total_pnl, 6),
            "roi_pct": round((total_pnl / total_cost * 100.0) if total_cost > 0 else 0.0, 6),
            "settlement_basis": "lifecycle_realized_pnl_plus_residual_expiry_inventory",
        }
    status = "PASS"
    blockers: list[str] = []
    if ignored_events:
        status = "WATCH"
        blockers.append("lifecycle_event_without_matching_paper_inventory")
    return {
        "status": status,
        "blockers": blockers,
        "copy_model": "wallet_buy_plus_merge_redeem_sell_lifecycle_on_own_scaled_inventory",
        "paper_only": True,
        "live_orders_allowed": False,
        "buy_order_count": len(orders),
        "lifecycle_event_count": len(lifecycle_events),
        "action_counts": action_counts,
        "final_inventory": final_inventory,
        "realized_cash_usd": round(realized_cash, 6),
        "realized_cost_basis_usd": round(realized_cost, 6),
        "realized_pnl_usd": round(realized_pnl, 6),
        "applied_events": applied_events[-20:],
        "ignored_events": ignored_events[-20:],
        "resolved": resolved,
    }


def _confirm_preconfirm_order(
    order: dict[str, Any],
    event: dict[str, Any],
    *,
    canonical_key: str,
    now_ts: float,
) -> dict[str, Any]:
    observed_ts = num(event.get("observed_ts"), 0.0)
    preconfirm_ts = num(order.get("preconfirm_observed_ts"), 0.0) or num(order.get("ws_recv_ts"), 0.0)
    confirmation_latency_s = observed_ts - preconfirm_ts if observed_ts > 0 and preconfirm_ts > 0 else None
    ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
    previous_source_mode = str(order.get("source_mode") or "")
    confirmed_source_mode = (
        "wallet_api_preconfirm_confirmed"
        if previous_source_mode == "wallet_api_preconfirm"
        else "market_ws_preconfirm_confirmed"
    )
    previous_attribution_status = str(order.get("wallet_attribution_status") or "")
    order.update(
        {
            "source_mode": confirmed_source_mode,
            "confirmation_status": "CONFIRMED",
            "confirmed_wallet_match": True,
            "confirmed_at": datetime.fromtimestamp(now_ts, UTC).isoformat(),
            "confirmed_wallet_canonical_key": canonical_key,
            "wallet_trade_ts": event.get("event_ts"),
            "wallet_observed_ts": event.get("observed_ts"),
            "api_latency_s": event.get("api_latency_s"),
            "market_ws_corroboration": ws or order.get("market_ws_corroboration"),
            "market_ws_matched": bool(ws.get("matched") or order.get("market_ws_matched")),
            "ws_to_api_observed_latency_s": round(confirmation_latency_s, 6) if confirmation_latency_s is not None else None,
            "confirmation_latency_s": round(confirmation_latency_s, 6) if confirmation_latency_s is not None else None,
            "transaction_hash": event.get("transaction_hash"),
            "wallet_condition_id": event.get("condition_id"),
            "wallet_token_id": event.get("token_id"),
            "wallet_action": "BUY",
            "wallet_outcome": event.get("outcome"),
            "wallet_price": num(event.get("price")),
            "wallet_size": num(event.get("size")),
            "wallet_usdc_size": round(num(event.get("usdc_size")), 6),
            "wallet_attribution_status": "CONFIRMED_BY_WALLET_API",
            "wallet_attribution_preconfirm_status": previous_attribution_status or None,
            "wallet_attribution_confirmed": True,
            "wallet_attribution_confirmed_at": datetime.fromtimestamp(now_ts, UTC).isoformat(),
            "wallet_attribution_live_admissible": True,
            "wallet_attribution_confirmation_basis": "confirmed Weird-Peak wallet API/onchain BUY matched by token, price, outcome, condition, and time window",
        }
    )
    return {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_exact_fast_preconfirm_confirmed",
        "paper_order_id": order.get("paper_order_id"),
        "candidate_family": order.get("candidate_family"),
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "condition_id": order.get("condition_id"),
        "token_id": order.get("token_id"),
        "outcome": order.get("outcome"),
        "confirmed_wallet_canonical_key": canonical_key,
        "confirmation_latency_s": order.get("confirmation_latency_s"),
        "wallet_attribution_status": order.get("wallet_attribution_status"),
        "wallet_attribution_preconfirm_status": previous_attribution_status or None,
    }


def _mark_expired_preconfirm_orders(
    orders_by_id: dict[str, dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
    new_records: list[dict[str, Any]],
) -> None:
    for order in _candidate_orders(orders_by_id):
        if not _is_pending_fast_preconfirm_order(order):
            continue
        if order.get("confirmation_status") != "PENDING_CONFIRMATION":
            continue
        preconfirm_ts = num(order.get("preconfirm_observed_ts"), 0.0) or num(order.get("ws_recv_ts"), 0.0)
        if preconfirm_ts <= 0 or now_ts - preconfirm_ts <= float(config.fast_confirm_timeout_s):
            continue
        order["confirmation_status"] = "UNCONFIRMED_EXPIRED"
        order["confirmed_wallet_match"] = False
        if order.get("wallet_attribution_status") == "PENDING_WALLET_ATTRIBUTION":
            order["wallet_attribution_status"] = "WALLET_ATTRIBUTION_EXPIRED"
        order["wallet_attribution_confirmed"] = False
        order["wallet_attribution_live_admissible"] = False
        new_records.append(
            {
                "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
                "event": "weird_peak_exact_fast_preconfirm_expired",
                "paper_order_id": order.get("paper_order_id"),
                "candidate_family": order.get("candidate_family"),
                "read_only": True,
                "paper_only": True,
                "can_trade": False,
                "live_orders_allowed": False,
                "condition_id": order.get("condition_id"),
                "token_id": order.get("token_id"),
                "outcome": order.get("outcome"),
                "preconfirm_ts": preconfirm_ts,
                "age_s": round(now_ts - preconfirm_ts, 6),
                "wallet_attribution_status": order.get("wallet_attribution_status"),
            }
        )


def _process_fast_preconfirm_signals(
    state: dict[str, Any],
    orders_by_id: dict[str, dict[str, Any]],
    processed_signals: set[str],
    token_map: dict[str, dict[str, Any]],
    wallet_attributed_tx_hashes: set[str],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
    new_records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not config.enable_fast_preconfirm:
        return {"enabled": False, "status": "DISABLED"}

    if (
        config.require_fast_preconfirm_wallet_attribution
        and not config.allow_pending_wallet_attribution_preconfirm
        and not wallet_attributed_tx_hashes
    ):
        return {
            "enabled": True,
            "status": "WATCH",
            "watch_reasons": ["wallet_attribution_source_empty"],
            "created": 0,
            "pending_wallet_attribution_created": 0,
            "wallet_tx_hash_preconfirmed_created": 0,
            "anonymous_created": 0,
            "skip_counts": {},
            "lookback_s": float(config.fast_preconfirm_lookback_s),
            "token_outcome_map_count": len(token_map),
            "wallet_attribution_required": True,
            "pending_wallet_attribution_preconfirm_enabled": False,
            "wallet_attribution_policy": (
                "latency-first market-WS BUY requires target-wallet tx hash/onchain attribution; "
                "generic market WS is diagnostic-only and not paper inventory"
            ),
            "wallet_attribution_note": (
                "no target-wallet tx hash/onchain source available in this poll; "
                "market WS tail skipped to avoid generic non-wallet copy orders"
            ),
            "wallet_attributed_tx_hash_count": 0,
            "market_ws_buffer": {
                "mode": "skipped_no_wallet_attributed_hashes",
                "path": str(config.raw_pm_events_path),
                "row_count": 0,
                "new_rows": 0,
            },
        }

    ws_buffer = MarketWsEventLogBuffer(
        Path(config.raw_pm_events_path),
        max_lines=int(config.market_ws_tail_lines),
        max_bytes=int(config.market_ws_tail_max_bytes),
    )
    rows = ws_buffer.refresh()
    buffer_stats = dict(ws_buffer.stats)
    cutoff = now_ts - max(1.0, float(config.fast_preconfirm_lookback_s))
    created = 0
    pending_wallet_attribution_created = 0
    wallet_tx_hash_preconfirmed_created = 0
    anonymous_created = 0
    skip_counts: dict[str, int] = {}
    signal_items: list[tuple[float | None, dict[str, Any]]] = []
    for row in rows:
        row_ts = _row_ts_s(row)
        if row_ts is None or row_ts < cutoff or row_ts > now_ts + 2.0:
            continue
        for item in _ws_items(row):
            signal_items.append((row_ts, item))
    signal_items.sort(key=lambda pair: pair[0] or 0.0)

    for row_ts, item in signal_items:
        if created >= int(config.max_fast_preconfirm_per_poll):
            break
        signal_key = fast_signal_key(item, row_ts)
        if signal_key in processed_signals:
            continue
        processed_signals.add(signal_key)
        token_id = str(item.get("token_id") or "")
        token_meta = token_map.get(token_id) or {}
        condition_id = str(item.get("market") or token_meta.get("condition_id") or "")
        price = num(item.get("price"))
        ws_size = num(item.get("size"))
        tx_hash = str(item.get("transaction_hash") or "").lower()
        reason = ""
        wallet_attribution_status = "NOT_REQUIRED_ANONYMOUS"
        wallet_attribution_mode = "anonymous_market_ws"
        event_type = str(item.get("event_type") or "")
        if event_type not in {"price_change", "last_trade_price", "trade"}:
            reason = "unsupported_ws_event_type"
        elif str(item.get("side") or "").upper() != "BUY":
            reason = "ws_side_not_buy"
        elif not condition_id:
            reason = "missing_condition_id"
        elif not token_id:
            reason = "missing_token_id"
        elif not token_meta:
            reason = "token_outcome_unknown"
        elif token_meta.get("condition_id") and str(token_meta.get("condition_id")).lower() != condition_id.lower():
            reason = "token_condition_mismatch"
        elif str(token_meta.get("outcome") or "") not in {"Up", "Down"}:
            reason = "token_outcome_unknown"
        elif price < float(config.min_price) or price > float(config.hard_price_cap):
            reason = "price_outside_paper_caps"
        elif ws_size < float(config.fast_preconfirm_min_ws_size):
            reason = "ws_size_below_minimum"
        elif config.require_fast_preconfirm_wallet_attribution:
            if tx_hash and tx_hash in wallet_attributed_tx_hashes:
                wallet_attribution_status = "WALLET_TX_HASH_PRECONFIRMED"
                wallet_attribution_mode = "ws_transaction_hash_target_wallet_source"
            elif tx_hash:
                reason = "wallet_attribution_not_target_wallet"
            elif config.allow_pending_wallet_attribution_preconfirm:
                wallet_attribution_status = "PENDING_WALLET_ATTRIBUTION"
                wallet_attribution_mode = "token_price_size_time_truth_loop"
            else:
                reason = "wallet_attribution_missing_transaction_hash"
        elif any(
            order.get("source_signal_key") == signal_key
            or (
                order.get("action") == "BUY"
                and str(order.get("condition_id") or "").lower() == condition_id.lower()
                and str(order.get("token_id") or "") == token_id
                and abs(num(order.get("limit_price")) - price) <= float(config.fast_confirm_price_tolerance)
                and abs(num(order.get("ws_recv_ts"), row_ts or 0.0) - float(row_ts or 0.0)) < 0.001
            )
            for order in _candidate_orders(orders_by_id)
        ):
            reason = "duplicate_fast_signal"

        if reason:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            if reason not in {
                "ws_side_not_buy",
                "duplicate_fast_signal",
                "wallet_attribution_not_target_wallet",
            }:
                new_records.append(_fast_skip_record(reason, item, signal_key=signal_key, row_ts=row_ts, now_ts=now_ts))
            continue

        window = _window(
            state,
            condition_id,
            {
                "market_slug": token_meta.get("market_slug"),
                "title": token_meta.get("title"),
                "window_start_s": token_meta.get("window_start_s"),
            },
        )
        if int(window.get("paper_order_count") or 0) >= int(config.max_orders_per_window):
            reason = "max_orders_per_window_reached"
        else:
            remaining_usd = max(0.0, float(config.max_window_usd) - num(window.get("cost_usd")))
            if remaining_usd <= 0:
                reason = "max_window_usd_reached"
        if reason:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            new_records.append(_fast_skip_record(reason, item, signal_key=signal_key, row_ts=row_ts, now_ts=now_ts))
            continue

        pseudo_event = {
            "usdc_size": price * ws_size,
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": token_meta.get("outcome"),
            "price": price,
        }
        size_usd = _copy_size_usd(pseudo_event, config, remaining_usd)
        if size_usd <= 0:
            skip_counts["max_window_usd_reached"] = skip_counts.get("max_window_usd_reached", 0) + 1
            new_records.append(_fast_skip_record("max_window_usd_reached", item, signal_key=signal_key, row_ts=row_ts, now_ts=now_ts))
            continue

        outcome = str(token_meta.get("outcome") or "")
        order_id = fast_preconfirm_order_id(signal_key)
        order = {
            "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
            "event": "weird_peak_exact_fast_preconfirm_order",
            "paper_order_id": order_id,
            "canonical_key": signal_key,
            "source_signal_key": signal_key,
            "candidate_family": "btc_weird_peak_latency_first_exact_copy_v1",
            "inventory_family": "btc_weird_peak_exact_copy_inventory_v1",
            "source_mode": "market_ws_preconfirm",
            "confirmation_status": "PENDING_CONFIRMATION",
            "confirmed_wallet_match": None,
            "wallet_attribution_required": bool(config.require_fast_preconfirm_wallet_attribution),
            "wallet_attribution_status": wallet_attribution_status,
            "wallet_attribution_mode": wallet_attribution_mode,
            "wallet_attribution_confirmed": False,
            "wallet_attribution_live_admissible": False,
            "wallet_attribution_confirmation_basis": None,
            "preconfirm_truth_loop_required": wallet_attribution_status
            in {"PENDING_WALLET_ATTRIBUTION", "NOT_REQUIRED_ANONYMOUS"},
            "preconfirm_wallet_tx_hash": tx_hash or None,
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "target_wallet": config.target_wallet.lower(),
            "wallet_condition_id": condition_id,
            "condition_id": condition_id,
            "market_slug": token_meta.get("market_slug"),
            "window_start_s": token_meta.get("window_start_s"),
            "wallet_trade_ts": None,
            "wallet_observed_ts": None,
            "api_latency_s": None,
            "ws_recv_ts": row_ts,
            "signal_age_s": round(max(0.0, now_ts - row_ts), 6) if row_ts is not None else None,
            "ws_price": price,
            "ws_size": ws_size,
            "ws_book_hash": item.get("book_hash"),
            "market_ws_corroboration": {
                "matched": False,
                "source": "lead_lag_raw_pm_events",
                "status": "PENDING_CONFIRMATION",
                "ws_event_type": item.get("event_type"),
                "ws_market": condition_id,
                "ws_book_hash": item.get("book_hash"),
                "ws_price": price,
                "ws_size": ws_size,
                "ws_side": item.get("side"),
                "ws_recv_ts": row_ts,
            },
            "market_ws_matched": False,
            "wallet_token_id": token_id,
            "token_id": token_id,
            "wallet_action": "BUY",
            "action": "BUY",
            "side": "BUY",
            "wallet_outcome": outcome,
            "outcome": outcome,
            "position_side": outcome,
            "wallet_price": price,
            "limit_price": price,
            "wallet_size": None,
            "wallet_usdc_size": None,
            "size_usd": size_usd,
            "shares": round(size_usd / price, 6),
            "sizing_policy": {
                "type": _sizing_policy_type(config),
                "order_usd": float(config.order_usd),
                "wallet_size_fraction": float(config.wallet_size_fraction),
                "max_order_usd": float(config.max_order_usd),
                "max_window_usd": float(config.max_window_usd),
            },
            "order_type": "PAPER_ONLY_LATENCY_FIRST_WALLET_COPY_PRECONFIRM",
            "reason": (
                "fresh market-WS BUY copied in paper quarantine only because pending wallet attribution mode is explicitly enabled"
                if wallet_attribution_status == "PENDING_WALLET_ATTRIBUTION"
                else "fresh market-WS BUY with target-wallet tx hash copied immediately in paper, pending wallet API confirmation"
                if wallet_attribution_status == "WALLET_TX_HASH_PRECONFIRMED"
                else "fresh market-WS BUY copied immediately in paper, pending Weird-Peak wallet confirmation"
            ),
        }
        orders_by_id[order_id] = order
        window["paper_order_count"] = int(window.get("paper_order_count") or 0) + 1
        window["copy_order_count"] = int(window.get("copy_order_count") or 0) + 1
        window["cost_usd"] = round(num(window.get("cost_usd")) + size_usd, 6)
        window["latest_copy_ts"] = order["ts"]
        window["latest_fast_ws_recv_ts"] = row_ts
        window.setdefault("orders", []).append(order_id)
        window["inventory"] = _inventory_from_orders([orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id])
        new_records.append(order)
        created += 1
        if wallet_attribution_status == "PENDING_WALLET_ATTRIBUTION":
            pending_wallet_attribution_created += 1
        elif wallet_attribution_status == "WALLET_TX_HASH_PRECONFIRMED":
            wallet_tx_hash_preconfirmed_created += 1
        elif wallet_attribution_status == "NOT_REQUIRED_ANONYMOUS":
            anonymous_created += 1

    watch_reasons: list[str] = []
    base_status = "PASS" if buffer_stats.get("row_count") and token_map else "WATCH"
    wallet_attribution_note = None
    if config.require_fast_preconfirm_wallet_attribution and not wallet_attributed_tx_hashes:
        if config.allow_pending_wallet_attribution_preconfirm:
            wallet_attribution_note = (
                "market WS has no target-wallet tx hash source in this poll; "
                "generic candidates are explicitly quarantined and excluded unless confirmed by Weird-Peak wallet truth-loop"
            )
        else:
            watch_reasons.append("wallet_attribution_source_empty")
    return {
        "enabled": True,
        "status": "WATCH" if watch_reasons else base_status,
        "watch_reasons": watch_reasons,
        "created": created,
        "pending_wallet_attribution_created": pending_wallet_attribution_created,
        "wallet_tx_hash_preconfirmed_created": wallet_tx_hash_preconfirmed_created,
        "anonymous_created": anonymous_created,
        "skip_counts": dict(sorted(skip_counts.items())),
        "lookback_s": float(config.fast_preconfirm_lookback_s),
        "token_outcome_map_count": len(token_map),
        "wallet_attribution_required": bool(config.require_fast_preconfirm_wallet_attribution),
        "pending_wallet_attribution_preconfirm_enabled": bool(config.allow_pending_wallet_attribution_preconfirm),
        "wallet_attribution_policy": (
            "latency-first market-WS BUY requires target-wallet tx hash/onchain attribution or explicit pending mode; generic WS is diagnostic-only"
            if config.require_fast_preconfirm_wallet_attribution
            else "anonymous market-WS preconfirm allowed; never live-admissible before wallet confirmation"
        ),
        "wallet_attribution_note": wallet_attribution_note,
        "wallet_attributed_tx_hash_count": len(wallet_attributed_tx_hashes),
        "market_ws_buffer": buffer_stats,
    }


def _process_wallet_api_preconfirm_events(
    state: dict[str, Any],
    orders_by_id: dict[str, dict[str, Any]],
    processed_signals: set[str],
    events: list[dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
    new_records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not config.enable_fast_preconfirm:
        return {"enabled": False, "status": "DISABLED", "reason": "fast_preconfirm_disabled"}
    if not config.enable_wallet_api_preconfirm:
        return {"enabled": False, "status": "DISABLED", "reason": "wallet_api_preconfirm_disabled"}

    created = 0
    skip_counts: dict[str, int] = {}
    max_latency_s = max(0.0, float(config.wallet_api_preconfirm_max_latency_s))
    seen_canonical_keys: set[str] = set()
    recent_events = sorted(
        [event for event in events if isinstance(event, dict)],
        key=lambda item: (int(num(item.get("event_ts"), 0.0)), num(item.get("observed_ts"), 0.0)),
    )
    for event in recent_events:
        if created >= int(config.max_fast_preconfirm_per_poll):
            break
        canonical_key = event_canonical_key(event)
        signal_key = wallet_api_preconfirm_signal_key(event)
        if signal_key in processed_signals:
            continue

        reason = ""
        target_wallet = str(event.get("target_wallet") or "").lower()
        is_buy, buy_reason = _is_confirmed_wallet_buy(event)
        price = num(event.get("price"))
        api_latency_value = event.get("api_latency_s")
        api_latency = num(api_latency_value, -1.0)
        observed_ts = num(event.get("observed_ts"), 0.0)
        tx_hash = str(event.get("transaction_hash") or "").lower()
        ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
        if canonical_key in seen_canonical_keys:
            reason = "duplicate_wallet_api_event_in_poll"
        elif target_wallet and target_wallet != config.target_wallet.lower():
            reason = "target_wallet_mismatch"
        elif not is_buy:
            reason = buy_reason
        elif not tx_hash.startswith("0x"):
            reason = "missing_transaction_hash"
        elif api_latency_value is None or api_latency < 0:
            reason = "missing_api_latency"
        elif max_latency_s > 0 and api_latency > max_latency_s:
            reason = "wallet_api_latency_above_preconfirm_threshold"
        elif observed_ts <= 0:
            reason = "missing_observed_ts"
        elif price < float(config.min_price) or price > float(config.hard_price_cap):
            reason = "price_outside_paper_caps"
        elif config.require_market_ws_match_for_copy and ws.get("matched") is not True:
            reason = "market_ws_match_required_but_missing"
        elif _existing_order_for_canonical_key(orders_by_id, canonical_key) is not None:
            reason = "duplicate_existing_wallet_copy"
        elif any(
            _is_pending_fast_preconfirm_order(order)
            and _matches_confirmed_event(order, event, config)
            for order in _candidate_orders(orders_by_id)
        ):
            reason = "duplicate_pending_preconfirm"

        if reason:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            processed_signals.add(signal_key)
            seen_canonical_keys.add(canonical_key)
            continue

        condition_id = str(event.get("condition_id") or "")
        outcome = str(event.get("outcome") or "")
        size_usd = _copy_size_usd(event, config, float("inf"))
        if size_usd <= 0:
            skip_counts["order_size_zero"] = skip_counts.get("order_size_zero", 0) + 1
            processed_signals.add(signal_key)
            continue

        order_id = fast_preconfirm_order_id(signal_key)
        if order_id in orders_by_id:
            skip_counts["duplicate_preconfirm_order_id"] = skip_counts.get("duplicate_preconfirm_order_id", 0) + 1
            processed_signals.add(signal_key)
            continue

        order = {
            "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
            "event": "weird_peak_exact_fast_preconfirm_order",
            "paper_order_id": order_id,
            "canonical_key": signal_key,
            "source_wallet_canonical_key": canonical_key,
            "source_signal_key": signal_key,
            "candidate_family": "btc_weird_peak_latency_first_exact_copy_v1",
            "inventory_family": "btc_weird_peak_exact_copy_inventory_v1",
            "source_mode": "wallet_api_preconfirm",
            "preconfirm_source": "confirmed_wallet_api_recent_event",
            "confirmation_status": "PENDING_CONFIRMATION",
            "confirmed_wallet_match": None,
            "wallet_attribution_required": True,
            "wallet_attribution_status": "WALLET_API_PRECONFIRMED",
            "wallet_attribution_mode": "confirmed_wallet_api_latency_preconfirm",
            "wallet_attribution_confirmed": True,
            "wallet_attribution_confirmed_at": datetime.fromtimestamp(now_ts, UTC).isoformat(),
            "wallet_attribution_live_admissible": True,
            "wallet_attribution_confirmation_basis": (
                "target-wallet Weird-Peak wallet API BUY row observed in latency-first pass before ledger confirmation"
            ),
            "preconfirm_truth_loop_required": False,
            "preconfirm_wallet_tx_hash": tx_hash,
            "preconfirm_observed_ts": observed_ts,
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "target_wallet": config.target_wallet.lower(),
            "wallet_condition_id": condition_id,
            "condition_id": condition_id,
            "market_id": event.get("market_id") or event.get("clob_market_id"),
            "market_slug": event.get("market_slug"),
            "window_start_s": event.get("window_start_s"),
            "wallet_trade_ts": event.get("event_ts"),
            "wallet_observed_ts": event.get("observed_ts"),
            "api_latency_s": event.get("api_latency_s"),
            "event_age_s": _event_age_s(event, now_ts),
            "ws_recv_ts": None,
            "wallet_api_preconfirm_latency_s": round(api_latency, 6),
            "market_ws_corroboration": ws or None,
            "market_ws_matched": bool(ws.get("matched")),
            "transaction_hash": event.get("transaction_hash"),
            "wallet_token_id": event.get("token_id"),
            "token_id": event.get("token_id"),
            "wallet_action": "BUY",
            "action": "BUY",
            "side": "BUY",
            "wallet_outcome": outcome,
            "outcome": outcome,
            "position_side": outcome,
            "wallet_price": price,
            "limit_price": price,
            "wallet_size": num(event.get("size")),
            "wallet_usdc_size": round(num(event.get("usdc_size")), 6),
            "size_usd": size_usd,
            "shares": round(size_usd / price, 6),
            "sizing_policy": {
                "type": _sizing_policy_type(config),
                "order_usd": float(config.order_usd),
                "wallet_size_fraction": float(config.wallet_size_fraction),
                "max_order_usd": float(config.max_order_usd),
                "confirmed_wallet_window_cap_applied": False,
                "confirmed_wallet_window_order_cap": None,
                "confirmed_wallet_window_usd_cap": None,
            },
            "order_type": "PAPER_ONLY_LATENCY_FIRST_WALLET_API_EXACT_COPY_PRECONFIRM",
            "reason": (
                "fresh target-wallet Weird-Peak API BUY copied in paper as latency-first exact-copy preconfirm; "
                "live orders remain disabled"
            ),
        }
        orders_by_id[order_id] = order
        window = _window(state, condition_id, event)
        window["paper_order_count"] = int(window.get("paper_order_count") or 0) + 1
        window["copy_order_count"] = int(window.get("copy_order_count") or 0) + 1
        window["cost_usd"] = round(num(window.get("cost_usd")) + size_usd, 6)
        window["latest_wallet_event_ts"] = event.get("event_ts")
        window["latest_copy_ts"] = order["ts"]
        order_ids = [str(item) for item in window.get("orders") or []]
        if order_id not in order_ids:
            order_ids.append(order_id)
            window["orders"] = order_ids
        window["inventory"] = _inventory_from_orders([orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id])
        new_records.append(order)
        processed_signals.add(signal_key)
        seen_canonical_keys.add(canonical_key)
        created += 1

    status = "PASS" if created > 0 else "WATCH"
    watch_reasons = [] if created > 0 else ["no_recent_wallet_api_event_within_preconfirm_latency_threshold"]
    return {
        "enabled": True,
        "status": status,
        "created": created,
        "skip_counts": dict(sorted(skip_counts.items())),
        "max_latency_s": max_latency_s,
        "event_count": len(recent_events),
        "watch_reasons": watch_reasons,
        "wallet_attribution_policy": (
            "target-wallet API BUY rows can enter paper as fast preconfirm; generic market WS remains diagnostic-only"
        ),
    }


def _promote_existing_wallet_api_copies_to_fast_confirmed(
    orders_by_id: dict[str, dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
    new_records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not config.enable_fast_preconfirm:
        return {"enabled": False, "status": "DISABLED", "reason": "fast_preconfirm_disabled"}
    if not config.enable_wallet_api_preconfirm:
        return {"enabled": False, "status": "DISABLED", "reason": "wallet_api_preconfirm_disabled"}

    migrated = 0
    skip_counts: dict[str, int] = {}
    max_latency_s = max(0.0, float(config.wallet_api_preconfirm_max_latency_s))
    for order in _candidate_orders(orders_by_id):
        if _is_fast_preconfirm_order(order):
            continue
        if order.get("candidate_family") != "btc_weird_peak_confirmed_exact_copy_v1":
            continue
        if order.get("wallet_attribution_mode") != "confirmed_wallet_api":
            skip_counts["not_confirmed_wallet_api"] = skip_counts.get("not_confirmed_wallet_api", 0) + 1
            continue
        tx_hash = str(order.get("transaction_hash") or "").lower()
        api_latency_value = order.get("api_latency_s")
        api_latency = num(api_latency_value, -1.0)
        canonical_key = str(order.get("canonical_key") or "")
        if not tx_hash.startswith("0x"):
            skip_counts["missing_transaction_hash"] = skip_counts.get("missing_transaction_hash", 0) + 1
            continue
        if api_latency_value is None or api_latency < 0:
            skip_counts["missing_api_latency"] = skip_counts.get("missing_api_latency", 0) + 1
            continue
        if max_latency_s > 0 and api_latency > max_latency_s:
            skip_counts["wallet_api_latency_above_preconfirm_threshold"] = (
                skip_counts.get("wallet_api_latency_above_preconfirm_threshold", 0) + 1
            )
            continue
        if not canonical_key:
            skip_counts["missing_canonical_key"] = skip_counts.get("missing_canonical_key", 0) + 1
            continue

        order.update(
            {
                "source_mode": "wallet_api_preconfirm_confirmed",
                "source_wallet_canonical_key": canonical_key,
                "source_signal_key": order.get("source_signal_key") or wallet_api_preconfirm_signal_key(order),
                "preconfirm_source": "confirmed_wallet_api_existing_order_migration",
                "confirmation_status": "CONFIRMED",
                "confirmed_wallet_match": True,
                "confirmed_at": order.get("confirmed_at") or order.get("ts"),
                "confirmed_wallet_canonical_key": canonical_key,
                "wallet_attribution_preconfirm_status": "WALLET_API_PRECONFIRMED_BY_MIGRATION",
                "preconfirm_truth_loop_required": False,
                "preconfirm_wallet_tx_hash": tx_hash,
                "preconfirm_observed_ts": order.get("wallet_observed_ts"),
                "wallet_api_preconfirm_latency_s": round(api_latency, 6),
                "confirmation_latency_s": 0.0,
                "migrated_to_wallet_api_preconfirm_at": datetime.fromtimestamp(now_ts, UTC).isoformat(),
                "migration_reason": (
                    "existing confirmed target-wallet API paper copy met wallet_api_preconfirm_max_latency_s; "
                    "reclassified as fast-confirmed without changing order size, price, inventory, or PnL"
                ),
            }
        )
        new_records.append(
            {
                "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
                "event": "weird_peak_exact_fast_preconfirm_migrated_confirmed",
                "paper_order_id": order.get("paper_order_id"),
                "candidate_family": order.get("candidate_family"),
                "read_only": True,
                "paper_only": True,
                "can_trade": False,
                "live_orders_allowed": False,
                "condition_id": order.get("condition_id"),
                "token_id": order.get("token_id"),
                "outcome": order.get("outcome"),
                "confirmed_wallet_canonical_key": canonical_key,
                "transaction_hash": tx_hash,
                "api_latency_s": api_latency,
                "wallet_attribution_status": order.get("wallet_attribution_status"),
                "wallet_attribution_preconfirm_status": order.get("wallet_attribution_preconfirm_status"),
            }
        )
        migrated += 1

    return {
        "enabled": True,
        "status": "PASS" if migrated > 0 else "WATCH",
        "migrated": migrated,
        "skip_counts": dict(sorted(skip_counts.items())),
        "max_latency_s": max_latency_s,
        "mode": "audit_migration_existing_confirmed_wallet_api_orders_to_fast_confirmed_preconfirm_class",
    }


def _wallet_attributed_tx_hashes(tracker_state: dict[str, Any]) -> set[str]:
    hashes: set[str] = set()
    for event in tracker_state.get("recent_events") or []:
        if not isinstance(event, dict):
            continue
        tx_hash = str(event.get("transaction_hash") or event.get("transactionHash") or "").lower()
        if tx_hash.startswith("0x"):
            hashes.add(tx_hash)
    recent_onchain = tracker_state.get("recent_onchain_logs") if isinstance(tracker_state.get("recent_onchain_logs"), dict) else {}
    for row in recent_onchain.get("logs") or []:
        if not isinstance(row, dict):
            continue
        tx_hash = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        if tx_hash.startswith("0x"):
            hashes.add(tx_hash)
    return hashes


def _is_btc_wallet_history_row(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("title", "slug", "eventSlug", "market_slug", "event_slug", "icon")
    ).lower()
    return "bitcoin" in text or "btc" in text or str(row.get("asset") or "").upper() == "BTC"


def _wallet_history_row_to_event(row: dict[str, Any], config: WeirdPeakExactCopyConfig, *, source: str) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    wallet = str(row.get("proxyWallet") or row.get("proxy_wallet") or row.get("target_wallet") or "").lower()
    if wallet and wallet != config.target_wallet.lower():
        return None
    if not _is_btc_wallet_history_row(row):
        return None
    row_type = str(row.get("type") or row.get("row_type") or "TRADE").upper()
    side = str(row.get("side") or "").upper()
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
    size = num(row.get("size"))
    event_ts = int(num(row.get("_unix_ts") or row.get("timestamp") or row.get("event_ts"), 0.0))
    if not condition_id or size <= 0 or event_ts <= 0:
        return None
    slug = row.get("eventSlug") or row.get("slug") or row.get("market_slug")
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    base: dict[str, Any] = {
        "source": source,
        "row_type": row_type,
        "target_wallet": config.target_wallet.lower(),
        "condition_id": condition_id,
        "market_slug": slug,
        "title": row.get("title"),
        "asset": "BTC",
        "window_start_s": row.get("window_start_s") or _window_start_from_slug(slug),
        "side": side,
        "price": num(row.get("price")),
        "size": size,
        "usdc_size": num(row.get("usdcSize") or row.get("usdc_size"), num(row.get("price")) * size),
        "event_ts": event_ts,
        "observed_ts": event_ts,
        "api_latency_s": None,
        "transaction_hash": tx_hash,
        "wallet_history_source": source,
    }
    if row_type == "TRADE":
        if side not in {"BUY", "SELL"}:
            return None
        outcome = str(row.get("outcome") or "")
        token_id = str(row.get("asset") or row.get("token_id") or "")
        price = num(row.get("price"))
        if outcome not in {"Up", "Down"} or not token_id or price <= 0:
            return None
        return {
            **base,
            "side": side,
            "outcome": outcome,
            "token_id": token_id,
            "dedupe_key": event_canonical_key(
                {
                    "transaction_hash": tx_hash,
                    "condition_id": condition_id,
                    "token_id": token_id,
                    "side": side,
                    "outcome": outcome,
                    "price": price,
                    "size": size,
                    "event_ts": event_ts,
                }
            ),
        }
    if row_type not in {"MERGE", "REDEEM"}:
        return None
    return {
        **base,
        "side": side,
        "outcome": str(row.get("outcome") or ""),
        "token_id": str(row.get("asset") or row.get("token_id") or ""),
        "dedupe_key": event_canonical_key(
            {
                "transaction_hash": tx_hash,
                "condition_id": condition_id,
                "token_id": "",
                "side": row_type,
                "outcome": "",
                "price": 0,
                "size": size,
                "event_ts": event_ts,
            }
        ),
    }


def _wallet_history_file_fingerprint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "mtime_ns": stat.st_mtime_ns,
        "size": stat.st_size,
    }


def _load_wallet_history_rows(path: Path, *, max_rows: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = raw if isinstance(raw, list) else []
    return [row for row in rows[: max(0, int(max_rows))] if isinstance(row, dict)]


def _historical_wallet_events(
    state: dict[str, Any],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
    cache_key: str = "wallet_history_confirm_cache",
    refresh_s: float | None = None,
    lookback_s: float | None = None,
    max_rows_per_file: int | None = None,
    include_lifecycle: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cache = state.get(cache_key) if isinstance(state.get(cache_key), dict) else {}
    paths = [Path(config.wallet_trades_path), Path(config.wallet_activity_path)]
    fingerprint = [_wallet_history_file_fingerprint(path) for path in paths]
    generated_at_s = num(cache.get("generated_at_s"), 0.0)
    refresh_s = float(config.wallet_history_confirm_refresh_s if refresh_s is None else refresh_s)
    cache_lifecycle_context_ok = (
        not include_lifecycle
        or cache.get("lifecycle_context_schema_version") == 1
    )
    if (
        generated_at_s > 0
        and cache.get("fingerprint") == fingerprint
        and isinstance(cache.get("events"), list)
        and cache_lifecycle_context_ok
    ):
        events = [row for row in cache.get("events") or [] if isinstance(row, dict)]
        public_cache = {key: value for key, value in cache.items() if key != "events"}
        return events, {**public_cache, "status": "CACHED", "event_count": len(events)}

    effective_lookback_s = float(config.wallet_history_confirm_lookback_s if lookback_s is None else lookback_s)
    cutoff = now_ts - max(0.0, effective_lookback_s)
    max_rows = max(0, int(config.wallet_history_confirm_max_rows_per_file if max_rows_per_file is None else max_rows_per_file))
    parsed_events: list[tuple[dict[str, Any], str]] = []
    lifecycle_context_conditions: set[str] = set()
    events: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    row_counts: dict[str, int] = {}
    for path, source in ((paths[0], "wallet_history_trades_30d"), (paths[1], "wallet_history_activity_30d")):
        rows = _load_wallet_history_rows(path, max_rows=max_rows)
        row_counts[source] = len(rows)
        for row in rows:
            event = _wallet_history_row_to_event(row, config, source=source)
            if not event:
                continue
            action = _wallet_lifecycle_action(event) or str(event.get("side") or "").upper()
            if action != "BUY" and not include_lifecycle:
                continue
            parsed_events.append((event, action or "UNKNOWN"))
            if include_lifecycle and action in {"MERGE", "REDEEM", "SELL"} and num(event.get("event_ts"), 0.0) >= cutoff:
                condition_id = str(event.get("condition_id") or "")
                if condition_id:
                    lifecycle_context_conditions.add(condition_id)

    context_expanded_event_count = 0
    for event, action in parsed_events:
        event_ts = num(event.get("event_ts"), 0.0)
        condition_id = str(event.get("condition_id") or "")
        in_direct_lookback = event_ts >= cutoff
        in_lifecycle_context = bool(include_lifecycle and condition_id and condition_id in lifecycle_context_conditions)
        if not in_direct_lookback and not in_lifecycle_context:
            continue
        if not in_direct_lookback and in_lifecycle_context:
            context_expanded_event_count += 1
            events.append(event)
        else:
            events.append(event)
        source = str(event.get("source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
        action_counts[action] = action_counts.get(action, 0) + 1
    events.sort(key=lambda item: (int(num(item.get("event_ts"), 0.0)), str(item.get("transaction_hash") or "")))
    meta = {
        "status": "PASS" if events else "WATCH",
        "generated_at": utc_now_iso(),
        "generated_at_s": now_ts,
        "fingerprint": fingerprint,
        "event_count": len(events),
        "source_counts": source_counts,
        "action_counts": action_counts,
        "row_counts": row_counts,
        "lookback_s": effective_lookback_s,
        "max_rows_per_file": max_rows,
        "include_lifecycle": include_lifecycle,
        "lifecycle_context_schema_version": 1 if include_lifecycle else 0,
        "lifecycle_context_condition_count": len(lifecycle_context_conditions),
        "lifecycle_context_expanded_event_count": context_expanded_event_count,
    }
    state[cache_key] = {**meta, "events": events[-max(5000, min(len(events), 20000)) :]}
    return events, meta


def _historical_wallet_buy_events(
    state: dict[str, Any],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events, meta = _historical_wallet_events(
        state,
        config,
        now_ts=now_ts,
        cache_key="wallet_history_confirm_cache",
        refresh_s=float(config.wallet_history_confirm_refresh_s),
        lookback_s=float(config.wallet_history_confirm_lookback_s),
        max_rows_per_file=int(config.wallet_history_confirm_max_rows_per_file),
        include_lifecycle=False,
    )
    return [event for event in events if _wallet_lifecycle_action(event) == ""], meta


def _source_contract(
    tracker_payload: dict[str, Any],
    events: list[dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    fast_preconfirm_contract: dict[str, Any] | None = None,
    gamma_token_map_meta: dict[str, Any] | None = None,
    wallet_history_confirm_meta: dict[str, Any] | None = None,
    wallet_history_replay_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target_wallet = config.target_wallet.lower()
    target_mismatches = [
        str(event.get("target_wallet") or "").lower()
        for event in events
        if str(event.get("target_wallet") or "").lower() not in {"", target_wallet}
    ]
    ws_rows = [
        event.get("market_ws_corroboration")
        for event in events
        if isinstance(event.get("market_ws_corroboration"), dict)
    ]
    non_history_events = [
        event
        for event in events
        if not str(event.get("wallet_history_source") or event.get("source") or "").startswith("wallet_history_")
    ]
    ws_matched = sum(1 for row in ws_rows if row.get("matched") is True)
    blockers: list[str] = []
    watch_reasons: list[str] = []
    if tracker_payload.get("read_only") is not True:
        blockers.append("tracker_not_read_only")
    if tracker_payload.get("can_trade") is not False:
        blockers.append("tracker_can_trade_not_false")
    if target_mismatches:
        blockers.append("target_wallet_mismatch")
    if non_history_events and not ws_rows:
        watch_reasons.append("market_ws_corroboration_missing_on_events")
    elif non_history_events and ws_matched <= 0:
        watch_reasons.append("market_ws_corroboration_zero_matches")
    fast_preconfirm_contract = fast_preconfirm_contract or {}
    gamma_token_map_meta = gamma_token_map_meta or {}
    wallet_history_confirm_meta = wallet_history_confirm_meta or {}
    wallet_history_replay_meta = wallet_history_replay_meta or {}
    if config.enable_fast_preconfirm and fast_preconfirm_contract.get("status") not in {"PASS", "DISABLED"}:
        watch_reasons.append("fast_preconfirm_source_watch")
    if config.enable_gamma_token_map and gamma_token_map_meta.get("status") != "PASS":
        watch_reasons.append("gamma_active_token_map_watch")
    return {
        "status": "FAIL" if blockers else "WATCH" if watch_reasons else "PASS",
        "blockers": blockers,
        "watch_reasons": watch_reasons,
        "tracker_state_path": str(config.tracker_state_path),
        "tracker_generated_at": tracker_payload.get("generated_at"),
        "tracker_read_only": tracker_payload.get("read_only"),
        "tracker_can_trade": tracker_payload.get("can_trade"),
        "target_wallet": target_wallet,
        "recent_event_count": len(tracker_payload.get("recent_events") or []),
        "confirmed_wallet_buy_events_seen": len(events),
        "market_ws_corroborated_events": len(ws_rows),
        "market_ws_matched_events": ws_matched,
        "fast_preconfirm": fast_preconfirm_contract,
        "gamma_active_token_map": gamma_token_map_meta,
        "wallet_history_confirm": wallet_history_confirm_meta,
        "wallet_history_replay": wallet_history_replay_meta,
        "source_contract": "confirmed Polymarket wallet API/onchain rows are truth; market WS is latency-first preconfirm pending truth-loop validation",
    }


def _copy_contract(
    orders: list[dict[str, Any]],
    skip_records: list[dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    lifecycle_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    for order in orders:
        problems: list[str] = []
        if order.get("wallet_action") != "BUY" or order.get("action") != "BUY" or order.get("side") != "BUY":
            problems.append("buy_action_not_mirrored")
        if order.get("outcome") != order.get("wallet_outcome"):
            problems.append("outcome_not_mirrored")
        if order.get("token_id") != order.get("wallet_token_id"):
            problems.append("token_not_mirrored")
        if order.get("condition_id") != order.get("wallet_condition_id"):
            problems.append("condition_not_mirrored")
        if abs(num(order.get("limit_price")) - num(order.get("wallet_price"))) > MIRROR_PRICE_TOLERANCE:
            problems.append("price_not_mirrored")
        if order.get("paper_only") is not True or order.get("live_orders_allowed") is not False:
            problems.append("paper_safety_flags_invalid")
        if problems:
            violations.append({"paper_order_id": order.get("paper_order_id"), "problems": problems})
    skip_counts: dict[str, int] = {}
    for record in skip_records:
        reason = str(record.get("reason") or "unknown")
        skip_counts[reason] = skip_counts.get(reason, 0) + 1
    lifecycle_action_counts: dict[str, int] = {}
    for event in lifecycle_events or []:
        action = str(event.get("action") or "UNKNOWN")
        lifecycle_action_counts[action] = lifecycle_action_counts.get(action, 0) + 1
    fast_orders = [order for order in orders if _is_fast_preconfirm_order(order)]
    invalidated_fast = [order for order in orders if _is_invalidated_fast_preconfirm_order(order)]
    confirmed_fast = [order for order in fast_orders if order.get("confirmation_status") == "CONFIRMED"]
    pending_fast = [order for order in fast_orders if order.get("confirmation_status") == "PENDING_CONFIRMATION"]
    expired_fast = [order for order in fast_orders if order.get("confirmation_status") == "UNCONFIRMED_EXPIRED"]
    wallet_attribution_status_counts: dict[str, int] = {}
    for order in fast_orders:
        status_key = str(order.get("wallet_attribution_status") or "UNKNOWN")
        wallet_attribution_status_counts[status_key] = wallet_attribution_status_counts.get(status_key, 0) + 1
    wallet_attribution_confirmed = [
        order for order in fast_orders if order.get("wallet_attribution_confirmed") is True
    ]
    wallet_attribution_pending = [
        order for order in fast_orders if order.get("wallet_attribution_status") == "PENDING_WALLET_ATTRIBUTION"
    ]
    wallet_attribution_expired = [
        order for order in fast_orders if order.get("wallet_attribution_status") == "WALLET_ATTRIBUTION_EXPIRED"
    ]
    wallet_attribution_live_admissible = [
        order for order in fast_orders if order.get("wallet_attribution_live_admissible") is True
    ]
    confirmation_latencies = [
        num(order.get("confirmation_latency_s"))
        for order in confirmed_fast
        if order.get("confirmation_latency_s") is not None
    ]
    resolved_fast_count = len(confirmed_fast) + len(expired_fast)
    confirmation_rate_pct = round((len(confirmed_fast) / len(fast_orders) * 100.0) if fast_orders else 0.0, 6)
    resolved_confirmation_rate_pct = round(
        (len(confirmed_fast) / resolved_fast_count * 100.0) if resolved_fast_count else 0.0,
        6,
    )
    fast_validation_reasons: list[str] = []
    if expired_fast and not confirmed_fast:
        fast_validation_reasons.append("fast_preconfirm_expired_without_confirmations")
    elif (
        resolved_fast_count >= int(config.fast_confirm_min_resolved_for_rate_gate)
        and resolved_confirmation_rate_pct < float(config.fast_confirm_min_confirmation_rate_pct)
    ):
        fast_validation_reasons.append("fast_preconfirm_confirmation_rate_below_threshold")
    status = "FAIL" if violations else "WATCH" if fast_validation_reasons else "PASS"
    return {
        "status": status,
        "mode": "latency_first_confirmed_wallet_1_to_1_buy_copy_with_paper_lifecycle_ledger",
        "paper_only": True,
        "live_orders_allowed": False,
        "mirror_fields": [
            "wallet_action=BUY",
            "condition_id",
            "token_id",
            "outcome",
            "limit_price=wallet_price",
            "wallet_lifecycle_action in {MERGE,REDEEM,SELL}",
        ],
        "lifecycle_policy": {
            "enabled": True,
            "event_count": len(lifecycle_events or []),
            "action_counts": lifecycle_action_counts,
            "merge_policy": "realize paired Up/Down paper shares in the same proportion as wallet merge size when wallet pair inventory is known; otherwise merge available paired paper inventory",
            "redeem_policy": "claim residual winning inventory after BTC window resolution",
            "sell_policy": "future-proof proportional reduction of own paper inventory when wallet TRADE SELL rows appear",
            "live_orders_allowed": False,
        },
        "fast_preconfirm": {
            "orders": len(fast_orders),
            "confirmed": len(confirmed_fast),
            "pending": len(pending_fast),
            "expired_unconfirmed": len(expired_fast),
            "invalidated_by_truth_loop": len(invalidated_fast),
            "resolved_samples": resolved_fast_count,
            "confirmation_rate_pct": confirmation_rate_pct,
            "resolved_confirmation_rate_pct": resolved_confirmation_rate_pct,
            "validation_status": "WATCH" if fast_validation_reasons else "PASS",
            "validation_reasons": fast_validation_reasons,
            "min_resolved_for_rate_gate": int(config.fast_confirm_min_resolved_for_rate_gate),
            "min_confirmation_rate_pct": float(config.fast_confirm_min_confirmation_rate_pct),
            "confirmation_latency_s": {
                "min": round(min(confirmation_latencies), 6) if confirmation_latencies else None,
                "max": round(max(confirmation_latencies), 6) if confirmation_latencies else None,
                "avg": round(sum(confirmation_latencies) / len(confirmation_latencies), 6) if confirmation_latencies else None,
            },
            "wallet_attribution_required": bool(config.require_fast_preconfirm_wallet_attribution),
            "pending_wallet_attribution_preconfirm_enabled": bool(config.allow_pending_wallet_attribution_preconfirm),
            "wallet_attribution_status_counts": dict(sorted(wallet_attribution_status_counts.items())),
            "wallet_attribution_confirmed": len(wallet_attribution_confirmed),
            "wallet_attribution_pending": len(wallet_attribution_pending),
            "wallet_attribution_expired": len(wallet_attribution_expired),
            "live_admissible_confirmed_orders": len(wallet_attribution_live_admissible),
            "quarantined_unverified_orders": len(fast_orders) - len(wallet_attribution_live_admissible),
            "live_admissible_rule": "only fast preconfirm orders confirmed by target-wallet API/onchain truth-loop are admissible",
            "invalidated_rule": (
                "market-WS tx-hash preconfirm is invalidated, not quarantined, when the same tx hash is "
                "confirmed by wallet API on a different token/outcome/price"
            ),
        },
        "sizing_policy": {
            "type": _sizing_policy_type(config),
            "confirmed_wallet_window_order_cap": None,
            "confirmed_wallet_window_usd_cap": None,
            "confirmed_wallet_window_cap_applied": False,
            "order_usd": float(config.order_usd),
            "wallet_size_fraction": float(config.wallet_size_fraction),
            "max_order_usd": float(config.max_order_usd),
            "preconfirm_max_window_usd": float(config.max_window_usd),
            "preconfirm_max_orders_per_window": int(config.max_orders_per_window),
        },
        "mirror_violation_count": len(violations),
        "mirror_violations": violations[:20],
        "skip_counts": skip_counts,
        "require_market_ws_match_for_copy": bool(config.require_market_ws_match_for_copy),
    }


def _performance_stats(sample_orders: list[dict[str, Any]]) -> dict[str, Any]:
    resolved_orders = [order for order in sample_orders if isinstance(order.get("resolution"), dict)]
    open_orders = [order for order in sample_orders if not isinstance(order.get("resolution"), dict)]
    wins = sum(1 for order in resolved_orders if (order.get("resolution") or {}).get("won") is True)
    cost = sum(num(order.get("size_usd")) for order in resolved_orders)
    pnl = sum(num((order.get("resolution") or {}).get("pnl_usd")) for order in resolved_orders)
    return {
        "orders": len(sample_orders),
        "open_orders": len(open_orders),
        "resolved_orders": len(resolved_orders),
        "wins": wins,
        "losses": len(resolved_orders) - wins,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / cost * 100.0) if cost > 0 else 0.0, 6),
        "wr_pct": round((wins / len(resolved_orders) * 100.0) if resolved_orders else 0.0, 6),
        "latest_order_ts": max((str(order.get("ts") or "") for order in sample_orders), default=None),
        "latest_wallet_event_ts": max((int(num(order.get("wallet_trade_ts"), 0.0)) for order in sample_orders), default=0),
    }


def _lifecycle_summary(windows: dict[str, Any], lifecycle_events: list[dict[str, Any]]) -> dict[str, Any]:
    action_counts: dict[str, int] = {}
    for event in lifecycle_events:
        action = str(event.get("action") or "UNKNOWN")
        action_counts[action] = action_counts.get(action, 0) + 1
    models = [
        window.get("wallet_lifecycle")
        for window in windows.values()
        if isinstance(window, dict) and isinstance(window.get("wallet_lifecycle"), dict)
    ]
    resolved_models = [model for model in models if isinstance(model.get("resolved"), dict)]
    ignored_events = [
        dict(event, condition_id=str(condition_id))
        for condition_id, window in windows.items()
        if isinstance(window, dict) and isinstance(window.get("wallet_lifecycle"), dict)
        for event in (window.get("wallet_lifecycle", {}).get("ignored_events") or [])
        if isinstance(event, dict)
    ]
    ignored_count = len(ignored_events)
    ignored_reason_counts: dict[str, int] = {}
    for event in ignored_events:
        reason = str(event.get("reason") or "unknown")
        ignored_reason_counts[reason] = ignored_reason_counts.get(reason, 0) + 1
    realized_pnl = sum(num(model.get("realized_pnl_usd")) for model in models)
    resolved_pnl = sum(num((model.get("resolved") or {}).get("total_pnl_usd")) for model in resolved_models)
    resolved_cost = sum(num((model.get("resolved") or {}).get("total_cost_basis_usd")) for model in resolved_models)
    return {
        "mode": "paper_wallet_lifecycle_copy",
        "paper_only": True,
        "live_orders_allowed": False,
        "event_count": len(lifecycle_events),
        "action_counts": action_counts,
        "windows_with_lifecycle": sum(1 for model in models if num(model.get("lifecycle_event_count")) > 0),
        "windows_with_merge": sum(1 for model in models if (model.get("action_counts") or {}).get("MERGE", 0) > 0),
        "windows_with_redeem": sum(1 for model in models if (model.get("action_counts") or {}).get("REDEEM", 0) > 0),
        "windows_with_sell": sum(1 for model in models if (model.get("action_counts") or {}).get("SELL", 0) > 0),
        "ignored_lifecycle_event_count": ignored_count,
        "ignored_lifecycle_reason_counts": ignored_reason_counts,
        "ignored_lifecycle_event_samples": ignored_events[-20:],
        "realized_pnl_usd": round(realized_pnl, 6),
        "resolved_windows": len(resolved_models),
        "resolved_total_cost_basis_usd": round(resolved_cost, 6),
        "resolved_total_pnl_usd": round(resolved_pnl, 6),
        "resolved_roi_pct": round((resolved_pnl / resolved_cost * 100.0) if resolved_cost > 0 else 0.0, 6),
        "status": "WATCH" if ignored_count else "PASS",
        "watch_reasons": ["lifecycle_event_without_matching_paper_inventory"] if ignored_count else [],
    }


def _wallet_history_replay_coverage(
    replay_events: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
) -> dict[str, Any]:
    source_buy_by_key: dict[str, dict[str, Any]] = {}
    uncopyable_counts: dict[str, int] = {}
    price_excluded_keys: set[str] = set()
    duplicate_buy_rows = 0
    for event in replay_events:
        is_buy, reason = _is_confirmed_wallet_buy(event)
        if not is_buy:
            action = _wallet_lifecycle_action(event)
            if action not in {"MERGE", "REDEEM", "SELL"}:
                uncopyable_counts[reason] = uncopyable_counts.get(reason, 0) + 1
            continue
        key = event_canonical_key(event)
        if key in source_buy_by_key:
            duplicate_buy_rows += 1
            continue
        source_buy_by_key[key] = event
        price = num(event.get("price"))
        if price < float(config.min_price) or price > float(config.hard_price_cap):
            price_excluded_keys.add(key)

    copied_keys: set[str] = set()
    for order in orders:
        if order.get("candidate_family") == "btc_weird_peak_confirmed_exact_copy_v1":
            key = str(order.get("canonical_key") or "")
            if key:
                copied_keys.add(key)
        if _is_confirmed_fast_preconfirm_order(order):
            key = str(order.get("confirmed_wallet_canonical_key") or "")
            if key:
                copied_keys.add(key)

    copyable_keys = set(source_buy_by_key) - price_excluded_keys
    copied_copyable_keys = copyable_keys & copied_keys
    missing_keys = sorted(copyable_keys - copied_keys)
    status = "PASS" if copyable_keys and not missing_keys else "WATCH"
    if missing_keys:
        status = "FAIL"
    return {
        "status": status,
        "mode": "confirmed_wallet_history_buy_universe_to_paper_order_coverage",
        "paper_only": True,
        "live_orders_allowed": False,
        "source_unique_buy_event_count": len(source_buy_by_key),
        "source_duplicate_buy_row_count": duplicate_buy_rows,
        "copyable_buy_event_count": len(copyable_keys),
        "copied_buy_event_count": len(copied_copyable_keys),
        "missing_copyable_buy_event_count": len(missing_keys),
        "price_excluded_buy_event_count": len(price_excluded_keys),
        "copy_coverage_pct": round((len(copied_copyable_keys) / len(copyable_keys) * 100.0) if copyable_keys else 0.0, 6),
        "uncopyable_counts": dict(sorted(uncopyable_counts.items())),
        "sample_missing_copyable_keys": missing_keys[:20],
        "retained_order_count": len(orders),
        "retained_order_capacity": int(config.retain_orders),
        "assertion": "every copyable confirmed Weird-Peak wallet-history BUY must have exactly one paper copy order or confirmed preconfirm order",
    }


def _summary(
    orders: list[dict[str, Any]],
    windows: dict[str, Any],
    new_records: list[dict[str, Any]],
    lifecycle_events: list[dict[str, Any]],
) -> dict[str, Any]:
    all_paper_performance = _performance_stats(orders)
    direct_confirmed_wallet_copy_orders = [
        order
        for order in orders
        if order.get("candidate_family") == "btc_weird_peak_confirmed_exact_copy_v1"
        and not _is_confirmed_fast_preconfirm_order(order)
    ]
    confirmed_exact_orders = [
        order
        for order in orders
        if order.get("candidate_family") == "btc_weird_peak_confirmed_exact_copy_v1"
        or _is_confirmed_fast_preconfirm_order(order)
    ]
    fast_orders = [order for order in orders if _is_fast_preconfirm_order(order)]
    invalidated_fast = [order for order in orders if _is_invalidated_fast_preconfirm_order(order)]
    confirmed_fast = [order for order in fast_orders if order.get("confirmation_status") == "CONFIRMED"]
    pending_fast = [order for order in fast_orders if order.get("confirmation_status") == "PENDING_CONFIRMATION"]
    expired_fast = [order for order in fast_orders if order.get("confirmation_status") == "UNCONFIRMED_EXPIRED"]
    unverified_fast = [order for order in fast_orders if order.get("confirmation_status") != "CONFIRMED"]
    fast_wallet_attribution_status_counts: dict[str, int] = {}
    for order in fast_orders:
        status_key = str(order.get("wallet_attribution_status") or "UNKNOWN")
        fast_wallet_attribution_status_counts[status_key] = fast_wallet_attribution_status_counts.get(status_key, 0) + 1
    fast_wallet_attribution_confirmed = [
        order for order in fast_orders if order.get("wallet_attribution_confirmed") is True
    ]
    fast_wallet_attribution_pending = [
        order for order in fast_orders if order.get("wallet_attribution_status") == "PENDING_WALLET_ATTRIBUTION"
    ]
    fast_wallet_attribution_expired = [
        order for order in fast_orders if order.get("wallet_attribution_status") == "WALLET_ATTRIBUTION_EXPIRED"
    ]
    fast_wallet_attribution_live_admissible = [
        order for order in fast_orders if order.get("wallet_attribution_live_admissible") is True
    ]
    truth_confirmed_performance = _performance_stats(confirmed_exact_orders)
    direct_confirmed_performance = _performance_stats(direct_confirmed_wallet_copy_orders)
    fast_all_performance = _performance_stats(fast_orders)
    fast_confirmed_performance = _performance_stats(confirmed_fast)
    fast_unverified_performance = _performance_stats(unverified_fast)
    confirmation_latencies = [
        num(order.get("confirmation_latency_s"))
        for order in confirmed_fast
        if order.get("confirmation_latency_s") is not None
    ]
    skip_records = [row for row in new_records if row.get("event") == "weird_peak_exact_copy_skip"]
    new_copy_orders = [row for row in new_records if row.get("event") == "weird_peak_exact_copy_order"]
    new_fast_orders = [row for row in new_records if row.get("event") == "weird_peak_exact_fast_preconfirm_order"]
    new_confirmed_fast_orders = [
        row for row in new_records if row.get("event") == "weird_peak_exact_fast_preconfirm_confirmed"
    ]
    new_lifecycle_events = [row for row in new_records if row.get("event") == "weird_peak_exact_copy_lifecycle_event"]
    lifecycle = _lifecycle_summary(windows, lifecycle_events)
    confirmed_buy_orders_seen = len(new_copy_orders) + len(new_confirmed_fast_orders)
    wallet_buy_seen = confirmed_buy_orders_seen + len(
        [
            row
            for row in skip_records
            if row.get("reason")
            in {
                "price_outside_paper_caps",
                "confirmed_wallet_event_stale_for_paper",
                "market_ws_match_required_but_missing",
                "max_orders_per_window_reached",
                "max_window_usd_reached",
            }
        ]
    )
    copyable_seen = confirmed_buy_orders_seen + len(
        [
            row
            for row in skip_records
            if row.get("reason")
            in {
                "market_ws_match_required_but_missing",
                "max_orders_per_window_reached",
                "max_window_usd_reached",
            }
        ]
    )
    return {
        "new_records": len(new_records),
        "paper_orders": len(orders),
        "exact_copy_orders": len(confirmed_exact_orders),
        "confirmed_exact_copy_orders": len(confirmed_exact_orders),
        "direct_confirmed_wallet_copy_orders": len(direct_confirmed_wallet_copy_orders),
        "open_orders": all_paper_performance["open_orders"],
        "resolved_orders": all_paper_performance["resolved_orders"],
        "wins": all_paper_performance["wins"],
        "losses": all_paper_performance["losses"],
        "cost_usd": all_paper_performance["cost_usd"],
        "pnl_usd": all_paper_performance["pnl_usd"],
        "roi_pct": all_paper_performance["roi_pct"],
        "wr_pct": all_paper_performance["wr_pct"],
        "performance_basis": "all_paper_orders_mixed_not_live_admissible",
        "live_admissible_performance_basis": "truth_confirmed_wallet_copy",
        "mixed_performance_live_admissible": False,
        "all_paper_performance": all_paper_performance,
        "truth_confirmed_wallet_copy_performance": truth_confirmed_performance,
        "direct_confirmed_wallet_copy_performance": direct_confirmed_performance,
        "fast_preconfirm_candidate_performance": fast_all_performance,
        "fast_preconfirm_confirmed_performance": fast_confirmed_performance,
        "fast_preconfirm_unverified_performance": fast_unverified_performance,
        "truth_confirmed_wallet_copy_resolved_orders": truth_confirmed_performance["resolved_orders"],
        "truth_confirmed_wallet_copy_pnl_usd": truth_confirmed_performance["pnl_usd"],
        "truth_confirmed_wallet_copy_roi_pct": truth_confirmed_performance["roi_pct"],
        "truth_confirmed_wallet_copy_wr_pct": truth_confirmed_performance["wr_pct"],
        "window_count": len(windows),
        "both_sided_window_count": sum(
            1
            for window in windows.values()
            if isinstance(window, dict) and (window.get("inventory") or {}).get("both_sides") is True
        ),
        "new_copy_orders": len(new_copy_orders),
        "new_fast_preconfirm_orders": len(new_fast_orders),
        "fast_preconfirm_orders": len(fast_orders),
        "fast_preconfirm_confirmed": len(confirmed_fast),
        "fast_preconfirm_pending": len(pending_fast),
        "fast_preconfirm_expired_unconfirmed": len(expired_fast),
        "fast_preconfirm_invalidated_by_truth_loop": len(invalidated_fast),
        "fast_preconfirm_wallet_attribution_status_counts": dict(sorted(fast_wallet_attribution_status_counts.items())),
        "fast_preconfirm_wallet_attribution_confirmed": len(fast_wallet_attribution_confirmed),
        "fast_preconfirm_wallet_attribution_pending": len(fast_wallet_attribution_pending),
        "fast_preconfirm_wallet_attribution_expired": len(fast_wallet_attribution_expired),
        "fast_preconfirm_live_admissible_confirmed": len(fast_wallet_attribution_live_admissible),
        "fast_preconfirm_quarantined_unverified": len(fast_orders) - len(fast_wallet_attribution_live_admissible),
        "fast_preconfirm_confirmation_rate_pct": round((len(confirmed_fast) / len(fast_orders) * 100.0) if fast_orders else 0.0, 6),
        "fast_preconfirm_confirmation_latency_s": {
            "min": round(min(confirmation_latencies), 6) if confirmation_latencies else None,
            "max": round(max(confirmation_latencies), 6) if confirmation_latencies else None,
            "avg": round(sum(confirmation_latencies) / len(confirmation_latencies), 6) if confirmation_latencies else None,
        },
        "wallet_lifecycle": lifecycle,
        "new_wallet_lifecycle_events": len(new_lifecycle_events),
        "new_skip_records": len(skip_records),
        "wallet_buy_events_seen_in_poll": wallet_buy_seen,
        "copyable_wallet_buy_events_in_poll": copyable_seen,
        "copy_ratio_pct_in_poll": round((confirmed_buy_orders_seen / copyable_seen * 100.0) if copyable_seen else 0.0, 6),
        "latest_copy_ts": all_paper_performance["latest_order_ts"],
        "latest_wallet_event_ts": all_paper_performance["latest_wallet_event_ts"],
    }


def _process_confirmed_wallet_event(
    event: dict[str, Any],
    *,
    processed: set[str],
    state: dict[str, Any],
    orders_by_id: dict[str, dict[str, Any]],
    lifecycle_events_by_id: dict[str, dict[str, Any]],
    config: WeirdPeakExactCopyConfig,
    now_ts: float,
    new_records: list[dict[str, Any]],
    eligible_confirmed_buy_events: list[dict[str, Any]],
    max_age_s: float,
    allow_preconfirm_match: bool,
    wallet_attribution_status: str,
    wallet_attribution_mode: str,
    wallet_attribution_confirmation_basis: str,
) -> str:
    key = event_canonical_key(event)
    already_processed = key in processed
    processed.add(key)

    lifecycle_action = _wallet_lifecycle_action(event)
    if lifecycle_action:
        condition_id = str(event.get("condition_id") or "")
        if not condition_id:
            if not already_processed:
                new_records.append(_skip_record("missing_condition_id_for_lifecycle_event", event, canonical_key=key, now_ts=now_ts))
            return "skipped_lifecycle_missing_condition"
        record = _lifecycle_record(lifecycle_action, event, canonical_key=key, now_ts=now_ts)
        event_id = str(record["paper_lifecycle_event_id"])
        if event_id in lifecycle_events_by_id:
            return "deduped_existing_lifecycle"
        lifecycle_events_by_id[event_id] = record
        window = _window(state, condition_id, event)
        event_ids = [str(item) for item in window.get("wallet_lifecycle_event_ids") or []]
        if event_id not in event_ids:
            event_ids.append(event_id)
            window["wallet_lifecycle_event_ids"] = event_ids
        window["latest_wallet_event_ts"] = event.get("event_ts")
        new_records.append(record)
        return "lifecycle_recorded"

    is_buy, reason = _is_confirmed_wallet_buy(event)
    if not is_buy:
        if not already_processed:
            new_records.append(_skip_record(reason, event, canonical_key=key, now_ts=now_ts))
        return f"skipped_{reason}"
    if already_processed and _existing_order_for_canonical_key(orders_by_id, key) is not None:
        return "deduped_existing_order"
    eligible_confirmed_buy_events.append(event)

    condition_id = str(event.get("condition_id") or "")
    outcome = str(event.get("outcome") or "")
    price = num(event.get("price"))
    age = _event_age_s(event, now_ts)
    ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
    if price < config.min_price or price > config.hard_price_cap:
        if not already_processed:
            new_records.append(_skip_record("price_outside_paper_caps", event, canonical_key=key, now_ts=now_ts))
        return "skipped_price_outside_paper_caps"
    if age is None or (max_age_s > 0 and age > max_age_s):
        if not already_processed:
            new_records.append(_skip_record("confirmed_wallet_event_stale_for_paper", event, canonical_key=key, now_ts=now_ts))
        return "skipped_stale"
    if config.require_market_ws_match_for_copy and ws.get("matched") is not True:
        if not already_processed:
            new_records.append(_skip_record("market_ws_match_required_but_missing", event, canonical_key=key, now_ts=now_ts))
        return "skipped_market_ws_match_required"

    if allow_preconfirm_match:
        preconfirmed_order = _find_matching_preconfirm_order(orders_by_id, event, config)
        if preconfirmed_order is not None:
            new_records.append(_confirm_preconfirm_order(preconfirmed_order, event, canonical_key=key, now_ts=now_ts))
            return "preconfirm_confirmed"

    order_id = exact_copy_order_id(key)
    if order_id in orders_by_id:
        return "deduped_existing_order"

    window = _window(state, condition_id, event)
    size_usd = _copy_size_usd(event, config, float("inf"))
    if size_usd <= 0:
        if not already_processed:
            new_records.append(_skip_record("order_size_zero", event, canonical_key=key, now_ts=now_ts))
        return "skipped_order_size_zero"

    shares = round(size_usd / price, 6)
    order = {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_exact_copy_order",
        "paper_order_id": order_id,
        "canonical_key": key,
        "candidate_family": "btc_weird_peak_confirmed_exact_copy_v1",
        "inventory_family": "btc_weird_peak_exact_copy_inventory_v1",
        "wallet_attribution_required": True,
        "wallet_attribution_status": wallet_attribution_status,
        "wallet_attribution_mode": wallet_attribution_mode,
        "wallet_attribution_confirmed": True,
        "wallet_attribution_confirmed_at": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "wallet_attribution_live_admissible": True,
        "wallet_attribution_confirmation_basis": wallet_attribution_confirmation_basis,
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "target_wallet": config.target_wallet.lower(),
        "wallet_condition_id": condition_id,
        "condition_id": condition_id,
        "market_id": event.get("market_id") or event.get("clob_market_id"),
        "market_slug": event.get("market_slug"),
        "window_start_s": event.get("window_start_s"),
        "wallet_trade_ts": event.get("event_ts"),
        "wallet_observed_ts": event.get("observed_ts"),
        "api_latency_s": event.get("api_latency_s"),
        "market_ws_corroboration": ws or None,
        "market_ws_matched": bool(ws.get("matched")),
        "ws_to_api_observed_latency_s": ws.get("ws_to_api_observed_latency_s"),
        "event_age_s": round(age, 6) if age is not None else None,
        "transaction_hash": event.get("transaction_hash"),
        "wallet_token_id": event.get("token_id"),
        "token_id": event.get("token_id"),
        "wallet_action": "BUY",
        "action": "BUY",
        "side": "BUY",
        "wallet_outcome": outcome,
        "outcome": outcome,
        "position_side": outcome,
        "wallet_price": price,
        "limit_price": price,
        "wallet_size": num(event.get("size")),
        "wallet_usdc_size": round(num(event.get("usdc_size")), 6),
        "size_usd": size_usd,
        "shares": shares,
        "sizing_policy": {
            "type": _sizing_policy_type(config),
            "order_usd": float(config.order_usd),
            "wallet_size_fraction": float(config.wallet_size_fraction),
            "max_order_usd": float(config.max_order_usd),
            "confirmed_wallet_window_cap_applied": False,
            "confirmed_wallet_window_order_cap": None,
            "confirmed_wallet_window_usd_cap": None,
        },
        "order_type": "PAPER_ONLY_EXACT_WALLET_COPY",
        "reason": "confirmed Weird-Peak wallet BUY trade copied 1:1 in paper with wallet-size sizing",
    }
    orders_by_id[order_id] = order
    window["paper_order_count"] = int(window.get("paper_order_count") or 0) + 1
    window["copy_order_count"] = int(window.get("copy_order_count") or 0) + 1
    window["cost_usd"] = round(num(window.get("cost_usd")) + size_usd, 6)
    window["latest_wallet_event_ts"] = event.get("event_ts")
    window["latest_copy_ts"] = order["ts"]
    order_ids = [str(item) for item in window.get("orders") or []]
    if order_id not in order_ids:
        order_ids.append(order_id)
        window["orders"] = order_ids
    window["inventory"] = _inventory_from_orders([orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id])
    new_records.append(order)
    return "copy_order_created"


def run_exact_copy_once(
    tracker_payload: dict[str, Any],
    config: WeirdPeakExactCopyConfig,
    *,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now = float(now_ts if now_ts is not None else time.time())
    state_path = Path(config.state_path)
    state = load_json(state_path) or _initial_state(config)
    processed = set(str(key) for key in state.get("processed_trade_keys") or [])
    processed_signals = set(str(key) for key in state.get("processed_signal_keys") or [])
    orders = [order for order in state.get("paper_orders") or [] if isinstance(order, dict)]
    orders_by_id = {str(order.get("paper_order_id")): order for order in orders if order.get("paper_order_id")}
    lifecycle_events = [row for row in state.get("wallet_lifecycle_events") or [] if isinstance(row, dict)]
    lifecycle_events_by_id = {
        str(row.get("paper_lifecycle_event_id")): row
        for row in lifecycle_events
        if row.get("paper_lifecycle_event_id")
    }
    new_records: list[dict[str, Any]] = []
    eligible_confirmed_buy_events: list[dict[str, Any]] = []
    tracker_state = tracker_payload if tracker_payload.get("recent_events") else load_json(Path(config.tracker_state_path))
    confirmed_state = load_json(Path(config.confirmed_paper_state_path))
    gamma_token_map, gamma_token_map_meta = _active_btc_gamma_token_map(state, config, now_ts=now)
    token_map = build_token_outcome_map(state=state, tracker_state=tracker_state, confirmed_state=confirmed_state)
    token_map = {**gamma_token_map, **token_map}
    state["token_outcome_map"] = token_map
    wallet_attributed_tx_hashes = _wallet_attributed_tx_hashes(tracker_state)
    wallet_history_confirm_meta: dict[str, Any] = {}
    fast_preconfirm_contract = _process_fast_preconfirm_signals(
        state,
        orders_by_id,
        processed_signals,
        token_map,
        wallet_attributed_tx_hashes,
        config,
        now_ts=now,
        new_records=new_records,
    )

    recent_events = [row for row in tracker_payload.get("recent_events") or [] if isinstance(row, dict)]
    recent_events.sort(key=lambda item: (int(num(item.get("event_ts"), 0.0)), num(item.get("observed_ts"))))
    market_ws_preconfirm_invalidation_meta = _invalidate_mismatched_market_ws_preconfirm_orders(
        orders_by_id,
        recent_events,
        config,
        now_ts=now,
        new_records=new_records,
    )
    wallet_api_preconfirm_contract = _process_wallet_api_preconfirm_events(
        state,
        orders_by_id,
        processed_signals,
        recent_events,
        config,
        now_ts=now,
        new_records=new_records,
    )
    fast_preconfirm_contract = {
        **fast_preconfirm_contract,
        "market_ws_status": fast_preconfirm_contract.get("status"),
        "wallet_api_preconfirm": wallet_api_preconfirm_contract,
        "wallet_api_preconfirm_created": wallet_api_preconfirm_contract.get("created", 0),
        "market_ws_preconfirm_invalidation": market_ws_preconfirm_invalidation_meta,
        "market_ws_preconfirm_invalidated": market_ws_preconfirm_invalidation_meta.get("invalidated", 0),
        "status": (
            "PASS"
            if wallet_api_preconfirm_contract.get("status") == "PASS"
            or market_ws_preconfirm_invalidation_meta.get("invalidated", 0) > 0
            else fast_preconfirm_contract.get("status")
        ),
    }
    for event in recent_events:
        _process_confirmed_wallet_event(
            event,
            processed=processed,
            state=state,
            orders_by_id=orders_by_id,
            lifecycle_events_by_id=lifecycle_events_by_id,
            config=config,
            now_ts=now,
            new_records=new_records,
            eligible_confirmed_buy_events=eligible_confirmed_buy_events,
            max_age_s=float(config.max_confirmed_age_s),
            allow_preconfirm_match=True,
            wallet_attribution_status="CONFIRMED_BY_WALLET_API",
            wallet_attribution_mode="confirmed_wallet_api",
            wallet_attribution_confirmation_basis="confirmed Weird-Peak wallet API/onchain BUY row",
        )

    wallet_history_replay_meta: dict[str, Any] = {"enabled": bool(config.enable_wallet_history_replay), "status": "DISABLED"}
    wallet_history_replay_events_for_coverage: list[dict[str, Any]] = []
    if config.enable_wallet_history_replay:
        historical_replay_events, wallet_history_replay_meta = _historical_wallet_events(
            state,
            config,
            now_ts=now,
            cache_key="wallet_history_replay_cache",
            refresh_s=float(config.wallet_history_confirm_refresh_s),
            lookback_s=float(config.wallet_history_replay_lookback_s),
            max_rows_per_file=int(config.wallet_history_replay_max_rows_per_file),
            include_lifecycle=True,
        )
        cached_replay_coverage = (
            wallet_history_replay_meta.get("coverage")
            if wallet_history_replay_meta.get("status") == "CACHED"
            and isinstance(wallet_history_replay_meta.get("coverage"), dict)
            else None
        )
        wallet_history_replay_events_for_coverage = [] if cached_replay_coverage else historical_replay_events
        replay_status_counts: dict[str, int] = {}
        max_replay_events = max(0, int(config.wallet_history_replay_max_events_per_poll))
        replay_events = [] if cached_replay_coverage else historical_replay_events[-max_replay_events:] if max_replay_events else []
        for event in replay_events:
            status = _process_confirmed_wallet_event(
                event,
                processed=processed,
                state=state,
                orders_by_id=orders_by_id,
                lifecycle_events_by_id=lifecycle_events_by_id,
                config=config,
                now_ts=now,
                new_records=new_records,
                eligible_confirmed_buy_events=eligible_confirmed_buy_events,
                max_age_s=0.0,
                allow_preconfirm_match=True,
                wallet_attribution_status="CONFIRMED_BY_WALLET_HISTORY",
                wallet_attribution_mode="confirmed_wallet_history_replay",
                wallet_attribution_confirmation_basis="confirmed Weird-Peak wallet history BUY row replayed into paper",
            )
            replay_status_counts[status] = replay_status_counts.get(status, 0) + 1
        wallet_history_replay_meta = {
            **wallet_history_replay_meta,
            "enabled": True,
            "mode": "confirmed_wallet_history_replay_to_paper_copy_and_lifecycle",
            "processed_this_poll": len(replay_events),
            "status_counts": dict(sorted(replay_status_counts.items())),
            "created_copy_orders": replay_status_counts.get("copy_order_created", 0),
            "recorded_lifecycle_events": replay_status_counts.get("lifecycle_recorded", 0),
            "deduped_events": sum(
                count for status, count in replay_status_counts.items() if status.startswith("deduped")
            ),
            "max_events_per_poll": max_replay_events,
            "cache_hit_skipped_replay": bool(cached_replay_coverage),
        }
        if replay_status_counts.get("preconfirm_confirmed", 0) > 0:
            wallet_history_confirm_meta = {
                **wallet_history_replay_meta,
                "confirmed_preconfirm_orders": replay_status_counts.get("preconfirm_confirmed", 0),
                "mode": "post_hoc_target_wallet_history_truth_loop_via_replay",
            }

    if any(
        _is_pending_fast_preconfirm_order(order)
        and order.get("confirmation_status") != "CONFIRMED"
        for order in _candidate_orders(orders_by_id)
    ):
        historical_events, wallet_history_confirm_meta = _historical_wallet_buy_events(state, config, now_ts=now)
        historical_confirmed = 0
        for event in historical_events:
            preconfirmed_order = _find_matching_preconfirm_order(orders_by_id, event, config)
            if preconfirmed_order is None:
                continue
            key = event_canonical_key(event)
            processed.add(key)
            new_records.append(_confirm_preconfirm_order(preconfirmed_order, event, canonical_key=key, now_ts=now))
            historical_confirmed += 1
        wallet_history_confirm_meta = {
            **wallet_history_confirm_meta,
            "confirmed_preconfirm_orders": historical_confirmed,
            "mode": "post_hoc_target_wallet_history_truth_loop",
        }

    wallet_api_preconfirm_migration_meta = _promote_existing_wallet_api_copies_to_fast_confirmed(
        orders_by_id,
        config,
        now_ts=now,
        new_records=new_records,
    )
    fast_preconfirm_contract = {
        **fast_preconfirm_contract,
        "wallet_api_preconfirm_migration": wallet_api_preconfirm_migration_meta,
        "wallet_api_preconfirm_migrated": wallet_api_preconfirm_migration_meta.get("migrated", 0),
        "status": (
            "PASS"
            if wallet_api_preconfirm_contract.get("status") == "PASS"
            or wallet_api_preconfirm_migration_meta.get("status") == "PASS"
            else fast_preconfirm_contract.get("status")
        ),
    }

    _mark_expired_preconfirm_orders(orders_by_id, config, now_ts=now, new_records=new_records)

    resolutions = load_resolutions(Path(config.resolutions_path))
    resolved_ids = set(str(item) for item in state.get("resolved_order_ids") or [])
    for order_id, order in list(orders_by_id.items()):
        if order_id in resolved_ids:
            continue
        if _is_invalidated_fast_preconfirm_order(order):
            continue
        resolution = resolution_for_order(resolutions, order)
        if not resolution:
            continue
        order["resolution"] = _resolve_order(order, resolution)
        resolved_ids.add(order_id)
        new_records.append(
            {
                "ts": utc_now_iso(),
                "event": "weird_peak_exact_copy_resolve",
                "paper_order_id": order_id,
                "candidate_family": order.get("candidate_family"),
                "read_only": True,
                "paper_only": True,
                "can_trade": False,
                "live_orders_allowed": False,
                "condition_id": order.get("condition_id"),
                "outcome": order.get("outcome"),
                "resolution": order.get("resolution"),
            }
        )

    processed_list = list(processed)[-max(1, int(config.retain_processed_keys)) :]
    processed_signal_list = list(processed_signals)[-max(1, int(config.retain_processed_signal_keys)) :]
    lifecycle_events = sorted(list(lifecycle_events_by_id.values()), key=lambda row: str(row.get("ts") or ""))[
        -max(1, int(config.retain_processed_keys)) :
    ]
    lifecycle_events_by_id = {
        str(row.get("paper_lifecycle_event_id")): row
        for row in lifecycle_events
        if row.get("paper_lifecycle_event_id")
    }
    orders = sorted(list(orders_by_id.values()), key=lambda row: str(row.get("ts") or ""))[
        -max(1, int(config.retain_orders)) :
    ]
    orders_by_id = {str(order.get("paper_order_id")): order for order in orders if order.get("paper_order_id")}
    windows = state.get("windows") if isinstance(state.get("windows"), dict) else {}
    for condition_id, window in windows.items():
        if isinstance(window, dict):
            active_order_ids = [
                str(oid)
                for oid in window.get("orders", [])
                if str(oid) in orders_by_id and not _is_invalidated_fast_preconfirm_order(orders_by_id[str(oid)])
            ]
            window_orders = [
                orders_by_id[oid]
                for oid in active_order_ids
                if oid in orders_by_id
            ]
            window["paper_order_count"] = len(window_orders)
            window["copy_order_count"] = sum(
                1
                for order in window_orders
                if order.get("candidate_family") in {
                    "btc_weird_peak_confirmed_exact_copy_v1",
                    "btc_weird_peak_latency_first_exact_copy_v1",
                }
            )
            window["cost_usd"] = round(sum(num(order.get("size_usd")) for order in window_orders), 6)
            window["inventory"] = _inventory_from_orders(window_orders)
            active_window = {**window, "orders": active_order_ids}
            _update_window_resolution(active_window, orders_by_id)
            window["resolved"] = active_window.get("resolved")
            window["wallet_lifecycle"] = _window_lifecycle_model(window, orders_by_id, lifecycle_events_by_id)
            window["condition_id"] = condition_id

    skip_records = [row for row in new_records if row.get("event") == "weird_peak_exact_copy_skip"]
    summary = _summary(orders, windows, new_records, lifecycle_events)
    if config.enable_wallet_history_replay:
        replay_coverage = (
            wallet_history_replay_meta.get("coverage")
            if wallet_history_replay_meta.get("cache_hit_skipped_replay")
            and isinstance(wallet_history_replay_meta.get("coverage"), dict)
            else _wallet_history_replay_coverage(
                wallet_history_replay_events_for_coverage,
                orders,
                config,
            )
        )
        wallet_history_replay_meta = {
            **wallet_history_replay_meta,
            "coverage": replay_coverage,
        }
        replay_cache = state.get("wallet_history_replay_cache")
        if isinstance(replay_cache, dict):
            replay_cache["coverage"] = replay_coverage
    summary["wallet_history_replay"] = wallet_history_replay_meta
    state.update(
        {
            "schema_version": 1,
            "kind": "btc_weird_peak_exact_copy_paper_flow",
            "generated_at": utc_now_iso(),
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "target_wallet": config.target_wallet.lower(),
            "strategy_families": [
                "btc_weird_peak_confirmed_exact_copy_v1",
                "btc_weird_peak_latency_first_exact_copy_v1",
            ],
            "runtime_config": asdict(config),
            "processed_trade_keys": processed_list,
            "processed_signal_keys": processed_signal_list,
            "token_outcome_map": token_map,
            "paper_orders": orders,
            "wallet_lifecycle_events": lifecycle_events,
            "resolved_order_ids": sorted(resolved_ids)[-max(1, int(config.retain_orders)) :],
            "source_contract": _source_contract(
                tracker_payload,
                eligible_confirmed_buy_events,
                config,
                fast_preconfirm_contract,
                gamma_token_map_meta,
                wallet_history_confirm_meta,
                wallet_history_replay_meta,
            ),
            "copy_contract": _copy_contract(orders, skip_records, config, lifecycle_events),
            "summary": summary,
            "latest_records": new_records[-50:],
        }
    )
    append_jsonl(Path(config.event_log_path), new_records)
    atomic_write_json(state_path, state)
    return state
