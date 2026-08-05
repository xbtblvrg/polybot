"""Paper-only fast-inferred Weird-Peak market-WS inventory flow.

This layer does not claim wallet confirmation. It turns fresh public market-WS
price-change events into a low-latency paper inventory hypothesis and keeps the
confirmed Weird-Peak tracker as the truth/audit layer.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.weird_peak_paper_flow import (
    _inventory_from_orders,
    _resolve_order,
    load_json,
    load_resolutions,
    num,
    resolution_for_order,
)
from src.weird_peak_wallet_tracker import (
    MarketWsEventLogBuffer,
    _row_ts_s,
    _ws_items,
    append_jsonl,
    atomic_write_json,
)


UTC = timezone.utc


@dataclass(frozen=True)
class WeirdPeakFastInferredConfig:
    state_path: str = "data/research/weird_peak_fast_inferred_paper_flow_state.json"
    event_log_path: str = "data/research/weird_peak_fast_inferred_paper_flow_events.jsonl"
    raw_pm_events_path: str = "data/lead_lag_raw_pm_events.jsonl"
    tracker_state_path: str = "data/research/weird_peak_wallet_tracker_state.json"
    confirmed_paper_state_path: str = "data/research/weird_peak_paper_flow_state.json"
    resolutions_path: str = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
    order_usd: float = 1.0
    max_orders_per_window: int = 12
    max_window_usd: float = 12.0
    min_price: float = 0.01
    hard_price_cap: float = 0.98
    min_ws_size: float = 1.0
    lookback_s: float = 45.0
    max_signals_per_poll: int = 60
    max_skip_records_per_poll: int = 80
    retain_processed_keys: int = 50_000
    retain_orders: int = 10_000
    market_ws_tail_lines: int = 150_000
    market_ws_tail_max_bytes: int = 128 * 1024 * 1024


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _initial_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "btc_weird_peak_fast_inferred_paper_flow",
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "strategy_families": [
            "btc_weird_peak_fast_inferred_ws_inventory_v1",
            "btc_weird_peak_hybrid_confirmed_fast_inventory_v1",
        ],
        "processed_signal_keys": [],
        "paper_orders": [],
        "resolved_order_ids": [],
        "windows": {},
        "token_outcome_map": {},
    }


def _signal_key(item: dict[str, Any], row_ts: float | None) -> str:
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    parts = [
        str(item.get("market") or ""),
        str(item.get("token_id") or ""),
        f"{num(item.get('price')):.10f}",
        f"{num(item.get('size')):.10f}",
        str(item.get("book_hash") or ""),
        str(raw.get("timestamp") or ""),
        f"{float(row_ts or 0.0):.6f}",
    ]
    return "|".join(parts)


def _paper_order_id(signal_key: str) -> str:
    return hashlib.sha256(f"weird_peak_fast_inferred|{signal_key}".encode()).hexdigest()[:24]


def _side_from_outcome(outcome: str) -> str:
    if outcome == "Up":
        return "YES"
    if outcome == "Down":
        return "NO"
    return ""


def _token_record(
    *,
    condition_id: str,
    token_id: str,
    outcome: str,
    market_slug: Any = None,
    window_start_s: Any = None,
    title: Any = None,
) -> dict[str, Any]:
    return {
        "condition_id": condition_id,
        "token_id": token_id,
        "outcome": outcome,
        "side": _side_from_outcome(outcome),
        "market_slug": market_slug,
        "window_start_s": window_start_s,
        "title": title,
        "source": "confirmed_weird_peak_truth_cache",
    }


def build_token_outcome_map(*, state: dict[str, Any], tracker_state: dict[str, Any], confirmed_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    token_map: dict[str, dict[str, Any]] = {}
    for token_id, row in (state.get("token_outcome_map") or {}).items():
        if isinstance(row, dict) and token_id:
            token_map[str(token_id)] = dict(row)

    for event in tracker_state.get("recent_events") or []:
        if not isinstance(event, dict):
            continue
        token_id = str(event.get("token_id") or "")
        outcome = str(event.get("outcome") or "")
        condition_id = str(event.get("condition_id") or "")
        if token_id and outcome in {"Up", "Down"} and condition_id:
            token_map[token_id] = _token_record(
                condition_id=condition_id,
                token_id=token_id,
                outcome=outcome,
                market_slug=event.get("market_slug"),
                window_start_s=event.get("window_start_s"),
                title=event.get("title"),
            )

    for order in confirmed_state.get("paper_orders") or []:
        if not isinstance(order, dict):
            continue
        token_id = str(order.get("token_id") or "")
        outcome = str(order.get("outcome") or "")
        condition_id = str(order.get("condition_id") or "")
        if token_id and outcome in {"Up", "Down"} and condition_id:
            existing = token_map.get(token_id, {})
            token_map[token_id] = {
                **_token_record(
                    condition_id=condition_id,
                    token_id=token_id,
                    outcome=outcome,
                    market_slug=order.get("market_slug"),
                    window_start_s=order.get("window_start_s"),
                ),
                **{k: v for k, v in existing.items() if v is not None},
            }
    return token_map


def _window(state: dict[str, Any], *, condition_id: str, token_meta: dict[str, Any]) -> dict[str, Any]:
    windows = state.setdefault("windows", {})
    window = windows.setdefault(
        condition_id,
        {
            "condition_id": condition_id,
            "market_slug": token_meta.get("market_slug"),
            "title": token_meta.get("title"),
            "window_start_s": token_meta.get("window_start_s"),
            "paper_order_count": 0,
            "cost_usd": 0.0,
            "orders": [],
            "inventory": {},
            "resolved": None,
        },
    )
    window["market_slug"] = window.get("market_slug") or token_meta.get("market_slug")
    window["title"] = window.get("title") or token_meta.get("title")
    window["window_start_s"] = window.get("window_start_s") or token_meta.get("window_start_s")
    return window


def _skip_record(reason: str, item: dict[str, Any], *, signal_key: str, row_ts: float | None, now_ts: float) -> dict[str, Any]:
    return {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_fast_inferred_skip",
        "reason": reason,
        "candidate_family": "btc_weird_peak_fast_inferred_ws_inventory_v1",
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "signal_key": signal_key,
        "condition_id": str(item.get("market") or ""),
        "token_id": str(item.get("token_id") or ""),
        "ws_event_type": item.get("event_type"),
        "ws_side": item.get("side"),
        "ws_price": num(item.get("price")),
        "ws_size": num(item.get("size")),
        "ws_recv_ts": row_ts,
        "signal_age_s": round(max(0.0, now_ts - row_ts), 6) if row_ts is not None else None,
    }


def _resolve_open_orders(state: dict[str, Any], config: WeirdPeakFastInferredConfig, new_records: list[dict[str, Any]]) -> None:
    resolutions = load_resolutions(Path(config.resolutions_path))
    resolved_ids = set(str(item) for item in state.get("resolved_order_ids") or [])
    orders = [order for order in state.get("paper_orders") or [] if isinstance(order, dict)]
    for order in orders:
        order_id = str(order.get("paper_order_id") or "")
        if not order_id or order_id in resolved_ids:
            continue
        resolution = resolution_for_order(resolutions, order)
        if not resolution:
            continue
        order["resolution"] = _resolve_order(order, resolution)
        resolved_ids.add(order_id)
        new_records.append({
            "ts": utc_now_iso(),
            "event": "weird_peak_fast_inferred_resolve",
            "paper_order_id": order_id,
            "candidate_family": order.get("candidate_family"),
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "condition_id": order.get("condition_id"),
            "outcome": order.get("outcome"),
            "resolution": order.get("resolution"),
        })
    state["paper_orders"] = orders
    state["resolved_order_ids"] = sorted(resolved_ids)[-max(1, int(config.retain_orders)) :]


def _summarize(state: dict[str, Any], *, new_records: int, skip_counts: dict[str, int], buffer_stats: dict[str, Any]) -> dict[str, Any]:
    orders = [order for order in state.get("paper_orders") or [] if isinstance(order, dict)]
    resolved = [order for order in orders if isinstance(order.get("resolution"), dict)]
    open_orders = [order for order in orders if not isinstance(order.get("resolution"), dict)]
    wins = sum(1 for order in resolved if (order.get("resolution") or {}).get("won") is True)
    cost = sum(num(order.get("size_usd")) for order in resolved)
    pnl = sum(num((order.get("resolution") or {}).get("pnl_usd")) for order in resolved)
    windows = state.get("windows") if isinstance(state.get("windows"), dict) else {}
    return {
        "new_records": int(new_records),
        "paper_orders": len(orders),
        "open_orders": len(open_orders),
        "resolved_orders": len(resolved),
        "wins": wins,
        "losses": len(resolved) - wins,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / cost * 100.0) if cost > 0 else 0.0, 6),
        "wr_pct": round((wins / len(resolved) * 100.0) if resolved else 0.0, 6),
        "window_count": len(windows),
        "both_sided_window_count": sum(
            1
            for window in windows.values()
            if isinstance(window, dict) and (window.get("inventory") or {}).get("both_sides") is True
        ),
        "skip_counts": dict(sorted(skip_counts.items())),
        "market_ws_buffer": buffer_stats,
    }


def run_fast_inferred_once(
    config: WeirdPeakFastInferredConfig,
    *,
    buffer: MarketWsEventLogBuffer | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now = float(now_ts if now_ts is not None else time.time())
    state_path = Path(config.state_path)
    state = load_json(state_path) or _initial_state()
    tracker_state = load_json(Path(config.tracker_state_path))
    confirmed_state = load_json(Path(config.confirmed_paper_state_path))
    token_map = build_token_outcome_map(state=state, tracker_state=tracker_state, confirmed_state=confirmed_state)
    state["token_outcome_map"] = token_map
    processed = set(str(key) for key in state.get("processed_signal_keys") or [])
    orders = [order for order in state.get("paper_orders") or [] if isinstance(order, dict)]
    orders_by_id = {str(order.get("paper_order_id")): order for order in orders if order.get("paper_order_id")}
    new_records: list[dict[str, Any]] = []
    skip_counts: dict[str, int] = {}

    ws_buffer = buffer or MarketWsEventLogBuffer(
        Path(config.raw_pm_events_path),
        max_lines=int(config.market_ws_tail_lines),
        max_bytes=int(config.market_ws_tail_max_bytes),
    )
    rows = ws_buffer.refresh()
    buffer_stats = dict(ws_buffer.stats)
    cutoff = now - max(1.0, float(config.lookback_s))
    signal_items: list[tuple[float | None, dict[str, Any]]] = []
    for row in rows:
        row_ts = _row_ts_s(row)
        if row_ts is None or row_ts < cutoff:
            continue
        for item in _ws_items(row):
            signal_items.append((row_ts, item))
    signal_items.sort(key=lambda pair: pair[0] or 0.0)

    for row_ts, item in signal_items:
        signal_key = _signal_key(item, row_ts)
        if signal_key in processed:
            continue
        processed.add(signal_key)
        if len([row for row in new_records if row.get("event") == "weird_peak_fast_inferred_order"]) >= int(config.max_signals_per_poll):
            break
        reason = ""
        condition_id = str(item.get("market") or "")
        token_id = str(item.get("token_id") or "")
        token_meta = token_map.get(token_id) or {}
        price = num(item.get("price"))
        ws_size = num(item.get("size"))
        if str(item.get("event_type") or "") != "price_change":
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
        elif price < float(config.min_price) or price > float(config.hard_price_cap):
            reason = "price_outside_paper_caps"
        elif ws_size < float(config.min_ws_size):
            reason = "ws_size_below_minimum"

        if reason:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            if len([row for row in new_records if row.get("event") == "weird_peak_fast_inferred_skip"]) < int(config.max_skip_records_per_poll):
                new_records.append(_skip_record(reason, item, signal_key=signal_key, row_ts=row_ts, now_ts=now))
            continue

        window = _window(state, condition_id=condition_id, token_meta=token_meta)
        if int(window.get("paper_order_count") or 0) >= int(config.max_orders_per_window):
            reason = "max_orders_per_window_reached"
        else:
            remaining_usd = max(0.0, float(config.max_window_usd) - num(window.get("cost_usd")))
            if remaining_usd <= 0:
                reason = "max_window_usd_reached"
        if reason:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            if len([row for row in new_records if row.get("event") == "weird_peak_fast_inferred_skip"]) < int(config.max_skip_records_per_poll):
                new_records.append(_skip_record(reason, item, signal_key=signal_key, row_ts=row_ts, now_ts=now))
            continue

        size_usd = round(min(float(config.order_usd), remaining_usd), 6)
        shares = round(size_usd / price, 6)
        order_id = _paper_order_id(signal_key)
        order = {
            "ts": datetime.fromtimestamp(now, UTC).isoformat(),
            "event": "weird_peak_fast_inferred_order",
            "paper_order_id": order_id,
            "signal_key": signal_key,
            "candidate_family": "btc_weird_peak_fast_inferred_ws_inventory_v1",
            "inventory_family": "btc_weird_peak_inventory_mirror_v1",
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "condition_id": condition_id,
            "market_slug": token_meta.get("market_slug"),
            "window_start_s": token_meta.get("window_start_s"),
            "token_id": token_id,
            "outcome": str(token_meta.get("outcome") or ""),
            "side": str(token_meta.get("outcome") or ""),
            "ws_recv_ts": row_ts,
            "ws_price": price,
            "ws_size": ws_size,
            "ws_book_hash": item.get("book_hash"),
            "limit_price": price,
            "size_usd": size_usd,
            "shares": shares,
            "order_type": "PAPER_ONLY_FAST_INFERRED",
            "reason": "fresh market-WS BUY price_change inferred as Weird-Peak-like inventory signal",
        }
        orders_by_id[order_id] = order
        window["paper_order_count"] = int(window.get("paper_order_count") or 0) + 1
        window["cost_usd"] = round(num(window.get("cost_usd")) + size_usd, 6)
        window.setdefault("orders", []).append(order_id)
        window["inventory"] = _inventory_from_orders([orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id])
        new_records.append(order)

    state["processed_signal_keys"] = list(processed)[-max(1, int(config.retain_processed_keys)) :]
    state["paper_orders"] = sorted(list(orders_by_id.values()), key=lambda row: str(row.get("ts") or ""))[
        -max(1, int(config.retain_orders)) :
    ]
    _resolve_open_orders(state, config, new_records)
    orders_by_id = {str(order.get("paper_order_id")): order for order in state.get("paper_orders") or [] if order.get("paper_order_id")}
    for condition_id, window in (state.get("windows") or {}).items():
        if not isinstance(window, dict):
            continue
        window["condition_id"] = condition_id
        window_orders = [orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id]
        window["inventory"] = _inventory_from_orders(window_orders)

    state.update({
        "schema_version": 1,
        "kind": "btc_weird_peak_fast_inferred_paper_flow",
        "generated_at": utc_now_iso(),
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "strategy_families": [
            "btc_weird_peak_fast_inferred_ws_inventory_v1",
            "btc_weird_peak_hybrid_confirmed_fast_inventory_v1",
        ],
        "source_contract": {
            "status": "PASS" if buffer_stats.get("row_count") and token_map else "WATCH",
            "market_ws_buffer": buffer_stats,
            "token_outcome_map_count": len(token_map),
            "lookback_s": float(config.lookback_s),
            "max_signals_per_poll": int(config.max_signals_per_poll),
            "note": "market-WS is fast inference; confirmed wallet tracker remains truth/audit layer",
        },
        "summary": _summarize(state, new_records=len(new_records), skip_counts=skip_counts, buffer_stats=buffer_stats),
        "latest_records": new_records[-50:],
    })
    append_jsonl(Path(config.event_log_path), new_records)
    atomic_write_json(state_path, state)
    return state
