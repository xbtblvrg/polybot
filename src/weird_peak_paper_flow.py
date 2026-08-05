"""Paper-only Weird-Peak flow and inventory mirror.

The production BTC ML runner remains the source of live trading.  This module
turns confirmed Weird-Peak wallet events into paper orders so the new
multi-order/two-sided hypothesis can collect evidence without any execution
path.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.weird_peak_wallet_tracker import append_jsonl, atomic_write_json


UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class WeirdPeakPaperConfig:
    target_wallet: str
    state_path: str = "data/research/weird_peak_paper_flow_state.json"
    event_log_path: str = "data/research/weird_peak_paper_flow_events.jsonl"
    resolutions_path: str = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
    order_usd: float = 1.0
    mirror_wallet_size_fraction: float = 0.0
    max_orders_per_window: int = 12
    max_window_usd: float = 12.0
    max_confirmed_age_s: float = 300.0
    hard_price_cap: float = 0.98
    min_price: float = 0.01
    retain_processed_keys: int = 20_000
    retain_orders: int = 5_000


def event_canonical_key(event: dict[str, Any]) -> str:
    tx_hash = str(event.get("transaction_hash") or event.get("transactionHash") or "")
    if tx_hash:
        return "|".join(
            [
                tx_hash,
                str(event.get("condition_id") or event.get("conditionId") or ""),
                str(event.get("token_id") or event.get("asset") or ""),
                str(event.get("side") or "").upper(),
                str(event.get("outcome") or ""),
                f"{num(event.get('price')):.10f}",
                f"{num(event.get('size')):.10f}",
                str(int(num(event.get("event_ts") or event.get("timestamp"), 0.0))),
            ]
        )
    return str(event.get("dedupe_key") or hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest())


def paper_order_id(canonical_key: str) -> str:
    return hashlib.sha256(f"weird_peak_paper|{canonical_key}".encode()).hexdigest()[:24]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def load_resolutions(path: Path) -> dict[str, dict[str, Any]]:
    resolutions: dict[str, dict[str, Any]] = {}
    for row in load_jsonl(path):
        direction = str(row.get("direction") or "").upper()
        if direction not in {"UP", "DOWN"}:
            continue
        for key in ("condition_id", "market_id", "clob_market_id", "yes_token", "no_token", "token_id"):
            value = str(row.get(key) or "")
            if value:
                resolutions[value] = row
    return resolutions


def resolution_for_order(resolutions: dict[str, dict[str, Any]], order: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("condition_id", "market_id", "clob_market_id", "token_id", "asset_id"):
        value = str(order.get(key) or "")
        if value and value in resolutions:
            return resolutions[value]
    return None


def _initial_state(config: WeirdPeakPaperConfig) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "btc_weird_peak_paper_flow",
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "target_wallet": config.target_wallet.lower(),
        "strategy_families": [
            "btc_weird_peak_confirmed_copy_v1",
            "btc_weird_peak_flow_consensus_v1",
            "btc_weird_peak_inventory_mirror_v1",
        ],
        "processed_trade_keys": [],
        "paper_orders": [],
        "windows": {},
        "resolved_order_ids": [],
    }


def _event_age_s(event: dict[str, Any], now_ts: float) -> float | None:
    event_ts = int(num(event.get("event_ts"), 0.0))
    if event_ts <= 0:
        return None
    return max(0.0, now_ts - float(event_ts))


def _skip_record(reason: str, event: dict[str, Any], *, canonical_key: str, now_ts: float) -> dict[str, Any]:
    ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
    return {
        "ts": datetime.fromtimestamp(now_ts, UTC).isoformat(),
        "event": "weird_peak_paper_skip",
        "reason": reason,
        "read_only": True,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "candidate_family": "btc_weird_peak_confirmed_copy_v1",
        "canonical_key": canonical_key,
        "target_wallet": str(event.get("target_wallet") or "").lower(),
        "condition_id": str(event.get("condition_id") or ""),
        "market_slug": str(event.get("market_slug") or ""),
        "outcome": str(event.get("outcome") or ""),
        "wallet_side": str(event.get("side") or "").upper(),
        "wallet_price": num(event.get("price")),
        "wallet_usdc_size": round(num(event.get("usdc_size")), 6),
        "event_ts": event.get("event_ts"),
        "observed_ts": event.get("observed_ts"),
        "api_latency_s": event.get("api_latency_s"),
        "market_ws_matched": bool(ws.get("matched")),
        "ws_to_api_observed_latency_s": ws.get("ws_to_api_observed_latency_s"),
        "event_age_s": _event_age_s(event, now_ts),
    }


def _event_order_size(event: dict[str, Any], config: WeirdPeakPaperConfig, remaining_window_usd: float) -> float:
    base = max(0.0, float(config.order_usd))
    wallet_usdc = max(0.0, num(event.get("usdc_size")))
    if config.mirror_wallet_size_fraction > 0.0 and wallet_usdc > 0.0:
        base = min(base, wallet_usdc * float(config.mirror_wallet_size_fraction))
    return round(max(0.0, min(base, remaining_window_usd)), 6)


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
            "cost_usd": 0.0,
            "orders": [],
            "inventory": {},
            "latest_wallet_event_ts": None,
            "resolved": None,
        },
    )
    window["market_slug"] = window.get("market_slug") or event.get("market_slug")
    window["title"] = window.get("title") or event.get("title")
    window["window_start_s"] = window.get("window_start_s") or event.get("window_start_s")
    return window


def _inventory_from_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    by_outcome: dict[str, dict[str, float]] = {
        "Up": {"cost_usd": 0.0, "shares": 0.0, "orders": 0.0},
        "Down": {"cost_usd": 0.0, "shares": 0.0, "orders": 0.0},
    }
    for order in orders:
        outcome = str(order.get("outcome") or "")
        if outcome not in by_outcome:
            continue
        by_outcome[outcome]["cost_usd"] += num(order.get("size_usd"))
        by_outcome[outcome]["shares"] += num(order.get("shares"))
        by_outcome[outcome]["orders"] += 1

    up = by_outcome["Up"]
    down = by_outcome["Down"]
    paired_shares = min(up["shares"], down["shares"])
    up_avg = up["cost_usd"] / up["shares"] if up["shares"] > 0 else 0.0
    down_avg = down["cost_usd"] / down["shares"] if down["shares"] > 0 else 0.0
    paired_cost = paired_shares * (up_avg + down_avg)
    paired_edge = paired_shares - paired_cost
    residual_outcome = ""
    residual_shares = 0.0
    if up["shares"] > down["shares"]:
        residual_outcome = "Up"
        residual_shares = up["shares"] - down["shares"]
    elif down["shares"] > up["shares"]:
        residual_outcome = "Down"
        residual_shares = down["shares"] - up["shares"]
    return {
        "up": {
            "cost_usd": round(up["cost_usd"], 6),
            "shares": round(up["shares"], 6),
            "orders": int(up["orders"]),
        },
        "down": {
            "cost_usd": round(down["cost_usd"], 6),
            "shares": round(down["shares"], 6),
            "orders": int(down["orders"]),
        },
        "total_cost_usd": round(up["cost_usd"] + down["cost_usd"], 6),
        "both_sides": up["shares"] > 0 and down["shares"] > 0,
        "paired_shares": round(paired_shares, 6),
        "paired_cost_usd": round(paired_cost, 6),
        "paired_edge_usd": round(paired_edge, 6),
        "residual_outcome": residual_outcome,
        "residual_shares": round(residual_shares, 6),
    }


def _resolve_order(order: dict[str, Any], resolution: dict[str, Any]) -> dict[str, Any]:
    winner = str(resolution.get("direction") or "").upper()
    outcome = str(order.get("outcome") or "").upper()
    won = (outcome == "UP" and winner == "UP") or (outcome == "DOWN" and winner == "DOWN")
    cost = num(order.get("size_usd"))
    payout = num(order.get("shares")) if won else 0.0
    pnl = payout - cost
    return {
        "resolved": True,
        "winner": winner,
        "won": won,
        "cost_usd": round(cost, 6),
        "payout_usd": round(payout, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / cost * 100.0) if cost > 0 else 0.0, 6),
        "resolution_source": resolution.get("source"),
        "resolved_at_iso": resolution.get("computed_at_iso"),
        "expiry_iso": resolution.get("expiry_iso"),
    }


def _update_window_resolution(window: dict[str, Any], orders_by_id: dict[str, dict[str, Any]]) -> None:
    order_ids = [str(item) for item in window.get("orders") or []]
    orders = [orders_by_id[order_id] for order_id in order_ids if order_id in orders_by_id]
    resolved = [order for order in orders if isinstance(order.get("resolution"), dict)]
    if not orders or len(resolved) != len(orders):
        return
    cost = sum(num(order.get("size_usd")) for order in orders)
    pnl = sum(num((order.get("resolution") or {}).get("pnl_usd")) for order in orders)
    wins = sum(1 for order in resolved if (order.get("resolution") or {}).get("won") is True)
    window["resolved"] = {
        "order_count": len(orders),
        "wins": wins,
        "losses": len(orders) - wins,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / cost * 100.0) if cost > 0 else 0.0, 6),
        "won": pnl > 0,
    }


def _flow_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    latest = payload.get("latest_windows") or []
    if not latest:
        return {
            "status": "NO_FLOW",
            "latest_windows": [],
            "active_candidate_family": "btc_weird_peak_flow_consensus_v1",
        }
    rows = []
    for row in latest[:10]:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "condition_id": row.get("condition_id"),
                "market_slug": row.get("market_slug"),
                "window_start_s": row.get("window_start_s"),
                "trade_count": row.get("trade_count"),
                "buy_up_usdc": row.get("buy_up_usdc"),
                "buy_down_usdc": row.get("buy_down_usdc"),
                "dominant_buy_outcome": row.get("dominant_buy_outcome"),
                "dominant_buy_ratio": row.get("dominant_buy_ratio"),
                "both_buy_outcomes": row.get("both_buy_outcomes"),
                "event_age_s": row.get("event_age_s"),
            }
        )
    return {
        "status": "FLOW_PRESENT",
        "active_candidate_family": "btc_weird_peak_flow_consensus_v1",
        "latest_windows": rows,
    }


def run_paper_flow_once(
    tracker_payload: dict[str, Any],
    config: WeirdPeakPaperConfig,
    *,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now = float(now_ts if now_ts is not None else time.time())
    state_path = Path(config.state_path)
    state = load_json(state_path) or _initial_state(config)
    processed = set(str(key) for key in state.get("processed_trade_keys") or [])
    orders = [order for order in state.get("paper_orders") or [] if isinstance(order, dict)]
    orders_by_id = {str(order.get("paper_order_id")): order for order in orders if order.get("paper_order_id")}
    new_records: list[dict[str, Any]] = []

    recent_events = [
        row
        for row in tracker_payload.get("recent_events") or []
        if isinstance(row, dict)
    ]
    recent_events.sort(key=lambda item: (int(num(item.get("event_ts"), 0.0)), num(item.get("observed_ts"))))
    for event in recent_events:
        key = event_canonical_key(event)
        if key in processed:
            continue
        processed.add(key)
        condition_id = str(event.get("condition_id") or "")
        outcome = str(event.get("outcome") or "")
        wallet_side = str(event.get("side") or "").upper()
        price = num(event.get("price"))
        age = _event_age_s(event, now)
        if str(event.get("row_type") or "").upper() != "TRADE":
            new_records.append(_skip_record("not_trade_row", event, canonical_key=key, now_ts=now))
            continue
        if wallet_side != "BUY":
            new_records.append(_skip_record("wallet_side_not_buy", event, canonical_key=key, now_ts=now))
            continue
        if outcome not in {"Up", "Down"}:
            new_records.append(_skip_record("missing_or_unknown_outcome", event, canonical_key=key, now_ts=now))
            continue
        if not condition_id:
            new_records.append(_skip_record("missing_condition_id", event, canonical_key=key, now_ts=now))
            continue
        if price < config.min_price or price > config.hard_price_cap:
            new_records.append(_skip_record("price_outside_paper_caps", event, canonical_key=key, now_ts=now))
            continue
        if age is None or (config.max_confirmed_age_s > 0 and age > config.max_confirmed_age_s):
            new_records.append(_skip_record("confirmed_wallet_event_stale_for_paper", event, canonical_key=key, now_ts=now))
            continue

        window = _window(state, condition_id, event)
        if int(window.get("paper_order_count") or 0) >= int(config.max_orders_per_window):
            new_records.append(_skip_record("max_orders_per_window_reached", event, canonical_key=key, now_ts=now))
            continue
        remaining_usd = max(0.0, float(config.max_window_usd) - num(window.get("cost_usd")))
        size_usd = _event_order_size(event, config, remaining_usd)
        if size_usd <= 0:
            new_records.append(_skip_record("max_window_usd_reached", event, canonical_key=key, now_ts=now))
            continue

        shares = round(size_usd / price, 6)
        order_id = paper_order_id(key)
        ws = event.get("market_ws_corroboration") if isinstance(event.get("market_ws_corroboration"), dict) else {}
        order = {
            "ts": datetime.fromtimestamp(now, UTC).isoformat(),
            "event": "weird_peak_paper_order",
            "paper_order_id": order_id,
            "canonical_key": key,
            "candidate_family": "btc_weird_peak_confirmed_copy_v1",
            "inventory_family": "btc_weird_peak_inventory_mirror_v1",
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "target_wallet": config.target_wallet.lower(),
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
            "token_id": event.get("token_id"),
            "outcome": outcome,
            "side": outcome,
            "wallet_price": price,
            "limit_price": price,
            "size_usd": size_usd,
            "shares": shares,
            "order_type": "PAPER_ONLY",
            "reason": "confirmed Weird-Peak BUY trade mirrored in paper inventory",
        }
        orders_by_id[order_id] = order
        window["paper_order_count"] = int(window.get("paper_order_count") or 0) + 1
        window["cost_usd"] = round(num(window.get("cost_usd")) + size_usd, 6)
        window["latest_wallet_event_ts"] = event.get("event_ts")
        window.setdefault("orders", []).append(order_id)
        window["inventory"] = _inventory_from_orders([orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id])
        new_records.append(order)

    resolutions = load_resolutions(Path(config.resolutions_path))
    resolved_ids = set(str(item) for item in state.get("resolved_order_ids") or [])
    for order_id, order in list(orders_by_id.items()):
        if order_id in resolved_ids:
            continue
        resolution = resolution_for_order(resolutions, order)
        if not resolution:
            continue
        order["resolution"] = _resolve_order(order, resolution)
        resolved_ids.add(order_id)
        new_records.append(
            {
                "ts": utc_now_iso(),
                "event": "weird_peak_paper_resolve",
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
    orders = sorted(list(orders_by_id.values()), key=lambda row: str(row.get("ts") or ""))[
        -max(1, int(config.retain_orders)) :
    ]
    orders_by_id = {str(order.get("paper_order_id")): order for order in orders if order.get("paper_order_id")}
    windows = state.get("windows") if isinstance(state.get("windows"), dict) else {}
    for condition_id, window in windows.items():
        if isinstance(window, dict):
            window_orders = [orders_by_id[oid] for oid in window.get("orders", []) if oid in orders_by_id]
            window["inventory"] = _inventory_from_orders(window_orders)
            _update_window_resolution(window, orders_by_id)
            window["condition_id"] = condition_id

    resolved_orders = [order for order in orders if isinstance(order.get("resolution"), dict)]
    wins = sum(1 for order in resolved_orders if (order.get("resolution") or {}).get("won") is True)
    cost = sum(num(order.get("size_usd")) for order in resolved_orders)
    pnl = sum(num((order.get("resolution") or {}).get("pnl_usd")) for order in resolved_orders)
    open_orders = [order for order in orders if not isinstance(order.get("resolution"), dict)]
    state.update(
        {
            "schema_version": 1,
            "kind": "btc_weird_peak_paper_flow",
            "generated_at": utc_now_iso(),
            "read_only": True,
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "target_wallet": config.target_wallet.lower(),
            "strategy_families": [
                "btc_weird_peak_confirmed_copy_v1",
                "btc_weird_peak_flow_consensus_v1",
                "btc_weird_peak_inventory_mirror_v1",
            ],
            "processed_trade_keys": processed_list,
            "paper_orders": orders,
            "resolved_order_ids": sorted(resolved_ids)[-max(1, int(config.retain_orders)) :],
            "flow_snapshot": _flow_snapshot(tracker_payload),
            "summary": {
                "new_records": len(new_records),
                "paper_orders": len(orders),
                "open_orders": len(open_orders),
                "resolved_orders": len(resolved_orders),
                "wins": wins,
                "losses": len(resolved_orders) - wins,
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
                "roi_pct": round((pnl / cost * 100.0) if cost > 0 else 0.0, 6),
                "wr_pct": round((wins / len(resolved_orders) * 100.0) if resolved_orders else 0.0, 6),
                "window_count": len(windows),
                "both_sided_window_count": sum(
                    1
                    for window in windows.values()
                    if isinstance(window, dict) and (window.get("inventory") or {}).get("both_sides") is True
                ),
            },
            "latest_records": new_records[-50:],
        }
    )
    append_jsonl(Path(config.event_log_path), new_records)
    atomic_write_json(state_path, state)
    return state
