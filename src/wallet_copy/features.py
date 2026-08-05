"""Feature extraction for wallet-copy research and ML datasets."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from src.wallet_copy.models import WalletEvent, num
from src.wallet_copy.research import unique_wallet_events


def slug_window_start(market_slug: str) -> int | None:
    match = re.search(r"(\d{9,})$", str(market_slug or ""))
    return int(match.group(1)) if match else None


def slug_duration_s(market_slug: str) -> int:
    text = str(market_slug or "").lower()
    if "15m" in text:
        return 900
    if "1h" in text:
        return 3600
    return 300


def wallet_event_feature_rows(events: list[WalletEvent]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in unique_wallet_events(events):
        start = slug_window_start(event.market_slug)
        duration = slug_duration_s(event.market_slug)
        seconds_from_open = None
        seconds_to_close = None
        if start is not None and event.event_ts is not None:
            seconds_from_open = round(float(event.event_ts) - float(start), 6)
            seconds_to_close = round(float(start + duration) - float(event.event_ts), 6)
        rows.append(
            {
                "feature_type": "wallet_event",
                "wallet": event.source_wallet.lower(),
                "wallet_name": event.wallet_name,
                "event_id": event.event_id,
                "source_fingerprint": event.source_fingerprint,
                "condition_id": event.condition_id,
                "market_slug": event.market_slug,
                "asset": event.asset,
                "duration": event.duration,
                "action": event.action.upper(),
                "outcome": event.outcome,
                "is_buy": event.action.upper() == "BUY",
                "price": event.price,
                "price_bucket_5c": round(int(max(0.0, min(0.99, float(event.price))) / 0.05) * 0.05, 2),
                "wallet_shares": event.size,
                "wallet_usdc_size": event.usdc_size,
                "seconds_from_open": seconds_from_open,
                "seconds_to_close": seconds_to_close,
                "api_latency_s": event.api_latency_s,
                "event_ts": event.event_ts,
                "observed_ts": event.observed_ts,
            }
        )
    return sorted(rows, key=lambda row: (row.get("event_ts") or 0.0, row.get("wallet") or ""))


def window_feature_rows(events: list[WalletEvent]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[WalletEvent]] = defaultdict(list)
    for event in unique_wallet_events(events):
        if event.condition_id:
            grouped[(event.condition_id, event.outcome)].append(event)
    rows: list[dict[str, Any]] = []
    for (condition_id, outcome), group in sorted(grouped.items()):
        buys = [event for event in group if event.action.upper() == "BUY"]
        if not buys:
            continue
        prices = [float(event.price) for event in buys if event.price > 0]
        sizes = [float(event.usdc_size) for event in buys]
        latencies = [num(event.api_latency_s) for event in buys if event.api_latency_s is not None]
        wallets = sorted({event.source_wallet.lower() for event in buys})
        rows.append(
            {
                "feature_type": "window_outcome",
                "condition_id": condition_id,
                "market_slug": buys[0].market_slug,
                "outcome": outcome,
                "wallet_count": len(wallets),
                "wallets": wallets,
                "buy_events": len(buys),
                "notional_usd": round(sum(sizes), 6),
                "avg_price": round(sum(prices) / len(prices), 6) if prices else 0.0,
                "min_price": round(min(prices), 6) if prices else 0.0,
                "max_price": round(max(prices), 6) if prices else 0.0,
                "price_spread": round(max(prices) - min(prices), 6) if prices else 0.0,
                "avg_latency_s": round(sum(latencies) / len(latencies), 6) if latencies else None,
                "max_latency_s": round(max(latencies), 6) if latencies else None,
                "first_event_ts": min((event.event_ts or 0.0 for event in buys), default=0.0),
                "last_event_ts": max((event.event_ts or 0.0 for event in buys), default=0.0),
            }
        )
    return rows


def build_feature_payload(events: list[WalletEvent]) -> dict[str, Any]:
    event_rows = wallet_event_feature_rows(events)
    window_rows = window_feature_rows(events)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_feature_payload",
        "event_features": event_rows,
        "window_features": window_rows,
        "summary": {
            "event_feature_rows": len(event_rows),
            "window_feature_rows": len(window_rows),
            "wallet_count": len({row["wallet"] for row in event_rows}),
        },
    }
