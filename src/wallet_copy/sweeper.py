"""Post-resolution sweeper profile analysis for BTC 5-minute wallets.

This module is research-only. It measures whether a source wallet behaves like
the public "sweeper" pattern: high-price buys near or after the 5-minute close,
followed by lifecycle/redeem activity. The output is evidence for research and
paper filters, never live admission by itself.
"""

from __future__ import annotations

import re
from collections import defaultdict
from statistics import median
from typing import Any

from src.wallet_copy.models import utc_now_iso


WINDOW_SECONDS = 300
WINDOW_RE = re.compile(r"btc[-_a-z]*5m[-_]?(\d{10})|btc-updown-5m-(\d{10})", re.IGNORECASE)


def extract_btc_5m_window_start(event: dict[str, Any]) -> int | None:
    """Return the BTC 5m window start timestamp embedded in Polymarket slugs."""

    text = " ".join(
        str(event.get(key) or "")
        for key in ("market_slug", "event_slug", "slug", "title")
    )
    match = WINDOW_RE.search(text)
    if not match:
        return None
    value = match.group(1) or match.group(2)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pct(part: int | float, total: int | float) -> float:
    return round((float(part) / float(total)) * 100.0, 6) if total else 0.0


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def analyze_sweeper_profiles(
    events: list[dict[str, Any]],
    *,
    high_price_threshold: float = 0.95,
    queue_band_s: float = 30.0,
) -> dict[str, Any]:
    """Score wallets for high-price near-close sweeper behavior."""

    wallets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "address": "",
            "wallet_name": "",
            "total_events": 0,
            "buy_events": 0,
            "lifecycle_events": 0,
            "redeem_events": 0,
            "high_price_buy_events": 0,
            "near_close_buy_events": 0,
            "post_close_buy_events": 0,
            "high_price_near_or_post_close_buy_events": 0,
            "buy_notional_usd": 0.0,
            "high_price_buy_notional_usd": 0.0,
            "seconds_to_close_values": [],
            "high_price_seconds_to_close_values": [],
            "sample_events": [],
        }
    )
    for event in events:
        wallet_key = str(event.get("source_wallet") or event.get("wallet_name") or "unknown").lower()
        row = wallets[wallet_key]
        row["address"] = str(event.get("source_wallet") or wallet_key).lower()
        row["wallet_name"] = str(event.get("wallet_name") or row["wallet_name"] or wallet_key)
        row["total_events"] += 1
        action = str(event.get("action") or "").upper()
        if action in {"SELL", "MERGE", "REDEEM"}:
            row["lifecycle_events"] += 1
            if action == "REDEEM":
                row["redeem_events"] += 1
            continue
        if action != "BUY":
            continue
        row["buy_events"] += 1
        price = float(event.get("price") or 0.0)
        notional = float(event.get("usdc_size") or 0.0)
        row["buy_notional_usd"] = round(float(row["buy_notional_usd"]) + notional, 6)
        window_start = extract_btc_5m_window_start(event)
        seconds_to_close: float | None = None
        if window_start is not None and event.get("event_ts") is not None:
            seconds_to_close = round((float(window_start) + WINDOW_SECONDS) - float(event["event_ts"]), 6)
            row["seconds_to_close_values"].append(seconds_to_close)
        is_high = price >= high_price_threshold
        is_near = seconds_to_close is not None and seconds_to_close <= queue_band_s
        is_post = seconds_to_close is not None and seconds_to_close <= 0
        if is_high:
            row["high_price_buy_events"] += 1
            row["high_price_buy_notional_usd"] = round(float(row["high_price_buy_notional_usd"]) + notional, 6)
            if seconds_to_close is not None:
                row["high_price_seconds_to_close_values"].append(seconds_to_close)
        if is_near:
            row["near_close_buy_events"] += 1
        if is_post:
            row["post_close_buy_events"] += 1
        if is_high and (is_near or is_post):
            row["high_price_near_or_post_close_buy_events"] += 1
            if len(row["sample_events"]) < 10:
                row["sample_events"].append(
                    {
                        "event_id": event.get("event_id"),
                        "market_slug": event.get("market_slug") or event.get("event_slug"),
                        "outcome": event.get("outcome"),
                        "price": price,
                        "usdc_size": notional,
                        "seconds_to_close": seconds_to_close,
                        "event_ts": event.get("event_ts"),
                    }
                )

    profiles: list[dict[str, Any]] = []
    for row in wallets.values():
        buy_events = int(row["buy_events"])
        high_events = int(row["high_price_buy_events"])
        near_high_events = int(row["high_price_near_or_post_close_buy_events"])
        high_price_pct = _pct(high_events, buy_events)
        near_high_pct = _pct(near_high_events, buy_events)
        lifecycle_to_buy_pct = _pct(int(row["lifecycle_events"]), buy_events)
        status = "NO_SWEEPER_SIGNATURE"
        if buy_events >= 20 and high_price_pct >= 80.0 and near_high_pct >= 50.0:
            status = "STRONG_SWEEPER_SIGNATURE"
        elif buy_events >= 20 and high_price_pct >= 40.0 and near_high_pct >= 10.0:
            status = "PARTIAL_SWEEPER_SIGNATURE"
        profile = {
            "address": row["address"],
            "wallet_name": row["wallet_name"],
            "status": status,
            "total_events": int(row["total_events"]),
            "buy_events": buy_events,
            "lifecycle_events": int(row["lifecycle_events"]),
            "redeem_events": int(row["redeem_events"]),
            "buy_notional_usd": round(float(row["buy_notional_usd"]), 6),
            "high_price_buy_events": high_events,
            "high_price_buy_notional_usd": round(float(row["high_price_buy_notional_usd"]), 6),
            "high_price_buy_pct": high_price_pct,
            "near_close_buy_events": int(row["near_close_buy_events"]),
            "post_close_buy_events": int(row["post_close_buy_events"]),
            "high_price_near_or_post_close_buy_events": near_high_events,
            "high_price_near_or_post_close_buy_pct": near_high_pct,
            "lifecycle_to_buy_pct": lifecycle_to_buy_pct,
            "seconds_to_close_median": _median(row["seconds_to_close_values"]),
            "high_price_seconds_to_close_median": _median(row["high_price_seconds_to_close_values"]),
            "sample_events": row["sample_events"],
        }
        profiles.append(profile)
    profiles.sort(
        key=lambda item: (
            item["status"] != "STRONG_SWEEPER_SIGNATURE",
            -float(item["high_price_near_or_post_close_buy_pct"]),
            -int(item["buy_events"]),
        )
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_sweeper_profile_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "config": {
            "high_price_threshold": high_price_threshold,
            "queue_band_s": queue_band_s,
            "window_seconds": WINDOW_SECONDS,
        },
        "profiles": profiles,
        "strong_sweeper_wallets": [
            row for row in profiles if row["status"] == "STRONG_SWEEPER_SIGNATURE"
        ],
    }
