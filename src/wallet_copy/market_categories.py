"""Market-category normalization for wallet-copy evidence reports."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


MARKET_CATEGORY_ORDER = ("btc_5m", "crypto_other", "sports", "politics", "events", "unknown")

_EVENT_CATEGORY_VALUES = {
    "business",
    "economics",
    "economy",
    "events",
    "event",
    "pop_culture",
    "pop culture",
    "science",
    "culture",
    "entertainment",
}


def _as_iterable(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _normalize_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _contains_btc_5m_signal(text: str) -> bool:
    value = str(text or "").strip().lower()
    if not value:
        return False
    if value.startswith("btc-updown-5m-") or "btc_updown_5m" in value:
        return True
    has_btc = "btc" in value or "bitcoin" in value
    has_5m = bool(re.search(r"\b5\s*(m|min|minute|minutes)\b", value))
    return has_btc and has_5m


def normalize_market_category(value: Any) -> str:
    """Normalize leaderboard/Gamma category labels to flow-report buckets."""

    raw = str(value or "").strip()
    if _contains_btc_5m_signal(raw):
        return "btc_5m"
    normalized = _normalize_text(raw)
    if not normalized:
        return ""
    if normalized in {"btc_5m", "bitcoin_5m"}:
        return "btc_5m"
    if normalized in {"crypto", "cryptocurrency", "crypto_other"}:
        return "crypto_other"
    if normalized in {"sports", "sport"}:
        return "sports"
    if normalized in {"politics", "political"}:
        return "politics"
    if normalized in _EVENT_CATEGORY_VALUES:
        return "events"
    if normalized in MARKET_CATEGORY_ORDER:
        return normalized
    return ""


def market_categories_from_metadata(*records: Mapping[str, Any] | None, fallback: Iterable[Any] = ()) -> list[str]:
    """Classify market metadata without inventing categories from token IDs alone."""

    categories: list[str] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        text_values: list[Any] = []
        for key in (
            "market_slug",
            "marketSlug",
            "event_slug",
            "eventSlug",
            "slug",
            "question",
            "title",
            "name",
        ):
            text_values.extend(_as_iterable(record.get(key)))
        if any(_contains_btc_5m_signal(str(value)) for value in text_values):
            categories.append("btc_5m")
        for key in ("market_category", "market_categories", "category", "categories", "tags"):
            for value in _as_iterable(record.get(key)):
                category = normalize_market_category(value)
                if category:
                    categories.append(category)
    for value in fallback:
        category = normalize_market_category(value)
        if category:
            categories.append(category)
    ordered: list[str] = []
    for category in MARKET_CATEGORY_ORDER:
        if category in categories and category not in ordered:
            ordered.append(category)
    for category in categories:
        if category and category not in ordered:
            ordered.append(category)
    return ordered or ["unknown"]


def primary_market_category(categories: Iterable[Any]) -> str:
    normalized = market_categories_from_metadata({"market_categories": list(categories)})
    return normalized[0] if normalized else "unknown"


def summarize_wallet_market_categories(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Aggregate top-10 wallet rows by primary market category."""

    summary: dict[str, dict[str, Any]] = {}
    selected_wallet_categories = Counter()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        categories = market_categories_from_metadata(row)
        primary = categories[0] if categories else "unknown"
        selected_wallet_categories[primary] += 1
        item = summary.setdefault(
            primary,
            {
                "selected_wallets": 0,
                "wallets_with_buy_sample": 0,
                "wallets_with_copyable_buys": 0,
                "events_seen": 0,
                "buy_events": 0,
                "sell_events": 0,
                "copyable_buy_events": 0,
                "rejected_buy_events": 0,
                "paper_pnl_usd": 0.0,
            },
        )
        item["selected_wallets"] += 1
        buy_events = int(row.get("buy_events") or row.get("recent_polygon_ws_buy_events") or 0)
        copyable = int(row.get("copyable_buy_events") or row.get("recent_copy_sized_buy_events") or 0)
        item["events_seen"] += int(row.get("events_seen") or row.get("recent_polygon_ws_events") or 0)
        item["buy_events"] += buy_events
        item["sell_events"] += int(row.get("sell_events") or row.get("recent_polygon_ws_sell_events") or 0)
        item["copyable_buy_events"] += copyable
        item["rejected_buy_events"] += int(row.get("rejected_buy_events") or 0)
        item["paper_pnl_usd"] = round(float(item["paper_pnl_usd"]) + float(row.get("paper_pnl_usd") or 0.0), 6)
        if buy_events > 0:
            item["wallets_with_buy_sample"] += 1
        if copyable > 0:
            item["wallets_with_copyable_buys"] += 1
    for item in summary.values():
        buy_events = int(item.get("buy_events") or 0)
        item["copyable_rate_pct"] = round(100.0 * int(item.get("copyable_buy_events") or 0) / buy_events, 6) if buy_events else None
    return {category: summary[category] for category in MARKET_CATEGORY_ORDER if category in summary}
