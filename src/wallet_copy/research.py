"""Research and cross-analysis helpers for wallet-copy data."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from typing import Any

from src.wallet_copy.models import WalletEvent, num


def _event_identity_key(event: WalletEvent) -> str:
    return event.source_fingerprint or event.event_id or (
        f"{event.source_wallet.lower()}:{event.transaction_hash}:"
        f"{event.condition_id}:{event.action}:{event.outcome}:"
        f"{event.event_ts}:{event.price}:{event.usdc_size}"
    )


def _prefer_detection_event(current: WalletEvent, candidate: WalletEvent) -> WalletEvent:
    current_seen = float(current.observed_ts or current.event_ts or 0.0)
    candidate_seen = float(candidate.observed_ts or candidate.event_ts or 0.0)
    if candidate_seen <= 0:
        winner = current
        loser = candidate
    elif current_seen <= 0 or candidate_seen < current_seen:
        winner = candidate
        loser = current
    elif candidate_seen == current_seen and candidate.source == "rtds_activity" and current.source != "rtds_activity":
        winner = candidate
        loser = current
    else:
        winner = current
        loser = candidate

    raw = dict(winner.raw or {})
    sources = set(raw.get("observation_sources") or [])
    for source in (winner.source, loser.source):
        if source:
            sources.add(source)
    raw["observation_sources"] = sorted(sources)
    raw["detection_source"] = winner.source
    raw["deduped_observed_ts"] = float(winner.observed_ts or 0.0)
    if loser.observed_ts:
        raw["alternate_observed_ts"] = float(loser.observed_ts)
        raw["alternate_detection_source"] = loser.source
    return replace(winner, raw=raw)


def unique_wallet_events(events: list[WalletEvent]) -> list[WalletEvent]:
    unique: dict[str, WalletEvent] = {}
    for event in events:
        key = _event_identity_key(event)
        existing = unique.get(key)
        unique[key] = event if existing is None else _prefer_detection_event(existing, event)
    return list(unique.values())


def wallet_event_summary(events: list[WalletEvent]) -> dict[str, Any]:
    events = unique_wallet_events(events)
    by_wallet: dict[str, list[WalletEvent]] = defaultdict(list)
    for event in events:
        by_wallet[event.source_wallet.lower()].append(event)
    wallet_rows = {}
    for wallet, rows in by_wallet.items():
        buys = [event for event in rows if event.action.upper() == "BUY"]
        wallet_rows[wallet] = {
            "wallet_name": rows[0].wallet_name if rows else "",
            "events": len(rows),
            "buy_events": len(buys),
            "notional_usd": round(sum(float(event.usdc_size) for event in buys), 6),
            "latest_event_ts": max((event.event_ts or 0.0 for event in rows), default=0.0),
        }
    return {
        "wallet_count": len(wallet_rows),
        "events": len(events),
        "buy_events": sum(1 for event in events if event.action.upper() == "BUY"),
        "wallets": dict(sorted(wallet_rows.items())),
    }


def cross_wallet_windows(events: list[WalletEvent]) -> list[dict[str, Any]]:
    events = unique_wallet_events(events)
    grouped: dict[str, list[WalletEvent]] = defaultdict(list)
    for event in events:
        if event.condition_id:
            grouped[event.condition_id].append(event)
    rows: list[dict[str, Any]] = []
    for condition_id, market_events in grouped.items():
        buy_events = [
            event
            for event in market_events
            if event.action.upper() == "BUY" and event.outcome and float(event.price) > 0
        ]
        if not buy_events:
            continue
        outcomes: dict[str, list[WalletEvent]] = defaultdict(list)
        for event in buy_events:
            outcomes[event.outcome].append(event)
        rows.append(
            {
                "condition_id": condition_id,
                "market_slug": buy_events[0].market_slug,
                "wallet_count": len({event.source_wallet.lower() for event in buy_events}),
                "buy_events": len(buy_events),
                "outcomes": {
                    outcome: {
                        "wallets": sorted({event.wallet_name for event in outcome_events}),
                        "wallet_addresses": sorted({event.source_wallet.lower() for event in outcome_events}),
                        "events": len(outcome_events),
                        "notional_usd": round(sum(float(event.usdc_size) for event in outcome_events), 6),
                        "avg_price": round(
                            sum(float(event.price) for event in outcome_events) / len(outcome_events),
                            6,
                        ),
                    }
                    for outcome, outcome_events in sorted(outcomes.items())
                },
            }
        )
    return sorted(rows, key=lambda row: (-int(row["wallet_count"]), str(row["market_slug"])))


def train_rows_from_events(events: list[WalletEvent]) -> list[dict[str, Any]]:
    events = unique_wallet_events(events)
    rows: list[dict[str, Any]] = []
    for event in events:
        rows.append(
            {
                "wallet": event.source_wallet.lower(),
                "wallet_name": event.wallet_name,
                "event_id": event.event_id,
                "condition_id": event.condition_id,
                "market_slug": event.market_slug,
                "asset": event.asset,
                "duration": event.duration,
                "action": event.action,
                "outcome": event.outcome,
                "price": event.price,
                "size": event.size,
                "usdc_size": event.usdc_size,
                "event_ts": event.event_ts,
                "observed_ts": event.observed_ts,
                "api_latency_s": event.api_latency_s,
                "is_buy": event.action.upper() == "BUY",
                "price_x_notional": round(float(event.price) * num(event.usdc_size), 6),
            }
        )
    return rows
