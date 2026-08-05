#!/usr/bin/env python3
"""Merge active-set Data API trade-poll events into wallet-copy history state.

Flow stage: LIVE/PROMOTE/SELF-DEV. This is a second signal source for active
wallets when RTDS does not carry wallet trades. It writes the same
WalletEvent-shaped history state consumed by the single live guard; it never
submits orders.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.ingest import WalletHistoryClient  # noqa: E402
from src.wallet_copy.models import WalletEvent, WalletSpec, utc_now_iso  # noqa: E402
from src.wallet_copy.profit_engine import (  # noqa: E402
    CandidatePolicy,
    build_history_window_index,
    load_history_window_index,
    policy_accepts_event,
)
from src.wallet_copy.research import unique_wallet_events  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


DEFAULT_STATE = "data/research/wallet_copy_active_set_dataapi_poller_state.json"
DEFAULT_DATAAPI_FIRST_SEEN = "data/research/dataapi_first_seen.jsonl"
DEFAULT_OBSERVATION_WATERMARK_STATE = "data/research/wallet_copy_rtds_observation_watermarks.json"
DATA_API_SOURCE_BASE_ENV_VARS = ("POLYMARKET_DATA_API_BASE_URL",)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-wallet", action="append", default=[])
    parser.add_argument("--source-wallets", default="")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--history-window-index", default="data/research/wallet_copy_history_window_index.json")
    parser.add_argument("--wallet-event-log", default="data/research/wallet_copy_live_guard_wallet_events.jsonl")
    parser.add_argument("--dataapi-first-seen-jsonl", default=DEFAULT_DATAAPI_FIRST_SEEN)
    parser.add_argument("--observation-watermark-state", default=DEFAULT_OBSERVATION_WATERMARK_STATE)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--trade-query-keys", default="user")
    parser.add_argument("--parallel-sources", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--disable-source-base-overrides", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--poll-interval-s", type=float, default=5.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-event-age-s", type=float, default=300.0)
    parser.add_argument("--history-retain-events", type=int, default=250_000)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _split_csv(value: Any) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def source_wallets_from_args(args: argparse.Namespace) -> list[str]:
    wallets = [*_split_csv(getattr(args, "source_wallets", ""))]
    for value in getattr(args, "source_wallet", []) or []:
        wallets.extend(_split_csv(value))
    out: list[str] = []
    seen: set[str] = set()
    for wallet in wallets:
        normalized = _norm_wallet(wallet)
        if normalized and normalized not in seen:
            out.append(normalized)
            seen.add(normalized)
    return out


def _trade_query_keys(value: Any) -> tuple[str, ...]:
    keys = tuple(item for item in _split_csv(value) if item in {"user", "proxyWallet"})
    return keys or ("user",)


def _wallet_name(wallet: str) -> str:
    return f"active_set_poll_{wallet[-8:]}"


@contextmanager
def _disabled_source_base_overrides(disabled: bool):
    if not disabled:
        yield
        return
    previous = {key: os.environ.get(key) for key in DATA_API_SOURCE_BASE_ENV_VARS}
    for key in DATA_API_SOURCE_BASE_ENV_VARS:
        os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _tag_event_as_poll(event: WalletEvent) -> WalletEvent:
    payload = event.asdict()
    payload.pop("source_fingerprint", None)
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    payload["source"] = "polymarket_data_api_poll"
    payload["raw"] = {
        **raw,
        "_walletCopyPollOriginalSource": raw.get("_walletCopySource"),
        "_walletCopySource": "polymarket_data_api_poll",
        "_walletCopyEventSource": "poll",
    }
    return WalletEvent.from_dict(payload)


def _event_tx_key(event: WalletEvent) -> tuple[str, str, str, str, str, str] | None:
    tx = str(event.transaction_hash or "").strip().lower()
    if not tx:
        return None
    return (
        event.source_wallet.lower(),
        tx,
        str(event.token_id or "").lower(),
        str(event.outcome or "").lower(),
        event.action.upper(),
        str(event.condition_id or "").lower(),
    )


def _events_new_to_history(
    existing_events: list[WalletEvent],
    candidate_events: list[WalletEvent],
) -> tuple[list[WalletEvent], dict[str, int]]:
    existing_fingerprints = {event.source_fingerprint for event in existing_events}
    existing_tx_keys = {key for event in existing_events if (key := _event_tx_key(event)) is not None}
    new_events: list[WalletEvent] = []
    seen_fingerprints: set[str] = set()
    seen_tx_keys: set[tuple[str, str, str, str, str, str]] = set()
    duplicate_source_fingerprint = 0
    duplicate_tx_hash = 0
    duplicate_candidate = 0
    for event in candidate_events:
        fingerprint = event.source_fingerprint
        tx_key = _event_tx_key(event)
        if tx_key is not None and tx_key in existing_tx_keys:
            duplicate_tx_hash += 1
            continue
        if fingerprint in existing_fingerprints:
            duplicate_source_fingerprint += 1
            continue
        if fingerprint in seen_fingerprints or (tx_key is not None and tx_key in seen_tx_keys):
            duplicate_candidate += 1
            continue
        new_events.append(event)
        seen_fingerprints.add(fingerprint)
        if tx_key is not None:
            seen_tx_keys.add(tx_key)
    return new_events, {
        "duplicate_source_fingerprint": duplicate_source_fingerprint,
        "duplicate_tx_hash": duplicate_tx_hash,
        "duplicate_candidate": duplicate_candidate,
    }


def _fetch_wallet_events(wallet: str, args: argparse.Namespace) -> tuple[str, list[WalletEvent], dict[str, Any]]:
    client = WalletHistoryClient(
        WalletSpec(
            name=_wallet_name(wallet),
            address=wallet,
            tags=("data_api_poll",),
            notes="Active-set Data API trade poll source.",
        ),
        timeout_s=float(getattr(args, "timeout_s", 2.0)),
        retries=int(getattr(args, "retries", 1)),
    )
    started = time.time()
    try:
        events = client.fetch_events(
            limit=int(getattr(args, "limit", 80)),
            include_activity=False,
            pages=int(getattr(args, "pages", 1)),
            parallel_sources=bool(getattr(args, "parallel_sources", True)),
            trade_query_keys=_trade_query_keys(getattr(args, "trade_query_keys", "user")),
        )
        tagged = [_tag_event_as_poll(event) for event in events if event.row_type == "trade"]
        report = client.last_fetch_report if isinstance(client.last_fetch_report, dict) else {}
        return wallet, tagged, {
            "status": "PASS",
            "duration_s": round(max(0.0, time.time() - started), 6),
            "raw_rows": int(report.get("raw_rows") or 0),
            "normalized_trade_events": int(report.get("normalized_trade_events") or 0),
            "fresh_buy_rows_le_10s_by_source": report.get("fresh_buy_rows_le_10s_by_source") or {},
            "freshest_buy_lag_s_by_source": report.get("freshest_buy_lag_s_by_source") or {},
            "trade_query_keys": list(_trade_query_keys(getattr(args, "trade_query_keys", "user"))),
            "blockers": report.get("blockers") or [],
            "api_errors": report.get("api_errors") or [],
        }
    except Exception as exc:  # pragma: no cover - live telemetry path.
        return wallet, [], {
            "status": "ERROR",
            "duration_s": round(max(0.0, time.time() - started), 6),
            "error": f"{type(exc).__name__}: {exc}",
            "trade_query_keys": list(_trade_query_keys(getattr(args, "trade_query_keys", "user"))),
        }


def _load_history(path: str) -> dict[str, Any]:
    existing = load_json(path, default={})
    if not isinstance(existing, dict) or existing.get("kind") != "wallet_copy_history_state":
        return {
            "schema_version": 1,
            "kind": "wallet_copy_history_state",
            "paper_only": True,
            "live_orders_allowed": False,
            "events": [],
            "copy_intents": [],
            "wallets": [],
            "wallet_results": [],
        }
    return existing


def _history_events(existing: dict[str, Any]) -> list[WalletEvent]:
    out: list[WalletEvent] = []
    for row in existing.get("events") or []:
        if not isinstance(row, dict):
            continue
        try:
            out.append(WalletEvent.from_dict(row))
        except TypeError:
            continue
    return out


def _retain_tail(rows: list[dict[str, Any]], retain: int) -> list[dict[str, Any]]:
    return rows if int(retain) <= 0 else rows[-int(retain) :]


def _event_sort_key(row: dict[str, Any]) -> tuple[float, str]:
    try:
        event_ts = float(row.get("event_ts") or 0.0)
    except (TypeError, ValueError):
        event_ts = 0.0
    return event_ts, str(row.get("event_id") or row.get("source_fingerprint") or "")


def _event_age_s(event: WalletEvent, reference_ts: float) -> float | None:
    if event.event_ts is None:
        return None
    return max(0.0, float(reference_ts) - float(event.event_ts))


def _candidate_policy_from_mapping(raw: Any) -> CandidatePolicy | None:
    if not isinstance(raw, dict):
        return None
    policy_id = str(raw.get("policy_id") or "")
    if not policy_id:
        return None
    return CandidatePolicy(
        policy_id=policy_id,
        min_price=float(raw.get("min_price") if raw.get("min_price") is not None else 0.01),
        max_price=float(raw.get("max_price") if raw.get("max_price") is not None else 1.0),
        min_wallet_usdc=float(raw.get("min_wallet_usdc") if raw.get("min_wallet_usdc") is not None else 0.0),
        max_wallet_usdc=float(raw.get("max_wallet_usdc") if raw.get("max_wallet_usdc") is not None else 0.0),
        min_seconds_from_open=(
            None if raw.get("min_seconds_from_open") is None else float(raw.get("min_seconds_from_open"))
        ),
        max_seconds_from_open=(
            None if raw.get("max_seconds_from_open") is None else float(raw.get("max_seconds_from_open"))
        ),
        wallet_fraction=float(raw.get("wallet_fraction") if raw.get("wallet_fraction") is not None else 0.05),
        max_order_usd=float(raw.get("max_order_usd") if raw.get("max_order_usd") is not None else 2.0),
        min_order_usd=float(raw.get("min_order_usd") if raw.get("min_order_usd") is not None else 0.0),
    )


def _policy_by_wallet_from_args(args: argparse.Namespace) -> dict[str, CandidatePolicy]:
    raw = getattr(args, "active_set_policy_by_wallet", {})
    raw = raw if isinstance(raw, dict) else {}
    policies: dict[str, CandidatePolicy] = {}
    for wallet, policy_row in raw.items():
        normalized = _norm_wallet(wallet)
        policy = _candidate_policy_from_mapping(policy_row)
        if normalized and policy is not None:
            policies[normalized] = policy
    return policies


def _policy_feedback_by_wallet(
    events: list[WalletEvent],
    *,
    policies: dict[str, CandidatePolicy],
    reference_ts: float,
    max_event_age_s: float,
) -> dict[str, dict[str, Any]]:
    if not policies:
        return {}
    rows: dict[str, dict[str, Any]] = {
        wallet: {
            "policy_id": policy.policy_id,
            "policy_compatible_fresh_buy_rows_le_30s": 0,
            "freshest_policy_compatible_buy_lag_s": None,
            "policy_reject_counts_fresh_buy_le_30s": {},
        }
        for wallet, policy in policies.items()
    }
    reject_counts: dict[str, Counter[str]] = {wallet: Counter() for wallet in policies}
    threshold_s = max(0.0, float(max_event_age_s))
    for event in events:
        wallet = _norm_wallet(event.source_wallet)
        policy = policies.get(wallet)
        if policy is None or event.action.upper() != "BUY":
            continue
        age = _event_age_s(event, reference_ts)
        if age is None or (threshold_s > 0 and age > threshold_s):
            continue
        accepted, reason = policy_accepts_event(policy, event)
        if not accepted:
            reject_counts[wallet][reason] += 1
            continue
        row = rows[wallet]
        row["policy_compatible_fresh_buy_rows_le_30s"] = int(row["policy_compatible_fresh_buy_rows_le_30s"]) + 1
        lag = round(float(age), 6)
        prior_lag = row.get("freshest_policy_compatible_buy_lag_s")
        row["freshest_policy_compatible_buy_lag_s"] = lag if prior_lag is None else min(float(prior_lag), lag)
    for wallet, counts in reject_counts.items():
        rows[wallet]["policy_reject_counts_fresh_buy_le_30s"] = dict(sorted(counts.items()))
    return rows


def _append_dataapi_first_seen_rows(
    path: str,
    events: list[WalletEvent],
    *,
    observed_reference_ts: float,
    poll_interval_s: float,
    max_event_age_s: float,
) -> int:
    if not path:
        return 0
    rows: list[dict[str, Any]] = []
    for event in events:
        wallet = _norm_wallet(event.source_wallet)
        tx = str(event.transaction_hash or "").strip().lower()
        if not wallet or not tx:
            continue
        observed_ts = float(event.observed_ts or observed_reference_ts)
        event_ts = float(event.event_ts) if event.event_ts is not None else None
        age_s = None if event_ts is None else max(0.0, observed_ts - event_ts)
        rows.append(
            {
                "event": "dataapi_first_seen",
                "captured_at_s": observed_ts,
                "captured_at_iso": utc_now_iso(),
                "wallet": wallet,
                "transactionHash": tx,
                "poller_started_at_s": observed_reference_ts,
                "poll_interval_s": float(poll_interval_s),
                "backfill": False,
                "dataapi_event_age_s": None if age_s is None else round(age_s, 6),
                "max_event_age_s": float(max_event_age_s),
                "endpoint": "active_set_dataapi_poller",
                "query_key": "active_set",
                "timestamp": event.event_ts,
                "observed_ts": event.observed_ts,
                "source": "polymarket_data_api_poll",
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    if rows:
        append_jsonl_many(path, rows)
    return len(rows)


def _write_observation_watermarks(
    path: str,
    *,
    wallets: list[str],
    fetched_events: list[WalletEvent],
    new_events: list[WalletEvent],
    generated_at: str,
    completed_ts: float,
) -> dict[str, Any]:
    if not path:
        return {"enabled": False, "updated_wallets": 0}
    existing = load_json(path, default={})
    existing = existing if isinstance(existing, dict) else {}
    prior_wallets = existing.get("wallets") if isinstance(existing.get("wallets"), dict) else {}
    rows_by_wallet: dict[str, list[WalletEvent]] = {wallet: [] for wallet in wallets}
    for event in fetched_events:
        wallet = _norm_wallet(event.source_wallet)
        if wallet in rows_by_wallet:
            rows_by_wallet[wallet].append(event)
    new_by_wallet = Counter(_norm_wallet(event.source_wallet) for event in new_events)
    wallet_rows = {str(wallet).lower(): row for wallet, row in prior_wallets.items() if isinstance(row, dict)}
    updated_wallets = 0
    for wallet, events in rows_by_wallet.items():
        if not events:
            continue
        latest = max(events, key=lambda event: (float(event.observed_ts or event.event_ts or 0.0), event.event_id))
        latest_observed_ts = float(latest.observed_ts or latest.event_ts or 0.0)
        latest_event_ts = float(latest.event_ts or 0.0)
        new_count = int(new_by_wallet.get(wallet) or 0)
        wallet_rows[wallet] = {
            **(wallet_rows.get(wallet) or {}),
            "generated_at": generated_at,
            "source_wallet": wallet,
            "latest_checked_ts": round(float(max(completed_ts, latest_observed_ts)), 6),
            "latest_matching_event_ts": round(latest_event_ts, 6) if latest_event_ts > 0 else None,
            "latest_matching_observed_ts": round(latest_observed_ts, 6) if latest_observed_ts > 0 else None,
            "retained_matching_rows": len(events),
            "new_matching_events": new_count,
            "history_write_skipped_safe": bool(new_count == 0),
            "observation_source": "polymarket_data_api_poll",
            "endpoint": "active_set_dataapi_poller",
            "paper_only": True,
            "live_orders_allowed": False,
        }
        updated_wallets += 1
    payload = {
        **existing,
        "kind": existing.get("kind") or "wallet_copy_rtds_observation_watermarks",
        "flow_stage": "LIVE/PROMOTE/SELF-DEV",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "wallets": wallet_rows,
        "dataapi_observation_watermark": {
            "updated_wallets": updated_wallets,
            "source_wallets": wallets,
            "rule": "Data API duplicate visibility refreshes observation freshness only; it creates no CopyIntent and submits no orders.",
        },
    }
    atomic_write_json(path, payload)
    return {
        "enabled": True,
        "path": path,
        "updated_wallets": updated_wallets,
        "source_wallets": wallets,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _skip_interval(args: argparse.Namespace, *, now_ts: float) -> dict[str, Any] | None:
    if bool(getattr(args, "force", False)):
        return None
    interval_s = float(getattr(args, "poll_interval_s", 0.0) or 0.0)
    if interval_s <= 0:
        return None
    prior = load_json(getattr(args, "state", DEFAULT_STATE), default={})
    prior = prior if isinstance(prior, dict) else {}
    last_poll_ts = float(prior.get("last_poll_ts") or 0.0)
    if last_poll_ts <= 0 or now_ts - last_poll_ts >= interval_s:
        return None
    summary = prior.get("summary") if isinstance(prior.get("summary"), dict) else {}
    return {
        "status": "SKIPPED_INTERVAL",
        "kind": "wallet_copy_active_set_dataapi_poller",
        "generated_at": utc_now_iso(),
        "last_poll_ts": last_poll_ts,
        "next_poll_due_s": round(max(0.0, interval_s - (now_ts - last_poll_ts)), 6),
        "summary": summary,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def run_poll(args: argparse.Namespace, *, source_wallets: list[str] | None = None) -> dict[str, Any]:
    now_ts = time.time()
    interval_skip = _skip_interval(args, now_ts=now_ts)
    if interval_skip is not None:
        return interval_skip
    wallets = source_wallets if source_wallets is not None else source_wallets_from_args(args)
    wallets = [wallet for wallet in dict.fromkeys(_norm_wallet(wallet) for wallet in wallets) if wallet]
    generated_at = utc_now_iso()
    if not wallets:
        summary = {
            "status": "SKIPPED_NO_WALLETS",
            "kind": "wallet_copy_active_set_dataapi_poller",
            "generated_at": generated_at,
            "summary": {"active_set_wallets": 0, "poll_only_signals": 0},
            "paper_only": True,
            "live_orders_allowed": False,
        }
        atomic_write_json(getattr(args, "state", DEFAULT_STATE), {**summary, "last_poll_ts": now_ts})
        return summary

    started = time.time()
    fetched_events: list[WalletEvent] = []
    fetch_meta: dict[str, Any] = {}
    max_workers = max(1, min(int(getattr(args, "max_workers", 8) or 1), len(wallets)))
    source_base_overrides_disabled = bool(getattr(args, "disable_source_base_overrides", True))
    with _disabled_source_base_overrides(source_base_overrides_disabled):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_wallet_events, wallet, args): wallet for wallet in wallets}
            for future in as_completed(futures):
                wallet, events, meta = future.result()
                fetched_events.extend(events)
                fetch_meta[wallet] = meta
    policy_feedback = _policy_feedback_by_wallet(
        fetched_events,
        policies=_policy_by_wallet_from_args(args),
        reference_ts=now_ts,
        max_event_age_s=float(getattr(args, "max_event_age_s", 300.0)),
    )
    for wallet, feedback in policy_feedback.items():
        fetch_meta.setdefault(wallet, {})["policy_feedback"] = feedback

    existing = _load_history(getattr(args, "history_state", "data/research/wallet_copy_history_state.json"))
    existing_events = _history_events(existing)
    new_events, duplicate_counts = _events_new_to_history(existing_events, fetched_events)
    merged_events = sorted(unique_wallet_events([*existing_events, *new_events]), key=lambda event: (event.event_ts or 0.0, event.event_id))
    event_rows = _retain_tail([event.asdict() for event in merged_events], int(getattr(args, "history_retain_events", 250_000)))
    existing_wallet_rows = [
        row
        for row in existing.get("wallets") or []
        if isinstance(row, dict) and _norm_wallet(row.get("address")) not in set(wallets)
    ]
    wallet_rows = [
        *existing_wallet_rows,
        *[
            {
                "name": _wallet_name(wallet),
                "address": wallet,
                "enabled": True,
                "market_filter": "btc_5m",
                "asset_allowlist": ["BTC"],
                "tags": ["data_api_poll", "active_set"],
                "notes": "Merged from active-set Data API trade poller.",
            }
            for wallet in wallets
        ],
    ]
    poll_only_by_wallet = Counter(event.source_wallet.lower() for event in new_events)
    fresh_new_events = [
        event
        for event in new_events
        if (_event_age_s(event, time.time()) is not None and _event_age_s(event, time.time()) <= float(getattr(args, "max_event_age_s", 300.0)))
    ]
    fresh_poll_only_by_wallet = Counter(event.source_wallet.lower() for event in fresh_new_events)
    existing_wallet_set = {
        _norm_wallet(row.get("address"))
        for row in existing.get("wallets") or []
        if isinstance(row, dict)
    }
    skip_history_write = not new_events and all(wallet in existing_wallet_set for wallet in wallets)
    payload = dict(existing)
    payload.update(
        {
            "schema_version": 1,
            "kind": "wallet_copy_history_state",
            "generated_at": generated_at,
            "paper_only": True,
            "live_orders_allowed": False,
            "events": sorted(event_rows, key=_event_sort_key),
            "wallets": wallet_rows,
            "wallet_results": [
                *(row for row in existing.get("wallet_results") or [] if isinstance(row, dict)),
                {
                    "wallet": {"name": "active_set_dataapi_poll", "address": ",".join(wallets)},
                    "events": len(new_events),
                    "copy_intents": None,
                    "latest_event_ts": max((float(event.event_ts or 0.0) for event in new_events), default=0.0),
                    "source": "polymarket_data_api_poll_merge",
                },
            ][-10_000:],
            "dataapi_poll_ingest": {
                "source_wallets": wallets,
                "new_matching_events": len(new_events),
                "poll_only_signals": len(new_events),
                "fresh_poll_only_signals": len(fresh_new_events),
                "duplicate_counts": duplicate_counts,
                "poll_only_by_wallet": dict(sorted(poll_only_by_wallet.items())),
                "fresh_poll_only_by_wallet": dict(sorted(fresh_poll_only_by_wallet.items())),
                "fetch_meta": fetch_meta,
            },
        }
    )
    if not skip_history_write:
        atomic_write_json(getattr(args, "history_state", "data/research/wallet_copy_history_state.json"), payload)
        history_window_index = build_history_window_index(
            getattr(args, "history_state", "data/research/wallet_copy_history_state.json"),
            index_path=getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json"),
        )
    else:
        history_window_index = load_history_window_index(
            getattr(args, "history_state", "data/research/wallet_copy_history_state.json"),
            index_path=getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json"),
        )
    append_jsonl_many(
        getattr(args, "wallet_event_log", "data/research/wallet_copy_live_guard_wallet_events.jsonl"),
        [
            {
                "event": "wallet_copy_wallet_event",
                "generated_at": generated_at,
                **event.asdict(),
            }
            for event in new_events
        ],
    )
    completed = time.time()
    first_seen_rows = _append_dataapi_first_seen_rows(
        getattr(args, "dataapi_first_seen_jsonl", ""),
        new_events,
        observed_reference_ts=completed,
        poll_interval_s=float(getattr(args, "poll_interval_s", 5.0)),
        max_event_age_s=float(getattr(args, "max_event_age_s", 300.0)),
    )
    observation_watermark = _write_observation_watermarks(
        getattr(args, "observation_watermark_state", DEFAULT_OBSERVATION_WATERMARK_STATE),
        wallets=wallets,
        fetched_events=fetched_events,
        new_events=new_events,
        generated_at=generated_at,
        completed_ts=completed,
    )
    errored_wallets = [
        wallet
        for wallet, meta in fetch_meta.items()
        if isinstance(meta, dict) and str(meta.get("status") or "") == "ERROR"
    ]
    summary = {
        "status": "PARTIAL" if errored_wallets and new_events else "ERROR" if errored_wallets else "PASS" if new_events else "ANALYZE",
        "kind": "wallet_copy_active_set_dataapi_poller",
        "flow_stage": "LIVE/PROMOTE/SELF-DEV",
        "generated_at": generated_at,
        "duration_s": round(max(0.0, completed - started), 6),
        "last_poll_ts": completed,
        "source_wallets": wallets,
        "source_base_overrides_disabled": source_base_overrides_disabled,
        "history_state": getattr(args, "history_state", "data/research/wallet_copy_history_state.json"),
        "history_window_index": {
            "path": getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json"),
            "indexed_rows": int(history_window_index.get("indexed_rows") or 0) if isinstance(history_window_index, dict) else 0,
            "skipped_rows": int(history_window_index.get("skipped_rows") or 0) if isinstance(history_window_index, dict) else 0,
            "windows": len(history_window_index.get("windows") or {}) if isinstance(history_window_index, dict) else 0,
            "rebuilt": not skip_history_write,
        },
        "summary": {
            "active_set_wallets": len(wallets),
            "events_fetched": len(fetched_events),
            "poll_only_signals": len(new_events),
            "fresh_poll_only_signals": len(fresh_new_events),
            "duplicate_counts": duplicate_counts,
            "poll_only_by_wallet": dict(sorted(poll_only_by_wallet.items())),
            "fresh_poll_only_by_wallet": dict(sorted(fresh_poll_only_by_wallet.items())),
            "errored_wallets": errored_wallets,
            "history_write_skipped": skip_history_write,
            "source_base_overrides_disabled": source_base_overrides_disabled,
            "dataapi_first_seen_rows": first_seen_rows,
            "observation_watermark_updated_wallets": int(observation_watermark.get("updated_wallets") or 0),
        },
        "observation_watermark": observation_watermark,
        "fetch_meta": fetch_meta,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    atomic_write_json(getattr(args, "state", DEFAULT_STATE), {**summary, "last_poll_ts": completed})
    return summary


def main() -> int:
    summary = run_poll(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if summary.get("status") != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
