#!/usr/bin/env python3
"""Split unobserved no-signal coverage gaps into idle source vs ingest gap."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_no_copy_signal_watcher_gap import (  # noqa: E402
    _btc_window_start_from_slug,
    _fetch_wallet_trades,
    _norm_wallet,
    _parse_ts,
)
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_COVERAGE_GAP = "data/research/coverage_gap_diagnosis_latest.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_HISTORY_STATE = "data/research/wallet_copy_history_state.json"
DEFAULT_OUTPUT = "data/research/coverage_gap_signal_supply_check_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-gap", default=DEFAULT_COVERAGE_GAP)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    return parser.parse_args()


def _window_iso(window_start_s: int) -> str:
    return datetime.fromtimestamp(int(window_start_s), tz=UTC).isoformat().replace("+00:00", "Z")


def _unobserved_no_signal_windows(coverage_gap: dict[str, Any]) -> list[dict[str, Any]]:
    rows = coverage_gap.get("rows") if isinstance(coverage_gap.get("rows"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("reason_class") != "no-eligible-signal":
            continue
        if bool(row.get("observed_guard_rollup")):
            continue
        try:
            window_start_s = int(float(row.get("window_start_s")))
        except (TypeError, ValueError):
            continue
        item = dict(row)
        item["window_start_s"] = window_start_s
        item["market_slug"] = str(item.get("market_slug") or f"btc-updown-5m-{window_start_s}")
        out.append(item)
    return sorted(out, key=lambda item: int(item["window_start_s"]))


def _fetch_incomplete(meta: dict[str, Any]) -> bool:
    return bool(meta.get("errored") or meta.get("truncated") or meta.get("error"))


def _active_roster_wallets(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    runtime = guard_state.get("active_set_runtime") if isinstance(guard_state.get("active_set_runtime"), dict) else {}
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    if not members:
        members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for member in members:
        if not isinstance(member, dict) or member.get("enabled") is False:
            continue
        wallet = _norm_wallet(member.get("source_wallet"))
        if not wallet or wallet in seen:
            continue
        seen.add(wallet)
        out.append(
            {
                "wallet": wallet,
                "candidate_id": str(member.get("candidate_id") or ""),
                "policy_id": str(member.get("policy_id") or ""),
                "role": str(member.get("role") or "member"),
            }
        )
    return out


def _btc5m_trades_by_window(wallet_trades: dict[str, list[dict[str, Any]]]) -> dict[int, list[dict[str, Any]]]:
    by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for wallet, trades in wallet_trades.items():
        normalized_wallet = _norm_wallet(wallet)
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            window_start_s = _btc_window_start_from_slug(str(trade.get("slug") or ""))
            if window_start_s is None:
                continue
            row = dict(trade)
            row["_wallet"] = normalized_wallet
            by_window[int(window_start_s)].append(row)
    return by_window


def _history_events_by_window(
    history_state: dict[str, Any],
    *,
    active_wallets: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    active_wallet_set = {str(member["wallet"]) for member in active_wallets}
    by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    events = history_state.get("events") if isinstance(history_state.get("events"), list) else []
    for event in events:
        if not isinstance(event, dict):
            continue
        wallet = _norm_wallet(event.get("source_wallet"))
        if wallet not in active_wallet_set:
            continue
        window_start_s = _btc_window_start_from_slug(str(event.get("market_slug") or ""))
        if window_start_s is None:
            continue
        by_window[int(window_start_s)].append(event)
    return by_window


def _history_sample(events: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for event in events[: int(limit)]:
        out.append(
            {
                "wallet": _norm_wallet(event.get("source_wallet")),
                "source": str(event.get("source") or ""),
                "action": str(event.get("action") or ""),
                "outcome": str(event.get("outcome") or ""),
                "price": event.get("price"),
                "event_ts": event.get("event_ts"),
                "observed_ts": event.get("observed_ts"),
                "tx": str(event.get("transaction_hash") or ""),
            }
        )
    return out


def _trade_sample(trades: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for trade in trades[: int(limit)]:
        out.append(
            {
                "wallet": _norm_wallet(trade.get("proxyWallet") or trade.get("_wallet")),
                "tx": str(trade.get("transactionHash") or ""),
                "side": str(trade.get("side") or ""),
                "outcome": str(trade.get("outcome") or ""),
                "size": trade.get("size"),
                "price": trade.get("price"),
                "timestamp": _parse_ts(trade.get("timestamp")),
            }
        )
    return out


def build_signal_supply_report(
    *,
    coverage_gap: dict[str, Any],
    guard_state: dict[str, Any],
    wallet_trades: dict[str, list[dict[str, Any]]],
    fetch_meta: dict[str, dict[str, Any]],
    history_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    windows = _unobserved_no_signal_windows(coverage_gap)
    active_wallets = _active_roster_wallets(guard_state)
    incomplete_wallets = sorted(wallet for wallet, meta in fetch_meta.items() if _fetch_incomplete(meta))
    fetch_complete = not incomplete_wallets
    trades_by_window = _btc5m_trades_by_window(wallet_trades)
    history_by_window = _history_events_by_window(history_state or {}, active_wallets=active_wallets)

    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    local_history_counts: Counter[str] = Counter()
    idle_hour_histogram: Counter[str] = Counter()
    traded_hour_histogram: Counter[str] = Counter()
    unknown_hour_histogram: Counter[str] = Counter()
    wallet_hit_counts: Counter[str] = Counter()

    for window in windows:
        window_start_s = int(window["window_start_s"])
        trades = trades_by_window.get(window_start_s, [])
        history_events = history_by_window.get(window_start_s, [])
        wallets = sorted({_norm_wallet(trade.get("proxyWallet") or trade.get("_wallet")) for trade in trades})
        wallets = [wallet for wallet in wallets if wallet]
        if trades:
            classification = "sources_traded_but_unobserved"
            traded_hour_histogram[f"{datetime.fromtimestamp(window_start_s, tz=UTC).hour:02d}"] += 1
            for wallet in wallets:
                wallet_hit_counts[wallet] += 1
            if history_events:
                local_history_counts["traded_unobserved_with_local_history_events"] += 1
            else:
                local_history_counts["traded_unobserved_without_local_history_events"] += 1
        elif fetch_complete:
            classification = "sources_idle"
            idle_hour_histogram[f"{datetime.fromtimestamp(window_start_s, tz=UTC).hour:02d}"] += 1
        else:
            classification = "unknown_fetch_incomplete"
            unknown_hour_histogram[f"{datetime.fromtimestamp(window_start_s, tz=UTC).hour:02d}"] += 1
        counts[classification] += 1
        rows.append(
            {
                "market_slug": str(window.get("market_slug") or f"btc-updown-5m-{window_start_s}"),
                "window_start_s": window_start_s,
                "window_start_iso": _window_iso(window_start_s),
                "classification": classification,
                "remote_btc5m_trade_count": len(trades),
                "active_wallets_with_remote_trades": wallets,
                "local_history_event_count": len(history_events),
                "local_history_sources": sorted({str(event.get("source") or "") for event in history_events if isinstance(event, dict)}),
                "fetch_complete": fetch_complete,
                "sample_trades": _trade_sample(trades),
                "sample_local_history_events": _history_sample(history_events),
            }
        )

    target_window_starts = {int(item["window_start_s"]) for item in windows}
    total_remote_btc_windows = set(trades_by_window) & target_window_starts
    active_wallet_rows: list[dict[str, Any]] = []
    for member in active_wallets:
        wallet = str(member["wallet"])
        wallet_windows = {
            int(start)
            for start, trades in trades_by_window.items()
            if int(start) in target_window_starts
            if any(_norm_wallet(trade.get("proxyWallet") or trade.get("_wallet")) == wallet for trade in trades)
        }
        active_wallet_rows.append(
            {
                "wallet": wallet,
                "candidate_id": member.get("candidate_id"),
                "policy_id": member.get("policy_id"),
                "remote_trade_windows_inside_unobserved": len(wallet_windows),
                "sample_windows": sorted(wallet_windows)[:20],
                "fetch_meta": fetch_meta.get(wallet, {}),
            }
        )

    if windows:
        dominant = max(
            ("sources_idle", "sources_traded_but_unobserved", "unknown_fetch_incomplete"),
            key=lambda key: (counts[key], key),
        )
    else:
        dominant = "none"
    traded_unobserved = int(counts["sources_traded_but_unobserved"])
    with_history = int(local_history_counts["traded_unobserved_with_local_history_events"])
    without_history = int(local_history_counts["traded_unobserved_without_local_history_events"])
    if not windows:
        root_cause = "no_unobserved_no_signal_windows_after_history_derived_coverage"
    elif traded_unobserved and with_history == traded_unobserved:
        root_cause = "participation_rollup_retention_gap_not_source_ingest"
    elif traded_unobserved and without_history > with_history:
        root_cause = "source_ingest_gap"
    elif traded_unobserved:
        root_cause = "mixed_history_and_ingest_gap"
    else:
        root_cause = "sources_idle_dominates"
    window_payload = coverage_gap.get("window") if isinstance(coverage_gap.get("window"), dict) else {}
    return {
        "schema_version": 1,
        "kind": "coverage_gap_signal_supply_check",
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "inputs": {
            "coverage_gap_kind": coverage_gap.get("kind"),
            "coverage_gap_generated_at": coverage_gap.get("generated_at"),
            "coverage_gap_window": window_payload,
        },
        "summary": {
            "unobserved_no_signal_windows": len(windows),
            "sources_idle_windows": int(counts["sources_idle"]),
            "sources_traded_but_unobserved_windows": int(counts["sources_traded_but_unobserved"]),
            "unknown_fetch_incomplete_windows": int(counts["unknown_fetch_incomplete"]),
            "dominant_class": dominant,
            "active_set_wallets": len(active_wallets),
            "fetch_complete": fetch_complete,
            "incomplete_wallets": incomplete_wallets,
            "remote_btc5m_trade_windows_inside_unobserved": len(total_remote_btc_windows),
            "local_history_btc5m_event_windows_inside_unobserved": len(set(history_by_window) & target_window_starts),
            "traded_unobserved_with_local_history_events": with_history,
            "traded_unobserved_without_local_history_events": without_history,
            "root_cause": root_cause,
            "root_cause_detail": (
                "Coverage diagnosis found no remaining unobserved no-signal windows after deriving active source "
                "activity from retained wallet history."
                if root_cause == "no_unobserved_no_signal_windows_after_history_derived_coverage"
                else "Remote DataAPI trade windows are already present in local wallet_copy_history_state; "
                "the earlier no-eligible-signal default came from coverage diagnosis reading compact "
                "participation rollups that retain only recent rows, not a missing source ingest path."
                if root_cause == "participation_rollup_retention_gap_not_source_ingest"
                else "Remote DataAPI trade windows are absent from local history for enough windows to require ingest root-cause."
                if root_cause in {"source_ingest_gap", "mixed_history_and_ingest_gap"}
                else "Most unobserved no-signal windows had no active-roster remote DataAPI BTC-5m trade."
            ),
            "idle_hour_utc_histogram": dict(sorted(idle_hour_histogram.items())),
            "traded_but_unobserved_hour_utc_histogram": dict(sorted(traded_hour_histogram.items())),
            "unknown_hour_utc_histogram": dict(sorted(unknown_hour_histogram.items())),
            "wallet_hit_counts": dict(sorted(wallet_hit_counts.items())),
            "classification_rule": (
                "remote DataAPI BTC-5m trade by any active roster wallet inside the unobserved window "
                "=> sources_traded_but_unobserved; otherwise sources_idle when all wallet fetches complete"
            ),
        },
        "active_wallets": active_wallet_rows,
        "rows": rows,
    }


def _fetch_roster_trades(
    *,
    active_wallets: list[dict[str, Any]],
    start_ts: float,
    end_ts: float,
    limit: int,
    max_pages: int,
    timeout_s: float,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    wallet_trades: dict[str, list[dict[str, Any]]] = {}
    fetch_meta: dict[str, dict[str, Any]] = {}
    for member in active_wallets:
        wallet = str(member["wallet"])
        trades, meta = _fetch_wallet_trades(
            wallet,
            start_ts=float(start_ts),
            end_ts=float(end_ts),
            limit=int(limit),
            max_pages=int(max_pages),
            timeout_s=float(timeout_s),
        )
        wallet_trades[wallet] = trades
        fetch_meta[wallet] = {key: value for key, value in meta.items() if key != "urls"}
    return wallet_trades, fetch_meta


def main() -> int:
    args = parse_args()
    coverage_gap = load_json(args.coverage_gap, default={})
    guard_state = load_json(args.guard_state, default={})
    history_state = load_json(args.history_state, default={})
    coverage_gap = coverage_gap if isinstance(coverage_gap, dict) else {}
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    history_state = history_state if isinstance(history_state, dict) else {}
    active_wallets = _active_roster_wallets(guard_state)
    window = coverage_gap.get("window") if isinstance(coverage_gap.get("window"), dict) else {}
    wallet_trades, fetch_meta = _fetch_roster_trades(
        active_wallets=active_wallets,
        start_ts=float(window.get("start_ts") or 0.0),
        end_ts=float(window.get("end_ts") or 0.0),
        limit=int(args.limit),
        max_pages=int(args.max_pages),
        timeout_s=float(args.timeout_s),
    )
    report = build_signal_supply_report(
        coverage_gap=coverage_gap,
        guard_state=guard_state,
        history_state=history_state,
        wallet_trades=wallet_trades,
        fetch_meta=fetch_meta,
    )
    report["sources"] = {
        "coverage_gap": args.coverage_gap,
        "guard_state": args.guard_state,
        "history_state": args.history_state,
        "remote_dataapi": "https://data-api.polymarket.com/trades?user=<wallet>",
        "limit": int(args.limit),
        "max_pages": int(args.max_pages),
        "timeout_s": float(args.timeout_s),
    }
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
