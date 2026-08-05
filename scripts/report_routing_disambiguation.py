#!/usr/bin/env python3
"""Disambiguate retained 4d8b signals that did not become live routed intents."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_coverage_gap_signal_supply_check import _active_roster_wallets  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_SIGNAL_SUPPLY = "data/research/coverage_gap_signal_supply_check_latest.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_HISTORY_STATE = "data/research/wallet_copy_history_state.json"
DEFAULT_LIVE_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_REALTIME_SHADOW = "data/research/wallet_copy_realtime_shadow_watch_scored_state.json"
DEFAULT_GUARD_SHADOW = "data/research/wallet_copy_guard_shadow_lanes_state.json"
DEFAULT_OUTPUT = "data/research/routing_disambiguation_latest.json"
DEFAULT_TARGET_WALLET = "0x4d8bc628487bbc9931b4d039e6a7529b8ae1a00d"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-supply", default=DEFAULT_SIGNAL_SUPPLY)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY_STATE)
    parser.add_argument("--live-ledger", default=DEFAULT_LIVE_LEDGER)
    parser.add_argument("--realtime-shadow", default=DEFAULT_REALTIME_SHADOW)
    parser.add_argument("--guard-shadow", default=DEFAULT_GUARD_SHADOW)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--target-wallet", default=DEFAULT_TARGET_WALLET)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _slug_start(slug: Any) -> int:
    text = str(slug or "")
    if not text.startswith("btc-updown-5m-"):
        return 0
    try:
        return int(text.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return 0


def _sample_evenly(rows: list[dict[str, Any]], sample_size: int) -> list[dict[str, Any]]:
    if sample_size <= 0 or len(rows) <= sample_size:
        return rows
    if sample_size == 1:
        return [rows[0]]
    last = len(rows) - 1
    indexes = [round(index * last / (sample_size - 1)) for index in range(sample_size)]
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    for index in indexes:
        if index not in seen:
            out.append(rows[index])
            seen.add(index)
    return out


def _window_iso(window_start_s: int) -> str:
    return datetime.fromtimestamp(window_start_s, tz=UTC).isoformat().replace("+00:00", "Z")


def _rows_for_wallet_slug(rows: list[dict[str, Any]], *, wallet: str, slug: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet") or row.get("proxyWallet"))
        if not source_wallet:
            intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            source_wallet = _norm_wallet(intent.get("source_wallet"))
        market_slug = str(row.get("market_slug") or "")
        if not market_slug:
            intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            market_slug = str(intent.get("market_slug") or "")
        if source_wallet == wallet and market_slug == slug:
            out.append(row)
    return out


def _history_indexes(history_state: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events = [row for row in history_state.get("events") or [] if isinstance(row, dict)]
    intents = [row for row in history_state.get("copy_intents") or [] if isinstance(row, dict)]
    return events, intents


def _realtime_wallets(realtime_shadow: dict[str, Any]) -> set[str]:
    wallets: set[str] = set()
    for row in realtime_shadow.get("events") or []:
        if isinstance(row, dict):
            wallet = _norm_wallet(row.get("wallet"))
            if wallet:
                wallets.add(wallet)
    return wallets


def _passive_wallets(realtime_watch_state: dict[str, Any]) -> set[str]:
    wallets: set[str] = set()
    for row in realtime_watch_state.get("passive_wallets") or []:
        if isinstance(row, dict):
            wallet = _norm_wallet(row.get("wallet"))
            if wallet:
                wallets.add(wallet)
    return wallets


def _event_sample(rows: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for row in rows[: int(limit)]:
        sample.append(
            {
                "source": row.get("source"),
                "action": row.get("action"),
                "outcome": row.get("outcome"),
                "price": row.get("price"),
                "event_ts": row.get("event_ts"),
                "observed_ts": row.get("observed_ts"),
                "tx": row.get("transaction_hash") or row.get("tx"),
            }
        )
    return sample


def build_report(
    *,
    signal_supply: dict[str, Any],
    guard_state: dict[str, Any],
    history_state: dict[str, Any],
    live_ledger: dict[str, Any],
    realtime_shadow: dict[str, Any],
    realtime_watch_state: dict[str, Any] | None = None,
    guard_shadow: dict[str, Any],
    target_wallet: str,
    sample_size: int,
) -> dict[str, Any]:
    wallet = _norm_wallet(target_wallet)
    candidate_rows = [
        row
        for row in signal_supply.get("rows") or []
        if isinstance(row, dict)
        and row.get("classification") == "sources_traded_but_unobserved"
        and wallet in {str(item).lower() for item in row.get("active_wallets_with_remote_trades") or []}
    ]
    candidate_rows = sorted(candidate_rows, key=lambda row: int(row.get("window_start_s") or 0))
    sampled = _sample_evenly(candidate_rows, int(sample_size))

    active_wallets = _active_roster_wallets(guard_state)
    runtime_member = next((row for row in active_wallets if row.get("wallet") == wallet), {})
    runtime = guard_state.get("active_set_runtime") if isinstance(guard_state.get("active_set_runtime"), dict) else {}
    selection = (
        runtime.get("fresh_runtime_member_selection")
        if isinstance(runtime.get("fresh_runtime_member_selection"), dict)
        else {}
    )
    selected_wallet = _norm_wallet(selection.get("selected_wallet") or runtime.get("selected_wallet"))
    premerge = guard_state.get("active_set_rtds_premerge") if isinstance(guard_state.get("active_set_rtds_premerge"), dict) else {}
    active_rtds_wallets = {
        _norm_wallet(row.get("source_wallet"))
        for row in premerge.get("rows") or []
        if isinstance(row, dict) and _norm_wallet(row.get("source_wallet"))
    }
    realtime_wallets = _realtime_wallets(realtime_shadow)
    passive_wallets = _passive_wallets(realtime_watch_state or {})
    history_events, history_intents = _history_indexes(history_state)
    live_orders = [row for row in live_ledger.get("orders") or [] if isinstance(row, dict)]
    guard_shadow_rows = [row for row in guard_shadow.get("rows") or [] if isinstance(row, dict)]

    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for source in sampled:
        slug = str(source.get("market_slug") or "")
        window_start_s = int(source.get("window_start_s") or _slug_start(slug))
        local_events = _rows_for_wallet_slug(history_events, wallet=wallet, slug=slug)
        copy_intents = _rows_for_wallet_slug(history_intents, wallet=wallet, slug=slug)
        live_wallet_orders = _rows_for_wallet_slug(live_orders, wallet=wallet, slug=slug)
        realtime_events = _rows_for_wallet_slug(
            [row for row in realtime_shadow.get("events") or [] if isinstance(row, dict)],
            wallet=wallet,
            slug=slug,
        )
        guard_shadow_hits = _rows_for_wallet_slug(guard_shadow_rows, wallet=wallet, slug=slug)

        if wallet not in active_rtds_wallets and wallet not in realtime_wallets and wallet not in passive_wallets:
            classification = "wallet-not-in-watched-source-set"
        elif copy_intents or live_wallet_orders or guard_shadow_hits:
            classification = "signal-emitted-but-filtered"
        elif local_events and selected_wallet and selected_wallet != wallet:
            classification = "signal-emitted-but-not-selected"
        elif local_events:
            classification = "signal-emitted-but-filtered"
        else:
            classification = "rows-aged-out-unknowable"
        counts[classification] += 1
        rows.append(
            {
                "market_slug": slug,
                "window_start_s": window_start_s,
                "window_start_iso": _window_iso(window_start_s) if window_start_s else "",
                "classification": classification,
                "target_wallet": wallet,
                "history_event_count": len(local_events),
                "history_buy_event_count": sum(1 for row in local_events if str(row.get("action") or "").upper() == "BUY"),
                "copy_intent_count": len(copy_intents),
                "live_order_count": len(live_wallet_orders),
                "realtime_shadow_event_count": len(realtime_events),
                "guard_shadow_row_count": len(guard_shadow_hits),
                "active_rtds_watch": wallet in active_rtds_wallets,
                "realtime_shadow_watch": wallet in realtime_wallets,
                "passive_watch_config": wallet in passive_wallets,
                "selected_wallet_at_report_time": selected_wallet,
                "runtime_member_enabled": bool(runtime_member),
                "runtime_member": runtime_member,
                "sample_history_events": _event_sample(local_events),
            }
        )

    return {
        "schema_version": 1,
        "kind": "routing_disambiguation",
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "target_wallet": wallet,
        "summary": {
            "candidate_windows": len(candidate_rows),
            "sampled_windows": len(rows),
            "class_counts": dict(sorted(counts.items())),
            "dominant_class": max(counts, key=lambda key: (counts[key], key)) if counts else "",
            "target_wallet_active_runtime_member": bool(runtime_member),
            "target_wallet_candidate_id": runtime_member.get("candidate_id"),
            "target_wallet_policy_id": runtime_member.get("policy_id"),
            "target_wallet_active_rtds_watch": wallet in active_rtds_wallets,
            "target_wallet_realtime_shadow_watch": wallet in realtime_wallets,
            "target_wallet_passive_watch_config": wallet in passive_wallets,
            "selection_mode": runtime.get("selection_mode"),
            "selected_wallet_at_report_time": selected_wallet,
            "runtime_status_note": (
                "target wallet is enabled in active_set_runtime; no runtime_auto_degrade exclusion observed"
                if runtime_member
                else "target wallet absent from active_set_runtime"
            ),
        },
        "rows": rows,
    }


def main() -> int:
    args = parse_args()
    report = build_report(
        signal_supply=load_json(args.signal_supply, default={}),
        guard_state=load_json(args.guard_state, default={}),
        history_state=load_json(args.history_state, default={}),
        live_ledger=load_json(args.live_ledger, default={}),
        realtime_shadow=load_json(args.realtime_shadow, default={}),
        realtime_watch_state=load_json("data/research/wallet_copy_realtime_shadow_watch_state.json", default={}),
        guard_shadow=load_json(args.guard_shadow, default={}),
        target_wallet=str(args.target_wallet),
        sample_size=int(args.sample_size),
    )
    report["sources"] = {
        "signal_supply": args.signal_supply,
        "guard_state": args.guard_state,
        "history_state": args.history_state,
        "live_ledger": args.live_ledger,
        "realtime_shadow": args.realtime_shadow,
        "guard_shadow": args.guard_shadow,
    }
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
