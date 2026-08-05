#!/usr/bin/env python3
"""Run the wallet-copy pipeline in paper mode.

Default path:
1. fetch target wallet Polymarket history
2. normalize wallet events
3. build deterministic copy intents
4. apply the same intents to the paper order lifecycle engine

Live execution is intentionally not enabled here. The live adapter consumes the
same CopyIntent objects, but requires an explicit operator gate and CLOB token
mapping outside this paper-first reset script.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.ingest import WalletHistoryClient
from src.wallet_copy.models import SizingPolicy, WalletEvent, WalletSpec, utc_now_iso
from src.wallet_copy.paper import PaperExecutionConfig, PaperWalletCopyEngine
from src.wallet_copy.profit_engine import build_history_window_index
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json
from src.wallet_copy.strategy import CopyPolicy, WEIRD_PEAK_WALLET, build_intents, wallet_spec_from_mapping
from src.wallet_copy.weird_peak import run_weird_peak_exact_copy_paper_once


COMPACT_RAW_EVENT_KEYS = {
    "proxyWallet",
    "proxy_wallet",
    "wallet",
    "walletAddress",
    "wallet_address",
    "user",
    "marketSlug",
    "market_slug",
    "eventSlug",
    "event_slug",
    "slug",
    "title",
    "question",
    "timestamp",
    "createdAt",
    "created_at",
    "time",
    "date",
    "type",
    "activityType",
    "side",
    "action",
    "transactionType",
    "takerSide",
    "makerSide",
    "conditionId",
    "condition_id",
    "market",
    "marketId",
    "market_id",
    "outcome",
    "outcomeName",
    "tokenOutcome",
    "outcomeIndex",
    "outcome_index",
    "price",
    "avgPrice",
    "limitPrice",
    "size",
    "shares",
    "amount",
    "usdcSize",
    "usdc_size",
    "notional",
    "value",
    "asset",
    "tokenId",
    "token_id",
    "clobTokenId",
    "transactionHash",
    "transaction_hash",
    "txHash",
    "_walletCopySource",
    "_walletCopyQueryKey",
    "_walletCopySourceFetchStartedTs",
    "_walletCopySourceFetchCompletedTs",
    "_walletCopySourceFetchDurationS",
    "_walletCopySourceFetchOffset",
    "_walletCopySourceFetchLimit",
    "_walletCopySourceFetchSource",
    "_walletCopySourceRouteStatus",
    "_walletCopySourceRouteHost",
    "_walletCopySourceRouteOriginalHost",
    "_walletCopySourceRouteRoutedHost",
    "_walletCopySourceBaseOverrideConfigured",
    "_walletCopySourceBaseOverrideEnvVar",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", default=WEIRD_PEAK_WALLET)
    parser.add_argument("--wallet-name", default="weird_peak")
    parser.add_argument("--wallets-config", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--include-activity", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--data-api-timeout-s", type=float, default=8.0)
    parser.add_argument("--data-api-connect-timeout-s", type=float, default=10.0)
    parser.add_argument("--data-api-read-timeout-s", type=float, default=0.0)
    parser.add_argument("--data-api-retries", type=int, default=2)
    parser.add_argument("--parallel-data-api-sources", action="store_true")
    parser.add_argument(
        "--data-api-trade-query-keys",
        default="user,proxyWallet",
        help="Comma-separated trade query keys to fetch from the Data API: user, proxyWallet, or both.",
    )
    parser.add_argument("--market-filter", default="btc_5m")
    parser.add_argument("--asset", action="append", default=["BTC"])
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--history-window-index", default="data/research/wallet_copy_history_window_index.json")
    parser.add_argument("--wallet-event-log", default="data/research/wallet_copy_events.jsonl")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_paper_events.jsonl")
    parser.add_argument(
        "--merge-history-state",
        action="store_true",
        help=(
            "merge fetched events/intents into an existing history state instead of replacing it; "
            "used by resumable deep wallet coverage"
        ),
    )
    parser.add_argument("--reset-paper-state", action="store_true")
    parser.add_argument("--wallet-fraction", type=float, default=1.0)
    parser.add_argument("--fixed-usd", type=float, default=1.0)
    parser.add_argument("--max-order-usd", type=float, default=0.0)
    parser.add_argument("--min-order-usd", type=float, default=0.0)
    parser.add_argument("--min-price", type=float, default=0.01)
    parser.add_argument("--max-price", type=float, default=1.0)
    parser.add_argument("--max-event-age-s", type=float, default=0.0)
    parser.add_argument("--sizing-basis", choices=["wallet_usdc_fraction", "fixed_usd"], default="wallet_usdc_fraction")
    parser.add_argument("--policy-id", default="exact_copy_all_buys")
    parser.add_argument("--sizing-policy-id", default="")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--explicit-live-operator-go", action="store_true")
    parser.add_argument("--live-orders-allowed", action="store_true")
    parser.add_argument("--history-retain-events", type=int, default=250_000)
    parser.add_argument("--history-retain-copy-intents", type=int, default=250_000)
    parser.add_argument("--history-retain-wallet-reports", type=int, default=10_000)
    parser.add_argument("--paper-retain-orders", type=int, default=50_000)
    parser.add_argument("--paper-retain-lifecycle-events", type=int, default=150_000)
    parser.add_argument(
        "--weird-peak-exact-flow",
        action="store_true",
        help="run the existing low-latency Weird-Peak exact paper flow with tracker/fast-preconfirm support",
    )
    parser.add_argument("--weird-peak-tracker-state", default="data/research/weird_peak_exact_copy_wallet_tracker_state.json")
    parser.add_argument("--weird-peak-tracker-event-log", default="data/research/weird_peak_exact_copy_wallet_tracker_events.jsonl")
    parser.add_argument("--weird-peak-paper-state", default="data/research/weird_peak_exact_copy_paper_flow_state.json")
    parser.add_argument("--weird-peak-paper-event-log", default="data/research/weird_peak_exact_copy_paper_flow_events.jsonl")
    parser.add_argument("--disable-fast-preconfirm", action="store_true")
    return parser.parse_args()


def load_wallet_specs(args: argparse.Namespace) -> list[WalletSpec]:
    if args.wallets_config:
        payload = json.loads(Path(args.wallets_config).read_text(encoding="utf-8"))
        rows = payload.get("wallets") if isinstance(payload, dict) else payload
        return [wallet_spec_from_mapping(row) for row in rows if isinstance(row, dict)]
    return [
        WalletSpec(
            name=args.wallet_name,
            address=args.wallet,
            market_filter=args.market_filter,
            asset_allowlist=tuple(str(asset).upper() for asset in args.asset if asset),
        )
    ]


def _unique_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    no_key_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = row.get(key)
        if value:
            merged[str(value)] = row
        else:
            no_key_rows.append(row)
    return [*no_key_rows, *merged.values()]


def _compact_event_payload(event: WalletEvent) -> dict[str, Any]:
    payload = event.asdict()
    raw = payload.get("raw")
    if isinstance(raw, dict):
        payload["raw"] = {key: raw.get(key) for key in COMPACT_RAW_EVENT_KEYS if key in raw}
    else:
        payload["raw"] = {}
    return payload


def _compact_event_payloads(events: list[WalletEvent]) -> list[dict[str, Any]]:
    return [_compact_event_payload(event) for event in events]


def _retain_tail(rows: list[dict[str, Any]], retain: int) -> list[dict[str, Any]]:
    if int(retain) <= 0:
        return rows
    return rows[-int(retain):]


def _event_sort_key(row: dict[str, Any]) -> tuple[float, str]:
    try:
        event_ts = float(row.get("event_ts") or 0.0)
    except (TypeError, ValueError):
        event_ts = 0.0
    return event_ts, str(row.get("event_id") or row.get("source_fingerprint") or "")


def _intent_sort_key(row: dict[str, Any]) -> tuple[float, str]:
    try:
        event_ts = float(row.get("source_event_ts") or row.get("created_ts") or 0.0)
    except (TypeError, ValueError):
        event_ts = 0.0
    return event_ts, str(row.get("intent_id") or "")


def _wallet_address_from_result(row: dict[str, Any]) -> str:
    wallet = row.get("wallet") if isinstance(row.get("wallet"), dict) else {}
    return str(wallet.get("address") or row.get("address") or "").lower()


def _copy_intent_acceptance_summary(
    events: list[WalletEvent],
    *,
    policy: CopyPolicy,
    now_ts: float,
) -> dict[str, Any]:
    buy_events = [event for event in events if event.is_buy]
    accepted = 0
    reject_reasons: dict[str, int] = {}
    latest_buy_ts = max((float(event.event_ts) for event in buy_events if event.event_ts is not None), default=None)
    latest_buy_lag_s = round(max(0.0, float(now_ts) - latest_buy_ts), 6) if latest_buy_ts is not None else None
    fresh_buy_rows_le_10s = 0
    fresh_buy_rows_le_30s = 0
    for event in buy_events:
        if event.event_ts is not None:
            age_s = max(0.0, float(now_ts) - float(event.event_ts))
            if age_s <= 10.0:
                fresh_buy_rows_le_10s += 1
            if age_s <= 30.0:
                fresh_buy_rows_le_30s += 1
        ok, reason = policy.accepts(event, now_ts=now_ts)
        if ok:
            accepted += 1
        else:
            reject_reasons[str(reason)] = int(reject_reasons.get(str(reason), 0)) + 1
    return {
        "buy_events": len(buy_events),
        "accepted_buy_events": accepted,
        "rejected_buy_events": max(0, len(buy_events) - accepted),
        "reject_reasons": dict(sorted(reject_reasons.items())),
        "latest_buy_event_ts": latest_buy_ts,
        "latest_buy_lag_s": latest_buy_lag_s,
        "fresh_buy_rows_le_10s": fresh_buy_rows_le_10s,
        "fresh_buy_rows_le_30s": fresh_buy_rows_le_30s,
    }


def _wallet_results_from_merged_rows(
    *,
    wallet_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    intent_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events_by_wallet: dict[str, list[dict[str, Any]]] = {}
    intents_by_wallet: dict[str, int] = {}
    for row in event_rows:
        wallet = str(row.get("source_wallet") or "").lower()
        if wallet:
            events_by_wallet.setdefault(wallet, []).append(row)
    for row in intent_rows:
        wallet = str(row.get("source_wallet") or "").lower()
        if wallet:
            intents_by_wallet[wallet] = int(intents_by_wallet.get(wallet, 0)) + 1

    results: list[dict[str, Any]] = []
    for wallet in wallet_rows:
        address = str(wallet.get("address") or "").lower()
        if not address:
            continue
        wallet_events = events_by_wallet.get(address, [])
        results.append(
            {
                "wallet": wallet,
                "events": len(wallet_events),
                "copy_intents": int(intents_by_wallet.get(address, 0)),
                "latest_event_ts": max((float(row.get("event_ts") or 0.0) for row in wallet_events), default=0.0),
            }
        )
    return results


def _merge_history_payload(
    args: argparse.Namespace,
    *,
    base_payload: dict[str, Any],
    new_events: list[WalletEvent],
    new_intents: list[Any],
    wallet_results: list[dict[str, Any]],
    specs: list[WalletSpec],
    policy: CopyPolicy,
    ingest_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    existing = load_json(args.history_state, default={}) if args.merge_history_state else {}
    if not isinstance(existing, dict) or existing.get("kind") != "wallet_copy_history_state":
        existing = {}

    event_rows = _unique_by(
        [
            *(row for row in existing.get("events", []) if isinstance(row, dict)),
            *_compact_event_payloads(new_events),
        ],
        "event_id",
    )
    event_rows = _retain_tail(sorted(event_rows, key=_event_sort_key), int(getattr(args, "history_retain_events", 0)))
    intent_rows = _unique_by(
        [
            *(row for row in existing.get("copy_intents", []) if isinstance(row, dict)),
            *(intent.asdict() for intent in new_intents),
        ],
        "intent_id",
    )
    intent_rows = _retain_tail(
        sorted(intent_rows, key=_intent_sort_key),
        int(getattr(args, "history_retain_copy_intents", 0)),
    )
    wallet_rows = _unique_by(
        [
            *(row for row in existing.get("wallets", []) if isinstance(row, dict)),
            *(spec.asdict() for spec in specs),
        ],
        "address",
    )
    existing_ingest = existing.get("ingest") if isinstance(existing.get("ingest"), dict) else {}
    existing_reports = [row for row in existing_ingest.get("wallet_reports", []) if isinstance(row, dict)]
    merged_reports = _retain_tail(
        [*existing_reports, *ingest_reports],
        int(getattr(args, "history_retain_wallet_reports", 0)),
    )
    offsets_seen = sorted(
        {
            int(row.get("last_offset") or row.get("offset") or 0)
            for row in merged_reports
            if isinstance(row, dict)
        }
    )

    payload = dict(base_payload)
    payload.update(
        {
            "events": event_rows,
            "copy_intents": intent_rows,
            "wallets": wallet_rows,
            "wallet_results": _wallet_results_from_merged_rows(
                wallet_rows=wallet_rows,
                event_rows=event_rows,
                intent_rows=intent_rows,
            ),
            "ingest": {
                "limit": int(args.limit),
                "pages": int(args.pages),
                "offset": int(args.offset),
                "include_activity": bool(args.include_activity),
                "dedupe_key": "WalletEvent.source_fingerprint",
                "wallet_reports": merged_reports,
                "merge_history_state": bool(args.merge_history_state),
                "merged_event_count": len(event_rows),
                "merged_copy_intent_count": len(intent_rows),
                "merged_wallet_count": len(wallet_rows),
                "retention": {
                    "history_retain_events": int(getattr(args, "history_retain_events", 0)),
                    "history_retain_copy_intents": int(getattr(args, "history_retain_copy_intents", 0)),
                    "history_retain_wallet_reports": int(getattr(args, "history_retain_wallet_reports", 0)),
                    "full_pre_compact_state_archived": True,
                },
                "offsets_seen": offsets_seen,
                "latest_slice": {
                    "offset": int(args.offset),
                    "pages": int(args.pages),
                    "limit": int(args.limit),
                    "events": len(new_events),
                    "copy_intents": len(new_intents),
                },
                "policy_id": policy.policy_id,
            },
        }
    )
    return payload


def build_policy(args: argparse.Namespace) -> CopyPolicy:
    sizing_policy_id = args.sizing_policy_id or (
        f"wallet_fraction_{args.wallet_fraction:g}_cap_{args.max_order_usd:g}"
        if args.sizing_basis == "wallet_usdc_fraction"
        else f"fixed_usd_{args.fixed_usd:g}_cap_{args.max_order_usd:g}"
    )
    return CopyPolicy(
        policy_id=args.policy_id,
        allowed_assets=tuple(str(asset).upper() for asset in args.asset if asset),
        market_filter=args.market_filter,
        min_price=args.min_price,
        max_price=args.max_price,
        max_event_age_s=args.max_event_age_s,
        sizing=SizingPolicy(
            policy_id=sizing_policy_id,
            basis=args.sizing_basis,
            wallet_fraction=args.wallet_fraction,
            fixed_usd=args.fixed_usd,
            max_order_usd=args.max_order_usd,
            min_order_usd=args.min_order_usd,
        ),
    )


def compact_paper_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": state.get("kind"),
        "paper_only": state.get("paper_only"),
        "live_orders_allowed": state.get("live_orders_allowed"),
        "summary": state.get("summary"),
    }


def _data_api_skip_rows(ingest_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for report in ingest_reports:
        wallet = str(report.get("wallet") or "").strip().lower()
        wallet_name = str(report.get("wallet_name") or "")
        for raw in report.get("data_api_skip_rows") or []:
            if isinstance(raw, dict):
                rows.append({"wallet": wallet, "wallet_name": wallet_name, **raw})
    return rows


def run_generic(args: argparse.Namespace) -> dict[str, Any]:
    if args.mode == "live":
        if not args.explicit_live_operator_go or not args.live_orders_allowed:
            raise SystemExit("live wallet-copy is disabled: run paper first, then pass explicit live gates to the live adapter")
        raise SystemExit("live execution is not wired in this reset script; use CopyExecutionAdapter with token maps")

    specs = [spec for spec in load_wallet_specs(args) if spec.enabled]
    policy = build_policy(args)
    all_events: list[WalletEvent] = []
    wallet_results = []
    ingest_reports = []
    now_ts = time.time()
    for spec in specs:
        legacy_data_api_timeout_s = float(getattr(args, "data_api_timeout_s", 8.0))
        data_api_connect_timeout_s = float(getattr(args, "data_api_connect_timeout_s", 10.0))
        data_api_read_timeout_s = float(getattr(args, "data_api_read_timeout_s", 0.0))
        client = WalletHistoryClient(
            spec,
            timeout_s=legacy_data_api_timeout_s,
            connect_timeout_s=min(10.0, max(0.001, data_api_connect_timeout_s)),
            read_timeout_s=min(
                30.0,
                max(
                    0.001,
                    data_api_read_timeout_s
                    if data_api_read_timeout_s > 0
                    else legacy_data_api_timeout_s,
                ),
            ),
            retries=max(1, min(3, int(args.data_api_retries))),
        )
        events = client.fetch_events(
            limit=args.limit,
            include_activity=args.include_activity,
            pages=args.pages,
            offset=args.offset,
            parallel_sources=bool(args.parallel_data_api_sources),
            trade_query_keys=tuple(
                key.strip()
                for key in str(args.data_api_trade_query_keys or "").split(",")
                if key.strip()
            ),
        )
        intents = build_intents(events, policy=policy, mode="paper", now_ts=now_ts)
        acceptance_summary = _copy_intent_acceptance_summary(events, policy=policy, now_ts=now_ts)
        all_events.extend(events)
        ingest_report = dict(client.last_fetch_report)
        ingest_report["wallet"] = spec.normalized_address()
        ingest_report["wallet_name"] = spec.name
        ingest_report["normalized_events"] = len(events)
        ingest_report["copy_intent_acceptance"] = acceptance_summary
        ingest_reports.append(ingest_report)
        wallet_results.append(
            {
                "wallet": spec.asdict(),
                "events": len(events),
                "copy_intents": len(intents),
                "latest_event_ts": max((event.event_ts or 0.0 for event in events), default=0.0),
                "copy_intent_acceptance": acceptance_summary,
            }
        )

    all_intents = build_intents(all_events, policy=policy, mode="paper", now_ts=now_ts)
    history_events = _retain_tail(
        sorted(_compact_event_payloads(all_events), key=_event_sort_key),
        int(args.history_retain_events),
    )
    history_intents = _retain_tail(
        sorted([intent.asdict() for intent in all_intents], key=_intent_sort_key),
        int(args.history_retain_copy_intents),
    )
    base_history_payload = {
        "schema_version": 1,
        "kind": "wallet_copy_history_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallets": [spec.asdict() for spec in specs],
        "policy": {
            "policy_id": policy.policy_id,
            "sizing": policy.sizing.asdict(),
            "market_filter": policy.market_filter,
        },
        "events": history_events,
        "copy_intents": history_intents,
        "wallet_results": wallet_results,
        "ingest": {
            "limit": int(args.limit),
            "pages": int(args.pages),
            "offset": int(args.offset),
            "include_activity": bool(args.include_activity),
            "dedupe_key": "WalletEvent.source_fingerprint",
            "wallet_reports": _retain_tail(ingest_reports, int(args.history_retain_wallet_reports)),
            "retention": {
                "history_retain_events": int(args.history_retain_events),
                "history_retain_copy_intents": int(args.history_retain_copy_intents),
                "history_retain_wallet_reports": int(args.history_retain_wallet_reports),
                "full_pre_compact_state_archived": True,
            },
        },
    }
    if args.merge_history_state:
        base_history_payload = _merge_history_payload(
            args,
            base_payload=base_history_payload,
            new_events=all_events,
            new_intents=all_intents,
            wallet_results=wallet_results,
            specs=specs,
            policy=policy,
            ingest_reports=ingest_reports,
        )
    atomic_write_json(args.history_state, base_history_payload)
    history_window_index_path = str(
        getattr(args, "history_window_index", "data/research/wallet_copy_history_window_index.json")
    )
    history_window_index = build_history_window_index(args.history_state, index_path=history_window_index_path)
    append_jsonl_many(
        args.wallet_event_log,
        [
            {
                "event": "wallet_copy_wallet_event",
                "generated_at": utc_now_iso(),
                **_compact_event_payload(event),
            }
            for event in all_events
        ],
    )
    paper = PaperWalletCopyEngine(
        PaperExecutionConfig(
            state_path=args.paper_state,
            event_log_path=args.paper_event_log,
            reset_existing_state=bool(args.reset_paper_state),
            retain_orders=max(1, int(args.paper_retain_orders)),
            retain_lifecycle_events=max(1, int(args.paper_retain_lifecycle_events)),
        )
    )
    state = paper.apply_wallet_events_in_order(all_events, intents=all_intents)
    data_api_skip_rows = _data_api_skip_rows(ingest_reports)
    return {
        "generated_at": utc_now_iso(),
        "mode": "paper",
        "history_state": args.history_state,
        "history_window_index": {
            "path": history_window_index_path,
            "indexed_rows": int(history_window_index.get("indexed_rows") or 0),
            "skipped_rows": int(history_window_index.get("skipped_rows") or 0),
            "windows": len(history_window_index.get("windows") or {}),
        },
        "paper_state": args.paper_state,
        "wallet_results": wallet_results,
        "data_api_ingest_status": "PARTIAL_WITH_NAMED_SKIPS" if data_api_skip_rows else "PASS",
        "data_api_skip_rows": data_api_skip_rows,
        "data_api_skip_count": len(data_api_skip_rows),
        "data_api_timeout": {
            "connect_s": min(10.0, max(0.001, float(getattr(args, "data_api_connect_timeout_s", 10.0)))),
            "read_s": min(
                30.0,
                max(
                    0.001,
                    float(getattr(args, "data_api_read_timeout_s", 0.0))
                    if float(getattr(args, "data_api_read_timeout_s", 0.0)) > 0
                    else float(getattr(args, "data_api_timeout_s", 8.0)),
                ),
            ),
            "retries": max(1, min(3, int(getattr(args, "data_api_retries", 2)))),
        },
        "copy_intents": len(all_intents),
        "paper": compact_paper_state(state),
    }


def run_weird_peak(args: argparse.Namespace) -> dict[str, Any]:
    if args.mode != "paper":
        raise SystemExit("Weird-Peak exact flow is paper-only in the reset pipeline")
    state = run_weird_peak_exact_copy_paper_once(
        wallet=args.wallet,
        limit=args.limit,
        tracker_state_path=args.weird_peak_tracker_state,
        tracker_event_log_path=args.weird_peak_tracker_event_log,
        paper_state_path=args.weird_peak_paper_state,
        paper_event_log_path=args.weird_peak_paper_event_log,
        wallet_size_fraction=args.wallet_fraction,
        max_order_usd=args.max_order_usd,
        enable_fast_preconfirm=not args.disable_fast_preconfirm,
    )
    return {
        "generated_at": utc_now_iso(),
        "mode": "paper",
        "pipeline": "weird_peak_exact_flow",
        "wallet": args.wallet.lower(),
        "tracker_state": args.weird_peak_tracker_state,
        "paper_state": args.weird_peak_paper_state,
        "paper_only": state.get("paper_only"),
        "live_orders_allowed": state.get("live_orders_allowed"),
        "summary": state.get("summary"),
        "copy_contract": state.get("copy_contract"),
        "source_contract": state.get("source_contract"),
    }


def main() -> int:
    args = parse_args()
    payload = run_weird_peak(args) if args.weird_peak_exact_flow else run_generic(args)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
