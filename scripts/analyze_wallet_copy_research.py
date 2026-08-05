#!/usr/bin/env python3
"""Analyze wallet-copy history, paper states, and multi-wallet consensus."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.consensus import ConsensusConfig, build_consensus_signals, consensus_signal_to_intent
from src.wallet_copy.features import build_feature_payload
from src.wallet_copy.inventory import InventoryConfig, build_inventory_plans, inventory_plan_to_intent
from src.wallet_copy.models import CopyIntent, WalletEvent, utc_now_iso
from src.wallet_copy.paper import PaperExecutionConfig, PaperWalletCopyEngine
from src.wallet_copy.performance import AdmissionConfig, score_paper_state
from src.wallet_copy.research import cross_wallet_windows, train_rows_from_events, wallet_event_summary
from src.wallet_copy.store import atomic_write_json, load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", action="append", default=[])
    parser.add_argument("--paper-state", action="append", default=[])
    parser.add_argument("--output", default="data/research/wallet_copy_research_state.json")
    parser.add_argument("--min-agreeing-wallets", type=int, default=2)
    parser.add_argument("--allow-opposing-wallets", action="store_true")
    parser.add_argument("--max-price-spread", type=float, default=0.08)
    parser.add_argument("--max-consensus-usd", type=float, default=0.0)
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--min-resolved-orders", type=int, default=30)
    parser.add_argument("--min-roi-pct", type=float, default=1.0)
    parser.add_argument("--min-wr-pct", type=float, default=70.0)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.5)
    parser.add_argument("--max-avg-latency-s", type=float, default=30.0)
    parser.add_argument("--inventory-min-wallets", type=int, default=2)
    parser.add_argument("--inventory-max-price-spread", type=float, default=0.08)
    parser.add_argument("--inventory-max-window-usd", type=float, default=10.0)
    parser.add_argument("--inventory-max-per-wallet-usd", type=float, default=2.0)
    parser.add_argument("--inventory-min-plan-usd", type=float, default=1.0)
    parser.add_argument("--inventory-allow-opposing-wallets", action="store_true")
    parser.add_argument("--inventory-paper-state", default="")
    parser.add_argument("--inventory-paper-event-log", default="data/research/wallet_copy_inventory_paper_events.jsonl")
    parser.add_argument("--print-full", action="store_true", help="print the full research payload instead of a compact summary")
    return parser.parse_args()


def load_events(path: str) -> list[WalletEvent]:
    payload = load_json(path, default={})
    rows = payload.get("events") if isinstance(payload, dict) else []
    events: list[WalletEvent] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            events.append(WalletEvent.from_dict(row))
        except TypeError:
            continue
    return events


def load_intents_from_history(path: str) -> list[CopyIntent]:
    payload = load_json(path, default={})
    rows = payload.get("copy_intents") if isinstance(payload, dict) else []
    intents: list[CopyIntent] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            intents.append(CopyIntent.from_dict(row))
        except TypeError:
            continue
    return intents


def load_intents_from_paper(path: str) -> list[CopyIntent]:
    payload = load_json(path, default={})
    rows = payload.get("orders") if isinstance(payload, dict) else []
    intents: list[CopyIntent] = []
    for order in rows or []:
        if not isinstance(order, dict):
            continue
        source_intent = order.get("source_intent")
        if not isinstance(source_intent, dict):
            continue
        try:
            intents.append(CopyIntent.from_dict(source_intent))
        except TypeError:
            continue
    return intents


def main() -> int:
    args = parse_args()
    events: list[WalletEvent] = []
    intents: list[CopyIntent] = []
    paper_orders: list[dict[str, Any]] = []
    paper_lifecycle_events: list[dict[str, Any]] = []
    for path in args.history_state:
        events.extend(load_events(path))
        intents.extend(load_intents_from_history(path))
    for path in args.paper_state:
        payload = load_json(path, default={})
        if isinstance(payload, dict):
            paper_orders.extend(row for row in payload.get("orders") or [] if isinstance(row, dict))
            paper_lifecycle_events.extend(row for row in payload.get("lifecycle_events") or [] if isinstance(row, dict))
        intents.extend(load_intents_from_paper(path))

    consensus = build_consensus_signals(
        intents,
        config=ConsensusConfig(
            min_agreeing_wallets=args.min_agreeing_wallets,
            allow_opposing_wallets=args.allow_opposing_wallets,
            max_price_spread=args.max_price_spread,
            max_consensus_usd=args.max_consensus_usd,
        ),
    )
    consensus_intents = [intent.asdict() for intent in (consensus_signal_to_intent(signal) for signal in consensus) if intent]
    inventory_plans = build_inventory_plans(
        intents,
        config=InventoryConfig(
            min_agreeing_wallets=args.inventory_min_wallets,
            allow_opposing_wallets=args.inventory_allow_opposing_wallets,
            max_price_spread=args.inventory_max_price_spread,
            max_window_usd=args.inventory_max_window_usd,
            max_per_wallet_usd=args.inventory_max_per_wallet_usd,
            min_plan_usd=args.inventory_min_plan_usd,
        ),
    )
    inventory_intents = [
        intent
        for intent in (inventory_plan_to_intent(plan) for plan in inventory_plans)
        if intent is not None
    ]
    inventory_paper: dict[str, Any] | None = None
    if args.inventory_paper_state:
        inventory_paper = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=args.inventory_paper_state,
                event_log_path=args.inventory_paper_event_log,
            )
        ).apply_intents(inventory_intents)
    paper_state_for_scoring = {
        "schema_version": 1,
        "kind": "wallet_copy_paper_state_merged_for_research",
        "paper_only": True,
        "live_orders_allowed": False,
        "orders": paper_orders,
        "lifecycle_events": paper_lifecycle_events,
    }
    performance = score_paper_state(
        paper_state_for_scoring,
        resolutions_path=args.resolutions,
        admission_config=AdmissionConfig(
            min_resolved_orders=args.min_resolved_orders,
            min_roi_pct=args.min_roi_pct,
            min_wr_pct=args.min_wr_pct,
            max_unresolved_ratio=args.max_unresolved_ratio,
            max_avg_latency_s=args.max_avg_latency_s,
        ),
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "wallet_copy_research_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "inputs": {
            "history_state": args.history_state,
            "paper_state": args.paper_state,
        },
        "event_summary": wallet_event_summary(events),
        "cross_wallet_windows": cross_wallet_windows(events),
        "copy_intents": len(intents),
        "consensus_signals": [signal.asdict() for signal in consensus],
        "consensus_copy_intents": consensus_intents,
        "inventory_plans": [plan.asdict() for plan in inventory_plans],
        "inventory_copy_intents": [intent.asdict() for intent in inventory_intents],
        "inventory_paper": (
            {
                "state_path": args.inventory_paper_state,
                "summary": inventory_paper.get("summary") if isinstance(inventory_paper, dict) else None,
                "paper_only": inventory_paper.get("paper_only") if isinstance(inventory_paper, dict) else None,
                "live_orders_allowed": inventory_paper.get("live_orders_allowed") if isinstance(inventory_paper, dict) else None,
            }
            if inventory_paper is not None
            else None
        ),
        "features": build_feature_payload(events),
        "performance": {k: v for k, v in performance.items() if k != "scored_orders"},
        "train_rows": train_rows_from_events(events),
    }
    atomic_write_json(args.output, payload)
    if args.print_full:
        printed = {k: v for k, v in payload.items() if k != "train_rows"}
    else:
        performance_summary = (payload.get("performance") or {}).get("summary") if isinstance(payload.get("performance"), dict) else {}
        admission = (payload.get("performance") or {}).get("admission") if isinstance(payload.get("performance"), dict) else {}
        lifecycle = (
            (payload.get("performance") or {}).get("lifecycle_realized")
            if isinstance(payload.get("performance"), dict)
            else {}
        )
        printed = {
            "output": args.output,
            "paper_only": payload.get("paper_only"),
            "live_orders_allowed": payload.get("live_orders_allowed"),
            "event_summary": payload.get("event_summary"),
            "copy_intents": payload.get("copy_intents"),
            "consensus_signals": len(payload.get("consensus_signals") or []),
            "consensus_pass": sum(1 for row in payload.get("consensus_signals") or [] if row.get("status") == "PASS"),
            "inventory_plans": len(payload.get("inventory_plans") or []),
            "inventory_pass": sum(1 for row in payload.get("inventory_plans") or [] if row.get("status") == "PASS"),
            "performance_summary": performance_summary,
            "lifecycle_realized": {k: v for k, v in (lifecycle or {}).items() if k != "rows"},
            "admission": {
                "status": admission.get("status") if isinstance(admission, dict) else None,
                "blockers": admission.get("blockers") if isinstance(admission, dict) else None,
                "admitted_wallets": len(admission.get("admitted_wallets") or []) if isinstance(admission, dict) else 0,
            },
        }
    print(json.dumps(printed, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
