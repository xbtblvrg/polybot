#!/usr/bin/env python3
"""Offline dispatch-throughput audit for 100-source wallet-copy bursts.

Flow stage: SELF-DEV/LIVE. This is an evidence tool only; it never submits
orders and preserves the single live guard invariant.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import CopyIntent, utc_now_iso
from src.wallet_copy.portfolio_allocator import (
    PortfolioAllocatorConfig,
    allocate_portfolio_intents,
    allocation_result_id,
)


DEFAULT_OUTPUT = "data/research/wallet_copy_dispatch_throughput_audit_latest.json"


def _synthetic_intents(signal_count: int, *, request_usd: float, price: float) -> list[CopyIntent]:
    intents: list[CopyIntent] = []
    for idx in range(max(0, int(signal_count))):
        wallet = f"0x{idx + 1:040x}"
        window = 1_783_600_000 + (idx % 12) * 300
        intent = CopyIntent(
            source_wallet=wallet,
            wallet_name=f"member_{idx + 1:03d}",
            source_event_id=f"burst-{idx + 1}",
            condition_id=f"cond-{idx % 12:02d}",
            market_slug=f"btc-updown-5m-{window}",
            outcome="Up" if idx % 2 == 0 else "Down",
            side="YES" if idx % 2 == 0 else "NO",
            limit_price=price,
            wallet_usdc_size=request_usd / 0.10,
            copy_size_usd=request_usd,
            shares=round(request_usd / price, 6),
            observed_ts=1_783_600_001.0 + idx * 0.001,
            token_id=f"token-{idx:03d}",
            market_id=f"market-{idx % 12:02d}",
            metadata={"portfolio_member_id": wallet, "copy_model": "drip"},
        )
        intents.append(intent)
    return intents


def build_dispatch_throughput_audit(
    *,
    signal_count: int = 100,
    available_cash_usd: float = 125.0,
    request_usd_per_signal: float = 2.5,
    price: float = 0.50,
    max_intents_per_cycle: int = 6,
    guard_cycle_interval_s: float = 0.5,
    per_order_submit_budget_s: float = 0.15,
    max_api_requests_per_s: float = 25.0,
    target_drain_s: float = 30.0,
    min_member_allocation_usd: float = 1.0,
) -> dict[str, Any]:
    intents = _synthetic_intents(signal_count, request_usd=request_usd_per_signal, price=price)
    member_scores = {
        intent.source_wallet.lower(): {"portfolio_score": max(1.0, float(signal_count - idx))}
        for idx, intent in enumerate(intents)
    }
    allocation = allocate_portfolio_intents(
        intents,
        member_scores=member_scores,
        available_cash_usd=available_cash_usd,
        now_ts=1_783_600_000.0,
        config=PortfolioAllocatorConfig(
            min_member_allocation_usd=min_member_allocation_usd,
            min_intent_allocation_usd=min_member_allocation_usd,
            recycling_horizon_s=300.0,
            strategy_family="wallet_copy_dispatch_throughput_audit_allocator_v1",
        ),
    )
    allocated_orders = len(allocation.scaled_intents)
    cycle_capacity = max(1, int(max_intents_per_cycle))
    dispatch_cycles = int(math.ceil(allocated_orders / cycle_capacity)) if allocated_orders else 0
    estimated_drain_s = round(
        dispatch_cycles * max(0.0, float(guard_cycle_interval_s))
        + allocated_orders * max(0.0, float(per_order_submit_budget_s)),
        6,
    )
    api_request_count = allocated_orders * 2
    api_requests_per_s = round(api_request_count / estimated_drain_s, 6) if estimated_drain_s > 0 else 0.0
    member_allocations = list(allocation.member_allocations)
    allocated_values = [float(row["allocated_usd"]) for row in member_allocations if float(row["allocated_usd"]) > 0]
    fairness = {
        "allocated_members": allocation.allocated_member_count,
        "starved_members": allocation.starved_member_count,
        "min_allocated_usd": round(min(allocated_values), 6) if allocated_values else 0.0,
        "max_allocated_usd": round(max(allocated_values), 6) if allocated_values else 0.0,
        "all_members_receive_floor_when_cash_sufficient": bool(
            allocation.starved_member_count == 0
            and available_cash_usd >= signal_count * min_member_allocation_usd
        ),
    }
    status = "PASS"
    blockers: list[str] = []
    if allocation.starved_member_count:
        blockers.append("portfolio_allocator_starved_members")
    if estimated_drain_s > float(target_drain_s):
        blockers.append("estimated_dispatch_drain_exceeds_target")
    if api_requests_per_s > float(max_api_requests_per_s):
        blockers.append("estimated_api_rate_exceeds_budget")
    if blockers:
        status = "ANALYZE"
    return {
        "kind": "wallet_copy_dispatch_throughput_audit",
        "flow_stage": "SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": status,
        "blockers": blockers,
        "single_submitter_invariant": "single_guard_serial_submitter_preserved",
        "audit_scope": {
            "signal_count": int(signal_count),
            "request_usd_per_signal": round(float(request_usd_per_signal), 6),
            "available_cash_usd": round(float(available_cash_usd), 6),
            "target_drain_s": round(float(target_drain_s), 6),
        },
        "allocator": {
            "allocation_result_id": allocation_result_id(allocation),
            "status": allocation.status,
            "budget_usd": allocation.budget_usd,
            "requested_usd": allocation.requested_usd,
            "allocated_usd": allocation.allocated_usd,
            "member_count": allocation.member_count,
            "allocated_member_count": allocation.allocated_member_count,
            "starved_member_count": allocation.starved_member_count,
            "sample_member_allocations": member_allocations[:10],
        },
        "dispatch_model": {
            "max_intents_per_cycle": cycle_capacity,
            "guard_cycle_interval_s": round(float(guard_cycle_interval_s), 6),
            "per_order_submit_budget_s": round(float(per_order_submit_budget_s), 6),
            "allocated_orders": allocated_orders,
            "dispatch_cycles": dispatch_cycles,
            "estimated_drain_s": estimated_drain_s,
        },
        "api_budget": {
            "estimated_api_request_count": api_request_count,
            "estimated_api_requests_per_s": api_requests_per_s,
            "max_api_requests_per_s": round(float(max_api_requests_per_s), 6),
        },
        "fairness": fairness,
        "next_action": (
            "wire allocator into a paper shadow lane, then compare measured cycle timers before live adoption"
            if status == "PASS"
            else "fix throughput blocker named in blockers before mass onboarding"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=int, default=100)
    parser.add_argument("--available-cash-usd", type=float, default=125.0)
    parser.add_argument("--request-usd-per-signal", type=float, default=2.5)
    parser.add_argument("--price", type=float, default=0.50)
    parser.add_argument("--max-intents-per-cycle", type=int, default=6)
    parser.add_argument("--guard-cycle-interval-s", type=float, default=0.5)
    parser.add_argument("--per-order-submit-budget-s", type=float, default=0.15)
    parser.add_argument("--max-api-requests-per-s", type=float, default=25.0)
    parser.add_argument("--target-drain-s", type=float, default=30.0)
    parser.add_argument("--min-member-allocation-usd", type=float, default=1.0)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_dispatch_throughput_audit(
        signal_count=args.signals,
        available_cash_usd=args.available_cash_usd,
        request_usd_per_signal=args.request_usd_per_signal,
        price=args.price,
        max_intents_per_cycle=args.max_intents_per_cycle,
        guard_cycle_interval_s=args.guard_cycle_interval_s,
        per_order_submit_budget_s=args.per_order_submit_budget_s,
        max_api_requests_per_s=args.max_api_requests_per_s,
        target_drain_s=args.target_drain_s,
        min_member_allocation_usd=args.min_member_allocation_usd,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
