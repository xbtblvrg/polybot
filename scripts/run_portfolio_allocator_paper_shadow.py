#!/usr/bin/env python3
"""Paper-shadow timing harness for the portfolio allocator.

Flow stage: LIVE/PROMOTE/SELF-DEV. This script never submits orders and never
mutates the live guard. It consumes CopyIntent inputs, runs the portfolio
allocator, chunks the resulting paper intents as the single guard would drain
them, and records measured cycle/drain/rps timings next to the modeled dispatch
budget before any live-path wiring is considered.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import CopyIntent, utc_now_iso  # noqa: E402
from src.wallet_copy.portfolio_allocator import (  # noqa: E402
    PortfolioAllocatorConfig,
    allocate_portfolio_intents,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/portfolio_allocator_paper_shadow_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--intents-json", default="", help="JSON file containing CopyIntent dicts or {'intents': [...]}.")
    parser.add_argument("--shadow-events-jsonl", default="", help="Realtime shadow watch JSONL to replay with recorded arrivals.")
    parser.add_argument("--max-shadow-events", type=int, default=100)
    parser.add_argument("--arrival-selection", choices=("densest", "latest"), default="densest")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--synthetic-signals", type=int, default=0, help="Generate synthetic paper-only intents when no input exists.")
    parser.add_argument("--available-cash-usd", type=float, default=125.0)
    parser.add_argument("--reserve-cash-usd", type=float, default=0.0)
    parser.add_argument("--min-member-allocation-usd", type=float, default=1.0)
    parser.add_argument("--min-intent-allocation-usd", type=float, default=1.0)
    parser.add_argument("--max-intents-per-cycle", type=int, default=6)
    parser.add_argument("--guard-cycle-interval-s", type=float, default=0.5)
    parser.add_argument("--per-order-submit-budget-s", type=float, default=0.15)
    parser.add_argument("--max-api-requests-per-s", type=float, default=25.0)
    parser.add_argument("--target-drain-s", type=float, default=30.0)
    parser.add_argument("--real-arrival-gate-max-s", type=float, default=47.0)
    return parser.parse_args()


def _abs(path: str) -> Path:
    target = Path(path)
    return target if target.is_absolute() else ROOT / target


def _intent(idx: int, *, size_usd: float = 10.0, price: float = 0.50) -> CopyIntent:
    wallet = f"0x{idx + 1:040x}"
    return CopyIntent(
        source_wallet=wallet,
        wallet_name=f"portfolio-shadow-{idx:03d}",
        source_event_id=f"portfolio-shadow-event-{idx}",
        condition_id=f"portfolio-shadow-condition-{idx % 10}",
        market_slug=f"btc-updown-5m-{1_783_600_000 + (idx % 12) * 300}",
        outcome="Up" if idx % 2 == 0 else "Down",
        side="YES",
        limit_price=price,
        wallet_usdc_size=size_usd / 0.10,
        copy_size_usd=size_usd,
        shares=round(size_usd / price, 6),
        observed_ts=1_783_600_001.0 + idx * 0.01,
        token_id=f"portfolio-shadow-token-{idx}",
        metadata={
            "portfolio_member_id": wallet,
            "copy_model": "portfolio_allocator_paper_shadow",
            "portfolio_score": max(1.0, 100.0 - idx * 0.25),
        },
    )


def _ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _shadow_event_intent(row: dict[str, Any]) -> CopyIntent | None:
    wallet = str(row.get("wallet") or row.get("source_wallet") or "").lower()
    event_id = str(row.get("event_id") or "")
    market_slug = str(row.get("market_slug") or "")
    token_id = str(row.get("token_id") or "")
    source_price = float(row.get("source_price") or 0.0)
    parity = row.get("parity") if isinstance(row.get("parity"), dict) else {}
    copy_size_usd = float(row.get("drip_tranche_usd") or parity.get("copy_size_usd") or 1.0)
    limit_price = float(row.get("parity_limit_price") or parity.get("max_copy_price") or source_price or 0.5)
    observed_ts = float(row.get("received_at_s") or row.get("book_fetch_started_at_s") or _ts(row.get("observed_at")))
    if not wallet or not event_id or not market_slug or not token_id or copy_size_usd <= 0 or limit_price <= 0 or observed_ts <= 0:
        return None
    score = 2.0 if row.get("taker_fillable") is True or row.get("parity_fillable") is True else 1.0
    return CopyIntent(
        source_wallet=wallet,
        wallet_name=f"shadow-{wallet[-6:]}",
        source_event_id=event_id,
        condition_id=str(parity.get("book_market") or market_slug),
        market_slug=market_slug,
        outcome=str(row.get("outcome") or "Up"),
        side=str(row.get("contract_side") or "YES"),
        limit_price=round(limit_price, 6),
        wallet_usdc_size=round(copy_size_usd / 0.10, 6),
        copy_size_usd=round(copy_size_usd, 6),
        shares=round(copy_size_usd / limit_price, 6),
        observed_ts=observed_ts,
        token_id=token_id,
        event_ts=float(row.get("source_ts") or observed_ts),
        metadata={
            "portfolio_member_id": wallet,
            "portfolio_score": score,
            "copy_model": "portfolio_allocator_shadow_replay",
            "shadow_replay": {
                "event_id": event_id,
                "observed_at": row.get("observed_at"),
                "received_at_s": row.get("received_at_s"),
                "book_fetch_started_at_s": row.get("book_fetch_started_at_s"),
                "within_copy_latency_window": row.get("within_copy_latency_window"),
                "taker_fillable": row.get("taker_fillable"),
                "parity_fillable": row.get("parity_fillable"),
                "source_price": row.get("source_price"),
                "needed_bps": row.get("needed_bps"),
                "source_path": "realtime_shadow_watch_jsonl",
            },
        },
    )


def _select_shadow_rows(rows: list[dict[str, Any]], *, max_events: int, selection: str) -> list[dict[str, Any]]:
    if max_events <= 0 or len(rows) <= max_events:
        return rows
    ordered = sorted(rows, key=lambda row: float(row.get("_arrival_ts") or 0.0))
    if selection == "latest":
        return ordered[-max_events:]
    best_start = 0
    best_span = float("inf")
    for start in range(0, len(ordered) - max_events + 1):
        end = start + max_events - 1
        span = float(ordered[end].get("_arrival_ts") or 0.0) - float(ordered[start].get("_arrival_ts") or 0.0)
        if span < best_span:
            best_span = span
            best_start = start
    return ordered[best_start : best_start + max_events]


def _load_shadow_event_intents(path: str, *, max_events: int, selection: str) -> tuple[list[CopyIntent], dict[str, Any]]:
    loaded: list[dict[str, Any]] = []
    path_obj = _abs(path)
    with path_obj.open(errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict) or row.get("kind") != "wallet_copy_realtime_shadow_watch_event":
                continue
            arrival_ts = float(row.get("received_at_s") or row.get("book_fetch_started_at_s") or _ts(row.get("observed_at")))
            if arrival_ts <= 0:
                continue
            row["_arrival_ts"] = arrival_ts
            loaded.append(row)
    selected = _select_shadow_rows(loaded, max_events=max_events, selection=selection)
    intents = [intent for row in selected if (intent := _shadow_event_intent(row)) is not None]
    arrival_ts = [float(intent.observed_ts or 0.0) for intent in intents]
    return intents, {
        "path": str(path),
        "rows_loaded": len(loaded),
        "rows_selected": len(selected),
        "intents_loaded": len(intents),
        "selection": selection,
        "max_events": int(max_events),
        "selected_arrival_span_s": round(max(arrival_ts) - min(arrival_ts), 6) if len(arrival_ts) >= 2 else 0.0,
    }


def _load_intents(
    path: str,
    *,
    shadow_events_jsonl: str,
    max_shadow_events: int,
    arrival_selection: str,
    synthetic_signals: int,
) -> tuple[list[CopyIntent], str, dict[str, Any]]:
    if path:
        payload = load_json(_abs(path), default={})
        rows = payload.get("intents") if isinstance(payload, dict) else payload
        if isinstance(rows, list):
            return [CopyIntent.from_dict(row) for row in rows if isinstance(row, dict)], str(path), {"kind": "intents_json"}
    if shadow_events_jsonl:
        intents, summary = _load_shadow_event_intents(
            shadow_events_jsonl,
            max_events=int(max_shadow_events),
            selection=str(arrival_selection),
        )
        return intents, f"shadow_jsonl:{shadow_events_jsonl}:{arrival_selection}:{len(intents)}", summary
    if synthetic_signals > 0:
        return [_intent(idx) for idx in range(int(synthetic_signals))], f"synthetic:{int(synthetic_signals)}", {"kind": "synthetic"}
    return [], "empty", {"kind": "empty"}


def _chunks(rows: list[CopyIntent], size: int) -> list[list[CopyIntent]]:
    chunk_size = max(1, int(size))
    return [rows[idx : idx + chunk_size] for idx in range(0, len(rows), chunk_size)]


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * float(pct)))))
    return round(float(ordered[idx]), 6)


def _arrival_profile(intents: list[CopyIntent]) -> dict[str, Any]:
    arrivals = sorted(float(intent.observed_ts or 0.0) for intent in intents if float(intent.observed_ts or 0.0) > 0)
    gaps = [arrivals[idx] - arrivals[idx - 1] for idx in range(1, len(arrivals))]
    return {
        "intent_count": len(arrivals),
        "first_arrival_ts": round(arrivals[0], 6) if arrivals else None,
        "last_arrival_ts": round(arrivals[-1], 6) if arrivals else None,
        "arrival_span_s": round(arrivals[-1] - arrivals[0], 6) if len(arrivals) >= 2 else 0.0,
        "min_gap_s": round(min(gaps), 6) if gaps else 0.0,
        "p50_gap_s": _percentile(gaps, 0.50),
        "p95_gap_s": _percentile(gaps, 0.95),
        "max_gap_s": round(max(gaps), 6) if gaps else 0.0,
    }


def _simulate_guard_cycles(
    intents: list[CopyIntent],
    *,
    max_intents_per_cycle: int,
    guard_cycle_interval_s: float,
    per_order_submit_budget_s: float,
) -> dict[str, Any]:
    ordered = sorted(intents, key=lambda intent: (float(intent.observed_ts or 0.0), intent.intent_id))
    if not ordered:
        return {
            "cycle_count": 0,
            "cycles": [],
            "simulated_guard_drain_s": 0.0,
            "post_last_arrival_drain_s": 0.0,
            "max_wait_s": 0.0,
            "p95_wait_s": 0.0,
            "processed_intents": 0,
        }
    max_per_cycle = max(1, int(max_intents_per_cycle))
    cycle_gap = max(0.0, float(guard_cycle_interval_s))
    submit_s = max(0.0, float(per_order_submit_budget_s))
    next_idx = 0
    pending: list[CopyIntent] = []
    now_ts = float(ordered[0].observed_ts or 0.0)
    first_arrival = now_ts
    last_arrival = max(float(intent.observed_ts or 0.0) for intent in ordered)
    last_completion = first_arrival
    waits: list[float] = []
    cycles: list[dict[str, Any]] = []
    while next_idx < len(ordered) or pending:
        if not pending and next_idx < len(ordered):
            now_ts = max(now_ts, float(ordered[next_idx].observed_ts or now_ts))
        while next_idx < len(ordered) and float(ordered[next_idx].observed_ts or 0.0) <= now_ts + 1e-9:
            pending.append(ordered[next_idx])
            next_idx += 1
        if not pending:
            continue
        chunk = pending[:max_per_cycle]
        pending = pending[max_per_cycle:]
        submit_start = now_ts + cycle_gap
        completions = []
        for offset, intent in enumerate(chunk, start=1):
            completion = submit_start + offset * submit_s
            wait_s = max(0.0, completion - float(intent.observed_ts or completion))
            waits.append(wait_s)
            completions.append(completion)
        last_completion = max(last_completion, max(completions) if completions else now_ts)
        cycles.append(
            {
                "cycle_index": len(cycles) + 1,
                "cycle_start_ts": round(now_ts, 6),
                "intent_count": len(chunk),
                "pending_after_cycle": len(pending),
                "planned_notional_usd": round(sum(float(intent.copy_size_usd or 0.0) for intent in chunk), 6),
                "first_arrival_ts": round(min(float(intent.observed_ts or 0.0) for intent in chunk), 6),
                "last_completion_ts": round(max(completions) if completions else now_ts, 6),
                "max_wait_s": round(max(waits[-len(chunk) :]), 6) if chunk else 0.0,
            }
        )
        now_ts = last_completion
    return {
        "cycle_count": len(cycles),
        "cycles": cycles,
        "simulated_guard_drain_s": round(last_completion - first_arrival, 6),
        "post_last_arrival_drain_s": round(max(0.0, last_completion - last_arrival), 6),
        "max_wait_s": round(max(waits), 6) if waits else 0.0,
        "p95_wait_s": _percentile(waits, 0.95),
        "processed_intents": len(waits),
    }


def _cycle_sample(cycles: list[dict[str, Any]], *, limit: int = 40) -> dict[str, Any]:
    if len(cycles) <= limit:
        return {"truncated": False, "total_cycles": len(cycles), "rows": cycles}
    half = max(1, limit // 2)
    return {
        "truncated": True,
        "total_cycles": len(cycles),
        "head_rows": cycles[:half],
        "tail_rows": cycles[-half:],
    }


def build_paper_shadow_report(
    intents: list[CopyIntent],
    *,
    source: str,
    available_cash_usd: float,
    reserve_cash_usd: float,
    min_member_allocation_usd: float,
    min_intent_allocation_usd: float,
    max_intents_per_cycle: int,
    guard_cycle_interval_s: float,
    per_order_submit_budget_s: float,
    max_api_requests_per_s: float,
    target_drain_s: float,
    real_arrival_gate_max_s: float,
    source_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    allocation = allocate_portfolio_intents(
        intents,
        member_scores={
            str(intent.source_wallet or "").lower(): (intent.metadata or {}).get("portfolio_score", 1.0)
            for intent in intents
        },
        available_cash_usd=float(available_cash_usd),
        now_ts=time.time(),
        config=PortfolioAllocatorConfig(
            reserve_cash_usd=float(reserve_cash_usd),
            min_member_allocation_usd=max(0.0, float(min_member_allocation_usd)),
            min_intent_allocation_usd=max(0.0, float(min_intent_allocation_usd)),
        ),
    )
    allocation_s = time.perf_counter() - started

    drain_started = time.perf_counter()
    simulation = _simulate_guard_cycles(
        list(allocation.scaled_intents),
        max_intents_per_cycle=int(max_intents_per_cycle),
        guard_cycle_interval_s=float(guard_cycle_interval_s),
        per_order_submit_budget_s=float(per_order_submit_budget_s),
    )
    measured_drain_s = time.perf_counter() - drain_started

    scaled_count = len(allocation.scaled_intents)
    modeled_cycle_count = int(math.ceil(scaled_count / max(1, int(max_intents_per_cycle)))) if scaled_count else 0
    modeled_submit_s = scaled_count * max(0.0, float(per_order_submit_budget_s))
    modeled_wait_s = modeled_cycle_count * max(0.0, float(guard_cycle_interval_s))
    modeled_drain_s = round(modeled_submit_s + modeled_wait_s, 6)
    modeled_api_rps = round((scaled_count * 2.0) / modeled_drain_s, 6) if modeled_drain_s > 0 else 0.0
    measured_rps = round(scaled_count / measured_drain_s, 6) if measured_drain_s > 0 else 0.0
    arrivals = _arrival_profile(intents)
    real_arrival_gate_drain_s = (
        simulation["post_last_arrival_drain_s"]
        if arrivals["arrival_span_s"] > modeled_drain_s
        else simulation["simulated_guard_drain_s"]
    )
    status = "PASS"
    defects: list[str] = []
    if allocation.starved_member_count:
        status = "ANALYZE"
        defects.append("starved_members")
    if float(target_drain_s) > 0 and modeled_drain_s > float(target_drain_s):
        status = "ANALYZE"
        defects.append("modeled_drain_above_target")
    if modeled_api_rps > float(max_api_requests_per_s):
        status = "ANALYZE"
        defects.append("modeled_api_rps_above_budget")
    fixed_real_arrival_gate_s = max(0.0, float(real_arrival_gate_max_s))
    if fixed_real_arrival_gate_s > 0 and real_arrival_gate_drain_s > fixed_real_arrival_gate_s:
        status = "ANALYZE"
        defects.append("real_arrival_drain_above_gate")

    return {
        "schema_version": 1,
        "kind": "portfolio_allocator_paper_shadow",
        "flow_stage": "LIVE/PROMOTE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source": source,
        "source_summary": source_summary or {},
        "arrival_profile": arrivals,
        "status": status,
        "defects": defects,
        "input_intent_count": len(intents),
        "scaled_intent_count": scaled_count,
        "allocator": {
            "status": allocation.status,
            "budget_usd": allocation.budget_usd,
            "allocated_usd": allocation.allocated_usd,
            "allocated_member_count": allocation.allocated_member_count,
            "starved_member_count": allocation.starved_member_count,
            "allocation_runtime_s": round(allocation_s, 6),
        },
        "timing": {
            "cycle_count": modeled_cycle_count,
            "simulated_cycle_count": simulation["cycle_count"],
            "max_intents_per_cycle": int(max_intents_per_cycle),
            "measured_python_drain_s": round(measured_drain_s, 6),
            "measured_python_intents_per_s": measured_rps,
            "modeled_guard_drain_s": modeled_drain_s,
            "modeled_api_requests_per_s": modeled_api_rps,
            "simulated_guard_drain_s": simulation["simulated_guard_drain_s"],
            "simulated_post_last_arrival_drain_s": simulation["post_last_arrival_drain_s"],
            "simulated_max_wait_s": simulation["max_wait_s"],
            "simulated_p95_wait_s": simulation["p95_wait_s"],
            "real_arrival_gate_drain_s": round(float(real_arrival_gate_drain_s), 6),
            "real_arrival_gate_max_s": round(fixed_real_arrival_gate_s, 6),
            "model_2x_gate_s": round(2.0 * modeled_drain_s, 6),
            "target_drain_s": float(target_drain_s),
            "max_api_requests_per_s": float(max_api_requests_per_s),
        },
        "cycles": _cycle_sample(simulation["cycles"]),
        "next": "compare this paper-shadow timing artifact against real signal-arrival patterns before any live adoption",
    }


def main() -> int:
    args = parse_args()
    intents, source, source_summary = _load_intents(
        args.intents_json,
        shadow_events_jsonl=args.shadow_events_jsonl,
        max_shadow_events=int(args.max_shadow_events),
        arrival_selection=str(args.arrival_selection),
        synthetic_signals=int(args.synthetic_signals),
    )
    report = build_paper_shadow_report(
        intents,
        source=source,
        source_summary=source_summary,
        available_cash_usd=float(args.available_cash_usd),
        reserve_cash_usd=float(args.reserve_cash_usd),
        min_member_allocation_usd=float(args.min_member_allocation_usd),
        min_intent_allocation_usd=float(args.min_intent_allocation_usd),
        max_intents_per_cycle=int(args.max_intents_per_cycle),
        guard_cycle_interval_s=float(args.guard_cycle_interval_s),
        per_order_submit_budget_s=float(args.per_order_submit_budget_s),
        max_api_requests_per_s=float(args.max_api_requests_per_s),
        target_drain_s=float(args.target_drain_s),
        real_arrival_gate_max_s=float(args.real_arrival_gate_max_s),
    )
    atomic_write_json(_abs(args.output), report)
    print(json.dumps({key: report[key] for key in ("status", "source", "input_intent_count", "scaled_intent_count", "timing")}, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
