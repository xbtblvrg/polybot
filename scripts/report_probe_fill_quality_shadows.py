#!/usr/bin/env python3
"""Accrue preregistered a689 fill-quality and window-time paper shadows.

Flow stage: OBSERVE/LEARN.  This reporter is evidence-only: it never changes
the live policy, selection pin, caps, or order submission path.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_scorecard import _default_resolutions_path  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

A689 = "0xa6896d11f76dfa2820662c1f441496f51553559b"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/probe_fill_quality_shadows_latest.json"
FEE_RATE = 0.069997697


def _dt(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except (TypeError, ValueError):
        return None


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price <= 0.50:
        return "01_25_50"
    if price <= 0.70:
        return "02_50_70"
    return "03_GT_70"


def _summary(values: list[float], *, target_n: int) -> dict[str, Any]:
    pnl = round(sum(values), 6)
    return {
        "n": len(values),
        "target_n": target_n,
        "n_gap": max(0, target_n - len(values)),
        "post_fee_pnl_usd": pnl,
        "post_fee_ev_usd": round(pnl / len(values), 6) if values else None,
        "positive": sum(value > 0 for value in values),
        "negative": sum(value < 0 for value in values),
        "gate_crossed": len(values) >= target_n,
    }


def _resolution_by_slug(resolutions: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in resolutions.values():
        if not isinstance(row, dict):
            continue
        slug = str(row.get("market_slug") or "")
        if slug:
            result[slug] = row
    return result


def build_packet(
    *,
    ledger: dict[str, Any],
    guard: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    now = _dt(generated_at) or datetime.now(UTC)
    day_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict) or str(order.get("source_wallet") or "").lower() != A689:
            continue
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        submitted = _dt(order.get("submitted_at"))
        slug = str(order.get("market_slug") or "")
        if submitted and slug:
            groups[slug].append(order)

    current_windows: list[float] = []
    cells: dict[str, list[float]] = defaultdict(list)
    early_probe: list[float] = []
    later_probe: list[float] = []
    for slug, orders in groups.items():
        scored = [score_order(order, resolutions) for order in orders]
        if not scored or not all(row.get("resolved") for row in scored):
            continue
        pnl = sum(_num(row.get("pnl_usd")) for row in scored)
        first = min(orders, key=lambda row: str(row.get("submitted_at") or ""))
        submitted = _dt(first.get("submitted_at"))
        if submitted is None:
            continue
        intent = first.get("source_intent") if isinstance(first.get("source_intent"), dict) else {}
        meta = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
        inventory = meta.get("inventory_v2") if isinstance(meta.get("inventory_v2"), dict) else {}
        price = _num(first.get("limit_price"), _num(intent.get("limit_price")))
        offset = _num(inventory.get("observed_slug_epoch_delta_s"), -1.0)
        effective_cap = _num((meta.get("wallet_copy_policy") or {}).get("effective_live_cap_usd"), 999.0)
        if submitted >= day_start and submitted.weekday() < 5:
            current_windows.append(pnl)
            offset_bucket = "lt120" if offset < 120 else "120_179" if offset < 180 else "gte180"
            cells[f"{_price_bucket(price)}|{offset_bucket}"].append(pnl)
        if submitted.weekday() < 5 and effective_cap <= 1.0:
            minutes = submitted.hour * 60 + submitted.minute + submitted.second / 60.0
            (early_probe if minutes < 180 else later_probe).append(pnl)

    resolution_rows = _resolution_by_slug(resolutions)
    near_miss_values: list[float] = []
    near_miss_samples: list[dict[str, Any]] = []
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    seen: set[tuple[str, str]] = set()
    for row in participation.get("rows") or []:
        if not isinstance(row, dict) or str(row.get("source_wallet") or "").lower() != A689:
            continue
        if str(row.get("dominant_skip_reason") or "") != "window_time_gte_180s":
            continue
        elapsed = _num(row.get("observed_slug_epoch_delta_s"), -1.0)
        if not 180.0 <= elapsed <= 200.0:
            continue
        slug = str(row.get("market_slug") or "")
        outcome = str(row.get("outcome") or "").upper()
        key = (slug, outcome)
        if key in seen:
            continue
        seen.add(key)
        resolved = resolution_rows.get(slug) or {}
        direction = str(resolved.get("direction") or "").upper()
        price = _num(row.get("source_inventory_vwap"), _num(row.get("latest_source_price")))
        size = _num(row.get("would_floor_min_order_usd"), _num(row.get("guard_sized_copy_usd"), 1.0))
        if direction not in {"UP", "DOWN"} or outcome not in {"UP", "DOWN"} or not 0 < price < 1 or size <= 0:
            continue
        fee = FEE_RATE * size * (1.0 - price)
        pnl = size * (1.0 / price - 1.0) - fee if outcome == direction else -size - fee
        near_miss_values.append(pnl)
        near_miss_samples.append({"market_slug": slug, "outcome": outcome, "direction": direction, "elapsed_s": elapsed, "price": price, "size_usd": size, "post_fee_pnl_usd": round(pnl, 6)})

    micro = _summary(current_windows, target_n=40)
    early = _summary(early_probe, target_n=20)
    later = _summary(later_probe, target_n=30)
    near = _summary(near_miss_values, target_n=40)
    status = "EVIDENCE_GATE_READY" if micro["gate_crossed"] and early["gate_crossed"] and later["gate_crossed"] and near["gate_crossed"] else "ACCRUING"
    return {
        "schema_version": 1,
        "kind": "probe_fill_quality_shadows",
        "flow_stage": "OBSERVE/LEARN",
        "generated_at": generated_at,
        "status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "single_submitter_unchanged": True,
        "preregistration": {
            "weekday_micro_loss": "weekday; entry-price buckets <.25/.25-.50/.50-.70/>.70; source offsets <120/120-179/>=180; n>=40 resolved windows",
            "window_time_near_miss": "a689 window_time_gte_180s; observed offset [180,200]; unique slug/outcome; n>=40 resolved windows; fixed fee model",
            "early_utc_probe_fill_quality": "weekday effective cap <=$1; early UTC fixed at [00:00,03:00); n>=20 versus later-day n>=30",
        },
        "weekday_micro_loss": {**micro, "cells": {key: _summary(values, target_n=0) for key, values in sorted(cells.items())}},
        "window_time_near_miss": {**near, "samples": near_miss_samples[:20]},
        "early_utc_probe_fill_quality": {"early_utc": early, "later_day": later, "ev_delta_usd": round((early["post_fee_ev_usd"] or 0.0) - (later["post_fee_ev_usd"] or 0.0), 6)},
        "decision": "REPORT_ONLY_NO_LIVE_DENY_OR_BAR_CHANGE",
        "next": "accrue until preregistered n gates cross; then ask Fable for a separate holdout/live ruling",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--guard", default=str(DEFAULT_GUARD))
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    resolutions_path = args.resolutions or str(ROOT / _default_resolutions_path())
    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    packet = build_packet(
        ledger=load_json(Path(args.ledger), default={}),
        guard=load_json(Path(args.guard), default={}),
        resolutions=load_resolutions(resolutions_path),
        generated_at=generated_at,
    )
    atomic_write_json(Path(args.output), packet)
    print(Path(args.output))


if __name__ == "__main__":
    main()
