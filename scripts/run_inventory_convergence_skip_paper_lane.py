#!/usr/bin/env python3
"""Measure inventory-target-met skips against routing-shadow PnL.

This runner is paper/research only. It reads live guard participation rows and
routing-shadow measurement rows, then writes a separate artifact showing whether
inventory_target_already_met skips are hiding positive post-fee windows under
the current probe-cap size.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_ROUTING_SHADOW = "data/research/routing_shadow_validation_latest.json"
DEFAULT_OUTPUT = "data/research/inventory_convergence_skip_paper_lane_latest.json"
SKIP_REASON = "inventory_target_already_met"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--routing-shadow", default=DEFAULT_ROUTING_SHADOW)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--probe-cap-usd", type=float, default=1.0)
    parser.add_argument("--min-post-fee-pnl-usd", type=float, default=0.0)
    parser.add_argument("--sample-limit", type=int, default=25)
    return parser.parse_args()


def _window_start_s(row: dict[str, Any]) -> int:
    try:
        return int(float(row.get("window_start_s") or 0))
    except (TypeError, ValueError):
        return 0


def _window_key(row: dict[str, Any]) -> tuple[str, int]:
    return (str(row.get("market_slug") or ""), _window_start_s(row))


def _routing_by_window(payload: dict[str, Any]) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for row in payload.get("rows") or []:
        if not isinstance(row, dict):
            continue
        key = _window_key(row)
        if key[0] and key[1]:
            out[key] = row
    return out


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _scaled_post_fee(route: dict[str, Any], *, probe_cap_usd: float) -> dict[str, Any] | None:
    outcome = route.get("realized_paper_outcome") if isinstance(route.get("realized_paper_outcome"), dict) else {}
    gross_pnl = _maybe_float(outcome.get("paper_pnl_usd"))
    if gross_pnl is None:
        return None
    expected_fee = num(route.get("expected_fee_usd"), 0.0)
    shares = num(route.get("shares"), 0.0)
    limit_price = num(route.get("limit_price"), 0.0)
    measured_notional = max(0.0, shares * limit_price)
    if probe_cap_usd > 0.0 and measured_notional > 0.0:
        scale = min(1.0, float(probe_cap_usd) / measured_notional)
    else:
        scale = 1.0
    scaled_gross = gross_pnl * scale
    scaled_fee = expected_fee * scale
    return {
        "measured_notional_usd": round(measured_notional, 6),
        "probe_cap_scale": round(scale, 6),
        "gross_pnl_usd": round(gross_pnl, 6),
        "expected_fee_usd": round(expected_fee, 6),
        "probe_cap_gross_pnl_usd": round(scaled_gross, 6),
        "probe_cap_expected_fee_usd": round(scaled_fee, 6),
        "probe_cap_post_fee_pnl_usd": round(scaled_gross - scaled_fee, 6),
    }


def _target_met_rows(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    participation = guard_state.get("window_participation") if isinstance(guard_state.get("window_participation"), dict) else {}
    rows: list[dict[str, Any]] = []
    for row in participation.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if row.get("current_set_generation") is False:
            continue
        if str(row.get("dominant_skip_reason") or "") != SKIP_REASON:
            continue
        rows.append(row)
    return rows


def build_state(
    *,
    guard_state: dict[str, Any],
    routing_shadow: dict[str, Any],
    probe_cap_usd: float = 1.0,
    min_post_fee_pnl_usd: float = 0.0,
    sample_limit: int = 25,
) -> dict[str, Any]:
    routing = _routing_by_window(routing_shadow)
    rows: list[dict[str, Any]] = []
    no_join: list[dict[str, Any]] = []
    total_post_fee = 0.0
    positive_post_fee = 0.0
    negative_post_fee = 0.0
    recoverable_post_fee = 0.0
    positive_windows = 0
    recoverable_positive_windows = 0
    unfilled_positive_windows = 0
    unresolved_joined_windows = 0
    source_order_gap_units = 0
    would_submit_windows = 0

    target_rows = _target_met_rows(guard_state)
    for participation in target_rows:
        route = routing.get(_window_key(participation))
        if not route:
            no_join.append(
                {
                    "market_slug": participation.get("market_slug"),
                    "window_start_s": participation.get("window_start_s"),
                    "source_wallet": participation.get("source_wallet"),
                }
            )
            continue

        eligible = int(num(participation.get("wallet_eligible_orders"), 0.0))
        fills = int(num(participation.get("our_fills"), 0.0))
        submits = int(num(participation.get("our_submits"), 0.0))
        source_gap = max(0, eligible - fills)
        would_submit = bool(route.get("winning_intent_id") and route.get("winning_policy_id"))
        if would_submit:
            would_submit_windows += 1

        outcome = route.get("realized_paper_outcome") if isinstance(route.get("realized_paper_outcome"), dict) else {}
        if str(outcome.get("status") or "").upper() != "RESOLVED":
            unresolved_joined_windows += 1
        scaled = _scaled_post_fee(route, probe_cap_usd=float(probe_cap_usd))
        post_fee = scaled.get("probe_cap_post_fee_pnl_usd") if scaled else None
        if post_fee is not None:
            total_post_fee += float(post_fee)
            if float(post_fee) > float(min_post_fee_pnl_usd):
                positive_windows += 1
                positive_post_fee += float(post_fee)
            else:
                negative_post_fee += float(post_fee)

        recoverable = bool(
            would_submit
            and source_gap > 0
            and post_fee is not None
            and float(post_fee) > float(min_post_fee_pnl_usd)
        )
        if recoverable:
            recoverable_positive_windows += 1
            source_order_gap_units += source_gap
            recoverable_post_fee += float(post_fee)
            if fills == 0:
                unfilled_positive_windows += 1

        rows.append(
            {
                "market_slug": participation.get("market_slug"),
                "window_start_s": _window_start_s(participation),
                "source_wallet": participation.get("source_wallet"),
                "outcome": participation.get("outcome"),
                "wallet_eligible_orders": eligible,
                "our_submits": submits,
                "our_fills": fills,
                "source_order_gap_units": source_gap,
                "winning_intent_id": route.get("winning_intent_id"),
                "winning_policy_id": route.get("winning_policy_id"),
                "would_submit": would_submit,
                "resolution_status": outcome.get("status"),
                "wins": outcome.get("wins"),
                "probe_cap_usd": float(probe_cap_usd),
                "post_fee_measurement": scaled,
                "recoverable_positive_window": recoverable,
            }
        )

    rows.sort(key=lambda row: int(row.get("window_start_s") or 0), reverse=True)
    status = "ACCRUING_POSITIVE_RECOVERABLE_WINDOWS" if recoverable_positive_windows else "ACCRUING_NO_POSITIVE_RECOVERABLE_WINDOWS"
    return {
        "schema_version": 1,
        "kind": "inventory_convergence_skip_paper_lane",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source": "Fable 2026-07-14T03:42Z structural-inventory-convergence-skip",
        "copyintent_parity_sanity": "PASS_PAPER_ONLY_MEASUREMENT_NO_LIVE_SUBMITTER",
        "criteria": {
            "skip_reason": SKIP_REASON,
            "probe_cap_usd": float(probe_cap_usd),
            "min_post_fee_pnl_usd": float(min_post_fee_pnl_usd),
            "join_key": "market_slug + window_start_s",
        },
        "summary": {
            "paper_lane_status": status,
            "inventory_target_already_met_windows": len(target_rows),
            "routing_shadow_joined_windows": len(rows),
            "routing_shadow_missing_windows": len(no_join),
            "unresolved_joined_windows": unresolved_joined_windows,
            "would_submit_windows": would_submit_windows,
            "positive_post_fee_windows": positive_windows,
            "recoverable_positive_windows": recoverable_positive_windows,
            "unfilled_positive_windows": unfilled_positive_windows,
            "source_order_gap_positive_units": source_order_gap_units,
            "estimated_recoverable_probe_cap_notional_usd": round(
                recoverable_positive_windows * float(probe_cap_usd),
                6,
            ),
            "total_probe_cap_post_fee_pnl_usd": round(total_post_fee, 6),
            "positive_probe_cap_post_fee_pnl_usd": round(positive_post_fee, 6),
            "negative_probe_cap_post_fee_pnl_usd": round(negative_post_fee, 6),
            "recoverable_probe_cap_post_fee_pnl_usd": round(recoverable_post_fee, 6),
            "next": (
                "keep paper lane running; promote only after Fable adjudicates a positive repeatable inventory-convergence lever"
                if recoverable_positive_windows
                else "keep paper lane running until positive recoverable windows accrue under probe caps"
            ),
        },
        "missing_routing_shadow_samples": no_join[: max(0, int(sample_limit))],
        "rows": rows[: max(0, int(sample_limit))],
    }


def main() -> int:
    args = parse_args()
    payload = build_state(
        guard_state=load_json(args.guard_state, default={}) or {},
        routing_shadow=load_json(args.routing_shadow, default={}) or {},
        probe_cap_usd=float(args.probe_cap_usd),
        min_post_fee_pnl_usd=float(args.min_post_fee_pnl_usd),
        sample_limit=int(args.sample_limit),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
