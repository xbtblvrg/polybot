#!/usr/bin/env python3
"""Publish defect_BK resolved-tape closure against Fable's preregistered gate."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json


WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"


def _wallet_row(temporal: dict[str, Any], wallet: str) -> dict[str, Any]:
    return next(
        (
            row
            for row in temporal.get("wallets") or []
            if isinstance(row, dict) and str(row.get("wallet") or "").lower() == wallet
        ),
        {},
    )


def build_report(
    *,
    temporal: dict[str, Any],
    liveness: dict[str, Any],
    replay: dict[str, Any],
    overlay: dict[str, Any],
    wallet: str = WALLET,
) -> dict[str, Any]:
    wallet = wallet.lower()
    row = _wallet_row(temporal, wallet)
    recent = row.get("recent") if isinstance(row.get("recent"), dict) else {}
    liveness_row = next(
        (
            item
            for item in liveness.get("rows") or []
            if isinstance(item, dict)
            and str(item.get("wallet") or "").lower() == wallet
        ),
        {},
    )
    selection = (
        liveness_row.get("address_selection")
        if isinstance(liveness_row.get("address_selection"), dict)
        else {}
    )
    replay_row = next(
        (
            item
            for item in replay.get("results") or []
            if isinstance(item, dict)
            and str(item.get("wallet") or "").lower() == wallet
        ),
        {},
    )
    overlay_enabled = any(
        isinstance(item, dict)
        and str(item.get("source_wallet") or item.get("wallet") or "").lower()
        == wallet
        and item.get("enabled") is not False
        for item in overlay.get("members") or []
    )
    latest_age_h = recent.get("latest_event_age_h")
    distinct_windows = int(recent.get("unique_windows") or 0)
    recent_pnl = recent.get("pnl_usd")
    gap_closed = bool(
        latest_age_h is not None
        and float(latest_age_h) <= 24.0
        and int(replay_row.get("normalized_btc5m_buy_events") or 0) > 0
    )
    recent_positive = bool(recent_pnl is not None and float(recent_pnl) > 0.0)
    diversity_pass = distinct_windows >= 5
    acceptance_a = gap_closed and diversity_pass
    acceptance_b = gap_closed and diversity_pass and not recent_positive
    acceptance_c = not gap_closed
    return {
        "schema_version": 1,
        "kind": "82c8_resolved_tape_gap_closure",
        "flow_stage": "DISCOVER/PROMOTE/SELF-DEV",
        "generated_at": datetime.now(UTC).isoformat(),
        "wallet": wallet,
        "temporal_generated_at": temporal.get("generated_at"),
        "liveness_generated_at": liveness.get("generated_at"),
        "replay_generated_at": replay.get("generated_at"),
        "live_trade_age_h": selection.get("last_trade_age_h"),
        "resolved_latest_event_age_h": latest_age_h,
        "resolved_latest_event_ts": recent.get("latest_event_ts"),
        "resolved_recent_trades": recent.get("resolved_trades"),
        "resolved_recent_unique_windows": distinct_windows,
        "resolved_recent_pnl_usd": recent_pnl,
        "resolved_recent_roi_pct": recent.get("roi_pct"),
        "replayed_btc5m_buy_events": replay_row.get(
            "normalized_btc5m_buy_events"
        ),
        "overlay_enabled": overlay_enabled,
        "checks": {
            "resolved_tape_gap_closed_inside_24h": gap_closed,
            "recent_distinct_windows_gte_5": diversity_pass,
            "recent_pnl_positive": recent_positive,
        },
        "preregistered_acceptance": {
            "a_gap_closed_and_recent_gte_5_windows": acceptance_a,
            "b_refreshed_recent_non_positive_after_diverse_sample": acceptance_b,
            "c_gap_does_not_close": acceptance_c,
        },
        "decision": (
            "GAP_CLOSED_DIVERSE_SAMPLE_READY_FOR_MECHANICAL_RECLASSIFICATION"
            if acceptance_a
            else "GAP_UNCLOSED_REFUSE_ON_ABSENCE"
            if acceptance_c
            else "GAP_CLOSED_SAMPLE_DIVERSITY_PENDING_REFUSE"
        ),
        "live_admission_authority": False,
        "gate_change_authority": False,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--temporal",
        default="data/research/wallet_temporal_profitability_latest.json",
    )
    parser.add_argument(
        "--liveness",
        default="data/research/hot_standby_source_liveness_latest.json",
    )
    parser.add_argument(
        "--replay",
        default="data/research/source_active_policy_history_replay_latest.json",
    )
    parser.add_argument(
        "--overlay",
        default="data/research/wallet_copy_active_set_auto_degrade_state.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/82c8_resolved_tape_gap_closure_latest.json",
    )
    args = parser.parse_args()
    report = build_report(
        temporal=load_json(args.temporal, default={}),
        liveness=load_json(args.liveness, default={}),
        replay=load_json(args.replay, default={}),
        overlay=load_json(args.overlay, default={}),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
