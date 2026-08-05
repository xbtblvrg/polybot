#!/usr/bin/env python3
"""Measure ORDER127 unchanged-bar live-seat distance for the three F1-clean wallets."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

TARGETS = (
    "0xf9b7876ba2c35370e0bedb7f8ee772da722aa37c",
    "0x3048d65321be3497164cdfc2996f94f98a2e7537",
    "0x82c857cb4d18e919c1b7d3c6865be4debe50da77",
)


def _future_trade_scenarios(pnl_gap_usd: float, stake_usd: float, trades: int) -> list[dict[str, Any]]:
    avg_stake = stake_usd / trades if trades else 0.0
    rows = []
    for roi_pct in (5.0, 10.0, 20.0):
        pnl_per_trade = avg_stake * roi_pct / 100.0
        rows.append(
            {
                "assumed_future_roi_pct": roi_pct,
                "assumed_avg_stake_usd": round(avg_stake, 6),
                "minimum_additive_resolved_trades_to_positive_pnl": (
                    math.floor(pnl_gap_usd / pnl_per_trade) + 1 if pnl_per_trade > 0 else None
                ),
            }
        )
    return rows


def build_packet(frontier: dict[str, Any], temporal: dict[str, Any]) -> dict[str, Any]:
    rows_by_wallet = {
        str(row.get("wallet") or "").lower(): row
        for row in frontier.get("nearest_frontier") or []
        if isinstance(row, dict)
    }
    temporal_by_wallet = {
        str(row.get("wallet") or "").lower(): row
        for row in temporal.get("wallets") or []
        if isinstance(row, dict)
    }
    rows = []
    for wallet in TARGETS:
        source = rows_by_wallet.get(wallet, {})
        active = source.get("active_temporal") or {}
        direct = source.get("direct_source") or {}
        distances: list[dict[str, Any]] = []
        if wallet == TARGETS[0]:
            gap = abs(float(active.get("pnl_usd") or 0.0))
            stake = gap / abs(float(active.get("roi_pct") or 0.0) / 100.0) if active.get("roi_pct") else 0.0
            distances.append(
                {
                    "gate": "active_temporal_not_proven_negative + f1_walk_forward_admissible",
                    "current": {"weekday_pnl_usd": active.get("pnl_usd"), "weekday_roi_pct": active.get("roi_pct"), "resolved_trades": active.get("resolved_trades")},
                    "required": "weekday aggregate pnl_usd > 0 and roi_pct > 0 under unchanged walk-forward bars",
                    "pnl_gap_usd_strictly_greater_than": round(gap, 6),
                    "scenarios": _future_trade_scenarios(gap, stake, int(active.get("resolved_trades") or 0)),
                }
            )
        elif wallet == TARGETS[1]:
            recent = (temporal_by_wallet.get(wallet) or {}).get("recent") or {}
            gap = abs(float(recent.get("pnl_usd") or 0.0)) if float(recent.get("pnl_usd") or 0.0) <= 0 else 0.0
            distances.extend(
                [
                    {
                        "gate": "f2_fresh_rows_and_own_policy_copyable",
                        "current": {"attempts": direct.get("attempts"), "copyable": direct.get("copyable")},
                        "required": "at least one copyable row under the current evidenced fingerprint",
                        "copyable_rows_gap": max(0, 1 - int(direct.get("copyable") or 0)),
                    },
                    {
                        "gate": "f3_not_enabled_or_cooloff_or_fading + f1_walk_forward_admissible",
                        "current": {"classification": "FADING", "recent_pnl_usd": recent.get("pnl_usd"), "recent_roi_pct": recent.get("roi_pct"), "recent_resolved_trades": recent.get("resolved_trades")},
                        "required": "fresh recent resolved sample pnl_usd > 0 and roi_pct > 0",
                        "recent_pnl_gap_usd_strictly_greater_than": round(gap, 6),
                        "note": "rolling-sample replacement can require more than this additive lower bound",
                    },
                ]
            )
        else:
            park = source.get("standby_exclusion") or {}
            distances.append(
                {
                    "gate": "not_terminal_park_red_clock_or_measured_loser",
                    "current": {"permanent_park": park.get("permanent_park"), "marginal_post_fee_usd_per_fill": (park.get("park_basis") or {}).get("observed_marginal_post_fee_usd_per_fill"), "cooloff_until": source.get("cooloff_until")},
                    "required": "new primary-Fable evidence reversal; unchanged bars cannot clear an evidenced permanent park",
                    "marginal_post_fee_uplift_gap_usd_per_fill_strictly_greater_than": 0.309917,
                    "authority_change_required": True,
                }
            )
        rows.append(
            {
                "wallet": wallet,
                "eligible": source.get("eligible") is True,
                "evidence_deficits": source.get("evidence_deficits") or [],
                "distances": distances,
            }
        )
    return {
        "schema_version": 1,
        "kind": "order127_measured_seat_distance",
        "flow_stage": "PROMOTE/LIVE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "bars_unchanged": True,
        "source_generated_at": frontier.get("generated_at"),
        "candidate_count": frontier.get("candidate_count"),
        "eligible_count": frontier.get("eligible_count"),
        "rows": rows,
        "verdict": "NO_WALLET_WITHIN_REACH_UNDER_UNCHANGED_BARS",
        "admission_authority": False,
        "next": "publish finding; Fable must choose a new lawful money route rather than claim the seat is one generation away",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frontier", default="data/research/wide_direct_admissible_frontier_latest.json")
    parser.add_argument("--temporal", default="data/research/wallet_temporal_profitability_latest.json")
    parser.add_argument("--output", default="data/research/order127_measured_seat_distance_latest.json")
    args = parser.parse_args()
    packet = build_packet(load_json(args.frontier, default={}), load_json(args.temporal, default={}))
    atomic_write_json(args.output, packet)
    print(packet["verdict"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
