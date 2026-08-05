#!/usr/bin/env python3
"""Write a dedicated paper-only hot-standby lane for 0xa6896d11."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
DEFAULT_READY_SHADOW_STATE = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_WATCH_TIER_SHADOW_EV = "data/research/watch_tier_shadow_ev_latest.json"
DEFAULT_READMISSION_RULINGS = "data/research/watch_tier_readmission_rulings.json"
DEFAULT_HOT_STANDBY_LIVENESS = "data/research/hot_standby_source_liveness_latest.json"
DEFAULT_OUTPUT = "data/research/a6896d11_hot_standby_paper_lane_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--ready-shadow-state", default=DEFAULT_READY_SHADOW_STATE)
    parser.add_argument("--watch-tier-shadow-ev", default=DEFAULT_WATCH_TIER_SHADOW_EV)
    parser.add_argument("--readmission-rulings", default=DEFAULT_READMISSION_RULINGS)
    parser.add_argument("--hot-standby-liveness", default=DEFAULT_HOT_STANDBY_LIVENESS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _wallet(value: Any) -> str:
    wallet = str(value or "").strip().lower()
    return wallet if wallet.startswith("0x") else ""


def _find_wallet(rows: Any, wallet: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _wallet(row.get("wallet") or row.get("source_wallet")) == wallet:
            return row
    return {}


def build_state(
    *,
    ready_shadow_state: dict[str, Any],
    watch_tier_shadow_ev: dict[str, Any],
    readmission_rulings: dict[str, Any],
    hot_standby_liveness: dict[str, Any],
    wallet: str = DEFAULT_WALLET,
) -> dict[str, Any]:
    wallet = _wallet(wallet)
    lane = _find_wallet(ready_shadow_state.get("lanes"), wallet)
    candidate = _find_wallet(ready_shadow_state.get("hot_standby_ranked_candidates"), wallet)
    feed = _find_wallet(watch_tier_shadow_ev.get("wallets"), wallet)
    ruling = _find_wallet(readmission_rulings.get("rulings"), wallet)
    liveness = _find_wallet(hot_standby_liveness.get("rows"), wallet)
    source_liveness = lane.get("source_liveness") if isinstance(lane.get("source_liveness"), dict) else {}
    address_selection = liveness.get("address_selection") if isinstance(liveness.get("address_selection"), dict) else {}

    hot_standby_ready = bool(lane.get("hot_standby_ready") or candidate.get("hot_standby_ready"))
    succession_eligible = bool(lane.get("succession_eligible") or hot_standby_ready)
    if hot_standby_ready:
        status = "HOT_STANDBY_READY_PAPER_LANE"
    elif lane or candidate:
        status = "HOT_STANDBY_PAPER_LANE_PENDING_EVIDENCE"
    else:
        status = "HOT_STANDBY_PAPER_LANE_MISSING_READY_SHADOW_SOURCE"

    return {
        "schema_version": 1,
        "kind": "a6896d11_hot_standby_paper_lane",
        "flow_stage": "PROMOTE/LEARN/ROTATE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallet": wallet,
        "source": "Fable 2026-07-14T03:42Z hot_standby_ready paper lane",
        "status": status,
        "copyintent_parity_sanity": "PASS_SAME_POLICY_FAMILY_PAPER_ONLY_NO_LIVE_SUBMITTER",
        "summary": {
            "lane_present": bool(lane),
            "candidate_present": bool(candidate),
            "hot_standby_ready": hot_standby_ready,
            "succession_eligible": succession_eligible,
            "readiness_verdict": lane.get("readiness_verdict") or candidate.get("readiness_verdict") or "",
            "shadow_status": lane.get("shadow_status") or candidate.get("shadow_status") or "",
            "paper_policy_id": lane.get("paper_policy_id") or "",
            "in_lane_post_fee_pnl_usd": (
                lane.get("in_lane_post_fee_pnl_usd")
                if lane.get("in_lane_post_fee_pnl_usd") is not None
                else candidate.get("in_lane_post_fee_pnl_usd")
            ),
            "resolved_paper_fills": (
                lane.get("resolved_paper_fills")
                if lane.get("resolved_paper_fills") is not None
                else candidate.get("resolved_count")
            ),
            "paper_orders": lane.get("paper_orders"),
            "resolved_fill_gap": lane.get("resolved_fill_gap"),
            "feed_eligible_signals": feed.get("eligible_signals"),
            "feed_resolved_signals": feed.get("resolved_signals"),
            "feed_roi_pct": feed.get("roi_pct"),
            "ruling": ruling.get("ruling") or ruling.get("decision") or "",
            "ruling_id": ruling.get("ruling_id") or ruling.get("authority") or "",
            "source_liveness_status": source_liveness.get("status") or "",
            "source_liveness_last_trade_age_h": source_liveness.get("last_trade_age_h"),
            "source_binding_status": lane.get("source_binding_status") or "",
            "standby_evidence_started_at": lane.get("standby_evidence_started_at"),
            "standby_evidence_elapsed_h": lane.get("standby_evidence_elapsed_h"),
            "standby_evidence_minimum_h": lane.get("standby_evidence_minimum_h"),
            "query_liveness_raw_probe_age_h": address_selection.get("last_trade_age_h"),
            "query_liveness_authoritative_age_h": source_liveness.get("last_trade_age_h"),
            "query_liveness_age_basis": "ready-shadow recomputes raw probe age through current lane refresh",
            "next": (
                "keep paper-only hot standby armed; use only on recorded succession trigger"
                if hot_standby_ready
                else "continue paper-only hot standby measurement until readiness and liveness pass"
            ),
        },
        "lane": lane,
        "hot_standby_ranked_candidate": candidate,
        "watch_tier_feed_row": feed,
        "readmission_ruling": ruling,
        "source_liveness_row": liveness,
    }


def main() -> int:
    args = parse_args()
    payload = build_state(
        ready_shadow_state=load_json(args.ready_shadow_state, default={}) or {},
        watch_tier_shadow_ev=load_json(args.watch_tier_shadow_ev, default={}) or {},
        readmission_rulings=load_json(args.readmission_rulings, default={}) or {},
        hot_standby_liveness=load_json(args.hot_standby_liveness, default={}) or {},
        wallet=args.wallet,
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
