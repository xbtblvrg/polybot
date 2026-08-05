#!/usr/bin/env python3
"""Prepare or atomically execute Fable's a689 -> 0x82c8 paper-seat cut."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_ready_wallet_shadow_lanes import (  # noqa: E402
    A689_HOT_STANDBY_EVIDENCE_HOURS,
    A689_HOT_STANDBY_WALLET,
    PASS_POLICY,
    RANK1_SUCCESSOR_SOURCE_BINDING,
    RANK1_SUCCESSOR_WALLET,
    WATCH_TIER_READMISSION_FRESH_FILL_GATE,
)
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, json_file_lock, load_json  # noqa: E402


CUT_AT = dt.datetime(2026, 7, 23, 18, 22, 7, 136412, tzinfo=dt.timezone.utc)
DEFAULT_STATE = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_OUTPUT = "data/research/a689_82c8_ready_shadow_cut_spec_latest.json"


def _parse_iso(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _wallet(value: Any) -> str:
    value = str(value or "").strip().lower()
    return value if value.startswith("0x") and len(value) == 42 else ""


def build_spec(
    state: dict[str, Any],
    queue: dict[str, Any],
    *,
    now: dt.datetime,
) -> dict[str, Any]:
    lanes = [row for row in state.get("lanes") or [] if isinstance(row, dict)]
    a689 = next((row for row in lanes if _wallet(row.get("wallet")) == A689_HOT_STANDBY_WALLET), {})
    successor = next(
        (
            row
            for row in queue.get("ranked_members") or []
            if isinstance(row, dict) and _wallet(row.get("wallet")) == RANK1_SUCCESSOR_WALLET
        ),
        {},
    )
    replay = successor.get("replay") if isinstance(successor.get("replay"), dict) else {}
    started = _parse_iso(a689.get("standby_evidence_started_at"))
    elapsed_h = max(0.0, (now - started).total_seconds() / 3600.0) if started else 0.0
    resolved = int(a689.get("resolved_paper_fills") or 0)
    post_fee = num(a689.get("in_lane_post_fee_pnl_usd"), 0.0)
    a689_pass = bool(
        elapsed_h >= A689_HOT_STANDBY_EVIDENCE_HOURS
        and resolved >= WATCH_TIER_READMISSION_FRESH_FILL_GATE
        and post_fee > 0.0
        and a689.get("source_binding_status") == "WIRED"
    )
    return {
        "schema_version": 1,
        "kind": "a689_82c8_ready_shadow_cut_spec",
        "flow_stage": "ROTATE/PROMOTE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "authority": "fable DIRECTION 2026-07-23T05:52Z",
        "cut_at": CUT_AT.isoformat().replace("+00:00", "Z"),
        "cut_due": now >= CUT_AT,
        "a689": {
            "wallet": A689_HOT_STANDBY_WALLET,
            "present": bool(a689),
            "elapsed_h": round(elapsed_h, 6),
            "resolved_paper_fills": resolved,
            "in_lane_post_fee_pnl_usd": post_fee,
            "source_binding_status": a689.get("source_binding_status"),
            "fill_backed_gate_pass": a689_pass,
        },
        "successor": {
            "wallet": RANK1_SUCCESSOR_WALLET,
            "present_in_queue": bool(successor),
            "source_binding": RANK1_SUCCESSOR_SOURCE_BINDING,
            "paper_policy_id": PASS_POLICY,
            "fresh_clock_h": A689_HOT_STANDBY_EVIDENCE_HOURS,
            "fresh_resolved_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
            "post_fee_positive_required": True,
            "feed_baseline_paper_orders": int(replay.get("paper_orders") or 0),
            "feed_baseline_resolved_signals": int(replay.get("resolved_orders") or 0),
            "feed_baseline_gross_pnl_usd": num(replay.get("paper_pnl_usd"), 0.0),
            "liveness_probe": {
                "status": "WIRED_FOR_BIND_TIME",
                "recommended_query_key": "user",
                "include_wallet": RANK1_SUCCESSOR_WALLET,
                "output": "data/research/hot_standby_source_liveness_latest.json",
                "command": (
                    "python3 scripts/report_wallet_data_api_address_form.py "
                    f"--include-wallet {RANK1_SUCCESSOR_WALLET} "
                    "--output data/research/hot_standby_source_liveness_latest.json"
                ),
            },
        },
        "decision": (
            "KEEP_A689_82C8_WAITS_NEXT_VACANCY"
            if a689_pass
            else "TERMINATE_A689_EMPTY_AND_BIND_82C8"
        ),
        "executable": bool(a689 and successor),
    }


def execute_cut(state: dict[str, Any], spec: dict[str, Any], *, now: dt.datetime) -> dict[str, Any]:
    if now < CUT_AT:
        raise ValueError(f"cut is not due before {CUT_AT.isoformat()}")
    if not spec.get("executable"):
        raise ValueError("cut requires both the a689 lane and the 0x82c8 queue row")
    if spec.get("decision") != "TERMINATE_A689_EMPTY_AND_BIND_82C8":
        return state
    lanes = [row for row in state.get("lanes") or [] if isinstance(row, dict)]
    successor = spec["successor"]
    cut_iso = now.isoformat().replace("+00:00", "Z")
    lanes = [
        row
        for row in lanes
        if _wallet(row.get("wallet")) not in {A689_HOT_STANDBY_WALLET, RANK1_SUCCESSOR_WALLET}
    ]
    lanes.append(
        {
            "wallet": RANK1_SUCCESSOR_WALLET,
            "source_binding": RANK1_SUCCESSOR_SOURCE_BINDING,
            "source_binding_status": "WIRED",
            "source_binding_authority": spec["authority"],
            "shadow_status": "RANK1_FORWARD_FILL_BACKED_READY_SHADOW",
            "paper_policy_id": PASS_POLICY,
            "copy_policy_family": PASS_POLICY,
            "paper_only": True,
            "live_orders_allowed": False,
            "ready_for_live": False,
            "standby_evidence_started_at": cut_iso,
            "standby_evidence_elapsed_h": 0.0,
            "standby_evidence_minimum_h": A689_HOT_STANDBY_EVIDENCE_HOURS,
            "standby_evidence_clock_complete": False,
            "feed_baseline_paper_orders": successor["feed_baseline_paper_orders"],
            "feed_baseline_resolved_signals": successor["feed_baseline_resolved_signals"],
            "feed_baseline_gross_pnl_usd": successor["feed_baseline_gross_pnl_usd"],
            "paper_orders": 0,
            "resolved_paper_fills": 0,
            "in_lane_fresh_resolved_signals": 0,
            "promotion_resolved_fill_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
            "resolved_fill_gap": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
            "in_lane_gross_pnl_usd": 0.0,
            "in_lane_post_fee_pnl_usd": 0.0,
            "post_fee_evidence_bar_crossed": False,
            "source_liveness": {
                **successor["liveness_probe"],
                "living_source": False,
            },
            "hot_standby_ready": False,
            "succession_eligible": False,
            "readiness_verdict": "RANK1_48H_FORWARD_EVIDENCE_ACCRUING",
            "copyintent_parity_sanity": "PASS_SAME_POLICY_FAMILY_PAPER_ONLY_NO_LIVE_SUBMITTER",
            "next": "accrue 48h and >=30 fresh resolved with positive post-fee would-PnL; no live mutation",
        }
    )
    adjudications = [
        row for row in state.get("standby_adjudications") or [] if isinstance(row, dict)
    ]
    adjudications.append(
        {
            "wallet": A689_HOT_STANDBY_WALLET,
            "status": "A689_FILL_BACKED_CUT_FAIL_EMPTY_SEAT_REBOUND",
            "adjudicated_at": cut_iso,
            "resolved_paper_fills": spec["a689"]["resolved_paper_fills"],
            "in_lane_post_fee_pnl_usd": spec["a689"]["in_lane_post_fee_pnl_usd"],
            "slot_action": f"BIND_{RANK1_SUCCESSOR_WALLET}",
            "authority": spec["authority"],
            "paper_only": True,
            "live_orders_allowed": False,
        }
    )
    return {
        **state,
        "generated_at": cut_iso,
        "lanes": lanes,
        "standby_adjudications": adjudications,
        "a689_82c8_cut": {
            "status": "EXECUTED_ATOMIC_STATE_REBIND",
            "executed_at": cut_iso,
            "authority": spec["authority"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--now")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = _parse_iso(args.now) if args.now else dt.datetime.now(dt.timezone.utc)
    if now is None:
        raise ValueError("--now must be an ISO timestamp")
    state = load_json(args.state, default={}) or {}
    queue = load_json(args.queue, default={}) or {}
    spec = build_spec(state, queue, now=now)
    atomic_write_json(args.output, spec)
    if args.execute:
        with json_file_lock(args.state):
            current = load_json(args.state, default={}) or {}
            current_spec = build_spec(current, queue, now=now)
            updated = execute_cut(current, current_spec, now=now)
            atomic_write_json(args.state, updated)
        spec["execution_status"] = "EXECUTED"
        atomic_write_json(args.output, spec)
    print(json.dumps(spec, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
