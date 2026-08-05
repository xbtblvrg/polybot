#!/usr/bin/env python3
"""Consume fresh OrderFilled rows through the live bridge in paper-only mode."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_wallet_copy_live_guard as guard


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--guard-state",
        default="data/research/wallet_copy_live_guard_state.json",
    )
    parser.add_argument(
        "--cursor-state",
        default="data/research/copy_source_wake_paper_proof_cursor.json",
    )
    parser.add_argument(
        "--accumulator-state",
        default="",
        help="Token metadata source; defaults to the guard's read-only hot-source accumulator.",
    )
    parser.add_argument(
        "--activation-state",
        default=guard.DEFAULT_COPY_SOURCE_WAKE_ACTIVATION_STATE,
    )
    parser.add_argument(
        "--proof-state",
        default="data/research/copy_source_wake_paper_proof_latest.json",
    )
    parser.add_argument(
        "--router-state",
        default=guard.DEFAULT_COPY_SOURCE_IDENTITY_ROUTER_STATE,
    )
    return parser.parse_args()


def _live_defaults() -> argparse.Namespace:
    original = list(sys.argv)
    try:
        sys.argv = [str(Path(guard.__file__).name)]
        return guard.parse_args()
    finally:
        sys.argv = original


def main() -> int:
    args = parse_args()
    live_args = _live_defaults()
    state = guard.load_json(args.guard_state, default={})
    runtime = state.get("active_set_runtime") if isinstance(state, dict) else {}
    runtime = runtime if isinstance(runtime, dict) else {}
    selection = guard._orderfilled_live_source_selection(live_args)
    if not selection.get("gate_passed"):
        print({"status": "PAPER_GATE_CLOSED", "selection": selection})
        return 0
    rows, source_report = guard._active_member_orderfilled_direct_delta(
        polygon_jsonl=selection["source_jsonl"],
        gate_state_path=live_args.active_member_orderfilled_hot_source_state,
        accumulator_state_path=(
            args.accumulator_state
            or live_args.active_member_orderfilled_accumulator_state
        ),
        cursor_state_path=args.cursor_state,
        active_set_runtime=runtime,
        now_ts=time.time(),
        bootstrap_bytes=int(live_args.active_member_orderfilled_direct_bootstrap_bytes),
        source_identity_router_state_path=args.router_state,
    )
    generation = guard._orderfilled_fast_lane_runtime_generation(live_args, runtime)
    _routed, bridge_report = guard._alternate_transport_copyintent_bridge(
        live_args,
        active_set_runtime=runtime,
        active_set_rtds_premerge={"matching_events_delta": rows},
        now_ts=time.time(),
        generation_fence=lambda: True,
        allow_submit_stage=False,
    )
    bridge_report.pop("_live_result", None)
    activation = guard._persist_copy_source_wake_activation(
        bridge_report,
        runtime_generation=generation,
        activation_state_path=args.activation_state,
        router_state_path=args.router_state,
    )
    proof = {
        "schema_version": 1,
        "kind": "copy_source_wake_paper_proof",
        "flow_stage": "LIVE/OBSERVE/DEFEND",
        "generated_at": guard.utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_report": source_report,
        "bridge_report": bridge_report,
        "activation": activation,
    }
    prior_proof = guard.load_json(args.proof_state, default={})
    if rows or not isinstance(prior_proof, dict) or not prior_proof:
        guard.atomic_write_json(args.proof_state, proof)
    print(
        {
            "status": bridge_report.get("status"),
            "source_report": source_report,
            "input_rows": len(rows),
            "post_protection_survivors": bridge_report.get(
                "post_protection_survivors"
            ),
            "submit_stage_invocations": bridge_report.get(
                "submit_stage_invocations"
            ),
            "activation": activation,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
