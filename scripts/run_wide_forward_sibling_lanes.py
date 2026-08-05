#!/usr/bin/env python3
"""Continuously score the two preregistered 82c8 fingerprint sibling lanes."""

from __future__ import annotations

import argparse
import fcntl
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_wide_prospective_supervisor import (
    ForwardLaneSpec,
    _path,
    score_forward_lane,
)
from src.wallet_copy.store import atomic_write_json, load_json

WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
SIBLING_LANES = (
    ForwardLaneSpec(
        run_id="82c8_8bb70201_forward_only",
        wallet=WALLET,
        fingerprint="8bb70201d2dbe6b82767a134d0e9bc2d1c42df1ac5b5807d030e1e05bf59bfe5",
        policy_family="fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
        manifest="data/research/wide_exact_policy_manifest_82c8_8bb70201_forward_only.json",
        state="data/research/82c8_8bb70201_forward_only_measurement_state.json",
        ledger="data/research/82c8_8bb70201_forward_only_orders.jsonl",
        evidence="data/research/82c8_8bb70201_forward_only_evidence_latest.json",
        atomic_output="data/research/82c8_8bb70201_forward_only_atomic_move_slice_rescore_latest.json",
        lane_output="data/research/82c8_8bb70201_forward_only_lane_latest.json",
        source_history="data/research/82c8_8bb70201_forward_only_no_retrospective_source.json",
        observation_window_s=172_800,
    ),
    ForwardLaneSpec(
        run_id="82c8_fdd8af33_forward_only",
        wallet=WALLET,
        fingerprint="fdd8af33f97936e6e8f4b22bbfab79621b02d1f31868543bdf3bb692e2f3c268",
        policy_family="fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
        manifest="data/research/wide_exact_policy_manifest_82c8_fdd8af33_forward_only.json",
        state="data/research/82c8_fdd8af33_forward_only_measurement_state.json",
        ledger="data/research/82c8_fdd8af33_forward_only_orders.jsonl",
        evidence="data/research/82c8_fdd8af33_forward_only_evidence_latest.json",
        atomic_output="data/research/82c8_fdd8af33_forward_only_atomic_move_slice_rescore_latest.json",
        lane_output="data/research/82c8_fdd8af33_forward_only_lane_latest.json",
        source_history="data/research/82c8_fdd8af33_forward_only_no_retrospective_source.json",
        observation_window_s=172_800,
    ),
)


def acquire_lock(path: str | Path):
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError("WIDE_FORWARD_SIBLING_LANES_LOCK_HELD")
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def run_cycle(
    *,
    supervisor_state: dict[str, Any],
    resolution_path: str,
    scorer: Callable[..., list[dict[str, Any]]] = score_forward_lane,
) -> dict[str, Any]:
    managed_run = str(supervisor_state.get("managed_run_id") or "")
    seed_run = str(supervisor_state.get("seed_run_id") or "")
    if (
        supervisor_state.get("status") != "CAPTURE_AND_SCORER_RESIDENT"
        or not managed_run
        or not seed_run
    ):
        return {
            "status": "WAITING_FOR_WIDE_CAPTURE",
            "managed_run_id": managed_run or None,
            "seed_run_id": seed_run or None,
            "lanes": [],
        }
    seed_alpha = _path("alpha_decay_report", seed_run, ".json")
    polygon = _path(
        "polygon_orderfilled_ws_capture_alpha_decay", managed_run, ".jsonl"
    )
    if not Path(seed_alpha).exists() or not Path(polygon).exists():
        return {
            "status": "WAITING_FOR_WIDE_CAPTURE_ARTIFACTS",
            "managed_run_id": managed_run,
            "seed_run_id": seed_run,
            "seed_alpha": seed_alpha,
            "polygon_jsonl": polygon,
            "lanes": [],
        }
    lanes = []
    for spec in SIBLING_LANES:
        try:
            results = scorer(
                spec=spec,
                seed_alpha=seed_alpha,
                polygon_jsonl=polygon,
                resolution_path=resolution_path,
                direct_event=None,
            )
            error = None
        except Exception as exc:
            results = []
            error = f"{type(exc).__name__}: {exc}"
        lanes.append(
            {
                "run_id": spec.run_id,
                "wide_policy_fingerprint": spec.fingerprint,
                "manifest": spec.manifest,
                "state": spec.state,
                "ledger": spec.ledger,
                "evidence": spec.evidence,
                "lane_output": spec.lane_output,
                "results": results,
                "error": error,
            }
        )
    failed_results = [
        {
            "run_id": lane["run_id"],
            "returncode": result.get("returncode"),
            "cmd": result.get("cmd"),
        }
        for lane in lanes
        for result in lane["results"]
        if result.get("ok") is not True
    ] + [
        {
            "run_id": lane["run_id"],
            "error": lane["error"],
        }
        for lane in lanes
        if lane["error"] is not None
    ]
    return {
        "status": (
            "SIBLING_FORWARD_LANES_SCORED"
            if not failed_results
            else "SIBLING_FORWARD_LANES_CYCLE_FAILED"
        ),
        "managed_run_id": managed_run,
        "seed_run_id": seed_run,
        "seed_alpha": seed_alpha,
        "polygon_jsonl": polygon,
        "lanes": lanes,
        "failed_results": failed_results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--supervisor-state",
        default="data/research/wide_prospective_supervisor_state.json",
    )
    parser.add_argument(
        "--resolutions",
        default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
    )
    parser.add_argument(
        "--state",
        default="data/research/wide_forward_sibling_lanes_state.json",
    )
    parser.add_argument(
        "--lock-file",
        default="data/research/wide_forward_sibling_lanes.lock",
    )
    parser.add_argument("--sleep-s", type=float, default=30.0)
    parser.add_argument("--iterations", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    os.chdir(ROOT)
    args = parse_args()
    try:
        lock = acquire_lock(args.lock_file)
    except RuntimeError as exc:
        if str(exc) != "WIDE_FORWARD_SIBLING_LANES_LOCK_HELD":
            raise
        print(
            '{"status":"WIDE_FORWARD_SIBLING_LANES_LOCK_HELD",'
            '"exit_reason":"healthy_singleton_already_resident"}'
        )
        return 0
    iteration = 0
    try:
        while args.iterations <= 0 or iteration < args.iterations:
            cycle = run_cycle(
                supervisor_state=load_json(args.supervisor_state, default={}),
                resolution_path=args.resolutions,
            )
            atomic_write_json(
                args.state,
                {
                    "schema_version": 1,
                    "kind": "wide_forward_sibling_lanes_state",
                    "flow_stage": "OBSERVE/LEARN",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "pid": os.getpid(),
                    "iteration": iteration + 1,
                    **cycle,
                },
            )
            iteration += 1
            if args.iterations <= 0 or iteration < args.iterations:
                time.sleep(max(0.25, args.sleep_s))
    finally:
        lock.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
