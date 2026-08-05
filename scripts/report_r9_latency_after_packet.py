#!/usr/bin/env python3
"""Build the R9 latency-after closure packet from live guard event rows."""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _event_pid(row: dict[str, Any], fallback_pid: int | None) -> int | None:
    for key in ("pid", "guard_pid", "process_pid"):
        value = row.get(key)
        if isinstance(value, int):
            return value
    guard_processes = row.get("guard_processes")
    if isinstance(guard_processes, dict):
        rows = guard_processes.get("rows")
        if isinstance(rows, list) and rows:
            value = rows[0].get("pid") if isinstance(rows[0], dict) else None
            if isinstance(value, int):
                return value
    return fallback_pid


def _sample_from_profile(
    *,
    generated_at: str | None,
    cycle: int | None,
    pid: int | None,
    profile: dict[str, Any],
) -> dict[str, Any] | None:
    total = profile.get("total_s_before_state_write")
    if total is None:
        return None
    stage_timers = profile.get("stage_timers")
    if not isinstance(stage_timers, list):
        stage_timers = []
    top_stage_timers = sorted(
        [
            {
                "name": timer.get("name"),
                "duration_s": timer.get("duration_s"),
                "elapsed_s": timer.get("elapsed_s"),
            }
            for timer in stage_timers
            if isinstance(timer, dict)
        ],
        key=lambda timer: timer.get("duration_s") or 0,
        reverse=True,
    )[:6]
    total_s = float(total)
    return {
        "generated_at": generated_at or profile.get("cycle_started_at"),
        "cycle": cycle,
        "pid": pid,
        "total_s_before_state_write": total_s,
        "over_threshold": total_s >= 30.0,
        "slow_path_cadence": profile.get("slow_path_cadence"),
        "top_stage_timers": top_stage_timers,
    }


def _sort_key(sample: dict[str, Any]) -> tuple[str, int]:
    cycle = sample.get("cycle")
    return (str(sample.get("generated_at") or ""), cycle if isinstance(cycle, int) else -1)


def build_packet(data_dir: Path, output_path: Path) -> dict[str, Any]:
    state_path = data_dir / "wallet_copy_live_guard_state.json"
    events_path = data_dir / "wallet_copy_live_guard_events.jsonl"
    state = _read_json(state_path)
    current_pid = state.get("pid") if isinstance(state.get("pid"), int) else None

    samples: list[dict[str, Any]] = []
    if events_path.exists():
        with events_path.open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                profile = row.get("guard_loop_profile")
                if not isinstance(profile, dict):
                    continue
                pid = _event_pid(row, current_pid)
                if pid != current_pid:
                    continue
                sample = _sample_from_profile(
                    generated_at=row.get("generated_at") or row.get("ts"),
                    cycle=row.get("cycle") if isinstance(row.get("cycle"), int) else None,
                    pid=pid,
                    profile=profile,
                )
                if sample is not None:
                    samples.append(sample)

    profile = state.get("guard_loop_profile")
    if isinstance(profile, dict):
        latest = _sample_from_profile(
            generated_at=state.get("generated_at"),
            cycle=state.get("cycle") if isinstance(state.get("cycle"), int) else None,
            pid=current_pid,
            profile=profile,
        )
        if latest is not None:
            seen = {(sample.get("generated_at"), sample.get("cycle")) for sample in samples}
            if (latest.get("generated_at"), latest.get("cycle")) not in seen:
                samples.append(latest)

    samples = sorted(samples, key=_sort_key)
    latest_20 = samples[-20:]
    closed = len(latest_20) >= 20 and all(not sample["over_threshold"] for sample in latest_20)
    totals = [sample["total_s_before_state_write"] for sample in latest_20]

    packet = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "source_of_truth": "docs/agents/HANDOFF.md latest Fable 2026-07-15T00:41Z DIRECTION",
        "artifact": "r9_latency_after_packet",
        "pid": current_pid,
        "current_guard_pid": current_pid,
        "acceptance_bar": (
            "20 consecutive fresh-pid cycles with "
            "guard_loop_profile.total_s_before_state_write < 30s"
        ),
        "threshold_s": 30.0,
        "goal_s": 15.0,
        "samples_available_current_pid": len(samples),
        "sample_count": len(latest_20),
        "all_under_threshold": bool(latest_20) and all(not sample["over_threshold"] for sample in latest_20),
        "status": "R9_LATENCY_CLOSED" if closed else "R9_LATENCY_PENDING_MORE_SAMPLES",
        "min_total_s": min(totals) if totals else None,
        "max_total_s": max(totals) if totals else None,
        "median_total_s": statistics.median(totals) if totals else None,
        "breach_cycles": [sample for sample in latest_20 if sample["over_threshold"]],
        "samples": latest_20,
        "admission_expansion_freeze": (
            "lifted_automatically_by_r9_closure" if closed else "holds_until_closure"
        ),
        "structural_invariants": {
            "single_live_guard": state.get("status") == "LIVE_GUARD_RUNNING",
            "live_orders_allowed": state.get("live_orders_allowed"),
            "paper_only": state.get("paper_only"),
        },
    }
    output_path.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
    return packet


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/research")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    packet = build_packet(Path(args.data_dir), Path(args.output))
    print(
        json.dumps(
            {
                "output": args.output,
                "status": packet["status"],
                "current_guard_pid": packet["current_guard_pid"],
                "sample_count": packet["sample_count"],
                "samples_available_current_pid": packet["samples_available_current_pid"],
                "min_total_s": packet["min_total_s"],
                "median_total_s": packet["median_total_s"],
                "max_total_s": packet["max_total_s"],
                "admission_expansion_freeze": packet["admission_expansion_freeze"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
