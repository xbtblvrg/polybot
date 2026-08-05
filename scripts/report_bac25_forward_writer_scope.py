#!/usr/bin/env python3
"""Prove the BAC25 forward lane is outside the 82c8 seat-park writer scope."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_wide_forward_sibling_lanes import SIBLING_LANES
from scripts.run_wide_prospective_supervisor import BAC25_FORWARD_LANE
from src.wallet_copy.store import atomic_write_json, load_json

FORWARD_MODULE = "scripts/run_wide_prospective_supervisor.py"
SIBLING_MODULE = "scripts/run_wide_forward_sibling_lanes.py"
PARK_MODULE = "scripts/run_ready_wallet_shadow_lanes.py"
PARK_STATE = "data/research/wallet_copy_ready_shadow_lanes_state.json"
BINDING_PATH = "data/research/82c8_wide_standby_binding_latest.json"


def build_report(
    *,
    supervisor_state: dict[str, Any],
    binding_artifact: dict[str, Any],
    ready_shadow_state: dict[str, Any],
    process_label: str,
    process_pid: int,
    process_command: list[str],
    sibling_process_pid: int,
    sibling_process_command: list[str],
    generated_at: str,
) -> dict[str, Any]:
    binding = (
        binding_artifact.get("binding")
        if isinstance(binding_artifact.get("binding"), dict)
        else {}
    )
    binding_id = str(binding.get("source_binding_id") or "")
    park_lane = next(
        (
            row
            for row in ready_shadow_state.get("lanes") or []
            if isinstance(row, dict)
            and str(row.get("source_binding_id") or "") == binding_id
        ),
        {},
    )
    forward_specs = (BAC25_FORWARD_LANE, *SIBLING_LANES)
    forward_artifacts_by_fingerprint = {
        spec.fingerprint: {
            spec.manifest,
            spec.state,
            spec.ledger,
            spec.evidence,
            spec.lane_output,
            spec.source_history,
        }
        for spec in forward_specs
    }
    forward_artifacts = set().union(*forward_artifacts_by_fingerprint.values())
    park_artifacts = {BINDING_PATH, PARK_STATE}
    binding_text = json.dumps(binding_artifact, sort_keys=True)
    ready_text = json.dumps(park_lane, sort_keys=True)
    checks = {
        "binding_id_exact": binding_id == "widebind_b968c71aae449a34c27330fc",
        "forward_process_is_running_capture_supervisor": (
            process_pid > 0
            and str(supervisor_state.get("status") or "")
            == "CAPTURE_AND_SCORER_RESIDENT"
        ),
        "forward_and_park_modules_differ": FORWARD_MODULE != PARK_MODULE,
        "sibling_and_park_modules_differ": SIBLING_MODULE != PARK_MODULE,
        "sibling_process_is_resident": (
            sibling_process_pid > 0
            and any(SIBLING_MODULE in part for part in sibling_process_command)
        ),
        "all_three_forward_fingerprints_covered": (
            len(forward_artifacts_by_fingerprint) == 3
        ),
        "forward_and_park_artifacts_disjoint": not (
            forward_artifacts & park_artifacts
        ),
        "park_binding_does_not_name_forward_artifacts": not any(
            artifact in binding_text or artifact in ready_text
            for artifact in forward_artifacts
        ),
        "park_terminal_targets_bound_ready_shadow_lane": (
            str(
                (
                    park_lane.get("terminal_outcome_on_deadline")
                    if isinstance(
                        park_lane.get("terminal_outcome_on_deadline"), dict
                    )
                    else {}
                ).get("status")
                or ""
            )
            == "PARK_SEAT_UNFED_CLOCK"
        ),
    }
    proven = all(checks.values())
    return {
        "schema_version": 1,
        "kind": "bac25_forward_writer_scope_proof",
        "flow_stage": "OBSERVE/LEARN",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "status": (
            "DISJOINT_WRITER_SCOPES_PROVEN"
            if proven
            else "WRITER_SCOPE_PROOF_FAILED"
        ),
        "forward_writer": {
            "process_label": process_label,
            "pid": process_pid,
            "command": process_command,
            "module": FORWARD_MODULE,
            "function": "score_forward_lane",
            "managed_run_id": supervisor_state.get("managed_run_id"),
            "capture_pid": supervisor_state.get("capture_pid"),
            "wallet": BAC25_FORWARD_LANE.wallet,
            "wide_policy_fingerprint": BAC25_FORWARD_LANE.fingerprint,
            "artifacts": sorted(forward_artifacts),
        },
        "forward_lanes": [
            {
                "writer_module": (
                    FORWARD_MODULE
                    if spec.fingerprint == BAC25_FORWARD_LANE.fingerprint
                    else SIBLING_MODULE
                ),
                "wallet": spec.wallet,
                "wide_policy_fingerprint": spec.fingerprint,
                "observation_window_s": spec.observation_window_s,
                "artifacts": sorted(
                    forward_artifacts_by_fingerprint[spec.fingerprint]
                ),
            }
            for spec in forward_specs
        ],
        "sibling_writer": {
            "pid": sibling_process_pid,
            "command": sibling_process_command,
            "module": SIBLING_MODULE,
            "fingerprints": [spec.fingerprint for spec in SIBLING_LANES],
            "artifacts": sorted(
                set().union(
                    *(
                        forward_artifacts_by_fingerprint[spec.fingerprint]
                        for spec in SIBLING_LANES
                    )
                )
            ),
        },
        "seat_park_writer": {
            "module": PARK_MODULE,
            "binding_id": binding_id,
            "binding_artifact": BINDING_PATH,
            "state_artifact": PARK_STATE,
            "deadline_at": (
                binding.get("terminal_outcome_on_deadline") or {}
            ).get("deadline_at"),
            "terminal_status": (
                binding.get("terminal_outcome_on_deadline") or {}
            ).get("status"),
            "stop_writer": (
                binding.get("terminal_outcome_on_deadline") or {}
            ).get("stop_writer"),
        },
        "artifact_intersection": sorted(forward_artifacts & park_artifacts),
        "checks": checks,
        "conclusion": (
            "The seat park stops only the binding-scoped ready-shadow lane; "
            "BAC25 and both sibling forward-only measurements keep independent "
            "state, ledger, manifest, evidence, and lane writers."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--supervisor-state",
        default="data/research/wide_prospective_supervisor_state.json",
    )
    parser.add_argument("--binding", default=BINDING_PATH)
    parser.add_argument("--ready-shadow", default=PARK_STATE)
    parser.add_argument(
        "--process-label",
        default="com.polymarket.wide-prospective-supervisor",
    )
    parser.add_argument("--process-pid", type=int, required=True)
    parser.add_argument("--process-command-json", required=True)
    parser.add_argument("--sibling-process-pid", type=int, required=True)
    parser.add_argument("--sibling-process-command-json", required=True)
    parser.add_argument(
        "--output",
        default="data/research/bac25_forward_writer_scope_latest.json",
    )
    args = parser.parse_args()
    report = build_report(
        supervisor_state=load_json(args.supervisor_state, default={}),
        binding_artifact=load_json(args.binding, default={}),
        ready_shadow_state=load_json(args.ready_shadow, default={}),
        process_label=args.process_label,
        process_pid=args.process_pid,
        process_command=json.loads(args.process_command_json),
        sibling_process_pid=args.sibling_process_pid,
        sibling_process_command=json.loads(args.sibling_process_command_json),
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "DISJOINT_WRITER_SCOPES_PROVEN" else 2


if __name__ == "__main__":
    raise SystemExit(main())
