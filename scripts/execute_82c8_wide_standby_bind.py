#!/usr/bin/env python3
"""Bind the 82c8 paper standby clock to Fable's reconciled WIDE receipt."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_ready_wallet_shadow_lanes import (  # noqa: E402
    A689_HOT_STANDBY_EVIDENCE_HOURS,
    PASS_POLICY,
    RANK1_SUCCESSOR_WALLET,
    WATCH_TIER_READMISSION_FRESH_FILL_GATE,
    WIDE_STANDBY_SOURCE_BINDING,
)
from src.wallet_copy.store import atomic_write_json, json_file_lock, load_json  # noqa: E402


AUTHORITY = "fable DIRECTION 2026-07-29T06:34:15Z"
EXPECTED_PACKET_CHECKSUM = "4b2e025d8068437b4513ebb3486b0b69f04e2a845cb53947cdaf7ecd39f7a5fb"
EXPECTED_SOURCE_GENERATION = "6f48db434840b3f3c81f46ad81457e08ed1a31e3e8213f38eece9998b1f88181"
EXPECTED_RUN_ID = "wide_20260729T062602Z"
DEFAULT_STATE = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_JOURNAL = "data/research/wide_direct_handoff_journal.jsonl"
DEFAULT_FRONTIER = "data/research/wide_direct_admissible_frontier_latest.json"
DEFAULT_OUTPUT = "data/research/82c8_wide_standby_binding_latest.json"
OBSERVATION_WINDOW_H = 48.0


def terminal_outcome(clock_start: str) -> dict[str, Any]:
    started = _parse_iso(clock_start)
    if started is None:
        raise ValueError("82c8 immutable standby clock missing")
    return {
        "status": "PARK_SEAT_UNFED_CLOCK",
        "deadline_at": (
            started + dt.timedelta(hours=OBSERVATION_WINDOW_H)
        ).isoformat().replace("+00:00", "Z"),
        "reason": "UNFED_CLOCK_CANNOT_MATURE",
        "terminal": True,
        "stop_writer": True,
        "deadline_extension_allowed": False,
        "promotion_authority": False,
        "live_authority": False,
    }


def precommit_existing_terminal(
    state: dict[str, Any],
    artifact: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = (
        dict(artifact.get("binding"))
        if isinstance(artifact.get("binding"), dict)
        else {}
    )
    clock_start = str(binding.get("standby_evidence_started_at") or "")
    terminal = terminal_outcome(clock_start)
    binding["terminal_outcome_on_deadline"] = terminal
    lanes = [
        {
            **row,
            "terminal_outcome_on_deadline": terminal,
        }
        if isinstance(row, dict)
        and str(row.get("wallet") or "").lower() == RANK1_SUCCESSOR_WALLET
        and row.get("source_binding") == WIDE_STANDBY_SOURCE_BINDING
        else row
        for row in state.get("lanes") or []
    ]
    return {**state, "lanes": lanes}, {**artifact, "binding": binding}


def execute_terminal_outcome(
    state: dict[str, Any],
    artifact: dict[str, Any],
    *,
    now: dt.datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = artifact.get("binding") if isinstance(artifact.get("binding"), dict) else {}
    terminal = (
        binding.get("terminal_outcome_on_deadline")
        if isinstance(binding.get("terminal_outcome_on_deadline"), dict)
        else {}
    )
    deadline = _parse_iso(terminal.get("deadline_at"))
    if deadline is None or now < deadline:
        raise ValueError("WIDE_BIND_TERMINAL_NOT_DUE")
    if terminal.get("status") != "PARK_SEAT_UNFED_CLOCK" or terminal.get("terminal") is not True:
        raise ValueError("WIDE_BIND_TERMINAL_OUTCOME_NOT_PRECOMMITTED")
    lanes = [
        row
        for row in state.get("lanes") or []
        if not (
            isinstance(row, dict)
            and str(row.get("wallet") or "").lower() == RANK1_SUCCESSOR_WALLET
            and row.get("source_binding") == WIDE_STANDBY_SOURCE_BINDING
        )
    ]
    adjudications = [
        row for row in state.get("standby_adjudications") or [] if isinstance(row, dict)
    ]
    if not any(
        str(row.get("wallet") or "").lower() == RANK1_SUCCESSOR_WALLET
        and row.get("status") == "PARK_SEAT_UNFED_CLOCK"
        for row in adjudications
    ):
        adjudications.append(
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                **terminal,
                "adjudicated_at": now.isoformat().replace("+00:00", "Z"),
                "source_binding": WIDE_STANDBY_SOURCE_BINDING,
                "slot_action": "RELEASED_TO_NEXT_MECHANISM",
                "paper_only": True,
                "live_orders_allowed": False,
                "authority": AUTHORITY,
            }
        )
    updated = {
        **state,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "lanes": lanes,
        "standby_adjudications": adjudications,
        "summary": {
            **(state.get("summary") or {}),
            "lane_count": len(lanes),
            "82c8_wide_seat_terminalized": True,
        },
        "terminal_82c8_wide_seat_decision": {
            **terminal,
            "executed_at": now.isoformat().replace("+00:00", "Z"),
            "immutable": True,
        },
    }
    executed_artifact = {
        **artifact,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "status": "PARK_SEAT_UNFED_CLOCK",
        "execution_status": "PARK_COMMITTED",
        "binding": {**binding, "terminal_executed_at": now.isoformat().replace("+00:00", "Z")},
    }
    return updated, executed_artifact


def _parse_iso(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _snapshot(journal_path: Path) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    with journal_path.open(encoding="utf-8") as handle:
        for raw in handle:
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            identity = row.get("identity") if isinstance(row.get("identity"), dict) else {}
            if (
                row.get("source_generation") == EXPECTED_SOURCE_GENERATION
                and identity.get("run_id") == EXPECTED_RUN_ID
                and row.get("input_equals_terminal") is True
            ):
                matches.append(row)
    if not matches:
        raise ValueError("WIDE_BIND_REFUSED_NO_RECONCILED_MATCHING_GENERATION")
    return max(matches, key=lambda row: str(row.get("captured_at") or ""))


def build_binding(
    state: dict[str, Any],
    snapshot: dict[str, Any],
    frontier: dict[str, Any],
    *,
    now: dt.datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    existing = next(
        (
            row
            for row in state.get("lanes") or []
            if isinstance(row, dict)
            and str(row.get("wallet") or "").lower() == RANK1_SUCCESSOR_WALLET
            and row.get("source_binding") == WIDE_STANDBY_SOURCE_BINDING
            and row.get("standby_evidence_started_at")
        ),
        None,
    )
    if existing is not None:
        raise ValueError("WIDE_BIND_REFUSED_CLOCK_RESET")
    if frontier.get("source_checksum") != EXPECTED_PACKET_CHECKSUM:
        raise ValueError("WIDE_BIND_REFUSED_PACKET_CHECKSUM_MISMATCH")
    receipts = [
        row
        for row in snapshot.get("rows") or []
        if isinstance(row, dict)
        and str(row.get("wallet") or "").lower() == RANK1_SUCCESSOR_WALLET
        and str((row.get("f1_f4_terminal") or {}).get("F3_receipt_freshness") or "") == "PASS"
    ]
    if not receipts:
        raise ValueError("WIDE_BIND_REFUSED_NO_FRESH_82C8_RECEIPT")
    receipt = max(receipts, key=lambda row: str(row.get("recorded_at") or ""))
    binding_id = hashlib.sha256(
        (
            f"{EXPECTED_PACKET_CHECKSUM}|{EXPECTED_SOURCE_GENERATION}|"
            f"{receipt.get('row_identity') or receipt.get('attempt_id')}"
        ).encode()
    ).hexdigest()
    clock_start = now.isoformat().replace("+00:00", "Z")
    lane = {
        "wallet": RANK1_SUCCESSOR_WALLET,
        "source_binding": WIDE_STANDBY_SOURCE_BINDING,
        "source_binding_status": "WIRED",
        "source_binding_authority": AUTHORITY,
        "source_binding_id": f"widebind_{binding_id[:24]}",
        "source_binding_evidence": {
            "packet_checksum": EXPECTED_PACKET_CHECKSUM,
            "source_generation": EXPECTED_SOURCE_GENERATION,
            "run_id": EXPECTED_RUN_ID,
            "snapshot_captured_at": snapshot.get("captured_at"),
            "receipt_id": receipt.get("row_identity") or receipt.get("attempt_id"),
            "attempt_id": receipt.get("attempt_id"),
            "source_event_id": receipt.get("source_event_id"),
            "transaction_hash": receipt.get("transaction_hash"),
            "receipt_recorded_at": receipt.get("recorded_at"),
            "receipt_terminal": (receipt.get("f1_f4_terminal") or {}).get("terminal"),
            "receipt_freshness": (receipt.get("f1_f4_terminal") or {}).get("F3_receipt_freshness"),
            "input_equals_terminal": snapshot.get("input_equals_terminal"),
        },
        "shadow_status": "WIDE_RECEIPT_BOUND_FORWARD_READY_SHADOW",
        "paper_policy_id": PASS_POLICY,
        "copy_policy_family": PASS_POLICY,
        "paper_only": True,
        "live_orders_allowed": False,
        "ready_for_live": False,
        "standby_evidence_started_at": clock_start,
        "standby_evidence_elapsed_h": 0.0,
        "standby_evidence_minimum_h": A689_HOT_STANDBY_EVIDENCE_HOURS,
        "standby_evidence_clock_complete": False,
        "terminal_outcome_on_deadline": terminal_outcome(clock_start),
        "feed_baseline_paper_orders": 0,
        "feed_baseline_resolved_signals": 0,
        "feed_baseline_gross_pnl_usd": 0.0,
        "paper_orders": 0,
        "resolved_paper_fills": 0,
        "in_lane_fresh_resolved_signals": 0,
        "promotion_resolved_fill_gate": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
        "resolved_fill_gap": WATCH_TIER_READMISSION_FRESH_FILL_GATE,
        "in_lane_gross_pnl_usd": 0.0,
        "in_lane_post_fee_pnl_usd": 0.0,
        "post_fee_evidence_bar_crossed": False,
        "source_liveness": {"status": "BOUND_WIDE_RECEIPT", "living_source": True},
        "hot_standby_ready": False,
        "succession_eligible": False,
        "readiness_verdict": "WIDE_RECEIPT_BOUND_48H_FORWARD_EVIDENCE_ACCRUING",
        "copyintent_parity_sanity": "PASS_PAPER_ONLY_NO_LIVE_SUBMITTER",
        "next": "accrue non-backdated 48h and >=30 resolved WIDE exact-policy fills; no live mutation",
    }
    lanes = [
        row
        for row in state.get("lanes") or []
        if isinstance(row, dict) and str(row.get("wallet") or "").lower() != RANK1_SUCCESSOR_WALLET
    ]
    lanes.append(lane)
    updated = {**state, "generated_at": clock_start, "lanes": lanes}
    artifact = {
        "schema_version": 1,
        "kind": "82c8_wide_standby_binding",
        "flow_stage": "OBSERVE/PROMOTE",
        "generated_at": clock_start,
        "status": "BOUND_WIDE_RECEIPT_NON_BACKDATED",
        "paper_only": True,
        "live_orders_allowed": False,
        "authority": AUTHORITY,
        "binding": lane,
    }
    return updated, artifact


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--journal", default=DEFAULT_JOURNAL)
    parser.add_argument("--frontier", default=DEFAULT_FRONTIER)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--now")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--precommit-existing-terminal", action="store_true")
    parser.add_argument("--execute-terminal-outcome", action="store_true")
    args = parser.parse_args()
    if args.execute_terminal_outcome:
        now = _parse_iso(args.now) if args.now else dt.datetime.now(dt.timezone.utc)
        if now is None:
            raise ValueError("--now must be an ISO timestamp")
        artifact = load_json(args.output, default={}) or {}
        with json_file_lock(args.state):
            current = load_json(args.state, default={}) or {}
            updated, artifact = execute_terminal_outcome(current, artifact, now=now)
            atomic_write_json(args.state, updated)
        atomic_write_json(args.output, artifact)
        print(json.dumps(artifact, indent=2, sort_keys=True))
        return 0
    if args.precommit_existing_terminal:
        state = load_json(args.state, default={}) or {}
        artifact = load_json(args.output, default={}) or {}
        with json_file_lock(args.state):
            current = load_json(args.state, default={}) or {}
            updated, artifact = precommit_existing_terminal(current, artifact)
            atomic_write_json(args.state, updated)
        atomic_write_json(args.output, artifact)
        print(json.dumps(artifact, indent=2, sort_keys=True))
        return 0
    now = _parse_iso(args.now) if args.now else dt.datetime.now(dt.timezone.utc)
    if now is None:
        raise ValueError("--now must be an ISO timestamp")
    snapshot = _snapshot(Path(args.journal))
    state = load_json(args.state, default={}) or {}
    frontier = load_json(args.frontier, default={}) or {}
    updated, artifact = build_binding(state, snapshot, frontier, now=now)
    if args.execute:
        with json_file_lock(args.state):
            current = load_json(args.state, default={}) or {}
            updated, artifact = build_binding(current, snapshot, frontier, now=now)
            atomic_write_json(args.state, updated)
        artifact["execution_status"] = "EXECUTED"
    else:
        artifact["execution_status"] = "DRY_RUN"
    atomic_write_json(args.output, artifact)
    print(json.dumps(artifact, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
