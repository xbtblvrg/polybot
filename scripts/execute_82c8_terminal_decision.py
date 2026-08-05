#!/usr/bin/env python3
"""Execute the immutable 48-hour terminal decision for the 0x82c8 paper seat."""

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

from src.wallet_copy.store import atomic_write_json, json_file_lock, load_json  # noqa: E402

WALLET = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
SOURCE_BINDING = "FABLE_20260723_82C8_READY_SHADOW"
CLOCK_START = dt.datetime(2026, 7, 23, 18, 32, 33, 543905, tzinfo=dt.timezone.utc)
DECISION_AT = dt.datetime(2026, 7, 25, 18, 32, 33, 543905, tzinfo=dt.timezone.utc)
REQUIRED_RESOLVED = 30
PARK = "PARK_READY_SHADOW_EVIDENCE_ABSENCE"
LEGACY_PARK = "PARK_VOLUME_STANDBY_PAPER_ONLY"
DEFAULT_STATE = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_OUTPUT = "data/research/82c8_terminal_decision_latest.json"


def _parse_iso(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _wallet(value: Any) -> str:
    return str(value or "").strip().lower()


def _lane(state: dict[str, Any]) -> dict[str, Any]:
    return next(
        (
            row
            for row in state.get("lanes") or []
            if isinstance(row, dict) and _wallet(row.get("wallet")) == WALLET
        ),
        {},
    )


def _own_source_rows(lane: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    candidates: list[dict[str, Any]] = []
    rejected: list[str] = []
    rows = lane.get("post_bind_resolved_rows") or []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            rejected.append(f"row_{index}:not_object")
            continue
        resolved_at = _parse_iso(row.get("resolved_at") or row.get("submitted_at"))
        reasons = []
        order_id = str(row.get("order_id") or "").strip()
        if not order_id:
            reasons.append("immutable_order_id_missing")
        if _wallet(row.get("source_wallet")) != WALLET:
            reasons.append("foreign_wallet")
        if resolved_at is None or not CLOCK_START <= resolved_at <= DECISION_AT:
            reasons.append("outside_immutable_clock")
        if row.get("resolved") is not True:
            reasons.append("not_resolved")
        if row.get("copyintent_parity") is not True:
            reasons.append("copyintent_parity_not_exact")
        try:
            float(row["post_fee_pnl_usd"])
        except (KeyError, TypeError, ValueError):
            reasons.append("post_fee_pnl_missing")
        if reasons:
            rejected.append(f"row_{index}:{','.join(reasons)}")
            continue
        candidates.append({**row, "_resolved_at": resolved_at})
    by_identity: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        by_identity.setdefault(str(row["order_id"]), []).append(row)
    accepted: list[dict[str, Any]] = []
    for order_id, identity_rows in sorted(by_identity.items()):
        fingerprints = {
            json.dumps(
                {key: value for key, value in row.items() if key != "_resolved_at"},
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            for row in identity_rows
        }
        if len(fingerprints) != 1:
            rejected.append(f"order_id_{order_id}:conflicting_duplicate")
            continue
        accepted.append(identity_rows[0])
    accepted.sort(key=lambda row: (row["_resolved_at"], str(row.get("order_id") or "")))
    return accepted, rejected


def build_decision(state: dict[str, Any], *, now: dt.datetime) -> dict[str, Any]:
    lane = _lane(state)
    rows, rejected = _own_source_rows(lane)
    pnls = [float(row["post_fee_pnl_usd"]) for row in rows]
    midpoint = len(pnls) // 2
    first = pnls[:midpoint]
    second = pnls[midpoint:]
    exact_clock = _parse_iso(lane.get("standby_evidence_started_at")) == CLOCK_START
    exact_binding = lane.get("source_binding") == SOURCE_BINDING
    aggregate = round(sum(pnls), 6)
    first_pnl = round(sum(first), 6)
    second_pnl = round(sum(second), 6)
    checks = {
        "wallet_present": bool(lane),
        "source_binding_exact": exact_binding,
        "clock_start_exact": exact_clock,
        "resolved_gte_30": len(rows) >= REQUIRED_RESOLVED,
        "post_fee_aggregate_positive": aggregate > 0.0,
        "first_half_positive": bool(first) and first_pnl > 0.0,
        "second_half_positive": bool(second) and second_pnl > 0.0,
        "copyintent_parity_exact": bool(rows)
        and all(row.get("copyintent_parity") is True for row in rows),
        "historical_or_foreign_rows_excluded": True,
    }
    due = now >= DECISION_AT
    all_pass = all(checks.values())
    status = (
        "NOT_DUE"
        if not due
        else "READY_FOR_FABLE_PROMOTION_HANDOFF"
        if all_pass
        else PARK
    )
    return {
        "schema_version": 1,
        "kind": "82c8_48h_terminal_decision",
        "flow_stage": "PROMOTE/ROTATE/SELF-DEV",
        "generated_at": _iso(now),
        "wallet": WALLET,
        "source_binding": SOURCE_BINDING,
        "clock_start": _iso(CLOCK_START),
        "decision_at": _iso(DECISION_AT),
        "required_hours": 48,
        "required_resolved": REQUIRED_RESOLVED,
        "due": due,
        "status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "evidence": {
            "source": "ready-shadow lane post_bind_resolved_rows only",
            "resolved": len(rows),
            "post_fee_pnl_usd": aggregate,
            "first_half": {"resolved": len(first), "post_fee_pnl_usd": first_pnl},
            "second_half": {"resolved": len(second), "post_fee_pnl_usd": second_pnl},
            "rejected_rows": rejected,
            "generic_72h_reporter_allowed": False,
            "historical_clearance_rows_allowed": False,
        },
        "checks": checks,
        "all_pass": all_pass,
        "next": (
            f"run --execute at or after {_iso(DECISION_AT)}"
            if not due
            else "Fable promotion adjudication; no automatic live mutation"
            if all_pass
            else "terminal PARK and release capacity claim"
        ),
    }


def apply_terminal_decision(
    state: dict[str, Any], decision: dict[str, Any], *, now: dt.datetime
) -> dict[str, Any]:
    if now < DECISION_AT:
        raise ValueError(f"82c8 decision is not due before {_iso(DECISION_AT)}")
    prior = state.get("terminal_82c8_decision")
    if isinstance(prior, dict) and prior.get("status") in {PARK, LEGACY_PARK}:
        return {
            **state,
            "standby_adjudications": [
                {
                    **row,
                    "status": PARK,
                    "reason": "READY_SHADOW_EVIDENCE_ABSENT_AT_DEADLINE",
                }
                if isinstance(row, dict)
                and _wallet(row.get("wallet")) == WALLET
                and row.get("status") == LEGACY_PARK
                else row
                for row in state.get("standby_adjudications") or []
            ],
            "terminal_82c8_decision": {
                **prior,
                "status": PARK,
                "reason": "READY_SHADOW_EVIDENCE_ABSENT_AT_DEADLINE",
            },
        }
    if decision.get("status") != PARK:
        return state
    lanes = [
        row
        for row in state.get("lanes") or []
        if not (isinstance(row, dict) and _wallet(row.get("wallet")) == WALLET)
    ]
    adjudications = [
        row for row in state.get("standby_adjudications") or [] if isinstance(row, dict)
    ]
    if not any(
        _wallet(row.get("wallet")) == WALLET and row.get("status") == PARK
        for row in adjudications
    ):
        adjudications.append(
            {
                "wallet": WALLET,
                "status": PARK,
                "reason": "READY_SHADOW_EVIDENCE_ABSENT_AT_DEADLINE",
                "adjudicated_at": _iso(now),
                "clock_start": _iso(CLOCK_START),
                "decision_at": _iso(DECISION_AT),
                "resolved": decision["evidence"]["resolved"],
                "post_fee_pnl_usd": decision["evidence"]["post_fee_pnl_usd"],
                "slot_action": "RELEASED_TO_NEXT_MECHANISM",
                "paper_only": True,
                "live_orders_allowed": False,
                "authority": "fable DIRECTION 2026-07-25T15:45:00Z",
            }
        )
    return {
        **state,
        "generated_at": _iso(now),
        "lanes": lanes,
        "standby_adjudications": adjudications,
        "summary": {
            **(state.get("summary") or {}),
            "lane_count": len(lanes),
            "82c8_terminalized": True,
            "capacity_claims_released": 1,
        },
        "terminal_82c8_decision": {
            "status": PARK,
            "reason": "READY_SHADOW_EVIDENCE_ABSENT_AT_DEADLINE",
            "executed_at": _iso(now),
            "clock_start": _iso(CLOCK_START),
            "decision_at": _iso(DECISION_AT),
            "immutable": True,
            "capacity_claim_released": True,
        },
        "readiness_regeneration": {
            "status": "COMPLETE_AFTER_TERMINAL_RELEASE",
            "released_wallet": WALLET,
            "released_at": _iso(now),
            "remaining_lane_count": len(lanes),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
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
    decision = build_decision(state, now=now)
    if args.execute:
        if not decision["due"]:
            atomic_write_json(args.output, decision)
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 2
        with json_file_lock(args.state):
            current = load_json(args.state, default={}) or {}
            decision = build_decision(current, now=now)
            updated = apply_terminal_decision(current, decision, now=now)
            if updated != current:
                atomic_write_json(args.state, updated)
            decision["execution_status"] = (
                "PARK_COMMITTED"
                if decision["status"] == PARK
                else "HANDOFF_ONLY_NO_LIVE_MUTATION"
            )
    atomic_write_json(args.output, decision)
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
