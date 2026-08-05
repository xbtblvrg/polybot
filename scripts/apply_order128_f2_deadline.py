#!/usr/bin/env python3
"""Apply the preregistered ORDER128 F2 paper-focus deadline decision."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.wallet_copy.store import atomic_write_json, load_json

WALLET = "0x3048d65321be3497164cdfc2996f94f98a2e7537"
FINGERPRINT = "8c39887edd0b5adbcb75bde537372fa9e4b98665c92f0579cf486488514f7a58"
ARM_AT = dt.datetime(2026, 8, 1, 1, 30, 3, tzinfo=dt.timezone.utc)
DEADLINE_AT = dt.datetime(2026, 8, 1, 5, 0, 0, tzinfo=dt.timezone.utc)


def _parse(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _target_rows(deadman: dict[str, Any]) -> list[dict[str, Any]]:
    """Every generation of the graded identity, not just the first one seen."""
    rows = (
        (((deadman.get("policy_choke") or {}).get("source_roster_drought") or {}).get("candidate_evidence") or {}).get("rows")
        or []
    )
    return [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("wallet") or "").lower() == WALLET
        and str(row.get("wide_policy_fingerprint") or "") == FINGERPRINT
    ]


def _series_row(
    deadman: dict[str, Any],
    *,
    manifest_score_run_id: str | None = None,
    cuts_agree: bool | str = "UNASSERTABLE",
) -> dict[str, Any] | None:
    checked_at = str(deadman.get("checked_at") or "")
    checked = _parse(checked_at)
    rows = _target_rows(deadman)
    if checked is None or not rows or not (ARM_AT <= checked < DEADLINE_AT):
        return None
    directs = [
        row.get("direct_source") if isinstance(row.get("direct_source"), dict) else {}
        for row in rows
    ]
    by_generation = {
        str(row.get("source_generation") or "UNASSERTABLE"): int(row.get("f2_evaluated_copyable") or 0)
        for row in rows
    }
    return {
        "deadman_checked_at": checked_at,
        "manifest_score_run_id": manifest_score_run_id or "UNASSERTABLE",
        "cuts_agree": cuts_agree,
        # graded on the best generation, so a zero can never be an artefact of row order
        "f2_evaluated_copyable": max(by_generation.values()),
        "f2_by_generation": dict(sorted(by_generation.items())),
        "generations_observed": len(rows),
        "attempts": sum(int(direct.get("attempts") or 0) for direct in directs),
        "copyable": sum(int(direct.get("copyable") or 0) for direct in directs),
        "policy_depth_pass": sum(int(direct.get("policy_depth_pass") or 0) for direct in directs),
        "packet_checksum": directs[0].get("packet_checksum") if directs else None,
    }


def _incident_cuts(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return rows
    for line in lines:
        try:
            payload = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            cut = _series_row(payload)
            if cut is not None:
                rows.append(cut)
    return rows


def build_decision(
    *,
    now: dt.datetime,
    incident_cuts: list[dict[str, Any]],
    current_deadman: dict[str, Any],
    packet: dict[str, Any],
    manifest: dict[str, Any],
    frontier: dict[str, Any],
) -> dict[str, Any]:
    if now < DEADLINE_AT:
        raise ValueError(f"ORDER128_F2_DEADLINE_NOT_DUE:{_iso(DEADLINE_AT)}")
    packet_cut = str(packet.get("score_run_id") or "")
    manifest_cut = str(manifest.get("score_run_id") or "")
    exact_cut = bool(
        packet_cut
        and packet_cut == manifest_cut
        and packet.get("deadman_checked_at") == current_deadman.get("checked_at")
    )
    current = _series_row(
        current_deadman,
        manifest_score_run_id=packet_cut if exact_cut else None,
        cuts_agree=True if exact_cut else "UNASSERTABLE",
    )
    by_checked = {
        str(row.get("deadman_checked_at") or ""): dict(row)
        for row in incident_cuts
        if isinstance(row, dict) and row.get("deadman_checked_at")
    }
    if current is not None:
        by_checked[current["deadman_checked_at"]] = current
    series = sorted(by_checked.values(), key=lambda row: row["deadman_checked_at"])
    qualifying = [
        row
        for row in series
        if row.get("cuts_agree") is True
        and int(row.get("f2_evaluated_copyable") or 0) >= 1
    ]
    distinct_qualifying = {
        (row["manifest_score_run_id"], row["deadman_checked_at"])
        for row in qualifying
    }
    passed = len(distinct_qualifying) >= 2
    for row in series:
        row["disqualified_on"] = (
            None
            if row.get("cuts_agree") is True and int(row.get("f2_evaluated_copyable") or 0) >= 1
            else "coupling_unassertable"
            if row.get("cuts_agree") is not True
            else "f2_measured_zero"
        )
    latest_cut = _parse(series[-1]["deadman_checked_at"]) if series else None
    supply_gap_s = (
        max(0.0, (DEADLINE_AT - latest_cut).total_seconds()) if latest_cut else None
    )
    # coupling can only be asserted against the live packet/manifest pair, so a cut
    # replayed from the incident stream can never carry it; say so rather than let
    # the standing read as a bar the identity could have cleared
    retained_coupling_cuts = 0
    retained_coupling_invariant = "INCIDENT_REPLAY_DOES_NOT_RETAIN_MANIFEST_COUPLING"
    max_assertable_coupled_cuts = retained_coupling_cuts + (1 if current is not None else 0)
    if not series:
        absence_class = "NO_CUT_IN_WINDOW"
    elif all(int(row.get("f2_evaluated_copyable") or 0) == 0 for row in series):
        absence_class = "MEASURED_ZERO_AT_EVERY_CUT"
    else:
        absence_class = "NONZERO_BUT_COUPLING_UNRETAINED"
    reason = "QUALIFYING_CUTS_MET" if passed else f"NO_QUALIFYING_CUT_{absence_class}"
    frontier_rows = [
        row for row in frontier.get("nearest_frontier") or [] if isinstance(row, dict)
    ]
    eligible = [
        row
        for row in frontier_rows
        if row.get("eligible") is True
        and ((row.get("checks") or {}).get("not_terminal_park_red_clock_or_measured_loser") is True)
    ]
    successor_pool = [
        row
        for row in frontier_rows
        if str(row.get("wallet") or "").lower() != WALLET
        and (
            (row.get("checks") or {}).get(
                "not_terminal_park_red_clock_or_measured_loser"
            )
            is True
        )
    ]
    fewest_deficits = min(
        (len(row.get("evidence_deficits") or []) for row in successor_pool),
        default=None,
    )
    tied = sorted(
        {
            str(row.get("wallet") or "").lower()
            for row in successor_pool
            if len(row.get("evidence_deficits") or []) == fewest_deficits
        }
    )
    tied_rows = [
        row
        for row in successor_pool
        if str(row.get("wallet") or "").lower() in tied
    ]
    rejected_on_by_wallet = {
        str(row.get("wallet") or "").lower(): sorted(
            str(value) for value in row.get("evidence_deficits") or []
        )
        for row in tied_rows
    }
    common_rejections = (
        sorted(
            set.intersection(
                *(set(values) for values in rejected_on_by_wallet.values())
            )
        )
        if rejected_on_by_wallet
        else "UNASSERTABLE"
    )
    generation_counts: dict[str, int] = {}
    for row in frontier_rows:
        generation = str(row.get("source_generation") or "UNASSERTABLE")
        generation_counts[generation] = generation_counts.get(generation, 0) + 1
    # never-lawful is a measured claim about every generation of the focus, and is
    # UNASSERTABLE when the frontier retains no row to measure
    focus_checks = [
        (row.get("checks") or {}).get("f1_walk_forward_admissible")
        for row in frontier_rows
        if str(row.get("wallet") or "").lower() == WALLET
    ]
    focus_was_never_lawful: bool | str
    if not focus_checks or any(check is None for check in focus_checks):
        focus_was_never_lawful = "UNASSERTABLE"
    else:
        focus_was_never_lawful = all(check is False for check in focus_checks)
    frontier_generation_split = {
        "row_counts": dict(sorted(generation_counts.items())),
        "supply_dropouts": frontier.get("supply_dropouts") or [],
    }
    refill = (
        {
            # more than one admissible row is not a selection problem to solve here;
            # no tiebreak is to be invented, so refuse and escalate instead
            "status": "AMBIGUOUS_ADMISSIBLE_SUCCESSORS_NO_TIEBREAK_INVENTED",
            "wallets": sorted({str(row.get("wallet") or "").lower() for row in eligible}),
            "candidate_count": frontier.get("candidate_count"),
            "eligible_count": frontier.get("eligible_count"),
        }
        if not passed and len(eligible) > 1
        else {
            "status": "SELECTED_ADMISSIBLE_SUCCESSOR",
            "wallet": eligible[0].get("wallet"),
            "wide_policy_fingerprint": eligible[0].get("wide_policy_fingerprint"),
            "candidate_count": frontier.get("candidate_count"),
            "eligible_count": frontier.get("eligible_count"),
        }
        if not passed and eligible
        else {
            "status": "NO_ADMISSIBLE_SUCCESSOR",
            "candidate_count": frontier.get("candidate_count"),
            "eligible_count": frontier.get("eligible_count"),
            "tie_at_fewest_deficits": tied,
            "rejected_on": common_rejections,
            "rejected_on_by_wallet": rejected_on_by_wallet,
        }
        if not passed
        else {"status": "FOCUS_HELD_QUALIFYING_CUTS_MET"}
    )
    return {
        "schema_version": 1,
        "kind": "order128_f2_deadline_decision",
        "flow_stage": "PROMOTE/DEFEND/SELF-DEV",
        "generated_at": _iso(now),
        "deadline_at": _iso(DEADLINE_AT),
        "executed_at": _iso(now),
        "executed_late_s": round(max(0.0, (now - DEADLINE_AT).total_seconds()), 6),
        "wallet": WALLET,
        "wide_policy_fingerprint": FINGERPRINT,
        "standing": f"{len(distinct_qualifying)}/2",
        "reason": reason,
        "cuts_observed_in_window": len(series),
        "per_cut_series": series,
        "recorded_as_measured_negative": False,
        "measured_negative_eligible": False,
        "basis": absence_class if not passed else "QUALIFYING_CUTS_MET",
        "absence_class": absence_class,
        "coupling_unassertable_cuts": sum(
            1 for row in series if row.get("cuts_agree") is not True
        ),
        "retained_coupling_cuts": retained_coupling_cuts,
        "retained_coupling_invariant": retained_coupling_invariant,
        "max_assertable_coupled_cuts": max_assertable_coupled_cuts,
        "gate_satisfiable_in_single_execution": max_assertable_coupled_cuts >= 2,
        "promotion_authority": False,
        "live_authority": False,
        "paper_only": True,
        "live_orders_allowed": False,
        "admitted": 0,
        "focus_action": "HOLD" if passed else "DROP",
        "focus_was_never_lawful": focus_was_never_lawful,
        "refill": refill,
        "frontier_generation_split": frontier_generation_split,
        "supply_gap_s": supply_gap_s,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incidents", default="data/research/order_flow_deadman_incidents.jsonl")
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--packet", default="data/research/order128_fastest_lawful_path_latest.json")
    parser.add_argument("--manifest-pointer", default="data/research/wide_exact_policy_manifest_active.json")
    parser.add_argument("--frontier", default="data/research/wide_direct_admissible_frontier_latest.json")
    parser.add_argument("--output", default="data/research/order128_f2_drop_latest.json")
    parser.add_argument("--now")
    args = parser.parse_args()
    now = _parse(args.now) if args.now else dt.datetime.now(dt.timezone.utc)
    if now is None:
        raise ValueError("invalid --now")
    pointer = load_json(args.manifest_pointer, default={}) or {}
    manifest = load_json(str(pointer.get("manifest_path") or ""), default={}) or {}
    payload = build_decision(
        now=now,
        incident_cuts=_incident_cuts(Path(args.incidents)),
        current_deadman=load_json(args.deadman, default={}) or {},
        packet=load_json(args.packet, default={}) or {},
        manifest=manifest,
        frontier=load_json(args.frontier, default={}) or {},
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
