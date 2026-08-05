#!/usr/bin/env python3
"""Write the R8 event-triggered scheduler verdict packet.

Flow stage: LIVE/MEASURE/PROMOTE. This packet reproduces the preregistered
persistent-accumulator gate for the copy-event-triggered scheduler paper lane.
It is read-only: it does not alter live eligibility, caps, thresholds, rotation,
CopyIntent construction, or the live guard.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_STATE = "data/research/copy_event_triggered_cycle_scheduler_paper_lane_latest.json"
DEFAULT_OUTPUT = "data/research/copy_event_triggered_cycle_scheduler_verdict_latest.json"
RULING_ID = "2026-07-17T12:30Z-fable-ruling21-r8-event-triggered-scheduler-verdict"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _hours_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds()) / 3600.0, 6)


def _resolved_accumulator_summary(accumulator: dict[str, Any]) -> dict[str, Any]:
    rows_landed = len([row for row in accumulator.values() if isinstance(row, dict)])
    rows_resolved = 0
    recovered_candidates = 0
    pnl = 0.0
    positive_rows = 0
    window_pnl: dict[str, float] = {}
    for row in accumulator.values():
        if not isinstance(row, dict):
            continue
        if row.get("row_type") == "paper_clock_recovered_candidate" or row.get("event_id"):
            recovered_candidates += 1
        if row.get("post_fee_would_pnl_status") != "RESOLVED_POST_FEE_MEASURED":
            continue
        rows_resolved += 1
        row_pnl = _num(row.get("post_fee_would_pnl_usd"))
        pnl += row_pnl
        if row_pnl > 0.0:
            positive_rows += 1
        market_slug = str(row.get("market_slug") or "")
        if market_slug:
            window_pnl[market_slug] = window_pnl.get(market_slug, 0.0) + row_pnl
    positive_windows = sum(1 for value in window_pnl.values() if value > 0.0)
    return {
        "paper_clock_recovered_candidates": recovered_candidates,
        "paper_clock_rows_landed": rows_landed,
        "paper_clock_rows_resolved": rows_resolved,
        "paper_clock_post_fee_would_pnl_usd": round(pnl, 6),
        "paper_clock_resolved_positive_rows": positive_rows,
        "paper_clock_resolved_windows": len(window_pnl),
        "paper_clock_resolved_positive_windows": positive_windows,
    }


def _summary_gate_fields(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_clock_rows_landed": _int(summary.get("paper_clock_rows_landed")),
        "paper_clock_rows_resolved": _int(summary.get("paper_clock_rows_resolved")),
        "paper_clock_post_fee_would_pnl_usd": round(_num(summary.get("paper_clock_post_fee_would_pnl_usd")), 6),
        "paper_clock_resolved_positive_rows": _int(summary.get("paper_clock_resolved_positive_rows")),
        "paper_clock_resolved_windows": _int(summary.get("paper_clock_resolved_windows")),
        "paper_clock_resolved_positive_windows": _int(summary.get("paper_clock_resolved_positive_windows")),
    }


def _match_summary(summary_fields: dict[str, Any], accumulator_fields: dict[str, Any]) -> dict[str, Any]:
    mismatches: dict[str, dict[str, Any]] = {}
    for key, summary_value in summary_fields.items():
        accumulator_value = accumulator_fields.get(key)
        if summary_value != accumulator_value:
            mismatches[key] = {
                "summary": summary_value,
                "accumulator": accumulator_value,
            }
    return {
        "matches": not mismatches,
        "mismatches": mismatches,
    }


def _invariant_flags(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_only": state.get("paper_only"),
        "live_orders_allowed": state.get("live_orders_allowed"),
        "guard_code_touched": state.get("guard_code_touched"),
        "single_submitter_change": state.get("single_submitter_change"),
        "copyintent_parity_change": state.get("copyintent_parity_change"),
    }


def _invariants_clean(flags: dict[str, Any]) -> bool:
    return (
        flags.get("paper_only") is True
        and flags.get("live_orders_allowed") is False
        and flags.get("guard_code_touched") is False
        and flags.get("single_submitter_change") is False
        and flags.get("copyintent_parity_change") is False
    )


def build_verdict_packet(
    state: dict[str, Any],
    *,
    generated_at: str,
    source_state_path: str = DEFAULT_STATE,
    source_state_git_ref: str | None = None,
    source_state_git_commit: str | None = None,
) -> dict[str, Any]:
    summary = _as_dict(state.get("summary"))
    accumulator = _as_dict(state.get("paper_clock_accumulator"))
    summary_fields = _summary_gate_fields(summary)
    accumulator_fields = _resolved_accumulator_summary(accumulator)
    match = _match_summary(summary_fields, accumulator_fields)
    clock_start = _parse_iso(state.get("clock_start_utc"))
    clock_end = _parse_iso(state.get("clock_end_utc"))
    generated_dt = _parse_iso(state.get("generated_at")) or _parse_iso(generated_at)
    accumulation_start = _parse_iso(
        state.get("paper_clock_accumulation_started_at")
        or summary.get("paper_clock_accumulation_started_at")
    )
    clock_complete = bool(summary.get("clock_complete"))
    if generated_dt is not None and clock_end is not None:
        clock_complete = clock_complete or generated_dt >= clock_end
    flags = _invariant_flags(state)
    invariants_clean = _invariants_clean(flags)
    gate_metric = accumulator_fields["paper_clock_post_fee_would_pnl_usd"]
    gate_pass = bool(clock_complete and match["matches"] and invariants_clean and gate_metric > 0.0)
    verdict = "PASS_PREAUTHORIZED_LIVE_PROMOTION" if gate_pass else "FAIL_NO_LIVE_PROMOTION"
    return {
        "schema_version": 1,
        "kind": "copy_event_triggered_cycle_scheduler_verdict",
        "flow_stage": "LIVE/MEASURE/PROMOTE",
        "ruling_id": RULING_ID,
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "source_state": {
            "path": source_state_path,
            "git_ref": source_state_git_ref,
            "git_commit": source_state_git_commit,
            "kind": state.get("kind"),
            "generated_at": state.get("generated_at"),
            "status": state.get("status"),
        },
        "clock": {
            "clock_start_utc": state.get("clock_start_utc"),
            "clock_end_utc": state.get("clock_end_utc"),
            "clock_complete": clock_complete,
            "state_generated_after_clock_end": (
                None if generated_dt is None or clock_end is None else generated_dt >= clock_end
            ),
            "paper_clock_accumulation_started_at": (
                state.get("paper_clock_accumulation_started_at")
                or summary.get("paper_clock_accumulation_started_at")
            ),
            "nominal_clock_hours": _hours_between(clock_start, clock_end),
            "accumulation_late_start_h": _hours_between(clock_start, accumulation_start),
            "accumulation_observed_hours_until_clock_end": _hours_between(accumulation_start, clock_end),
            "late_start_caveat": (
                "persistent accumulator began after nominal clock start; RULING21 accepts n as adequate, "
                "packet names the caveat and does not rerun the clock"
            ),
        },
        "gate": {
            "basis": "persistent_accumulator_gate_metric",
            "success_rule": "clock complete and persistent accumulator post-fee would-PnL > 0 with clean invariants",
            "recovered_candidates_context": summary.get("paper_clock_recovered_candidates"),
            "recovered_candidates_context_rule": (
                "rolling current-scan count; reported for context, not compared to accumulator rows_landed"
            ),
            "summary_fields": summary_fields,
            "accumulator_recomputed": accumulator_fields,
            "summary_matches_accumulator": match["matches"],
            "summary_mismatches": match["mismatches"],
            "paper_clock_post_fee_would_pnl_usd": gate_metric,
            "gate_metric_gt_zero": gate_metric > 0.0,
            "gate_pass": gate_pass,
        },
        "context_not_gate": {
            "aggregate_post_fee_would_pnl_usd": summary.get("aggregate_post_fee_would_pnl_usd"),
            "resolved_measured_rows": summary.get("resolved_measured_rows"),
            "resolved_measured_windows": summary.get("resolved_measured_windows"),
            "rolling_scan_basis": summary.get("rolling_scan_basis"),
            "rule": "rolling aggregate is context only and never overrides the persistent accumulator gate",
        },
        "invariants": {
            "flags": flags,
            "clean": invariants_clean,
            "rule": "packet must stay paper-only; live promotion code must preserve single guard submitter and CopyIntent parity",
        },
        "promotion": {
            "verdict": verdict,
            "pre_authorized_by_ruling21b": gate_pass,
            "fresh_fable_ask_required_before_promotion": not gate_pass,
            "allowed_live_change": (
                "wire incoming copy events to trigger an in-window live guard decide cycle; no cap/threshold/eligibility changes"
                if gate_pass
                else None
            ),
            "first_live_evidence_required": (
                "event_id, cycle stamp, decide latency versus window close"
                if gate_pass
                else None
            ),
        },
        "summary": {
            "verdict": verdict,
            "gate_pass": gate_pass,
            "gate_pnl_usd": gate_metric,
            "clock_complete": clock_complete,
            "rows_landed": accumulator_fields["paper_clock_rows_landed"],
            "rows_resolved": accumulator_fields["paper_clock_rows_resolved"],
            "resolved_windows": accumulator_fields["paper_clock_resolved_windows"],
            "recovered_candidates": summary.get("paper_clock_recovered_candidates"),
            "invariants_clean": invariants_clean,
            "summary_matches_accumulator": match["matches"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument(
        "--state-git-ref",
        default="",
        help=(
            "Optional git ref to read --state from, e.g. HEAD. Use for a frozen "
            "RULING21 source snapshot instead of the mutable live latest file."
        ),
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _git_relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT)).replace("/", "/")
    except ValueError:
        return str(path)


def _load_state(path: Path, *, git_ref: str) -> tuple[dict[str, Any], str | None]:
    if not git_ref:
        return load_json(path, default={}) or {}, None
    rel = _git_relative_path(path)
    commit = subprocess.check_output(["git", "rev-parse", git_ref], cwd=ROOT, text=True).strip()
    raw = subprocess.check_output(["git", "show", f"{commit}:{rel}"], cwd=ROOT, text=True)
    data = json.loads(raw)
    return data if isinstance(data, dict) else {}, commit


def main() -> int:
    args = parse_args()
    state_path = Path(args.state)
    output_path = Path(args.output)
    git_ref = str(args.state_git_ref or "").strip()
    state, git_commit = _load_state(state_path, git_ref=git_ref)
    payload = build_verdict_packet(
        state,
        generated_at=_utc_now_iso(),
        source_state_path=str(state_path),
        source_state_git_ref=git_ref or None,
        source_state_git_commit=git_commit,
    )
    atomic_write_json(output_path, payload)
    print(
        {
            "output": str(output_path),
            "verdict": payload["summary"]["verdict"],
            "gate_pnl_usd": payload["summary"]["gate_pnl_usd"],
            "rows_resolved": payload["summary"]["rows_resolved"],
            "resolved_windows": payload["summary"]["resolved_windows"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
