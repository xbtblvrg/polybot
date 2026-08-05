#!/usr/bin/env python3
"""Decompose a689 PIPELINE_LATE windows into first-touch and cycle timing."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import datetime as dt
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import report_a689_0200_tripwire as tripwire  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_WALLET = tripwire.DEFAULT_WALLET
DEFAULT_POSTFIX_START = "2026-07-16T02:29:18Z"
DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_GUARD_EVENTS = ROOT / "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_TRIPWIRE = ROOT / "data/research/a689_0200_tripwire_latest.json"
DEFAULT_OUTPUT = ROOT / "data/research/pipeline_late_decomposition_latest.json"
FIRST_TOUCH_LATE_THRESHOLD_S = 180.0
PIPELINE_LATE_REASONS = {"inventory_late_window_guard", "window_time_gte_180s"}


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _parse_iso_ts(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _pct(part: int, total: int) -> float:
    return round((float(part) / float(total)) * 100.0, 6) if total > 0 else 0.0


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = (len(sorted_values) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[int(index)]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + (upper_value - lower_value) * (index - lower)


def _summary(values: Iterable[float]) -> dict[str, Any]:
    cleaned = sorted(float(value) for value in values if value is not None)
    if not cleaned:
        return {"count": 0}
    return {
        "count": len(cleaned),
        "min": round(cleaned[0], 6),
        "p50": _round(_percentile(cleaned, 0.50)),
        "p90": _round(_percentile(cleaned, 0.90)),
        "max": round(cleaned[-1], 6),
        "avg": round(sum(cleaned) / len(cleaned), 6),
    }


def _gap_histogram(values: Iterable[float]) -> dict[str, int]:
    buckets = {
        "lte_10s": 0,
        "gt_10_lte_30s": 0,
        "gt_30_lte_60s": 0,
        "gt_60s": 0,
    }
    for value in values:
        gap = float(value)
        if gap <= 10.0:
            buckets["lte_10s"] += 1
        elif gap <= 30.0:
            buckets["gt_10_lte_30s"] += 1
        elif gap <= 60.0:
            buckets["gt_30_lte_60s"] += 1
        else:
            buckets["gt_60s"] += 1
    return buckets


def _window_key(row: dict[str, Any]) -> tuple[str, float]:
    return str(row.get("market_slug") or ""), num(row.get("window_start_s"), 0.0)


def _compact_tripwire_row(row: dict[str, Any]) -> dict[str, Any]:
    source_delta, effective_delta = tripwire._row_deltas(row)
    return {
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "window_start_s": row.get("window_start_s"),
        "first_seen_at": row.get("first_seen_at"),
        "last_seen_at": row.get("last_seen_at"),
        "first_seen_delta_s": _round(
            (_parse_iso_ts(row.get("first_seen_at")) or 0.0) - num(row.get("window_start_s"), 0.0)
            if _parse_iso_ts(row.get("first_seen_at")) is not None and num(row.get("window_start_s"), 0.0) > 0
            else None
        ),
        "source_delta_s": _round(source_delta),
        "effective_delta_s": _round(effective_delta),
        "latest_observed_ts": row.get("latest_observed_ts"),
        "effective_latest_observed_ts": row.get("effective_latest_observed_ts"),
        "source_detection_observed_ts": row.get("source_detection_observed_ts"),
        "dominant_skip_reason": row.get("dominant_skip_reason"),
        "participation_skip_category": row.get("participation_skip_category"),
        "policy_max_order_usd": row.get("policy_max_order_usd"),
        "window_budget_usd": row.get("window_budget_usd"),
        "wallet_eligible_orders": row.get("wallet_eligible_orders"),
        "our_submits": row.get("our_submits"),
        "our_fills": row.get("our_fills"),
    }


def _pipeline_late_rows(
    guard_state: dict[str, Any],
    *,
    wallet: str,
    postfix_start_ts: float,
) -> list[dict[str, Any]]:
    rows = []
    for row in tripwire._participation_rows(guard_state):
        if _norm_wallet(row.get("source_wallet")) != wallet:
            continue
        if tripwire._row_ts(row) < postfix_start_ts:
            continue
        row_class = tripwire._classify_row(row)
        if row_class not in {"PIPELINE_LATE", "WINDOW_AGED_OUT_AFTER_TIMELY_TOUCH"}:
            continue
        rows.append(row)
    rows.sort(key=lambda row: (num(row.get("window_start_s"), 0.0), str(row.get("outcome") or "")))
    return rows


def _event_participation_rows(participation: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for key in ("recent_window_rollups", "window_rollups", "rows", "retained_rows", "current_cycle_rows"):
        value = participation.get(key)
        if not isinstance(value, list):
            continue
        for row in value:
            if isinstance(row, dict):
                yield key, row


def _event_duration(profile: dict[str, Any]) -> float | None:
    direct = num(profile.get("cycle_duration_s"), 0.0) or num(profile.get("total_s_before_state_write"), 0.0)
    if direct > 0:
        return direct
    timers = profile.get("stage_timers")
    if not isinstance(timers, list):
        return None
    elapsed = [
        num(timer.get("elapsed_s"), 0.0)
        for timer in timers
        if isinstance(timer, dict) and num(timer.get("elapsed_s"), 0.0) > 0
    ]
    return max(elapsed) if elapsed else None


def _scan_guard_events(
    guard_events_path: Path,
    *,
    wallet: str,
    postfix_start_ts: float,
    target_windows: set[tuple[str, float]],
) -> tuple[dict[tuple[str, float], list[dict[str, Any]]], dict[str, Any]]:
    observations: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    cycle_durations: list[float] = []
    generated_ts_values: list[float] = []
    stage_duration_by_name: dict[str, list[float]] = defaultdict(list)
    parsed_events = 0
    malformed_events = 0

    try:
        handle = guard_events_path.open("r", encoding="utf-8")
    except OSError:
        return {}, {
            "status": "GUARD_EVENTS_UNREADABLE",
            "path": _display(guard_events_path),
            "cycle_duration_s": {"count": 0},
            "event_generated_gap_s": {"count": 0},
        }

    with handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                malformed_events += 1
                continue
            generated_ts = _parse_iso_ts(event.get("generated_at"))
            if generated_ts is None or generated_ts < postfix_start_ts:
                continue
            parsed_events += 1
            generated_ts_values.append(generated_ts)

            profile = event.get("guard_loop_profile") if isinstance(event.get("guard_loop_profile"), dict) else {}
            duration = _event_duration(profile)
            if duration is not None:
                cycle_durations.append(duration)
            for timer in profile.get("stage_timers") or []:
                if not isinstance(timer, dict):
                    continue
                duration_s = num(timer.get("duration_s"), 0.0)
                name = str(timer.get("name") or "").strip()
                if name and duration_s > 0:
                    stage_duration_by_name[name].append(duration_s)

            participation = event.get("window_participation")
            if not isinstance(participation, dict):
                continue
            seen_in_event: set[tuple[str, float, str]] = set()
            for source_key, row in _event_participation_rows(participation):
                if _norm_wallet(row.get("source_wallet")) != wallet:
                    continue
                key = _window_key(row)
                if key not in target_windows:
                    continue
                dedupe = (key[0], key[1], source_key)
                if dedupe in seen_in_event:
                    continue
                seen_in_event.add(dedupe)
                observations[key].append(
                    {
                        "generated_at": event.get("generated_at"),
                        "generated_delta_s": _round(generated_ts - key[1]),
                        "cycle": event.get("cycle"),
                        "source": source_key,
                        "first_seen_at": row.get("first_seen_at"),
                        "first_seen_delta_s": _round(
                            (_parse_iso_ts(row.get("first_seen_at")) or 0.0) - key[1]
                            if _parse_iso_ts(row.get("first_seen_at")) is not None
                            else None
                        ),
                        "last_seen_at": row.get("last_seen_at"),
                        "last_seen_delta_s": _round(
                            (_parse_iso_ts(row.get("last_seen_at")) or 0.0) - key[1]
                            if _parse_iso_ts(row.get("last_seen_at")) is not None
                            else None
                        ),
                        "dominant_skip_reason": row.get("dominant_skip_reason"),
                        "participation_skip_category": row.get("participation_skip_category"),
                        "outcomes": row.get("outcomes"),
                        "wallet_eligible_orders": row.get("wallet_eligible_orders"),
                    }
                )

    generated_ts_values.sort()
    event_gaps = [
        generated_ts_values[index] - generated_ts_values[index - 1]
        for index in range(1, len(generated_ts_values))
    ]
    top_stage_p50 = sorted(
        (
            {
                "name": name,
                "duration_s": _summary(values),
            }
            for name, values in stage_duration_by_name.items()
        ),
        key=lambda item: num(item["duration_s"].get("p50"), 0.0),
        reverse=True,
    )[:8]
    wall_time = {
        "status": "DERIVED_FROM_GUARD_EVENT_PROFILES",
        "path": _display(guard_events_path),
        "parsed_events_since_postfix": parsed_events,
        "malformed_events": malformed_events,
        "cycle_duration_s": _summary(cycle_durations),
        "event_generated_gap_s": _summary(event_gaps),
        "event_generated_gap_histogram": _gap_histogram(event_gaps),
        "top_stage_duration_p50_s": top_stage_p50,
        "rule": "cycle_duration_s uses guard_loop_profile.cycle_duration_s when present, else total_s_before_state_write/stage elapsed",
    }
    return observations, wall_time


def _window_record(
    *,
    key: tuple[str, float],
    rows: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    observations = sorted(
        observations,
        key=lambda item: (
            num(item.get("generated_delta_s"), 999999999.0),
            num(item.get("cycle"), 999999999.0),
        ),
    )
    event_deltas = [
        num(item.get("generated_delta_s"), 0.0)
        for item in observations
        if item.get("generated_delta_s") is not None
    ]
    event_gaps = [event_deltas[index] - event_deltas[index - 1] for index in range(1, len(event_deltas))]
    event_first_seen_deltas = [
        num(item.get("first_seen_delta_s"), 0.0)
        for item in observations
        if item.get("first_seen_delta_s") is not None
    ]
    late_event_deltas = [
        num(item.get("generated_delta_s"), 0.0)
        for item in observations
        if str(item.get("dominant_skip_reason") or "") in PIPELINE_LATE_REASONS
        and item.get("generated_delta_s") is not None
    ]
    state_first_seen_deltas = [
        (_parse_iso_ts(row.get("first_seen_at")) or 0.0) - key[1]
        for row in rows
        if _parse_iso_ts(row.get("first_seen_at")) is not None
    ]
    first_eval_candidates = event_first_seen_deltas + state_first_seen_deltas
    first_eval_delta = min(first_eval_candidates) if first_eval_candidates else None
    state_first_eval_delta = min(state_first_seen_deltas) if state_first_seen_deltas else None
    first_late_delta = min(late_event_deltas) if late_event_deltas else state_first_eval_delta
    outcome_rows = [_compact_tripwire_row(row) for row in sorted(rows, key=lambda row: str(row.get("outcome") or ""))]
    reason_counts = Counter(str(item.get("dominant_skip_reason") or "UNKNOWN") for item in observations)
    if not reason_counts:
        reason_counts = Counter(str(row.get("dominant_skip_reason") or "UNKNOWN") for row in rows)
    sample_limit = 12
    return {
        "market_slug": key[0],
        "window_start_s": key[1],
        "outcomes": sorted({str(row.get("outcome") or "") for row in rows if row.get("outcome")}),
        "tripwire_rows": outcome_rows,
        "evaluation_count": len(observations),
        "first_evaluation_delta_s": _round(first_eval_delta),
        "first_late_evaluation_delta_s": _round(first_late_delta),
        "first_state_row_seen_delta_s": _round(state_first_eval_delta),
        "last_evaluation_delta_s": _round(max(event_deltas) if event_deltas else None),
        "first_touch_timely_lt_180s": bool(
            first_eval_delta is not None and first_eval_delta < FIRST_TOUCH_LATE_THRESHOLD_S
        ),
        "first_evaluation_late_gte_180s": bool(
            first_eval_delta is not None and first_eval_delta >= FIRST_TOUCH_LATE_THRESHOLD_S
        ),
        "timely_first_eval_with_later_late_skip": bool(
            first_eval_delta is not None
            and first_eval_delta < FIRST_TOUCH_LATE_THRESHOLD_S
            and first_late_delta is not None
            and first_late_delta >= FIRST_TOUCH_LATE_THRESHOLD_S
        ),
        "inter_evaluation_gap_s": _summary(event_gaps),
        "inter_evaluation_gap_histogram": _gap_histogram(event_gaps),
        "dominant_skip_reason_counts": dict(sorted(reason_counts.items())),
        "raw_observation_samples": observations[:sample_limit],
        "raw_observation_sample_limit": sample_limit,
    }


def _branch(window_records: list[dict[str, Any]], wall_time: dict[str, Any]) -> tuple[str, str, str]:
    total = len(window_records)
    first_late_count = sum(1 for row in window_records if row.get("first_evaluation_late_gte_180s"))
    first_timely_count = sum(1 for row in window_records if row.get("first_touch_timely_lt_180s"))
    timely_artifact_count = sum(1 for row in window_records if row.get("timely_first_eval_with_later_late_skip"))
    cycle_p50 = num(wall_time.get("cycle_duration_s", {}).get("p50"), 0.0)
    if total > 0 and (first_late_count / total >= 0.50 or cycle_p50 >= 60.0):
        return (
            "PIPELINE_LATENCY_CONFIRMED",
            "PROMOTE_EVENT_TRIGGERED_CYCLE_SCHEDULER_PAPER_LANE_IF_EVIDENCE_GATE_PASSES",
            "Fable 04:37Z branch (a): first-touch late share >=50% or guard cycle p50 >=60s.",
        )
    if total > 0 and first_timely_count / total > 0.50:
        return (
            "TAXONOMY_ARTIFACT_FIRST_TOUCH_TIMELY",
            "FIX_TRIPWIRE_CLASSIFIER_USE_FIRST_TOUCH_DELTA",
            "Fable 04:37Z branch (b): first evaluations were majority-timely and late labels should use first-touch delta.",
        )
    return (
        "MIXED_OR_INSUFFICIENT_RAW_EVIDENCE",
        "ASK_FABLE_WITH_DECOMPOSITION_PACKET",
        "First-touch evidence is mixed or incomplete; do not self-remediate.",
    )


def build_report(
    *,
    guard_state_path: Path,
    guard_events_path: Path,
    wallet: str,
    postfix_start_iso: str,
    tripwire_path: Path | None = None,
) -> dict[str, Any]:
    wallet = _norm_wallet(wallet)
    postfix_start_ts = _parse_iso_ts(postfix_start_iso)
    if postfix_start_ts is None:
        raise ValueError(f"invalid postfix_start_iso: {postfix_start_iso!r}")
    guard_state = load_json(guard_state_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    rows = _pipeline_late_rows(guard_state, wallet=wallet, postfix_start_ts=postfix_start_ts)
    rows_by_window: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _window_key(row)
        if key[0] and key[1] > 0:
            rows_by_window[key].append(row)

    observations_by_window, wall_time = _scan_guard_events(
        guard_events_path,
        wallet=wallet,
        postfix_start_ts=postfix_start_ts,
        target_windows=set(rows_by_window),
    )
    window_records = [
        _window_record(key=key, rows=window_rows, observations=observations_by_window.get(key, []))
        for key, window_rows in sorted(rows_by_window.items(), key=lambda item: item[0][1])
    ]
    verdict, pre_ruled_action, rule = _branch(window_records, wall_time)
    first_late_count = sum(1 for row in window_records if row.get("first_evaluation_late_gte_180s"))
    first_timely_count = sum(1 for row in window_records if row.get("first_touch_timely_lt_180s"))
    timely_artifact_count = sum(1 for row in window_records if row.get("timely_first_eval_with_later_late_skip"))

    tripwire_report: dict[str, Any] = {}
    if tripwire_path is not None:
        loaded = load_json(tripwire_path, default={})
        tripwire_report = loaded if isinstance(loaded, dict) else {}

    return {
        "schema_version": 1,
        "flow_stage": "LIVE/DEFEND/MEASURE",
        "generated_at": utc_now_iso(),
        "source_wallet": wallet,
        "postfix_start_iso": postfix_start_iso,
        "guard_state_path": _display(guard_state_path),
        "guard_events_path": _display(guard_events_path),
        "tripwire_path": _display(tripwire_path) if tripwire_path is not None else None,
        "tripwire_basis": {
            "verdict": tripwire_report.get("verdict"),
            "pre_ruled_action": tripwire_report.get("pre_ruled_action"),
            "pipeline_late_rows": (tripwire_report.get("pipeline_late") or {}).get("rows")
            if isinstance(tripwire_report.get("pipeline_late"), dict)
            else None,
            "tripwire_class_counts": tripwire_report.get("tripwire_class_counts"),
        },
        "pipeline_late_rows": len(rows),
        "pipeline_late_windows": len(window_records),
        "first_evaluation_late_gte_180s": {
            "windows": first_late_count,
            "pct": _pct(first_late_count, len(window_records)),
            "threshold_s": FIRST_TOUCH_LATE_THRESHOLD_S,
        },
        "first_touch_timely_lt_180s": {
            "windows": first_timely_count,
            "pct": _pct(first_timely_count, len(window_records)),
            "threshold_s": FIRST_TOUCH_LATE_THRESHOLD_S,
        },
        "timely_first_eval_with_later_late_skip": {
            "windows": timely_artifact_count,
            "pct": _pct(timely_artifact_count, len(window_records)),
        },
        "guard_cycle_wall_time_estimate": wall_time,
        "window_records": window_records,
        "verdict": verdict,
        "pre_ruled_action": pre_ruled_action,
        "rule": rule,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", type=Path, default=DEFAULT_GUARD_STATE)
    parser.add_argument("--guard-events", type=Path, default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--tripwire", type=Path, default=DEFAULT_TRIPWIRE)
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--postfix-start", default=DEFAULT_POSTFIX_START)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(
        guard_state_path=args.guard_state,
        guard_events_path=args.guard_events,
        wallet=args.wallet,
        postfix_start_iso=args.postfix_start,
        tripwire_path=args.tripwire,
    )
    if not args.no_write:
        atomic_write_json(args.output, report)
    print(
        json.dumps(
            {
                "verdict": report["verdict"],
                "pre_ruled_action": report["pre_ruled_action"],
                "pipeline_late_rows": report["pipeline_late_rows"],
                "pipeline_late_windows": report["pipeline_late_windows"],
                "first_evaluation_late_gte_180s": report["first_evaluation_late_gte_180s"],
                "cycle_duration_s": report["guard_cycle_wall_time_estimate"].get("cycle_duration_s"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
