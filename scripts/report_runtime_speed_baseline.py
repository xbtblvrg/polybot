#!/usr/bin/env python3
"""Pin and compare wide-lane runtime speed metrics.

Flow stage: SELF-DEV. This reads existing telemetry and writes a report
only; it never submits orders and never mutates the live trading path.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json


DEFAULT_PINNED = "data/research/runtime_speed_baseline_pinned.json"
DEFAULT_LATEST = "data/research/runtime_speed_baseline_latest.json"
REGRESSION_RATIO = 1.20
ABSOLUTE_REGRESSION_DELTA_FLOOR_S = 1.0
HEARTBEAT_CADENCE_MAX_UNANNOTATED_S = 1200.0
BRAINLESS_RUN_STALE_SAMPLE_AGE_S = 1200.0
BRAINLESS_STEP_ROLLING_BASELINE_MIN_SAMPLES = 5
BRAINLESS_STEP_ROLLING_BASELINE_MAX_SAMPLES = 40
BRAINLESS_WORKLOAD_VARIABLE_STEPS = {
    "market_mining_cadence": "normalize by work units mined/replayed/packetized before treating wall-time ratio as actionable",
    "member_queue": "normalize by ranked queue depth before treating wall-time ratio as actionable",
    "own_redeemer": "normalize by redeemable candidate count before treating wall-time ratio as actionable",
    "state_digest": "normalize by parsed research artifact bytes/sections before treating wall-time ratio as actionable",
}
ASK_FABLE_SLOW_WALL_S = 900.0
SIGNAL_AGE_MIN_REGRESSION_SAMPLES = 10
SIGNAL_AGE_ABSOLUTE_STALENESS_S = 10.0
NON_REGRESSION_STATUS_OVERRIDES = {
    "PASS_LOW_N",
    "STALE_SAMPLE",
    "WARM_UP",
    "INFO",
    "ASK_FABLE_SLOW",
    "KNOWN_PENDING_ATTRIBUTION",
}
COMPARISON_ATTENTION_STATUSES = {"STALE_SAMPLE", "WARM_UP", "KNOWN_PENDING_ATTRIBUTION"}
METRIC_ORDER = (
    "guard_cycle_total_s",
    "guard_cycle_recent_p50_s",
    "guard_cycle_recent_p90_s",
    "signal_age_p50_s",
    "signal_age_p90_s",
    "heartbeat_cadence_latest_s",
    "heartbeat_cadence_p50_s",
    "brainless_run_duration_s",
    "ask_fable_latest_wall_s",
    "state_digest_generation_s",
    "signal_to_order_p50_s",
    "signal_to_order_p90_s",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _duration_map(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, float] = {}
    for key, raw in value.items():
        number = _coerce_float(raw)
        if number is None:
            continue
        out[str(key)] = round(number, 6)
    return out


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _mtime_dt(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return None


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iter_json_objects(text: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    idx = 0
    out: list[dict[str, Any]] = []
    while idx < len(text):
        start = text.find("{", idx)
        if start < 0:
            break
        try:
            parsed, consumed = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            idx = start + 1
            continue
        if isinstance(parsed, dict):
            out.append(parsed)
        idx = start + consumed
    return out


def _post_reclaim_run_ordinal(root: Path, sampled_started: datetime | None) -> int | None:
    if sampled_started is None:
        return None
    path = root / "data" / "research" / "brainless_ops.launchd.out"
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    marker = '{"status":"STALE_LOCK_RECLAIMED"}'
    marker_index = text.rfind(marker)
    if marker_index < 0:
        return None
    run_starts: list[datetime] = []
    for obj in _iter_json_objects(text[marker_index + len(marker) :]):
        if obj.get("kind") != "brainless_ops_state":
            continue
        started = _parse_ts(obj.get("started_at"))
        if started is not None:
            run_starts.append(started)
    if not run_starts:
        return None
    return sum(1 for started in sorted(run_starts) if started <= sampled_started)


def _timed_brainless_sample_count(root: Path) -> int:
    path = root / "data" / "research" / "brainless_ops.launchd.out"
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return 0
    count = 0
    for obj in _iter_json_objects(text):
        if obj.get("kind") != "brainless_ops_state":
            continue
        if isinstance(obj.get("step_durations"), dict) and obj["step_durations"]:
            count += 1
    return count


def _coerce_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _brainless_step_samples(root: Path) -> list[dict[str, Any]]:
    path = root / "data" / "research" / "brainless_ops.launchd.out"
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for index, obj in enumerate(_iter_json_objects(text)):
        if obj.get("kind") != "brainless_ops_state":
            continue
        steps = _duration_map(obj.get("step_durations"))
        if not steps:
            continue
        started = _parse_ts(obj.get("started_at"))
        finished = _parse_ts(obj.get("finished_at"))
        if started is None or finished is None or finished < started:
            continue
        rows.append(
            {
                "index": index,
                "started_dt": started,
                "started_at": obj.get("started_at"),
                "finished_at": obj.get("finished_at"),
                "status": obj.get("status"),
                "failed_steps": obj.get("failed_steps") if isinstance(obj.get("failed_steps"), list) else [],
                "step_durations": steps,
            }
        )
    rows.sort(key=lambda row: (row["started_dt"], row["index"]))
    return rows


def _brainless_rolling_step_baseline(root: Path, *, current_started: datetime | None) -> dict[str, Any]:
    samples = _brainless_step_samples(root)
    if current_started is not None:
        eligible = [row for row in samples if row["started_dt"] <= current_started]
        if not eligible:
            eligible = samples
    else:
        eligible = samples
    recent = eligible[-BRAINLESS_STEP_ROLLING_BASELINE_MAX_SAMPLES:]
    by_step: dict[str, list[float]] = {}
    for row in recent:
        for step, duration_s in row["step_durations"].items():
            by_step.setdefault(step, []).append(duration_s)
    step_medians: dict[str, dict[str, Any]] = {}
    for step, values in sorted(by_step.items()):
        if len(values) < BRAINLESS_STEP_ROLLING_BASELINE_MIN_SAMPLES:
            continue
        step_medians[step] = {
            "median_s": round(float(statistics.median(values)), 6),
            "sample_count": len(values),
            "min_sample_s": round(min(values), 6),
            "max_sample_s": round(max(values), 6),
        }
    return {
        "comparison_rule": "rolling_median_last_natural_brainless_runs",
        "min_samples_per_step": BRAINLESS_STEP_ROLLING_BASELINE_MIN_SAMPLES,
        "max_samples_considered": BRAINLESS_STEP_ROLLING_BASELINE_MAX_SAMPLES,
        "sample_count": len(recent),
        "eligible_sample_count": len(eligible),
        "available": len(recent) >= BRAINLESS_STEP_ROLLING_BASELINE_MIN_SAMPLES,
        "sample_window_started_at": recent[0]["started_at"] if recent else None,
        "sample_window_finished_at": recent[-1]["finished_at"] if recent else None,
        "sample_status_counts": {
            str(status): sum(1 for row in recent if row.get("status") == status)
            for status in sorted({row.get("status") for row in recent}, key=lambda item: str(item))
        },
        "step_medians": step_medians,
        "step_median_count": len(step_medians),
    }


def _percentile(values: list[float], percentile: float) -> float | None:
    clean = sorted(value for value in values if not math.isnan(value) and not math.isinf(value))
    if not clean:
        return None
    if len(clean) == 1:
        return round(clean[0], 6)
    rank = (len(clean) - 1) * percentile
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return round(clean[int(rank)], 6)
    weight = rank - low
    return round(clean[low] * (1.0 - weight) + clean[high] * weight, 6)


def _metric(value: Any, *, source: str, description: str) -> dict[str, Any]:
    numeric = _coerce_float(value)
    return {
        "value": None if numeric is None else round(numeric, 6),
        "direction": "lower_is_better",
        "source": source,
        "description": description,
    }


def _jsonl_tail(path: Path, *, limit: int = 80) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max(1, int(limit)))
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return list(rows)


def _guard_cycle_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    data = root / "data" / "research"
    guard = _load_json(data / "wallet_copy_live_guard_state.json", {})
    profile = guard.get("guard_loop_profile") if isinstance(guard.get("guard_loop_profile"), dict) else {}
    current_total = _coerce_float(profile.get("total_s_before_state_write"))

    recent_totals: list[float] = []
    for row in _jsonl_tail(data / "wallet_copy_live_guard_events.jsonl"):
        event_profile = row.get("guard_loop_profile") if isinstance(row.get("guard_loop_profile"), dict) else {}
        total = _coerce_float(event_profile.get("total_s_before_state_write"))
        if total is not None:
            recent_totals.append(total)

    metrics = {
        "guard_cycle_total_s": _metric(
            current_total,
            source="data/research/wallet_copy_live_guard_state.json.guard_loop_profile.total_s_before_state_write",
            description="Latest guard loop wall time before state write.",
        ),
        "guard_cycle_recent_p50_s": _metric(
            _percentile(recent_totals, 0.50),
            source="data/research/wallet_copy_live_guard_events.jsonl.guard_loop_profile.total_s_before_state_write",
            description="Recent guard loop p50 from retained guard events.",
        ),
        "guard_cycle_recent_p90_s": _metric(
            _percentile(recent_totals, 0.90),
            source="data/research/wallet_copy_live_guard_events.jsonl.guard_loop_profile.total_s_before_state_write",
            description="Recent guard loop p90 from retained guard events.",
        ),
    }
    evidence = {
        "guard_pid": guard.get("pid"),
        "guard_status": guard.get("status"),
        "recent_guard_cycle_count": len(recent_totals),
        "target_median_iteration_lt_s": profile.get("target_median_iteration_lt_s"),
    }
    return metrics, evidence


def _signal_age_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    deadman = _load_json(root / "data" / "research" / "order_flow_deadman_state.json", {})
    rows = deadman.get("member_signal_age") if isinstance(deadman.get("member_signal_age"), dict) else {}
    p50s: list[float] = []
    eligible_p90_rows: list[tuple[float, str, int]] = []
    low_n_p90_rows: list[tuple[float, str, int]] = []
    by_wallet: list[dict[str, Any]] = []
    for wallet, row in sorted(rows.items()):
        if not isinstance(row, dict):
            continue
        p50 = _coerce_float(row.get("signal_age_p50_s"))
        p90 = _coerce_float(row.get("signal_age_p90_s"))
        count_raw = _coerce_float(row.get("signal_age_count"))
        count = int(count_raw) if count_raw is not None else 0
        if p50 is not None:
            p50s.append(p50)
        if p90 is not None:
            if count >= SIGNAL_AGE_MIN_REGRESSION_SAMPLES:
                eligible_p90_rows.append((p90, wallet, count))
            else:
                low_n_p90_rows.append((p90, wallet, count))
        by_wallet.append(
            {
                "wallet": wallet,
                "signal_age_p50_s": None if p50 is None else round(p50, 6),
                "signal_age_p90_s": None if p90 is None else round(p90, 6),
                "signal_age_count": count,
                "eligible_intents": row.get("eligible_intents"),
                "suppressed_intents": row.get("suppressed_intents"),
            }
        )
    eligible_worst = max(eligible_p90_rows, default=None, key=lambda item: item[0])
    low_n_worst = max(low_n_p90_rows, default=None, key=lambda item: item[0])
    absolute_worst = max([*eligible_p90_rows, *low_n_p90_rows], default=None, key=lambda item: item[0])
    absolute_stale = absolute_worst is not None and absolute_worst[0] > SIGNAL_AGE_ABSOLUTE_STALENESS_S
    worst = absolute_worst if absolute_stale else eligible_worst or low_n_worst
    signal_age_p90 = _metric(
        worst[0] if worst is not None else None,
        source="data/research/order_flow_deadman_state.json.member_signal_age.*.signal_age_p90_s",
        description=(
            "Worst active-member signal-age p90 using members with enough samples for regression "
            "decisions; low-n fallback is annotated PASS_LOW_N."
        ),
    )
    signal_age_p90.update(
        {
            "low_n_policy": f"exclude signal_age_count < {SIGNAL_AGE_MIN_REGRESSION_SAMPLES} from regression decisions",
            "min_regression_samples": SIGNAL_AGE_MIN_REGRESSION_SAMPLES,
            "absolute_staleness_threshold_s": SIGNAL_AGE_ABSOLUTE_STALENESS_S,
            "worst_member": worst[1] if worst is not None else None,
            "worst_member_signal_age_count": worst[2] if worst is not None else None,
            "sample_count": worst[2] if worst is not None else None,
            "eligible_member_signal_age_count": len(eligible_p90_rows),
            "low_n_excluded_member_count": len(low_n_p90_rows),
        }
    )
    if absolute_stale:
        signal_age_p90["status"] = "REGRESSION"
        signal_age_p90["comparison_status_override"] = "REGRESSION"
        signal_age_p90["regression_reason"] = "absolute_staleness"
    elif eligible_worst is None and low_n_worst is not None:
        signal_age_p90["status"] = "PASS_LOW_N"
        signal_age_p90["comparison_status_override"] = "PASS_LOW_N"
    elif eligible_worst is not None:
        signal_age_p90["status"] = "MEASURED"
    return (
        {
            "signal_age_p50_s": _metric(
                _percentile(p50s, 0.50),
                source="data/research/order_flow_deadman_state.json.member_signal_age.*.signal_age_p50_s",
                description="Fleet median of per-member signal-age p50.",
            ),
            "signal_age_p90_s": signal_age_p90,
        },
        {
            "member_signal_age_rows": by_wallet[:12],
            "member_signal_age_count": len(by_wallet),
            "member_signal_age_min_regression_samples": SIGNAL_AGE_MIN_REGRESSION_SAMPLES,
            "member_signal_age_eligible_count": len(eligible_p90_rows),
            "member_signal_age_low_n_excluded_count": len(low_n_p90_rows),
        },
    )


def _handoff_status_times(root: Path) -> list[datetime]:
    handoff = root / "docs" / "agents" / "HANDOFF.md"
    try:
        text = handoff.read_text(errors="replace")
    except OSError:
        return []
    times: list[datetime] = []
    now = datetime.now(timezone.utc)
    for match in re.finditer(r"^##\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?Z)\s+codex STATUS", text, re.M):
        parsed = _parse_ts(match.group(1))
        if parsed is not None and parsed <= now:
            times.append(parsed)
    return sorted(times)


def _serving_heartbeat_marker_times(root: Path) -> list[datetime]:
    path = root / "data" / "research" / "codex_serving_heartbeat.jsonl"
    now = datetime.now(timezone.utc)
    times: list[datetime] = []
    for row in _jsonl_tail(path, limit=500):
        if row.get("kind") != "codex_serving_heartbeat":
            continue
        parsed = _parse_ts(row.get("generated_at"))
        if parsed is not None and parsed <= now:
            times.append(parsed)
    return sorted(times)


def _effective_status_deltas(
    status_times: list[datetime],
    marker_times: list[datetime],
) -> tuple[list[float], list[dict[str, Any]]]:
    deltas: list[float] = []
    intervals: list[dict[str, Any]] = []
    for earlier, later in zip(status_times, status_times[1:]):
        raw_delta = (later - earlier).total_seconds()
        if raw_delta <= 0:
            continue
        markers = [marker for marker in marker_times if earlier < marker < later]
        points = [earlier, *markers, later]
        segment_deltas = [
            (right - left).total_seconds()
            for left, right in zip(points, points[1:])
            if (right - left).total_seconds() > 0
        ]
        effective_delta = max(segment_deltas) if segment_deltas else raw_delta
        deltas.append(effective_delta)
        intervals.append(
            {
                "start": earlier.isoformat().replace("+00:00", "Z"),
                "end": later.isoformat().replace("+00:00", "Z"),
                "raw_delta_s": round(raw_delta, 6),
                "marker_count": len(markers),
                "max_unannotated_gap_s": round(effective_delta, 6),
                "annotated": bool(markers),
            }
        )
    return deltas, intervals


def _heartbeat_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    times = _handoff_status_times(root)
    markers = _serving_heartbeat_marker_times(root)
    deltas, intervals = _effective_status_deltas(times, markers)
    latest = deltas[-1] if deltas else None
    latest_interval = intervals[-1] if intervals else {}
    latest_metric = _metric(
        latest,
        source=(
            "docs/agents/HANDOFF.md codex STATUS timestamp deltas segmented by "
            "data/research/codex_serving_heartbeat.jsonl markers"
        ),
        description=(
            "Latest max unannotated interval between codex STATUS entries after "
            "serving heartbeat markers are applied."
        ),
    )
    latest_metric.update(
        {
            "raw_status_gap_s": latest_interval.get("raw_delta_s"),
            "serving_marker_count": latest_interval.get("marker_count", 0),
            "regression_threshold_s": HEARTBEAT_CADENCE_MAX_UNANNOTATED_S,
            "regression_rule": "REGRESSION only when max_unannotated_gap_s exceeds 1200s",
        }
    )
    p50_metric = _metric(
        statistics.median(deltas[-20:]) if deltas else None,
        source=(
            "docs/agents/HANDOFF.md codex STATUS timestamp deltas segmented by "
            "data/research/codex_serving_heartbeat.jsonl markers"
        ),
        description="Median of the last 20 effective codex STATUS intervals.",
    )
    p50_metric.update(
        {
            "regression_threshold_s": HEARTBEAT_CADENCE_MAX_UNANNOTATED_S,
            "regression_rule": "REGRESSION only when effective cadence exceeds 1200s",
        }
    )
    return (
        {
            "heartbeat_cadence_latest_s": latest_metric,
            "heartbeat_cadence_p50_s": p50_metric,
        },
        {
            "status_timestamp_count": len(times),
            "serving_heartbeat_marker_count": len(markers),
            "cadence_sample_count": len(deltas),
            "heartbeat_latest_interval": latest_interval,
            "heartbeat_long_intervals": [
                row
                for row in intervals[-20:]
                if float(row.get("raw_delta_s") or 0.0) > HEARTBEAT_CADENCE_MAX_UNANNOTATED_S
                or float(row.get("max_unannotated_gap_s") or 0.0) > HEARTBEAT_CADENCE_MAX_UNANNOTATED_S
            ],
        },
    )


def _brainless_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    state_path = root / "data" / "research" / "brainless_ops_latest.json"
    state = _load_json(state_path, {})
    started = _parse_ts(state.get("started_at"))
    finished = _parse_ts(state.get("finished_at"))
    duration = (finished - started).total_seconds() if started and finished and finished >= started else None
    now = datetime.now(timezone.utc)
    source_mtime = _mtime_dt(state_path)
    sample_age_s = (now - source_mtime).total_seconds() if source_mtime is not None else None
    failed_steps = state.get("failed_steps") if isinstance(state.get("failed_steps"), list) else []
    post_reclaim_ordinal = _post_reclaim_run_ordinal(root, started)
    step_durations = state.get("step_durations") if isinstance(state.get("step_durations"), dict) else {}
    step_duration_map = _duration_map(step_durations)
    timed_sample_count = _timed_brainless_sample_count(root)
    if step_durations and timed_sample_count == 0:
        timed_sample_count = 1
    slowest_step: dict[str, Any] | None = None
    if isinstance(state.get("slowest_step"), dict):
        slowest_step = state["slowest_step"]
    elif step_durations:
        slowest_name = max(step_durations, key=lambda key: _coerce_float(step_durations.get(key)) or 0.0)
        slowest_step = {
            "name": slowest_name,
            "duration_s": _coerce_float(step_durations.get(slowest_name)),
        }
    metric = _metric(
        duration,
        source="data/research/brainless_ops_latest.json started_at..finished_at",
        description="Latest deterministic brainless_ops run wall time.",
    )
    metric.update(
        {
            "sampled_run_started_at": state.get("started_at"),
            "sampled_run_finished_at": state.get("finished_at"),
            "sampled_run_duration_s": None if duration is None else round(duration, 6),
            "source_mtime": _iso(source_mtime),
            "sample_age_s": None if sample_age_s is None else round(max(0.0, sample_age_s), 6),
            "stale_sample_threshold_s": BRAINLESS_RUN_STALE_SAMPLE_AGE_S,
            "producer_status": state.get("status"),
            "producer_failed_steps": failed_steps,
            "post_stale_lock_reclaim_run_ordinal": post_reclaim_ordinal,
            "step_duration_count": len(step_duration_map),
            "step_duration_sum_s": round(sum(step_duration_map.values()), 6) if step_duration_map else None,
            "step_durations": step_duration_map,
            "timed_sample_count": timed_sample_count,
            "slowest_step": slowest_step,
            "rolling_step_baseline": _brainless_rolling_step_baseline(root, current_started=started),
        }
    )
    if sample_age_s is not None and sample_age_s > BRAINLESS_RUN_STALE_SAMPLE_AGE_S:
        metric.update(
            {
                "status": "STALE_SAMPLE",
                "comparison_status_override": "STALE_SAMPLE",
                "sample_status": "STALE_SAMPLE",
                "producer_defect": "brainless_ops_latest.json mtime older than 2x 600s cadence",
            }
        )
    elif post_reclaim_ordinal == 1:
        metric.update(
            {
                "status": "WARM_UP",
                "comparison_status_override": "WARM_UP",
                "sample_status": "WARM_UP",
                "sample_reason": "first completed brainless_ops run after stale lock reclaim",
            }
        )
    return (
        {"brainless_run_duration_s": metric},
        {
            "brainless_status": state.get("status"),
            "brainless_started_at": state.get("started_at"),
            "brainless_finished_at": state.get("finished_at"),
            "brainless_failed_steps": failed_steps,
            "brainless_source_mtime": _iso(source_mtime),
            "brainless_sample_age_s": None if sample_age_s is None else round(max(0.0, sample_age_s), 6),
            "brainless_post_stale_lock_reclaim_run_ordinal": post_reclaim_ordinal,
            "brainless_timed_sample_count": timed_sample_count,
            "brainless_slowest_step": slowest_step,
        },
    )


def _ask_fable_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    log_dir = root / "data" / "research" / "ask_fable_provider_logs"
    candidates = sorted(log_dir.glob("*_rc*.log")) if log_dir.exists() else []
    if not candidates:
        wall_s = None
        latest = None
    else:
        latest = candidates[-1]
        match = re.match(r"(\d{8}T\d{6}Z)_", latest.name)
        started = _parse_ts(match.group(1)) if match else None
        finished = datetime.fromtimestamp(latest.stat().st_mtime, tz=timezone.utc)
        wall_s = (finished - started).total_seconds() if started and finished >= started else None
    rc_match = re.search(r"_rc(\d+)\.log$", latest.name) if latest is not None else None
    rc = int(rc_match.group(1)) if rc_match else None
    metric = _metric(
        wall_s,
        source="data/research/ask_fable_provider_logs latest filename timestamp to mtime",
        description="Latest ask_fable provider wall time estimate.",
    )
    ask_status = None
    if wall_s is not None or rc is not None:
        ask_status = "ASK_FABLE_SLOW" if (wall_s is not None and wall_s > ASK_FABLE_SLOW_WALL_S) or rc not in (None, 0) else "INFO"
        metric.update(
            {
                "status": ask_status,
                "comparison_status_override": ask_status,
                "comparison_rule": "INFO metric; never contributes to runtime_speed comparison.status",
                "slow_threshold_s": ASK_FABLE_SLOW_WALL_S,
                "return_code": rc,
                "latest_log": None if latest is None else str(latest.relative_to(root)),
            }
        )
    return (
        {"ask_fable_latest_wall_s": metric},
        {
            "ask_fable_latest_log": None if latest is None else str(latest.relative_to(root)),
            "ask_fable_latest_rc": rc,
            "ask_fable_metric_status": ask_status,
        },
    )


def _state_digest_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    digest = _load_json(root / "data" / "research" / "state_digest.json", {})
    duration = _coerce_float(digest.get("generation_duration_s"))
    return (
        {
            "state_digest_generation_s": _metric(
                duration,
                source="data/research/state_digest.json.generation_duration_s",
                description="Latest state digest build duration.",
            )
        },
        {"state_digest_generated_at": digest.get("generated_at"), "state_digest_line_count": digest.get("line_count")},
    )


def _signal_to_order_metrics(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    live = _load_json(root / "data" / "research" / "wallet_copy_live_execution_state.json", {})
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    deltas: list[float] = []
    sample: list[dict[str, Any]] = []
    for order in orders[-500:]:
        if not isinstance(order, dict):
            continue
        submitted = _parse_ts(order.get("submitted_at") or order.get("updated_at"))
        intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        observed_ts = _coerce_float(intent.get("observed_ts"))
        if submitted is None or observed_ts is None:
            continue
        observed = datetime.fromtimestamp(observed_ts, tz=timezone.utc)
        delta = (submitted - observed).total_seconds()
        if delta < 0:
            continue
        deltas.append(delta)
        if len(sample) < 8:
            sample.append(
                {
                    "submitted_at": order.get("submitted_at"),
                    "status": order.get("status") or order.get("final_status"),
                    "source_wallet": intent.get("source_wallet"),
                    "market_slug": order.get("market_slug"),
                    "observed_to_submitted_s": round(delta, 6),
                }
            )
    return (
        {
            "signal_to_order_p50_s": _metric(
                _percentile(deltas, 0.50),
                source="data/research/wallet_copy_live_execution_state.json.orders[].source_intent.observed_ts..submitted_at",
                description="Recent live orders observed-signal to submitted p50.",
            ),
            "signal_to_order_p90_s": _metric(
                _percentile(deltas, 0.90),
                source="data/research/wallet_copy_live_execution_state.json.orders[].source_intent.observed_ts..submitted_at",
                description="Recent live orders observed-signal to submitted p90.",
            ),
        },
        {"signal_to_order_sample_count": len(deltas), "signal_to_order_sample": sample},
    )


def collect_current_metrics(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    evidence: dict[str, Any] = {}
    for metric_func in (
        _guard_cycle_metrics,
        _signal_age_metrics,
        _heartbeat_metrics,
        _brainless_metrics,
        _ask_fable_metrics,
        _state_digest_metrics,
        _signal_to_order_metrics,
    ):
        next_metrics, next_evidence = metric_func(root)
        metrics.update(next_metrics)
        evidence.update(next_evidence)
    ordered = {name: metrics[name] for name in METRIC_ORDER if name in metrics}
    return ordered, evidence


def _build_pin(now: str, metrics: dict[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "runtime_speed_baseline_pin",
        "flow_stage": "SELF-DEV",
        "created_at": now,
        "direction": "2026-07-10T19:30Z fable DIRECTION SPEED INVARIANT",
        "reason": reason,
        "regression_threshold_ratio": REGRESSION_RATIO,
        "metrics": metrics,
        "live_orders_allowed": False,
        "paper_only": True,
    }


def compare_to_baseline(metrics: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    baseline_metrics = baseline.get("metrics") if isinstance(baseline.get("metrics"), dict) else {}
    rows: list[dict[str, Any]] = []
    regressions: list[dict[str, Any]] = []
    missing: list[str] = []
    def metric_value(source: dict[str, Any], metric_name: str) -> float | None:
        metric = source.get(metric_name) if isinstance(source.get(metric_name), dict) else {}
        return _coerce_float(metric.get("value"))

    def add_brainless_step_rows(
        *,
        current_steps: dict[str, float],
        baseline_steps: dict[str, float],
        rolling_step_baseline: dict[str, Any],
        regression_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        step_rows: list[dict[str, Any]] = []
        rolling_steps = _rolling_step_values(rolling_step_baseline)
        baseline_reference = rolling_steps or baseline_steps
        comparison_rule = "per_step_rolling_median_ratio" if rolling_steps else "per_step_baseline_ratio"
        shared = sorted(set(current_steps) & set(baseline_reference))
        new_steps = sorted(set(current_steps) - set(baseline_reference))
        removed_steps = sorted(set(baseline_reference) - set(current_steps))
        for step in shared:
            current_step = current_steps[step]
            baseline_step = baseline_reference[step]
            ratio = current_step / baseline_step if baseline_step > 0 else None
            status = "REGRESSION" if ratio is not None and ratio > REGRESSION_RATIO else "PASS"
            workload_note = BRAINLESS_WORKLOAD_VARIABLE_STEPS.get(step)
            if status == "REGRESSION" and workload_note:
                status = "WORKLOAD_VARIABLE"
            absolute_delta_s = current_step - baseline_step
            absolute_delta_floor_applied = (
                status == "REGRESSION"
                and absolute_delta_s < ABSOLUTE_REGRESSION_DELTA_FLOOR_S
            )
            if absolute_delta_floor_applied:
                status = "PASS_ABS_DELTA_LT_1S"
            row = {
                "metric": f"brainless_step:{step}",
                "status": status,
                "current": round(current_step, 6),
                "baseline": round(baseline_step, 6),
                "ratio": None if ratio is None else round(ratio, 6),
                "threshold_ratio": REGRESSION_RATIO,
                "comparison_rule": comparison_rule,
            }
            if absolute_delta_floor_applied:
                row.update(
                    {
                        "absolute_delta_s": round(absolute_delta_s, 6),
                        "absolute_delta_floor_s": ABSOLUTE_REGRESSION_DELTA_FLOOR_S,
                        "classification_rule": "ratio regression ignored when absolute delta is below 1s",
                    }
                )
            if workload_note:
                row.update(
                    {
                        "workload_variable": True,
                        "normalization_required": workload_note,
                    }
                )
            if rolling_steps:
                median_row = rolling_step_baseline.get("step_medians", {}).get(step, {})
                row.update(
                    {
                        "rolling_baseline_sample_count": median_row.get("sample_count"),
                        "rolling_baseline_min_samples": rolling_step_baseline.get("min_samples_per_step"),
                        "rolling_baseline_window": {
                            "started_at": rolling_step_baseline.get("sample_window_started_at"),
                            "finished_at": rolling_step_baseline.get("sample_window_finished_at"),
                        },
                        "pinned_baseline": (
                            round(baseline_steps[step], 6) if step in baseline_steps else None
                        ),
                    }
                )
            step_rows.append(row)
            if status == "REGRESSION":
                regression_rows.append(row)
        for step in new_steps:
            step_rows.append(
                {
                    "metric": f"brainless_step:{step}",
                    "status": "NEW_STEP",
                    "current": round(current_steps[step], 6),
                    "baseline": None,
                    "ratio": None,
                    "threshold_ratio": REGRESSION_RATIO,
                    "comparison_rule": (
                        "rolling_baseline_absent_step_informative_only"
                        if rolling_steps
                        else "baseline_absent_step_informative_only"
                    ),
                }
            )
        for step in removed_steps:
            step_rows.append(
                {
                    "metric": f"brainless_step:{step}",
                    "status": "REMOVED_STEP",
                    "current": None,
                    "baseline": round(baseline_reference[step], 6),
                    "ratio": None,
                    "threshold_ratio": REGRESSION_RATIO,
                    "comparison_rule": (
                        "rolling_baseline_step_absent_current_informative_only"
                        if rolling_steps
                        else "baseline_step_absent_current_informative_only"
                    ),
                }
            )
        return step_rows

    def _rolling_step_values(rolling_step_baseline: dict[str, Any]) -> dict[str, float]:
        if not isinstance(rolling_step_baseline, dict) or not rolling_step_baseline.get("available"):
            return {}
        medians = (
            rolling_step_baseline.get("step_medians")
            if isinstance(rolling_step_baseline.get("step_medians"), dict)
            else {}
        )
        min_samples = int(
            _coerce_float(rolling_step_baseline.get("min_samples_per_step"))
            or BRAINLESS_STEP_ROLLING_BASELINE_MIN_SAMPLES
        )
        out: dict[str, float] = {}
        for step, row in medians.items():
            if not isinstance(row, dict):
                continue
            sample_count = int(_coerce_float(row.get("sample_count")) or 0)
            median_s = _coerce_float(row.get("median_s"))
            if sample_count >= min_samples and median_s is not None:
                out[str(step)] = round(median_s, 6)
        return out

    def add_metric_annotations(row: dict[str, Any], current: dict[str, Any]) -> None:
        for key in (
            "sample_age_s",
            "stale_sample_threshold_s",
            "sample_status",
            "sample_reason",
            "sampled_run_started_at",
            "sampled_run_finished_at",
            "sampled_run_duration_s",
            "source_mtime",
            "producer_status",
            "producer_failed_steps",
            "producer_defect",
            "post_stale_lock_reclaim_run_ordinal",
            "step_duration_count",
            "step_duration_sum_s",
            "timed_sample_count",
            "slowest_step",
            "rolling_step_baseline",
            "attribution_rule",
            "comparison_rule",
            "slow_threshold_s",
            "return_code",
            "latest_log",
        ):
            if key in current:
                row[key] = current.get(key)

    def add_signal_age_annotations(row: dict[str, Any], current: dict[str, Any], status_override: str) -> None:
        if status_override not in {"PASS_LOW_N", "REGRESSION"}:
            return
        row["low_n_policy"] = current.get("low_n_policy")
        row["worst_member"] = current.get("worst_member")
        row["worst_member_signal_age_count"] = current.get("worst_member_signal_age_count")
        row["sample_count"] = current.get("sample_count")
        row["min_regression_samples"] = current.get("min_regression_samples")
        row["absolute_staleness_threshold_s"] = current.get("absolute_staleness_threshold_s")
        if current.get("regression_reason"):
            row["regression_reason"] = current.get("regression_reason")

    for name in METRIC_ORDER:
        current = metrics.get(name) if isinstance(metrics.get(name), dict) else {}
        base = baseline_metrics.get(name) if isinstance(baseline_metrics.get(name), dict) else {}
        current_value = _coerce_float(current.get("value"))
        baseline_value = _coerce_float(base.get("value"))
        threshold_s = _coerce_float(current.get("regression_threshold_s"))
        status_override = str(current.get("comparison_status_override") or "").strip().upper()
        brainless_step_rows: list[dict[str, Any]] = []
        brainless_shared_step_summary: dict[str, Any] = {}
        absolute_staleness_override = (
            status_override == "REGRESSION"
            and str(current.get("regression_reason") or "").strip() == "absolute_staleness"
            and current_value is not None
        )
        if status_override in NON_REGRESSION_STATUS_OVERRIDES:
            ratio = (
                current_value / baseline_value
                if current_value is not None and baseline_value is not None and baseline_value > 0
                else None
            )
            row = {
                "metric": name,
                "status": status_override,
                "current": None if current_value is None else round(current_value, 6),
                "baseline": None if baseline_value is None else round(baseline_value, 6),
                "ratio": None if ratio is None else round(ratio, 6),
                "threshold_ratio": REGRESSION_RATIO,
            }
            add_metric_annotations(row, current)
            add_signal_age_annotations(row, current, status_override)
            rows.append(row)
            continue
        if absolute_staleness_override:
            ratio = (
                current_value / baseline_value
                if baseline_value is not None and baseline_value > 0
                else None
            )
            row = {
                "metric": name,
                "status": "REGRESSION",
                "current": round(current_value, 6),
                "baseline": None if baseline_value is None else round(baseline_value, 6),
                "ratio": None if ratio is None else round(ratio, 6),
                "threshold_ratio": REGRESSION_RATIO,
            }
            add_metric_annotations(row, current)
            add_signal_age_annotations(row, current, status_override)
            rows.append(row)
            regressions.append(row)
            continue
        if current_value is None or baseline_value is None or baseline_value <= 0:
            missing.append(name)
            rows.append(
                {
                    "metric": name,
                    "status": "MISSING_COMPARABLE_VALUE",
                    "current": current_value,
                    "baseline": baseline_value,
                    "ratio": None,
                }
            )
            continue
        if name == "brainless_run_duration_s":
            current_steps = _duration_map(current.get("step_durations"))
            baseline_steps = _duration_map(base.get("step_durations"))
            rolling_step_baseline = (
                current.get("rolling_step_baseline")
                if isinstance(current.get("rolling_step_baseline"), dict)
                else {}
            )
            rolling_steps = _rolling_step_values(rolling_step_baseline)
            baseline_reference = rolling_steps or baseline_steps
            if current_steps and baseline_reference:
                shared_steps = sorted(set(current_steps) & set(baseline_reference))
                regression_shared_steps = [
                    step for step in shared_steps if step not in BRAINLESS_WORKLOAD_VARIABLE_STEPS
                ]
                aggregate_steps = regression_shared_steps or shared_steps
                new_steps = sorted(set(current_steps) - set(baseline_reference))
                removed_steps = sorted(set(baseline_reference) - set(current_steps))
                current_shared_sum = round(sum(current_steps[step] for step in aggregate_steps), 6)
                baseline_shared_sum = round(sum(baseline_reference[step] for step in aggregate_steps), 6)
                if shared_steps and baseline_shared_sum > 0:
                    aggregate_rule = (
                        "composition_aware_rolling_step_median_sum"
                        if rolling_steps
                        else "composition_aware_shared_step_sum"
                    )
                    brainless_shared_step_summary = {
                        "comparison_rule": aggregate_rule,
                        "observed_current_total_s": round(current_value, 6),
                        "observed_baseline_total_s": round(baseline_value, 6),
                        "current_shared_step_sum_s": current_shared_sum,
                        "baseline_shared_step_sum_s": baseline_shared_sum,
                        "shared_step_count": len(aggregate_steps),
                        "raw_shared_step_count": len(shared_steps),
                        "new_step_count": len(new_steps),
                        "removed_step_count": len(removed_steps),
                        "new_steps": [{"name": step, "duration_s": current_steps[step]} for step in new_steps],
                        "removed_steps": [{"name": step, "duration_s": baseline_reference[step]} for step in removed_steps],
                    }
                    excluded_variable_steps = [
                        {
                            "name": step,
                            "current_s": current_steps[step],
                            "baseline_s": baseline_reference[step],
                            "normalization_required": BRAINLESS_WORKLOAD_VARIABLE_STEPS[step],
                        }
                        for step in shared_steps
                        if step in BRAINLESS_WORKLOAD_VARIABLE_STEPS
                    ]
                    if excluded_variable_steps:
                        brainless_shared_step_summary.update(
                            {
                                "aggregate_excludes_workload_variable_steps": True,
                                "excluded_workload_variable_step_count": len(excluded_variable_steps),
                                "excluded_workload_variable_steps": excluded_variable_steps,
                                "comparison_rule_note": (
                                    "known workload-variable steps are row-reported but excluded from "
                                    "aggregate REGRESSION until per-unit normalization exists"
                                ),
                            }
                        )
                    if rolling_steps:
                        brainless_shared_step_summary.update(
                            {
                                "baseline_source": "rolling_step_median",
                                "rolling_baseline_sample_count": rolling_step_baseline.get("sample_count"),
                                "rolling_baseline_min_samples": rolling_step_baseline.get("min_samples_per_step"),
                                "rolling_baseline_window": {
                                    "started_at": rolling_step_baseline.get("sample_window_started_at"),
                                    "finished_at": rolling_step_baseline.get("sample_window_finished_at"),
                                },
                                "pinned_baseline_shared_step_sum_s": round(
                                    sum(baseline_steps[step] for step in aggregate_steps if step in baseline_steps),
                                    6,
                                )
                                if baseline_steps
                                else None,
                            }
                        )
                    current_value = current_shared_sum
                    baseline_value = baseline_shared_sum
                    brainless_step_rows = add_brainless_step_rows(
                        current_steps=current_steps,
                        baseline_steps=baseline_steps,
                        rolling_step_baseline=rolling_step_baseline,
                        regression_rows=regressions,
                    )
        ratio = current_value / baseline_value
        if status_override == "REGRESSION":
            status = "REGRESSION"
        elif status_override == "PASS_LOW_N":
            status = "PASS_LOW_N"
        elif name.startswith("heartbeat_cadence_") and threshold_s is not None:
            status = "REGRESSION" if current_value > threshold_s else "PASS"
        else:
            status = "REGRESSION" if ratio > REGRESSION_RATIO else "PASS"
        absolute_delta_floor_applied = False
        ratio_based_status = not (name.startswith("heartbeat_cadence_") and threshold_s is not None)
        if status == "REGRESSION" and status_override != "REGRESSION" and ratio_based_status:
            absolute_delta_s = current_value - baseline_value
            absolute_delta_floor_applied = absolute_delta_s < ABSOLUTE_REGRESSION_DELTA_FLOOR_S
            if absolute_delta_floor_applied:
                status = "PASS_ABS_DELTA_LT_1S"
        if (
            name == "brainless_run_duration_s"
            and status == "REGRESSION"
            and int(_coerce_float(current.get("timed_sample_count")) or 0) < 2
        ):
            status = "KNOWN_PENDING_ATTRIBUTION"
            current["sample_status"] = "KNOWN_PENDING_ATTRIBUTION"
            current["sample_reason"] = "fewer than two fresh brainless_ops samples with per-step timings"
            current["attribution_rule"] = (
                "collect 2 fresh timed samples before rebaseline-or-targeted-fix decision"
            )
        tail_sample_grace = False
        if name == "guard_cycle_total_s" and status == "REGRESSION":
            recent_p50_current = metric_value(metrics, "guard_cycle_recent_p50_s")
            recent_p50_baseline = metric_value(baseline_metrics, "guard_cycle_recent_p50_s")
            recent_p90_current = metric_value(metrics, "guard_cycle_recent_p90_s")
            recent_p90_baseline = metric_value(baseline_metrics, "guard_cycle_recent_p90_s")
            recent_p50_ratio = (
                recent_p50_current / recent_p50_baseline
                if recent_p50_current is not None and recent_p50_baseline not in (None, 0)
                else None
            )
            recent_p90_ratio = (
                recent_p90_current / recent_p90_baseline
                if recent_p90_current is not None and recent_p90_baseline not in (None, 0)
                else None
            )
            if (
                (
                    recent_p90_current is not None
                    and current_value <= recent_p90_current
                    or recent_p90_baseline is not None
                    and current_value <= recent_p90_baseline
                )
                and recent_p50_ratio is not None
                and recent_p50_ratio <= REGRESSION_RATIO
                and recent_p90_ratio is not None
                and recent_p90_ratio <= REGRESSION_RATIO
            ):
                status = "PASS"
                tail_sample_grace = True
        row = {
            "metric": name,
            "status": status,
            "current": round(current_value, 6),
            "baseline": round(baseline_value, 6),
            "ratio": round(ratio, 6),
            "threshold_ratio": REGRESSION_RATIO,
        }
        if threshold_s is not None:
            row["threshold_s"] = round(threshold_s, 6)
            row["threshold_basis"] = "absolute_unannotated_gap"
        if tail_sample_grace:
            row["tail_sample_grace"] = "guard_latest_within_recent_p90_and_distribution_passes"
        if absolute_delta_floor_applied:
            row.update(
                {
                    "absolute_delta_s": round(current_value - baseline_value, 6),
                    "absolute_delta_floor_s": ABSOLUTE_REGRESSION_DELTA_FLOOR_S,
                    "classification_rule": "ratio regression ignored when absolute delta is below 1s",
                }
            )
        if brainless_shared_step_summary:
            row.update(brainless_shared_step_summary)
        add_metric_annotations(row, current)
        add_signal_age_annotations(row, current, status_override)
        rows.append(row)
        rows.extend(brainless_step_rows)
        if status == "REGRESSION":
            regressions.append(row)
    attention = [
        row
        for row in rows
        if str(row.get("status") or "").upper() in COMPARISON_ATTENTION_STATUSES
    ]
    if regressions:
        status = "REGRESSION"
    elif any(str(row.get("status") or "").upper() == "STALE_SAMPLE" for row in attention):
        status = "STALE_SAMPLE"
    elif any(str(row.get("status") or "").upper() == "WARM_UP" for row in attention):
        status = "WARM_UP"
    elif any(str(row.get("status") or "").upper() == "KNOWN_PENDING_ATTRIBUTION" for row in attention):
        status = "KNOWN_PENDING_ATTRIBUTION"
    else:
        status = "PASS"
    return {
        "status": status,
        "rows": rows,
        "regressions": regressions,
        "attention": attention,
        "missing_comparable_metrics": missing,
        "regression_count": len(regressions),
        "attention_count": len(attention),
    }


def build_report(
    root: Path,
    *,
    pinned_path: str = DEFAULT_PINNED,
    pin_if_missing: bool = True,
    force_pin: bool = False,
    pin_reason: str = "first speed baseline pin",
) -> dict[str, Any]:
    now = _utc_now()
    metrics, evidence = collect_current_metrics(root)
    pinned = root / pinned_path
    baseline = _load_json(pinned, {})
    pin_written = False
    if force_pin or not baseline:
        if not pin_if_missing and not force_pin:
            baseline = {}
        else:
            baseline = _build_pin(now, metrics, reason=pin_reason)
            atomic_write_json(pinned, baseline)
            pin_written = True
    comparison = compare_to_baseline(metrics, baseline) if baseline else {
        "status": "NO_BASELINE",
        "rows": [],
        "regressions": [],
        "missing_comparable_metrics": list(METRIC_ORDER),
        "regression_count": 0,
    }
    return {
        "schema_version": 1,
        "kind": "runtime_speed_baseline_report",
        "flow_stage": "SELF-DEV",
        "generated_at": now,
        "direction": "2026-07-10T19:30Z fable DIRECTION SPEED INVARIANT",
        "status": comparison["status"],
        "pin_written": pin_written,
        "pinned_path": pinned_path,
        "baseline_created_at": baseline.get("created_at") if isinstance(baseline, dict) else None,
        "regression_threshold_ratio": REGRESSION_RATIO,
        "metrics": metrics,
        "comparison": comparison,
        "evidence": evidence,
        "live_orders_allowed": False,
        "paper_only": True,
        "next_action": (
            "name and fix the speed regression same-day or revert the causing wide-lane change"
            if comparison.get("status") == "REGRESSION"
            else "restore the brainless_ops producer lane; stale samples are producer defects, not speed regressions"
            if comparison.get("status") == "STALE_SAMPLE"
            else "continue measuring; the next fresh post-outage sample starts the brainless duration observation count"
            if comparison.get("status") == "WARM_UP"
            else "collect 2 fresh per-step brainless samples before rebaseline-or-targeted-fix decision"
            if comparison.get("status") == "KNOWN_PENDING_ATTRIBUTION"
            else "continue measuring speed invariant before live-path canaries"
        ),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--pinned", default=DEFAULT_PINNED)
    parser.add_argument("--output", default=DEFAULT_LATEST)
    parser.add_argument("--pin-if-missing", action="store_true", default=True)
    parser.add_argument("--no-pin-if-missing", dest="pin_if_missing", action="store_false")
    parser.add_argument("--force-pin", action="store_true")
    parser.add_argument("--pin-reason", default="first speed baseline pin")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = Path(args.root).resolve()
    report = build_report(
        root,
        pinned_path=args.pinned,
        pin_if_missing=bool(args.pin_if_missing),
        force_pin=bool(args.force_pin),
        pin_reason=str(args.pin_reason),
    )
    atomic_write_json(root / args.output, report)
    print(
        json.dumps(
            {
                "output": args.output,
                "pinned": args.pinned,
                "status": report["status"],
                "pin_written": report["pin_written"],
                "regression_count": report["comparison"]["regression_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
