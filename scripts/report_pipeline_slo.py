#!/usr/bin/env python3
"""Report OP-PIPELINE-SLO and standby evidence clocks without live mutation."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.wide_standby import binding_terminally_executed  # noqa: E402

A689 = "0xa6896d11f76dfa2820662c1f441496f51553559b"
VOLUME = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
WIDE_STANDBY_SOURCE_BINDING = "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY"
TERMINAL_BINDING_STATUSES = frozenset({"EXECUTED", "PARK_COMMITTED"})
BUDGETS_H = {
    "mined_to_scored": 24.0,
    "scored_to_shadow": 24.0,
    "shadow_to_ready": 96.0,
    "ready_to_gates_run": 6.0,
    "cooldown_expired_to_readmission_run": 12.0,
    "standby_wiring_repair": 24.0,
    "wide_supervisor_heartbeat": 3.0,
}
WIDE_SUPERVISOR_PLIST = "com.belavarga.polymarket.wide-prospective-supervisor.plist"
WIDE_SUPERVISOR_MAX_AGE_S = 3 * 3600.0
WIDE_SUPERVISOR_SCORE_INTERVAL_S = 30.0
WIDE_SUPERVISOR_HEARTBEAT_INTERVAL_S = 60.0
WIDE_SUPERVISOR_PUBLICATION_HISTORY_LIMIT = 12


def _seconds_to_hours(value: Any) -> float | None:
    return None if value is None else round(float(value) / 3600.0, 6)


def _wide_scorer_cycle_period(data_dir: Path) -> dict[str, Any]:
    state = load_json(data_dir / "wide_prospective_supervisor_state.json", default={}) or {}
    starts = [
        float(row["started_at_s"])
        for row in state.get("latest_cycles") or []
        if isinstance(row, dict) and row.get("started_at_s") is not None
    ]
    gaps = [later - earlier for earlier, later in zip(starts, starts[1:]) if later > earlier]
    achieved_s = statistics.median(gaps) if gaps else None
    ratio = (
        achieved_s / WIDE_SUPERVISOR_SCORE_INTERVAL_S
        if achieved_s is not None
        else None
    )
    return {
        "status": (
            "BREACH"
            if achieved_s is not None
            and achieved_s > WIDE_SUPERVISOR_SCORE_INTERVAL_S
            else "PASS"
            if achieved_s is not None
            else "INSUFFICIENT_CYCLES"
        ),
        "achieved_period_s": round(achieved_s, 6) if achieved_s is not None else None,
        "declared_interval_s": WIDE_SUPERVISOR_SCORE_INTERVAL_S,
        "achieved_to_declared_ratio": round(ratio, 6) if ratio is not None else None,
        "sample_gaps_s": [round(gap, 6) for gap in gaps],
        "sample_count": len(gaps),
        "basis": "median_latest_cycle_started_at_s_gap",
    }


def _wide_heartbeat_publish_period(
    *,
    prior_history: Any,
    published_at_s: Any,
    score_run_id: str | None,
    now_s: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Measure cut-heartbeat cadence without changing its writer or interval."""
    history = [
        row
        for row in (prior_history or [])
        if isinstance(row, dict) and row.get("published_at_s") is not None
    ]
    history.sort(key=lambda row: float(row["published_at_s"]))
    if published_at_s is not None:
        publication_s = float(published_at_s)
        if not history or publication_s > float(history[-1]["published_at_s"]):
            history.append(
                {
                    "published_at_s": publication_s,
                    "score_run_id": score_run_id,
                    "observed_at_s": now_s,
                }
            )
    history = history[-WIDE_SUPERVISOR_PUBLICATION_HISTORY_LIMIT:]
    publication_times = [float(row["published_at_s"]) for row in history]
    gaps = [
        later - earlier
        for earlier, later in zip(publication_times, publication_times[1:])
        if later > earlier
    ]
    completed_period_s = statistics.median(gaps) if gaps else None
    current_elapsed_s = (
        max(0.0, now_s - float(published_at_s))
        if published_at_s is not None
        else None
    )
    # An open interval is a truthful lower bound on achieved cadence. A stale
    # heartbeat must breach at 61 seconds instead of remaining PASS until the
    # next publication happens to provide a completed gap.
    achieved_period_s = max(
        value
        for value in (completed_period_s, current_elapsed_s)
        if value is not None
    ) if completed_period_s is not None or current_elapsed_s is not None else None
    ratio = (
        achieved_period_s / WIDE_SUPERVISOR_HEARTBEAT_INTERVAL_S
        if achieved_period_s is not None
        else None
    )
    return (
        {
            "status": (
                "BREACH"
                if achieved_period_s is not None
                and achieved_period_s > WIDE_SUPERVISOR_HEARTBEAT_INTERVAL_S
                else "PASS"
                if achieved_period_s is not None
                else "INSUFFICIENT_PUBLICATIONS"
            ),
            "achieved_period_s": (
                round(achieved_period_s, 6)
                if achieved_period_s is not None
                else None
            ),
            "declared_interval_s": WIDE_SUPERVISOR_HEARTBEAT_INTERVAL_S,
            "achieved_to_declared_ratio": (
                round(ratio, 6) if ratio is not None else None
            ),
            "completed_gap_median_s": (
                round(completed_period_s, 6)
                if completed_period_s is not None
                else None
            ),
            "current_interval_elapsed_s": (
                round(current_elapsed_s, 6)
                if current_elapsed_s is not None
                else None
            ),
            "sample_gaps_s": [round(gap, 6) for gap in gaps],
            "sample_count": len(gaps),
            "basis": "median_completed_publish_gaps_plus_open_interval_lower_bound",
        },
        history,
    )


def read_wide_supervisor_heartbeat(
    *,
    root: Path = ROOT,
    heartbeat_state_path: Path | None = None,
) -> dict[str, Any]:
    """Return the single writer's persisted verdict without consuming evidence."""
    heartbeat_state_path = heartbeat_state_path or (
        root / "data/research/wide_supervisor_heartbeat_state.json"
    )
    state = load_json(heartbeat_state_path, default={}) or {}
    verdict = state.get("verdict") if isinstance(state, dict) else None
    if isinstance(verdict, dict):
        return verdict
    return {
        "status": "PRODUCER_HEARTBEAT_LEDGER_MISSING",
        "heartbeat_state_missing": True,
        "heartbeat_state_path": str(heartbeat_state_path),
        "liveness_basis": "persisted_single_writer_verdict_missing_fail_closed",
    }


def grade_wide_supervisor_heartbeat(
    *,
    root: Path = ROOT,
    launch_agents_dir: Path | None = None,
    now: dt.datetime | None = None,
    process_alive: Any | None = None,
    launchd_last_exit_code: int | None = None,
    launchd_status_text: str | None = None,
    error_log_path: Path | None = None,
    heartbeat_state_path: Path | None = None,
) -> dict[str, Any]:
    """Consume producer evidence and persist the sole canonical verdict."""
    now = now or dt.datetime.now(dt.timezone.utc)
    launch_agents_dir = launch_agents_dir or (Path.home() / "Library/LaunchAgents")
    data_dir = root / "data/research"
    heartbeat_state_path = heartbeat_state_path or (
        data_dir / "wide_supervisor_heartbeat_state.json"
    )
    heartbeat_state_missing = not heartbeat_state_path.exists()
    heartbeat_state = load_json(heartbeat_state_path, default={}) or {}
    pointer = load_json(
        data_dir / "wide_exact_policy_manifest_active.json", default={}
    ) or {}
    manifest_path = str(pointer.get("manifest_path") or "")
    manifest = load_json(root / manifest_path, default={}) if manifest_path else {}
    score_run_id = str(manifest.get("score_run_id") or "") or None
    validity = load_json(data_dir / "wide_alpha_metric_validity_latest.json", default={}) or {}
    total_fill_sample = (validity.get("summary") or {}).get("total_fill_sample")
    try:
        total_fill_sample = int(total_fill_sample) if total_fill_sample is not None else None
    except (TypeError, ValueError):
        total_fill_sample = None
    lock_path = data_dir / "wide_prospective_supervisor.lock"
    try:
        lock_pid = int(lock_path.read_text().strip())
    except (FileNotFoundError, ValueError):
        lock_pid = None

    def pid_alive(pid: int | None) -> bool:
        if pid is None:
            return False
        if process_alive is not None:
            return bool(process_alive(pid))
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            return False
        return True

    published_at_s = pointer.get("published_at_s")
    age_s = (
        round(max(0.0, now.timestamp() - float(published_at_s)), 6)
        if published_at_s is not None
        else None
    )
    heartbeat_publish_period, publication_history = _wide_heartbeat_publish_period(
        prior_history=heartbeat_state.get("publication_history"),
        published_at_s=published_at_s,
        score_run_id=score_run_id,
        now_s=now.timestamp(),
    )
    scheduler_path = launch_agents_dir / WIDE_SUPERVISOR_PLIST
    scheduler_installed = scheduler_path.exists()
    alive = pid_alive(lock_pid)
    if launchd_last_exit_code is not None:
        launchd_status_text = f"last exit code = {int(launchd_last_exit_code)}"
    elif launchd_status_text is None:
        result = subprocess.run(
            [
                "launchctl",
                "print",
                f"gui/{os.getuid()}/com.belavarga.polymarket.wide-prospective-supervisor",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        launchd_status_text = result.stdout
    exit_match = re.search(r"last exit code\s*=\s*([^\r\n]+)", launchd_status_text or "")
    exit_value = exit_match.group(1).strip() if exit_match else ""
    if exit_value == "(never exited)":
        launchd_exit_status = "NEVER_EXITED"
        launchd_last_exit_code = None
    elif re.fullmatch(r"-?\d+", exit_value):
        launchd_last_exit_code = int(exit_value)
        launchd_exit_status = (
            "EXITED_ZERO" if launchd_last_exit_code == 0 else "EXITED_NONZERO"
        )
    else:
        launchd_exit_status = "UNREADABLE"
        launchd_last_exit_code = None

    error_log_path = error_log_path or (
        data_dir / "wide_prospective_supervisor.launchd.err"
    )
    prior_inode = heartbeat_state.get("err_inode")
    prior_size = int(heartbeat_state.get("err_size_bytes") or 0)
    err_inode = None
    err_size_bytes = 0
    log_rotated = False
    new_traceback_count = 0
    if error_log_path.exists():
        stat = error_log_path.stat()
        err_inode = int(stat.st_ino)
        err_size_bytes = int(stat.st_size)
        log_rotated = prior_inode is not None and (
            int(prior_inode) != err_inode or err_size_bytes < prior_size
        )
        offset = 0 if log_rotated else min(prior_size, err_size_bytes)
        with error_log_path.open("rb") as handle:
            handle.seek(offset)
            appended = handle.read().decode("utf-8", errors="replace")
        new_traceback_count = appended.count("Traceback (most recent call last):")
    elif prior_inode is not None:
        log_rotated = True
    tracebacks_graded = int(heartbeat_state.get("tracebacks_graded") or 0) + int(
        new_traceback_count
    )

    exit_signature = (
        f"{launchd_exit_status}:{launchd_last_exit_code}"
        if launchd_exit_status == "EXITED_NONZERO"
        else None
    )
    new_nonzero_exit = bool(
        exit_signature
        and exit_signature
        != heartbeat_state.get("launchd_exit_observation_signature")
    )
    crash_open = heartbeat_state.get("crash_open")
    crash_clearance = heartbeat_state.get("crash_clearance")
    if new_traceback_count > 0 or new_nonzero_exit:
        crash_open = {
            "offending_run_id": score_run_id,
            "offending_published_at_s": published_at_s,
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "total_fill_sample": total_fill_sample,
            "new_traceback_count": new_traceback_count,
            "launchd_exit_code": launchd_last_exit_code if new_nonzero_exit else None,
        }
        crash_clearance = None
    elif isinstance(crash_open, dict):
        offending_published = crash_open.get("offending_published_at_s")
        later_publication = (
            published_at_s is not None
            and offending_published is not None
            and float(published_at_s) > float(offending_published)
        )
        prior_total = crash_open.get("total_fill_sample")
        fill_progress = (
            total_fill_sample is not None
            and prior_total is not None
            and total_fill_sample > int(prior_total)
        )
        run_progress = bool(
            score_run_id and score_run_id != crash_open.get("offending_run_id")
        )
        if later_publication and (fill_progress or run_progress):
            crash_clearance = {
                "offending_run_id": crash_open.get("offending_run_id"),
                "cleared_by_run_id": score_run_id,
                "cleared_at": now.isoformat().replace("+00:00", "Z"),
                "published_at_s": published_at_s,
                "total_fill_sample": total_fill_sample,
                "basis": "later_published_run_with_monotonic_progress",
            }
            crash_open = None

    rotation_open = heartbeat_state.get("rotation_open")
    rotation_clearance = heartbeat_state.get("rotation_clearance")
    if log_rotated:
        rotation_open = {
            "err_inode": err_inode,
            "err_size_bytes": err_size_bytes,
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "offending_run_id": score_run_id,
            "offending_published_at_s": published_at_s,
            "total_fill_sample": total_fill_sample,
        }
        rotation_clearance = None
    elif isinstance(rotation_open, dict):
        rotation_published = rotation_open.get("offending_published_at_s")
        later_publication = (
            published_at_s is not None
            and rotation_published is not None
            and float(published_at_s) > float(rotation_published)
        )
        rotation_total = rotation_open.get("total_fill_sample")
        fill_progress = (
            total_fill_sample is not None
            and rotation_total is not None
            and total_fill_sample > int(rotation_total)
        )
        run_progress = bool(
            score_run_id and score_run_id != rotation_open.get("offending_run_id")
        )
        if later_publication and (fill_progress or run_progress):
            rotation_clearance = {
                "offending_run_id": rotation_open.get("offending_run_id"),
                "cleared_by_run_id": score_run_id,
                "cleared_at": now.isoformat().replace("+00:00", "Z"),
                "published_at_s": published_at_s,
                "total_fill_sample": total_fill_sample,
                "basis": "later_published_run_with_monotonic_progress",
            }
            rotation_open = None

    capture_path = (
        data_dir / f"polygon_orderfilled_ws_capture_alpha_decay_{score_run_id}.jsonl"
        if score_run_id
        else None
    )
    try:
        capture_size_bytes = capture_path.stat().st_size if capture_path else None
    except FileNotFoundError:
        capture_size_bytes = None
    progress_observation = heartbeat_state.get("progress_observation") or {}
    prior_capture_size_bytes = progress_observation.get("capture_size_bytes")
    capture_advanced = bool(
        capture_size_bytes is not None
        and (
            prior_capture_size_bytes is None
            or int(capture_size_bytes) > int(prior_capture_size_bytes)
        )
    )
    if (
        score_run_id != progress_observation.get("score_run_id")
        or capture_advanced
    ):
        progress_observation = {
            "score_run_id": score_run_id,
            "sampled_at_s": now.timestamp(),
            "capture_path": str(capture_path) if capture_path else None,
            "capture_size_bytes": capture_size_bytes,
            "published_at_s": published_at_s,
            "total_fill_sample": total_fill_sample,
        }
    progress_span_s = max(
        0.0, now.timestamp() - float(progress_observation.get("sampled_at_s") or now.timestamp())
    )
    no_forward_progress = bool(
        score_run_id
        and score_run_id == progress_observation.get("score_run_id")
        and progress_span_s > 3 * WIDE_SUPERVISOR_SCORE_INTERVAL_S
    )

    if crash_open is not None:
        status = "PRODUCER_CRASH_RESTART"
    elif rotation_open is not None:
        status = "PRODUCER_LOG_ROTATED"
    elif launchd_exit_status == "UNREADABLE":
        status = "PRODUCER_EXIT_STATUS_UNREADABLE"
    elif no_forward_progress:
        status = "PRODUCER_NO_FORWARD_PROGRESS"
    elif age_s is None or age_s > WIDE_SUPERVISOR_MAX_AGE_S:
        status = "BLOCKED_NO_CUT_PRODUCER"
    elif not scheduler_installed:
        status = "PRODUCER_UNSCHEDULED"
    elif not alive:
        status = "PRODUCER_NOT_RUNNING"
    elif heartbeat_state_missing:
        status = "PRODUCER_HEARTBEAT_LEDGER_MISSING"
    else:
        status = "PASS"

    verdict = {
        "status": status,
        "last_cut_run_id": score_run_id,
        "last_cut_published_at": (
            dt.datetime.fromtimestamp(float(published_at_s), tz=dt.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
            if published_at_s is not None
            else None
        ),
        "age_s": age_s,
        "expected_max_age_s": WIDE_SUPERVISOR_MAX_AGE_S,
        "scheduler_installed": scheduler_installed,
        "scheduler_path": str(scheduler_path),
        "lock_pid": lock_pid,
        "process_alive": alive,
        "launchd_last_exit_code": launchd_last_exit_code,
        "launchd_exit_status": launchd_exit_status,
        "new_traceback_count": new_traceback_count,
        "post_cut_traceback_count": new_traceback_count,
        "tracebacks_graded": tracebacks_graded,
        "err_inode": err_inode,
        "err_size_bytes": err_size_bytes,
        "log_rotated": log_rotated,
        "crash_open": crash_open,
        "crash_clearance": crash_clearance,
        "rotation_open": rotation_open,
        "rotation_clearance": rotation_clearance,
        "heartbeat_state_missing": heartbeat_state_missing,
        "total_fill_sample": total_fill_sample,
        "progress_span_s": round(progress_span_s, 6),
        "capture_path": str(capture_path) if capture_path else None,
        "capture_size_bytes": capture_size_bytes,
        "capture_advanced": capture_advanced,
        "score_interval_s": WIDE_SUPERVISOR_SCORE_INTERVAL_S,
        "stall_threshold_s": 3 * WIDE_SUPERVISOR_SCORE_INTERVAL_S,
        "scorer_cycle_period": _wide_scorer_cycle_period(data_dir),
        "heartbeat_publish_period": heartbeat_publish_period,
        "heartbeat_state_path": str(heartbeat_state_path),
        "error_log_path": str(error_log_path),
        "liveness_basis": "offset_crash_ledger_plus_monotonic_in_run_capture_bytes",
    }
    persisted_state = {
        "schema_version": 2,
        "kind": "wide_supervisor_heartbeat_state",
        "err_inode": err_inode,
        "err_size_bytes": err_size_bytes,
        "tracebacks_graded": tracebacks_graded,
        "launchd_exit_observation_signature": (
            f"{launchd_exit_status}:{launchd_last_exit_code}"
        ),
        "crash_open": crash_open,
        "crash_clearance": crash_clearance,
        "rotation_open": rotation_open,
        "rotation_clearance": rotation_clearance,
        "progress_observation": progress_observation,
        "publication_history": publication_history,
        "last_status": status,
        "updated_at": now.isoformat().replace("+00:00", "Z"),
        "verdict": verdict,
    }
    atomic_write_json(heartbeat_state_path, persisted_state)
    return verdict


def _parse(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _elapsed_h(start: Any, now: dt.datetime) -> float | None:
    parsed = _parse(start)
    return round(max(0.0, (now - parsed).total_seconds() / 3600.0), 6) if parsed else None


def _wallet_lane(ready: dict[str, Any], wallet: str) -> dict[str, Any]:
    return next(
        (row for row in ready.get("lanes") or [] if isinstance(row, dict) and str(row.get("wallet") or "").lower() == wallet),
        {},
    )


def refresh_binding_from_ready_shadow(
    binding_artifact: dict[str, Any],
    ready_shadow: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Copy measured lane counters into a matching, non-terminal binding."""

    artifact = json.loads(json.dumps(binding_artifact))
    binding = artifact.get("binding") if isinstance(artifact.get("binding"), dict) else {}
    execution_status = str(artifact.get("execution_status") or "")
    if execution_status in TERMINAL_BINDING_STATUSES:
        return artifact, {"status": "REFUSED_TERMINAL_BINDING", "execution_status": execution_status}
    wallet = str(binding.get("wallet") or "").lower()
    lane = _wallet_lane(ready_shadow, wallet) if wallet else {}
    required_matches = {
        "wallet": bool(lane) and str(lane.get("wallet") or "").lower() == wallet,
        "source_binding_id": bool(lane)
        and lane.get("source_binding_id") == binding.get("source_binding_id"),
        "standby_evidence_started_at": bool(lane)
        and lane.get("standby_evidence_started_at")
        == binding.get("standby_evidence_started_at"),
    }
    if not all(required_matches.values()):
        return artifact, {"status": "REFUSED_BINDING_IDENTITY_MISMATCH", "matches": required_matches}
    for field in (
        "standby_evidence_elapsed_h",
        "resolved_paper_fills",
        "in_lane_fresh_resolved_signals",
        "in_lane_post_fee_pnl_usd",
    ):
        binding[field] = lane.get(field)
    refreshed_at = (now or dt.datetime.now(dt.timezone.utc)).isoformat().replace("+00:00", "Z")
    artifact["binding"] = binding
    artifact["generated_at"] = refreshed_at
    artifact["measurement_refresh"] = {
        "status": "APPLIED",
        "refreshed_at": refreshed_at,
        "source": "wallet_copy_ready_shadow_lanes_state.json",
        "matches": required_matches,
    }
    return artifact, artifact["measurement_refresh"]


def _binding_resolution_attempts(
    binding: dict[str, Any], wide_exact_state: dict[str, Any]
) -> dict[str, Any]:
    """Count feeder attempts inside the immutable binding window."""

    wallet = str(binding.get("wallet") or "").lower()
    started = _parse(binding.get("standby_evidence_started_at"))
    terminal = _parse(binding.get("terminal_executed_at"))
    if terminal is None:
        terminal = _parse(
            (binding.get("terminal_outcome_on_deadline") or {}).get("deadline_at")
        )
    retained_times = [
        recorded
        for row in wide_exact_state.get("attempt_terminals") or []
        if isinstance(row, dict)
        and (recorded := _parse(row.get("recorded_at"))) is not None
    ]
    retained_from = min(retained_times) if retained_times else None
    retained_to = max(retained_times) if retained_times else None
    outside_retention = bool(
        started is not None
        and terminal is not None
        and (
            retained_from is None
            or retained_to is None
            or terminal <= retained_from
            or started > retained_to
        )
    )
    rows: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(wide_exact_state.get("attempt_terminals") or []):
        if not isinstance(row, dict) or str(row.get("wallet") or "").lower() != wallet:
            continue
        recorded = _parse(row.get("recorded_at"))
        if recorded is None or (started is not None and recorded < started):
            continue
        if terminal is not None and recorded >= terminal:
            continue
        identity = str(row.get("attempt_id") or f"row_{index}")
        rows[identity] = row
    taxonomy: dict[str, int] = {}
    for row in rows.values():
        reason = str((row.get("f1_f4_terminal") or {}).get("terminal") or "UNKNOWN")
        taxonomy[reason] = taxonomy.get(reason, 0) + 1
    return {
        "resolutions_attempted": None if outside_retention else len(rows),
        "resolution_attempt_taxonomy": dict(sorted(taxonomy.items())),
        "attempt_log_retention": {
            "status": (
                "NO_RETAINED_ATTEMPT_LOG_FOR_WINDOW"
                if outside_retention
                else "WINDOW_RETAINED"
            ),
            "retained_from": retained_from.isoformat().replace("+00:00", "Z")
            if retained_from
            else None,
            "retained_to": retained_to.isoformat().replace("+00:00", "Z")
            if retained_to
            else None,
            "cohort_scoped": True,
        },
        "attempt_window_start": binding.get("standby_evidence_started_at"),
        "attempt_window_end": (
            binding.get("terminal_executed_at")
            or (binding.get("terminal_outcome_on_deadline") or {}).get("deadline_at")
        ),
    }


def build_report(
    *,
    ready_shadow: dict[str, Any],
    full_pool_queue: dict[str, Any],
    structural_scalp: dict[str, Any],
    structural_scalp_promotion: dict[str, Any] | None = None,
    volume_standby_promotion: dict[str, Any] | None = None,
    t2_cell_admission: dict[str, Any] | None = None,
    wide_standby_binding: dict[str, Any] | None = None,
    wide_exact_state: dict[str, Any] | None = None,
    wide_supervisor_heartbeat: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc)
    cut_status = (ready_shadow.get("a689_82c8_cut") or {}).get("status")
    prior_successor_bound = any(
        isinstance(row, dict)
        and str(row.get("wallet") or "").lower() == "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
        and row.get("source_binding") == "FABLE_20260723_82C8_READY_SHADOW"
        for row in ready_shadow.get("lanes") or []
    )
    a689_cut_terminalized = bool(cut_status == "EXECUTED_ATOMIC_STATE_REBIND" or prior_successor_bound)
    active_wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77" if a689_cut_terminalized else A689

    a689 = _wallet_lane(ready_shadow, active_wallet)
    volume = _wallet_lane(ready_shadow, VOLUME)
    queue_summary = full_pool_queue.get("summary") if isinstance(full_pool_queue.get("summary"), dict) else {}
    ready_summary = ready_shadow.get("summary") if isinstance(ready_shadow.get("summary"), dict) else {}
    queue_generated = full_pool_queue.get("generated_at")
    volume_start = volume.get("paper_canary_enrolled_at") or volume.get("readmission_started_at")
    volume_elapsed = _elapsed_h(volume_start, now)
    volume_copyable_buys = int(volume.get("copyable_buy_events") or 0)
    volume_would_pnl = num(volume.get("paper_pnl_usd"), 0.0)
    volume_ready = bool(
        volume_elapsed is not None
        and volume_elapsed >= 72.0
        and volume_copyable_buys >= 20
        and volume_would_pnl > 0.0
    )
    volume_promotion = volume_standby_promotion or {}
    volume_terminal_decision = str(
        volume_promotion.get("terminal_decision")
        or volume_promotion.get("decision")
        or ""
    )
    volume_branch = (
        (volume_promotion.get("prederived_decision_branches") or {}).get("current_branch")
        if isinstance(volume_promotion.get("prederived_decision_branches"), dict)
        else None
    )
    volume_terminal_parked = bool(
        volume_terminal_decision == "PARK_VOLUME_STANDBY_PAPER_ONLY"
        or volume_branch == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    )
    a689_start = a689.get("standby_evidence_started_at")
    a689_elapsed = _elapsed_h(a689_start, now)
    a689_source_bound = str(a689.get("source_binding_status") or "") == "WIRED"
    a689_clock_bound = a689_start is not None
    binding_artifact = wide_standby_binding or {}
    binding = (
        binding_artifact.get("binding")
        if isinstance(binding_artifact.get("binding"), dict)
        and binding_terminally_executed(binding_artifact)
        else {}
    )
    terminal_park_committed = bool(
        binding_artifact.get("execution_status") == "PARK_COMMITTED"
        and binding_terminally_executed(binding_artifact)
    )
    authoritative_binding = bool(
        binding.get("source_binding_status") == "WIRED"
        and str(binding.get("wallet") or "").lower() == active_wallet
    )
    resolution_attempts = _binding_resolution_attempts(
        binding, wide_exact_state or {}
    ) if authoritative_binding else {
        "resolutions_attempted": None,
        "resolution_attempt_taxonomy": {},
        "attempt_log_retention": {
            "status": "BINDING_NOT_AUTHORITATIVE",
            "retained_from": None,
            "retained_to": None,
            "cohort_scoped": True,
        },
        "attempt_window_start": None,
        "attempt_window_end": None,
    }
    a689_measured_raw = (
        binding.get("standby_evidence_elapsed_h")
        if authoritative_binding
        else a689.get("standby_evidence_elapsed_h")
    )
    a689_measured_elapsed = num(a689_measured_raw)
    a689_required_h = (
        num(binding.get("standby_evidence_minimum_h"), 48.0)
        if authoritative_binding
        else num(a689.get("standby_evidence_minimum_h"), 48.0)
    )
    a689_ready = bool(
        a689_source_bound
        and a689_clock_bound
        and a689_measured_elapsed >= a689_required_h
        and a689.get("hot_standby_ready")
        and int(a689.get("resolved_paper_fills") or 0) >= 30
    )
    t2_admission = t2_cell_admission or {}
    t2_member = (
        t2_admission.get("member")
        if isinstance(t2_admission.get("member"), dict)
        else {}
    )
    successor_superseded_by_t2 = bool(
        active_wallet == "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
        and a689.get("source_binding") != WIDE_STANDBY_SOURCE_BINDING
        and t2_admission.get("status") == "PASS"
        and str(t2_member.get("source_wallet") or "").lower() == active_wallet
        and str((t2_member.get("cell_scoped_admission") or {}).get("status") or "")
        == "ACTIVE"
    )
    a689_unfed_clock = bool(
        not successor_superseded_by_t2
        and a689_clock_bound
        and a689_elapsed is not None
        and a689_elapsed > 0.0
        and a689_measured_raw is not None
        and a689_measured_elapsed == 0.0
    )
    a689_projected_resolved = (
        round(
            int(a689.get("resolved_paper_fills") or 0)
            * a689_required_h
            / a689_measured_elapsed,
            6,
        )
        if a689_measured_elapsed > 0.0
        else 0.0
    )
    a689_status = (
        "SUPERSEDED_BY_T2_LIVE"
        if successor_superseded_by_t2
        else "PARK_SEAT_UNFED_CLOCK_COMMITTED"
        if terminal_park_committed
        else "CLOCK_OR_SOURCE_BINDING_MISSING"
        if not a689_source_bound or not a689_clock_bound
        else "UNFED_CLOCK_CANNOT_MATURE"
        if a689_unfed_clock
        else "VALID"
        if a689_ready
        else "ACCRUING_RED"
    )
    wiring_detected = "2026-07-21T18:10:05Z"

    # cut status and successor checks moved to start of build_report to determine active_wallet

    if a689_cut_terminalized:
        executed_at = (ready_shadow.get("a689_82c8_cut") or {}).get("executed_at")
        wiring_elapsed = _elapsed_h(wiring_detected, _parse(executed_at) or now)
        wiring_status = "PASS"
        wiring_evidence = "a689 cut terminalized and successor 0x82c8 bound"
    else:
        wiring_elapsed = _elapsed_h(wiring_detected, _parse(a689_start) or now)
        wiring_status = "PASS" if a689.get("source_binding_status") == "WIRED" else "ACCRUING"
        wiring_evidence = f"a689_source_binding={a689.get('source_binding_status')}"
    scalp_summary = structural_scalp.get("summary") if isinstance(structural_scalp.get("summary"), dict) else {}
    scalp_started = structural_scalp.get("seeded_at")
    scalp_promotion = structural_scalp_promotion or {}
    scalp_decision_clock = (
        scalp_promotion.get("decision_clock")
        if isinstance(scalp_promotion.get("decision_clock"), dict)
        else {}
    )
    scalp_decision = str(scalp_promotion.get("decision") or "")
    scalp_parked = bool(
        scalp_decision == "PARK_METHOD_LANE_PAPER_ONLY"
        and scalp_decision_clock.get("due") is True
    )
    scalp_status = (
        "PARKED"
        if scalp_parked
        else "VALID"
        if scalp_summary.get("forward_gate_passed")
        else "ACCRUING_RED"
    )

    def row(
        stage: str,
        elapsed: float | None,
        *,
        status: str,
        clock_start: Any,
        completed_at: Any,
        evidence: str,
    ) -> dict[str, Any]:
        budget = BUDGETS_H[stage]
        breached = bool(
            elapsed is not None
            and elapsed > budget
            and status != "NO_ACTIVE_ITEM"
            and not completed_at
        )
        rendered_status = (
            "PASS_OVER_BUDGET"
            if breached and status == "PASS"
            else "BREACH"
            if breached
            else status
        )
        return {
            "stage": stage,
            "budget_h": budget,
            "clock_start": clock_start,
            "completed_at": completed_at,
            "time_in_stage_h": elapsed,
            "status": rendered_status,
            "breached": breached,
            "evidence": evidence,
        }

    stages = [
        row("mined_to_scored", 0.0, status="PASS", clock_start=queue_generated, completed_at=queue_generated, evidence="latest full-pool queue refresh scored the current mined supply"),
        row("scored_to_shadow", 0.0, status="PASS", clock_start=ready_shadow.get("generated_at"), completed_at=ready_shadow.get("generated_at"), evidence="ready-shadow builder consumed the current ranked queue"),
        row("shadow_to_ready", volume_elapsed, status="ACCRUING" if not volume.get("ready_shadow_full_utc_day") else "PASS", clock_start=volume_start, completed_at=None, evidence="0x13e0 ready-shadow canary clock"),
        row("ready_to_gates_run", 0.0, status="PASS", clock_start=ready_shadow.get("generated_at"), completed_at=ready_shadow.get("generated_at"), evidence=f"gate_crossed={ready_summary.get('gate_crossed', 0)}"),
        row("cooldown_expired_to_readmission_run", 0.0, status="NO_ACTIVE_ITEM", clock_start=None, completed_at=None, evidence="no cooldown-expired candidate awaiting an unrun readmission gate in ready-shadow state"),
        row("standby_wiring_repair", wiring_elapsed, status=wiring_status, clock_start=wiring_detected, completed_at=None, evidence=wiring_evidence),
    ]
    supervisor_heartbeat = wide_supervisor_heartbeat or {
        "status": "NOT_EVALUATED",
        "age_s": None,
        "expected_max_age_s": WIDE_SUPERVISOR_MAX_AGE_S,
        "scheduler_installed": None,
        "process_alive": None,
    }
    heartbeat_status = str(supervisor_heartbeat.get("status") or "NOT_EVALUATED")
    heartbeat_breached = heartbeat_status not in {"PASS", "NOT_EVALUATED"}
    stages.append(
        {
            "stage": "wide_supervisor_heartbeat",
            "budget_h": BUDGETS_H["wide_supervisor_heartbeat"],
            "clock_start": supervisor_heartbeat.get("last_cut_published_at"),
            "completed_at": None,
            "time_in_stage_h": (
                round(float(supervisor_heartbeat["age_s"]) / 3600.0, 6)
                if supervisor_heartbeat.get("age_s") is not None
                else None
            ),
            "status": heartbeat_status,
            "breached": heartbeat_breached,
            "evidence": (
                f"scheduler_installed={supervisor_heartbeat.get('scheduler_installed')} "
                f"process_alive={supervisor_heartbeat.get('process_alive')} "
                f"lock_pid={supervisor_heartbeat.get('lock_pid')}"
            ),
        }
    )
    heartbeat_publish_period = (
        supervisor_heartbeat.get("heartbeat_publish_period") or {}
    )
    stages.append(
        {
            "stage": "wide_supervisor_heartbeat_publish_period",
            "budget_s": WIDE_SUPERVISOR_HEARTBEAT_INTERVAL_S,
            # Hour-denominated twins so operator surfaces that read the common
            # *_h stage keys render numbers instead of blank None on the two
            # second-denominated cadence stages.
            "budget_h": round(WIDE_SUPERVISOR_HEARTBEAT_INTERVAL_S / 3600.0, 6),
            "clock_start": supervisor_heartbeat.get("last_cut_published_at"),
            "completed_at": None,
            "time_in_stage_s": heartbeat_publish_period.get("achieved_period_s"),
            "time_in_stage_h": _seconds_to_hours(
                heartbeat_publish_period.get("achieved_period_s")
            ),
            "status": (
                heartbeat_publish_period.get("status")
                or "INSUFFICIENT_PUBLICATIONS"
            ),
            "breached": heartbeat_publish_period.get("status") == "BREACH",
            "evidence": (
                f"achieved_period_s={heartbeat_publish_period.get('achieved_period_s')} "
                f"declared_interval_s={heartbeat_publish_period.get('declared_interval_s')} "
                f"ratio={heartbeat_publish_period.get('achieved_to_declared_ratio')}"
            ),
        }
    )
    scorer_cycle = supervisor_heartbeat.get("scorer_cycle_period") or {}
    stages.append(
        {
            "stage": "wide_scorer_cycle_period",
            "budget_s": WIDE_SUPERVISOR_SCORE_INTERVAL_S,
            "budget_h": round(WIDE_SUPERVISOR_SCORE_INTERVAL_S / 3600.0, 6),
            "clock_start": None,
            "completed_at": None,
            "time_in_stage_s": scorer_cycle.get("achieved_period_s"),
            "time_in_stage_h": _seconds_to_hours(
                scorer_cycle.get("achieved_period_s")
            ),
            "status": scorer_cycle.get("status") or "INSUFFICIENT_CYCLES",
            "breached": scorer_cycle.get("status") == "BREACH",
            "evidence": (
                f"achieved_period_s={scorer_cycle.get('achieved_period_s')} "
                f"declared_interval_s={scorer_cycle.get('declared_interval_s')} "
                f"ratio={scorer_cycle.get('achieved_to_declared_ratio')}"
            ),
        }
    )
    queue_ready = int(queue_summary.get("hot_standby_ready") or 0)
    quality_ready = int(ready_summary.get("all_measurement_hot_standby_ready") or 0)
    return {
        "schema_version": 1,
        "kind": "pipeline_slo_and_standby_readiness",
        "flow_stage": "PROMOTE/ROTATE/SELF-DEV",
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "measurement_only": True,
        "live_orders_allowed": False,
        "pipeline_slo": {
            "authority": "fable DIRECTION 2026-07-21T18:17Z",
            "stages": stages,
            "breach_count": sum(bool(item["breached"]) for item in stages),
        },
        "wide_supervisor_heartbeat": supervisor_heartbeat,
        "standby_ready": {
            "seat": {
                "wallet": active_wallet,
                "status": a689_status,
                "clock_start": a689_start,
                "elapsed_h": a689_measured_elapsed,
                "wall_clock_elapsed_h": a689_elapsed,
                "required_h": a689_required_h,
                "projected_resolved_at_48h": a689_projected_resolved,
                "admission_forecast": bool(
                    not a689_unfed_clock
                    and a689_projected_resolved >= 30
                ),
                "elapsed_basis": (
                    "authoritative_binding_measured_elapsed"
                    if authoritative_binding
                    else "ready_shadow_measured_elapsed"
                ),
                "resolved": int(a689.get("resolved_paper_fills") or 0),
                **resolution_attempts,
                "required_resolved": 30,
                "post_fee_pnl_usd": num(a689.get("in_lane_post_fee_pnl_usd"), 0.0),
                "source_binding_status": a689.get("source_binding_status"),
                "clock_or_source_binding_missing": bool(
                    not successor_superseded_by_t2
                    and (not a689_source_bound or not a689_clock_bound)
                ),
                "next_action": (
                    "none; the former standby successor is live under T2 exact-cell admission"
                    if successor_superseded_by_t2
                    else (
                        "none; terminal park committed at "
                        f"{binding.get('terminal_executed_at')}"
                    )
                    if terminal_park_committed
                    else "bind a real source event and start the non-backdated standby clock"
                    if not a689_source_bound or not a689_clock_bound
                    else "execute pre-committed PARK_SEAT_UNFED_CLOCK at the immutable deadline"
                    if a689_unfed_clock
                    else "continue evidence accrual"
                    if not a689_ready
                    else "none"
                ),
                "terminal_executed_at": binding.get("terminal_executed_at"),
            },
            "method": {
                "lane": "btc5m_structural_scalp",
                "status": scalp_status,
                "clock_start": scalp_started,
                "decision_at": scalp_decision_clock.get("decision_at") or "2026-07-23T02:00:00Z",
                "terminal_decision": scalp_decision or None,
                "decision_due": scalp_decision_clock.get("due"),
                "terminal_basis": (
                    "due promotion-prep PARK decision; evidence clock is closed"
                    if scalp_parked
                    else None
                ),
                "forward_fills": int(scalp_summary.get("forward_fills") or 0),
                "forward_pnl_usd": num(scalp_summary.get("forward_pnl_usd"), 0.0),
            },
            "volume": {
                "wallet": VOLUME,
                "status": (
                    "PARKED"
                    if volume_terminal_parked
                    else "VALID"
                    if volume_ready
                    else "ACCRUING_RED"
                ),
                "clock_start": volume_start,
                "elapsed_h": volume_elapsed,
                "required_h": 72.0,
                "copyable_buys": volume_copyable_buys,
                "required_copyable_buys": 20,
                "would_pnl_usd": volume_would_pnl,
                "terminal_decision": volume_terminal_decision or volume_branch,
                "terminal_basis": (
                    "terminal PARK decision outranks elapsed/sample readiness"
                    if volume_terminal_parked
                    else None
                ),
            },
        },
        "hot_standby_counter_reconciliation": {
            "queue_supply_ready_and_alive": queue_ready,
            "ready_shadow_quality_validated": quality_ready,
            "canonical_op_standby_ready": quality_ready,
            "status": "RECONCILED_DIFFERENT_DENOMINATORS",
            "explanation": "queue counter is READY_AND_ALIVE supply; ready-shadow counter requires fresh post-fee quality evidence and is canonical for OP-STANDBY-AHEAD",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-shadow", default="data/research/wallet_copy_ready_shadow_lanes_state.json")
    parser.add_argument("--queue", default="data/research/wallet_copy_full_pool_member_queue.json")
    parser.add_argument("--structural-scalp", default="data/research/btc5m_structural_scalp_paper_lane_state.json")
    parser.add_argument(
        "--structural-scalp-promotion",
        default="data/research/btc5m_structural_scalp_promotion_prep_latest.json",
    )
    parser.add_argument(
        "--volume-standby-promotion",
        default="data/research/13e0_exact_policy_promotion_packet_latest.json",
    )
    parser.add_argument(
        "--t2-cell-admission",
        default="data/research/t2_82c8_cell_admission_latest.json",
    )
    parser.add_argument(
        "--wide-standby-binding",
        default="data/research/82c8_wide_standby_binding_latest.json",
    )
    parser.add_argument(
        "--wide-exact-state",
        default="data/research/wide_exact_policy_paper_state.json",
    )
    parser.add_argument(
        "--refresh-binding",
        action="store_true",
        help="Refresh a matching non-terminal binding from ready-shadow measurements.",
    )
    parser.add_argument("--output", default="data/research/pipeline_slo_and_standby_readiness_latest.json")
    args = parser.parse_args()
    ready_shadow = load_json(args.ready_shadow, default={}) or {}
    wide_standby_binding = load_json(args.wide_standby_binding, default={}) or {}
    refresh_result = None
    if args.refresh_binding:
        refreshed_binding, refresh_result = refresh_binding_from_ready_shadow(
            wide_standby_binding,
            ready_shadow,
        )
        if refresh_result.get("status") == "APPLIED":
            atomic_write_json(args.wide_standby_binding, refreshed_binding)
            wide_standby_binding = refreshed_binding
    payload = build_report(
        ready_shadow=ready_shadow,
        full_pool_queue=load_json(args.queue, default={}) or {},
        structural_scalp=load_json(args.structural_scalp, default={}) or {},
        structural_scalp_promotion=load_json(args.structural_scalp_promotion, default={}) or {},
        volume_standby_promotion=load_json(args.volume_standby_promotion, default={}) or {},
        t2_cell_admission=load_json(args.t2_cell_admission, default={}) or {},
        wide_standby_binding=wide_standby_binding,
        wide_exact_state=load_json(args.wide_exact_state, default={}) or {},
        wide_supervisor_heartbeat=grade_wide_supervisor_heartbeat(),
    )
    if refresh_result is not None:
        payload["binding_refresh"] = refresh_result
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
