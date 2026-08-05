#!/usr/bin/env python3
"""Zero-AI live-guard restart actuator for red order-flow incidents."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from scripts.post_boot_recovery_audit import sweep_stale_remnants  # noqa: E402


DEFAULT_DEADMAN = ROOT / "data/research/order_flow_deadman_state.json"
DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_STATE = ROOT / "data/research/brainless_live_guard_restart_state.json"
DEFAULT_EVENT_LOG = ROOT / "data/research/brainless_live_guard_restart_events.jsonl"
DEFAULT_LIVE_CHANGE_JOURNAL = ROOT / "data/research/live_change_journal.jsonl"
DEFAULT_GOLDEN_SNAPSHOT = ROOT / "data/research/golden_config_latest.json"
DEFAULT_START_SCRIPT = ROOT / "scripts/start_live_guard.sh"
DEFAULT_LOCK = ROOT / "data/research/wallet_copy_live_guard.lock"
DEFAULT_LAUNCHD_PLIST = ROOT / "launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist"
DEFAULT_LAUNCHD_LABEL = "com.belavarga.polymarket.wallet-copy-live-guard"
DEFAULT_FEED_CIRCUIT_STATE = ROOT / "data/research/rtds_feed_circuit_breaker_state.json"
DEFAULT_FEED_PATH = ROOT / DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_EXTERNAL_LIVENESS_STATE = (
    ROOT / "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json"
)
DEFAULT_EXTERNAL_LIVENESS_PROBE = (
    ROOT / "scripts/probe_queue_remote_dataapi_fresh_flow.py"
)
QUIESCENT_REMNANT_SWEEP_PATHS = {
    ROOT / "data/research/.btc_resolutions_from_btcusdt_ticks.jsonl.lock",
    ROOT / "data/research/.wallet_copy_ready_shadow_lanes_state.json.lock",
    ROOT / "data/research/order_flow_deadman_incidents.jsonl.lock",
    ROOT / "data/research/wide_direct_handoff_journal.jsonl.lock",
    ROOT / "data/research/.wallet_copy_orderfilled_sidecar_live_cursor_state.json.zj6g139_.tmp",
}
GENERATION_FILES = (
    ROOT / "scripts/run_wallet_copy_live_guard.py",
    ROOT / "scripts/run_wallet_copy_live_execution.py",
    ROOT / "scripts/start_live_guard.sh",
    ROOT / "launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist",
    ROOT / "src/trade_executor.py",
    ROOT / "src/wallet_copy/execution.py",
    ROOT / "src/wallet_copy/mission.py",
    ROOT / "src/wallet_copy/pnl_truth.py",
)
GENERATION_VERDICT_MAX_AGE_S = 600.0


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _selection_adoption_wallet(deadman: dict[str, Any]) -> str:
    policy_choke = deadman.get("policy_choke")
    policy_choke = policy_choke if isinstance(policy_choke, dict) else {}
    actuator = policy_choke.get("actuator")
    actuator = actuator if isinstance(actuator, dict) else {}
    evidence = actuator.get("candidate_evidence")
    evidence = evidence if isinstance(evidence, dict) else {}
    selected = evidence.get("selected")
    selected = selected if isinstance(selected, dict) else {}
    wallet = str(selected.get("wallet") or selected.get("source_wallet") or "").lower()
    return wallet if wallet.startswith("0x") and len(wallet) == 42 else ""


def _refresh_selection_adoption_liveness(
    wallet: str,
    *,
    probe_script: Path = DEFAULT_EXTERNAL_LIVENESS_PROBE,
    output_path: Path = DEFAULT_EXTERNAL_LIVENESS_STATE,
    timeout_s: float = 120.0,
) -> dict[str, Any]:
    """Refresh and judge the canonical liveness plane before spending a restart."""
    command = [
        sys.executable,
        str(probe_script),
        "--include-wallet",
        wallet,
        "--clearance-limit",
        "0",
        "--ranked-limit",
        "0",
        "--cohort-limit",
        "0",
        "--no-default-include-wallet",
    ]
    try:
        proc = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_s)),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "ERROR",
            "passed": False,
            "reason": "selection_liveness_refresh_timeout",
            "source_wallet": wallet,
            "timeout_s": float(timeout_s),
            "stderr_tail": str(exc)[-1000:],
            "command": command,
        }
    if proc.returncode != 0:
        return {
            "status": "ERROR",
            "passed": False,
            "reason": "selection_liveness_refresh_failed",
            "source_wallet": wallet,
            "returncode": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-1000:],
            "stderr_tail": (proc.stderr or "")[-1000:],
            "command": command,
        }
    state = _load_json(output_path, {})
    if not isinstance(state, dict):
        state = {}
    from scripts.run_wallet_copy_live_guard import (  # local import avoids actuator startup cost
        _external_liveness_gate_for_wallet,
        _external_liveness_rows_by_wallet,
    )

    rows_by_wallet = _external_liveness_rows_by_wallet(state)
    gate = _external_liveness_gate_for_wallet(
        wallet,
        rows_by_wallet=rows_by_wallet,
        state=state,
        state_path=output_path,
    )
    row = rows_by_wallet.get(wallet) if wallet else None
    selected_this_run = bool(isinstance(row, dict) and row.get("selected_this_run") is True)
    passed = bool(gate.get("passed") is True and selected_this_run)
    return {
        "status": "PASS" if passed else "ERROR",
        "passed": passed,
        "reason": (
            "selection_liveness_refresh_pass"
            if passed
            else "selection_liveness_row_not_refreshed"
            if gate.get("passed") is True
            else str(gate.get("reason") or "selection_liveness_gate_failed")
        ),
        "source_wallet": wallet,
        "selected_this_run": selected_this_run,
        "external_liveness_gate": gate,
        "output": _display(output_path),
        "command": command,
    }


def _jsonl_tail_clean(path: Path, *, max_bytes: int = 1_048_576, max_rows: int = 100) -> dict[str, Any]:
    if not path.exists():
        return {"clean": False, "rows_checked": 0, "invalid_rows": 0, "reason": "feed_path_missing"}
    try:
        with path.open("rb") as handle:
            size = handle.seek(0, os.SEEK_END)
            start = max(0, size - max_bytes)
            handle.seek(start)
            data = handle.read()
    except OSError as exc:
        return {"clean": False, "rows_checked": 0, "invalid_rows": 0, "reason": f"read_error:{exc}"}
    lines = data.splitlines()
    if start > 0 and lines:
        lines = lines[1:]
    lines = [line for line in lines if line.strip()][-max_rows:]
    invalid = 0
    for line in lines:
        try:
            parsed = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            invalid += 1
            continue
        if not isinstance(parsed, dict):
            invalid += 1
    return {
        "clean": bool(lines) and invalid == 0,
        "rows_checked": len(lines),
        "invalid_rows": invalid,
        "reason": "clean" if lines and invalid == 0 else ("empty_tail" if not lines else "invalid_json_tail"),
    }


def feed_health_preflight(
    *,
    circuit_state_path: Path = DEFAULT_FEED_CIRCUIT_STATE,
    feed_path: Path = DEFAULT_FEED_PATH,
    canonical_feed_path: Path = DEFAULT_FEED_PATH,
    min_uptime_s: float = 900.0,
    max_feed_age_s: float = 60.0,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now_ts = float(now_ts if now_ts is not None else time.time())
    circuit = _load_json(circuit_state_path, {})
    circuit = circuit if isinstance(circuit, dict) else {}
    started_at_s = circuit.get("current_process_started_at_s")
    try:
        uptime_s = max(0.0, now_ts - float(started_at_s)) if started_at_s is not None else None
    except (TypeError, ValueError):
        uptime_s = None
    try:
        feed_age_s = max(0.0, now_ts - feed_path.stat().st_mtime)
    except OSError:
        feed_age_s = None
    try:
        writer_pid = int(circuit.get("pid") or 0)
    except (TypeError, ValueError):
        writer_pid = 0
    writer_alive = False
    if writer_pid > 0:
        try:
            os.kill(writer_pid, 0)
            writer_alive = True
        except OSError:
            writer_alive = False
    tail = _jsonl_tail_clean(feed_path)
    checks = {
        "writer_status_ok": str(circuit.get("status") or "") == "OK",
        "writer_pid_alive": writer_alive,
        "writer_uptime_gte_min": uptime_s is not None and uptime_s >= float(min_uptime_s),
        "feed_fresh_lt_max": feed_age_s is not None and feed_age_s < float(max_feed_age_s),
        "path_canonical": feed_path.resolve() == canonical_feed_path.resolve(),
        "parse_tail_clean": bool(tail.get("clean")),
    }
    passed = all(checks.values())
    return {
        "schema_version": 1,
        "flow_stage": "LIVE/DEFEND",
        "status": "PASS" if passed else "WAIT_FEED_HEALTH",
        "passed": passed,
        "checks": checks,
        "writer_pid": writer_pid or None,
        "writer_uptime_s": None if uptime_s is None else round(uptime_s, 6),
        "minimum_writer_uptime_s": float(min_uptime_s),
        "feed_age_s": None if feed_age_s is None else round(feed_age_s, 6),
        "maximum_feed_age_s": float(max_feed_age_s),
        "feed_path": _display(feed_path),
        "canonical_feed_path": _display(canonical_feed_path),
        "parse_tail": tail,
        "next_action": (
            "feed-health preflight passed; canonical live-guard restart may proceed"
            if passed
            else "hold live-guard restart until writer uptime, freshness, canonical path, and parse-tail all pass"
        ),
    }


def _restart_events_from_log(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists():
        return events
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return events
    for line in lines[-200:]:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict) or row.get("status") != "RESTART_EXECUTED":
            continue
        execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
        events.append(
            {
                "at": row.get("generated_at"),
                "reason": row.get("reason"),
                "started_pid": execution.get("started_pid"),
                "disk_generation_sha256": (row.get("disk_generation") or {}).get("sha256")
                if isinstance(row.get("disk_generation"), dict)
                else None,
            }
        )
    return [row for row in events if row.get("at")]


def _sha256_file(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def disk_generation(files: tuple[Path, ...] | None = None) -> dict[str, Any]:
    files = files if files is not None else GENERATION_FILES
    rows: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    for path in files:
        file_hash = _sha256_file(path)
        exists = file_hash is not None
        row = {
            "path": _display(path),
            "exists": exists,
            "sha256": file_hash,
        }
        rows.append(row)
        digest.update(str(row["path"]).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(file_hash or "MISSING").encode("utf-8"))
        digest.update(b"\0")
    return {
        "flow_stage": "LIVE/SELF-DEV",
        "schema_version": 1,
        "sha256": digest.hexdigest(),
        "files": rows,
        "rule": "stable content hash, not raw mtime, gates brainless live-guard restarts",
    }


def _loaded_generation(guard_state: dict[str, Any]) -> dict[str, Any]:
    identity = guard_state.get("guard_code_identity") if isinstance(guard_state, dict) else {}
    identity = identity if isinstance(identity, dict) else {}
    generation_sha256 = identity.get("live_guard_generation_sha256")
    return {
        "sha256": generation_sha256,
        "generation_sha256": generation_sha256,
        "script_sha256": identity.get("script_sha256"),
        "started_at_utc": identity.get("started_at_utc"),
        "pid": identity.get("pid") or guard_state.get("pid"),
    }


def generation_verdict(
    decision: dict[str, Any],
    *,
    now: dt.datetime,
    max_age_s: float = GENERATION_VERDICT_MAX_AGE_S,
) -> dict[str, Any]:
    """Fail closed when a cached generation comparison is no longer current."""
    generated_ts = _parse_iso(decision.get("generated_at"))
    loaded = decision.get("loaded_generation")
    loaded = loaded if isinstance(loaded, dict) else {}
    loaded_started_ts = _parse_iso(loaded.get("started_at_utc"))
    now_ts = now.timestamp()
    reasons: list[str] = []
    if generated_ts is None:
        reasons.append("missing_or_unreadable_generated_at")
    if loaded_started_ts is None:
        reasons.append("missing_or_unreadable_loaded_generation_started_at")
    if generated_ts is not None and loaded_started_ts is not None and generated_ts < loaded_started_ts:
        reasons.append("verdict_predates_loaded_generation")
    age_s = max(0.0, now_ts - generated_ts) if generated_ts is not None else None
    if age_s is not None and age_s > float(max_age_s):
        reasons.append("verdict_older_than_declared_cadence")
    stale = bool(reasons)
    return {
        "status": "GENERATION_VERDICT_STALE" if stale else "GENERATION_VERDICT_FRESH",
        "stale": stale,
        "reasons": reasons,
        "generated_at": decision.get("generated_at"),
        "loaded_generation_started_at_utc": loaded.get("started_at_utc"),
        "age_s": round(age_s, 6) if age_s is not None else None,
        "max_age_s": float(max_age_s),
        "generation_mismatch_citable": bool(not stale),
    }


def _read_lock_holder(path: Path = DEFAULT_LOCK) -> dict[str, Any]:
    loaded = _load_json(path, {})
    if not isinstance(loaded, dict):
        return {}
    try:
        pid = int(loaded.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    return {
        "pid": pid or None,
        "started_at": loaded.get("started_at"),
        "state": loaded.get("state"),
        "path": _display(path),
    }


def _live_guard_process_rows() -> list[dict[str, Any]]:
    try:
        proc = subprocess.run(
            ["pgrep", "-fl", "scripts/run_wallet_copy_live_guard.py"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=5.0,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    rows: list[dict[str, Any]] = []
    for line in (proc.stdout or "").splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        command = parts[1] if len(parts) > 1 else ""
        if "scripts/run_wallet_copy_live_guard.py" not in command:
            continue
        rows.append({"pid": pid, "command": command})
    return rows


def _actual_live_guard_pid(lock_path: Path = DEFAULT_LOCK) -> dict[str, Any]:
    holder = _read_lock_holder(lock_path)
    rows = _live_guard_process_rows()
    row_pids = {row.get("pid") for row in rows}
    holder_pid = holder.get("pid")
    if holder_pid and holder_pid in row_pids:
        actual_pid = holder_pid
        source = "lock_holder"
    elif rows:
        actual_pid = rows[0].get("pid")
        source = "pgrep"
    else:
        actual_pid = None
        source = "none"
    return {
        "actual_pid": actual_pid,
        "actual_pid_source": source,
        "lock_holder": holder,
        "pgrep_rows": rows[:5],
    }


def _guard_unresponsive(guard_state: dict[str, Any], *, max_age_s: float, now_ts: float) -> tuple[bool, dict[str, Any]]:
    status = str(guard_state.get("status") or "") if isinstance(guard_state, dict) else ""
    generated_ts = _parse_iso(guard_state.get("generated_at") if isinstance(guard_state, dict) else None)
    age_s = None if generated_ts is None else max(0.0, now_ts - generated_ts)
    pid = None
    try:
        pid = int(guard_state.get("pid") or 0) if isinstance(guard_state, dict) else 0
    except (TypeError, ValueError):
        pid = 0
    pid_alive = False
    if pid > 0:
        try:
            os.kill(pid, 0)
            pid_alive = True
        except OSError:
            pid_alive = False
    stale_already_running_state = status == "LIVE_GUARD_ALREADY_RUNNING"
    stale_breach = (
        stale_already_running_state
        or generated_ts is None
        or (age_s is not None and age_s > max_age_s)
    )
    pid_dead = pid > 0 and not pid_alive
    unresponsive = stale_breach or pid_dead
    return unresponsive, {
        "status": status,
        "generated_at": guard_state.get("generated_at") if isinstance(guard_state, dict) else None,
        "age_s": None if age_s is None else round(age_s, 6),
        "max_age_s": float(max_age_s),
        "pid": pid or None,
        "pid_alive": pid_alive,
        "pid_dead": pid_dead,
        "stale_breach": stale_breach,
        "stale_already_running_state": stale_already_running_state,
    }


def _deadman_red(deadman: dict[str, Any], *, min_red_s: float) -> tuple[bool, dict[str, Any]]:
    status = str(deadman.get("status") or "")
    deadman_class = str(deadman.get("deadman_class") or "")
    raw_limb = deadman.get("raw_accepted_order_deadman")
    raw_limb = raw_limb if isinstance(raw_limb, dict) else {}
    raw_limb_class = str(raw_limb.get("deadman_class") or "")
    raw_limb_firing = bool(raw_limb.get("firing"))
    raw_limb_red = raw_limb_firing and raw_limb_class == "ORDER_FLOW_DEAD"
    raw_limb_exempt = bool(raw_limb.get("ruled_posture_exemption"))
    idle_s = float(deadman.get("idle_s") or deadman.get("accepted_order_idle_s") or 0.0)
    guard_side_halt = status == "INCIDENT_GUARD_SIDE_HALT" or deadman_class == "GUARD_SIDE_HALT"
    headline_red = status == "INCIDENT_ORDER_FLOW_DEAD" or deadman_class == "ORDER_FLOW_DEAD"
    order_flow_red = (headline_red or raw_limb_red) and not raw_limb_exempt
    ruled_posture = status == "MEASURED_SOURCE_QUIET"
    order_flow_red_effective = (
        order_flow_red and idle_s >= float(min_red_s) and not ruled_posture
    )
    red = bool(guard_side_halt or order_flow_red_effective)
    return red, {
        "status": status,
        "deadman_class": deadman_class,
        "guard_side_halt": guard_side_halt,
        "idle_s": round(idle_s, 6),
        "min_red_s": float(min_red_s),
        "ruled_posture_exemption": ruled_posture,
        "red_basis": (
            "guard_side_halt"
            if guard_side_halt
            else "raw_accepted_order_limb"
            if raw_limb_red and order_flow_red_effective
            else "headline"
            if headline_red and order_flow_red_effective
            else None
        ),
        "raw_limb_class": raw_limb_class or None,
        "raw_limb_firing": raw_limb_firing,
        "raw_limb_ruled_posture_exemption": raw_limb_exempt,
    }


def _same_utc_day(ts_iso: Any, now: dt.datetime) -> bool:
    parsed = _parse_iso(ts_iso)
    if parsed is None:
        return False
    return dt.datetime.fromtimestamp(parsed, dt.timezone.utc).date() == now.date()


def _storm_guard(
    restart_state: dict[str, Any],
    *,
    now: dt.datetime,
    cooldown_s: float,
    max_per_day: int,
) -> dict[str, Any]:
    last_restart_at = restart_state.get("last_restart_at")
    last_ts = _parse_iso(last_restart_at)
    now_ts = now.timestamp()
    cooldown_remaining_s = 0.0
    if last_ts is not None:
        cooldown_remaining_s = max(0.0, float(cooldown_s) - (now_ts - last_ts))
    restart_events = restart_state.get("restart_events") if isinstance(restart_state.get("restart_events"), list) else []
    restarts_today = sum(
        1
        for row in restart_events
        if isinstance(row, dict)
        and _same_utc_day(row.get("at"), now)
    )
    return {
        "last_restart_at": last_restart_at,
        "cooldown_s": float(cooldown_s),
        "cooldown_remaining_s": round(cooldown_remaining_s, 6),
        "cooldown_clear": cooldown_remaining_s <= 0.0,
        "restarts_today": int(restarts_today),
        "max_restarts_per_day": int(max_per_day),
        "daily_cap_clear": restarts_today < int(max_per_day),
        "excluded_non_storm_reasons": [],
        "count_basis": "all_RESTART_EXECUTED_events_regardless_of_reason",
    }


def build_decision(
    *,
    deadman: dict[str, Any],
    guard_state: dict[str, Any],
    restart_state: dict[str, Any],
    now: dt.datetime,
    cooldown_s: float = 1800.0,
    max_per_day: int = 3,
    min_red_s: float = 1800.0,
    max_guard_state_age_s: float = 120.0,
    generation_mismatch_quiescence_s: float = 900.0,
    allow_generation_reload: bool = False,
    generation_reload_reason: str = "",
    force_restart_reason: str = "",
) -> dict[str, Any]:
    now_ts = now.timestamp()
    disk = disk_generation()
    loaded = _loaded_generation(guard_state)
    loaded_generation = loaded.get("generation_sha256")
    loaded_script = loaded.get("script_sha256")
    disk_script = next(
        (row.get("sha256") for row in disk["files"] if row.get("path") == "scripts/run_wallet_copy_live_guard.py"),
        None,
    )
    if disk_script is None and disk["files"]:
        disk_script = disk["files"][0].get("sha256")
    generation_mismatch = bool(
        (loaded_generation and loaded_generation != disk["sha256"])
        or (not loaded_generation and loaded_script and disk_script and loaded_script != disk_script)
    )
    red, deadman_payload = _deadman_red(deadman, min_red_s=min_red_s)
    raw_unresponsive, guard_payload = _guard_unresponsive(
        guard_state,
        max_age_s=max_guard_state_age_s,
        now_ts=now_ts,
    )
    prior_unresponsive_breaches = int(
        restart_state.get("consecutive_guard_liveness_breaches") or 0
    )
    consecutive_unresponsive_breaches = (
        prior_unresponsive_breaches + 1
        if guard_payload.get("stale_breach")
        else 0
    )
    unresponsive = bool(
        guard_payload.get("pid_dead")
        or consecutive_unresponsive_breaches >= 2
    )
    guard_payload["raw_breach"] = raw_unresponsive
    guard_payload["consecutive_breaches"] = consecutive_unresponsive_breaches
    guard_payload["required_consecutive_breaches"] = 2
    prior_mismatch_sha = str(
        restart_state.get("mismatch_generation_sha256") or ""
    )
    prior_mismatch_since = _parse_iso(
        restart_state.get("mismatch_generation_unchanged_since")
    )
    if generation_mismatch:
        if prior_mismatch_sha == str(disk["sha256"]) and prior_mismatch_since is not None:
            mismatch_since_ts = prior_mismatch_since
        else:
            mismatch_since_ts = now_ts
        mismatch_age_s = max(0.0, now_ts - mismatch_since_ts)
        mismatch_quiescent = mismatch_age_s >= float(
            generation_mismatch_quiescence_s
        )
        mismatch_since = dt.datetime.fromtimestamp(
            mismatch_since_ts, dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")
    else:
        mismatch_age_s = 0.0
        mismatch_quiescent = False
        mismatch_since = None
    storm = _storm_guard(
        restart_state,
        now=now,
        cooldown_s=cooldown_s,
        max_per_day=max_per_day,
    )
    restart_events = (
        restart_state.get("restart_events")
        if isinstance(restart_state.get("restart_events"), list)
        else []
    )
    generation_adoption_reasons = {
        "generation_mismatch_adoption",
        "red_order_flow_generation_mismatch",
    }
    generation_adoption_restarts_today = sum(
        1
        for row in restart_events
        if isinstance(row, dict)
        and _same_utc_day(row.get("at"), now)
    )
    generation_adoption_budget_clear = generation_adoption_restarts_today < 1
    local_skip_reload_restarts_today = sum(
        1
        for row in restart_events
        if isinstance(row, dict)
        and row.get("reason") == "local_skip_generation_mismatch"
        and _same_utc_day(row.get("at"), now)
    )
    local_skip_reload_budget_clear = local_skip_reload_restarts_today < 1
    storm_escalation_events = (
        restart_state.get("storm_escalation_events")
        if isinstance(restart_state.get("storm_escalation_events"), list)
        else []
    )
    storm_escalated_today = any(
        isinstance(row, dict) and _same_utc_day(row.get("at"), now)
        for row in storm_escalation_events
    )
    local_skip_reload_due = bool(
        deadman_payload.get("deadman_class") == "POLICY_CHOKE_LOCAL_SKIP"
        and generation_mismatch
        and mismatch_quiescent
        and float(deadman_payload.get("idle_s") or 0.0) >= float(min_red_s)
        and bool(deadman.get("can_trade"))
    )
    no_admissible_target = bool(
        (
            (deadman.get("policy_choke") or {}).get("wallet_policy_diagnostic")
            if isinstance(deadman.get("policy_choke"), dict)
            else None
        )
        == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
        or deadman.get("wallet_policy_diagnostic")
        == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
        or deadman.get("episode_fire_wallet_policy_diagnostic")
        == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
    )
    deadman_restart_authority = bool(red and not no_admissible_target)
    # A successfully gated selection can coexist with a measured-quiet
    # top-level deadman class: the raw accepted-order deadman is still firing,
    # but the quiet classifier truthfully describes the just-finished source
    # cut.  The explicit mechanical escalation is the adoption authority in
    # that state.  Requiring the top-level status to remain INCIDENT_* made a
    # valid pin impossible to adopt until it expired.
    selection_adoption_due = bool(
        deadman.get("mechanical_escalation")
        == "MANAGED_RESTART_SELECTION_PENDING_ADOPTION"
        and not no_admissible_target
    )
    generation_adoption_due = bool(
        generation_mismatch
        and mismatch_quiescent
        and (deadman_restart_authority or local_skip_reload_due)
    )
    reason = "no_restart_condition"
    restart_required = False
    if deadman_restart_authority and deadman_payload.get("guard_side_halt"):
        reason = "guard_side_halt"
        restart_required = True
    elif force_restart_reason:
        reason = str(force_restart_reason)
        restart_required = True
    elif local_skip_reload_due and local_skip_reload_budget_clear:
        reason = "local_skip_generation_mismatch"
        restart_required = True
    elif local_skip_reload_due and generation_adoption_budget_clear:
        reason = "generation_mismatch_adoption"
        restart_required = True
    elif selection_adoption_due:
        reason = "selection_pending_adoption"
        restart_required = True
    elif deadman_restart_authority and generation_mismatch and mismatch_quiescent:
        reason = "red_order_flow_generation_mismatch"
        restart_required = True
    elif allow_generation_reload and generation_mismatch:
        reason = "operator_approved_generation_reload"
        restart_required = True
    elif unresponsive:
        reason = "guard_unresponsive"
        restart_required = True
    uses_generation_adoption_budget = reason in generation_adoption_reasons
    uses_local_skip_reload_budget = reason == "local_skip_generation_mismatch"
    if uses_local_skip_reload_budget:
        restart_budget_clear = local_skip_reload_budget_clear
    elif uses_generation_adoption_budget:
        restart_budget_clear = generation_adoption_budget_clear
    else:
        restart_budget_clear = storm["daily_cap_clear"]
    next_eligible_ts = now_ts
    next_eligible_reasons: list[str] = []
    if not storm["cooldown_clear"]:
        next_eligible_ts = max(
            next_eligible_ts,
            now_ts + float(storm["cooldown_remaining_s"]),
        )
        next_eligible_reasons.append("storm_cooldown")
    if (
        not restart_budget_clear
        and not uses_generation_adoption_budget
        and not uses_local_skip_reload_budget
    ):
        next_utc_day = (
            now.astimezone(dt.timezone.utc).date() + dt.timedelta(days=1)
        )
        next_midnight = dt.datetime.combine(
            next_utc_day,
            dt.time.min,
            tzinfo=dt.timezone.utc,
        ).timestamp()
        next_eligible_ts = max(next_eligible_ts, next_midnight)
        next_eligible_reasons.append("daily_restart_cap")
    if not restart_budget_clear and uses_generation_adoption_budget:
        next_utc_day = (
            now.astimezone(dt.timezone.utc).date() + dt.timedelta(days=1)
        )
        next_midnight = dt.datetime.combine(
            next_utc_day,
            dt.time.min,
            tzinfo=dt.timezone.utc,
        ).timestamp()
        next_eligible_ts = max(next_eligible_ts, next_midnight)
        next_eligible_reasons.append("generation_adoption_cap")
    if local_skip_reload_due and not local_skip_reload_budget_clear:
        next_utc_day = (
            now.astimezone(dt.timezone.utc).date() + dt.timedelta(days=1)
        )
        next_midnight = dt.datetime.combine(
            next_utc_day,
            dt.time.min,
            tzinfo=dt.timezone.utc,
        ).timestamp()
        next_eligible_ts = max(next_eligible_ts, next_midnight)
        next_eligible_reasons.append("local_skip_reload_cap")
    if generation_adoption_due and not generation_adoption_budget_clear:
        next_utc_day = (
            now.astimezone(dt.timezone.utc).date() + dt.timedelta(days=1)
        )
        next_midnight = dt.datetime.combine(
            next_utc_day,
            dt.time.min,
            tzinfo=dt.timezone.utc,
        ).timestamp()
        next_eligible_ts = max(next_eligible_ts, next_midnight)
        if "generation_adoption_cap" not in next_eligible_reasons:
            next_eligible_reasons.append("generation_adoption_cap")
    if generation_mismatch and not mismatch_quiescent:
        next_eligible_ts = max(
            next_eligible_ts,
            now_ts + max(
                0.0,
                float(generation_mismatch_quiescence_s) - mismatch_age_s,
            ),
        )
        next_eligible_reasons.append("generation_mismatch_quiescence")
    gated = bool(
        restart_required
        and storm["cooldown_clear"]
        and restart_budget_clear
        and not storm_escalated_today
    )
    if restart_required and storm_escalated_today:
        status = "ESCALATE_RESTART_STORM"
    elif restart_required and not storm["cooldown_clear"]:
        status = "WATCH_COOLDOWN"
    elif restart_required and not restart_budget_clear:
        status = "ESCALATE_RESTART_STORM"
    elif gated:
        status = "RESTART_REQUIRED"
    else:
        status = "WATCH"
    decision = {
        "schema_version": 1,
        "kind": "brainless_live_guard_restart",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "snapshot_of": "restart_actuator_decision_at_generated_at",
        "superseded_by_guard_state": {
            "path": "data/research/wallet_copy_live_guard_state.json",
            "authority": (
                "after a restart, guard_code_identity in the live guard state is "
                "authoritative for the resident PID and loaded generation"
            ),
        },
        "status": status,
        "restart_required": restart_required,
        "restart_allowed": gated,
        "reason": reason,
        "deadman_restart_authority": deadman_restart_authority,
        "no_admissible_target": no_admissible_target,
        "deadman": deadman_payload,
        "guard_liveness": guard_payload,
        "disk_generation": disk,
        "loaded_generation": loaded,
        "generation_mismatch": generation_mismatch,
        "generation_mismatch_quiescence": {
            "required_s": float(generation_mismatch_quiescence_s),
            "unchanged_age_s": round(mismatch_age_s, 6),
            "unchanged_since": mismatch_since,
            "clear": mismatch_quiescent,
        },
        "mismatch_generation_sha256": (
            str(disk["sha256"]) if generation_mismatch else None
        ),
        "mismatch_generation_unchanged_since": mismatch_since,
        "consecutive_guard_liveness_breaches": consecutive_unresponsive_breaches,
        "generation_reload": {
            "allowed": bool(allow_generation_reload),
            "reason": str(generation_reload_reason or ""),
        },
        "forced_restart": {
            "enabled": bool(force_restart_reason),
            "reason": str(force_restart_reason or ""),
        },
        "storm_guard": storm,
        "generation_adoption_budget": {
            "restarts_today": generation_adoption_restarts_today,
            "max_restarts_per_day": 1,
            "clear": generation_adoption_budget_clear,
            "reasons": sorted(generation_adoption_reasons),
            "count_basis": "all_RESTART_EXECUTED_events_regardless_of_reason",
        },
        "storm_escalated_today": storm_escalated_today,
        "storm_escalation_events_today": sum(
            1
            for row in storm_escalation_events
            if isinstance(row, dict) and _same_utc_day(row.get("at"), now)
        ),
        "next_eligible_restart_at": dt.datetime.fromtimestamp(
            next_eligible_ts,
            dt.timezone.utc,
        ).isoformat().replace("+00:00", "Z"),
        "next_eligible_restart_reasons": next_eligible_reasons,
        "local_skip_reload_budget": {
            "restarts_today": local_skip_reload_restarts_today,
            "max_restarts_per_day": 1,
            "clear": local_skip_reload_budget_clear,
            "due": local_skip_reload_due,
        },
        "selection_adoption_due": selection_adoption_due,
        "rule": "restart only on red selection adoption, red order-flow generation mismatch, or guard unresponsive; 30m cooldown, max 3/day plus one generation adoption/day",
        "live_orders_allowed": False,
        "paper_only": False,
        "live_path_mutated": False,
    }
    verdict = generation_verdict(decision, now=now)
    decision["generation_verdict"] = verdict
    decision["stale"] = verdict["stale"]
    decision["generation_verdict_status"] = verdict["status"]
    return decision


def _execute_restart(
    start_script: Path,
    *,
    stdout_path: Path,
    stderr_path: Path,
    lock_path: Path = DEFAULT_LOCK,
) -> dict[str, Any]:
    launchd_target = f"gui/{os.getuid()}/{DEFAULT_LAUNCHD_LABEL}"
    launchd_domain = f"gui/{os.getuid()}"
    try:
        launchd_print = subprocess.run(
            ["launchctl", "print", launchd_target],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=10.0,
        )
        launchd_loaded = (
            launchd_print.returncode == 0
            and DEFAULT_LAUNCHD_LABEL in (launchd_print.stdout or "")
        )
    except OSError:
        launchd_print = None
        launchd_loaded = False

    bootout = None
    if launchd_loaded:
        bootout = subprocess.run(
            ["launchctl", "bootout", launchd_target],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=10.0,
        )
        if bootout.returncode != 0:
            raise RuntimeError(
                "refusing restart: launchd guard job could not be unloaded before quiescent sweep"
            )

    pkill = subprocess.run(
        ["pkill", "-f", "scripts/run_wallet_copy_live_guard.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=10.0,
    )
    quiescent = False
    for _attempt in range(100):
        if not _live_guard_process_rows():
            quiescent = True
            break
        time.sleep(0.1)
    if not quiescent:
        if launchd_loaded:
            subprocess.run(
                ["launchctl", "bootstrap", launchd_domain, str(DEFAULT_LAUNCHD_PLIST)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=10.0,
            )
        raise RuntimeError("refusing sweep: live guard did not become quiescent after launchd unload and pkill")

    remnant_sweep = sweep_stale_remnants(
        allowed_paths=QUIESCENT_REMNANT_SWEEP_PATHS,
    )
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    start_process_pid = None
    bootstrap = None
    if launchd_loaded:
        bootstrap = subprocess.run(
            ["launchctl", "bootstrap", launchd_domain, str(DEFAULT_LAUNCHD_PLIST)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=10.0,
        )
        if bootstrap.returncode != 0:
            raise RuntimeError("launchd guard job was unloaded but could not be re-enabled after sweep")
    else:
        stdout = stdout_path.open("ab")
        stderr = stderr_path.open("ab")
        try:
            proc = subprocess.Popen(
                [str(start_script)],
                cwd=ROOT,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            start_process_pid = int(proc.pid)
        finally:
            stdout.close()
            stderr.close()

    actual = _actual_live_guard_pid(lock_path)
    for _attempt in range(100):
        if actual.get("actual_pid"):
            break
        time.sleep(0.1)
        actual = _actual_live_guard_pid(lock_path)
    actual_pid = actual.get("actual_pid") or start_process_pid
    if actual_pid is None:
        raise RuntimeError("restart command completed but no live-guard PID became observable")
    return {
        "pkill_returncode": pkill.returncode,
        "pkill_stdout_tail": (pkill.stdout or "")[-500:],
        "pkill_stderr_tail": (pkill.stderr or "")[-500:],
        "start_script": _display(start_script),
        "start_process_pid": start_process_pid,
        "started_pid": int(actual_pid),
        "actual_pid": actual_pid,
        "actual_pid_source": actual.get("actual_pid_source"),
        "lock_holder": actual.get("lock_holder"),
        "pgrep_rows": actual.get("pgrep_rows"),
        "quiescent_remnant_sweep": remnant_sweep,
        "launchd_coordination": {
            "job_was_loaded": launchd_loaded,
            "target": launchd_target,
            "unload_before_sweep": bool(launchd_loaded and bootout and bootout.returncode == 0),
            "reloaded_after_sweep": bool(launchd_loaded and bootstrap and bootstrap.returncode == 0),
            "print_returncode": None if launchd_print is None else launchd_print.returncode,
            "bootout_returncode": None if bootout is None else bootout.returncode,
            "bootstrap_returncode": None if bootstrap is None else bootstrap.returncode,
        },
        "stdout": _display(stdout_path),
        "stderr": _display(stderr_path),
        "canonical_restart": (
            "launchd bootout -> pkill/wait-quiescent -> sweep -> launchd bootstrap"
            if launchd_loaded
            else "pkill/wait-quiescent -> sweep -> scripts/start_live_guard.sh"
        ),
    }


def _restart_journal_entry(
    decision: dict[str, Any],
    *,
    golden_snapshot: Path,
) -> dict[str, Any]:
    generated_at = str(decision.get("generated_at") or _utc_now())
    compact_ts = generated_at.replace("-", "").replace(":", "").replace("+00:00", "Z")
    reason = str(decision.get("reason") or "guard_restart")
    loaded = decision.get("loaded_generation") if isinstance(decision.get("loaded_generation"), dict) else {}
    return {
        "ts": generated_at,
        "change_id": f"{compact_ts}-canonical-live-guard-restart",
        "mode": "restore",
        "enemy_id": "",
        "defect_id": reason,
        "justification": (
            "Canonical guarded restart required by the live-guard actuator; journaled before the sole "
            "submitter PID is changed."
        ),
        "expected_effect": (
            "Replace the sole live guard and reload the current disk generation while preserving the "
            "operator gate, CopyIntent parity, and configured live caps."
        ),
        "diff_or_commit_ref": str((decision.get("disk_generation") or {}).get("sha256") or ""),
        "golden_snapshot": _display(golden_snapshot),
        "rollback_command": "true # process-only restart; launchd/start_live_guard owns recovery with unchanged config",
        "touched_paths": [
            "data/research/wallet_copy_live_guard_state.json",
            "data/research/brainless_live_guard_restart_state.json",
        ],
        "measured_outcome": (
            f"PENDING_RESTART: previous_pid={loaded.get('pid')}; reason={reason}; "
            f"disk_generation={str((decision.get('disk_generation') or {}).get('sha256') or '')}"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default=str(DEFAULT_DEADMAN))
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    parser.add_argument("--live-change-journal", default=str(DEFAULT_LIVE_CHANGE_JOURNAL))
    parser.add_argument("--golden-snapshot", default=str(DEFAULT_GOLDEN_SNAPSHOT))
    parser.add_argument("--start-script", default=str(DEFAULT_START_SCRIPT))
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--cooldown-s", type=float, default=1800.0)
    parser.add_argument("--max-restarts-per-day", type=int, default=3)
    parser.add_argument("--min-red-s", type=float, default=1800.0)
    parser.add_argument("--max-guard-state-age-s", type=float, default=120.0)
    parser.add_argument("--generation-mismatch-quiescence-s", type=float, default=900.0)
    parser.add_argument("--allow-generation-reload", action="store_true")
    parser.add_argument("--generation-reload-reason", default="")
    parser.add_argument("--force-restart-reason", choices=["guard_memory_rss_threshold"], default="")
    parser.add_argument(
        "--require-feed-health-preflight",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--feed-circuit-state", default=str(DEFAULT_FEED_CIRCUIT_STATE))
    parser.add_argument("--feed-path", default=str(DEFAULT_FEED_PATH))
    parser.add_argument("--feed-min-uptime-s", type=float, default=900.0)
    parser.add_argument("--feed-max-age-s", type=float, default=60.0)
    args = parser.parse_args(argv)

    now = dt.datetime.now(dt.timezone.utc)
    state_path = Path(args.state)
    event_log = Path(args.event_log)
    restart_state = _load_json(state_path, {})
    restart_state = restart_state if isinstance(restart_state, dict) else {}
    if not isinstance(restart_state.get("restart_events"), list):
        log_events = _restart_events_from_log(event_log)
        if log_events:
            restart_state["restart_events"] = log_events[-20:]
            restart_state["last_restart_at"] = log_events[-1].get("at")
    if not isinstance(restart_state.get("storm_escalation_events"), list):
        restart_state["storm_escalation_events"] = []
    try:
        raw_log_rows = [
            json.loads(line)
            for line in event_log.read_text(encoding="utf-8", errors="ignore").splitlines()[-200:]
            if line.strip()
        ] if event_log.exists() else []
    except (OSError, json.JSONDecodeError):
        raw_log_rows = []
    restart_state["storm_escalation_events"] = [
        {
            "at": row.get("generated_at"),
            "status": "ESCALATE_RESTART_STORM",
            "reason": row.get("reason"),
        }
        for row in raw_log_rows
        if isinstance(row, dict)
        and row.get("status") == "ESCALATE_RESTART_STORM"
        and row.get("generated_at")
    ][-20:]
    deadman_state = _load_json(Path(args.deadman), {})
    deadman_state = deadman_state if isinstance(deadman_state, dict) else {}
    decision = build_decision(
        deadman=deadman_state,
        guard_state=_load_json(Path(args.guard_state), {}),
        restart_state=restart_state,
        now=now,
        cooldown_s=float(args.cooldown_s),
        max_per_day=int(args.max_restarts_per_day),
        min_red_s=float(args.min_red_s),
        max_guard_state_age_s=float(args.max_guard_state_age_s),
        generation_mismatch_quiescence_s=float(
            args.generation_mismatch_quiescence_s
        ),
        allow_generation_reload=bool(args.allow_generation_reload),
        generation_reload_reason=str(args.generation_reload_reason or ""),
        force_restart_reason=str(args.force_restart_reason or ""),
    )
    preflight = feed_health_preflight(
        circuit_state_path=Path(args.feed_circuit_state),
        feed_path=Path(args.feed_path),
        canonical_feed_path=DEFAULT_FEED_PATH,
        min_uptime_s=float(args.feed_min_uptime_s),
        max_feed_age_s=float(args.feed_max_age_s),
    )
    decision["feed_health_preflight"] = preflight
    decision["feed_health_preflight_required"] = bool(args.require_feed_health_preflight)
    if decision["restart_allowed"] and args.require_feed_health_preflight and not preflight["passed"]:
        decision["restart_allowed"] = False
        decision["status"] = "WAIT_FEED_HEALTH"
        decision["restart_deferred_reason"] = "feed_health_preflight_failed"
    if (
        decision["restart_allowed"]
        and args.execute
        and decision.get("reason") == "selection_pending_adoption"
    ):
        selected_wallet = _selection_adoption_wallet(deadman_state)
        selection_preflight = (
            _refresh_selection_adoption_liveness(selected_wallet)
            if selected_wallet
            else {
                "status": "ERROR",
                "passed": False,
                "reason": "selection_adoption_wallet_missing",
                "source_wallet": None,
            }
        )
        decision["selection_adoption_liveness_preflight"] = selection_preflight
        if selection_preflight.get("passed") is not True:
            decision["restart_allowed"] = False
            decision["status"] = "WAIT_SELECTION_LIVENESS"
            decision["restart_deferred_reason"] = str(
                selection_preflight.get("reason")
                or "selection_adoption_liveness_preflight_failed"
            )
    if decision["restart_allowed"]:
        if args.execute:
            journal_entry = _restart_journal_entry(
                decision,
                golden_snapshot=Path(args.golden_snapshot),
            )
            _append_jsonl(Path(args.live_change_journal), journal_entry)
            decision["pre_restart_journal"] = {
                "status": "RECORDED_BEFORE_RESTART",
                "path": _display(Path(args.live_change_journal)),
                "change_id": journal_entry["change_id"],
            }
            execution = _execute_restart(
                Path(args.start_script),
                stdout_path=ROOT / "data/research/wallet_copy_live_guard_stdout.log",
                stderr_path=ROOT / "data/research/wallet_copy_live_guard_stderr.log",
                lock_path=Path(args.lock_file),
            )
            decision.update(
                {
                    "status": "RESTART_EXECUTED",
                    "execution": execution,
                    "live_path_mutated": True,
                }
            )
            restart_events = restart_state.get("restart_events")
            restart_events = restart_events if isinstance(restart_events, list) else []
            restart_events = [row for row in restart_events if isinstance(row, dict)]
            restart_events.append(
                {
                    "at": decision["generated_at"],
                    "reason": decision["reason"],
                    "started_pid": execution["started_pid"],
                    "disk_generation_sha256": decision["disk_generation"]["sha256"],
                }
            )
            restart_state = {
                **restart_state,
                "last_restart_at": decision["generated_at"],
                "restart_events": restart_events[-20:],
            }
            decision["storm_guard"] = _storm_guard(
                restart_state,
                now=now,
                cooldown_s=float(args.cooldown_s),
                max_per_day=int(args.max_restarts_per_day),
            )
        else:
            decision["status"] = "WOULD_RESTART"
    persisted_decision = dict(decision)
    published_decision = dict(decision)
    if not args.execute:
        published_decision["dry_run"] = True
        published_decision["computed_liveness_breaches"] = decision.get(
            "consecutive_guard_liveness_breaches"
        )
        for key in (
            "consecutive_guard_liveness_breaches",
            "mismatch_generation_sha256",
            "mismatch_generation_unchanged_since",
        ):
            persisted_decision[key] = restart_state.get(key)
            published_decision[key] = restart_state.get(key)
    else:
        published_decision["dry_run"] = False
    persisted_state = {
        **persisted_decision,
        "last_restart_at": restart_state.get("last_restart_at"),
        "restart_events": restart_state.get("restart_events", []),
        "storm_escalation_events": restart_state.get(
            "storm_escalation_events", []
        ),
        "latest_decision": published_decision,
    }
    _write_json(state_path, persisted_state)
    _append_jsonl(event_log, decision)
    print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
