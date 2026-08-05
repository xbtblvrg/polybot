#!/usr/bin/env python3
"""Run the post-boot live-stack recovery audit.

Flow stage: LIVE/SELF-DEV. This script is intentionally read-only except for
its audit state/log output and optional HANDOFF entry.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402

DATA = ROOT / "data" / "research"
DEFAULT_STATE = DATA / "boot_recovery_audit_state.json"
DEFAULT_HANDOFF = ROOT / "docs" / "agents" / "HANDOFF.md"
HOT_JSON_STATES = (
    DATA / "wallet_copy_live_guard_state.json",
    DATA / "wallet_copy_live_execution_state.json",
    DATA / "wallet_copy_live_execution_arm_state.json",
    DATA / "wallet_copy_history_state.json",
    DATA / "wallet_copy_history_state.json.rotation_d97.rtds_offset.json",
    DATA / "wallet_copy_full_pool_member_queue.json",
    DATA / "state_digest.json",
    DATA / "member_factory_kpi_state.json",
)
RTDS_CAPTURE = ROOT / DEFAULT_RTDS_ACTIVITY_JSONL
RTDS_OFFSET = DATA / "wallet_copy_history_state.json.rotation_d97.rtds_offset.json"
RTDS_RESTAT_ATTEMPTS = 5
RTDS_RESTAT_SLEEP_S = 0.25
DEFAULT_LOCK_HOLDER_PATTERNS = {
    "alpha_decay_eligible_profiles_paper_lane.lock": (
        "run_alpha_decay_eligible_profile_paper_lane.py",
    ),
    "fee_aware_long_horizon_copy_paper.lock": (
        "run_fee_aware_long_horizon_copy_paper_lane.py",
    ),
}
DEFAULT_LOCK_COLLECTOR_STATES = {
    "alpha_decay_eligible_profiles_paper_lane.lock": (
        "alpha_decay_eligible_profiles_paper_lane_latest.json",
    ),
    "fee_aware_long_horizon_copy_paper.lock": (
        "fee_aware_long_horizon_copy_paper_latest.json",
    ),
}
LOCK_START_MTIME_TOLERANCE_S = 2.0
RTDS_ROTATION_FRESH_S = 300.0


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def guard_processes() -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["pgrep", "-fl", "run_wallet_copy_live_guard.py"],
        text=True,
        capture_output=True,
        check=False,
    )
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue
        command = parts[1] if len(parts) > 1 else ""
        if "pgrep" in command or "post_boot_recovery_audit.py" in command:
            continue
        rows.append({"pid": int(parts[0]), "command": command})
    return rows


def process_rows() -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        text=True,
        capture_output=True,
        check=False,
    )
    rows = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        rows.append({"pid": pid, "command": parts[1] if len(parts) > 1 else ""})
    return rows


def _lock_holder(path: Path, processes: list[dict[str, Any]]) -> dict[str, Any]:
    patterns = DEFAULT_LOCK_HOLDER_PATTERNS.get(path.name, ())
    relative = str(path.relative_to(ROOT))
    explicit_lock_args = (
        f"--lock-file {relative}",
        f"--lock-file={relative}",
        f"--lock-file {path}",
        f"--lock-file={path}",
    )
    for process in processes:
        command = str(process.get("command") or "")
        if any(pattern in command for pattern in patterns):
            return {
                "holder_pid": int(process["pid"]),
                "holder_alive": True,
                "holder_command": command,
            }
        if any(lock_arg in command for lock_arg in explicit_lock_args):
            return {
                "holder_pid": int(process["pid"]),
                "holder_alive": True,
                "holder_command": command,
            }
    return {"holder_pid": None, "holder_alive": False, "holder_command": None}


def _lock_collector_start(path: Path) -> dict[str, Any]:
    lock_mtime_ts = path.stat().st_mtime
    for state_name in DEFAULT_LOCK_COLLECTOR_STATES.get(path.name, ()):
        state_path = DATA / state_name
        payload = load_json(state_path, {})
        try:
            collector_start_ts = float(payload.get("collector_start_ts"))
        except (AttributeError, TypeError, ValueError):
            continue
        delta_s = abs(lock_mtime_ts - collector_start_ts)
        return {
            "collector_state": str(state_path.relative_to(ROOT)),
            "collector_start_ts": collector_start_ts,
            "lock_mtime_ts": lock_mtime_ts,
            "collector_start_matches_lock_mtime": delta_s <= LOCK_START_MTIME_TOLERANCE_S,
            "collector_start_mtime_delta_s": round(delta_s, 6),
        }
    return {
        "collector_state": None,
        "collector_start_ts": None,
        "lock_mtime_ts": lock_mtime_ts,
        "collector_start_matches_lock_mtime": False,
        "collector_start_mtime_delta_s": None,
    }


def _nonblocking_flock_probe(path: Path) -> dict[str, Any]:
    try:
        with path.open("a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {
                    "flock_probe_ok": True,
                    "flock_acquired": False,
                    "kernel_holder_present": True,
                    "flock_error": None,
                }
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        return {
            "flock_probe_ok": True,
            "flock_acquired": True,
            "kernel_holder_present": False,
            "flock_error": None,
        }
    except OSError as exc:
        return {
            "flock_probe_ok": False,
            "flock_acquired": False,
            "kernel_holder_present": None,
            "flock_error": f"{type(exc).__name__}: {exc}",
        }


def _lsof_open_handle(path: Path) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["lsof", "-t", "--", str(path)],
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        return {
            "lsof_probe_ok": False,
            "open_handle_present": None,
            "open_handle_pids": [],
            "lsof_error": f"{type(exc).__name__}: {exc}",
        }
    pids = sorted({int(row) for row in proc.stdout.splitlines() if row.strip().isdigit()})
    probe_ok = proc.returncode in {0, 1}
    return {
        "lsof_probe_ok": probe_ok,
        "open_handle_present": bool(pids) if probe_ok else None,
        "open_handle_pids": pids,
        "lsof_error": None if probe_ok else (proc.stderr.strip() or f"returncode={proc.returncode}"),
    }


def json_state_audit() -> dict[str, Any]:
    failures = []
    rows = []
    for path in HOT_JSON_STATES:
        try:
            loaded = json.loads(path.read_text())
            rows.append({"path": str(path.relative_to(ROOT)), "ok": True, "type": type(loaded).__name__, "bytes": path.stat().st_size})
        except Exception as exc:
            failures.append({"path": str(path.relative_to(ROOT)), "error": f"{type(exc).__name__}: {exc}"})
    return {"failures": failures, "rows": rows}


def stale_remnants(
    *,
    processes: list[dict[str, Any]] | None = None,
    now_ts: float | None = None,
    previous_lock_mtimes: dict[str, float] | None = None,
) -> dict[str, Any]:
    now_ts = now_ts if now_ts is not None else datetime.now(tz=UTC).timestamp()
    processes = process_rows() if processes is None else processes
    stale_tmp = []
    stale_tmp_evidence = []
    stale_locks = []
    stale_lock_evidence = []
    held_locks = []
    held_tmp = []
    cyclic_active_locks = []
    lock_observations: dict[str, float] = {}
    active_allow = {
        "brainless_ops.lock",
        "wallet_copy_live_guard.lock",
        ".wallet_copy_live_execution_state.json.lock",
        ".btc5m_late_window_penny_watcher_state.json.lock",
        ".e7_1_spot_open_paper_lane_state.json.lock",
        ".e7_spot_open_paper_lane_state.json.lock",
    }
    for path in DATA.glob(".*.tmp"):
        try:
            age_s = now_ts - path.stat().st_mtime
        except FileNotFoundError:
            # Atomic writers may remove a temporary file between glob() and
            # stat(). A vanished remnant is already clean.
            continue
        if age_s > 600:
            open_handle = _lsof_open_handle(path)
            evidence = {
                "path": str(path.relative_to(ROOT)),
                "age_s": round(age_s, 6),
                **open_handle,
            }
            if open_handle["lsof_probe_ok"] and open_handle["open_handle_present"] is False:
                stale_tmp.append(str(path.relative_to(ROOT)))
                stale_tmp_evidence.append(evidence)
            else:
                held_tmp.append(evidence)
    lock_paths = {
        path.resolve(): path
        for pattern in ("*.lock", ".*.lock")
        for path in DATA.glob(pattern)
    }
    for path in sorted(lock_paths.values(), key=lambda item: str(item)):
        try:
            lock_mtime_ts = path.stat().st_mtime
        except FileNotFoundError:
            # Lock owners can finish and unlink after discovery.
            continue
        relative = str(path.relative_to(ROOT))
        lock_observations[relative] = lock_mtime_ts
        age_s = now_ts - lock_mtime_ts
        if path.name not in active_allow and age_s > 3600:
            holder = _lock_holder(path, processes)
            collector = _lock_collector_start(path)
            flock = _nonblocking_flock_probe(path)
            evidence = {
                "path": str(path.relative_to(ROOT)),
                "age_s": round(age_s, 6),
                **holder,
                **collector,
                **flock,
            }
            unheld = (
                not holder["holder_alive"]
                and not collector["collector_start_matches_lock_mtime"]
                and flock["flock_probe_ok"]
                and flock["flock_acquired"]
            )
            prior_mtime = (
                previous_lock_mtimes.get(relative)
                if previous_lock_mtimes is not None
                else lock_mtime_ts
            )
            unchanged_across_audits = (
                prior_mtime is not None
                and abs(float(prior_mtime) - float(lock_mtime_ts)) <= 1e-6
            )
            if unheld and unchanged_across_audits:
                stale_locks.append(relative)
                stale_lock_evidence.append(
                    {**evidence, "classification": "STALE_UNHELD"}
                )
            elif unheld:
                cyclic_active_locks.append(
                    {
                        **evidence,
                        "classification": "CYCLIC_ACTIVE",
                        "previous_lock_mtime_ts": prior_mtime,
                    }
                )
            else:
                held_locks.append(evidence)
    return {
        "stale_tmp": sorted(stale_tmp),
        "stale_tmp_evidence": sorted(stale_tmp_evidence, key=lambda row: row["path"]),
        "stale_locks": sorted(stale_locks),
        "stale_lock_evidence": sorted(stale_lock_evidence, key=lambda row: row["path"]),
        "held_locks": sorted(held_locks, key=lambda row: row["path"]),
        "held_tmp": sorted(held_tmp, key=lambda row: row["path"]),
        "cyclic_active_locks": sorted(
            cyclic_active_locks, key=lambda row: row["path"]
        ),
        "lock_observations": lock_observations,
    }


def sweep_stale_remnants(
    *,
    allowed_paths: set[Path] | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Delete only freshly re-proven remnants while the live guard is quiescent."""
    now_ts = now_ts if now_ts is not None else time.time()
    guards = guard_processes()
    if guards:
        return {
            "status": "REFUSED_GUARD_NOT_QUIESCENT",
            "flow_stage": "LIVE/SELF-DEV",
            "guard_processes": guards,
            "deleted": [],
            "retained": [],
        }
    allowed = (
        {path.resolve() for path in allowed_paths}
        if allowed_paths is not None
        else None
    )
    processes = process_rows()
    evidence = stale_remnants(processes=processes, now_ts=now_ts)
    deleted = []
    retained = []

    for row in evidence["stale_lock_evidence"]:
        path = ROOT / str(row["path"])
        if allowed is not None and path.resolve() not in allowed:
            retained.append({**row, "sweep_status": "RETAINED_NOT_ALLOWLISTED"})
            continue
        try:
            handle = path.open("a+")
        except OSError as exc:
            retained.append(
                {
                    **row,
                    "sweep_status": "RETAINED_OPEN_FAILED",
                    "sweep_error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                retained.append({**row, "sweep_status": "RETAINED_KERNEL_HELD"})
                continue
            age_s = now_ts - path.stat().st_mtime
            holder = _lock_holder(path, process_rows())
            collector = _lock_collector_start(path)
            if (
                age_s <= 3600
                or holder["holder_alive"]
                or collector["collector_start_matches_lock_mtime"]
            ):
                retained.append(
                    {
                        **row,
                        **holder,
                        **collector,
                        "age_s": round(age_s, 6),
                        "sweep_status": "RETAINED_REPROOF_FAILED",
                    }
                )
                continue
            path.unlink()
            deleted.append(
                {
                    **row,
                    "age_s": round(age_s, 6),
                    "sweep_status": "DELETED_PROVEN_STALE_LOCK",
                    "proof": "allowlisted+age_gt_3600+dead_holder+collector_mismatch+exclusive_flock_acquired",
                }
            )
        except OSError as exc:
            retained.append(
                {
                    **row,
                    "sweep_status": "RETAINED_DELETE_FAILED",
                    "sweep_error": f"{type(exc).__name__}: {exc}",
                }
            )
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            handle.close()

    for row in evidence["stale_tmp_evidence"]:
        path = ROOT / str(row["path"])
        if allowed is not None and path.resolve() not in allowed:
            retained.append({**row, "sweep_status": "RETAINED_NOT_ALLOWLISTED"})
            continue
        try:
            age_s = now_ts - path.stat().st_mtime
        except OSError as exc:
            retained.append(
                {
                    **row,
                    "sweep_status": "RETAINED_STAT_FAILED",
                    "sweep_error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        open_handle = _lsof_open_handle(path)
        if (
            age_s <= 600
            or not open_handle["lsof_probe_ok"]
            or open_handle["open_handle_present"] is not False
        ):
            retained.append(
                {
                    **row,
                    **open_handle,
                    "age_s": round(age_s, 6),
                    "sweep_status": "RETAINED_REPROOF_FAILED",
                }
            )
            continue
        try:
            path.unlink()
            deleted.append(
                {
                    **row,
                    **open_handle,
                    "age_s": round(age_s, 6),
                    "sweep_status": "DELETED_PROVEN_STALE_TMP",
                    "proof": "allowlisted+age_gt_600+lsof_no_open_handle",
                }
            )
        except OSError as exc:
            retained.append(
                {
                    **row,
                    "sweep_status": "RETAINED_DELETE_FAILED",
                    "sweep_error": f"{type(exc).__name__}: {exc}",
                }
            )
    return {
        "status": "PASS" if not retained else "PARTIAL_RETAINED",
        "flow_stage": "LIVE/SELF-DEV",
        "guard_processes": [],
        "deleted": deleted,
        "retained": retained,
    }


def _capture_stat(path: Path) -> Any:
    return path.stat()


def rtds_audit(*, now_ts: float | None = None) -> dict[str, Any]:
    now_ts = now_ts if now_ts is not None else time.time()
    offset = load_json(RTDS_OFFSET, {})
    if not RTDS_CAPTURE.exists() or not isinstance(offset, dict):
        return {"ok": False, "reason": "capture_or_offset_missing"}
    stat = _capture_stat(RTDS_CAPTURE)
    state_inode = int(offset.get("inode") or 0)
    state_offset = int(offset.get("offset") or 0)
    state_size = int(offset.get("size") or 0)
    initial_capture_size = stat.st_size
    restat_capture_size = None
    restat_recovered = False
    same_inode = state_inode == stat.st_ino
    capture_mtime = getattr(stat, "st_mtime", None)
    capture_mtime_age_s = (
        max(0.0, now_ts - float(capture_mtime))
        if capture_mtime is not None
        else None
    )
    current_capture_fresh = bool(
        capture_mtime_age_s is not None and capture_mtime_age_s < RTDS_ROTATION_FRESH_S
    )
    state_size_consistent = same_inode and state_size <= stat.st_size
    ok = same_inode and state_offset <= stat.st_size
    if same_inode and (not ok or not state_size_consistent):
        for attempt in range(RTDS_RESTAT_ATTEMPTS):
            if attempt > 0:
                time.sleep(RTDS_RESTAT_SLEEP_S)
            try:
                restat = _capture_stat(RTDS_CAPTURE)
            except Exception:
                break
            restat_capture_size = restat.st_size
            restat_offset_ok = restat.st_ino == state_inode and state_offset <= restat.st_size
            restat_state_size_consistent = restat.st_ino == state_inode and state_size <= restat.st_size
            if restat_offset_ok:
                stat = restat
                ok = True
                restat_recovered = not (same_inode and state_offset <= initial_capture_size) or (
                    not state_size_consistent and restat_state_size_consistent
                )
                state_size_consistent = restat_state_size_consistent
                break
    rotation_fully_consumed = bool(
        not same_inode
        and state_offset == state_size
        and state_size > 0
    )
    rotation_growing = False
    if rotation_fully_consumed and not current_capture_fresh:
        for attempt in range(RTDS_RESTAT_ATTEMPTS):
            if attempt > 0:
                time.sleep(RTDS_RESTAT_SLEEP_S)
            try:
                restat = _capture_stat(RTDS_CAPTURE)
            except Exception:
                break
            restat_capture_size = restat.st_size
            if restat.st_ino == stat.st_ino and restat.st_size > initial_capture_size:
                stat = restat
                rotation_growing = True
                break
    if rotation_fully_consumed and (current_capture_fresh or rotation_growing):
        ok = True
        race_classification = "EXPECTED_ROTATION_FULLY_CONSUMED_PENDING_MIDNIGHT_ADOPTION"
    elif ok and not state_size_consistent:
        race_classification = "KNOWN_BENIGN_STATE_SIZE_AHEAD_CAPTURE_RACE"
    elif restat_recovered:
        race_classification = "KNOWN_BENIGN_GROWING_CAPTURE_RACE"
    else:
        race_classification = None
    return {
        "ok": ok,
        "capture_size": stat.st_size,
        "initial_capture_size": initial_capture_size,
        "restat_capture_size": restat_capture_size,
        "restat_recovered": restat_recovered,
        "race_classification": race_classification,
        "rotation_fully_consumed": rotation_fully_consumed,
        "rotation_growing": rotation_growing,
        "capture_mtime_age_s": (
            round(capture_mtime_age_s, 6)
            if capture_mtime_age_s is not None
            else None
        ),
        "current_capture_fresh": current_capture_fresh,
        "state_size_consistent": state_size_consistent,
        "state_size": state_size,
        "offset": state_offset,
        "capture_inode": stat.st_ino,
        "state_inode": state_inode,
    }


def ledger_audit() -> dict[str, Any]:
    ledger = load_json(DATA / "wallet_copy_live_execution_state.json", {})
    summary = ledger.get("summary") if isinstance(ledger, dict) else {}
    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    submitted = [row for row in orders if isinstance(row, dict) and str(row.get("final_status") or row.get("status") or "").upper() == "SUBMITTED"]
    latest = orders[-1] if orders and isinstance(orders[-1], dict) else {}
    return {
        "orders": summary.get("live_orders", len(orders)),
        "fills": summary.get("filled_orders"),
        "rejects": summary.get("rejected_orders"),
        "submitted_summary": summary.get("submitted_orders"),
        "submitted_rows": len(submitted),
        "latest_order_ts": summary.get("latest_order_ts") or latest.get("submitted_at"),
        "latest_order_status": latest.get("final_status") or latest.get("status"),
    }


def build_payload() -> dict[str, Any]:
    guards = guard_processes()
    states = json_state_audit()
    previous_state = load_json(DEFAULT_STATE, {})
    previous_lock_mtimes = (
        (previous_state.get("stale_remnants") or {}).get("lock_observations")
        if isinstance(previous_state, dict)
        else {}
    )
    remnants = stale_remnants(
        previous_lock_mtimes=previous_lock_mtimes
        if isinstance(previous_lock_mtimes, dict)
        else {}
    )
    rtds = rtds_audit()
    ledger = ledger_audit()
    defects = []
    if len(guards) != 1:
        defects.append("single_submitter_pid_count")
    if states["failures"]:
        defects.append("hot_state_json_parse")
    if ledger["submitted_rows"] or int(ledger.get("submitted_summary") or 0):
        defects.append("ledger_submitted_orders_present")
    if not rtds.get("ok"):
        defects.append("rtds_offset_inconsistent")
    if remnants["stale_tmp"] or remnants["stale_locks"]:
        defects.append("stale_tmp_or_lock_remnants")
    return {
        "schema_version": 1,
        "kind": "boot_recovery_audit",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "PASS" if not defects else "DEFECT",
        "defects": defects,
        "single_submitter": {"pid_count": len(guards), "processes": guards},
        "ledger": ledger,
        "hot_json_states": states,
        "rtds_capture": rtds,
        "stale_remnants": remnants,
    }


def append_handoff(path: Path, payload: dict[str, Any]) -> None:
    defects = payload.get("defects") or []
    lines = [
        "",
        f"## {payload['generated_at']} BOOT-RECOVERY STATUS [LIVE/SELF-DEV]",
        (
            "- LIVE audit: "
            f"single_submitter_pids={payload['single_submitter']['pid_count']} "
            f"ledger_submitted_rows={payload['ledger']['submitted_rows']} "
            f"json_failures={len(payload['hot_json_states']['failures'])} "
            f"rtds_offset_ok={payload['rtds_capture'].get('ok')} "
            f"stale_tmp={len(payload['stale_remnants']['stale_tmp'])} "
            f"stale_locks={len(payload['stale_remnants']['stale_locks'])}."
        ),
    ]
    if defects:
        lines.append(
            "- defect | attempts (3+) | next: boot-recovery audit defects="
            + ",".join(str(item) for item in defects)
            + " | attempts: launchd RunAtLoad audit, JSON parse audit, RTDS offset audit | next: run heartbeat repair path."
        )
    else:
        lines.append("- LIVE/SELF-DEV boot-recovery audit PASS; next: continue normal heartbeat flow.")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--handoff", default=str(DEFAULT_HANDOFF))
    parser.add_argument("--append-handoff", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_payload()
    state = Path(args.state)
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.append_handoff:
        append_handoff(Path(args.handoff), payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
