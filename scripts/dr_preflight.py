#!/usr/bin/env python3
"""Preflight the wallet-copy off-machine disaster-recovery push path."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "research" / "wallet_copy_dr_preflight_latest.json"
DEFAULT_BUNDLE_DIR = Path.home() / "polymarket-agent-dr"
DEFAULT_SNAPSHOT_BRANCH = "dr-main"
DEFAULT_MAX_TRACKED_FILE_BYTES = 95 * 1024 * 1024
DEFAULT_MAX_SNAPSHOT_BYTES = 400 * 1024 * 1024
SNAPSHOT_HEADROOM_FLOOR = 40 * 1024 * 1024
DR_SNAPSHOT_POLICY_ID = "DR-SNAPSHOT-POLICY-20260728"
SNAPSHOT_REGENERABLE_EXCLUDES = {
    "data/research/wallet_copy_full_universe_copyability_latest.json": (
        "regenerable full-universe replay output; source events and code remain in DR"
    ),
    "data/research/wallet_copy_top10_broad_paper_measurement_state.json": (
        "regenerable paper measurement state; rebuild from the tracked runner, "
        "registry, and canonical source-event inputs"
    ),
    "data/research/copy_event_triggered_cycle_scheduler_paper_lane_latest.json": (
        "regenerable paper scheduler state; canonical inputs and implementation remain in DR"
    ),
    "data/research/wide_multiwallet_consensus_state.json": (
        "regenerable WIDE multiwallet-consensus paper supervisor state; rebuild with "
        "`python3 scripts/run_wide_multiwallet_consensus_slice.py` from tracked "
        "code + canonical source events"
    ),
    "data/research/wide_sequential_quorum_state.json": (
        "regenerable WIDE sequential-quorum paper supervisor state; rebuild with "
        "`python3 scripts/run_wide_sequential_quorum_slice.py` from tracked "
        "code + canonical source events"
    ),
    "data/research/btc5m_structural_scalp_forward_source_events.jsonl": (
        "NOT regenerable: live forward evidence, but >95MiB exceeds the per-file "
        "snapshot cap and was failing the whole DR push; preserved on local disk "
        "and in main-repo history for the 2026-07-23T02:00Z method gate; full "
        "untrack-or-split disposition ruled for after that gate (Fable 2026-07-22)"
    ),
    "data/research/queue_remote_dataapi_fresh_flow_probe_checkpoint.json": (
        "regenerable queue liveness checkpoint; regenerate with "
        "`python3 scripts/probe_queue_remote_dataapi_fresh_flow.py` from the "
        "tracked candidate registry and canonical Data API inputs"
    ),
    "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json": (
        "regenerable queue liveness report; regenerate with "
        "`python3 scripts/probe_queue_remote_dataapi_fresh_flow.py` from the "
        "tracked candidate registry and canonical Data API inputs"
    ),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_timing() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "enabled": True,
        "started_at": _utc_now_iso(),
        "phases": [],
    }


def _finish_timing_phase(timing: dict[str, Any], name: str, started_at: float, **extra: Any) -> None:
    phase = {
        "name": name,
        "duration_s": round(max(0.0, time.perf_counter() - started_at), 6),
    }
    phase.update(extra)
    timing.setdefault("phases", []).append(phase)


def _run(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False, env=env)


def _git_lines(*args: str) -> list[str]:
    result = _run(["git", *args])
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _git_lines_z(*args: str) -> list[str]:
    result = _run(["git", *args])
    if result.returncode != 0:
        return []
    return [line for line in result.stdout.split("\0") if line]


def _contains_secret_path(path: str) -> bool:
    name = Path(path).name.lower()
    if name in {".env.example", ".env.sample", ".env.template"}:
        return False
    return name == ".env" or name.startswith(".env.") or "secret" in name or "private_key" in name


def _is_inside_repo(path: Path) -> bool:
    resolved = path.expanduser().resolve()
    root = ROOT.resolve()
    return resolved == root or root in resolved.parents


def _path_size(path: str | Path) -> int:
    try:
        return int((ROOT / path).stat().st_size if not isinstance(path, Path) or not path.is_absolute() else path.stat().st_size)
    except OSError:
        return 0


def _tracked_file_sizes() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _git_lines_z("ls-files", "-z"):
        rows.append({"path": path, "size_bytes": _path_size(path)})
    return rows


def _size_gate(rows: list[dict[str, Any]], *, max_file_bytes: int, max_total_bytes: int) -> dict[str, Any]:
    total = sum(int(row.get("size_bytes") or 0) for row in rows)
    oversized = [
        dict(row)
        for row in sorted(rows, key=lambda item: int(item.get("size_bytes") or 0), reverse=True)
        if int(row.get("size_bytes") or 0) > int(max_file_bytes)
    ]
    return {
        "file_count": len(rows),
        "total_bytes": total,
        "max_file_bytes": int(max_file_bytes),
        "max_total_bytes": int(max_total_bytes),
        "oversized_files": oversized,
        "oversized_file_count": len(oversized),
        "total_within_gate": total <= int(max_total_bytes),
        "max_file_within_gate": not oversized,
        "pass": total <= int(max_total_bytes) and not oversized,
    }


def _bundle_state_paths() -> list[Path]:
    candidates = [
        ROOT / "docs" / "agents" / "HANDOFF.md",
        ROOT / "docs" / "agents" / "AUTONOMOUS_FLOW.md",
        ROOT / "docs" / "agents" / "CODEX_TASK.md",
        ROOT / "docs" / "agents" / "HEARTBEAT_PROMPT.md",
        ROOT / "data" / "research" / "state_digest.md",
        ROOT / "data" / "research" / "state_digest.json",
        ROOT / "data" / "research" / "brainless_ops_latest.json",
        ROOT / "data" / "research" / "data_layer_v1_manifest.json",
        ROOT / "data" / "research" / "wallet_copy_dr_preflight_latest.json",
        ROOT / "data" / "research" / "wallet_copy_live_guard_state.json",
        ROOT / "data" / "research" / "wallet_copy_live_execution_state.json",
        ROOT / "data" / "research" / "wallet_copy_full_pool_member_queue.json",
        ROOT / "data" / "research" / "member_factory_kpi_state.json",
    ]
    config_dir = ROOT / "configs"
    if config_dir.exists():
        candidates.extend(path for path in config_dir.rglob("*") if path.is_file())
    return [path for path in candidates if path.exists() and not _contains_secret_path(str(path.relative_to(ROOT)))]


def _paper_state_marker(path: str) -> str | None:
    name = Path(path).name
    if not (
        name.endswith("_state.json")
        or name.endswith("_latest.json")
        or name.endswith("_checkpoint.json")
    ):
        return None
    try:
        payload = json.loads((ROOT / path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("paper_only") is True:
        return "paper_only=true"
    if payload.get("live_orders_allowed") is False:
        return "live_orders_allowed=false"
    return None


def _policy_auto_excludable(path: str, marker: str | None) -> bool:
    relative = Path(path)
    name = relative.name
    if relative.parts[:2] != ("data", "research") or marker is None:
        return False
    if not (
        name.endswith("_state.json")
        or name.endswith("_latest.json")
        or name.endswith("_checkpoint.json")
    ):
        return False
    lowered = path.lower()
    if (
        name.endswith("_events.jsonl")
        or name.endswith("_terminals.jsonl")
        or any(token in lowered for token in ("ledger", "orders", "decision", "guard"))
        or name.startswith("state_digest.")
        or relative.parts[0] in {"docs", "configs"}
    ):
        return False
    return True


def _apply_policy_auto_exclusions(
    rows: list[dict[str, Any]],
    *,
    max_total_bytes: int,
    headroom_floor: int = SNAPSHOT_HEADROOM_FLOOR,
) -> dict[str, Any]:
    kept = [dict(row) for row in rows]
    total = sum(int(row.get("size_bytes") or 0) for row in kept)
    excluded: list[dict[str, Any]] = []
    eligible = sorted(
        (
            row
            for row in kept
            if _policy_auto_excludable(
                str(row.get("path") or ""),
                str(row.get("marker")) if row.get("marker") else None,
            )
        ),
        key=lambda row: (-int(row.get("size_bytes") or 0), str(row.get("path") or "")),
    )
    for row in eligible:
        if int(max_total_bytes) - total >= int(headroom_floor):
            break
        kept.remove(row)
        total -= int(row.get("size_bytes") or 0)
        excluded.append(
            {
                "path": row.get("path"),
                "size_bytes": int(row.get("size_bytes") or 0),
                "marker": row.get("marker"),
                "reason": (
                    f"auto-excluded under {DR_SNAPSHOT_POLICY_ID}: regenerable "
                    "paper-only derived state, rebuilt by its tracked writer; "
                    "canonical inputs remain in DR"
                ),
            }
        )
    headroom = int(max_total_bytes) - total
    return {
        "rows": kept,
        "policy_auto_excluded": excluded,
        "headroom_bytes": headroom,
        "headroom_floor_bytes": int(headroom_floor),
        "headroom_below_floor": headroom < int(headroom_floor),
        "auto_excludable_class_exhausted": bool(
            headroom < int(headroom_floor)
            and not any(
                _policy_auto_excludable(
                    str(row.get("path") or ""),
                    str(row.get("marker")) if row.get("marker") else None,
                )
                for row in kept
            )
        ),
    }


def _snapshot_plan(
    *,
    max_file_bytes: int,
    max_total_bytes: int,
    headroom_floor: int = SNAPSHOT_HEADROOM_FLOOR,
) -> dict[str, Any]:
    tracked = set(_git_lines_z("ls-files", "-z"))
    raw_forced = {
        str(path.relative_to(ROOT))
        for path in _bundle_state_paths()
        if path.exists() and not _contains_secret_path(str(path.relative_to(ROOT)))
    }
    forced: set[str] = set()
    for path in raw_forced:
        gz_path = f"{path}.gz"
        if _path_size(path) > int(max_file_bytes) and (ROOT / gz_path).exists():
            forced.add(gz_path)
        else:
            forced.add(path)
    for path in (ROOT / "configs").rglob("*.gz") if (ROOT / "configs").exists() else []:
        forced.add(str(path.relative_to(ROOT)))
    candidates = sorted((tracked - set(SNAPSHOT_REGENERABLE_EXCLUDES)) | forced)
    rows: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for path in candidates:
        if _contains_secret_path(path):
            rejected.append({"path": path, "reason": "secret_path", "size_bytes": _path_size(path)})
            continue
        if not (ROOT / path).exists():
            rejected.append({"path": path, "reason": "missing", "size_bytes": 0})
            continue
        rows.append(
            {
                "path": path,
                "size_bytes": _path_size(path),
                "marker": _paper_state_marker(path),
            }
        )
    policy = _apply_policy_auto_exclusions(
        rows,
        max_total_bytes=max_total_bytes,
        headroom_floor=headroom_floor,
    )
    accepted: list[str] = []
    for row in policy["rows"]:
        if int(row["size_bytes"]) > int(max_file_bytes):
            rejected.append(
                {
                    "path": row["path"],
                    "reason": "over_max_file_bytes",
                    "size_bytes": row["size_bytes"],
                }
            )
        else:
            accepted.append(str(row["path"]))
    return {
        "paths": accepted,
        "rejected": rejected,
        **{key: value for key, value in policy.items() if key != "rows"},
    }


def _snapshot_candidate_paths(*, max_file_bytes: int) -> tuple[list[str], list[dict[str, Any]]]:
    plan = _snapshot_plan(
        max_file_bytes=max_file_bytes,
        max_total_bytes=DEFAULT_MAX_SNAPSHOT_BYTES,
    )
    return plan["paths"], plan["rejected"]


def _snapshot_regenerable_exclusions() -> list[dict[str, Any]]:
    tracked = set(_git_lines_z("ls-files", "-z"))
    return [
        {
            "path": path,
            "size_bytes": _path_size(path),
            "reason": reason,
        }
        for path, reason in sorted(SNAPSHOT_REGENERABLE_EXCLUDES.items())
        if path in tracked and (ROOT / path).exists()
    ]


def _snapshot_size_report(*, max_file_bytes: int, max_total_bytes: int) -> dict[str, Any]:
    plan = _snapshot_plan(
        max_file_bytes=max_file_bytes,
        max_total_bytes=max_total_bytes,
    )
    paths, rejected = plan["paths"], plan["rejected"]
    rows = [{"path": path, "size_bytes": _path_size(path)} for path in paths]
    policy_auto_excluded = plan["policy_auto_excluded"]
    excluded = [*_snapshot_regenerable_exclusions(), *policy_auto_excluded]
    return {
        **_size_gate(rows, max_file_bytes=max_file_bytes, max_total_bytes=max_total_bytes),
        "rejected_candidates": rejected,
        "rejected_candidate_count": len(rejected),
        "excluded_regenerable": excluded,
        "excluded_regenerable_count": len(excluded),
        "excluded_regenerable_bytes": sum(int(row.get("size_bytes") or 0) for row in excluded),
        "policy_auto_excluded": policy_auto_excluded,
        "headroom_bytes": int(max_total_bytes) - sum(int(row["size_bytes"]) for row in rows),
        "headroom_floor_bytes": plan["headroom_floor_bytes"],
        "headroom_below_floor": plan["headroom_below_floor"],
        "auto_excludable_class_exhausted": plan["auto_excludable_class_exhausted"],
    }


def push_snapshot(
    *,
    remote: str,
    snapshot_branch: str,
    max_file_bytes: int,
    max_total_bytes: int,
) -> dict[str, Any]:
    timing = _new_timing()
    phase_started = time.perf_counter()
    plan = _snapshot_plan(
        max_file_bytes=max_file_bytes,
        max_total_bytes=max_total_bytes,
    )
    paths, rejected = plan["paths"], plan["rejected"]
    policy_auto_excluded = plan["policy_auto_excluded"]
    excluded = [*_snapshot_regenerable_exclusions(), *policy_auto_excluded]
    rows = [{"path": path, "size_bytes": _path_size(path)} for path in paths]
    gate = _size_gate(rows, max_file_bytes=max_file_bytes, max_total_bytes=max_total_bytes)
    _finish_timing_phase(
        timing,
        "snapshot_candidate_size_gate",
        phase_started,
        paths=len(paths),
        rejected=len(rejected),
        total_bytes=gate.get("total_bytes"),
    )
    if rejected or not gate["pass"]:
        timing["total_s_before_return"] = round(
            sum(float(row.get("duration_s") or 0.0) for row in timing.get("phases", []) if isinstance(row, dict)),
            6,
        )
        return {
            "returncode": 2,
            "status": "SNAPSHOT_SIZE_GATE_FAILED",
            "remote": remote,
            "remote_ref": f"refs/heads/{snapshot_branch}",
            "size_gate": gate,
            "rejected_candidates": rejected,
            "excluded_regenerable": excluded,
            "policy_auto_excluded": policy_auto_excluded,
            "headroom_bytes": int(max_total_bytes) - int(gate["total_bytes"]),
            "headroom_floor_bytes": plan["headroom_floor_bytes"],
            "headroom_below_floor": plan["headroom_below_floor"],
            "stdout_tail": "",
            "stderr_tail": "",
            "timing": timing,
        }
    with tempfile.TemporaryDirectory(prefix="wallet-copy-dr-index-") as tmp_dir:
        env = dict(os.environ)
        env["GIT_INDEX_FILE"] = str(Path(tmp_dir) / "index")
        phase_started = time.perf_counter()
        read_tree = _run(["git", "read-tree", "--empty"], env=env)
        _finish_timing_phase(timing, "git_read_tree_empty", phase_started, returncode=read_tree.returncode)
        if read_tree.returncode != 0:
            return {
                "returncode": read_tree.returncode,
                "status": "SNAPSHOT_INDEX_FAILED",
                "remote": remote,
                "remote_ref": f"refs/heads/{snapshot_branch}",
                "size_gate": gate,
                "stdout_tail": read_tree.stdout[-2000:],
                "stderr_tail": read_tree.stderr[-2000:],
                "timing": timing,
            }
        add_started = time.perf_counter()
        add_chunks = 0
        for chunk_start in range(0, len(paths), 200):
            chunk = paths[chunk_start : chunk_start + 200]
            add_chunks += 1
            add = _run(["git", "add", "-f", "--", *chunk], env=env)
            if add.returncode != 0:
                _finish_timing_phase(
                    timing,
                    "git_add_snapshot_paths",
                    add_started,
                    chunks=add_chunks,
                    paths=len(paths),
                    returncode=add.returncode,
                )
                return {
                    "returncode": add.returncode,
                    "status": "SNAPSHOT_ADD_FAILED",
                    "remote": remote,
                    "remote_ref": f"refs/heads/{snapshot_branch}",
                    "size_gate": gate,
                    "stdout_tail": add.stdout[-2000:],
                    "stderr_tail": add.stderr[-2000:],
                    "timing": timing,
                }
        _finish_timing_phase(
            timing,
            "git_add_snapshot_paths",
            add_started,
            chunks=add_chunks,
            paths=len(paths),
            returncode=0,
        )
        phase_started = time.perf_counter()
        tree = _run(["git", "write-tree"], env=env)
        _finish_timing_phase(timing, "git_write_tree", phase_started, returncode=tree.returncode)
        if tree.returncode != 0:
            return {
                "returncode": tree.returncode,
                "status": "SNAPSHOT_TREE_FAILED",
                "remote": remote,
                "remote_ref": f"refs/heads/{snapshot_branch}",
                "size_gate": gate,
                "stdout_tail": tree.stdout[-2000:],
                "stderr_tail": tree.stderr[-2000:],
                "timing": timing,
            }
        message = f"DR snapshot {_utc_now_iso()}"
        phase_started = time.perf_counter()
        commit = _run(["git", "commit-tree", tree.stdout.strip(), "-m", message], env=env)
        _finish_timing_phase(timing, "git_commit_tree", phase_started, returncode=commit.returncode)
        if commit.returncode != 0:
            return {
                "returncode": commit.returncode,
                "status": "SNAPSHOT_COMMIT_FAILED",
                "remote": remote,
                "remote_ref": f"refs/heads/{snapshot_branch}",
                "size_gate": gate,
                "stdout_tail": commit.stdout[-2000:],
                "stderr_tail": commit.stderr[-2000:],
                "timing": timing,
            }
        commit_sha = commit.stdout.strip()
    phase_started = time.perf_counter()
    push = _run(["git", "push", "--force-with-lease", remote, f"{commit_sha}:refs/heads/{snapshot_branch}"])
    _finish_timing_phase(timing, "git_push_snapshot", phase_started, returncode=push.returncode)
    timing["total_s_before_return"] = round(
        sum(float(row.get("duration_s") or 0.0) for row in timing.get("phases", []) if isinstance(row, dict)),
        6,
    )
    return {
        "returncode": push.returncode,
        "status": "SNAPSHOT_PUSHED" if push.returncode == 0 else "SNAPSHOT_PUSH_FAILED",
        "remote": remote,
        "remote_ref": f"refs/heads/{snapshot_branch}",
        "commit_sha": commit_sha,
        "snapshot_branch": snapshot_branch,
        "size_gate": gate,
        "rejected_candidates": rejected,
        "excluded_regenerable": excluded,
        "policy_auto_excluded": policy_auto_excluded,
        "headroom_bytes": int(max_total_bytes) - int(gate["total_bytes"]),
        "headroom_floor_bytes": plan["headroom_floor_bytes"],
        "headroom_below_floor": plan["headroom_below_floor"],
        "stdout_tail": push.stdout[-2000:],
        "stderr_tail": push.stderr[-2000:],
        "timing": timing,
    }


def _prune_old_bundles(bundle_dir: Path, keep: int = 3) -> None:
    for pattern in ("repo-*.bundle", "state-*.tar.gz"):
        paths = sorted(bundle_dir.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
        for path in paths[keep:]:
            path.unlink(missing_ok=True)


def create_bundle_fallback(bundle_dir: Path, report: dict[str, Any]) -> dict[str, Any]:
    target = bundle_dir.expanduser().resolve()
    if _is_inside_repo(target):
        return {
            **report,
            "status": "BUNDLE_DIR_INSIDE_REPO",
            "next_action": "choose_bundle_dir_outside_repo",
            "bundle_fallback": {"bundle_dir": str(target), "created": False},
        }
    if report["summary"]["tracked_secret_paths"] or report["summary"]["uncommitted_secret_paths"]:
        return {
            **report,
            "status": "SECRET_PATH_RISK",
            "next_action": "remove_secret_paths_before_bundle_fallback",
            "bundle_fallback": {"bundle_dir": str(target), "created": False},
        }

    target.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    repo_bundle = target / f"repo-{stamp}.bundle"
    state_archive = target / f"state-{stamp}.tar.gz"
    bundle = _run(["git", "bundle", "create", str(repo_bundle), "--all"])
    if bundle.returncode != 0:
        return {
            **report,
            "status": "BUNDLE_FALLBACK_FAILED",
            "next_action": "inspect_git_bundle_error",
            "bundle_fallback": {
                "bundle_dir": str(target),
                "created": False,
                "returncode": bundle.returncode,
                "stderr_tail": bundle.stderr[-2000:],
            },
        }

    state_paths = _bundle_state_paths()
    with tarfile.open(state_archive, "w:gz") as archive:
        for path in state_paths:
            archive.add(path, arcname=str(path.relative_to(ROOT)))
    _prune_old_bundles(target)
    bundle_report = {
        "bundle_dir": str(target),
        "created": True,
        "repo_bundle": str(repo_bundle),
        "state_archive": str(state_archive),
        "state_artifact_count": len(state_paths),
        "secret_paths_included": [],
        "same_disk_only": True,
        "not_off_machine_dr": True,
        "prune_keep": 3,
    }
    return {
        **report,
        "status": "BUNDLE_FALLBACK_ONLY",
        "next_action": "operator_supplies_private_remote_then_run_dr_preflight_push",
        "bundle_fallback": bundle_report,
    }


def build_report(
    *,
    remote: str,
    snapshot_branch: str = DEFAULT_SNAPSHOT_BRANCH,
    max_tracked_file_bytes: int = DEFAULT_MAX_TRACKED_FILE_BYTES,
    max_snapshot_bytes: int = DEFAULT_MAX_SNAPSHOT_BYTES,
) -> dict[str, Any]:
    timing = _new_timing()
    phase_started = time.perf_counter()
    remotes = _git_lines("remote")
    remote_urls = _git_lines("remote", "-v")
    tracked_files = _git_lines("ls-files")
    status_rows = _git_lines("status", "--short")
    _finish_timing_phase(
        timing,
        "git_remote_status_inventory",
        phase_started,
        remote_count=len(remotes),
        tracked_files=len(tracked_files),
        dirty_paths=len(status_rows),
    )
    tracked_secret_paths = [path for path in tracked_files if _contains_secret_path(path)]
    uncommitted_secret_paths = [
        row[3:] if len(row) > 3 else row
        for row in status_rows
        if _contains_secret_path(row[3:] if len(row) > 3 else row)
    ]
    has_remote = remote in remotes
    if not has_remote and remote == "origin" and remotes:
        has_remote = True
    phase_started = time.perf_counter()
    tracked_rows = _tracked_file_sizes()
    tracked_size_gate = _size_gate(
        tracked_rows,
        max_file_bytes=int(max_tracked_file_bytes),
        max_total_bytes=int(max_snapshot_bytes),
    )
    _finish_timing_phase(
        timing,
        "tracked_size_gate",
        phase_started,
        tracked_file_count=tracked_size_gate["file_count"],
        tracked_total_bytes=tracked_size_gate["total_bytes"],
    )
    phase_started = time.perf_counter()
    snapshot_size_gate = _snapshot_size_report(
        max_file_bytes=int(max_tracked_file_bytes),
        max_total_bytes=int(max_snapshot_bytes),
    )
    _finish_timing_phase(
        timing,
        "snapshot_size_gate",
        phase_started,
        snapshot_file_count=snapshot_size_gate["file_count"],
        snapshot_total_bytes=snapshot_size_gate["total_bytes"],
        rejected=snapshot_size_gate["rejected_candidate_count"],
    )
    can_push_snapshot_now = bool(
        has_remote
        and not tracked_secret_paths
        and not uncommitted_secret_paths
        and snapshot_size_gate["pass"]
        and not snapshot_size_gate["rejected_candidates"]
    )
    summary = {
        "remote_requested": remote,
        "remote_count": len(remotes),
        "has_push_remote": has_remote,
        "remote_urls": remote_urls,
        "tracked_secret_paths": tracked_secret_paths,
        "uncommitted_secret_paths": uncommitted_secret_paths,
        "dirty_paths": len(status_rows),
        "can_push_now": can_push_snapshot_now,
        "can_push_snapshot_now": can_push_snapshot_now,
        "snapshot_branch": snapshot_branch,
        "tracked_file_count": tracked_size_gate["file_count"],
        "tracked_total_bytes": tracked_size_gate["total_bytes"],
        "tracked_oversized_files": tracked_size_gate["oversized_files"],
        "snapshot_file_count": snapshot_size_gate["file_count"],
        "snapshot_total_bytes": snapshot_size_gate["total_bytes"],
        "snapshot_oversized_files": snapshot_size_gate["oversized_files"],
        "snapshot_rejected_candidates": snapshot_size_gate["rejected_candidates"],
        "snapshot_excluded_regenerable": snapshot_size_gate["excluded_regenerable"],
        "snapshot_excluded_regenerable_count": snapshot_size_gate["excluded_regenerable_count"],
        "snapshot_excluded_regenerable_bytes": snapshot_size_gate["excluded_regenerable_bytes"],
        "policy_auto_excluded": snapshot_size_gate["policy_auto_excluded"],
        "snapshot_headroom_bytes": snapshot_size_gate["headroom_bytes"],
        "snapshot_headroom_floor_bytes": snapshot_size_gate["headroom_floor_bytes"],
        "headroom_below_floor": snapshot_size_gate["headroom_below_floor"],
        "auto_excludable_class_exhausted": snapshot_size_gate[
            "auto_excludable_class_exhausted"
        ],
        "max_tracked_file_bytes": int(max_tracked_file_bytes),
        "max_snapshot_bytes": int(max_snapshot_bytes),
    }
    if summary["can_push_snapshot_now"]:
        status = "READY_TO_PUSH_SNAPSHOT"
        next_action = f"scripts/dr_preflight.py --remote {remote} --push-snapshot --snapshot-branch {snapshot_branch}"
    elif not has_remote:
        status = "OFF_MACHINE_REMOTE_MISSING"
        next_action = "add_private_remote_then_run_dr_push"
    elif tracked_secret_paths or uncommitted_secret_paths:
        status = "SECRET_PATH_RISK"
        next_action = "remove_tracked_secret_paths_before_dr_push"
    else:
        status = "SNAPSHOT_SIZE_RISK"
        next_action = "slim tracked/snapshot files below DR size gates before push"
    report = {
        "schema_version": 1,
        "kind": "wallet_copy_dr_preflight",
        "generated_at": _utc_now_iso(),
        "status": status,
        "summary": summary,
        "next_action": next_action,
        "structural_invariant": ".env files stay out of git; DR pushes only git-tracked repo, HANDOFF, and key state artifacts.",
        "timing": timing,
    }
    timing["total_s_before_return"] = round(
        sum(float(row.get("duration_s") or 0.0) for row in timing.get("phases", []) if isinstance(row, dict)),
        6,
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", default="origin", help="Preferred private off-machine git remote name.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--push", action="store_true", help="Push HEAD when the preflight is clean.")
    parser.add_argument("--push-snapshot", action="store_true", help="Push a slim orphan DR snapshot branch.")
    parser.add_argument("--snapshot-branch", default=DEFAULT_SNAPSHOT_BRANCH)
    parser.add_argument("--max-tracked-file-bytes", type=int, default=DEFAULT_MAX_TRACKED_FILE_BYTES)
    parser.add_argument("--max-snapshot-bytes", type=int, default=DEFAULT_MAX_SNAPSHOT_BYTES)
    parser.add_argument(
        "--bundle",
        nargs="?",
        const=str(DEFAULT_BUNDLE_DIR),
        help="Create same-disk fallback bundle in DIR, defaulting to ~/polymarket-agent-dr.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(
        remote=args.remote,
        snapshot_branch=args.snapshot_branch,
        max_tracked_file_bytes=args.max_tracked_file_bytes,
        max_snapshot_bytes=args.max_snapshot_bytes,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.push_snapshot and report["summary"]["can_push_snapshot_now"]:
        snapshot_push = push_snapshot(
            remote=args.remote,
            snapshot_branch=args.snapshot_branch,
            max_file_bytes=args.max_tracked_file_bytes,
            max_total_bytes=args.max_snapshot_bytes,
        )
        report["snapshot_push"] = snapshot_push
        report["status"] = "SNAPSHOT_PUSHED" if snapshot_push["returncode"] == 0 else snapshot_push["status"]
        report["next_action"] = (
            "wire_hourly_brainless_dr_snapshot_push"
            if snapshot_push["returncode"] == 0
            else "inspect_snapshot_push_error"
        )
        output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(snapshot_push, indent=2, sort_keys=True))
        return 0 if snapshot_push["returncode"] == 0 else 1
    if args.push and report["summary"]["can_push_now"]:
        push = _run(["git", "push", "--follow-tags", args.remote, "HEAD"])
        report["push"] = {
            "returncode": push.returncode,
            "stdout_tail": push.stdout[-2000:],
            "stderr_tail": push.stderr[-2000:],
        }
        report["status"] = "PUSHED" if push.returncode == 0 else "PUSH_FAILED"
        output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(report["push"], indent=2, sort_keys=True))
        return 0 if push.returncode == 0 else 1
    if args.push_snapshot:
        output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps({"output": str(output), "status": report["status"], "next_action": report["next_action"]}, indent=2, sort_keys=True))
        return 2
    if args.bundle is not None:
        report = create_bundle_fallback(Path(args.bundle), report)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(output), "status": report["status"], "next_action": report["next_action"]}, indent=2, sort_keys=True))
    return 0 if not args.push or report["summary"]["can_push_now"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
