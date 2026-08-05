#!/usr/bin/env python3
"""Record brain CLI versions and smoke-test status.

This is a deterministic environment watcher. It does not call any brain for
advice; it only records version strings and, when a version changes, marks the
next ask_fable invocation as an explicit smoke test.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_STATE = Path("data/research/cli_versions_state.json")
DEFAULT_HANDOFF = Path("docs/agents/HANDOFF.md")
TOOLS = ("claude", "codex", "grok", "agy")
LATEST_CHECK_INTERVAL_S = 7 * 24 * 60 * 60
DEFAULT_LATEST_NPM_PACKAGES = {
    "claude": "@anthropic-ai/claude-code",
    "codex": "@openai/codex",
}


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _iso_from_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text())
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _version(tool: str, previous: dict[str, Any] | None = None) -> dict[str, Any]:
    path_source = "PATH"
    path = None
    if previous:
        prior_path = previous.get("path")
        if isinstance(prior_path, str) and prior_path and Path(prior_path).exists():
            path = prior_path
            path_source = "prior_absolute_path"
    if not path:
        path = shutil.which(tool)
    if not path:
        return {"available": False, "path": None, "path_source": None, "version": None, "rc": None}
    proc = subprocess.run(
        [path, "--version"],
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    output = (proc.stdout or proc.stderr or "").strip().splitlines()
    version = output[0].strip() if output else ""
    return {
        "available": True,
        "path": path,
        "path_source": path_source,
        "version": version,
        "rc": proc.returncode,
    }


def _semver_parts(value: Any) -> tuple[int, ...] | None:
    match = re.search(r"(\d+(?:\.\d+){1,3})", str(value or ""))
    if not match:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def _stale_flag(installed: Any, latest: Any) -> bool | None:
    installed_parts = _semver_parts(installed)
    latest_parts = _semver_parts(latest)
    if installed_parts is None or latest_parts is None:
        return None
    width = max(len(installed_parts), len(latest_parts))
    installed_key = installed_parts + (0,) * (width - len(installed_parts))
    latest_key = latest_parts + (0,) * (width - len(latest_parts))
    return installed_key < latest_key


def _same_version_identity(previous: Any, current: Any) -> bool:
    previous_parts = _semver_parts(previous)
    current_parts = _semver_parts(current)
    if previous_parts is not None and current_parts is not None:
        width = max(len(previous_parts), len(current_parts))
        return previous_parts + (0,) * (width - len(previous_parts)) == current_parts + (0,) * (
            width - len(current_parts)
        )
    return str(previous or "") == str(current or "")


def _latest_package(tool: str) -> str:
    env_key = f"CLI_VERSION_LATEST_PACKAGE_{tool.upper()}"
    return os.environ.get(env_key, DEFAULT_LATEST_NPM_PACKAGES.get(tool, "")).strip()


def _latest_grok_version(path: str | None, now_iso: str, next_check_due_at: str) -> dict[str, Any]:
    if not path:
        return {
            "latest_version": None,
            "source": "grok_update_check",
            "package": None,
            "checked_at": now_iso,
            "next_check_due_at": next_check_due_at,
            "status": "LATEST_CHECK_FAILED",
            "error": "grok binary unavailable",
        }
    proc = subprocess.run(
        [path, "update", "--check", "--json"],
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    try:
        payload = json.loads((proc.stdout or "").strip())
    except json.JSONDecodeError:
        payload = {}
    latest = payload.get("latestVersion") if isinstance(payload, dict) else None
    if proc.returncode != 0 or not latest or (isinstance(payload, dict) and payload.get("error")):
        return {
            "latest_version": None,
            "source": "grok_update_check",
            "package": None,
            "checked_at": now_iso,
            "next_check_due_at": next_check_due_at,
            "status": "LATEST_CHECK_FAILED",
            "rc": proc.returncode,
            "stderr": (proc.stderr or "").strip()[:500],
            "stdout": (proc.stdout or "").strip()[:500],
            "error": payload.get("error") if isinstance(payload, dict) else None,
        }
    return {
        "latest_version": str(latest),
        "source": "grok_update_check",
        "package": None,
        "checked_at": now_iso,
        "next_check_due_at": next_check_due_at,
        "status": "CHECKED",
        "rc": proc.returncode,
        "current_version": payload.get("currentVersion"),
        "update_available": payload.get("updateAvailable"),
        "installer": payload.get("installer"),
        "channel": payload.get("channel"),
    }


def _latest_agy_version(current: dict[str, Any], now_iso: str, next_check_due_at: str) -> dict[str, Any]:
    return {
        "latest_version": current.get("version"),
        "source": "manual_env_sweep",
        "package": None,
        "checked_at": now_iso,
        "next_check_due_at": next_check_due_at,
        "status": "MANUAL_ENV_SWEEP",
        "manual_policy": "R4 env-sweeper verifies AGY release freshness weekly; env CLI_VERSION_LATEST_AGY overrides this when known",
    }


def _latest_version(
    tool: str,
    current: dict[str, Any],
    previous: dict[str, Any] | None,
    *,
    force: bool,
    skip_network: bool,
) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    now_iso = now.isoformat().replace("+00:00", "Z")
    env_latest = os.environ.get(f"CLI_VERSION_LATEST_{tool.upper()}")
    package = _latest_package(tool)
    if env_latest:
        return {
            "latest_version": env_latest.strip(),
            "source": "env",
            "package": package or None,
            "checked_at": now_iso,
            "next_check_due_at": _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
            "status": "CHECKED",
        }

    previous_latest = previous.get("latest") if isinstance(previous, dict) and isinstance(previous.get("latest"), dict) else {}
    checked_at = _parse_iso(previous_latest.get("checked_at"))
    if (
        previous_latest
        and checked_at
        and not force
        and (now - checked_at).total_seconds() < LATEST_CHECK_INTERVAL_S
    ):
        cached = dict(previous_latest)
        cached["cache_status"] = "REUSED_UNTIL_WEEKLY_DUE"
        return cached

    if skip_network:
        if previous_latest:
            cached = dict(previous_latest)
            cached["cache_status"] = "NETWORK_SKIPPED_REUSING_PRIOR"
            return cached
        return {
            "latest_version": None,
            "source": "network_skipped",
            "package": package or None,
            "checked_at": now_iso,
            "next_check_due_at": _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
            "status": "LATEST_UNKNOWN",
            "error": "CLI_VERSION_SKIP_NETWORK=1 and no cached latest version",
        }

    if tool == "grok" and not package:
        return _latest_grok_version(
            current.get("path") if isinstance(current.get("path"), str) else None,
            now_iso,
            _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
        )

    if tool == "agy" and not package:
        return _latest_agy_version(
            current,
            now_iso,
            _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
        )

    if not package:
        return {
            "latest_version": None,
            "source": "unconfigured",
            "package": None,
            "checked_at": now_iso,
            "next_check_due_at": _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
            "status": "LATEST_SOURCE_UNCONFIGURED",
        }

    npm_path = shutil.which("npm")
    if not npm_path:
        # launchd runs with a minimal PATH; a missing npm must degrade to a
        # named status, never crash the whole cli_versions step.
        return {
            "latest_version": None,
            "source": "npm",
            "package": package,
            "checked_at": now_iso,
            "next_check_due_at": _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
            "status": "LATEST_TOOL_UNAVAILABLE",
            "error": "npm not on PATH",
        }
    proc = subprocess.run(
        [npm_path, "view", package, "version"],
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    output = (proc.stdout or "").strip().splitlines()
    latest = output[-1].strip() if output else None
    if proc.returncode != 0 or not latest:
        return {
            "latest_version": None,
            "source": "npm",
            "package": package,
            "checked_at": now_iso,
            "next_check_due_at": _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
            "status": "LATEST_CHECK_FAILED",
            "rc": proc.returncode,
            "stderr": (proc.stderr or "").strip()[:500],
        }
    return {
        "latest_version": latest,
        "source": "npm",
        "package": package,
        "checked_at": now_iso,
        "next_check_due_at": _iso_from_ts(now.timestamp() + LATEST_CHECK_INTERVAL_S),
        "status": "CHECKED",
        "rc": proc.returncode,
    }


def _freshness(tool: str, current: dict[str, Any], previous: dict[str, Any] | None, args: argparse.Namespace) -> dict[str, Any]:
    latest = _latest_version(
        tool,
        current,
        previous,
        force=bool(args.force_latest),
        skip_network=bool(args.skip_latest_network),
    )
    stale = _stale_flag(current.get("version"), latest.get("latest_version"))
    if stale is True:
        status = "STALE"
        next_action = "update this CLI in a quiet window, one brain at a time, then run the ask_fable smoke test"
    elif stale is False:
        status = "CURRENT"
        next_action = "no update needed before next weekly freshness check"
    else:
        status = latest.get("status") or "LATEST_UNKNOWN"
        next_action = "configure a latest-version source or retry at next weekly freshness check"
    return {
        "installed_version": current.get("version"),
        "latest": latest,
        "stale": stale,
        "status": status,
        "update_policy": "one_brain_at_a_time_quiet_moment_smoke_test_after",
        "next_action": next_action,
    }


def _append_handoff(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write("\n")
        fh.write("\n".join(lines))
        fh.write("\n")


def _record_versions(args: argparse.Namespace) -> dict[str, Any]:
    state_path = Path(args.state)
    handoff_path = Path(args.handoff)
    prior = _load(state_path)
    prior_versions = prior.get("versions") if isinstance(prior.get("versions"), dict) else {}
    versions = {
        tool: _version(
            tool,
            prior_versions.get(tool) if isinstance(prior_versions.get(tool), dict) else None,
        )
        for tool in TOOLS
    }
    freshness = {
        tool: _freshness(
            tool,
            versions[tool],
            prior_versions.get(tool) if isinstance(prior_versions.get(tool), dict) else None,
            args,
        )
        for tool in TOOLS
    }
    changed: list[str] = []
    for tool, current in versions.items():
        previous = prior_versions.get(tool) if isinstance(prior_versions.get(tool), dict) else {}
        if previous and (
            not _same_version_identity(previous.get("version"), current.get("version"))
            or previous.get("path") != current.get("path")
            or previous.get("available") != current.get("available")
        ):
            changed.append(tool)

    pending = prior.get("pending_smoke_test") if isinstance(prior.get("pending_smoke_test"), dict) else None
    if changed:
        pending = {
            "created_at": _now(),
            "status": "PENDING_NEXT_ASK_FABLE",
            "changed_tools": changed,
            "versions": {tool: versions[tool] for tool in changed},
            "next_action": "verify next ask_fable rc and output sanity, then record smoke verdict",
        }
        _append_handoff(
            handoff_path,
            [
                f"## {pending['created_at']} codex STATUS [SELF-DEV]",
                "- cli_version_change: "
                + ", ".join(
                    f"{tool}={versions[tool].get('version') or 'UNAVAILABLE'}"
                    for tool in changed
                ),
                "- next: next ask_fable invocation is the UPDATE SMOKE TEST; record rc + output sanity in cli_versions_state.json.",
            ],
        )

    stale_tools = [
        tool
        for tool, row in freshness.items()
        if isinstance(row, dict) and row.get("stale") is True
    ]
    unknown_latest_tools = [
        tool
        for tool, row in freshness.items()
        if isinstance(row, dict) and row.get("stale") is None
    ]
    payload = {
        "schema_version": 1,
        "kind": "cli_versions_state",
        "updated_at": _now(),
        "versions": versions,
        "freshness": freshness,
        "stale_tools": stale_tools,
        "unknown_latest_tools": unknown_latest_tools,
        "changed_tools": changed,
        "pending_smoke_test": pending,
        "status": (
            "VERSION_CHANGE_PENDING_SMOKE"
            if pending
            else "STALE_UPDATE_DUE"
            if stale_tools
            else "OK"
        ),
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def _record_smoke(args: argparse.Namespace) -> dict[str, Any]:
    state_path = Path(args.state)
    handoff_path = Path(args.handoff)
    payload = _load(state_path)
    pending = payload.get("pending_smoke_test") if isinstance(payload.get("pending_smoke_test"), dict) else None
    if not pending:
        payload["updated_at"] = _now()
        payload["status"] = payload.get("status") or "OK"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return payload

    success = args.rc == 0 and args.output_sanity == "PASS"
    smoke = {
        "tested_at": _now(),
        "status": "PASS" if success else "FAIL",
        "rc": args.rc,
        "output_sanity": args.output_sanity,
        "provider": args.provider,
        "changed_tools": pending.get("changed_tools") or [],
    }
    payload["last_smoke_test"] = smoke
    payload["pending_smoke_test"] = None if success else pending
    stale_tools = payload.get("stale_tools") if isinstance(payload.get("stale_tools"), list) else []
    payload["status"] = "STALE_UPDATE_DUE" if success and stale_tools else "OK" if success else "SMOKE_TEST_FAILED"
    payload["updated_at"] = _now()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _append_handoff(
        handoff_path,
        [
            f"## {smoke['tested_at']} codex STATUS [SELF-DEV]",
            f"- update_smoke_test: {smoke['status']}; provider={args.provider}; rc={args.rc}; output_sanity={args.output_sanity}.",
            "- next: continue normal brain chain." if success else "- next: failed smoke escalates through ask_fable fallback chain; inspect provider log before next update.",
        ],
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--handoff", default=str(DEFAULT_HANDOFF))
    parser.add_argument("--force-latest", action="store_true")
    parser.add_argument("--skip-latest-network", action="store_true", default=os.environ.get("CLI_VERSION_SKIP_NETWORK") == "1")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("record")
    smoke = sub.add_parser("smoke")
    smoke.add_argument("--provider", required=True)
    smoke.add_argument("--rc", type=int, required=True)
    smoke.add_argument("--output-sanity", choices=("PASS", "FAIL"), required=True)
    args = parser.parse_args()

    if args.command == "smoke":
        payload = _record_smoke(args)
    else:
        payload = _record_versions(args)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload.get("status") != "SMOKE_TEST_FAILED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
