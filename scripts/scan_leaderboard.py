#!/usr/bin/env python3
"""Hourly DISCOVER-stage leaderboard scanner.

This wrapper keeps the flow-level entrypoint small: refresh the broad
WEEK/MONTH leaderboard universe, preserve registry-known wallets, and persist a
scanner heartbeat that launchd/supervision can inspect. It is discovery only.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json  # noqa: E402


DEFAULT_DISCOVERY_CATEGORIES = "CRYPTO,SPORTS,POLITICS,POP_CULTURE,BUSINESS,ECONOMICS"
POLYMARKET_BASE_URL_ENV_VARS = (
    "POLYMARKET_DATA_API_BASE_URL",
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_CLOB_API_BASE_URL",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="data/research/wallet_copy_leaderboard_scan_state.json")
    parser.add_argument("--event-log", default="data/research/wallet_copy_leaderboard_scan_events.jsonl")
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument(
        "--leaderboard-progress-state",
        default="data/research/wallet_copy_leaderboard_scan_progress.json",
    )
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--weekly-limit", type=int, default=50)
    parser.add_argument("--monthly-limit", type=int, default=50)
    parser.add_argument(
        "--categories",
        default=os.getenv("WALLET_COPY_LEADERBOARD_CATEGORIES", DEFAULT_DISCOVERY_CATEGORIES),
        help="Comma-separated Polymarket leaderboard categories for broad discovery.",
    )
    parser.add_argument("--leaderboard-pages", type=int, default=0)
    parser.add_argument("--leaderboard-max-pages", type=int, default=20)
    parser.add_argument("--leaderboard-page-timeout-s", type=float, default=8.0)
    parser.add_argument("--leaderboard-max-wall-runtime-s", type=float, default=120.0)
    parser.add_argument("--leaderboard-page-retries", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=900.0)
    parser.add_argument("--run-pipeline", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--pipeline-max-wallets", type=int, default=4)
    parser.add_argument("--egress", default=os.getenv("WALLET_COPY_EGRESS", "unspecified"))
    parser.add_argument("--egress-check-url", default=os.getenv("WALLET_COPY_EGRESS_CHECK_URL", "https://api.ipify.org?format=json"))
    parser.add_argument("--egress-timeout-s", type=float, default=8.0)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--retry-backoff-s", type=float, default=10.0)
    parser.add_argument(
        "--direct-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Retry failed/source-blocked discovery with direct Polymarket base URLs.",
    )
    return parser.parse_args()


def _egress_preflight(args: argparse.Namespace) -> dict[str, Any]:
    """Record the observed discovery egress without making it a live gate."""

    started = time.time()
    url = str(args.egress_check_url or "").strip()
    if not url:
        return {"status": "SKIPPED", "reason": "egress_check_url_empty"}
    try:
        request = Request(url, headers={"User-Agent": "wallet-copy-discover-egress/1.0"})
        with urlopen(request, timeout=max(1.0, float(args.egress_timeout_s))) as response:
            body = response.read(500).decode("utf-8", "replace")
        return {
            "status": "PASS",
            "duration_s": round(time.time() - started, 6),
            "url": url,
            "response": body,
        }
    except Exception as exc:  # noqa: BLE001 - egress check is telemetry, not a live gate.
        return {
            "status": "WATCH",
            "duration_s": round(time.time() - started, 6),
            "url": url,
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
        }


def _direct_base_env() -> dict[str, str]:
    env = dict(os.environ)
    for key in POLYMARKET_BASE_URL_ENV_VARS:
        env[key] = ""
    return env


def _base_url_env_summary(env: dict[str, str] | None = None) -> dict[str, bool]:
    source = os.environ if env is None else env
    return {key: bool(str(source.get(key) or "").strip()) for key in POLYMARKET_BASE_URL_ENV_VARS}


def _run_onboarding_once(
    args: argparse.Namespace,
    *,
    route_mode: str = "configured",
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    argv = [
        sys.executable,
        "scripts/onboard_leaderboard_crypto_wallets.py",
        "--registry",
        args.registry,
        "--output",
        args.leaderboard_state,
        "--categories",
        str(args.categories),
        "--weekly-limit",
        str(int(args.weekly_limit)),
        "--monthly-limit",
        str(int(args.monthly_limit)),
        "--leaderboard-pages",
        str(int(args.leaderboard_pages)),
        "--leaderboard-max-pages",
        str(int(args.leaderboard_max_pages)),
        "--leaderboard-page-timeout-s",
        str(float(args.leaderboard_page_timeout_s)),
        "--leaderboard-max-wall-runtime-s",
        str(float(args.leaderboard_max_wall_runtime_s)),
        "--leaderboard-page-retries",
        str(int(args.leaderboard_page_retries)),
        "--leaderboard-progress-state",
        args.leaderboard_progress_state,
    ]
    if bool(args.run_pipeline):
        argv.extend(
            [
                "--run-pipeline",
                "--pipeline-max-wallets",
                str(int(args.pipeline_max_wallets)),
            ]
        )
    started = time.time()
    try:
        completed = subprocess.run(
            argv,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=max(1.0, float(args.timeout_s)),
            check=False,
        )
        return {
            "argv": argv,
            "route_mode": route_mode,
            "base_url_env_configured": _base_url_env_summary(env),
            "returncode": completed.returncode,
            "duration_s": round(time.time() - started, 6),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "leaderboard_progress": _summary_from_progress(args.leaderboard_progress_state),
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "argv": argv,
            "route_mode": route_mode,
            "base_url_env_configured": _base_url_env_summary(env),
            "returncode": 124,
            "duration_s": round(time.time() - started, 6),
            "stdout_tail": str(exc.stdout or "")[-4000:],
            "stderr_tail": str(exc.stderr or "")[-4000:],
            "timeout_s": float(args.timeout_s),
            "leaderboard_progress": _summary_from_progress(args.leaderboard_progress_state),
        }


def _run_onboarding(
    args: argparse.Namespace,
    *,
    route_mode: str = "configured",
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    max_attempts = max(1, int(args.attempts))
    for attempt_index in range(max_attempts):
        result = _run_onboarding_once(args, route_mode=route_mode, env=env)
        result["attempt_index"] = attempt_index
        attempts.append(result)
        if int(result.get("returncode") or 0) == 0:
            return {**result, "attempts": attempts, "attempts_requested": max_attempts}
        if attempt_index + 1 < max_attempts:
            time.sleep(max(0.0, float(args.retry_backoff_s)) * (2**attempt_index))
    last = attempts[-1] if attempts else {}
    return {**last, "attempts": attempts, "attempts_requested": max_attempts}


def _command_text(command: dict[str, Any]) -> str:
    parts = [str(command.get("stdout_tail") or ""), str(command.get("stderr_tail") or "")]
    for attempt in command.get("attempts") or []:
        if isinstance(attempt, dict):
            parts.append(str(attempt.get("stdout_tail") or ""))
            parts.append(str(attempt.get("stderr_tail") or ""))
    return "\n".join(parts).lower()


def _should_try_direct_fallback(command: dict[str, Any], leaderboard: dict[str, Any]) -> bool:
    if bool(leaderboard.get("source_route_blocked")):
        return True
    if int(command.get("returncode") or 0) == 124:
        return True
    text = _command_text(command)
    route_needles = (
        "503",
        "source_route_blocked",
        "polymarketrouteerror",
        "route variants failed",
        "timeout",
        "timed out",
    )
    return any(needle in text for needle in route_needles)


def _summary_from_leaderboard(path: str) -> dict[str, Any]:
    state = load_json(path, default={})
    if not isinstance(state, dict):
        return {}
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    blocked = state.get("source_route_blocked") if isinstance(state.get("source_route_blocked"), dict) else {}
    return {
        "leaderboard_status": state.get("status"),
        "updated_at": state.get("updated_at"),
        "unique_wallets": summary.get("unique_wallets"),
        "current_unique_wallets": summary.get("current_unique_wallets"),
        "preserved_historical_wallets": summary.get("preserved_historical_wallets"),
        "registry_preserved_historical_wallets": summary.get("registry_preserved_historical_wallets"),
        "copy_all_fetched_wallets_to_registry": summary.get("copy_all_fetched_wallets_to_registry"),
        "pipeline_requested": bool(state.get("pipeline_requested")),
        "pipeline_max_wallets": state.get("pipeline_max_wallets"),
        "pipeline_roster_path": state.get("pipeline_roster_path"),
        "pipeline_roster_wallets": state.get("pipeline_roster_wallets"),
        "pipeline_roster_status": state.get("pipeline_roster_status"),
        "paper_only": state.get("paper_only", True),
        "live_orders_allowed": state.get("live_orders_allowed", False),
        "source_route_blocked": blocked or None,
        "leaderboard_scan_progress": state.get("leaderboard_scan_progress"),
    }


def _summary_from_progress(path: str) -> dict[str, Any]:
    state = load_json(path, default={})
    if not isinstance(state, dict):
        return {}
    return {
        "status": state.get("status"),
        "updated_at": state.get("updated_at"),
        "completed_at": state.get("completed_at"),
        "page_mode": state.get("page_mode"),
        "page_timeout_s": state.get("page_timeout_s"),
        "max_wall_runtime_s": state.get("max_wall_runtime_s"),
        "page_retries": state.get("page_retries"),
        "budget_exhausted": state.get("budget_exhausted"),
        "pages_started": state.get("pages_started"),
        "pages_completed": state.get("pages_completed"),
        "rows_fetched": state.get("rows_fetched"),
        "current_page": state.get("current_page"),
        "last_successful_page": state.get("last_successful_page"),
        "last_error": state.get("last_error"),
        "progress_state": str(path),
    }


def main() -> int:
    args = parse_args()
    egress_preflight = _egress_preflight(args)
    primary_command = _run_onboarding(args, route_mode="configured")
    primary_leaderboard = _summary_from_leaderboard(args.leaderboard_state)
    direct_fallback_command: dict[str, Any] | None = None
    direct_fallback_leaderboard: dict[str, Any] | None = None
    command = primary_command
    leaderboard = primary_leaderboard
    if bool(args.direct_fallback) and _should_try_direct_fallback(primary_command, primary_leaderboard):
        direct_fallback_command = _run_onboarding(
            args,
            route_mode="direct_base_fallback",
            env=_direct_base_env(),
        )
        direct_fallback_leaderboard = _summary_from_leaderboard(args.leaderboard_state)
        fallback_returncode = int(direct_fallback_command.get("returncode") or 0)
        fallback_source_blocked = bool(direct_fallback_leaderboard.get("source_route_blocked"))
        if fallback_returncode == 0 and not fallback_source_blocked:
            command = direct_fallback_command
            leaderboard = direct_fallback_leaderboard
    returncode = int(command.get("returncode") or 0)
    source_blocked = bool(leaderboard.get("source_route_blocked"))
    status = "PASS" if returncode == 0 else "WATCH" if source_blocked else "CORRECTION"
    state = {
        "schema_version": 1,
        "kind": "wallet_copy_leaderboard_scan_state",
        "flow_stage": "DISCOVER",
        "status": status,
        "updated_at": utc_now_iso(),
        "egress": str(args.egress or "unspecified"),
        "egress_preflight": egress_preflight,
        "paper_only": True,
        "live_orders_allowed": False,
        "scan_scope": {
            "categories": [value.strip().upper() for value in str(args.categories or "").split(",") if value.strip()],
            "decision": "widen discovery beyond BTC-5m so slower copyable wallets can compete with the live lane",
            "flow_stage": "DISCOVER",
        },
        "supervision": {
            "schedule": "hourly_launchd",
            "decision": "refresh leaderboard discovery and preserve registry-known wallets",
            "threshold": "PASS or WATCH source-route-blocked state with preserved wallets; CORRECTION only on structural wrapper failure",
            "onboarding_attempts": int(command.get("attempts_requested") or max(1, int(args.attempts))),
            "per_page_source_retry": "PolymarketHttpClient retries each leaderboard page/offset internally; wrapper retries the whole discovery job for transient relay failures such as offset 100 HTTP 503, then falls back to direct-base discovery.",
            "page_timeout_s": float(args.leaderboard_page_timeout_s),
            "max_wall_runtime_s": float(args.leaderboard_max_wall_runtime_s),
            "page_retries": max(1, int(args.leaderboard_page_retries)),
            "page_progress_state": str(args.leaderboard_progress_state),
            "run_pipeline": bool(args.run_pipeline),
            "pipeline_max_wallets": int(args.pipeline_max_wallets),
        },
        "command": command,
        "route_fallback": {
            "enabled": bool(args.direct_fallback),
            "attempted": direct_fallback_command is not None,
            "selected_route_mode": str(command.get("route_mode") or "configured"),
            "primary": primary_command,
            "direct_fallback": direct_fallback_command,
            "direct_fallback_leaderboard": direct_fallback_leaderboard,
        },
        "leaderboard": leaderboard,
        "leaderboard_progress": _summary_from_progress(args.leaderboard_progress_state),
        "next_action": (
            "route degraded; preserve current registry and retry discovery on next scheduled scan"
            if source_blocked
            else "leaderboard discovery refreshed; downstream observe/promote lanes may consume registry"
            if returncode == 0
            else "repair leaderboard discovery command before claiming DISCOVER coverage"
        ),
    }
    atomic_write_json(args.state, state)
    append_jsonl(args.event_log, {"event": "wallet_copy_leaderboard_scan", **state})
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if status in {"PASS", "WATCH"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
