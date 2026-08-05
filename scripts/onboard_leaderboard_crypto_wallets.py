#!/usr/bin/env python3
"""Onboard Polymarket crypto leaderboard wallets into wallet-copy research.

This is discovery and paper/research only. It never enables live execution.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.leaderboard import (  # noqa: E402
    LeaderboardCategoryEmptyPayloadError,
    build_leaderboard_candidates,
    build_leaderboard_state,
    fetch_crypto_top_wallets,
    merge_leaderboard_wallets,
)
from src.wallet_copy.http_client import PolymarketRouteError  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.registry import load_wallet_registry  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


ACCEPTED_NON_GREEN_EVIDENCE_RETURNCODES = {
    "paper_live_tracker": {
        2: "paper_live_tracker_copy_efficiency_or_copyability_watch_evidence",
    },
    "live_execution_arm": {
        2: "live_execution_not_admissible_yet",
    },
}
DEFAULT_PIPELINE_ROSTER = "data/research/leaderboard_pipeline_roster.json"
LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID = "OP-LIVE-20260703-BELA"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--output", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--category", default="CRYPTO")
    parser.add_argument(
        "--categories",
        default="",
        help="Comma-separated leaderboard categories. Empty preserves --category for legacy runs.",
    )
    parser.add_argument("--order-by", default="PNL")
    parser.add_argument("--weekly-limit", type=int, default=50)
    parser.add_argument("--monthly-limit", type=int, default=50)
    parser.add_argument(
        "--leaderboard-pages",
        type=int,
        default=0,
        help="Pages per WEEK/MONTH period; 0 means fetch until the API is exhausted or --leaderboard-max-pages is hit",
    )
    parser.add_argument(
        "--leaderboard-max-pages",
        type=int,
        default=20,
        help="Hard cap for --leaderboard-pages 0 full-pagination discovery",
    )
    parser.add_argument(
        "--leaderboard-page-timeout-s",
        type=float,
        default=8.0,
        help="Per leaderboard page request timeout; page progress is checkpointed before each fetch",
    )
    parser.add_argument(
        "--leaderboard-max-wall-runtime-s",
        type=float,
        default=120.0,
        help="Wall-clock budget for leaderboard pagination before returning the checkpointed partial universe",
    )
    parser.add_argument(
        "--leaderboard-page-retries",
        type=int,
        default=1,
        help="Per leaderboard page retry count inside the measured Polymarket HTTP client",
    )
    parser.add_argument(
        "--leaderboard-progress-state",
        default="data/research/wallet_copy_leaderboard_scan_progress.json",
        help="Durable page-level progress checkpoint for full leaderboard pagination",
    )
    parser.add_argument("--max-wallets", type=int, default=0, help="Optional bounded smoke subset after dedupe")
    parser.add_argument("--run-pipeline", action="store_true")
    parser.add_argument(
        "--pipeline-max-wallets",
        type=int,
        default=0,
        help="Optional bounded roster for --run-pipeline; distinct from --max-wallets registration breadth.",
    )
    parser.add_argument(
        "--pipeline-roster",
        default=DEFAULT_PIPELINE_ROSTER,
        help="Wallets-config artifact used when --run-pipeline --pipeline-max-wallets is bounded.",
    )
    parser.add_argument(
        "--pipeline-artifact-dir",
        default="data/research/leaderboard_pipeline",
        help="Isolated output namespace for bounded heartbeat pipeline artifacts.",
    )
    parser.add_argument("--skip-live-tracker", action="store_true")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--research-state", default="data/research/wallet_copy_research_state.json")
    parser.add_argument("--inventory-paper-state", default="data/research/wallet_copy_inventory_paper_state.json")
    parser.add_argument("--ml-dataset", default="data/research/wallet_copy_ml_dataset.jsonl")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--live-tracker-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--market-ws-jsonl", default="data/research/clob_market_ws_events.jsonl")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pages", type=int, default=1, help="Use 0 for full API pagination")
    parser.add_argument("--wallet-fraction", type=float, default=0.05)
    parser.add_argument("--max-order-usd", type=float, default=2.0)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.5)
    parser.add_argument("--live-tracker-limit", type=int, default=50)
    parser.add_argument("--live-tracker-pages", type=int, default=1)
    parser.add_argument("--data-api-timeout-s", type=float, default=2.0)
    parser.add_argument("--no-consensus-search", action="store_true")
    parser.add_argument("--no-inventory-search", action="store_true")
    parser.add_argument(
        "--max-multi-wallet-base-intents",
        type=int,
        default=1000,
        help="Recent base-intent beam for fast leaderboard multi-wallet search; use 0 for exhaustive",
    )
    parser.add_argument("--max-wallets-for-search", type=int, default=25)
    parser.add_argument("--max-single-wallet-candidate-intents", type=int, default=500)
    parser.add_argument("--no-skip-low-intent-candidates", action="store_true")
    parser.add_argument("--policy-preset", choices=("default", "fast"), default="fast")
    parser.add_argument(
        "--arm-live-execution",
        action="store_true",
        help="After paper/profit refresh, write the guarded live execution arm proof state.",
    )
    parser.add_argument("--live-execution-arm-state", default="data/research/wallet_copy_live_execution_arm_state.json")
    parser.add_argument("--live-ledger-state", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--live-ledger-event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--operator-approval-id", default="")
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--explicit-live-operator-go", action="store_true")
    parser.add_argument("--live-orders-allowed", action="store_true")
    parser.add_argument("--live-max-intents", type=int, default=1)
    parser.add_argument("--live-max-event-age-s", type=float, default=30.0)
    return parser.parse_args()


def _leaderboard_categories(args: argparse.Namespace) -> list[str]:
    raw = str(getattr(args, "categories", "") or "")
    values = [value.strip().upper() for value in raw.split(",") if value.strip()]
    if not values:
        values = [str(args.category or "CRYPTO").strip().upper()]
    return list(dict.fromkeys(values))


def _new_leaderboard_scan_progress(args: argparse.Namespace, categories: list[str], periods: list[tuple[str, int]]) -> dict[str, Any]:
    now = utc_now_iso()
    requested_pages = int(args.leaderboard_pages)
    max_pages = int(args.leaderboard_max_pages)
    pages_per_period = max_pages if requested_pages <= 0 else min(max(1, requested_pages), max(1, max_pages))
    return {
        "schema_version": 1,
        "kind": "wallet_copy_leaderboard_scan_progress",
        "flow_stage": "DISCOVER",
        "status": "RUNNING",
        "started_at": now,
        "updated_at": now,
        "categories": categories,
        "periods": [period for period, _limit in periods],
        "period_limits": {period: int(limit) for period, limit in periods},
        "page_mode": "fetch_until_empty_or_cap" if requested_pages <= 0 else "fixed_page_count",
        "pages_requested_per_period": requested_pages,
        "max_pages_per_period": max_pages,
        "effective_pages_per_period": pages_per_period,
        "page_timeout_s": float(args.leaderboard_page_timeout_s),
        "max_wall_runtime_s": float(args.leaderboard_max_wall_runtime_s),
        "page_retries": max(1, int(args.leaderboard_page_retries)),
        "pages_started": 0,
        "pages_completed": 0,
        "rows_fetched": 0,
        "budget_exhausted": False,
        "last_event": None,
        "last_successful_page": None,
        "last_error": None,
        "events_tail": [],
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _checkpoint_leaderboard_scan_progress(path: str | Path, progress: dict[str, Any]) -> None:
    if str(path or "").strip():
        atomic_write_json(path, progress)


def _compact_leaderboard_scan_progress(progress: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "status",
        "started_at",
        "updated_at",
        "completed_at",
        "categories",
        "periods",
        "page_mode",
        "page_timeout_s",
        "max_wall_runtime_s",
        "page_retries",
        "budget_exhausted",
        "pages_started",
        "pages_completed",
        "rows_fetched",
        "last_event",
        "last_successful_page",
        "last_error",
        "progress_state",
    )
    return {key: progress.get(key) for key in keys if key in progress}


def _leaderboard_progress_callback(args: argparse.Namespace, progress: dict[str, Any]):
    progress["progress_state"] = str(args.leaderboard_progress_state)
    _checkpoint_leaderboard_scan_progress(args.leaderboard_progress_state, progress)

    def _callback(event: dict[str, Any]) -> None:
        status = str(event.get("status") or "UNKNOWN")
        progress["updated_at"] = utc_now_iso()
        progress["status"] = "PAGE_ERROR" if status == "PAGE_ERROR" else "RUNNING"
        progress["last_event"] = dict(event)
        if status == "PAGE_STARTED":
            progress["pages_started"] = int(progress.get("pages_started") or 0) + 1
            progress["current_page"] = dict(event)
        elif status == "PAGE_FETCHED":
            progress["pages_completed"] = int(progress.get("pages_completed") or 0) + 1
            progress["rows_fetched"] = int(event.get("rows_total_after_page") or progress.get("rows_fetched") or 0)
            progress["last_successful_page"] = dict(event)
            progress.pop("current_page", None)
        elif status == "PAGE_ERROR":
            progress["last_error"] = dict(event)
        elif status == "PAGE_BUDGET_EXHAUSTED":
            progress["status"] = "PAGE_BUDGET_EXHAUSTED"
            progress["budget_exhausted"] = True
            progress["last_error"] = dict(event)
            progress.pop("current_page", None)
        elif status == "CATEGORY_SKIPPED":
            progress["pages_completed"] = int(progress.get("pages_completed") or 0) + 1
            progress["last_category_skip"] = dict(event)
            progress.pop("current_page", None)
        events_tail = list(progress.get("events_tail") or [])
        events_tail.append(dict(event))
        progress["events_tail"] = events_tail[-20:]
        _checkpoint_leaderboard_scan_progress(args.leaderboard_progress_state, progress)

    return _callback


def _run_command(name: str, purpose: str, argv: list[str]) -> dict[str, Any]:
    started = time.time()
    result = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, check=False)
    stdout_json: dict[str, Any] = {}
    try:
        parsed = json.loads(result.stdout)
        if isinstance(parsed, dict):
            stdout_json = parsed
    except json.JSONDecodeError:
        pass
    return {
        "name": name,
        "purpose": purpose,
        "argv": argv,
        "returncode": result.returncode,
        "duration_s": round(max(0.0, time.time() - started), 6),
        "stdout_json": stdout_json,
        "stdout_tail": result.stdout[-4000:],
        "stderr_tail": result.stderr[-4000:],
    }


def _accepted_non_green_evidence_reason(command_name: str, returncode: int | None) -> str | None:
    if returncode is None:
        return None
    return ACCEPTED_NON_GREEN_EVIDENCE_RETURNCODES.get(command_name, {}).get(int(returncode))


def _annotate_command_result(result: dict[str, Any]) -> dict[str, Any]:
    returncode = int(result.get("returncode") or 0)
    evidence_reason = _accepted_non_green_evidence_reason(str(result.get("name") or ""), returncode)
    return {
        **result,
        "ok": returncode == 0 or evidence_reason is not None,
        "accepted_non_green_evidence": evidence_reason is not None,
        "accepted_non_green_reason": evidence_reason,
        "evidence_status": "WATCH" if evidence_reason else ("PASS" if returncode == 0 else "FAIL"),
    }


def _structural_failures(command_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in command_results
        if int(row.get("returncode") or 0) != 0 and not bool(row.get("accepted_non_green_evidence"))
    ]


def _classify_pipeline_status(command_results: list[dict[str, Any]]) -> str:
    if not command_results:
        return "BLOCKED"
    if _structural_failures(command_results):
        return "BLOCKED"
    if any(bool(row.get("accepted_non_green_evidence")) for row in command_results):
        return "WATCH"
    return "PASS" if all(int(row.get("returncode") or 0) == 0 for row in command_results) else "BLOCKED"


def _candidate_wallet_config_row(candidate: Any) -> dict[str, Any]:
    row = candidate.asdict() if hasattr(candidate, "asdict") else dict(candidate)
    address = str(row.get("address") or row.get("proxy_wallet") or "").strip().lower()
    return {
        "name": str(row.get("name") or f"leaderboard_{address[-8:]}"),
        "address": address,
        "enabled": True,
        "market_filter": "btc_5m",
        "asset_allowlist": ["BTC"],
        "tags": list(dict.fromkeys([*list(row.get("tags") or []), "leaderboard_pipeline_roster"])),
        "notes": str(row.get("notes") or "bounded heartbeat pipeline roster"),
    }


def _prepare_pipeline_roster(args: argparse.Namespace, candidates: list[Any]) -> dict[str, Any]:
    max_wallets = int(getattr(args, "pipeline_max_wallets", 0) or 0)
    if max_wallets <= 0:
        return {
            "status": "ALL_REGISTRY",
            "path": str(args.registry),
            "wallets": 0,
            "bounded": False,
            "reason": "pipeline_max_wallets_not_set",
        }
    path = Path(str(getattr(args, "pipeline_roster", "data/research/leaderboard_pipeline_roster.json")))
    selected = [_candidate_wallet_config_row(candidate) for candidate in candidates[:max_wallets]]
    selected = [row for row in selected if row.get("address")]
    if selected:
        atomic_write_json(
            path,
            {
                "schema_version": 1,
                "kind": "leaderboard_pipeline_roster",
                "generated_at": utc_now_iso(),
                "flow_stage": "SELF-DEV/DISCOVER",
                "source": "onboard_leaderboard_crypto_wallets.deduped_candidate_order",
                "pipeline_max_wallets": max_wallets,
                "wallets": selected,
                "paper_only": True,
                "live_orders_allowed": False,
            },
        )
        return {
            "status": "WRITTEN",
            "path": str(path),
            "wallets": len(selected),
            "bounded": True,
            "reason": "fresh_candidate_roster",
        }
    existing = load_json(path, default={})
    existing_wallets = existing.get("wallets") if isinstance(existing, dict) else []
    if isinstance(existing_wallets, list) and existing_wallets:
        return {
            "status": "REUSED_PREVIOUS",
            "path": str(path),
            "wallets": len(existing_wallets),
            "bounded": True,
            "reason": "no_fresh_candidates_reused_previous_roster",
        }
    return {
        "status": "SKIP_EMPTY",
        "path": str(path),
        "wallets": 0,
        "bounded": True,
        "reason": "pipeline_skipped_empty_roster",
    }


def _pipeline_skipped_empty_roster_result(roster: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": "history_and_paper",
        "purpose": "ingest bounded leaderboard pipeline roster and replay exact-copy intents to paper",
        "argv": [],
        "returncode": 0,
        "duration_s": 0.0,
        "stdout_json": {
            "data_api_ingest_status": "PARTIAL_WITH_NAMED_SKIPS",
            "data_api_skip_count": 1,
            "data_api_skip_rows": [{"skip_reason": "pipeline_skipped_empty_roster"}],
        },
        "stdout_tail": "",
        "stderr_tail": "",
        "ok": True,
        "accepted_non_green_evidence": False,
        "accepted_non_green_reason": None,
        "evidence_status": "PASS",
        "skip_reason": "pipeline_skipped_empty_roster",
        "pipeline_roster_path": roster.get("path"),
        "pipeline_roster_wallets": 0,
    }


def _preserve_previous_candidate_wallets(payload: dict[str, Any], output_path: str | Path) -> dict[str, Any]:
    """Keep previously discovered leaderboard wallets visible across top50 rotation.

    The current leaderboard rows remain the fresh API snapshot, but
    candidate_wallets is the durable discovery universe so downstream history,
    research, and copy-tracking do not go green by silently losing wallets.
    """

    path = Path(output_path)
    if not path.exists():
        return payload
    try:
        previous = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return payload
    if not isinstance(previous, dict):
        return payload

    current_candidates = list(payload.get("candidate_wallets") or [])
    current_by_address = {
        str(row.get("address") or "").strip().lower(): row
        for row in current_candidates
        if isinstance(row, dict) and row.get("address")
    }
    preserved: list[dict[str, Any]] = []
    for row in previous.get("candidate_wallets") or []:
        if not isinstance(row, dict):
            continue
        address = str(row.get("address") or "").strip().lower()
        if not address or address in current_by_address:
            continue
        historical = dict(row)
        historical["address"] = address
        tags = list(historical.get("tags") or [])
        for tag in ("leaderboard_historical", "leaderboard_previously_seen", "candidate", "btc_5m"):
            if tag not in tags:
                tags.append(tag)
        historical["tags"] = tags
        historical["current_leaderboard_rank_status"] = "not_in_current_fetch"
        notes = str(historical.get("notes") or "")
        marker = "Previously observed on CRYPTO leaderboard; preserved for wallet-copy history continuity."
        if marker not in notes:
            historical["notes"] = f"{notes}\n{marker}".strip()
        preserved.append(historical)
        current_by_address[address] = historical

    if not preserved:
        return payload

    payload["candidate_wallets"] = current_candidates + preserved
    summary = dict(payload.get("summary") or {})
    summary["current_unique_wallets"] = len(current_candidates)
    summary["preserved_historical_wallets"] = len(preserved)
    summary["unique_wallets"] = len(payload["candidate_wallets"])
    payload["summary"] = summary
    payload["preserved_historical_candidate_wallets"] = preserved
    return payload


def _preserve_registry_leaderboard_wallets(payload: dict[str, Any], registry_path: str | Path) -> dict[str, Any]:
    """Include registry-known leaderboard wallets in the durable candidate universe."""

    try:
        specs = load_wallet_registry(registry_path)
    except Exception:
        return payload
    current_candidates = list(payload.get("candidate_wallets") or [])
    current_by_address = {
        str(row.get("address") or "").strip().lower(): row
        for row in current_candidates
        if isinstance(row, dict) and row.get("address")
    }
    preserved: list[dict[str, Any]] = []
    for spec in specs:
        address = spec.normalized_address()
        if address in current_by_address or "leaderboard_crypto" not in set(spec.tags):
            continue
        historical = {
            "address": address,
            "name": spec.name,
            "periods": [],
            "ranks": {},
            "pnl_by_period": {},
            "vol_by_period": {},
            "user_name": "",
            "x_username": "",
            "tags": list(dict.fromkeys((*spec.tags, "leaderboard_historical", "leaderboard_registry_preserved"))),
            "notes": spec.notes,
            "current_leaderboard_rank_status": "not_in_current_fetch",
            "preserved_from_registry": True,
        }
        preserved.append(historical)
        current_by_address[address] = historical

    if not preserved:
        return payload

    prior_preserved = list(payload.get("preserved_historical_candidate_wallets") or [])
    payload["candidate_wallets"] = current_candidates + preserved
    summary = dict(payload.get("summary") or {})
    summary.setdefault("current_unique_wallets", len(current_candidates))
    summary["registry_preserved_historical_wallets"] = len(preserved)
    summary["preserved_historical_wallets"] = int(summary.get("preserved_historical_wallets") or 0) + len(preserved)
    summary["unique_wallets"] = len(payload["candidate_wallets"])
    payload["summary"] = summary
    payload["preserved_historical_candidate_wallets"] = prior_preserved + preserved
    return payload


def _write_source_route_blocked_state(args: argparse.Namespace, exc: Exception) -> dict[str, Any]:
    """Persist stale/preserved leaderboard coverage when Data API routing is blocked."""

    previous = load_json(args.output, default={})
    payload = dict(previous) if isinstance(previous, dict) else {}
    payload.setdefault("kind", "wallet_copy_leaderboard_crypto_state")
    payload.setdefault("category", args.category)
    payload.setdefault("categories", _leaderboard_categories(args))
    payload.setdefault("order_by", args.order_by)
    payload.setdefault("leaderboard_rows", [])
    payload.setdefault("top_wallets", [])
    payload.setdefault("candidate_wallets", [])
    payload = _preserve_registry_leaderboard_wallets(payload, args.registry)
    candidate_wallets = [row for row in (payload.get("candidate_wallets") or []) if isinstance(row, dict)]
    summary = dict(payload.get("summary") or {})
    summary.setdefault("current_unique_wallets", 0)
    summary["unique_wallets"] = len(candidate_wallets)
    summary["source_route_blocked"] = True
    payload.update(
        {
            "updated_at": utc_now_iso(),
            "status": "SOURCE_ROUTE_BLOCKED",
            "pipeline_requested": bool(args.run_pipeline),
            "pipeline_bounded_max_wallets": int(args.max_wallets),
            "leaderboard_pages": int(args.leaderboard_pages),
            "leaderboard_max_pages": int(getattr(args, "leaderboard_max_pages", 20)),
            "categories": _leaderboard_categories(args),
            "command_results": [],
            "accepted_non_green_evidence_commands": [],
            "structural_failures": [],
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": summary,
            "source_route_blocked": {
                "status": "POLYMARKET_ROUTE_RESET" if isinstance(exc, PolymarketRouteError) else "POLYMARKET_SOURCE_FETCH_FAILED",
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
                "route_report": getattr(exc, "route_report", {}),
                "preserved_candidate_wallets": len(candidate_wallets),
                "next_action": (
                    "rerun leaderboard discovery with Polymarket base overrides cleared; relay 5xx should fall back to direct source truth"
                ),
            },
        }
    )
    atomic_write_json(args.output, payload)
    return payload


def _exception_http_status(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    try:
        if status_code is not None:
            return int(status_code)
    except (TypeError, ValueError):
        pass
    text = str(exc)
    if "400 Client Error" in text or "status=400" in text or "HTTP 400" in text:
        return 400
    return None


def _is_terminal_leaderboard_category_skip(exc: Exception) -> bool:
    return isinstance(exc, LeaderboardCategoryEmptyPayloadError) or _exception_http_status(exc) == 400


def _pipeline_commands(args: argparse.Namespace) -> list[dict[str, Any]]:
    python = sys.executable
    pipeline_wallets_config = str(getattr(args, "pipeline_wallets_config", args.registry))
    bounded = int(getattr(args, "pipeline_max_wallets", 0) or 0) > 0
    artifact_dir = Path(str(getattr(args, "pipeline_artifact_dir", "data/research/leaderboard_pipeline")))

    def output_path(canonical: str, bounded_name: str) -> str:
        return str(artifact_dir / bounded_name) if bounded else str(canonical)

    history_state = output_path(args.history_state, "history_state.json")
    history_window_index = output_path(
        "data/research/wallet_copy_history_window_index.json",
        "history_window_index.json",
    )
    wallet_event_log = output_path("data/research/wallet_copy_events.jsonl", "wallet_events.jsonl")
    paper_state = output_path(args.paper_state, "paper_state.json")
    paper_event_log = output_path("data/research/wallet_copy_paper_events.jsonl", "paper_events.jsonl")
    sweeper_state = output_path(
        "data/research/wallet_copy_sweeper_profile_state.json",
        "sweeper_profile_state.json",
    )
    research_state = output_path(args.research_state, "research_state.json")
    inventory_paper_state = output_path(args.inventory_paper_state, "inventory_paper_state.json")
    ml_dataset = output_path(args.ml_dataset, "ml_dataset.jsonl")
    profit_state = output_path(args.profit_state, "profit_engine_state.json")
    live_tracker_state = output_path(args.live_tracker_state, "live_tracking_state.json")
    return [
        {
            "name": "history_and_paper",
            "purpose": "ingest registered BTC-5m wallets and replay exact-copy intents to paper",
            "argv": [
                python,
                "scripts/run_wallet_copy_pipeline.py",
                "--wallets-config",
                pipeline_wallets_config,
                "--limit",
                str(int(args.limit)),
                "--pages",
                str(int(args.pages)),
                "--data-api-connect-timeout-s",
                "10",
                "--data-api-read-timeout-s",
                "30",
                "--data-api-retries",
                "2",
                "--history-state",
                history_state,
                "--history-window-index",
                history_window_index,
                "--wallet-event-log",
                wallet_event_log,
                "--paper-state",
                paper_state,
                "--paper-event-log",
                paper_event_log,
                "--wallet-fraction",
                str(float(args.wallet_fraction)),
                "--max-order-usd",
                str(float(args.max_order_usd)),
                "--policy-id",
                "leaderboard_crypto_exact_copy_all_buys",
            ],
        },
        {
            "name": "sweeper_profile",
            "purpose": "classify close/post-close sweeper signatures among discovered wallets",
            "argv": [
                python,
                "scripts/analyze_wallet_sweeper_profile.py",
                "--history-state",
                history_state,
                "--output",
                sweeper_state,
            ],
        },
        {
            "name": "cross_wallet_research",
            "purpose": "build cross-wallet features, consensus, inventory, and resolution-backed paper scores",
            "argv": [
                python,
                "scripts/analyze_wallet_copy_research.py",
                "--history-state",
                history_state,
                "--paper-state",
                paper_state,
                "--inventory-paper-state",
                inventory_paper_state,
                "--resolutions",
                args.resolutions,
                "--max-unresolved-ratio",
                str(float(args.max_unresolved_ratio)),
                "--output",
                research_state,
            ],
        },
        {
            "name": "ml_dataset",
            "purpose": "export wallet-copy feature rows and labels for ML/reverse-engineering",
            "argv": [
                python,
                "scripts/export_wallet_copy_dataset.py",
                "--history-state",
                history_state,
                "--paper-state",
                paper_state,
                "--resolutions",
                args.resolutions,
                "--output",
                ml_dataset,
                "--include-unresolved",
            ],
        },
        {
            "name": "profit_admission",
            "purpose": "search single-wallet, consensus, and inventory policies with raw-baseline guards",
            "argv": [
                python,
                "scripts/run_wallet_copy_profit_engine.py",
                "--history-state",
                history_state,
                "--resolutions",
                args.resolutions,
                "--output",
                profit_state,
                "--live-tracker-state",
                live_tracker_state,
                "--max-unresolved-ratio",
                str(float(args.max_unresolved_ratio)),
                "--slippage-bps",
                str(float(args.slippage_bps)),
                "--max-multi-wallet-base-intents",
                str(int(args.max_multi_wallet_base_intents)),
                "--max-wallets-for-search",
                str(int(args.max_wallets_for_search)),
                "--max-single-wallet-candidate-intents",
                str(int(args.max_single_wallet_candidate_intents)),
                "--live-today-sprint-operator-approval-id",
                LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID,
                "--policy-preset",
                args.policy_preset,
                *(( "--no-consensus-search",) if args.no_consensus_search else ()),
                *(( "--no-inventory-search",) if args.no_inventory_search else ()),
                *(( "--no-skip-low-intent-candidates",) if args.no_skip_low_intent_candidates else ()),
            ],
        },
        *(
            []
            if args.skip_live_tracker
            else [
                {
                    "name": "paper_live_tracker",
                    "purpose": "poll registered wallets into paper live-tracking with CLOB-backed copy-efficiency evidence",
                    "argv": [
                        python,
                        "scripts/run_wallet_live_tracker.py",
                        "--registry",
                        pipeline_wallets_config,
                        "--state",
                        live_tracker_state,
                        "--profit-policy-state",
                        profit_state,
                        "--seed-before-poll",
                        "--seed-history-state",
                        history_state,
                        "--limit",
                        str(int(args.live_tracker_limit)),
                        "--pages",
                        str(int(args.live_tracker_pages)),
                        "--data-api-timeout-s",
                        str(float(args.data_api_timeout_s)),
                        "--iterations",
                        "1",
                        "--market-ws-jsonl",
                        args.market_ws_jsonl,
                        "--enable-clob-books",
                        "--strict-mirror-coverage",
                    ],
                },
                {
                    "name": "profit_admission_after_tracker",
                    "purpose": "refresh admission after live-tracker truth and copy-efficiency evidence",
                    "argv": [
                        python,
                        "scripts/run_wallet_copy_profit_engine.py",
                        "--history-state",
                        history_state,
                        "--resolutions",
                        args.resolutions,
                        "--output",
                        profit_state,
                        "--live-tracker-state",
                        live_tracker_state,
                        "--max-unresolved-ratio",
                        str(float(args.max_unresolved_ratio)),
                        "--slippage-bps",
                        str(float(args.slippage_bps)),
                        "--max-multi-wallet-base-intents",
                        str(int(args.max_multi_wallet_base_intents)),
                        "--max-wallets-for-search",
                        str(int(args.max_wallets_for_search)),
                        "--max-single-wallet-candidate-intents",
                        str(int(args.max_single_wallet_candidate_intents)),
                        "--live-today-sprint-operator-approval-id",
                        LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID,
                        "--policy-preset",
                        args.policy_preset,
                        *(( "--no-consensus-search",) if args.no_consensus_search else ()),
                        *(( "--no-inventory-search",) if args.no_inventory_search else ()),
                        *(( "--no-skip-low-intent-candidates",) if args.no_skip_low_intent_candidates else ()),
                    ],
                },
            ]
        ),
        *(
            [
                {
                    "name": "live_execution_arm",
                    "purpose": "build the guarded same-CopyIntent live execution proof and submit only if explicit live gates pass",
                    "argv": [
                        python,
                        "scripts/run_wallet_copy_live_execution.py",
                        "--profit-state",
                        args.profit_state,
                        "--history-state",
                        args.history_state,
                        "--state",
                        args.live_execution_arm_state,
                        "--live-ledger-state",
                        args.live_ledger_state,
                        "--live-ledger-event-log",
                        args.live_ledger_event_log,
                        "--max-intents",
                        str(int(args.live_max_intents)),
                        "--max-event-age-s",
                        str(float(args.live_max_event_age_s)),
                        *(
                            ["--operator-approval-id", str(args.operator_approval_id)]
                            if str(args.operator_approval_id or "")
                            else []
                        ),
                        *(["--execute-live"] if args.execute_live else []),
                        *(["--explicit-live-operator-go"] if args.explicit_live_operator_go else []),
                        *(["--live-orders-allowed"] if args.live_orders_allowed else []),
                    ],
                }
            ]
            if args.arm_live_execution
            else []
        ),
    ]


def main() -> int:
    args = parse_args()
    categories = _leaderboard_categories(args)
    periods = []
    if args.weekly_limit > 0:
        periods.append(("WEEK", args.weekly_limit))
    if args.monthly_limit > 0:
        periods.append(("MONTH", args.monthly_limit))
    leaderboard_scan_progress = _new_leaderboard_scan_progress(args, categories, periods)
    progress_callback = _leaderboard_progress_callback(args, leaderboard_scan_progress)

    rows = []
    category_errors: list[dict[str, Any]] = []
    category_terminal_skips: list[dict[str, Any]] = []
    for category in categories:
        for period, limit in periods:
            try:
                rows.extend(
                    fetch_crypto_top_wallets(
                        periods=(period,),
                        category=category,
                        order_by=args.order_by,
                        limit=limit,
                        pages=args.leaderboard_pages,
                        max_pages=args.leaderboard_max_pages,
                        timeout_s=args.leaderboard_page_timeout_s,
                        retries=args.leaderboard_page_retries,
                        max_wall_runtime_s=args.leaderboard_max_wall_runtime_s,
                        progress_callback=progress_callback,
                    )
                )
            except (PolymarketRouteError, requests.RequestException, ValueError) as exc:
                if _is_terminal_leaderboard_category_skip(exc):
                    skip = {
                        "category": category,
                        "period": period,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:500],
                        "http_status": _exception_http_status(exc),
                        "status": "TERMINAL_CATEGORY_SKIP",
                        "reason": "leaderboard_category_not_supported_by_data_api",
                        "flow_stage": "DISCOVER",
                    }
                    category_terminal_skips.append(skip)
                    progress_callback(
                        {
                            "status": "CATEGORY_SKIPPED",
                            "ts": utc_now_iso(),
                            "category": category,
                            "period": period,
                            "limit": limit,
                            "http_status": skip["http_status"],
                            "reason": skip["reason"],
                            "flow_stage": "DISCOVER",
                        }
                    )
                    continue
                category_errors.append(
                    {
                        "category": category,
                        "period": period,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:500],
                        "route_report": getattr(exc, "route_report", {}),
                    }
                )
                continue
    if not rows and category_errors:
        first_error = category_errors[0]
        payload = _write_source_route_blocked_state(args, RuntimeError(str(first_error.get("error") or "leaderboard fetch failed")))
        leaderboard_scan_progress["status"] = "SOURCE_ROUTE_BLOCKED"
        leaderboard_scan_progress["updated_at"] = utc_now_iso()
        leaderboard_scan_progress["completed_at"] = leaderboard_scan_progress["updated_at"]
        leaderboard_scan_progress["category_fetch_error_count"] = len(category_errors)
        leaderboard_scan_progress["category_terminal_skip_count"] = len(category_terminal_skips)
        leaderboard_scan_progress["last_error"] = category_errors[-1] if category_errors else leaderboard_scan_progress.get("last_error")
        _checkpoint_leaderboard_scan_progress(args.leaderboard_progress_state, leaderboard_scan_progress)
        payload["leaderboard_scan_progress"] = _compact_leaderboard_scan_progress(leaderboard_scan_progress)
        payload["category_fetch_errors"] = category_errors
        payload["category_terminal_skips"] = category_terminal_skips
        atomic_write_json(args.output, payload)
        print(
            json.dumps(
                {
                    "output": args.output,
                    "status": payload.get("status"),
                    "unique_wallets": (payload.get("summary") or {}).get("unique_wallets"),
                    "source_route_blocked": payload.get("source_route_blocked"),
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    candidates = build_leaderboard_candidates(rows)
    if args.max_wallets > 0:
        candidates = candidates[: args.max_wallets]
    registry_merge = merge_leaderboard_wallets(candidates, registry_path=args.registry)
    payload = build_leaderboard_state(rows, candidates, registry_merge, category=args.category, order_by=args.order_by)
    payload = _preserve_previous_candidate_wallets(payload, args.output)
    payload = _preserve_registry_leaderboard_wallets(payload, args.registry)
    budget_exhausted = bool(leaderboard_scan_progress.get("budget_exhausted"))
    leaderboard_scan_progress["status"] = (
        "COMPLETED_PARTIAL_BUDGET"
        if budget_exhausted
        else "COMPLETED_WITH_ERRORS"
        if category_errors
        else "COMPLETED"
    )
    leaderboard_scan_progress["updated_at"] = utc_now_iso()
    leaderboard_scan_progress["completed_at"] = leaderboard_scan_progress["updated_at"]
    leaderboard_scan_progress["category_fetch_error_count"] = len(category_errors)
    leaderboard_scan_progress["category_terminal_skip_count"] = len(category_terminal_skips)
    _checkpoint_leaderboard_scan_progress(args.leaderboard_progress_state, leaderboard_scan_progress)
    observation_policy = {
        "status": "MAXIMIZE_OBSERVATION_UNIVERSE",
        "categories": categories,
        "periods": [period for period, _limit in periods],
        "order_by": args.order_by,
        "category": args.category,
        "category_terminal_skips_are_nonfatal": True,
        "category_terminal_skip_count": len(category_terminal_skips),
        "pages_requested_per_period": int(args.leaderboard_pages),
        "max_pages_per_period": int(args.leaderboard_max_pages),
        "page_mode": "fetch_until_empty_or_cap" if int(args.leaderboard_pages) <= 0 else "fixed_page_count",
        "copy_all_fetched_wallets_to_registry": int(args.max_wallets) <= 0,
        "post_dedupe_wallet_limit": int(args.max_wallets),
        "preserve_previous_and_registry_wallets": True,
        "page_timeout_s": float(args.leaderboard_page_timeout_s),
        "max_wall_runtime_s": float(args.leaderboard_max_wall_runtime_s),
        "page_retries": max(1, int(args.leaderboard_page_retries)),
        "page_progress_state": str(args.leaderboard_progress_state),
        "partial_budget_exhausted": budget_exhausted,
        "paper_only": True,
        "live_orders_allowed": False,
        "arm_live_execution_requested": bool(args.arm_live_execution),
        "result_change_trigger": (
            "if no live-admissible profitable candidate emerges, expand/resume wallet coverage, re-rank "
            "copyability, and refresh the weighted multi-wallet inventory search instead of idling"
        ),
    }
    payload["observation_policy"] = observation_policy
    summary = dict(payload.get("summary") or {})
    summary["observation_mode"] = observation_policy["page_mode"]
    summary["leaderboard_max_pages_per_period"] = int(args.leaderboard_max_pages)
    summary["copy_all_fetched_wallets_to_registry"] = int(args.max_wallets) <= 0
    summary["leaderboard_categories"] = categories
    summary["category_fetch_error_count"] = len(category_errors)
    summary["category_terminal_skip_count"] = len(category_terminal_skips)
    summary["leaderboard_budget_exhausted"] = budget_exhausted
    pipeline_roster = _prepare_pipeline_roster(args, candidates) if args.run_pipeline else {}
    if pipeline_roster:
        summary["pipeline_roster_path"] = pipeline_roster.get("path")
        summary["pipeline_roster_wallets"] = pipeline_roster.get("wallets")
        summary["pipeline_roster_status"] = pipeline_roster.get("status")
        summary["pipeline_max_wallets"] = int(args.pipeline_max_wallets)
        summary["pipeline_output_isolated"] = bool(pipeline_roster.get("bounded"))
        summary["pipeline_artifact_dir"] = (
            str(args.pipeline_artifact_dir) if pipeline_roster.get("bounded") else None
        )
        setattr(args, "pipeline_wallets_config", pipeline_roster.get("path") or args.registry)
    payload["summary"] = summary
    command_results: list[dict[str, Any]] = []
    if args.run_pipeline:
        if pipeline_roster.get("status") == "SKIP_EMPTY":
            command_results.append(_pipeline_skipped_empty_roster_result(pipeline_roster))
        else:
            for command in _pipeline_commands(args):
                result = _annotate_command_result(_run_command(command["name"], command["purpose"], command["argv"]))
                command_results.append(result)
                if _structural_failures([result]):
                    break

    status = "DISCOVERY_REGISTERED"
    if args.run_pipeline:
        status = _classify_pipeline_status(command_results)
    accepted_non_green = [
        {
            "name": row["name"],
            "returncode": row["returncode"],
            "reason": row.get("accepted_non_green_reason"),
        }
        for row in command_results
        if bool(row.get("accepted_non_green_evidence"))
    ]
    structural_failures = [
        {"name": row["name"], "returncode": row["returncode"], "evidence_status": row.get("evidence_status")}
        for row in _structural_failures(command_results)
    ]
    payload.update(
        {
            "updated_at": utc_now_iso(),
            "status": status,
            "pipeline_requested": bool(args.run_pipeline),
            "pipeline_bounded_max_wallets": int(args.max_wallets),
            "pipeline_max_wallets": int(args.pipeline_max_wallets),
            "pipeline_roster_path": pipeline_roster.get("path") if pipeline_roster else None,
            "pipeline_roster_wallets": pipeline_roster.get("wallets") if pipeline_roster else None,
            "pipeline_roster_status": pipeline_roster.get("status") if pipeline_roster else None,
            "pipeline_output_isolated": bool(pipeline_roster.get("bounded")) if pipeline_roster else False,
            "pipeline_artifact_dir": (
                str(args.pipeline_artifact_dir)
                if pipeline_roster and pipeline_roster.get("bounded")
                else None
            ),
            "leaderboard_pages": int(args.leaderboard_pages),
            "leaderboard_max_pages": int(args.leaderboard_max_pages),
            "categories": categories,
            "observation_policy": observation_policy,
            "leaderboard_scan_progress": _compact_leaderboard_scan_progress(leaderboard_scan_progress),
            "category_fetch_errors": category_errors,
            "category_terminal_skips": category_terminal_skips,
            "command_results": command_results,
            "accepted_non_green_evidence_commands": accepted_non_green,
            "structural_failures": structural_failures,
        }
    )
    atomic_write_json(args.output, payload)
    print(
        json.dumps(
            {
                "output": args.output,
                "status": status,
                "unique_wallets": payload["summary"]["unique_wallets"],
                "registry_merge": registry_merge,
                "pipeline_max_wallets": int(args.pipeline_max_wallets),
                "pipeline_roster_path": pipeline_roster.get("path") if pipeline_roster else None,
                "pipeline_roster_wallets": pipeline_roster.get("wallets") if pipeline_roster else None,
                "commands": [
                    {
                        "name": row["name"],
                        "returncode": row["returncode"],
                        "evidence_status": row.get("evidence_status"),
                    }
                    for row in command_results
                ],
                "accepted_non_green_evidence_commands": accepted_non_green,
                "structural_failures": structural_failures,
                "paper_only": True,
                "live_orders_allowed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if status in {"PASS", "WATCH", "DISCOVERY_REGISTERED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
