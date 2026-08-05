#!/usr/bin/env python3
"""Continuously measure forward profit candidates with paper-only CopyIntent proof.

This guard is deliberately narrow. It does not discover new strategies, and it
never enables live execution. It keeps the current profit-engine forward queue
under hot polling so candidate-specific CLOB-backed copy evidence can appear as
soon as the followed wallet emits a policy-compatible BTC 5m BUY.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_wallet_copy_autonomous_repair import (  # noqa: E402
    _active_forward_probe_policy_command,
    _active_forward_probe_tracker_command,
    _candidate_forward_tracker_commands,
    _profit_command,
    _run_command,
)
from src.wallet_copy.source_route import (  # noqa: E402
    DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE,
    autonomous_repair_progress_blocker,
    source_route_allows_measurement,
    source_route_probe_progress_blocker,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


ACTIVE_GUARD_COMMAND_PROGRESS_STATUSES = {"STARTING", "RUNNING"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--state", default="data/research/wallet_copy_candidate_forward_guard_state.json")
    parser.add_argument(
        "--command-progress-state",
        default="data/research/wallet_copy_candidate_forward_guard_command_progress.json",
    )
    parser.add_argument("--history-state", default="data/research/wallet_copy_live_guard_hot_history_state.json")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--live-tracker-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument("--active-hotlane-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument("--source-route-state", default="data/research/wallet_copy_source_route_state.json")
    parser.add_argument(
        "--autonomous-repair-command-progress-state",
        default=DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE,
    )
    parser.add_argument(
        "--active-hotlane-live-tracker-state",
        default="data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-profit-state",
        default="data/research/wallet_copy_active_forward_probe_profit_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-live-tracker-state",
        default="data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-live-tracker-event-log",
        default="data/research/wallet_copy_active_forward_probe_live_tracking_events.jsonl",
    )
    parser.add_argument("--active-forward-probe-paper-state", default="data/research/wallet_copy_active_forward_probe_paper_state.json")
    parser.add_argument(
        "--active-forward-probe-paper-event-log",
        default="data/research/wallet_copy_active_forward_probe_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-forward-probe-tracker-time-replay-paper-state",
        default="data/research/wallet_copy_active_forward_probe_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_active_forward_probe_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-forward-probe-single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_active_forward_probe_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_active_forward_probe_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--candidate-forward-live-tracker-state",
        default="data/research/wallet_copy_candidate_forward_live_tracking_state.json",
    )
    parser.add_argument(
        "--candidate-forward-live-tracker-event-log",
        default="data/research/wallet_copy_candidate_forward_live_tracking_events.jsonl",
    )
    parser.add_argument("--candidate-forward-paper-state", default="data/research/wallet_copy_candidate_forward_paper_state.json")
    parser.add_argument(
        "--candidate-forward-paper-event-log",
        default="data/research/wallet_copy_candidate_forward_paper_events.jsonl",
    )
    parser.add_argument(
        "--candidate-forward-tracker-time-replay-paper-state",
        default="data/research/wallet_copy_candidate_forward_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--candidate-forward-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_candidate_forward_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--candidate-forward-single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_candidate_forward_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--candidate-forward-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_candidate_forward_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument("--candidate-forward-probe-ranks", type=int, default=12)
    parser.add_argument("--candidate-forward-probe-iterations", type=int, default=1)
    parser.add_argument("--candidate-forward-probe-max-runtime-s", type=float, default=90.0)
    parser.add_argument("--candidate-forward-probe-max-poll-runtime-s", type=float, default=25.0)
    parser.add_argument("--active-hotlane-max-wallets", type=int, default=32)
    parser.add_argument("--active-hotlane-wallets-per-tick", type=int, default=8)
    parser.add_argument("--active-hotlane-parallel-wallet-fetches", type=int, default=8)
    parser.add_argument("--active-hotlane-ticks", type=int, default=4)
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--market-ws-jsonl", default="data/research/clob_market_ws_events.jsonl")
    parser.add_argument("--live-tracker-limit", type=int, default=5)
    parser.add_argument("--live-tracker-pages", type=int, default=1)
    parser.add_argument("--live-tracker-iterations", type=int, default=1)
    parser.add_argument("--live-tracker-poll-interval-s", type=float, default=0.2)
    parser.add_argument("--live-tracker-max-runtime-s", type=float, default=30.0)
    parser.add_argument("--live-tracker-paper-retain-orders", type=int, default=1_000)
    parser.add_argument("--live-tracker-paper-retain-lifecycle-events", type=int, default=3_000)
    parser.add_argument("--live-tracker-paper-retain-dedupe-ids", type=int, default=250_000)
    parser.add_argument("--data-api-timeout-s", type=float, default=1.5)
    parser.add_argument(
        "--hot-path-data-api-trade-query-keys",
        default="user,proxyWallet",
        help=(
            "Comma-separated Data API trade query keys for candidate-forward hot-copy proof. "
            "Keep both user and proxyWallet visible; identity mismatches are quarantined by ingestion."
        ),
    )
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--gamma-timeout-s", type=float, default=1.0)
    parser.add_argument("--onchain-timeout-s", type=float, default=1.0)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.5)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--policy-preset", default="fast")
    parser.add_argument("--max-wallets-for-search", type=int, default=0)
    parser.add_argument("--max-single-wallet-candidate-intents", type=int, default=0)
    parser.add_argument("--max-multi-wallet-base-intents", type=int, default=0)
    parser.add_argument("--command-timeout-s", type=float, default=240.0)
    parser.add_argument("--cycles", type=int, default=1, help="0 means run forever.")
    parser.add_argument("--sleep-s", type=float, default=30.0)
    parser.add_argument(
        "--route-block-sleep-s",
        type=float,
        default=60.0,
        help="Sleep this long between cycles when Polymarket source routes are known blocked.",
    )
    parser.add_argument("--deep-research", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="Print a compact per-cycle summary while persisting full state.")
    return parser.parse_args()


def _repair_args(args: argparse.Namespace) -> SimpleNamespace:
    return SimpleNamespace(
        registry=args.registry,
        history_state=args.history_state,
        profit_state=args.profit_state,
        live_tracker_state=args.live_tracker_state,
        active_hotlane_state=args.active_hotlane_state,
        active_hotlane_registry=args.active_hotlane_registry,
        source_route_state=getattr(args, "source_route_state", "data/research/wallet_copy_source_route_state.json"),
        active_hotlane_live_tracker_state=args.active_hotlane_live_tracker_state,
        active_forward_probe_profit_state=args.active_forward_probe_profit_state,
        active_forward_probe_live_tracker_state=args.active_forward_probe_live_tracker_state,
        active_forward_probe_live_tracker_event_log=args.active_forward_probe_live_tracker_event_log,
        active_forward_probe_paper_state=args.active_forward_probe_paper_state,
        active_forward_probe_paper_event_log=args.active_forward_probe_paper_event_log,
        active_forward_probe_tracker_time_replay_paper_state=args.active_forward_probe_tracker_time_replay_paper_state,
        active_forward_probe_tracker_time_replay_paper_event_log=args.active_forward_probe_tracker_time_replay_paper_event_log,
        active_forward_probe_single_wallet_exact_copy_paper_state=args.active_forward_probe_single_wallet_exact_copy_paper_state,
        active_forward_probe_single_wallet_exact_copy_paper_event_log=args.active_forward_probe_single_wallet_exact_copy_paper_event_log,
        candidate_forward_live_tracker_state=args.candidate_forward_live_tracker_state,
        candidate_forward_live_tracker_event_log=args.candidate_forward_live_tracker_event_log,
        candidate_forward_paper_state=args.candidate_forward_paper_state,
        candidate_forward_paper_event_log=args.candidate_forward_paper_event_log,
        candidate_forward_tracker_time_replay_paper_state=args.candidate_forward_tracker_time_replay_paper_state,
        candidate_forward_tracker_time_replay_paper_event_log=args.candidate_forward_tracker_time_replay_paper_event_log,
        candidate_forward_single_wallet_exact_copy_paper_state=args.candidate_forward_single_wallet_exact_copy_paper_state,
        candidate_forward_single_wallet_exact_copy_paper_event_log=args.candidate_forward_single_wallet_exact_copy_paper_event_log,
        candidate_forward_probe_ranks=args.candidate_forward_probe_ranks,
        candidate_forward_probe_iterations=args.candidate_forward_probe_iterations,
        candidate_forward_probe_max_runtime_s=args.candidate_forward_probe_max_runtime_s,
        candidate_forward_probe_max_poll_runtime_s=args.candidate_forward_probe_max_poll_runtime_s,
        resolutions=args.resolutions,
        market_ws_jsonl=args.market_ws_jsonl,
        live_tracker_limit=args.live_tracker_limit,
        live_tracker_pages=args.live_tracker_pages,
        live_tracker_iterations=args.live_tracker_iterations,
        live_tracker_poll_interval_s=args.live_tracker_poll_interval_s,
        live_tracker_max_runtime_s=args.live_tracker_max_runtime_s,
        live_tracker_max_poll_runtime_s=args.candidate_forward_probe_max_poll_runtime_s,
        live_tracker_paper_retain_orders=getattr(args, "live_tracker_paper_retain_orders", 1_000),
        live_tracker_paper_retain_lifecycle_events=getattr(args, "live_tracker_paper_retain_lifecycle_events", 3_000),
        live_tracker_paper_retain_dedupe_ids=getattr(args, "live_tracker_paper_retain_dedupe_ids", 250_000),
        data_api_timeout_s=args.data_api_timeout_s,
        hot_path_data_api_trade_query_keys=args.hot_path_data_api_trade_query_keys,
        clob_timeout_s=args.clob_timeout_s,
        gamma_timeout_s=args.gamma_timeout_s,
        onchain_timeout_s=args.onchain_timeout_s,
        max_unresolved_ratio=args.max_unresolved_ratio,
        slippage_bps=args.slippage_bps,
        policy_preset=args.policy_preset,
        max_wallets_for_search=args.max_wallets_for_search,
        max_single_wallet_candidate_intents=args.max_single_wallet_candidate_intents,
        max_multi_wallet_base_intents=args.max_multi_wallet_base_intents,
        command_timeout_s=args.command_timeout_s,
        active_hotlane_max_wallets=getattr(args, "active_hotlane_max_wallets", 32),
        active_hotlane_wallets_per_tick=getattr(args, "active_hotlane_wallets_per_tick", 8),
        active_hotlane_parallel_wallet_fetches=getattr(args, "active_hotlane_parallel_wallet_fetches", 8),
        active_hotlane_ticks=getattr(args, "active_hotlane_ticks", 4),
        deep_research=args.deep_research,
    )


def _cycle_commands(args: argparse.Namespace) -> list[dict[str, Any]]:
    repair_args = _repair_args(args)
    commands = [_profit_command(repair_args, name="candidate_forward_guard_profit_before")]
    commands.append(
        _active_forward_probe_policy_command(
            repair_args,
            active_hotlane_state=args.active_hotlane_state,
        )
    )
    commands.append(
        _active_forward_probe_tracker_command(
            repair_args,
            active_hotlane_registry=args.active_hotlane_registry,
        )
    )
    commands.extend(_candidate_forward_tracker_commands(repair_args))
    commands.append(_profit_command(repair_args, name="candidate_forward_guard_profit_after"))
    return commands


def _source_route_status(path: str, *, progress_state: str | Path | None = None) -> dict[str, Any]:
    probe_blocker = source_route_probe_progress_blocker(progress_state)
    if probe_blocker:
        return {
            **probe_blocker,
            "blockers": ["source_route_probe_running"],
            "route_class_counts": {},
            "source_proxy_configured": None,
            "external_route_required": None,
            "next_action": "pause heavy guard children until the fresh source-route probe finishes",
        }
    state = load_json(path, default={})
    if not isinstance(state, dict):
        return {
            "status": "MISSING",
            "paper_only": True,
            "live_orders_allowed": False,
            "blockers": ["source_route_state_missing"],
        }
    status = str(state.get("status") or "UNKNOWN")
    return {
        "status": status,
        "paper_only": state.get("paper_only", True),
        "live_orders_allowed": state.get("live_orders_allowed", False),
        "blockers": [] if source_route_allows_measurement(status) else [f"source_route_{status.lower()}"],
        "route_class_counts": state.get("route_class_counts") or {},
        "source_proxy_configured": state.get("source_proxy_configured"),
        "external_route_required": state.get("external_route_required"),
        "generated_at": state.get("generated_at"),
        "next_action": state.get("next_action"),
    }


def _profit_status(path: str) -> dict[str, Any]:
    state = load_json(path, default={})
    if not isinstance(state, dict):
        return {"status": "MISSING", "live_orders_allowed": False, "paper_only": True}
    decision = state.get("decision") if isinstance(state.get("decision"), dict) else {}
    live_cert = state.get("live_readiness_certificate") if isinstance(state.get("live_readiness_certificate"), dict) else {}
    forward = state.get("forward_candidate") if isinstance(state.get("forward_candidate"), dict) else {}
    truth = state.get("forward_candidate_live_tracker_truth") if isinstance(state.get("forward_candidate_live_tracker_truth"), dict) else {}
    return {
        "decision_status": decision.get("status"),
        "live_admission_status": decision.get("live_admission_status"),
        "live_admission_blockers": decision.get("live_admission_blockers") or [],
        "live_readiness_status": live_cert.get("status"),
        "live_readiness_blockers": live_cert.get("blockers") or [],
        "live_orders_allowed": state.get("live_orders_allowed", False),
        "paper_only": state.get("paper_only", True),
        "forward_candidate_id": forward.get("candidate_id"),
        "forward_candidate_source_wallet": forward.get("source_wallet"),
        "forward_candidate_policy_id": forward.get("policy_id"),
        "forward_candidate_status": forward.get("status"),
        "forward_candidate_blockers": forward.get("blockers") or [],
        "forward_candidate_truth_status": truth.get("status"),
        "forward_candidate_truth_blockers": truth.get("blockers") or [],
    }


def _guard_command_progress_status(
    path: str | Path | None,
    *,
    max_age_s: float = 900.0,
) -> dict[str, Any]:
    if path is None:
        return {"status": "MISSING", "blockers": [], "paper_only": True, "live_orders_allowed": False}
    progress_path = Path(path)
    payload = load_json(progress_path, default={})
    if not isinstance(payload, dict) or not payload:
        return {
            "status": "MISSING",
            "progress_state": str(progress_path),
            "blockers": [],
            "paper_only": True,
            "live_orders_allowed": False,
        }
    progress_status = str(payload.get("status") or "").upper()
    age_s: float | None = None
    try:
        age_s = time.time() - progress_path.stat().st_mtime
    except OSError:
        age_s = None
    pid = 0
    try:
        pid = int(payload.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0

    stale_reasons: list[str] = []
    if progress_status in ACTIVE_GUARD_COMMAND_PROGRESS_STATUSES:
        if age_s is not None and age_s > max(0.0, float(max_age_s)):
            stale_reasons.append("progress_age_above_cap")
        if pid > 0:
            try:
                os.kill(pid, 0)
            except OSError:
                stale_reasons.append("pid_not_alive")

    status = progress_status or "UNKNOWN"
    blockers: list[str] = []
    if stale_reasons:
        status = f"STALE_{progress_status or 'RUNNING'}"
        blockers.append("guard_command_progress_stale")

    return {
        "status": status,
        "progress_state": str(progress_path),
        "name": payload.get("name"),
        "pid": pid or None,
        "generated_at": payload.get("generated_at"),
        "age_s": round(age_s, 3) if age_s is not None else None,
        "stale_reasons": stale_reasons,
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def run_cycle(args: argparse.Namespace, cycle: int) -> dict[str, Any]:
    guard_command_progress = _guard_command_progress_status(getattr(args, "command_progress_state", None))
    source_route = _source_route_status(
        args.source_route_state,
        progress_state=getattr(args, "autonomous_repair_command_progress_state", None),
    )
    source_route_status = str(source_route.get("status") or "")
    if source_route_status and not source_route_allows_measurement(source_route_status):
        return {
            "cycle": cycle,
            "generated_at_s": time.time(),
            "status": "SOURCE_ROUTE_BLOCKED_SLEEP",
            "paper_only": True,
            "live_orders_allowed": False,
            "source_route": source_route,
            "guard_command_progress": guard_command_progress,
            "command_results": [
                {
                    "name": "candidate_forward_guard_route_blocked_pause",
                    "ok": True,
                    "returncode": 2,
                    "skipped": True,
                    "skip_reason": "SOURCE_ROUTE_BLOCKED_NO_HEAVY_CHILDREN",
                    "source_route_status": source_route_status,
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ],
            "profit_status": _profit_status(args.profit_state),
        }
    repair_blocker = autonomous_repair_progress_blocker(
        getattr(args, "autonomous_repair_command_progress_state", None)
    )
    if repair_blocker:
        return {
            "cycle": cycle,
            "generated_at_s": time.time(),
            "status": "AUTONOMOUS_REPAIR_RUNNING_SLEEP",
            "paper_only": True,
            "live_orders_allowed": False,
            "source_route": source_route,
            "guard_command_progress": guard_command_progress,
            "autonomous_repair_progress": repair_blocker,
            "command_results": [
                {
                    "name": "candidate_forward_guard_autonomous_repair_pause",
                    "ok": True,
                    "returncode": 2,
                    "skipped": True,
                    "skip_reason": "AUTONOMOUS_REPAIR_RUNNING_NO_HEAVY_CHILDREN",
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ],
            "profit_status": _profit_status(args.profit_state),
        }

    command_results: list[dict[str, Any]] = []
    for command in _cycle_commands(args):
        result = _run_command(
            command["name"],
            command.get("purpose", ""),
            command["argv"],
            acceptable_returncodes=tuple(command.get("acceptable_returncodes") or (0,)),
            timeout_s=float(args.command_timeout_s),
            progress_state=Path(args.command_progress_state),
            progress_kind="wallet_copy_candidate_forward_guard_command_progress",
        )
        command_results.append(result)
        if not result.get("ok"):
            break
    return {
        "cycle": cycle,
        "generated_at_s": time.time(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_route": source_route,
        "guard_command_progress": _guard_command_progress_status(getattr(args, "command_progress_state", None)),
        "command_results": command_results,
        "profit_status": _profit_status(args.profit_state),
    }


def main() -> int:
    args = parse_args()
    cycles = int(args.cycles)
    cycle = 0
    last: dict[str, Any] = {}
    while cycles <= 0 or cycle < cycles:
        cycle += 1
        last = run_cycle(args, cycle)
        atomic_write_json(args.state, last)
        printable = last
        if args.quiet:
            printable = {
                "cycle": last.get("cycle"),
                "generated_at_s": last.get("generated_at_s"),
                "paper_only": True,
                "live_orders_allowed": False,
                "status": last.get("status"),
                "source_route": last.get("source_route"),
                "guard_command_progress": last.get("guard_command_progress"),
                "commands": [
                    {"name": row.get("name"), "returncode": row.get("returncode"), "ok": row.get("ok")}
                    for row in last.get("command_results", [])
                    if isinstance(row, dict)
                ],
                "autonomous_repair_progress": last.get("autonomous_repair_progress"),
                "profit_status": last.get("profit_status"),
            }
        print(json.dumps(printable, indent=2, sort_keys=True, default=str), flush=True)
        if cycles > 0 and cycle >= cycles:
            break
        sleep_s = float(args.sleep_s)
        if last.get("status") in {"SOURCE_ROUTE_BLOCKED_SLEEP", "AUTONOMOUS_REPAIR_RUNNING_SLEEP"}:
            sleep_s = max(sleep_s, float(args.route_block_sleep_s))
        time.sleep(max(0.0, sleep_s))
    failed = [row for row in last.get("command_results", []) if not row.get("ok")]
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
