#!/usr/bin/env python3
"""Autonomous paper-only wallet-copy audit, repair, and research loop.

The loop is intentionally conservative: it can refresh discovery, rebuild
paper/research/profit states, run a scoped paper live-tracker measurement, and
persist code-level backlog items. It never enables live execution and treats
strict tracker failures as measurement evidence rather than as a process crash.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.mission import mission_contract, mission_contract_check  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.source_route import (  # noqa: E402
    source_route_allows_live_execution,
    source_route_allows_measurement,
    source_route_is_recovered_degraded,
)
from src.wallet_copy.status import ANALYZE, CORRECTION, PASS, WATCH, active_plan_mode, active_status_from_blockers  # noqa: E402
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json  # noqa: E402


PYTHON = "python3"
LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID = "OP-LIVE-20260703-BELA"
RUN_ID = f"{int(time.time())}-{os.getpid()}"
COMMAND_PROGRESS_STATE = ROOT / "data/research/wallet_copy_autonomous_repair_command_progress.json"
COMMAND_LOG_DIR = ROOT / "data/research/wallet_copy_command_logs"
ACTIVE_CHILD_PROCESS_GROUPS: set[int] = set()
FOREIGN_WALLET_COPY_WRITER_SCRIPTS = (
    "scripts/run_wallet_copy_profit_engine.py",
    "scripts/run_wallet_copy_pipeline_resume.py",
    "scripts/run_wallet_copy_pipeline.py",
)
DEFAULT_SOURCE_ROUTE_STATE = "data/research/wallet_copy_source_route_state.json"


def _terminate_active_child_process_groups(*, sig: int = signal.SIGTERM) -> None:
    for pgid in list(ACTIVE_CHILD_PROCESS_GROUPS):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass


def _install_parent_signal_cleanup() -> None:
    def _handler(signum: int, _frame: Any) -> None:
        _terminate_active_child_process_groups(sig=signal.SIGTERM)
        raise SystemExit(128 + int(signum))

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--state", default="data/research/wallet_copy_autonomous_repair_state.json")
    parser.add_argument("--backlog-state", default="data/research/wallet_copy_autonomous_backlog.json")
    parser.add_argument("--backlog-log", default="data/research/wallet_copy_autonomous_backlog.jsonl")
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--research-state", default="data/research/wallet_copy_research_state.json")
    parser.add_argument("--inventory-paper-state", default="data/research/wallet_copy_inventory_paper_state.json")
    parser.add_argument("--ml-dataset", default="data/research/wallet_copy_ml_dataset.jsonl")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--wallet-analysis-state", default="data/research/wallet_copy_wallet_analysis_state.json")
    parser.add_argument("--strategy-direction-state", default="data/research/wallet_copy_strategy_direction_state.json")
    parser.add_argument(
        "--relaxed-copyability-report-state",
        default="data/research/wallet_copy_relaxed_copyability_report.json",
    )
    parser.add_argument("--live-tracker-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--live-tracker-event-log", default="data/research/wallet_copy_live_tracking_events.jsonl")
    parser.add_argument(
        "--candidate-forward-live-tracker-state",
        default="data/research/wallet_copy_candidate_forward_live_tracking_state.json",
    )
    parser.add_argument(
        "--candidate-forward-live-tracker-event-log",
        default="data/research/wallet_copy_candidate_forward_live_tracking_events.jsonl",
    )
    parser.add_argument(
        "--candidate-forward-paper-state",
        default="data/research/wallet_copy_candidate_forward_paper_state.json",
    )
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
    parser.add_argument(
        "--active-forward-probe-paper-state",
        default="data/research/wallet_copy_active_forward_probe_paper_state.json",
    )
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
    parser.add_argument("--active-forward-probe-iterations", type=int, default=240)
    parser.add_argument("--active-forward-probe-max-runtime-s", type=float, default=90.0)
    parser.add_argument("--active-forward-probe-max-poll-runtime-s", type=float, default=30.0)
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument("--active-hotlane-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument(
        "--active-hotlane-live-tracker-state",
        default="data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    )
    parser.add_argument(
        "--active-hotlane-live-tracker-event-log",
        default="data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
    )
    parser.add_argument("--active-hotlane-paper-state", default="data/research/wallet_copy_active_hotlane_paper_state.json")
    parser.add_argument(
        "--active-hotlane-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-tracker-time-replay-paper-state",
        default="data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--active-hotlane-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--active-hotlane-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-all-order-exact-copy-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
    )
    parser.add_argument(
        "--active-hotlane-all-order-exact-copy-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-all-order-tactic-replay-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
    )
    parser.add_argument(
        "--active-hotlane-all-order-tactic-replay-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy_aggressive_tactic_replay.jsonl",
    )
    parser.add_argument("--market-ws-jsonl", default="data/research/clob_market_ws_events.jsonl")
    parser.add_argument("--adaptive-bot-state", default="data/research/wallet_copy_adaptive_bot_state.json")
    parser.add_argument("--adaptive-bot-paper-state", default="data/research/wallet_copy_adaptive_bot_paper_state.json")
    parser.add_argument("--adaptive-bot-paper-event-log", default="data/research/wallet_copy_adaptive_bot_paper_events.jsonl")
    parser.add_argument(
        "--adaptive-bot-single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--adaptive-bot-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--adaptive-bot-tracker-time-replay-paper-state",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--adaptive-bot-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--leaderboard-stale-s", type=float, default=900.0)
    parser.add_argument("--tracker-stale-s", type=float, default=900.0)
    parser.add_argument("--pipeline-limit", type=int, default=500)
    parser.add_argument("--pipeline-pages", type=int, default=1)
    parser.add_argument("--pipeline-resume-state", default="data/research/wallet_copy_pipeline_resume_state.json")
    parser.add_argument("--pipeline-pages-per-resume-run", type=int, default=1)
    parser.add_argument(
        "--pipeline-wallet-batch-size",
        type=int,
        default=4,
        help="Registry wallets per resumable history/paper child batch; use 1 only on recovered-degraded source routes.",
    )
    parser.add_argument(
        "--leaderboard-pages",
        type=int,
        default=0,
        help="Pages per WEEK/MONTH leaderboard period; 0 fetches until empty or --leaderboard-max-pages is hit.",
    )
    parser.add_argument("--leaderboard-max-pages", type=int, default=20)
    parser.add_argument("--live-tracker-limit", type=int, default=5)
    parser.add_argument("--live-tracker-pages", type=int, default=1)
    parser.add_argument("--data-api-timeout-s", type=float, default=2.0)
    parser.add_argument(
        "--hot-path-data-api-trade-query-keys",
        default="user,proxyWallet",
        help=(
            "Comma-separated Data API trade query keys for latency-critical hot-lane/candidate-forward copy proof. "
            "Keep both user and proxyWallet visible; ingestion quarantines identity mismatches."
        ),
    )
    parser.add_argument("--live-tracker-iterations", type=int, default=3)
    parser.add_argument("--live-tracker-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--live-tracker-max-runtime-s", type=float, default=60.0)
    parser.add_argument("--live-tracker-max-poll-runtime-s", type=float, default=30.0)
    parser.add_argument("--live-tracker-max-wallets", type=int, default=5)
    parser.add_argument("--live-tracker-parallel-wallet-fetches", type=int, default=4)
    parser.add_argument("--live-tracker-paper-retain-orders", type=int, default=1_000)
    parser.add_argument("--live-tracker-paper-retain-lifecycle-events", type=int, default=3_000)
    parser.add_argument("--live-tracker-paper-retain-dedupe-ids", type=int, default=250_000)
    parser.add_argument("--skip-registry-sweep", action="store_true")
    parser.add_argument("--registry-sweep-state", default="data/research/wallet_copy_registry_sweep_live_tracking_state.json")
    parser.add_argument("--registry-sweep-event-log", default="data/research/wallet_copy_registry_sweep_live_tracking_events.jsonl")
    parser.add_argument("--registry-sweep-paper-state", default="data/research/wallet_copy_registry_sweep_paper_state.json")
    parser.add_argument("--registry-sweep-paper-event-log", default="data/research/wallet_copy_registry_sweep_paper_events.jsonl")
    parser.add_argument(
        "--registry-sweep-tracker-time-replay-paper-state",
        default="data/research/wallet_copy_registry_sweep_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--registry-sweep-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_registry_sweep_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--registry-sweep-all-order-paper-state",
        default="data/research/wallet_copy_registry_sweep_all_order_paper_state.json",
    )
    parser.add_argument(
        "--registry-sweep-all-order-paper-event-log",
        default="data/research/wallet_copy_registry_sweep_all_order_paper_events.jsonl",
    )
    parser.add_argument(
        "--registry-sweep-all-order-tactic-replay-paper-state",
        default="data/research/wallet_copy_registry_sweep_all_order_tactic_replay_paper_state.json",
    )
    parser.add_argument(
        "--registry-sweep-all-order-tactic-replay-paper-event-log",
        default="data/research/wallet_copy_registry_sweep_all_order_tactic_replay_paper_events.jsonl",
    )
    parser.add_argument("--registry-sweep-limit", type=int, default=20)
    parser.add_argument("--registry-sweep-pages", type=int, default=1)
    parser.add_argument("--registry-sweep-iterations", type=int, default=1)
    parser.add_argument("--registry-sweep-max-wallets", type=int, default=8)
    parser.add_argument("--registry-sweep-parallel-wallet-fetches", type=int, default=4)
    parser.add_argument("--registry-sweep-max-runtime-s", type=float, default=300.0)
    parser.add_argument("--registry-sweep-max-poll-runtime-s", type=float, default=25.0)
    parser.add_argument("--candidate-forward-probe-ranks", type=int, default=12)
    parser.add_argument("--candidate-forward-probe-iterations", type=int, default=240)
    parser.add_argument("--candidate-forward-probe-max-runtime-s", type=float, default=90.0)
    parser.add_argument("--candidate-forward-probe-max-poll-runtime-s", type=float, default=20.0)
    parser.add_argument(
        "--disable-live-ready-unlock",
        action="store_true",
        help="Disable the focused proof-led lane that prioritizes current live-ready blockers before broad sweeps.",
    )
    parser.add_argument(
        "--live-ready-unlock-probe-ranks",
        type=int,
        default=2,
        help=(
            "Candidate-forward ranks to measure in the short focused live-ready unlock lane. "
            "Deferred ranks stay visible as limit pressure; broad backup coverage is a separate paper-research step."
        ),
    )
    parser.add_argument("--live-ready-unlock-min-proof-events", type=int, default=10)
    parser.add_argument("--live-ready-unlock-min-proof-windows", type=int, default=3)
    parser.add_argument("--live-ready-unlock-max-proof-age-p95-s", type=float, default=10.0)
    parser.add_argument("--active-hotlane-max-wallets", type=int, default=32)
    parser.add_argument("--active-hotlane-recent-window-s", type=float, default=1800.0)
    parser.add_argument("--active-hotlane-iterations", type=int, default=3)
    parser.add_argument("--active-hotlane-max-poll-runtime-s", type=float, default=30.0)
    parser.add_argument("--active-hotlane-ticks", type=int, default=4)
    parser.add_argument("--active-hotlane-tick-slice-ticks", type=int, default=1)
    parser.add_argument("--active-hotlane-wallets-per-tick", type=int, default=8)
    parser.add_argument("--active-hotlane-parallel-wallet-fetches", type=int, default=8)
    parser.add_argument("--active-hotlane-tick-gap-s", type=float, default=0.2)
    parser.add_argument("--active-hotlane-tick-state", default="data/research/wallet_copy_hotlane_tick_state.json")
    parser.add_argument("--active-hotlane-guard-log", default="data/research/wallet_copy_active_hotlane_guard.out")
    parser.add_argument("--active-hotlane-guard-log-max-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--active-hotlane-guard-log-keep-tail-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument(
        "--runtime-log-rotation-state",
        default="data/research/wallet_copy_runtime_log_rotation_state.json",
    )
    parser.add_argument(
        "--runtime-log-rotation-event-log",
        default="data/research/wallet_copy_runtime_log_rotation_events.jsonl",
    )
    parser.add_argument("--source-route-state", default="data/research/wallet_copy_source_route_state.json")
    parser.add_argument(
        "--canonical-gamma-resolution-summary",
        default="data/research/wallet_copy_gamma_resolution_refresh_summary.json",
    )
    parser.add_argument("--canonical-gamma-resolution-max-windows", type=int, default=120)
    parser.add_argument("--canonical-gamma-resolution-sleep-s", type=float, default=0.02)
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--gamma-timeout-s", type=float, default=1.5)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.5)
    parser.add_argument("--slippage-bps", type=float, default=500.0)
    parser.add_argument("--min-wr-pct", type=float, default=70.0)
    parser.add_argument("--live-target-min-resolved-orders", type=int, default=100)
    parser.add_argument("--live-target-min-unique-windows", type=int, default=10)
    parser.add_argument("--live-target-min-avg-orders-per-window", type=float, default=2.0)
    parser.add_argument("--live-target-min-wr-pct", type=float, default=70.0)
    parser.add_argument("--live-target-min-validation-wr-pct", type=float, default=70.0)
    parser.add_argument("--live-target-min-roi-pct", type=float, default=2.0)
    parser.add_argument("--policy-preset", choices=("fast", "default"), default="fast")
    parser.add_argument("--max-wallets-for-search", type=int, default=0)
    parser.add_argument("--max-single-wallet-candidate-intents", type=int, default=0)
    parser.add_argument("--max-multi-wallet-base-intents", type=int, default=0)
    parser.add_argument("--command-timeout-s", type=float, default=240.0)
    parser.add_argument(
        "--max-wall-runtime-s",
        type=float,
        default=0.0,
        help=(
            "Hard wall-clock budget for one autonomous repair process. "
            "Default 0 uses --command-timeout-s so heartbeat runs do not sit in an unbounded command chain."
        ),
    )
    parser.add_argument("--force-leaderboard", action="store_true")
    parser.add_argument("--force-research", action="store_true")
    parser.add_argument("--force-tracker", action="store_true")
    parser.add_argument("--skip-live-tracker", action="store_true")
    parser.add_argument("--skip-active-hotlane", action="store_true")
    parser.add_argument("--skip-adaptive-bot", action="store_true")
    parser.add_argument("--deep-research", action="store_true")
    parser.add_argument("--no-append-post-audit-feedback", action="store_true")
    parser.add_argument("--lock-file", default="data/research/wallet_copy_autonomous_repair.lock")
    return parser.parse_args()


def _acquire_single_instance_lock(lock_file: str) -> tuple[Any, bool, dict[str, Any]]:
    """Acquire the autonomous repair lock without blocking.

    A skipped duplicate is safer than a second full pipeline tree mutating the
    same state files and consuming hundreds of MB of memory.
    """

    path = Path(lock_file)
    if not path.is_absolute():
        path = ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.seek(0)
        try:
            holder = json.loads(handle.read() or "{}")
        except json.JSONDecodeError:
            holder = {"raw_lock_file": path.read_text(errors="replace")[:1000]}
        return handle, False, holder if isinstance(holder, dict) else {}
    holder = {
        "pid": os.getpid(),
        "run_id": RUN_ID,
        "started_at": utc_now_iso(),
        "argv": sys.argv,
        "lock_file": str(path.relative_to(ROOT) if path.is_relative_to(ROOT) else path),
    }
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps(holder, sort_keys=True))
    handle.flush()
    os.fsync(handle.fileno())
    return handle, True, holder


def _release_single_instance_lock(handle: Any) -> None:
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps({"released_at": utc_now_iso(), "run_id": RUN_ID}, sort_keys=True))
        handle.flush()
        os.fsync(handle.fileno())
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _write_lock_conflict_state(args: argparse.Namespace, holder: dict[str, Any]) -> None:
    previous_state = load_json(args.state)
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_autonomous_repair_state",
        "generated_at": utc_now_iso(),
        "mission_contract": mission_contract(),
        "mission_contract_check": mission_contract_check(),
        "status": "BUG_SUSPECT",
        "paper_only": True,
        "live_orders_allowed": False,
        "blockers": ["duplicate_autonomous_repair_already_running"],
        "running_instance": holder,
        "progress_action": {
            "status": "OPEN",
            "type": "safe_fix",
            "severity": "P1",
            "area": "wallet-copy runtime process hygiene",
            "file": "scripts/run_wallet_copy_autonomous_repair.py",
            "function": "main/_acquire_single_instance_lock",
            "action": "do not start a second autonomous repair while another run owns the lock; inspect the existing PID if progress stalls",
            "blockers": ["duplicate_autonomous_repair_already_running"],
            "next_command": "ps -axo pid,ppid,etime,rss,command | rg 'run_wallet_copy_autonomous_repair|run_wallet_copy_pipeline_resume|run_wallet_copy_pipeline'",
            "verify": "python3 -m pytest -q tests/test_wallet_copy_core.py -k single_instance_lock",
        },
    }
    preserved_keys = [
        "green_semantics",
        "live_readiness_report",
        "strategy_direction",
        "source_route",
        "source_route_status",
        "limit_pressure",
        "post_audit_summary",
        "commands",
    ]
    for key in preserved_keys:
        if key in previous_state:
            payload[key] = copy.deepcopy(previous_state[key])
    if previous_state:
        payload["previous_state_preserved"] = True
        payload["previous_state_status"] = previous_state.get("status")
        payload["previous_state_generated_at"] = previous_state.get("generated_at")
    else:
        payload["previous_state_preserved"] = False
    atomic_write_json(args.state, payload, compact=True)


def _detect_foreign_wallet_copy_writers(*, ps_output: str | None = None) -> list[dict[str, Any]]:
    """Return wallet-copy state writers that are already active outside this process.

    The flock prevents duplicate autonomous repair parents, but a timed-out or
    manually launched profit/pipeline writer can still mutate the same
    source-of-truth state files. Treating that as process-hygiene evidence is
    safer than starting another repair tree.
    """

    if ps_output is None:
        try:
            completed = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,etime=,command="],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=3.0,
                check=False,
            )
        except Exception:
            return []
        ps_output = completed.stdout or ""

    current_pid = os.getpid()
    writers: list[dict[str, Any]] = []
    for line in ps_output.splitlines():
        parts = line.strip().split(maxsplit=3)
        if len(parts) < 4:
            continue
        pid_raw, ppid_raw, etime, command = parts
        try:
            pid = int(pid_raw)
            ppid = int(ppid_raw)
        except ValueError:
            continue
        if pid == current_pid:
            continue
        matched_script = next((script for script in FOREIGN_WALLET_COPY_WRITER_SCRIPTS if script in command), "")
        if not matched_script:
            continue
        writers.append(
            {
                "pid": pid,
                "ppid": ppid,
                "etime": etime,
                "script": matched_script,
                "command": command[:500],
            }
        )
    return writers


def _write_foreign_writer_conflict_state(args: argparse.Namespace, writers: list[dict[str, Any]]) -> None:
    previous_state = load_json(args.state)
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_autonomous_repair_state",
        "generated_at": utc_now_iso(),
        "mission_contract": mission_contract(),
        "mission_contract_check": mission_contract_check(),
        "status": "BUG_SUSPECT",
        "paper_only": True,
        "live_orders_allowed": False,
        "blockers": ["foreign_wallet_copy_state_writer_already_running"],
        "running_wallet_copy_writers": writers,
        "progress_action": {
            "status": "OPEN",
            "type": "safe_fix",
            "severity": "P1",
            "area": "wallet-copy runtime process hygiene",
            "file": "scripts/run_wallet_copy_autonomous_repair.py",
            "function": "_detect_foreign_wallet_copy_writers/_main_locked",
            "action": (
                "block autonomous repair startup while another wallet-copy profit or pipeline writer is active; "
                "let the existing writer finish or inspect it before launching another state mutation tree"
            ),
            "blockers": ["foreign_wallet_copy_state_writer_already_running"],
            "next_command": (
                "ps -axo pid,ppid,etime,rss,command | rg "
                "'run_wallet_copy_profit_engine|run_wallet_copy_pipeline_resume|run_wallet_copy_pipeline'"
            ),
            "verify": "python3 -m pytest -q tests/test_wallet_copy_core.py -k foreign_writer",
        },
    }
    preserved_keys = [
        "green_semantics",
        "live_readiness_report",
        "strategy_direction",
        "source_route",
        "source_route_status",
        "limit_pressure",
        "post_audit_summary",
        "commands",
    ]
    for key in preserved_keys:
        if key in previous_state:
            payload[key] = copy.deepcopy(previous_state[key])
    if previous_state:
        payload["previous_state_preserved"] = True
        payload["previous_state_status"] = previous_state.get("status")
        payload["previous_state_generated_at"] = previous_state.get("generated_at")
    else:
        payload["previous_state_preserved"] = False
    atomic_write_json(args.state, payload, compact=True)


def _hot_path_trade_query_keys(args: argparse.Namespace) -> str:
    keys = str(getattr(args, "hot_path_data_api_trade_query_keys", "") or "user,proxyWallet").strip()
    if keys == "user":
        return "user,proxyWallet"
    return keys or "user,proxyWallet"


def _source_route_state_path_from_tracker_argv(argv: list[str]) -> str:
    if "--source-route-state" in argv:
        try:
            value = str(argv[argv.index("--source-route-state") + 1] or "")
        except (ValueError, IndexError):
            value = ""
        if value:
            return value
    return DEFAULT_SOURCE_ROUTE_STATE


def _approved_source_route_env_overrides(source_route_state: str) -> dict[str, str]:
    route = load_json(source_route_state, default={})
    if not isinstance(route, dict) or not source_route_allows_live_execution(route):
        return {}
    overrides = route.get("source_base_overrides")
    if not isinstance(overrides, dict):
        return {}
    env: dict[str, str] = {}
    for override in overrides.values():
        if not isinstance(override, dict) or not override.get("configured"):
            continue
        env_var = str(override.get("env_var") or "").strip()
        base_url = str(override.get("base_url") or "").strip()
        if env_var and base_url:
            env[env_var] = base_url
    return env


def _augment_admission_tracker_source_route(argv: list[str]) -> tuple[list[str], dict[str, str]]:
    """Keep autonomous admission probes on an approved recovered source route.

    ``run_wallet_live_tracker.py`` clears Polymarket base overrides in admission
    mode by default. That is correct unless the source-route state has already
    been explicitly measured and approved for live execution. Without this
    augmentation, autonomous repair probes can falsely report no wallet activity
    because they fall back to a direct route that the route probe already marked
    as reset/suppressed.
    """

    augmented = list(argv)
    if "scripts/run_wallet_live_tracker.py" not in augmented or "--admission-mode" not in augmented:
        return augmented, {}
    env = _approved_source_route_env_overrides(_source_route_state_path_from_tracker_argv(augmented))
    if env and "--allow-source-base-overrides-in-admission" not in augmented:
        augmented.append("--allow-source-base-overrides-in-admission")
    return augmented, env


def _run_command(
    name: str,
    purpose: str,
    argv: list[str],
    *,
    acceptable_returncodes: tuple[int, ...] = (0,),
    timeout_s: float = 240.0,
    progress_state: Path | None = COMMAND_PROGRESS_STATE,
    progress_kind: str = "wallet_copy_autonomous_repair_command_progress",
) -> dict[str, Any]:
    argv, env_overrides = _augment_admission_tracker_source_route(argv)
    started = time.time()
    log_dir = COMMAND_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    command_slug = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)[:80] or "command"
    log_prefix = f"{int(started)}_{os.getpid()}_{command_slug}"
    stdout_path = log_dir / f"{log_prefix}.stdout.log"
    stderr_path = log_dir / f"{log_prefix}.stderr.log"

    def _write_progress(
        status: str,
        *,
        process: subprocess.Popen[str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "schema_version": 1,
            "kind": progress_kind,
            "run_id": RUN_ID,
            "generated_at": utc_now_iso(),
            "status": status,
            "name": name,
            "purpose": purpose,
            "argv": argv,
            "pid": process.pid if process is not None else None,
            "elapsed_s": round(time.time() - started, 3),
            "timeout_s": round(float(timeout_s), 3),
            "stdout_log": str(stdout_path.relative_to(ROOT)),
            "stderr_log": str(stderr_path.relative_to(ROOT)),
        }
        if extra:
            payload.update(extra)
        if progress_state is not None:
            atomic_write_json(progress_state, payload, compact=True)

    def _tail(path: Path, *, max_chars: int = 4000) -> str:
        try:
            with path.open("rb") as fh:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                fh.seek(max(0, size - max_chars))
                return fh.read().decode("utf-8", errors="replace")[-max_chars:]
        except FileNotFoundError:
            return ""

    returncode = 1
    timed_out = False
    _write_progress("STARTING")
    try:
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        env.update(env_overrides)
        with stdout_path.open("w", encoding="utf-8") as stdout_fh, stderr_path.open("w", encoding="utf-8") as stderr_fh:
            process = subprocess.Popen(
                argv,
                cwd=ROOT,
                text=True,
                stdout=stdout_fh,
                stderr=stderr_fh,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env=env,
            )
            ACTIVE_CHILD_PROCESS_GROUPS.add(process.pid)
            _write_progress("RUNNING", process=process)
            deadline = started + max(1.0, float(timeout_s))
            next_heartbeat = started + 5.0
            while True:
                returncode = process.poll()
                now = time.time()
                if returncode is not None:
                    break
                if now >= deadline:
                    timed_out = True
                    returncode = 124
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=5.0)
                    break
                if now >= next_heartbeat:
                    _write_progress(
                        "RUNNING",
                        process=process,
                        extra={
                            "stdout_tail": _tail(stdout_path, max_chars=1200),
                            "stderr_tail": _tail(stderr_path, max_chars=1200),
                        },
                    )
                    next_heartbeat = now + 5.0
                time.sleep(0.25)
            ACTIVE_CHILD_PROCESS_GROUPS.discard(process.pid)
    except Exception as exc:
        returncode = 1
        with stderr_path.open("a", encoding="utf-8") as stderr_fh:
            stderr_fh.write(f"\ncommand runner error: {type(exc).__name__}: {exc}\n")
    finally:
        if "process" in locals():
            ACTIVE_CHILD_PROCESS_GROUPS.discard(process.pid)
    stdout = _tail(stdout_path)
    stderr = _tail(stderr_path)
    if timed_out:
        returncode = 124
        stderr = (stderr + "\n" if stderr else "") + f"timeout after {timeout_s}s"
        with stderr_path.open("a", encoding="utf-8") as stderr_fh:
            stderr_fh.write(f"\ntimeout after {timeout_s}s\n")
    result = {
        "name": name,
        "purpose": purpose,
        "argv": argv,
        "returncode": returncode,
        "timeout_s": round(float(timeout_s), 3),
        "acceptable_returncodes": list(acceptable_returncodes),
        "ok": returncode in acceptable_returncodes,
        "duration_s": round(time.time() - started, 3),
        "stdout_tail": stdout[-4000:],
        "stderr_tail": stderr[-4000:],
        "stdout_log": str(stdout_path.relative_to(ROOT)),
        "stderr_log": str(stderr_path.relative_to(ROOT)),
        "timed_out": timed_out,
    }
    _write_progress(
        "COMPLETE" if result["ok"] else "FAILED",
        extra={
            "returncode": returncode,
            "ok": result["ok"],
            "duration_s": result["duration_s"],
            "timed_out": timed_out,
            "stdout_tail": result["stdout_tail"][-1200:],
            "stderr_tail": result["stderr_tail"][-1200:],
        },
    )
    return result


def _attach_command_metadata(result: dict[str, Any], command: dict[str, Any]) -> dict[str, Any]:
    """Keep scope/runtime limits visible in persisted command results."""

    for key in ("argv", "runtime_budget", "candidate_forward", "queue_rank"):
        if command.get(key) is not None:
            result[key] = command.get(key)
    return result


def _compact_command_summary(row: dict[str, Any]) -> dict[str, Any]:
    """Preserve the evidence needed to distinguish timeouts from bounded work."""

    summary: dict[str, Any] = {
        "name": row.get("name"),
        "returncode": row.get("returncode"),
        "ok": row.get("ok"),
    }
    for key in (
        "timeout_s",
        "timed_out",
        "duration_s",
        "skipped",
        "skip_reason",
        "stdout_log",
        "stderr_log",
    ):
        if row.get(key) is not None:
            summary[key] = row.get(key)
    runtime_budget = row.get("runtime_budget") if isinstance(row.get("runtime_budget"), dict) else {}
    if runtime_budget:
        summary["runtime_budget"] = {
            key: runtime_budget.get(key)
            for key in (
                "scope_mode",
                "status",
                "child_command_timeout_s",
                "effective_child_max_runtime_s",
                "effective_poll_max_runtime_s",
                "max_wall_runtime_s",
                "remaining_s",
                "required_remaining_s",
                "wall_runtime_budget_s",
            )
            if runtime_budget.get(key) is not None
        }
    return summary


def _child_runtime_within_command_timeout(
    requested_runtime_s: float,
    command_timeout_s: float,
    *,
    min_runtime_s: float = 30.0,
    reserve_s: float = 15.0,
) -> float:
    """Return a child --max-runtime-s that can finish before subprocess timeout.

    Some tracker commands enforce their own max runtime. If that child runtime is
    longer than the parent subprocess timeout, the autonomous repair loop reports
    a command failure even though the tracker is behaving as configured.
    """
    requested = max(1.0, float(requested_runtime_s))
    timeout_budget = max(1.0, float(command_timeout_s) - max(0.0, float(reserve_s)))
    floor = min(max(1.0, float(min_runtime_s)), timeout_budget)
    return round(max(floor, min(requested, timeout_budget)), 3)


def _wall_runtime_budget_s(args: argparse.Namespace) -> float:
    configured = float(getattr(args, "max_wall_runtime_s", 0.0) or 0.0)
    if configured > 0:
        return max(1.0, configured)
    return max(1.0, float(getattr(args, "command_timeout_s", 240.0)))


def _wall_runtime_remaining_s(started: float, budget_s: float, *, reserve_s: float = 5.0) -> float:
    return float(budget_s) - (time.time() - float(started)) - max(0.0, float(reserve_s))


def _wall_runtime_exhausted(started: float, budget_s: float, *, min_remaining_s: float = 5.0) -> bool:
    return _wall_runtime_remaining_s(started, budget_s, reserve_s=0.0) <= max(0.0, float(min_remaining_s))


def _wall_runtime_command_timeout(
    started: float,
    budget_s: float,
    *,
    requested_s: float,
    reserve_s: float = 5.0,
    min_timeout_s: float = 1.0,
) -> float:
    remaining = _wall_runtime_remaining_s(started, budget_s, reserve_s=reserve_s)
    capped = min(float(requested_s), max(float(min_timeout_s), remaining))
    return round(max(float(min_timeout_s), capped), 3)


def _wall_runtime_exhausted_result(
    *,
    name: str,
    purpose: str,
    started: float,
    budget_s: float,
) -> dict[str, Any]:
    elapsed_s = round(time.time() - float(started), 3)
    return {
        "name": name,
        "purpose": purpose,
        "returncode": 124,
        "timeout_s": round(float(budget_s), 3),
        "acceptable_returncodes": [0],
        "ok": False,
        "duration_s": elapsed_s,
        "stdout_tail": "",
        "stderr_tail": f"wall runtime budget exhausted after {elapsed_s}s",
        "timed_out": True,
        "wall_runtime_budget_exhausted": True,
        "runtime_budget": {
            "scope_mode": "AUTONOMOUS_REPAIR_WALL_CLOCK",
            "status": "WALL_RUNTIME_BUDGET_EXHAUSTED",
            "elapsed_s": elapsed_s,
            "wall_runtime_budget_s": round(float(budget_s), 3),
            "next_step": (
                "split the remaining proof work into smaller resumable slices or increase the wall budget; "
                "do not mark missing wallet-copy evidence green"
            ),
        },
    }


def _wall_runtime_deferred_result(
    *,
    name: str,
    purpose: str,
    started: float,
    budget_s: float,
    required_remaining_s: float,
) -> dict[str, Any]:
    elapsed_s = round(time.time() - float(started), 3)
    remaining_s = round(max(0.0, float(budget_s) - (time.time() - float(started))), 3)
    return {
        "name": name,
        "purpose": purpose,
        "returncode": 2,
        "acceptable_returncodes": [0, 2],
        "ok": True,
        "duration_s": 0.0,
        "stdout_tail": "",
        "stderr_tail": "deferred low wall budget; blocker evidence, not process-green success",
        "skipped": True,
        "skip_reason": "insufficient_wall_runtime_for_bounded_child",
        "runtime_budget": {
            "scope_mode": "AUTONOMOUS_REPAIR_WALL_CLOCK",
            "status": "DEFERRED_LOW_WALL_BUDGET",
            "elapsed_s": elapsed_s,
            "remaining_s": remaining_s,
            "required_remaining_s": round(float(required_remaining_s), 3),
            "wall_runtime_budget_s": round(float(budget_s), 3),
            "next_step": (
                "run this proof step in the next resumable heartbeat or increase the wall budget; "
                "do not spend the current heartbeat on a timeout-prone child"
            ),
        },
    }


def _requested_timeout_for_command(args: argparse.Namespace, command: dict[str, Any]) -> float:
    """Bound subprocess time to the command's own measurement budget when known."""

    command_timeout_s = max(1.0, float(getattr(args, "command_timeout_s", 240.0)))
    name = str(command.get("name") or "")
    if name.startswith("profit_admission"):
        cap_s = 180.0 if bool(getattr(args, "deep_research", False)) else 30.0
        return round(min(command_timeout_s, cap_s), 3)
    runtime_budget = command.get("runtime_budget") if isinstance(command.get("runtime_budget"), dict) else {}
    child_timeout = runtime_budget.get("child_command_timeout_s")
    if child_timeout is not None:
        return round(min(command_timeout_s, max(1.0, float(child_timeout))), 3)
    child_runtime = runtime_budget.get("effective_child_max_runtime_s")
    if child_runtime is not None:
        return round(min(command_timeout_s, max(1.0, float(child_runtime) + 5.0)), 3)
    return round(command_timeout_s, 3)


def _required_remaining_s_for_command(args: argparse.Namespace, command: dict[str, Any]) -> float | None:
    """Wall-clock budget a timeout-prone child needs before it is useful to launch."""

    name = str(command.get("name") or "")
    runtime_budget = command.get("runtime_budget") if isinstance(command.get("runtime_budget"), dict) else {}
    required_remaining_s: float | None = None
    child_timeout = runtime_budget.get("child_command_timeout_s")
    if child_timeout is not None:
        required_remaining_s = max(required_remaining_s or 0.0, float(child_timeout) + 5.0)
    child_runtime = runtime_budget.get("effective_child_max_runtime_s")
    if child_runtime is not None:
        required_remaining_s = max(required_remaining_s or 0.0, float(child_runtime) + 5.0)
    if name.startswith("profit_admission"):
        required_remaining_s = max(
            required_remaining_s or 0.0,
            _requested_timeout_for_command(args, command) + 5.0,
        )
    return required_remaining_s


def _defer_if_insufficient_wall_runtime(
    args: argparse.Namespace,
    command: dict[str, Any],
    *,
    started: float,
    budget_s: float,
) -> dict[str, Any] | None:
    """Return a measured deferral instead of launching a child that cannot finish."""

    name = str(command.get("name") or "")
    required_remaining_s = _required_remaining_s_for_command(args, command)
    if required_remaining_s is None:
        return None
    remaining_s = _wall_runtime_remaining_s(started, budget_s, reserve_s=0.0)
    if remaining_s >= required_remaining_s:
        return None
    return _wall_runtime_deferred_result(
        name=name or "bounded_child_measurement",
        purpose=str(command.get("purpose") or "defer timeout-prone child measurement"),
        started=started,
        budget_s=budget_s,
        required_remaining_s=required_remaining_s,
    )


def _defer_candidate_forward_if_profit_refresh_would_be_starved(
    args: argparse.Namespace,
    command: dict[str, Any],
    *,
    started: float,
    budget_s: float,
    profit_refresh_required_s: float,
) -> dict[str, Any] | None:
    """Keep candidate-forward probes from consuming the post-tracker admission refresh budget."""

    child_required_s = _required_remaining_s_for_command(args, command)
    if child_required_s is None or profit_refresh_required_s <= 0:
        return None
    required_with_refresh_s = float(child_required_s) + float(profit_refresh_required_s)
    remaining_s = _wall_runtime_remaining_s(started, budget_s, reserve_s=0.0)
    if remaining_s >= required_with_refresh_s:
        return None
    result = _wall_runtime_deferred_result(
        name=str(command.get("name") or "candidate_forward_tracker_measurement"),
        purpose=str(
            command.get("purpose")
            or "defer candidate-forward rank probe so profit admission can consume existing fresh tracker evidence"
        ),
        started=started,
        budget_s=budget_s,
        required_remaining_s=required_with_refresh_s,
    )
    result["skip_reason"] = "insufficient_wall_runtime_preserve_profit_admission_after_tracker"
    runtime_budget = dict(result.get("runtime_budget") or {})
    runtime_budget.update(
        {
            "status": "DEFERRED_TO_PRESERVE_PROFIT_ADMISSION_AFTER_TRACKER",
            "candidate_forward_required_remaining_s": round(float(child_required_s), 3),
            "reserved_profit_admission_after_tracker_s": round(float(profit_refresh_required_s), 3),
            "next_step": (
                "run the deferred forward-rank paper probe after profit_admission_after_tracker has consumed "
                "the fresh canonical tracker evidence"
            ),
        }
    )
    result["runtime_budget"] = runtime_budget
    return result


def _poll_runtime_within_child_runtime(
    requested_poll_runtime_s: float,
    child_runtime_s: float,
    iterations: int,
    *,
    reserve_per_iteration_s: float = 1.0,
) -> float:
    """Bound one poll so repeated iterations can exit within child runtime."""
    requested = max(1.0, float(requested_poll_runtime_s))
    per_iteration_budget = max(1.0, float(child_runtime_s) / max(1, int(iterations)))
    capped = min(requested, max(1.0, per_iteration_budget - max(0.0, float(reserve_per_iteration_s))))
    return round(capped, 3)


def _run_audit(*, append_feedback: bool, timeout_s: float) -> dict[str, Any]:
    argv = [PYTHON, "scripts/audit_wallet_copy_learning_logs.py"]
    if not append_feedback:
        argv.append("--no-append-feedback-log")
    result = _run_command(
        "wallet_copy_learning_audit",
        "refresh source-of-truth learning audit and feedback-loop status",
        argv,
        timeout_s=timeout_s,
    )
    payload = load_json("data/research/wallet_copy_learning_log_audit_state.json", default={})
    if not isinstance(payload, dict):
        payload = {}
    payload["_command_result"] = result
    return payload


def _runtime_log_rotation_command(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "name": "runtime_log_rotation",
        "purpose": "bound verbose active hot-lane guard stdout while preserving structured JSONL learning evidence",
        "acceptable_returncodes": (0,),
        "argv": [
            PYTHON,
            "scripts/rotate_wallet_copy_runtime_logs.py",
            "--log",
            str(args.active_hotlane_guard_log),
            "--state",
            str(args.runtime_log_rotation_state),
            "--event-log",
            str(args.runtime_log_rotation_event_log),
            "--max-bytes",
            str(int(args.active_hotlane_guard_log_max_bytes)),
            "--keep-tail-bytes",
            str(int(args.active_hotlane_guard_log_keep_tail_bytes)),
        ],
    }


def _wallet_analysis_report_command(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "name": "wallet_analysis_report",
        "purpose": "refresh per-wallet paper/copyability/onchain report before strategy-direction selection",
        "acceptable_returncodes": (0,),
        "argv": [
            PYTHON,
            "scripts/report_wallet_copy_wallets.py",
            "--top",
            "20",
            "--print-mode",
            "summary",
            "--output",
            str(getattr(args, "wallet_analysis_state", "data/research/wallet_copy_wallet_analysis_state.json")),
        ],
    }


def _strategy_direction_command(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "name": "strategy_direction_selection",
        "purpose": (
            "rank single-wallet copy, all-order multi-wallet copy, weighted inventory, and multi-wallet filter "
            "from current source-of-truth evidence"
        ),
        "acceptable_returncodes": (0,),
        "argv": [
            PYTHON,
            "scripts/select_wallet_copy_strategy_direction.py",
            "--wallet-analysis-state",
            str(getattr(args, "wallet_analysis_state", "data/research/wallet_copy_wallet_analysis_state.json")),
            "--profit-state",
            str(args.profit_state),
            "--active-hotlane-state",
            str(getattr(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json")),
            "--active-tracking-state",
            str(
                getattr(
                    args,
                    "active_hotlane_live_tracker_state",
                    "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
                )
            ),
            "--candidate-runtime-proof-index",
            str(getattr(args, "candidate_runtime_proof_index", "data/research/wallet_copy_candidate_runtime_proof_index.json")),
            "--source-route-state",
            str(getattr(args, "source_route_state", "data/research/wallet_copy_source_route_state.json")),
            "--leaderboard-state",
            str(getattr(args, "leaderboard_state", "data/research/wallet_copy_leaderboard_crypto_state.json")),
            "--output",
            str(getattr(args, "strategy_direction_state", "data/research/wallet_copy_strategy_direction_state.json")),
        ],
    }


def _relaxed_copyability_report_command(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "name": "relaxed_copyability_shadow_report",
        "purpose": (
            "paper-only shadow measurement of age/slippage copyability profiles; "
            "never relaxes live admission and quantifies whether looser profiles would help"
        ),
        "acceptable_returncodes": (0, 2),
        "argv": [
            PYTHON,
            "scripts/report_wallet_copy_relaxed_copyability.py",
            "--output",
            str(
                getattr(
                    args,
                    "relaxed_copyability_report_state",
                    "data/research/wallet_copy_relaxed_copyability_report.json",
                )
            ),
        ],
    }


def _source_route_probe_command(args: argparse.Namespace) -> dict[str, Any]:
    command_timeout_s = float(getattr(args, "command_timeout_s", 240.0))
    max_probe_wall_s = min(180.0, max(150.0, command_timeout_s - 60.0))
    max_probe_wall_s = min(max_probe_wall_s, max(30.0, command_timeout_s - 10.0))
    source_route_timeout_s = min(max(float(getattr(args, "data_api_timeout_s", 2.0)), 24.0), 30.0)
    return {
        "name": "source_route_probe",
        "purpose": (
            "measure direct Polymarket Data API/Gamma/CLOB route health separately from general internet health; "
            "non-PASS keeps live admission blocked and visible"
        ),
        "acceptable_returncodes": (0, 2),
        "argv": [
            PYTHON,
            "scripts/probe_polymarket_source_routes.py",
            "--output",
            str(getattr(args, "source_route_state", "data/research/wallet_copy_source_route_state.json")),
            "--timeout-s",
            str(source_route_timeout_s),
            "--probe-profile",
            "heartbeat",
            "--max-wall-runtime-s",
            str(round(max_probe_wall_s, 3)),
        ],
    }


def _source_route_fresh_probe_blocked_heavy_work_result(
    args: argparse.Namespace,
    *,
    name: str = "source_route_fresh_probe_blocked_heavy_work",
    purpose: str = "stop expensive wallet-copy repair stages after the fresh source-route probe still blocks measurement",
) -> dict[str, Any] | None:
    if not hasattr(args, "source_route_state"):
        return None
    state = load_json(getattr(args, "source_route_state", ""), default={})
    if not isinstance(state, dict):
        state = {}
    status = str(state.get("status") or "SOURCE_ROUTE_UNKNOWN")
    if source_route_allows_measurement(status):
        return None
    return {
        "name": name,
        "purpose": purpose,
        "ok": True,
        "returncode": 0,
        "skipped": True,
        "skip_reason": "fresh_source_route_probe_blocks_heavy_work",
        "source_route_status": status,
        "source_route_state": str(getattr(args, "source_route_state", "")),
        "verification_command": (
            f"{PYTHON} scripts/probe_polymarket_source_routes.py --output "
            f"{getattr(args, 'source_route_state', 'data/research/wallet_copy_source_route_state.json')} "
            "--timeout-s 24 --probe-profile heartbeat --max-wall-runtime-s 180 --print"
        ),
    }


def _final_source_truth_refresh_commands(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Refresh source-route truth before profit/readiness reports consume it."""

    return [
        _source_route_probe_command(args),
        _profit_command(args, name="profit_admission_after_source_route"),
        _wallet_analysis_report_command(args),
        _strategy_direction_command(args),
        _relaxed_copyability_report_command(args),
    ]


def _canonical_gamma_resolution_refresh_command(args: argparse.Namespace) -> dict[str, Any]:
    summary_output = str(
        getattr(
            args,
            "canonical_gamma_resolution_summary",
            "data/research/wallet_copy_gamma_resolution_refresh_summary.json",
        )
    )
    max_wall_runtime_s = min(35.0, max(10.0, float(getattr(args, "command_timeout_s", 240.0)) / 8.0))
    return {
        "name": "canonical_gamma_resolution_refresh",
        "purpose": (
            "replace research-only BTC 5m resolution evidence with Polymarket Gamma canonical outcomes for the "
            "current profit candidates before re-running admission"
        ),
        "acceptable_returncodes": (0, 2),
        "runtime_budget": {
            "scope_mode": "CANONICAL_GAMMA_RESOLUTION_REFRESH",
            "max_wall_runtime_s": round(max_wall_runtime_s, 3),
            "child_command_timeout_s": round(max_wall_runtime_s + 5.0, 3),
            "max_windows": int(getattr(args, "canonical_gamma_resolution_max_windows", 120)),
            "status": "CHECKPOINTED_CANONICAL_RESOLUTION_REFRESH",
        },
        "argv": [
            PYTHON,
            "scripts/refresh_btc_5m_resolutions_from_gamma.py",
            "--profit-state",
            str(args.profit_state),
            "--existing",
            str(args.resolutions),
            "--output",
            str(args.resolutions),
            "--summary-output",
            summary_output,
            "--merge-existing",
            "--max-windows",
            str(int(getattr(args, "canonical_gamma_resolution_max_windows", 120))),
            "--max-wall-runtime-s",
            str(max_wall_runtime_s),
            "--timeout-s",
            str(min(float(getattr(args, "gamma_timeout_s", 1.5)), 5.0)),
            "--sleep-s",
            str(float(getattr(args, "canonical_gamma_resolution_sleep_s", 0.02))),
        ],
    }


def _is_stale(path: str | Path, max_age_s: float) -> bool:
    if max_age_s <= 0:
        return False
    target = Path(path)
    if not target.exists():
        return True
    return time.time() - target.stat().st_mtime > float(max_age_s)


def _active_hotlane_tracker_measurement_reason(path: str | Path, max_age_s: float) -> str | None:
    if _is_stale(path, max_age_s):
        return "stale current-poll truth is refreshed before adaptive tick/candidate probes can age it out"

    state = load_json(path, default={})
    if not isinstance(state, dict):
        return "missing or invalid active hot-lane current-poll truth is refreshed before downstream probes"
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    current_poll = (
        summary.get("current_poll_diagnostics")
        if isinstance(summary.get("current_poll_diagnostics"), dict)
        else {}
    )
    hot_path = summary.get("hot_path_adaptive") if isinstance(summary.get("hot_path_adaptive"), dict) else {}
    all_order = summary.get("all_order_exact_copy") if isinstance(summary.get("all_order_exact_copy"), dict) else {}
    copy_efficiency = (
        summary.get("copy_efficiency")
        if isinstance(summary.get("copy_efficiency"), dict)
        else {}
    )
    poll_runtime = summary.get("poll_runtime") if isinstance(summary.get("poll_runtime"), dict) else {}

    current_poll_blockers = {str(row) for row in current_poll.get("blockers") or []}
    hot_path_blockers = {str(row) for row in hot_path.get("blockers") or []}
    copy_blockers = {str(row) for row in copy_efficiency.get("blockers") or []}
    critical_current_poll_blockers = {
        "current_poll_runtime_limited_before_event_processing",
        "current_poll_source_feed_pre_fetch_stale",
        "current_poll_source_trade_book_timing_unverified",
        "current_poll_source_route_recovered_degraded",
    }
    if current_poll_blockers & critical_current_poll_blockers:
        return "active hot-lane current-poll diagnostics are blocked, so refresh tracker truth before tick/candidate probes"
    if str(current_poll.get("zero_current_poll_root_cause") or "") == "runtime_limited_before_event_processing":
        return "active hot-lane tracker hit runtime limit before event processing, so refresh tracker truth directly"
    if int((current_poll.get("current_poll_ladder") or {}).get("runtime_limited_events_skipped") or 0) > 0:
        return "active hot-lane tracker skipped current-poll events due runtime budget, so refresh tracker truth directly"
    if str(poll_runtime.get("status") or "") == "LIMIT_REACHED":
        return "active hot-lane tracker poll runtime hit its limit, so refresh tracker truth directly"
    if str(hot_path.get("status") or "") in {"FAIL", "CORRECTION"} or "source_feed_delayed" in hot_path_blockers:
        return "active hot-lane hot-path evidence is non-green, so refresh tracker truth before adaptive tick"
    if bool(summary.get("hot_path_source_feed_delayed")):
        return "active hot-lane source feed is delayed, so refresh tracker truth before adaptive tick"
    if str(all_order.get("status") or "") == "FAIL" and str(all_order.get("live_truth_status") or "") == "CORRECTION":
        return "active hot-lane all-order live truth is corrective, so refresh tracker truth before downstream probes"
    if "no_required_buy_copy_evidence" in copy_blockers:
        return "active hot-lane has no required BUY copy evidence, so refresh tracker truth before downstream probes"
    return None


def _check_status(audit: dict[str, Any], check_name: str) -> str:
    checks = audit.get("checks") if isinstance(audit.get("checks"), dict) else {}
    check = checks.get(check_name) if isinstance(checks.get(check_name), dict) else {}
    return str(check.get("status") or "MISSING")


def _missing_paths(audit: dict[str, Any]) -> set[str]:
    summary = audit.get("summary") if isinstance(audit.get("summary"), dict) else {}
    return {str(row) for row in summary.get("missing_paths") or []}


def _source_route_blocks_heavy_work(args: argparse.Namespace) -> bool:
    if not hasattr(args, "source_route_state"):
        return False
    state = load_json(getattr(args, "source_route_state", ""), default={})
    if not isinstance(state, dict):
        status = "SOURCE_ROUTE_UNKNOWN"
    else:
        status = str(state.get("status") or "SOURCE_ROUTE_UNKNOWN")
    return not source_route_allows_measurement(status)


def _profit_state_needs_canonical_gamma_resolution(args: argparse.Namespace) -> bool:
    profit = load_json(getattr(args, "profit_state", ""), default={})
    if not isinstance(profit, dict):
        return False

    blocker_values: list[Any] = []

    def collect_resolution_evidence(value: dict[str, Any]) -> None:
        resolution = value.get("resolution_evidence_summary")
        if not isinstance(resolution, dict):
            return
        if int(resolution.get("research_only_resolved_orders") or 0) > 0:
            blocker_values.append("candidate_research_only_resolution_evidence")
        coverage_status = str(resolution.get("resolution_coverage_status") or "").lower()
        if coverage_status in {
            "pending_newer_than_resolution_index",
            "pending_canonical_resolution_refresh",
            "stale_resolution_index",
        }:
            blocker_values.append(coverage_status)
        if int(resolution.get("newer_than_resolution_index_events") or 0) > 0:
            blocker_values.append("candidate_events_newer_than_resolution_index")
        if int(resolution.get("matured_unresolved_count") or 0) > 0:
            blocker_values.append("candidate_matured_unresolved_resolution_gap")

    for key in ("decision", "live_readiness_certificate", "best_candidate", "forward_candidate"):
        value = profit.get(key)
        if isinstance(value, dict):
            blocker_values.extend(value.get("blockers") or [])
            blocker_values.extend(value.get("live_admission_blockers") or [])
            blocker_values.extend(value.get("reason_codes") or [])
            collect_resolution_evidence(value)
    for row in profit.get("ranked_candidates") or []:
        if not isinstance(row, dict):
            continue
        blocker_values.extend(row.get("blockers") or [])
        collect_resolution_evidence(row)

    text = " ".join(str(item) for item in blocker_values if item).lower()
    return (
        "research_only_resolution" in text
        or "candidate_research_only_resolution_evidence" in text
        or "pending_newer_than_resolution_index" in text
        or "candidate_events_newer_than_resolution_index" in text
        or "candidate_matured_unresolved_resolution_gap" in text
    )


def _profit_command(args: argparse.Namespace, *, name: str = "profit_admission_refresh") -> dict[str, Any]:
    profit_output_state = _profit_command_output_state(args)
    candidate_forward_state = str(
        getattr(
            args,
            "candidate_forward_live_tracker_state",
            "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
        )
    )
    candidate_forward_probe_args: list[str] = []
    rank_count = _candidate_forward_probe_rank_count(args)
    for rank in range(1, rank_count):
        candidate_forward_probe_args.extend(
            [
                "--candidate-forward-probe-live-tracker-state",
                _ranked_state_path(candidate_forward_state, rank),
            ]
        )
    def _search_limit(value: Any) -> int:
        parsed = int(value)
        return 0 if parsed <= 0 else max(1, parsed)

    max_wallets_for_search = 0 if args.deep_research else _search_limit(args.max_wallets_for_search)
    max_single_wallet_candidate_intents = (
        0 if args.deep_research else _search_limit(args.max_single_wallet_candidate_intents)
    )
    max_multi_wallet_base_intents = 0 if args.deep_research else _search_limit(args.max_multi_wallet_base_intents)
    return {
        "name": name,
        "purpose": "refresh raw-baseline guarded profit/admission state after research or tracker evidence",
        "acceptable_returncodes": (0,),
        "argv": [
            PYTHON,
            "scripts/run_wallet_copy_profit_engine.py",
            "--history-state",
            args.history_state,
            "--resolutions",
            args.resolutions,
            "--output",
            profit_output_state,
            "--live-tracker-state",
            args.live_tracker_state,
            "--active-hotlane-state",
            str(getattr(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json")),
            "--active-hotlane-live-tracker-state",
            str(getattr(args, "active_hotlane_live_tracker_state", "data/research/wallet_copy_active_hotlane_live_tracking_state.json")),
            "--candidate-forward-live-tracker-state",
            candidate_forward_state,
            *candidate_forward_probe_args,
            "--candidate-runtime-proof-index",
            str(
                getattr(
                    args,
                    "candidate_runtime_proof_index",
                    "data/research/wallet_copy_candidate_runtime_proof_index.json",
                )
            ),
            "--strategy-direction-state",
            str(getattr(args, "strategy_direction_state", "data/research/wallet_copy_strategy_direction_state.json")),
            "--source-route-state",
            str(getattr(args, "source_route_state", "data/research/wallet_copy_source_route_state.json")),
            "--max-unresolved-ratio",
            str(float(args.max_unresolved_ratio)),
            "--min-wr-pct",
            str(float(getattr(args, "min_wr_pct", 70.0))),
            "--slippage-bps",
            str(float(getattr(args, "slippage_bps", 500.0))),
            "--live-target-min-resolved-orders",
            str(int(getattr(args, "live_target_min_resolved_orders", 100))),
            "--live-target-min-unique-windows",
            str(int(getattr(args, "live_target_min_unique_windows", 10))),
            "--live-target-min-avg-orders-per-window",
            str(float(getattr(args, "live_target_min_avg_orders_per_window", 2.0))),
            "--live-target-min-wr-pct",
            str(float(getattr(args, "live_target_min_wr_pct", 70.0))),
            "--live-target-min-validation-wr-pct",
            str(float(getattr(args, "live_target_min_validation_wr_pct", 70.0))),
            "--live-target-min-roi-pct",
            str(float(getattr(args, "live_target_min_roi_pct", 2.0))),
            "--policy-preset",
            args.policy_preset,
            "--max-wallets-for-search",
            str(max_wallets_for_search),
            "--max-single-wallet-candidate-intents",
            str(max_single_wallet_candidate_intents),
            "--max-multi-wallet-base-intents",
            str(max_multi_wallet_base_intents),
            "--live-today-sprint-operator-approval-id",
            LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID,
            "--forward-tracking-queue-size",
            str(max(12, rank_count)),
        ],
    }


def _profit_command_output_state(args: argparse.Namespace) -> str:
    if bool(getattr(args, "deep_research", False)) and not bool(
        getattr(args, "allow_deep_research_main_profit_write", False)
    ):
        return str(
            getattr(
                args,
                "deep_research_profit_state",
                "data/research/wallet_copy_profit_engine_state_deep_research.json",
            )
        )
    return str(args.profit_state)


def _candidate_forward_specs(args: argparse.Namespace, *, max_specs: int = 3) -> list[dict[str, Any]]:
    profit = load_json(args.profit_state, default={})
    if not isinstance(profit, dict):
        return []
    specs: list[dict[str, str]] = []
    queue = profit.get("forward_tracking_queue") if isinstance(profit.get("forward_tracking_queue"), list) else []
    for row in queue:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").strip()
        policy_id = str(row.get("policy_id") or "")
        if not wallet or not policy_id:
            continue
        specs.append(
            {
                "wallet": wallet,
                "policy_id": policy_id,
                "candidate_type": str(row.get("candidate_type") or ""),
                "candidate_id": str(row.get("candidate_id") or ""),
                "queue_rank": str(row.get("forward_queue_rank") or len(specs)),
                "source": "forward_tracking_queue",
                "candidate": row,
            }
        )
        if len(specs) >= max(1, int(max_specs)):
            return specs
    best = profit.get("best_candidate")
    if isinstance(best, dict):
        metadata = best.get("metadata") if isinstance(best.get("metadata"), dict) else {}
        policy = best.get("policy") if isinstance(best.get("policy"), dict) else {}
        wallet = str(metadata.get("source_wallet") or "").strip()
        policy_id = str(policy.get("policy_id") or "")
        if wallet and policy_id:
            specs.append(
                {
                    "wallet": wallet,
                    "policy_id": policy_id,
                    "candidate_type": str(best.get("candidate_type") or ""),
                    "candidate_id": str(best.get("candidate_id") or ""),
                    "queue_rank": "0",
                    "source": "best_candidate_fallback",
                    "candidate": best,
                }
            )
    return specs[: max(1, int(max_specs))]


def _ranked_state_path(path: str | Path, rank: int) -> str:
    target = Path(path)
    if int(rank) <= 0:
        return str(target)
    return str(target.with_name(f"{target.stem}_rank_{int(rank)}{target.suffix}"))


def _candidate_forward_rank_profit_state(args: argparse.Namespace, spec: dict[str, Any], *, rank: int) -> str:
    if int(rank) <= 0:
        return str(args.profit_state)
    base = load_json(args.profit_state, default={})
    payload = copy.deepcopy(base) if isinstance(base, dict) else {}
    candidate = spec.get("candidate") if isinstance(spec.get("candidate"), dict) else {}
    payload["forward_candidate"] = candidate
    payload["forward_tracking_queue"] = [candidate]
    payload["paper_only"] = True
    payload["live_orders_allowed"] = False
    payload["queue_rank_probe"] = {
        "status": "PAPER_ONLY_FORWARD_QUEUE_RANK_PROBE",
        "rank": int(rank),
        "source_profit_state": str(args.profit_state),
        "candidate_id": spec.get("candidate_id"),
        "wallet": spec.get("wallet"),
        "policy_id": spec.get("policy_id"),
        "note": "rank probes are learning evidence only; rank0 canonical state remains the live-admission input",
    }
    if isinstance(payload.get("decision"), dict):
        payload["decision"] = dict(payload["decision"])
        payload["decision"]["live_orders_allowed"] = False
        payload["decision"]["paper_only"] = True
        payload["decision"]["queue_rank_probe"] = int(rank)
    output = Path(args.profit_state).with_name(f"{Path(args.profit_state).stem}_candidate_forward_rank_{int(rank)}.json")
    atomic_write_json(output, payload)
    return str(output)


def _candidate_forward_tracker_command(
    args: argparse.Namespace,
    *,
    spec: dict[str, Any] | None = None,
    rank: int = 0,
) -> dict[str, Any] | None:
    if spec is None:
        specs = _candidate_forward_specs(args, max_specs=1)
        spec = specs[0] if specs else {}
    wallet = spec.get("wallet")
    if not wallet:
        return None
    rank_int = int(rank)
    profit_policy_state = _candidate_forward_rank_profit_state(args, spec, rank=rank_int)
    command_name = "candidate_forward_tracker_measurement" if rank_int <= 0 else f"candidate_forward_tracker_measurement_rank_{rank_int}"
    iterations = max(
        1,
        int(
            getattr(
                args,
                "candidate_forward_probe_iterations",
                240,
            )
        ),
    )
    requested_runtime_s = float(getattr(args, "candidate_forward_probe_max_runtime_s", 90.0))
    child_command_cleanup_reserve_s = 30.0
    effective_runtime_s = _child_runtime_within_command_timeout(
        requested_runtime_s,
        float(getattr(args, "command_timeout_s", 240.0)),
        min_runtime_s=10.0,
        reserve_s=child_command_cleanup_reserve_s,
    )
    requested_poll_runtime_s = float(getattr(args, "candidate_forward_probe_max_poll_runtime_s", 20.0))
    effective_poll_runtime_s = _poll_runtime_within_child_runtime(
        requested_poll_runtime_s,
        effective_runtime_s,
        iterations,
        reserve_per_iteration_s=1.0,
    )
    child_command_timeout_s = min(
        float(getattr(args, "command_timeout_s", 240.0)),
        max(effective_runtime_s + 5.0, effective_runtime_s + child_command_cleanup_reserve_s),
    )
    data_api_timeout_s = min(float(getattr(args, "data_api_timeout_s", 2.0)), 1.5)
    clob_timeout_s = min(float(args.clob_timeout_s), 1.0)
    gamma_timeout_s = min(float(args.gamma_timeout_s), 1.0)
    return {
        "name": command_name,
        "purpose": (
            "paper-only forward measurement for the current forward-tracking candidate wallet/policy in isolated "
            "state/logs; this can satisfy candidate-specific tracker truth only if CLOB-backed required BUY evidence appears"
            if rank_int <= 0
            else (
                "paper-only forward queue rank probe in isolated state/logs; this is research evidence and never "
                "canonical live-admission truth"
            )
        ),
        "acceptable_returncodes": (0, 2),
        "runtime_budget": {
            "scope_mode": "ISOLATED_CANDIDATE_FORWARD_PROBE",
            "queue_rank": rank_int,
            "iterations": iterations,
            "requested_child_max_runtime_s": requested_runtime_s,
            "effective_child_max_runtime_s": effective_runtime_s,
            "child_command_timeout_s": child_command_timeout_s,
            "child_command_cleanup_reserve_s": child_command_cleanup_reserve_s,
            "requested_poll_max_runtime_s": requested_poll_runtime_s,
            "effective_poll_max_runtime_s": effective_poll_runtime_s,
            "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
            "reserve_s": 5.0,
            "status": (
                "FOCUSED_CANDIDATE_FORWARD_BURNIN"
                if iterations > 1 or requested_runtime_s > 45.0
                else "BOUNDED_TO_PARENT_TIMEOUT_OR_PROBE_DEFAULT"
                if effective_runtime_s < requested_runtime_s or effective_poll_runtime_s < requested_poll_runtime_s
                else "UNCHANGED"
            ),
            "stop_on_admission_evidence": True,
        },
        "argv": [
            PYTHON,
            "scripts/run_wallet_live_tracker.py",
            "--registry",
            args.registry,
            "--wallet-address",
            wallet,
            "--state",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_live_tracker_state",
                    "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
                ),
                rank_int,
            ),
            "--event-log",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_live_tracker_event_log",
                    "data/research/wallet_copy_candidate_forward_live_tracking_events.jsonl",
                ),
                rank_int,
            ),
            "--paper-state",
            _ranked_state_path(
                getattr(args, "candidate_forward_paper_state", "data/research/wallet_copy_candidate_forward_paper_state.json"),
                rank_int,
            ),
            "--paper-event-log",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_paper_event_log",
                    "data/research/wallet_copy_candidate_forward_paper_events.jsonl",
                ),
                rank_int,
            ),
            "--tracker-time-replay-paper-state",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_tracker_time_replay_paper_state",
                    "data/research/wallet_copy_candidate_forward_tracker_time_replay_paper_state.json",
                ),
                rank_int,
            ),
            "--tracker-time-replay-paper-event-log",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_tracker_time_replay_paper_event_log",
                    "data/research/wallet_copy_candidate_forward_tracker_time_replay_paper_events.jsonl",
                ),
                rank_int,
            ),
            "--single-wallet-exact-copy-paper-state",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_single_wallet_exact_copy_paper_state",
                    "data/research/wallet_copy_candidate_forward_single_wallet_exact_copy_paper_state.json",
                ),
                rank_int,
            ),
            "--single-wallet-exact-copy-paper-event-log",
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_single_wallet_exact_copy_paper_event_log",
                    "data/research/wallet_copy_candidate_forward_single_wallet_exact_copy_paper_events.jsonl",
                ),
                rank_int,
            ),
            "--profit-policy-state",
            profit_policy_state,
            "--track-blocked-profit-policy",
            "--seed-before-poll",
            "--seed-history-state",
            args.history_state,
            "--paper-retain-orders",
            str(max(1, int(getattr(args, "live_tracker_paper_retain_orders", 1_000)))),
            "--paper-retain-lifecycle-events",
            str(max(1, int(getattr(args, "live_tracker_paper_retain_lifecycle_events", 3_000)))),
            "--paper-retain-dedupe-ids",
            str(max(1, int(getattr(args, "live_tracker_paper_retain_dedupe_ids", 250_000)))),
            "--limit",
            str(max(5, int(args.live_tracker_limit))),
            "--pages",
            str(max(1, int(args.live_tracker_pages))),
            "--data-api-timeout-s",
            str(data_api_timeout_s),
            "--data-api-retries",
            "1",
            "--data-api-trade-query-keys",
            _hot_path_trade_query_keys(args),
            "--no-include-activity",
            "--max-book-slippage-bps",
            str(float(getattr(args, "slippage_bps", 500.0))),
            "--stop-on-admission-evidence",
            "--max-poll-runtime-s",
            str(effective_poll_runtime_s),
            "--iterations",
            str(iterations),
            "--poll-interval-s",
            str(float(args.live_tracker_poll_interval_s)),
            "--max-runtime-s",
            str(effective_runtime_s),
            "--enable-clob-books",
            "--admission-mode",
            "--strict-mirror-coverage",
            "--no-use-profit-search-scope",
            "--parallel-data-api-sources",
            "--clob-timeout-s",
            str(clob_timeout_s),
            "--gamma-timeout-s",
            str(gamma_timeout_s),
            "--market-ws-jsonl",
            str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
            "--enable-onchain-receipts",
            "--onchain-timeout-s",
            str(min(float(getattr(args, "onchain_timeout_s", 8.0)), 1.0)),
        ],
        "candidate_forward": spec,
        "queue_rank": rank_int,
    }


def _candidate_forward_tracker_commands(args: argparse.Namespace) -> list[dict[str, Any]]:
    specs = _candidate_forward_specs(args, max_specs=_candidate_forward_probe_rank_count(args))
    commands: list[dict[str, Any]] = []
    for index, spec in enumerate(specs):
        command = _candidate_forward_tracker_command(args, spec=spec, rank=index)
        if command is not None:
            commands.append(command)
    return commands


def _candidate_forward_tracker_dynamic_command() -> dict[str, Any]:
    return {
        "name": "candidate_forward_tracker_measurement_dynamic",
        "purpose": (
            "late-bind isolated candidate-forward trackers from the latest forward queue so the measured wallet/policy "
            "cannot drift from a stale pre-refresh queue; rank0 is canonical, rank1+ are research probes"
        ),
        "dynamic_factory": "candidate_forward_tracker_commands",
        "acceptable_returncodes": (0, 2),
    }


def _route_blocked_candidate_forward_skip_results(
    args: argparse.Namespace,
    commands: list[dict[str, Any]],
    source_route: dict[str, Any],
) -> list[dict[str, Any]]:
    route_status = str(source_route.get("status") or "")
    if not route_status or source_route_allows_measurement(route_status) or len(commands) <= 1:
        return []
    diagnostics = _source_route_diagnostic_fields(source_route)
    results: list[dict[str, Any]] = []
    for command in commands[1:]:
        results.append(
            {
                "name": command.get("name"),
                "purpose": command.get("purpose"),
                "ok": True,
                "returncode": 2,
                "skipped": True,
                "skip_reason": "SOURCE_ROUTE_BLOCKED_RESUMABLE_SLICE",
                "acceptable_returncodes": list(command.get("acceptable_returncodes") or (0,)),
                "candidate_forward": command.get("candidate_forward") or {},
                "queue_rank": command.get("queue_rank"),
                "runtime_budget": command.get("runtime_budget") or {},
                "source_route_status": route_status,
                "source_route_state": str(getattr(args, "source_route_state", "")),
                "paper_only": True,
                "live_orders_allowed": False,
                "next_command": (
                    f"python3 scripts/probe_polymarket_source_routes.py --output {args.source_route_state} --print && "
                    "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s "
                    f"{int(float(getattr(args, 'command_timeout_s', 240.0)))} --candidate-forward-probe-ranks "
                    f"{_candidate_forward_probe_rank_count(args)}"
                ),
                **diagnostics,
            }
        )
    return results


def _active_forward_probe_policy_command(args: argparse.Namespace, *, active_hotlane_state: str) -> dict[str, Any]:
    runtime_tracker_states = [
        str(
            getattr(
                args,
                "active_forward_probe_live_tracker_state",
                "data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
            )
        ),
        str(
            getattr(
                args,
                "candidate_forward_live_tracker_state",
                "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
            )
        ),
    ]
    for rank in range(1, _candidate_forward_probe_rank_count(args)):
        runtime_tracker_states.append(
            _ranked_state_path(
                getattr(
                    args,
                    "candidate_forward_live_tracker_state",
                    "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
                ),
                rank,
            )
        )
    runtime_args: list[str] = []
    for state_path in runtime_tracker_states:
        runtime_args.extend(["--runtime-tracker-state", state_path])
    return {
        "name": "active_forward_probe_policy_refresh",
        "purpose": (
            "select an active hot-lane single-wallet profit candidate for paper-only forward probing; "
            "this is a research probe and never live admission truth; prior tracker proof is used only "
            "to avoid repeatedly probing candidate/policy pairs with no required BUY evidence"
        ),
        "acceptable_returncodes": (0, 2),
        "argv": [
            PYTHON,
            "scripts/select_wallet_copy_active_forward_probe.py",
            "--profit-state",
            args.profit_state,
            "--active-hotlane-state",
            active_hotlane_state,
            "--output",
            str(
                getattr(
                    args,
                    "active_forward_probe_profit_state",
                    "data/research/wallet_copy_active_forward_probe_profit_state.json",
                )
            ),
            *runtime_args,
        ],
    }


def _active_hotlane_scope_refresh_command(
    args: argparse.Namespace,
    *,
    active_hotlane_state: str,
    active_hotlane_registry: str,
    active_hotlane_live_tracker_state: str,
    live_tracker_event_log: str,
) -> dict[str, Any]:
    return {
        "name": "active_hotlane_scope_refresh",
        "purpose": "select currently relevant BTC 5m wallets for low-latency paper tracking and adaptive bot evidence",
        "acceptable_returncodes": (0, 2),
        "argv": [
            PYTHON,
            "scripts/select_wallet_copy_active_hotlane.py",
            "--registry",
            args.registry,
            "--live-tracking-event-log",
            live_tracker_event_log,
            "--active-hotlane-live-tracking-state",
            active_hotlane_live_tracker_state,
            "--history-state",
            args.history_state,
            "--leaderboard-state",
            args.leaderboard_state,
            "--profit-state",
            args.profit_state,
            "--adaptive-state",
            str(getattr(args, "adaptive_bot_state", "data/research/wallet_copy_adaptive_bot_state.json")),
            "--hotlane-tick-state",
            str(getattr(args, "active_hotlane_tick_state", "data/research/wallet_copy_hotlane_tick_state.json")),
            "--output-registry",
            active_hotlane_registry,
            "--output",
            active_hotlane_state,
            "--max-wallets",
            str(int(getattr(args, "active_hotlane_max_wallets", 32))),
            "--recent-window-s",
            str(float(getattr(args, "active_hotlane_recent_window_s", 1800.0))),
        ],
    }


def _active_hotlane_scope_refresh_after_tracker_command(
    args: argparse.Namespace,
    *,
    active_hotlane_state: str,
    active_hotlane_registry: str,
    active_hotlane_live_tracker_state: str,
    live_tracker_event_log: str,
) -> dict[str, Any]:
    command = _active_hotlane_scope_refresh_command(
        args,
        active_hotlane_state=active_hotlane_state,
        active_hotlane_registry=active_hotlane_registry,
        active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
        live_tracker_event_log=live_tracker_event_log,
    )
    command["name"] = "active_hotlane_scope_refresh_after_direct_tracker"
    command["purpose"] = (
        "refresh active wallet scores after direct hot-lane tracker proof so active-forward candidate "
        "selection can rank fresh CLOB-copyable wallets ahead of stale anchor-only probes"
    )
    return command


def _active_forward_probe_tracker_command(
    args: argparse.Namespace,
    *,
    active_hotlane_registry: str,
) -> dict[str, Any]:
    source_iterations = max(1, int(args.live_tracker_iterations))
    iterations = max(
        source_iterations,
        int(getattr(args, "active_forward_probe_iterations", 240)),
    )
    source_requested_runtime_s = float(getattr(args, "live_tracker_max_runtime_s", 60.0))
    requested_runtime_s = float(getattr(args, "active_forward_probe_max_runtime_s", 90.0))
    effective_runtime_s = _child_runtime_within_command_timeout(
        requested_runtime_s,
        float(getattr(args, "command_timeout_s", 240.0)),
        min_runtime_s=10.0,
        reserve_s=5.0,
    )
    source_requested_poll_runtime_s = float(getattr(args, "live_tracker_max_poll_runtime_s", 30.0))
    requested_poll_runtime_s = min(
        source_requested_poll_runtime_s,
        float(getattr(args, "active_forward_probe_max_poll_runtime_s", 30.0)),
    )
    effective_poll_runtime_s = _poll_runtime_within_child_runtime(
        requested_poll_runtime_s,
        effective_runtime_s,
        iterations,
        reserve_per_iteration_s=1.0,
    )
    probe_max_wallets = max(
        2,
        min(
            max(1, int(getattr(args, "active_hotlane_max_wallets", 32))),
            max(1, int(getattr(args, "active_hotlane_wallets_per_tick", 8)))
            * max(3, int(getattr(args, "active_hotlane_ticks", 4))),
        ),
    )
    probe_parallel_wallet_fetches = max(
        2,
        min(
            probe_max_wallets,
            max(1, int(getattr(args, "active_hotlane_parallel_wallet_fetches", 8))),
        ),
    )
    return {
        "name": "active_forward_probe_tracker_measurement",
        "purpose": (
            "paper-only forward measurement for an active hot-lane candidate policy in isolated state/logs; "
            "keeps stale historical best-candidate admission blocked while forcing a broader multi-wallet hot-lane "
            "scope so current-poll consensus can be measured instead of structurally polling one wallet forever"
        ),
        "acceptable_returncodes": (0, 2),
        "runtime_budget": {
            "scope_mode": "ACTIVE_FORWARD_PROBE_MULTI_WALLET_HOTLANE_TRACKER",
            "iterations": iterations,
            "source_requested_iterations": source_iterations,
            "max_wallets": probe_max_wallets,
            "parallel_wallet_fetches": probe_parallel_wallet_fetches,
            "profit_policy_candidate_only": True,
            "source_requested_child_max_runtime_s": source_requested_runtime_s,
            "requested_child_max_runtime_s": requested_runtime_s,
            "effective_child_max_runtime_s": effective_runtime_s,
            "source_requested_poll_max_runtime_s": source_requested_poll_runtime_s,
            "requested_poll_max_runtime_s": requested_poll_runtime_s,
            "effective_poll_max_runtime_s": effective_poll_runtime_s,
            "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
            "reserve_s": 5.0,
            "status": (
                "FOCUSED_ACTIVE_FORWARD_BURNIN"
                if iterations > source_iterations or requested_runtime_s > source_requested_runtime_s
                else "RESUMABLE_ACTIVE_FORWARD_SLICE"
                if (
                    iterations < source_iterations
                    or requested_runtime_s < source_requested_runtime_s
                    or requested_poll_runtime_s < source_requested_poll_runtime_s
                )
                else "BOUNDED_TO_PARENT_TIMEOUT_OR_POLL_BUDGET"
                if effective_runtime_s < requested_runtime_s or effective_poll_runtime_s < requested_poll_runtime_s
                else "UNCHANGED"
            ),
            "stop_on_admission_evidence": True,
        },
        "argv": [
            PYTHON,
            "scripts/run_wallet_live_tracker.py",
            "--registry",
            active_hotlane_registry,
            "--state",
            str(
                getattr(
                    args,
                    "active_forward_probe_live_tracker_state",
                    "data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
                )
            ),
            "--event-log",
            str(
                getattr(
                    args,
                    "active_forward_probe_live_tracker_event_log",
                    "data/research/wallet_copy_active_forward_probe_live_tracking_events.jsonl",
                )
            ),
            "--paper-state",
            str(getattr(args, "active_forward_probe_paper_state", "data/research/wallet_copy_active_forward_probe_paper_state.json")),
            "--paper-event-log",
            str(
                getattr(
                    args,
                    "active_forward_probe_paper_event_log",
                    "data/research/wallet_copy_active_forward_probe_paper_events.jsonl",
                )
            ),
            "--tracker-time-replay-paper-state",
            str(
                getattr(
                    args,
                    "active_forward_probe_tracker_time_replay_paper_state",
                    "data/research/wallet_copy_active_forward_probe_tracker_time_replay_paper_state.json",
                )
            ),
            "--tracker-time-replay-paper-event-log",
            str(
                getattr(
                    args,
                    "active_forward_probe_tracker_time_replay_paper_event_log",
                    "data/research/wallet_copy_active_forward_probe_tracker_time_replay_paper_events.jsonl",
                )
            ),
            "--single-wallet-exact-copy-paper-state",
            str(
                getattr(
                    args,
                    "active_forward_probe_single_wallet_exact_copy_paper_state",
                    "data/research/wallet_copy_active_forward_probe_single_wallet_exact_copy_paper_state.json",
                )
            ),
            "--single-wallet-exact-copy-paper-event-log",
            str(
                getattr(
                    args,
                    "active_forward_probe_single_wallet_exact_copy_paper_event_log",
                    "data/research/wallet_copy_active_forward_probe_single_wallet_exact_copy_paper_events.jsonl",
                )
            ),
            "--profit-policy-state",
            str(
                getattr(
                    args,
                    "active_forward_probe_profit_state",
                    "data/research/wallet_copy_active_forward_probe_profit_state.json",
                )
            ),
            "--track-blocked-profit-policy",
            "--seed-before-poll",
            "--seed-history-state",
            args.history_state,
            "--paper-retain-orders",
            str(max(1, int(getattr(args, "live_tracker_paper_retain_orders", 1_000)))),
            "--paper-retain-lifecycle-events",
            str(max(1, int(getattr(args, "live_tracker_paper_retain_lifecycle_events", 3_000)))),
            "--paper-retain-dedupe-ids",
            str(max(1, int(getattr(args, "live_tracker_paper_retain_dedupe_ids", 250_000)))),
            "--limit",
            str(max(5, int(args.live_tracker_limit))),
            "--pages",
            str(max(1, int(args.live_tracker_pages))),
            "--data-api-timeout-s",
            str(min(float(getattr(args, "data_api_timeout_s", 2.0)), 1.5)),
            "--data-api-retries",
            "1",
            "--data-api-trade-query-keys",
            _hot_path_trade_query_keys(args),
            "--no-include-activity",
            "--max-book-slippage-bps",
            str(float(getattr(args, "slippage_bps", 500.0))),
            "--stop-on-admission-evidence",
            "--max-poll-runtime-s",
            str(effective_poll_runtime_s),
            "--iterations",
            str(iterations),
            "--poll-interval-s",
            str(float(args.live_tracker_poll_interval_s)),
            "--max-runtime-s",
            str(effective_runtime_s),
            "--enable-clob-books",
            "--admission-mode",
            "--strict-mirror-coverage",
            "--no-use-profit-search-scope",
            "--profit-policy-candidate-only",
            "--max-wallets",
            str(probe_max_wallets),
            "--parallel-wallet-fetches",
            str(probe_parallel_wallet_fetches),
            "--parallel-data-api-sources",
            "--clob-timeout-s",
            str(min(float(args.clob_timeout_s), 1.0)),
            "--gamma-timeout-s",
            str(min(float(args.gamma_timeout_s), 1.0)),
            "--market-ws-jsonl",
            str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
            "--enable-onchain-receipts",
            "--onchain-timeout-s",
            str(min(float(getattr(args, "onchain_timeout_s", 8.0)), 1.0)),
        ],
    }


def _adaptive_bot_command(
    args: argparse.Namespace,
    *,
    live_tracker_state_for_adaptive: str,
    live_tracker_event_log: str,
) -> dict[str, Any]:
    adaptive_bot_state = str(
        getattr(args, "adaptive_bot_state", "data/research/wallet_copy_adaptive_bot_state.json")
    )
    adaptive_bot_paper_state = str(
        getattr(args, "adaptive_bot_paper_state", "data/research/wallet_copy_adaptive_bot_paper_state.json")
    )
    adaptive_bot_paper_event_log = str(
        getattr(
            args,
            "adaptive_bot_paper_event_log",
            "data/research/wallet_copy_adaptive_bot_paper_events.jsonl",
        )
    )
    adaptive_single_wallet_paper_state = str(
        getattr(
            args,
            "adaptive_bot_single_wallet_exact_copy_paper_state",
            "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json",
        )
    )
    adaptive_single_wallet_paper_event_log = str(
        getattr(
            args,
            "adaptive_bot_single_wallet_exact_copy_paper_event_log",
            "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
        )
    )
    adaptive_tracker_time_replay_paper_state = str(
        getattr(
            args,
            "adaptive_bot_tracker_time_replay_paper_state",
            "data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json",
        )
    )
    adaptive_tracker_time_replay_paper_event_log = str(
        getattr(
            args,
            "adaptive_bot_tracker_time_replay_paper_event_log",
            "data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
        )
    )
    return {
        "name": "adaptive_wallet_derived_bot_measurement",
        "purpose": "run immediately after active hot-lane tracking so fresh multi-wallet CLOB-backed evidence does not age out",
        "acceptable_returncodes": (0, 2),
        "argv": [
            PYTHON,
            "scripts/run_wallet_copy_adaptive_bot.py",
            "--live-tracking-state",
            live_tracker_state_for_adaptive,
            "--live-tracking-event-log",
            live_tracker_event_log,
            "--output",
            adaptive_bot_state,
            "--paper-state",
            adaptive_bot_paper_state,
            "--paper-event-log",
            adaptive_bot_paper_event_log,
            "--single-wallet-exact-copy-paper-state",
            adaptive_single_wallet_paper_state,
            "--single-wallet-exact-copy-paper-event-log",
            adaptive_single_wallet_paper_event_log,
            "--tracker-time-replay-paper-state",
            adaptive_tracker_time_replay_paper_state,
            "--tracker-time-replay-paper-event-log",
            adaptive_tracker_time_replay_paper_event_log,
            "--max-event-log-rows",
            "3000",
            "--max-observation-age-s",
            "30",
            "--max-observed-event-age-s",
            "10",
            "--max-signal-cluster-age-s",
            "8",
        ],
    }


def _active_hotlane_tick_command(
    args: argparse.Namespace,
    *,
    active_hotlane_registry: str,
    active_hotlane_live_tracker_state: str,
    active_hotlane_live_tracker_event_log: str,
    active_hotlane_paper_state: str,
    active_hotlane_paper_event_log: str,
    active_hotlane_tracker_time_replay_paper_state: str,
    active_hotlane_tracker_time_replay_paper_event_log: str,
    active_hotlane_all_order_exact_copy_paper_state: str,
    active_hotlane_all_order_exact_copy_paper_event_log: str,
    active_hotlane_all_order_tactic_replay_paper_state: str,
    active_hotlane_all_order_tactic_replay_paper_event_log: str,
) -> dict[str, Any]:
    requested_ticks = max(1, int(getattr(args, "active_hotlane_ticks", 4)))
    effective_ticks = max(1, min(requested_ticks, int(getattr(args, "active_hotlane_tick_slice_ticks", 1))))
    command_timeout_s = float(getattr(args, "command_timeout_s", 240.0))
    hotlane_child_cap_s = 45.0 if bool(getattr(args, "deep_research", False)) else 20.0
    hotlane_child_floor_s = 20.0 if bool(getattr(args, "deep_research", False)) else 10.0
    effective_max_runtime_s = min(
        hotlane_child_cap_s,
        max(hotlane_child_floor_s, command_timeout_s / 12.0),
    )
    effective_poll_runtime_s = min(
        float(getattr(args, "active_hotlane_max_poll_runtime_s", 30.0)),
        max(8.0, effective_max_runtime_s / 3.0),
    )
    hotlane_subcommand_timeout_s = min(command_timeout_s, effective_max_runtime_s + 10.0)
    max_child_commands_per_tick = 4 if bool(getattr(args, "cohort_probe_on_single_wallet", True)) else 2
    child_command_timeout_s = min(
        command_timeout_s,
        hotlane_subcommand_timeout_s * max_child_commands_per_tick + 5.0,
    )
    return {
        "name": "active_hotlane_tick_measurement",
        "purpose": (
            "rotate tiny active-wallet slices through strict tracker ticks and run adaptive immediately after each "
            "tick so consensus evidence does not age out behind a full batch"
        ),
        "acceptable_returncodes": (0, 2),
        "runtime_budget": {
            "scope_mode": "RESUMABLE_ACTIVE_HOTLANE_TICK_SLICE",
            "requested_ticks": requested_ticks,
            "effective_ticks": effective_ticks,
            "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
            "effective_child_max_runtime_s": round(effective_max_runtime_s, 3),
            "effective_poll_max_runtime_s": round(effective_poll_runtime_s, 3),
            "hotlane_subcommand_timeout_s": round(hotlane_subcommand_timeout_s, 3),
            "max_child_commands_per_tick": max_child_commands_per_tick,
            "child_command_timeout_s": round(child_command_timeout_s, 3),
            "status": "RESUMABLE_ACTIVE_HOTLANE_TICK_SLICE" if effective_ticks < requested_ticks else "UNCHANGED",
        },
        "argv": [
            PYTHON,
            "scripts/run_wallet_copy_hotlane_tick.py",
            "--registry",
            active_hotlane_registry,
            "--tracker-state",
            active_hotlane_live_tracker_state,
            "--tracker-event-log",
            active_hotlane_live_tracker_event_log,
            "--paper-state",
            active_hotlane_paper_state,
            "--paper-event-log",
            active_hotlane_paper_event_log,
            "--tracker-time-replay-paper-state",
            active_hotlane_tracker_time_replay_paper_state,
            "--tracker-time-replay-paper-event-log",
            active_hotlane_tracker_time_replay_paper_event_log,
            "--single-wallet-exact-copy-paper-state",
            str(
                getattr(
                    args,
                    "active_hotlane_single_wallet_exact_copy_paper_state",
                    "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_state.json",
                )
            ),
            "--single-wallet-exact-copy-paper-event-log",
            str(
                getattr(
                    args,
                    "active_hotlane_single_wallet_exact_copy_paper_event_log",
                    "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
                )
            ),
            "--all-order-exact-copy-paper-state",
            active_hotlane_all_order_exact_copy_paper_state,
            "--all-order-exact-copy-paper-event-log",
            active_hotlane_all_order_exact_copy_paper_event_log,
            "--all-order-tactic-replay-paper-state",
            active_hotlane_all_order_tactic_replay_paper_state,
            "--all-order-tactic-replay-paper-event-log",
            active_hotlane_all_order_tactic_replay_paper_event_log,
            "--adaptive-state",
            str(getattr(args, "adaptive_bot_state", "data/research/wallet_copy_adaptive_bot_state.json")),
            "--adaptive-paper-state",
            str(
                getattr(
                    args,
                    "adaptive_bot_paper_state",
                    "data/research/wallet_copy_adaptive_bot_paper_state.json",
                )
            ),
            "--adaptive-paper-event-log",
            str(
                getattr(
                    args,
                    "adaptive_bot_paper_event_log",
                    "data/research/wallet_copy_adaptive_bot_paper_events.jsonl",
                )
            ),
            "--adaptive-single-wallet-exact-copy-paper-state",
            str(
                getattr(
                    args,
                    "adaptive_bot_single_wallet_exact_copy_paper_state",
                    "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json",
                )
            ),
            "--adaptive-single-wallet-exact-copy-paper-event-log",
            str(
                getattr(
                    args,
                    "adaptive_bot_single_wallet_exact_copy_paper_event_log",
                    "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
                )
            ),
            "--adaptive-tracker-time-replay-paper-state",
            str(
                getattr(
                    args,
                    "adaptive_bot_tracker_time_replay_paper_state",
                    "data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json",
                )
            ),
            "--adaptive-tracker-time-replay-paper-event-log",
            str(
                getattr(
                    args,
                    "adaptive_bot_tracker_time_replay_paper_event_log",
                    "data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
                )
            ),
            "--profit-policy-state",
            args.profit_state,
            "--seed-history-state",
            args.history_state,
            "--output",
            str(getattr(args, "active_hotlane_tick_state", "data/research/wallet_copy_hotlane_tick_state.json")),
            "--source-route-state",
            str(getattr(args, "source_route_state", "data/research/wallet_copy_source_route_state.json")),
            "--ticks",
            str(effective_ticks),
            "--wallets-per-tick",
            str(int(getattr(args, "active_hotlane_wallets_per_tick", 8))),
            "--parallel-wallet-fetches",
            str(
                max(
                    1,
                    int(
                        getattr(
                            args,
                            "active_hotlane_parallel_wallet_fetches",
                            getattr(args, "active_hotlane_wallets_per_tick", 8),
                        )
                    ),
                )
            ),
            "--parallel-data-api-sources",
            "--limit",
            "5",
            "--pages",
            "1",
            "--data-api-timeout-s",
            str(min(float(getattr(args, "data_api_timeout_s", 2.0)), 1.0)),
            "--data-api-trade-query-keys",
            _hot_path_trade_query_keys(args),
            "--max-poll-runtime-s",
            str(effective_poll_runtime_s),
            "--max-runtime-s",
            str(effective_max_runtime_s),
            "--poll-gap-s",
            str(float(getattr(args, "active_hotlane_tick_gap_s", 0.2))),
            "--clob-timeout-s",
            str(min(float(args.clob_timeout_s), 0.8)),
            "--gamma-timeout-s",
            str(min(float(args.gamma_timeout_s), 0.8)),
            "--market-ws-jsonl",
            str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
            "--command-timeout-s",
            str(hotlane_subcommand_timeout_s),
        ],
    }


def _active_hotlane_tracker_command(
    args: argparse.Namespace,
    *,
    active_hotlane_registry: str,
    active_hotlane_live_tracker_state: str,
    active_hotlane_live_tracker_event_log: str,
    active_hotlane_paper_state: str,
    active_hotlane_paper_event_log: str,
    active_hotlane_tracker_time_replay_paper_state: str,
    active_hotlane_tracker_time_replay_paper_event_log: str,
    active_hotlane_single_wallet_exact_copy_paper_state: str,
    active_hotlane_single_wallet_exact_copy_paper_event_log: str,
    active_hotlane_all_order_exact_copy_paper_state: str,
    active_hotlane_all_order_exact_copy_paper_event_log: str,
    active_hotlane_all_order_tactic_replay_paper_state: str,
    active_hotlane_all_order_tactic_replay_paper_event_log: str,
    purpose_suffix: str,
) -> dict[str, Any]:
    command_timeout_s = float(getattr(args, "command_timeout_s", 240.0))
    deep_research = bool(getattr(args, "deep_research", False))
    if deep_research:
        active_poll_runtime_s = min(
            max(float(getattr(args, "active_hotlane_max_poll_runtime_s", 30.0)), 60.0),
            120.0,
        )
    else:
        active_poll_runtime_s = min(
            float(getattr(args, "active_hotlane_max_poll_runtime_s", 30.0)),
            25.0,
        )
    if deep_research:
        active_runtime_s = min(
            max(1.0, command_timeout_s - 30.0),
            max(90.0, min(180.0, command_timeout_s / 4.0)),
        )
    else:
        active_runtime_s = max(45.0, active_poll_runtime_s + 20.0)
    requested_active_max_wallets = int(getattr(args, "active_hotlane_max_wallets", 32))
    # Direct tracker refresh must leave time for event processing. Broad hot-lane
    # coverage still happens in the resumable tick and registry sweep commands.
    direct_tracker_wallet_cap = 8 if deep_research else requested_active_max_wallets
    active_max_wallets = max(
        1,
        min(
            requested_active_max_wallets,
            direct_tracker_wallet_cap,
            max(1, int(getattr(args, "active_hotlane_wallets_per_tick", 8)))
            * max(3, int(getattr(args, "active_hotlane_ticks", 4))),
        ),
    )
    active_parallel_wallet_fetches = max(
        1,
        min(
            active_max_wallets,
            int(
                getattr(
                    args,
                    "active_hotlane_parallel_wallet_fetches",
                    getattr(args, "active_hotlane_max_wallets", 32),
                )
            ),
        ),
    )
    child_timeout_s = min(command_timeout_s, active_runtime_s + (30.0 if deep_research else 20.0))
    return {
        "name": "active_hotlane_tracker_measurement",
        "purpose": (
            "poll the active wallet hot-lane into isolated paper live-tracker logs; "
            f"{purpose_suffix}"
        ),
        "acceptable_returncodes": (0, 2),
        "runtime_budget": {
            "scope_mode": "ACTIVE_HOTLANE_DIRECT_TRACKER_REFRESH",
            "child_command_timeout_s": round(child_timeout_s, 3),
            "effective_child_max_runtime_s": active_runtime_s,
            "effective_poll_max_runtime_s": active_poll_runtime_s,
            "max_wallets": active_max_wallets,
            "status": "STALE_CURRENT_POLL_TRUTH_REFRESH",
        },
        "argv": [
            PYTHON,
            "scripts/run_wallet_live_tracker.py",
            "--registry",
            active_hotlane_registry,
            "--state",
            active_hotlane_live_tracker_state,
            "--event-log",
            active_hotlane_live_tracker_event_log,
            "--paper-state",
            active_hotlane_paper_state,
            "--paper-event-log",
            active_hotlane_paper_event_log,
            "--tracker-time-replay-paper-state",
            active_hotlane_tracker_time_replay_paper_state,
            "--tracker-time-replay-paper-event-log",
            active_hotlane_tracker_time_replay_paper_event_log,
            "--single-wallet-exact-copy-paper-state",
            active_hotlane_single_wallet_exact_copy_paper_state,
            "--single-wallet-exact-copy-paper-event-log",
            active_hotlane_single_wallet_exact_copy_paper_event_log,
            "--all-order-exact-copy-paper-state",
            active_hotlane_all_order_exact_copy_paper_state,
            "--all-order-exact-copy-paper-event-log",
            active_hotlane_all_order_exact_copy_paper_event_log,
            "--all-order-tactic-replay-paper-state",
            active_hotlane_all_order_tactic_replay_paper_state,
            "--all-order-tactic-replay-paper-event-log",
            active_hotlane_all_order_tactic_replay_paper_event_log,
            "--profit-policy-state",
            args.profit_state,
            "--seed-before-poll",
            "--seed-history-state",
            args.history_state,
            "--limit",
            "5",
            "--pages",
            "1",
            "--data-api-timeout-s",
            str(min(float(getattr(args, "data_api_timeout_s", 2.0)), 1.5)),
            "--data-api-trade-query-keys",
            _hot_path_trade_query_keys(args),
            "--max-book-slippage-bps",
            str(float(args.slippage_bps)),
            "--max-poll-runtime-s",
            str(active_poll_runtime_s),
            "--iterations",
            str(int(getattr(args, "active_hotlane_iterations", 1))),
            "--poll-interval-s",
            str(float(args.live_tracker_poll_interval_s)),
            "--max-runtime-s",
            str(active_runtime_s),
            "--enable-clob-books",
            "--admission-mode",
            "--strict-mirror-coverage",
            "--no-use-profit-search-scope",
            "--max-wallets",
            str(active_max_wallets),
            "--parallel-wallet-fetches",
            str(active_parallel_wallet_fetches),
            "--parallel-data-api-sources",
            "--clob-timeout-s",
            str(min(float(args.clob_timeout_s), 1.0)),
            "--gamma-timeout-s",
            str(min(float(args.gamma_timeout_s), 1.0)),
            "--market-ws-jsonl",
            str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
            "--enable-onchain-receipts",
            "--onchain-timeout-s",
            str(min(float(getattr(args, "onchain_timeout_s", 8.0)), 1.0)),
        ],
    }


def _leaderboard_discovery_command(args: argparse.Namespace) -> dict[str, Any]:
    leaderboard_pages = int(getattr(args, "leaderboard_pages", 0))
    leaderboard_max_pages = int(getattr(args, "leaderboard_max_pages", 20))
    return {
        "name": "leaderboard_discovery_refresh",
        "purpose": (
            "refresh WEEK and MONTH CRYPTO leaderboard wallet registry source with maximum bounded observation; "
            "all fetched wallets are copied into paper-only registry/history coverage"
        ),
        "acceptable_returncodes": (0, 2),
        "runtime_budget": {
            "scope_mode": "MAXIMIZE_WEEKLY_MONTHLY_CRYPTO_LEADERBOARD_OBSERVATION",
            "weekly_limit": 50,
            "monthly_limit": 50,
            "pages_per_period": leaderboard_pages,
            "max_pages_per_period": leaderboard_max_pages,
            "page_mode": "FETCH_UNTIL_EMPTY_OR_CAP" if leaderboard_pages <= 0 else "FIXED_PAGE_COUNT",
            "copy_all_fetched_wallets_to_registry": True,
            "paper_only": True,
            "live_orders_allowed": False,
        },
        "argv": [
            PYTHON,
            "scripts/onboard_leaderboard_crypto_wallets.py",
            "--weekly-limit",
            "50",
            "--monthly-limit",
            "50",
            "--leaderboard-pages",
            str(leaderboard_pages),
            "--leaderboard-max-pages",
            str(leaderboard_max_pages),
            "--registry",
            args.registry,
            "--output",
            args.leaderboard_state,
        ],
    }


def _proof_led_strategy_direction(args: argparse.Namespace) -> dict[str, Any] | None:
    if not hasattr(args, "strategy_direction_state"):
        return None
    payload = load_json(
        args.strategy_direction_state,
        default={},
    )
    if not isinstance(payload, dict):
        return None
    directions = payload.get("directions") if isinstance(payload.get("directions"), list) else []
    for row in directions:
        if isinstance(row, dict) and str(row.get("id") or "") == "single_wallet_best_copyable":
            return row
    return directions[0] if directions and isinstance(directions[0], dict) else None


def _live_ready_unlock_context(args: argparse.Namespace) -> dict[str, Any]:
    """Detect the short focused lane for turning clean runtime proof into live-ready proof.

    This is deliberately not a live-readiness gate. It only chooses command
    order and short-run rank breadth when the source-of-truth state already says
    a candidate has clean CLOB copy truth but profitability/sample/all-order
    gates remain unsatisfied.
    """

    if bool(getattr(args, "disable_live_ready_unlock", False)):
        return {"active": False, "reason": "disabled_by_flag"}

    previous = load_json(getattr(args, "state", ""), default={})
    previous = previous if isinstance(previous, dict) else {}
    live_readiness = previous.get("live_readiness_report") if isinstance(previous.get("live_readiness_report"), dict) else {}
    if bool(live_readiness.get("live_ready")):
        return {"active": False, "reason": "already_live_ready"}

    direction = _proof_led_strategy_direction(args)
    proof = direction.get("runtime_proof") if isinstance(direction, dict) and isinstance(direction.get("runtime_proof"), dict) else {}
    copy_truth = live_readiness.get("copy_truth") if isinstance(live_readiness.get("copy_truth"), dict) else {}
    paper_results = live_readiness.get("paper_results") if isinstance(live_readiness.get("paper_results"), dict) else {}
    green = previous.get("green_semantics") if isinstance(previous.get("green_semantics"), dict) else {}

    wallet = str(proof.get("source_wallet") or direction.get("wallet") if isinstance(direction, dict) else "").strip()
    policy_id = str(proof.get("policy_id") or "").strip()
    candidate_id = str(proof.get("candidate_id") or "").strip()
    proof_events = _safe_int(proof.get("source_events") or proof.get("proof_rows"))
    proof_windows = _safe_int(proof.get("market_windows"))
    proof_age_p95 = _safe_float(proof.get("event_age_p95_s"))

    if not proof and copy_truth:
        wallet = str(copy_truth.get("candidate_source_wallet") or paper_results.get("source_wallet") or "").strip()
        policy_id = str(copy_truth.get("candidate_policy_id") or paper_results.get("policy_id") or "").strip()
        candidate_id = str(copy_truth.get("candidate_id") or paper_results.get("candidate_id") or "").strip()
        proof_events = _safe_int(
            copy_truth.get("runtime_proof_index_rows")
            or copy_truth.get("required_buy_copy_events")
            or copy_truth.get("global_required_buy_copy_events")
        )
        proof_windows = _safe_int(
            paper_results.get("unique_windows")
            or copy_truth.get("runtime_candidate_distinct_market_windows")
        )
        proof_age_p95 = _safe_float(copy_truth.get("required_event_age_p95_s") or copy_truth.get("required_api_latency_p95_s"))

    min_events = max(1, int(getattr(args, "live_ready_unlock_min_proof_events", 10)))
    min_windows = max(1, int(getattr(args, "live_ready_unlock_min_proof_windows", 3)))
    max_age = float(getattr(args, "live_ready_unlock_max_proof_age_p95_s", 10.0))
    clean_copy_truth = (
        copy_truth.get("effective_live_tracker_truth_status") == "PASS"
        and _safe_int(copy_truth.get("required_buy_copy_events")) > 0
        and _safe_int(copy_truth.get("clob_filled_buy_copy_events")) >= _safe_int(copy_truth.get("required_buy_copy_events"))
        and _safe_int(copy_truth.get("fallback_filled_buy_copy_events")) == 0
        and _safe_int(copy_truth.get("rejected_buy_copy_events")) == 0
        and _safe_int(copy_truth.get("missed_buy_copy_events")) == 0
    )
    clean_runtime_proof = (
        proof_events >= min_events
        and proof_windows >= min_windows
        and proof_age_p95 is not None
        and proof_age_p95 <= max_age
    )
    active = bool(wallet and policy_id and (clean_runtime_proof or clean_copy_truth))
    original_ranks = max(1, int(getattr(args, "candidate_forward_probe_ranks", 1)))
    focused_ranks = max(1, int(getattr(args, "live_ready_unlock_probe_ranks", 2)))
    effective_ranks = min(original_ranks, focused_ranks)
    blockers = list(live_readiness.get("blockers") or [])
    return {
        "schema_version": 1,
        "active": active,
        "reason": "proof_led_candidate_has_clean_runtime_copy_truth_but_live_ready_gates_remain" if active else "missing_clean_proof_led_candidate",
        "paper_only": True,
        "live_orders_allowed": False,
        "candidate_id": candidate_id or None,
        "source_wallet": wallet or None,
        "policy_id": policy_id or None,
        "proof_events": proof_events,
        "proof_windows": proof_windows,
        "proof_age_p95_s": proof_age_p95,
        "clean_runtime_proof": clean_runtime_proof,
        "clean_candidate_copy_truth": clean_copy_truth,
        "live_ready": False,
        "current_blockers": blockers[:40],
        "copy_blockers": list(green.get("copy_blockers") or [])[:20],
        "bot_blockers": list(green.get("bot_blockers") or [])[:20],
        "original_candidate_forward_probe_ranks": original_ranks,
        "effective_candidate_forward_probe_ranks": effective_ranks,
        "deferred_candidate_forward_probe_ranks": max(0, original_ranks - effective_ranks),
        "deep_research_keeps_full_rank_coverage": False,
        "rank_policy": "focused_live_ready_unlock_first",
        "next_full_coverage_command": (
            "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900 "
            "--disable-live-ready-unlock"
        ),
    }


def _candidate_forward_probe_rank_count(args: argparse.Namespace) -> int:
    return max(
        1,
        int(
            getattr(
                args,
                "_live_ready_unlock_probe_ranks_effective",
                getattr(args, "candidate_forward_probe_ranks", 1),
            )
        ),
    )


def _proof_led_candidate_history_command(args: argparse.Namespace) -> dict[str, Any] | None:
    """Prioritize the already-CLOB-proven wallet/policy before broad registry slices.

    This does not relax live-readiness. It only prevents the best proof-led lane
    from being starved behind the full leaderboard pagination timeout while it
    still needs more resolved paper outcomes.
    """

    direction = _proof_led_strategy_direction(args)
    if not isinstance(direction, dict):
        return None
    proof = direction.get("runtime_proof") if isinstance(direction.get("runtime_proof"), dict) else {}
    if not proof:
        return None
    source_events = int(proof.get("source_events") or proof.get("proof_rows") or 0)
    market_windows = int(proof.get("market_windows") or 0)
    try:
        p95_age_s = float(proof.get("event_age_p95_s"))
    except (TypeError, ValueError):
        p95_age_s = float("inf")
    blockers = {str(item) for item in direction.get("blockers") or []}
    allowed_sample_blockers = {
        "single_wallet_resolved_orders_below_100",
        "single_wallet_wr_below_70pct",
        "single_wallet_roi_below_5pct",
    }
    if source_events < 10 or market_windows < 3 or p95_age_s > 10.0:
        return None
    if blockers and not blockers.issubset(allowed_sample_blockers):
        return None
    wallet = str(proof.get("source_wallet") or direction.get("wallet") or "").strip()
    policy_id = str(proof.get("policy_id") or "").strip()
    candidate_id = str(proof.get("candidate_id") or "").strip()
    if not wallet or not policy_id:
        return None
    digest = hashlib.sha1(f"{wallet}|{policy_id}|{candidate_id}".encode("utf-8")).hexdigest()[:12]
    resume_base = Path(getattr(args, "pipeline_resume_state", "data/research/wallet_copy_pipeline_resume_state.json"))
    resume_state = resume_base.with_name(f"{resume_base.stem}_proof_led_{digest}{resume_base.suffix}")
    child_timeout_s = _child_runtime_within_command_timeout(
        float(getattr(args, "command_timeout_s", 240.0)),
        float(getattr(args, "command_timeout_s", 240.0)),
        min_runtime_s=45.0,
        reserve_s=35.0,
    )
    if not bool(getattr(args, "deep_research", False)):
        child_timeout_s = min(
            child_timeout_s,
            max(45.0, min(75.0, float(getattr(args, "command_timeout_s", 240.0)) / 3.0)),
        )
    return {
        "name": "proof_led_candidate_history_and_paper",
        "purpose": (
            "advance the already CLOB-proven single-wallet/policy lane into canonical history/paper before broad "
            "leaderboard coverage, so sample-size gates are repaired with evidence instead of hidden"
        ),
        "acceptable_returncodes": (0,),
        "runtime_budget": {
            "scope_mode": "PROOF_LED_SINGLE_WALLET_HISTORY_PAPER",
            "wallet": wallet,
            "policy_id": policy_id,
            "candidate_id": candidate_id or None,
            "source_events": source_events,
            "market_windows": market_windows,
            "event_age_p95_s": p95_age_s,
            "remaining_blockers": sorted(blockers),
            "limit": int(args.pipeline_limit),
            "pages_per_run": max(1, int(getattr(args, "pipeline_pages_per_resume_run", 1))),
            "wallet_batch_size": max(1, int(getattr(args, "pipeline_wallet_batch_size", 1))),
            "resume_state": str(resume_state),
            "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
            "child_command_timeout_s": child_timeout_s,
            "status": "TARGETED_PROOF_LED_COVERAGE_NOT_GATE_RELAXATION",
        },
        "argv": [
            PYTHON,
            "scripts/run_wallet_copy_pipeline_resume.py",
            "--wallet",
            wallet,
            "--wallet-name",
            str(direction.get("wallet_name") or f"proof_led_{wallet[2:10]}"),
            "--limit",
            str(int(args.pipeline_limit)),
            "--pages-per-run",
            str(max(1, int(getattr(args, "pipeline_pages_per_resume_run", 1)))),
            "--wallet-batch-size",
            str(max(1, int(getattr(args, "pipeline_wallet_batch_size", 1)))),
            "--resume-state",
            str(resume_state),
            "--history-state",
            args.history_state,
            "--paper-state",
            args.paper_state,
            "--wallet-event-log",
            "data/research/wallet_copy_events.jsonl",
            "--paper-event-log",
            "data/research/wallet_copy_paper_events.jsonl",
            "--wallet-fraction",
            str(float(getattr(args, "wallet_fraction", 0.05))),
            "--max-order-usd",
            str(float(getattr(args, "max_order_usd", 2.0))),
            "--policy-id",
            policy_id,
            "--command-timeout-s",
            str(child_timeout_s),
        ],
    }


def _leaderboard_pipeline_stage_commands(
    args: argparse.Namespace,
    *,
    pipeline_pages: int,
    policy_preset: str,
    max_wallets_for_search: int,
    max_single_wallet_candidate_intents: int,
    max_multi_wallet_base_intents: int,
    include_proof_led: bool = True,
) -> list[dict[str, Any]]:
    """Return resumable leaderboard research stages for deep proof runs.

    The old monolithic `onboard_leaderboard_crypto_wallets.py --run-pipeline`
    hides which stage consumed the whole timeout. Deep proof runs need stage
    visibility so limits become repairable coverage gaps instead of a single
    opaque command failure.
    """

    common_profit_args = [
        "--max-unresolved-ratio",
        str(float(args.max_unresolved_ratio)),
        "--slippage-bps",
        str(float(args.slippage_bps)),
        "--max-multi-wallet-base-intents",
        str(int(max_multi_wallet_base_intents)),
        "--max-wallets-for-search",
        str(int(max_wallets_for_search)),
        "--max-single-wallet-candidate-intents",
        str(int(max_single_wallet_candidate_intents)),
        "--policy-preset",
        policy_preset,
    ]
    if getattr(args, "no_consensus_search", False):
        common_profit_args.append("--no-consensus-search")
    if getattr(args, "no_inventory_search", False):
        common_profit_args.append("--no-inventory-search")
    if getattr(args, "no_skip_low_intent_candidates", False):
        common_profit_args.append("--no-skip-low-intent-candidates")

    active_proof_reserve_s = min(120.0, max(35.0, float(getattr(args, "command_timeout_s", 240.0)) / 2.0))
    resume_child_timeout_s = _child_runtime_within_command_timeout(
        float(getattr(args, "command_timeout_s", 240.0)),
        float(getattr(args, "command_timeout_s", 240.0)),
        min_runtime_s=45.0,
        reserve_s=active_proof_reserve_s,
    )

    commands: list[dict[str, Any]] = []
    if include_proof_led:
        proof_led_command = _proof_led_candidate_history_command(args)
        if proof_led_command is not None:
            commands.append(proof_led_command)
    if int(pipeline_pages) > 0:
        commands.append(
            {
                "name": "leaderboard_history_and_paper",
                "purpose": "advance resumable registered BTC-5m wallet history coverage and replay exact-copy intents to paper",
                "acceptable_returncodes": (0,),
                "argv": [
                    PYTHON,
                    "scripts/run_wallet_copy_pipeline_resume.py",
                    "--wallets-config",
                    args.registry,
                    "--limit",
                    str(int(args.pipeline_limit)),
                    "--pages-per-run",
                    str(max(1, int(getattr(args, "pipeline_pages_per_resume_run", 1)))),
                    "--wallet-batch-size",
                    str(max(1, int(getattr(args, "pipeline_wallet_batch_size", 1)))),
                    "--resume-state",
                    str(getattr(args, "pipeline_resume_state", "data/research/wallet_copy_pipeline_resume_state.json")),
                    "--history-state",
                    args.history_state,
                    "--paper-state",
                    args.paper_state,
                    "--wallet-event-log",
                    "data/research/wallet_copy_events.jsonl",
                    "--paper-event-log",
                    "data/research/wallet_copy_paper_events.jsonl",
                    "--wallet-fraction",
                    str(float(getattr(args, "wallet_fraction", 0.05))),
                    "--max-order-usd",
                    str(float(getattr(args, "max_order_usd", 2.0))),
                    "--policy-id",
                    "leaderboard_crypto_exact_copy_all_buys",
                    "--command-timeout-s",
                    str(resume_child_timeout_s),
                ],
                "runtime_budget": {
                    "scope_mode": "RESUMABLE_HISTORY_PAPER_SLICE",
                    "limit": int(args.pipeline_limit),
                    "pages_per_run": max(1, int(getattr(args, "pipeline_pages_per_resume_run", 1))),
                    "wallet_batch_size": max(1, int(getattr(args, "pipeline_wallet_batch_size", 1))),
                    "resume_state": str(
                        getattr(args, "pipeline_resume_state", "data/research/wallet_copy_pipeline_resume_state.json")
                    ),
                    "requested_full_pages": int(pipeline_pages),
                    "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
                    "child_command_timeout_s": resume_child_timeout_s,
                    "reserve_s": active_proof_reserve_s,
                    "status": "CHECKPOINTED_NOT_BOUNDED_AWAY",
                },
            }
        )
    commands.extend(
        [
        {
            "name": "leaderboard_sweeper_profile",
            "purpose": "classify close/post-close sweeper signatures among discovered wallets",
            "acceptable_returncodes": (0,),
            "argv": [
                PYTHON,
                "scripts/analyze_wallet_sweeper_profile.py",
                "--history-state",
                args.history_state,
                "--output",
                "data/research/wallet_copy_sweeper_profile_state.json",
            ],
        },
        {
            "name": "leaderboard_cross_wallet_research",
            "purpose": "build cross-wallet features, consensus, inventory, and resolution-backed paper scores",
            "acceptable_returncodes": (0,),
            "argv": [
                PYTHON,
                "scripts/analyze_wallet_copy_research.py",
                "--history-state",
                args.history_state,
                "--paper-state",
                args.paper_state,
                "--inventory-paper-state",
                args.inventory_paper_state,
                "--resolutions",
                args.resolutions,
                "--max-unresolved-ratio",
                str(float(args.max_unresolved_ratio)),
                "--output",
                args.research_state,
            ],
        },
        {
            "name": "leaderboard_ml_dataset",
            "purpose": "export wallet-copy feature rows and labels for ML/reverse-engineering",
            "acceptable_returncodes": (0,),
            "argv": [
                PYTHON,
                "scripts/export_wallet_copy_dataset.py",
                "--history-state",
                args.history_state,
                "--paper-state",
                args.paper_state,
                "--resolutions",
                args.resolutions,
                "--output",
                args.ml_dataset,
                "--include-unresolved",
            ],
        },
        {
            "name": "leaderboard_profit_admission",
            "purpose": "search single-wallet, consensus, and inventory policies with raw-baseline guards",
            "acceptable_returncodes": (0,),
            "argv": [
                PYTHON,
                "scripts/run_wallet_copy_profit_engine.py",
                "--history-state",
                args.history_state,
                "--resolutions",
                args.resolutions,
                "--output",
                _profit_command_output_state(args),
                "--live-tracker-state",
                args.live_tracker_state,
                *common_profit_args,
                "--live-today-sprint-operator-approval-id",
                LIVE_TODAY_SPRINT_OPERATOR_APPROVAL_ID,
            ],
        },
        ]
    )
    return commands


def build_repair_plan(pre_audit: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    adaptive_added = False
    active_hotlane_scope_refresh_added = False
    active_forward_probe_burnin_added = False
    active_hotlane_tracker_measurement_added = False
    live_ready_unlock = _live_ready_unlock_context(args)
    live_ready_unlock_active = bool(live_ready_unlock.get("active"))
    setattr(args, "_live_ready_unlock_context", live_ready_unlock)
    setattr(
        args,
        "_live_ready_unlock_probe_ranks_effective",
        int(live_ready_unlock.get("effective_candidate_forward_probe_ranks") or getattr(args, "candidate_forward_probe_ranks", 1)),
    )
    missing = _missing_paths(pre_audit)
    source_route_state_present = hasattr(args, "source_route_state")
    source_route_blocks_heavy = _source_route_blocks_heavy_work(args)
    leaderboard_needs_refresh = (
        bool(args.force_leaderboard)
        or _check_status(pre_audit, "leaderboard_discovery") != "PASS"
        or "leaderboard_state" in missing
        or _is_stale(args.leaderboard_state, args.leaderboard_stale_s)
    )
    research_checks = {
        "history_ingest",
        "paper_order_lifecycle",
        "research_cross_wallet",
        "ml_dataset",
        "profit_admission",
        "lifecycle_realized_pnl",
    }
    research_needs_refresh = bool(args.force_research) or any(
        _check_status(pre_audit, check_name) != "PASS" for check_name in research_checks
    )
    research_needs_refresh = research_needs_refresh or bool(
        {"history_state", "paper_state", "research_state", "ml_dataset", "profit_state"} & missing
    )
    canonical_gamma_needs_refresh = bool(source_route_state_present) and _profit_state_needs_canonical_gamma_resolution(args)
    tracker_needs_refresh = (
        bool(args.force_tracker)
        or _check_status(pre_audit, "live_tracker_copy_efficiency") != "PASS"
        or _check_status(pre_audit, "live_admission_truth") != "PASS"
        or "live_tracking_state" in missing
        or _is_stale(args.live_tracker_state, args.tracker_stale_s)
    )
    if source_route_blocks_heavy:
        leaderboard_needs_refresh = False
        research_needs_refresh = False
        canonical_gamma_needs_refresh = False
        tracker_needs_refresh = False

    if source_route_state_present:
        plan.append(_source_route_probe_command(args))

    proof_led_command_added = False
    if live_ready_unlock_active and not source_route_blocks_heavy:
        proof_led_command = _proof_led_candidate_history_command(args)
        if proof_led_command is not None:
            plan.append(proof_led_command)
            proof_led_command_added = True

    leaderboard_discovery_added = False
    if leaderboard_needs_refresh and not source_route_blocks_heavy:
        plan.append(_leaderboard_discovery_command(args))
        leaderboard_discovery_added = True

    pipeline_pages = 0 if args.deep_research else int(args.pipeline_pages)
    # Deep runs widen coverage by removing candidate/history limits, but they
    # must not silently switch the policy grid. Runtime proof rows are keyed by
    # policy_id, so changing fast -> default can orphan the exact proof-led
    # candidate the heartbeat is trying to re-probe.
    policy_preset = args.policy_preset
    max_wallets_for_search = 0 if args.deep_research else int(args.max_wallets_for_search)
    max_single = 0 if args.deep_research else int(args.max_single_wallet_candidate_intents)
    max_multi = 0 if args.deep_research else int(args.max_multi_wallet_base_intents)

    if (
        tracker_needs_refresh
        and not source_route_blocks_heavy
        and not getattr(args, "skip_live_tracker", False)
        and not getattr(args, "skip_active_hotlane", False)
    ):
        active_hotlane_state = str(
            getattr(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json")
        )
        active_hotlane_registry = str(
            getattr(args, "active_hotlane_registry", "data/research/wallet_copy_active_hotlane_registry.json")
        )
        active_hotlane_live_tracker_state = str(
            getattr(
                args,
                "active_hotlane_live_tracker_state",
                "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
            )
        )
        active_hotlane_live_tracker_event_log = str(
            getattr(
                args,
                "active_hotlane_live_tracker_event_log",
                "data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
            )
        )
        active_hotlane_paper_state = str(
            getattr(args, "active_hotlane_paper_state", "data/research/wallet_copy_active_hotlane_paper_state.json")
        )
        active_hotlane_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events.jsonl",
            )
        )
        active_hotlane_tracker_time_replay_paper_state = str(
            getattr(
                args,
                "active_hotlane_tracker_time_replay_paper_state",
                "data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_state.json",
            )
        )
        active_hotlane_tracker_time_replay_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_tracker_time_replay_paper_event_log",
                "data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_events.jsonl",
            )
        )
        active_hotlane_single_wallet_exact_copy_paper_state = str(
            getattr(
                args,
                "active_hotlane_single_wallet_exact_copy_paper_state",
                "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_state.json",
            )
        )
        active_hotlane_single_wallet_exact_copy_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_single_wallet_exact_copy_paper_event_log",
                "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
            )
        )
        active_hotlane_all_order_exact_copy_paper_state = str(
            getattr(
                args,
                "active_hotlane_all_order_exact_copy_paper_state",
                "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
            )
        )
        active_hotlane_all_order_exact_copy_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_all_order_exact_copy_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy.jsonl",
            )
        )
        active_hotlane_all_order_tactic_replay_paper_state = str(
            getattr(
                args,
                "active_hotlane_all_order_tactic_replay_paper_state",
                "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
            )
        )
        active_hotlane_all_order_tactic_replay_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_all_order_tactic_replay_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy_aggressive_tactic_replay.jsonl",
            )
        )
        live_tracker_event_log = str(
            getattr(args, "live_tracker_event_log", "data/research/wallet_copy_live_tracking_events.jsonl")
        )
        plan.append(
            _active_hotlane_scope_refresh_command(
                args,
                active_hotlane_state=active_hotlane_state,
                active_hotlane_registry=active_hotlane_registry,
                active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
                live_tracker_event_log=live_tracker_event_log,
            )
        )
        active_hotlane_scope_refresh_added = True
        plan.append(
            _active_hotlane_tracker_command(
                args,
                active_hotlane_registry=active_hotlane_registry,
                active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
                active_hotlane_live_tracker_event_log=active_hotlane_live_tracker_event_log,
                active_hotlane_paper_state=active_hotlane_paper_state,
                active_hotlane_paper_event_log=active_hotlane_paper_event_log,
                active_hotlane_tracker_time_replay_paper_state=active_hotlane_tracker_time_replay_paper_state,
                active_hotlane_tracker_time_replay_paper_event_log=active_hotlane_tracker_time_replay_paper_event_log,
                active_hotlane_single_wallet_exact_copy_paper_state=active_hotlane_single_wallet_exact_copy_paper_state,
                active_hotlane_single_wallet_exact_copy_paper_event_log=active_hotlane_single_wallet_exact_copy_paper_event_log,
                active_hotlane_all_order_exact_copy_paper_state=active_hotlane_all_order_exact_copy_paper_state,
                active_hotlane_all_order_exact_copy_paper_event_log=active_hotlane_all_order_exact_copy_paper_event_log,
                active_hotlane_all_order_tactic_replay_paper_state=active_hotlane_all_order_tactic_replay_paper_state,
                active_hotlane_all_order_tactic_replay_paper_event_log=active_hotlane_all_order_tactic_replay_paper_event_log,
                purpose_suffix=(
                    "pre-forward direct hot-lane burn-in so active-forward candidate selection uses fresh "
                    "direct CLOB copyability evidence instead of stale prior probe rows"
                ),
            )
        )
        active_hotlane_tracker_measurement_added = True
        plan.append(
            _active_hotlane_scope_refresh_after_tracker_command(
                args,
                active_hotlane_state=active_hotlane_state,
                active_hotlane_registry=active_hotlane_registry,
                active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
                live_tracker_event_log=active_hotlane_live_tracker_event_log,
            )
        )
        plan.append(_active_forward_probe_policy_command(args, active_hotlane_state=active_hotlane_state))
        plan.append(
            _active_forward_probe_tracker_command(
                args,
                active_hotlane_registry=active_hotlane_registry,
            )
        )
        active_forward_probe_burnin_added = True

    if research_needs_refresh:
        if not leaderboard_discovery_added:
            plan.append(_leaderboard_discovery_command(args))
            leaderboard_discovery_added = True
        plan.extend(
            _leaderboard_pipeline_stage_commands(
                args,
                pipeline_pages=pipeline_pages,
                policy_preset=policy_preset,
                max_wallets_for_search=max_wallets_for_search,
                max_single_wallet_candidate_intents=max_single,
                max_multi_wallet_base_intents=max_multi,
                include_proof_led=not proof_led_command_added,
            )
        )
    elif leaderboard_needs_refresh:
        if not leaderboard_discovery_added:
            plan.append(_leaderboard_discovery_command(args))
            leaderboard_discovery_added = True
    elif live_ready_unlock_active and not source_route_blocks_heavy and not proof_led_command_added:
        proof_led_command = _proof_led_candidate_history_command(args)
        if proof_led_command is not None:
            plan.append(proof_led_command)

    if canonical_gamma_needs_refresh:
        plan.append(_canonical_gamma_resolution_refresh_command(args))

    if not getattr(args, "skip_active_hotlane", False):
        active_hotlane_state = str(getattr(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json"))
        active_hotlane_registry = str(
            getattr(args, "active_hotlane_registry", "data/research/wallet_copy_active_hotlane_registry.json")
        )
        active_hotlane_live_tracker_state = str(
            getattr(
                args,
                "active_hotlane_live_tracker_state",
                "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
            )
        )
        active_hotlane_live_tracker_event_log = str(
            getattr(
                args,
                "active_hotlane_live_tracker_event_log",
                "data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
            )
        )
        active_hotlane_paper_state = str(
            getattr(args, "active_hotlane_paper_state", "data/research/wallet_copy_active_hotlane_paper_state.json")
        )
        active_hotlane_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events.jsonl",
            )
        )
        active_hotlane_tracker_time_replay_paper_state = str(
            getattr(
                args,
                "active_hotlane_tracker_time_replay_paper_state",
                "data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_state.json",
            )
        )
        active_hotlane_tracker_time_replay_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_tracker_time_replay_paper_event_log",
                "data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_events.jsonl",
            )
        )
        active_hotlane_single_wallet_exact_copy_paper_state = str(
            getattr(
                args,
                "active_hotlane_single_wallet_exact_copy_paper_state",
                "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_state.json",
            )
        )
        active_hotlane_single_wallet_exact_copy_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_single_wallet_exact_copy_paper_event_log",
                "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
            )
        )
        active_hotlane_all_order_exact_copy_paper_state = str(
            getattr(
                args,
                "active_hotlane_all_order_exact_copy_paper_state",
                "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
            )
        )
        active_hotlane_all_order_exact_copy_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_all_order_exact_copy_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy.jsonl",
            )
        )
        active_hotlane_all_order_tactic_replay_paper_state = str(
            getattr(
                args,
                "active_hotlane_all_order_tactic_replay_paper_state",
                "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
            )
        )
        active_hotlane_all_order_tactic_replay_paper_event_log = str(
            getattr(
                args,
                "active_hotlane_all_order_tactic_replay_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy_aggressive_tactic_replay.jsonl",
            )
        )
        live_tracker_event_log = str(
            getattr(args, "live_tracker_event_log", "data/research/wallet_copy_live_tracking_events.jsonl")
        )
        if not active_hotlane_scope_refresh_added:
            plan.append(
                _active_hotlane_scope_refresh_command(
                    args,
                    active_hotlane_state=active_hotlane_state,
                    active_hotlane_registry=active_hotlane_registry,
                    active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
                    live_tracker_event_log=live_tracker_event_log,
                )
            )
            active_hotlane_scope_refresh_added = True
        if tracker_needs_refresh and not args.skip_live_tracker:
            active_hotlane_tracker_reason = _active_hotlane_tracker_measurement_reason(
                active_hotlane_live_tracker_state,
                args.tracker_stale_s,
            )
            active_hotlane_tracker_needs_measurement = (
                active_hotlane_tracker_reason is not None
                and not active_hotlane_tracker_measurement_added
            )
            active_hotlane_tracker_command = _active_hotlane_tracker_command(
                args,
                active_hotlane_registry=active_hotlane_registry,
                active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
                active_hotlane_live_tracker_event_log=active_hotlane_live_tracker_event_log,
                active_hotlane_paper_state=active_hotlane_paper_state,
                active_hotlane_paper_event_log=active_hotlane_paper_event_log,
                active_hotlane_tracker_time_replay_paper_state=active_hotlane_tracker_time_replay_paper_state,
                active_hotlane_tracker_time_replay_paper_event_log=active_hotlane_tracker_time_replay_paper_event_log,
                active_hotlane_single_wallet_exact_copy_paper_state=active_hotlane_single_wallet_exact_copy_paper_state,
                active_hotlane_single_wallet_exact_copy_paper_event_log=active_hotlane_single_wallet_exact_copy_paper_event_log,
                active_hotlane_all_order_exact_copy_paper_state=active_hotlane_all_order_exact_copy_paper_state,
                active_hotlane_all_order_exact_copy_paper_event_log=active_hotlane_all_order_exact_copy_paper_event_log,
                active_hotlane_all_order_tactic_replay_paper_state=active_hotlane_all_order_tactic_replay_paper_state,
                active_hotlane_all_order_tactic_replay_paper_event_log=active_hotlane_all_order_tactic_replay_paper_event_log,
                purpose_suffix=(
                    active_hotlane_tracker_reason
                    if active_hotlane_tracker_reason is not None
                    else "adaptive measurement was explicitly skipped"
                ),
            )
            if active_hotlane_tracker_needs_measurement:
                plan.append(active_hotlane_tracker_command)
                active_hotlane_tracker_measurement_added = True
            if not active_forward_probe_burnin_added:
                plan.append(_active_forward_probe_policy_command(args, active_hotlane_state=active_hotlane_state))
                plan.append(
                    _active_forward_probe_tracker_command(
                        args,
                        active_hotlane_registry=active_hotlane_registry,
                    )
                )
                active_forward_probe_burnin_added = True
            if not getattr(args, "skip_adaptive_bot", False):
                plan.append(
                    _active_hotlane_tick_command(
                        args,
                        active_hotlane_registry=active_hotlane_registry,
                        active_hotlane_live_tracker_state=active_hotlane_live_tracker_state,
                        active_hotlane_live_tracker_event_log=active_hotlane_live_tracker_event_log,
                        active_hotlane_paper_state=active_hotlane_paper_state,
                        active_hotlane_paper_event_log=active_hotlane_paper_event_log,
                        active_hotlane_tracker_time_replay_paper_state=(
                            active_hotlane_tracker_time_replay_paper_state
                        ),
                        active_hotlane_tracker_time_replay_paper_event_log=(
                            active_hotlane_tracker_time_replay_paper_event_log
                        ),
                        active_hotlane_all_order_exact_copy_paper_state=(
                            active_hotlane_all_order_exact_copy_paper_state
                        ),
                        active_hotlane_all_order_exact_copy_paper_event_log=(
                            active_hotlane_all_order_exact_copy_paper_event_log
                        ),
                        active_hotlane_all_order_tactic_replay_paper_state=(
                            active_hotlane_all_order_tactic_replay_paper_state
                        ),
                        active_hotlane_all_order_tactic_replay_paper_event_log=(
                            active_hotlane_all_order_tactic_replay_paper_event_log
                        ),
                    )
                )
                adaptive_added = True
            else:
                if not active_hotlane_tracker_needs_measurement and not active_hotlane_tracker_measurement_added:
                    plan.append(active_hotlane_tracker_command)
                    active_hotlane_tracker_measurement_added = True

    if tracker_needs_refresh and not getattr(args, "skip_registry_sweep", False) and not live_ready_unlock_active:
        registry_sweep_state = str(
            getattr(args, "registry_sweep_state", "data/research/wallet_copy_registry_sweep_live_tracking_state.json")
        )
        registry_sweep_event_log = str(
            getattr(args, "registry_sweep_event_log", "data/research/wallet_copy_registry_sweep_live_tracking_events.jsonl")
        )
        registry_sweep_paper_state = str(
            getattr(args, "registry_sweep_paper_state", "data/research/wallet_copy_registry_sweep_paper_state.json")
        )
        registry_sweep_paper_event_log = str(
            getattr(args, "registry_sweep_paper_event_log", "data/research/wallet_copy_registry_sweep_paper_events.jsonl")
        )
        registry_sweep_tracker_time_replay_paper_state = str(
            getattr(
                args,
                "registry_sweep_tracker_time_replay_paper_state",
                "data/research/wallet_copy_registry_sweep_tracker_time_replay_paper_state.json",
            )
        )
        registry_sweep_tracker_time_replay_paper_event_log = str(
            getattr(
                args,
                "registry_sweep_tracker_time_replay_paper_event_log",
                "data/research/wallet_copy_registry_sweep_tracker_time_replay_paper_events.jsonl",
            )
        )
        registry_sweep_all_order_paper_state = str(
            getattr(
                args,
                "registry_sweep_all_order_paper_state",
                "data/research/wallet_copy_registry_sweep_all_order_exact_copy_paper_state.json",
            )
        )
        registry_sweep_all_order_paper_event_log = str(
            getattr(
                args,
                "registry_sweep_all_order_paper_event_log",
                "data/research/wallet_copy_registry_sweep_all_order_exact_copy_paper_events.jsonl",
            )
        )
        registry_sweep_all_order_tactic_replay_paper_state = str(
            getattr(
                args,
                "registry_sweep_all_order_tactic_replay_paper_state",
                "data/research/wallet_copy_registry_sweep_all_order_tactic_replay_paper_state.json",
            )
        )
        registry_sweep_all_order_tactic_replay_paper_event_log = str(
            getattr(
                args,
                "registry_sweep_all_order_tactic_replay_paper_event_log",
                "data/research/wallet_copy_registry_sweep_all_order_tactic_replay_paper_events.jsonl",
            )
        )
        registry_sweep_limit = int(getattr(args, "registry_sweep_limit", 20))
        registry_sweep_pages = int(getattr(args, "registry_sweep_pages", 1))
        registry_sweep_iterations = int(getattr(args, "registry_sweep_iterations", 1))
        registry_sweep_max_runtime_s = float(getattr(args, "registry_sweep_max_runtime_s", 300.0))
        registry_sweep_effective_max_runtime_s = _child_runtime_within_command_timeout(
            registry_sweep_max_runtime_s,
            float(getattr(args, "command_timeout_s", 240.0)),
            min_runtime_s=30.0,
            reserve_s=15.0,
        )
        registry_sweep_max_poll_runtime_s = float(getattr(args, "registry_sweep_max_poll_runtime_s", 25.0))
        registry_sweep_effective_max_poll_runtime_s = _poll_runtime_within_child_runtime(
            registry_sweep_max_poll_runtime_s,
            registry_sweep_effective_max_runtime_s,
            registry_sweep_iterations,
            reserve_per_iteration_s=1.0,
        )
        registry_sweep_max_wallets = int(getattr(args, "registry_sweep_max_wallets", 8))
        registry_sweep_parallel_wallet_fetches = int(getattr(args, "registry_sweep_parallel_wallet_fetches", 4))
        plan.append(
            {
                "name": "wallet_copy_full_registry_tracker_sweep",
                "purpose": (
                    "paper-only rotating breadth slice across the full configured wallet registry; keeps low-latency "
                    "hot-lane small while measuring coverage, CLOB fillability, onchain receipts, and all-order "
                    "copyability for wallets that are not currently selected into the active hot-lane"
                ),
                "acceptable_returncodes": (0, 2),
                "runtime_budget": {
                    "scope_mode": "ROTATING_REGISTRY_SLICE_NOT_FULL_CYCLE",
                    "limit": max(1, registry_sweep_limit),
                    "pages": max(1, registry_sweep_pages),
                    "iterations": max(1, registry_sweep_iterations),
                    "max_wallets": max(1, registry_sweep_max_wallets),
                    "parallel_wallet_fetches": max(1, registry_sweep_parallel_wallet_fetches),
                    "requested_child_max_runtime_s": registry_sweep_max_runtime_s,
                    "effective_child_max_runtime_s": registry_sweep_effective_max_runtime_s,
                    "requested_poll_max_runtime_s": registry_sweep_max_poll_runtime_s,
                    "effective_poll_max_runtime_s": registry_sweep_effective_max_poll_runtime_s,
                    "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
                    "reserve_s": 15.0,
                    "status": (
                        "BOUNDED_TO_PARENT_TIMEOUT"
                        if (
                            registry_sweep_effective_max_runtime_s < registry_sweep_max_runtime_s
                            or registry_sweep_effective_max_poll_runtime_s < registry_sweep_max_poll_runtime_s
                        )
                        else "UNCHANGED"
                    ),
                },
                "argv": [
                    PYTHON,
                    "scripts/run_wallet_live_tracker.py",
                    "--registry",
                    args.registry,
                    "--state",
                    registry_sweep_state,
                    "--event-log",
                    registry_sweep_event_log,
                    "--paper-state",
                    registry_sweep_paper_state,
                    "--paper-event-log",
                    registry_sweep_paper_event_log,
                    "--tracker-time-replay-paper-state",
                    registry_sweep_tracker_time_replay_paper_state,
                    "--tracker-time-replay-paper-event-log",
                    registry_sweep_tracker_time_replay_paper_event_log,
                    "--all-order-exact-copy-paper-state",
                    registry_sweep_all_order_paper_state,
                    "--all-order-exact-copy-paper-event-log",
                    registry_sweep_all_order_paper_event_log,
                    "--all-order-tactic-replay-paper-state",
                    registry_sweep_all_order_tactic_replay_paper_state,
                    "--all-order-tactic-replay-paper-event-log",
                    registry_sweep_all_order_tactic_replay_paper_event_log,
                    "--profit-policy-state",
                    args.profit_state,
                    "--seed-before-poll",
                    "--seed-history-state",
                    args.history_state,
                    "--limit",
                    str(max(1, registry_sweep_limit)),
                    "--pages",
                    str(max(1, registry_sweep_pages)),
                    "--data-api-timeout-s",
                    str(float(getattr(args, "data_api_timeout_s", 2.0))),
                    "--max-poll-runtime-s",
                    str(registry_sweep_effective_max_poll_runtime_s),
                    "--iterations",
                    str(max(1, registry_sweep_iterations)),
                    "--poll-interval-s",
                    "0",
                    "--max-runtime-s",
                    str(registry_sweep_effective_max_runtime_s),
                    "--enable-clob-books",
                    "--enable-onchain-receipts",
                    "--onchain-timeout-s",
                    str(min(float(getattr(args, "onchain_timeout_s", 8.0)), 1.0)),
                    "--admission-mode",
                    "--strict-mirror-coverage",
                    "--no-use-profit-search-scope",
                    "--max-wallets",
                    str(max(1, registry_sweep_max_wallets)),
                    "--parallel-wallet-fetches",
                    str(max(1, registry_sweep_parallel_wallet_fetches)),
                    "--parallel-data-api-sources",
                    "--clob-timeout-s",
                    str(min(float(args.clob_timeout_s), 1.0)),
                    "--gamma-timeout-s",
                    str(min(float(args.gamma_timeout_s), 1.0)),
                    "--market-ws-jsonl",
                    str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
                ],
            }
        )

    if tracker_needs_refresh and not args.skip_live_tracker:
        live_admission_check = (
            (pre_audit.get("checks") or {}).get("live_admission_truth")
            if isinstance(pre_audit.get("checks"), dict)
            else {}
        )
        live_truth_blockers = (
            live_admission_check.get("live_tracker_truth_blockers")
            if isinstance(live_admission_check, dict)
            else []
        ) or []
        tracker_iterations = max(1, int(args.live_tracker_iterations))
        tracker_requested_runtime_s = float(getattr(args, "live_tracker_max_runtime_s", 60.0))
        tracker_effective_runtime_s = _child_runtime_within_command_timeout(
            tracker_requested_runtime_s,
            float(getattr(args, "command_timeout_s", 240.0)),
            min_runtime_s=20.0,
            reserve_s=5.0,
        )
        tracker_requested_poll_runtime_s = float(getattr(args, "live_tracker_max_poll_runtime_s", 30.0))
        tracker_effective_poll_runtime_s = _poll_runtime_within_child_runtime(
            tracker_requested_poll_runtime_s,
            tracker_effective_runtime_s,
            tracker_iterations,
            reserve_per_iteration_s=1.0,
        )
        tracker_child_command_timeout_s = min(
            max(1.0, float(getattr(args, "command_timeout_s", 240.0))),
            tracker_effective_runtime_s + 30.0,
        )
        paper_live_tracker_command = {
            "name": "paper_live_tracker_measurement",
            "purpose": "measure current paper copy-efficiency with CLOB evidence; rc=2 is expected when blockers remain",
            "acceptable_returncodes": (0, 2),
            "runtime_budget": {
                "scope_mode": "CANONICAL_PAPER_LIVE_TRACKER",
                "iterations": tracker_iterations,
                "requested_child_max_runtime_s": tracker_requested_runtime_s,
                "effective_child_max_runtime_s": tracker_effective_runtime_s,
                "requested_poll_max_runtime_s": tracker_requested_poll_runtime_s,
                "effective_poll_max_runtime_s": tracker_effective_poll_runtime_s,
                "parent_command_timeout_s": float(getattr(args, "command_timeout_s", 240.0)),
                "child_command_timeout_s": round(tracker_child_command_timeout_s, 3),
                "child_command_cleanup_reserve_s": 30.0,
                "reserve_s": 5.0,
                "status": (
                    "BOUNDED_TO_PARENT_TIMEOUT_OR_POLL_BUDGET"
                    if (
                        tracker_effective_runtime_s < tracker_requested_runtime_s
                        or tracker_effective_poll_runtime_s < tracker_requested_poll_runtime_s
                    )
                    else "UNCHANGED"
                ),
            },
            "argv": [
                PYTHON,
                "scripts/run_wallet_live_tracker.py",
                "--registry",
                args.registry,
                "--state",
                args.live_tracker_state,
                "--profit-policy-state",
                args.profit_state,
                "--seed-before-poll",
                "--seed-history-state",
                args.history_state,
                "--limit",
                str(int(args.live_tracker_limit)),
                "--pages",
                str(int(args.live_tracker_pages)),
                "--data-api-timeout-s",
                str(float(getattr(args, "data_api_timeout_s", 2.0))),
                "--max-poll-runtime-s",
                str(tracker_effective_poll_runtime_s),
                "--iterations",
                str(tracker_iterations),
                "--poll-interval-s",
                str(float(args.live_tracker_poll_interval_s)),
                "--max-runtime-s",
                str(tracker_effective_runtime_s),
                "--enable-clob-books",
                "--admission-mode",
                "--strict-mirror-coverage",
                "--no-use-profit-search-scope",
                "--track-blocked-profit-policy",
                "--max-wallets",
                str(int(args.live_tracker_max_wallets)),
                "--parallel-wallet-fetches",
                str(
                    max(
                        1,
                        int(
                            getattr(
                                args,
                                "live_tracker_parallel_wallet_fetches",
                                min(int(args.live_tracker_max_wallets), 4),
                            )
                        ),
                    )
                ),
                "--parallel-data-api-sources",
                "--clob-timeout-s",
                str(float(args.clob_timeout_s)),
                "--gamma-timeout-s",
                str(float(args.gamma_timeout_s)),
                "--market-ws-jsonl",
                str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
                "--enable-onchain-receipts",
                "--onchain-timeout-s",
                str(min(float(getattr(args, "onchain_timeout_s", 8.0)), 1.0)),
            ],
        }
        plan.append(_profit_command(args, name="profit_admission_before_candidate_forward_tracker"))
        if any(
            str(blocker)
            in {"candidate_source_wallet_copy_efficiency_missing", "candidate_policy_copy_efficiency_missing"}
            for blocker in live_truth_blockers
        ) or live_ready_unlock_active:
            plan.append(_candidate_forward_tracker_dynamic_command())
        plan.append(_profit_command(args, name="profit_admission_after_tracker"))
        plan.append(paper_live_tracker_command)
    elif research_needs_refresh:
        plan.append(_profit_command(args, name="profit_admission_after_research"))

    if not source_route_blocks_heavy and not getattr(args, "skip_adaptive_bot", False) and not adaptive_added:
        use_active_hotlane_feed = not getattr(args, "skip_active_hotlane", False)
        live_tracker_state_for_adaptive = (
            str(
                getattr(
                    args,
                    "active_hotlane_live_tracker_state",
                    "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
                )
            )
            if use_active_hotlane_feed
            else str(getattr(args, "live_tracker_state", "data/research/wallet_copy_live_tracking_state.json"))
        )
        live_tracker_event_log = (
            str(
                getattr(
                    args,
                    "active_hotlane_live_tracker_event_log",
                    "data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
                )
            )
            if use_active_hotlane_feed
            else str(getattr(args, "live_tracker_event_log", "data/research/wallet_copy_live_tracking_events.jsonl"))
        )
        plan.append(
            _adaptive_bot_command(
                args,
                live_tracker_state_for_adaptive=live_tracker_state_for_adaptive,
                live_tracker_event_log=live_tracker_event_log,
            )
        )

    return plan


def _backlog_key(item: dict[str, Any]) -> str:
    return "|".join(
        str(item.get(key) or "")
        for key in ("area", "file", "function", "action")
    )


def _backlog_group(item: dict[str, Any]) -> str:
    blockers = item.get("blockers") if isinstance(item.get("blockers"), list) else []
    if str(item.get("area") or "").lower() == "wallet-copy source route" or any(
        str(blocker).startswith("source_route_") for blocker in blockers
    ):
        return "wallet-copy source route"
    return "|".join(str(item.get(key) or "") for key in ("area", "file", "function"))


def _backlog_id(key: str) -> str:
    digest = hashlib.sha1(str(key).encode("utf-8")).hexdigest()[:12]
    return f"wcbl_{digest}"


def _non_green_blockers(non_green: list[Any]) -> list[str]:
    blockers: list[str] = []
    for row in non_green:
        if not isinstance(row, dict):
            continue
        check = str(row.get("check") or row.get("name") or "").strip()
        status = str(row.get("status") or "").strip()
        fingerprint = str(row.get("fingerprint") or "").strip()
        if check and status:
            blockers.append(f"{check}:{status}")
        elif fingerprint:
            blockers.append(fingerprint)
    return sorted(set(blockers))


def _replace_stale_status_text(value: str) -> str:
    stale_hold = "H" + "OLD"
    if value == stale_hold:
        return "ANALYZE"
    if value == f"{stale_hold}_RESEARCH_ONLY":
        return "ANALYZE_RESEARCH_ONLY"
    replacements = (
        (f"{stale_hold}_RESEARCH_ONLY", "ANALYZE_RESEARCH_ONLY"),
        (f"live admission {stale_hold}", "live admission ANALYZE/CORRECTION"),
        (f"live readiness {stale_hold}", "live readiness ANALYZE/CORRECTION"),
        (f"{stale_hold} live readiness", "ANALYZE/CORRECTION live readiness"),
        (f"{stale_hold} live admission", "ANALYZE/CORRECTION live admission"),
        (f"{stale_hold} until", "ANALYZE/CORRECTION until"),
        (f"must stay {stale_hold}", "must stay ANALYZE/CORRECTION"),
        (f"stay {stale_hold}", "stay ANALYZE/CORRECTION"),
        (f":{stale_hold}", ":ANALYZE"),
        (f'"{stale_hold}"', '"ANALYZE"'),
        (f"explicit_research_{stale_hold.lower()}", "explicit_research_analyze"),
    )
    result = value
    for old, new in replacements:
        result = result.replace(old, new)
    return result


def _replace_stale_wallet_copy_command_text(value: str) -> str:
    result = value
    had_stale_profit_scope = "--use-profit-search-scope" in result
    result = result.replace("--use-profit-search-scope", "--no-use-profit-search-scope")
    active_forward_probe_command = (
        "wallet_copy_active_forward_probe" in result
        or "active_forward_probe" in result
        or "active forward candidate probe" in result.lower()
    )
    if (had_stale_profit_scope or active_forward_probe_command) and "--max-wallets 1" in result:
        if "--parallel-wallet-fetches" in result:
            result = result.replace("--max-wallets 1", "--max-wallets 4")
        else:
            result = result.replace("--max-wallets 1", "--max-wallets 4 --parallel-wallet-fetches 4")
    if active_forward_probe_command and "scripts/run_wallet_live_tracker.py" in result:
        result = result.replace("--iterations 20", "--iterations 1")
        result = result.replace("--max-runtime-s 60.0", "--max-runtime-s 180")
        result = result.replace("--max-runtime-s 60", "--max-runtime-s 180")
        if "--max-poll-runtime-s" not in result and "--enable-clob-books" in result:
            result = result.replace("--enable-clob-books", "--max-poll-runtime-s 120 --enable-clob-books")
        result = result.replace("--max-poll-runtime-s 30.0", "--max-poll-runtime-s 120")
        result = result.replace("--max-poll-runtime-s 30", "--max-poll-runtime-s 120")
        if "--profit-policy-candidate-only" not in result and "--max-wallets" in result:
            result = result.replace("--max-wallets", "--profit-policy-candidate-only --max-wallets", 1)
        if "--parallel-data-api-sources" not in result and "--enable-clob-books" in result:
            result = result.replace("--enable-clob-books", "--parallel-data-api-sources --enable-clob-books", 1)
    return result


def _normalize_stale_status_terms(value: Any) -> Any:
    if isinstance(value, str):
        return _replace_stale_wallet_copy_command_text(_replace_stale_status_text(value))
    if isinstance(value, list):
        return [_normalize_stale_status_terms(row) for row in value]
    if isinstance(value, dict):
        return {key: _normalize_stale_status_terms(row) for key, row in value.items()}
    return value


def _normalize_backlog_action(action: dict[str, Any], *, non_green: list[Any], generated_at: str) -> dict[str, Any]:
    normalized = _normalize_stale_status_terms(dict(action))
    normalized["area"] = str(normalized.get("area") or "wallet-copy workflow")
    normalized["file"] = str(normalized.get("file") or "scripts/audit_wallet_copy_learning_logs.py")
    normalized["function"] = str(normalized.get("function") or "build_learning_log_audit")
    if not normalized.get("action"):
        blockers = _non_green_blockers(non_green)
        normalized["action"] = (
            "resolve non-green wallet-copy workflow evidence with a bounded fix, sharper measurement, "
            "or a more specific code-level backlog item"
        )
        if blockers:
            normalized["action"] += f"; current blockers: {', '.join(blockers[:5])}"
    if not normalized.get("verify"):
        normalized["verify"] = "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240"
    key = _backlog_key(normalized)
    blockers = _non_green_blockers(non_green)
    normalized.update(
        {
            "id": normalized.get("id") or _backlog_id(key),
            "key": key,
            "generated_at": generated_at,
            "last_seen_at": generated_at,
            "status": "OPEN",
            "source": "wallet_copy_autonomous_repair",
            "severity": normalized.get("severity") or ("P1" if any(":FAIL" in blocker for blocker in blockers) else "P2"),
            "blockers": normalized.get("blockers") or blockers,
            "next_command": normalized.get("next_command") or normalized.get("verify"),
            "non_green_checks": non_green,
        }
    )
    return normalized


def _clean_runtime_proof_row(row: dict[str, Any]) -> bool:
    return row.get("copy_status") == "COPIED_FILLED" and row.get("fill_source") == "clob_book_evidence"


def _runtime_proof_rows_for_scope(
    runtime_proof_index: dict[str, Any],
    *,
    candidate_id: str = "",
    policy_id: str = "",
    source_wallet: str = "",
) -> list[dict[str, Any]]:
    rows = runtime_proof_index.get("proof_rows") if isinstance(runtime_proof_index, dict) else []
    wallet = str(source_wallet or "").lower()
    matched: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or not _clean_runtime_proof_row(row):
            continue
        if candidate_id and str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "") != candidate_id:
            continue
        if policy_id and str(row.get("policy_id") or row.get("profit_policy_id") or "") != policy_id:
            continue
        if wallet and str(row.get("source_wallet") or "").lower() != wallet:
            continue
        matched.append(row)
    return matched


def _runtime_proof_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_events = {str(row.get("source_event_id") or "") for row in rows if row.get("source_event_id")}
    market_windows = {str(row.get("market_slug") or "") for row in rows if row.get("market_slug")}
    event_ages = sorted(float(row.get("event_age_s") or row.get("api_latency_s") or 0.0) for row in rows)
    p95_index = min(len(event_ages) - 1, int(round((len(event_ages) - 1) * 0.95))) if event_ages else None
    distinct_source_events = len(source_events)
    return {
        "runtime_proof_source": "candidate_runtime_proof_index",
        "runtime_proof_rows": len(rows),
        "runtime_proof_distinct_source_events": distinct_source_events,
        "runtime_proof_distinct_market_windows": len(market_windows),
        "runtime_proof_required_buy_copy_events": distinct_source_events,
        "runtime_proof_clob_filled_buy_copy_events": distinct_source_events,
        "runtime_proof_required_event_age_p95_s": event_ages[p95_index] if p95_index is not None else None,
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _paper_to_live_gap_report(
    *,
    profit: dict[str, Any],
    certificate: dict[str, Any],
    decision: dict[str, Any],
    source_route: dict[str, Any],
    active_all_order: dict[str, Any],
    candidate_forward_status: Any,
    candidate_forward_rank_results: list[dict[str, Any]],
    blockers: list[str],
    live_ready: bool,
) -> dict[str, Any]:
    """Explain why profitable paper/replay evidence did not become live-ready.

    This deliberately separates three surfaces that can otherwise look
    contradictory in heartbeat output: profitable replay candidates, runtime
    CLOB copyability, and global source-route/current-poll health.
    """

    def as_dict(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    def add_reason(code: str, *, severity: str = "BLOCKER", evidence: dict[str, Any] | None = None) -> None:
        if code in seen_reasons:
            return
        seen_reasons.add(code)
        reasons.append(
            {
                "code": code,
                "severity": severity,
                "evidence": evidence or {},
            }
        )

    best_candidate = as_dict(profit.get("best_candidate"))
    best_summary = as_dict(best_candidate.get("summary"))
    best_validation = as_dict(best_candidate.get("validation_summary"))
    best_window_metrics = as_dict(best_summary.get("window_metrics"))
    best_fill = as_dict(best_candidate.get("fill_evidence_summary") or best_candidate.get("executable_copy_summary"))
    best_resolution = as_dict(best_candidate.get("resolution_evidence_summary"))
    best_live_target = as_dict(best_candidate.get("live_target_profile"))
    best_live_observed = as_dict(best_live_target.get("observed"))
    certificate_paper = as_dict(certificate.get("paper_results"))
    copy_truth = as_dict(certificate.get("copy_truth"))
    route_status = str(source_route.get("status") or "")
    active_all_order_status = str(
        active_all_order.get("live_truth_status")
        or active_all_order.get("status")
        or ""
    )
    top_forward = candidate_forward_rank_results[0] if candidate_forward_rank_results else {}
    top_forward_current_poll = as_dict(top_forward.get("current_poll"))
    top_forward_tactic = as_dict(top_forward.get("all_order_tactic_replay"))

    best_candidate_id = str(best_candidate.get("candidate_id") or "")
    runtime_candidate_id = str(
        certificate_paper.get("candidate_id")
        or copy_truth.get("candidate_id")
        or decision.get("runtime_admission_candidate_id")
        or decision.get("forward_candidate_id")
        or ""
    )
    same_candidate = bool(best_candidate_id and runtime_candidate_id and best_candidate_id == runtime_candidate_id)

    reasons: list[dict[str, Any]] = []
    seen_reasons: set[str] = set()

    fallback_orders = _safe_int(best_fill.get("candidate_fallback_filled_orders"))
    clob_orders = _safe_int(best_fill.get("candidate_clob_backed_orders"))
    if best_candidate and (fallback_orders > 0 or (best_fill and clob_orders <= 0)):
        add_reason(
            "profitable_candidate_fallback_fill_only",
            evidence={
                "candidate_id": best_candidate_id or None,
                "candidate_fallback_filled_orders": fallback_orders,
                "candidate_clob_backed_orders": clob_orders,
                "fill_source_counts": best_fill.get("fill_source_counts") or {},
            },
        )
    research_only_orders = _safe_int(best_resolution.get("research_only_resolved_orders"))
    if research_only_orders > 0:
        add_reason(
            "profitable_candidate_research_only_resolution",
            evidence={
                "candidate_id": best_candidate_id or None,
                "research_only_resolved_orders": research_only_orders,
                "canonical_resolved_orders": _safe_int(best_resolution.get("canonical_resolved_orders")),
                "source_counts": best_resolution.get("source_counts") or {},
            },
        )
    if best_live_target and best_live_target.get("status") != "PASS":
        add_reason(
            "profitable_candidate_misses_live_targets",
            evidence={
                "candidate_id": best_candidate_id or None,
                "live_target_blockers": best_live_target.get("blockers") or [],
                "observed": {
                    "resolved_orders": best_live_observed.get("resolved_orders"),
                    "wr_pct": best_live_observed.get("wr_pct"),
                    "validation_wr_pct": best_live_observed.get("validation_wr_pct"),
                    "roi_pct": best_live_observed.get("roi_pct"),
                    "avg_orders_per_window": best_live_observed.get("avg_orders_per_window"),
                },
            },
        )
    if best_candidate_id and runtime_candidate_id and not same_candidate:
        add_reason(
            "profit_candidate_and_runtime_copy_candidate_are_different",
            evidence={
                "profit_candidate_id": best_candidate_id,
                "runtime_copy_candidate_id": runtime_candidate_id,
                "runtime_copy_candidate_source_wallet": (
                    certificate_paper.get("source_wallet")
                    or copy_truth.get("candidate_source_wallet")
                    or decision.get("forward_candidate_source_wallet")
                ),
            },
        )
    runtime_roi = _safe_float(certificate_paper.get("roi_pct"))
    runtime_wr = _safe_float(certificate_paper.get("wr_pct"))
    runtime_validation_wr = _safe_float(certificate_paper.get("validation_wr_pct"))
    runtime_pnl = _safe_float(certificate_paper.get("pnl_usd"))
    if (
        runtime_candidate_id
        and (
            (runtime_roi is not None and runtime_roi < 2.0)
            or (runtime_wr is not None and runtime_wr < 70.0)
            or (runtime_validation_wr is not None and runtime_validation_wr < 70.0)
            or (runtime_pnl is not None and runtime_pnl <= 0.0)
        )
    ):
        add_reason(
            "runtime_copy_candidate_not_profitable_enough",
            evidence={
                "candidate_id": runtime_candidate_id,
                "roi_pct": runtime_roi,
                "wr_pct": runtime_wr,
                "validation_wr_pct": runtime_validation_wr,
                "pnl_usd": runtime_pnl,
            },
        )
    if copy_truth and copy_truth.get("effective_live_tracker_truth_status") != "PASS":
        add_reason(
            "runtime_copy_truth_not_pass",
            evidence={
                "candidate_id": runtime_candidate_id or None,
                "effective_live_tracker_truth_status": copy_truth.get("effective_live_tracker_truth_status"),
                "required_buy_copy_events": copy_truth.get("required_buy_copy_events"),
                "clob_filled_buy_copy_events": copy_truth.get("clob_filled_buy_copy_events"),
                "fallback_filled_buy_copy_events": copy_truth.get("fallback_filled_buy_copy_events"),
                "rejected_buy_copy_events": copy_truth.get("rejected_buy_copy_events"),
                "missed_buy_copy_events": copy_truth.get("missed_buy_copy_events"),
            },
        )
    if route_status and route_status != "PASS":
        add_reason(
            "source_route_not_pass_for_current_poll",
            evidence={
                "source_route_status": route_status,
                "route_class_counts": source_route.get("route_class_counts") or {},
                "external_route_required": source_route.get("external_route_required"),
                "source_proxy_configured": source_route.get("source_proxy_configured"),
            },
        )
    if top_forward_current_poll and top_forward_current_poll.get("status") != "PASS":
        add_reason(
            "candidate_current_poll_truth_not_pass",
            evidence={
                "candidate_id": top_forward.get("candidate_id"),
                "current_poll_status": top_forward_current_poll.get("status"),
                "current_poll_blockers": top_forward_current_poll.get("blockers") or [],
                "zero_current_poll_root_cause": top_forward_current_poll.get("zero_current_poll_root_cause"),
                "raw_source_rows_seen": top_forward_current_poll.get("raw_source_rows_seen"),
                "normalized_source_rows_seen": top_forward_current_poll.get("normalized_source_rows_seen"),
            },
        )
    if (
        top_forward_tactic
        and top_forward_tactic.get("status") == "PASS"
        and _safe_int(top_forward_tactic.get("replay_intents")) > 0
    ):
        add_reason(
            "candidate_forward_strict_copy_rejects_have_measured_tactic_fill",
            severity="ACTIONABLE",
            evidence={
                "candidate_id": top_forward.get("candidate_id"),
                "source_wallet": top_forward.get("source_wallet"),
                "profile_id": top_forward_tactic.get("profile_id"),
                "replay_intents": top_forward_tactic.get("replay_intents"),
                "filled_orders": top_forward_tactic.get("filled_orders"),
                "rejected_orders": top_forward_tactic.get("rejected_orders"),
                "clob_filled_orders": top_forward_tactic.get("clob_filled_orders"),
                "fallback_filled_orders": top_forward_tactic.get("fallback_filled_orders"),
                "cost_delta_usd": top_forward_tactic.get("cost_delta_usd"),
                "live_admission_note": top_forward_tactic.get("live_admission_note"),
            },
        )
    if active_all_order and active_all_order_status != "PASS":
        add_reason(
            "all_order_exact_copy_not_live_ready",
            evidence={
                "active_all_order_status": active_all_order.get("status"),
                "active_all_order_live_truth_status": active_all_order.get("live_truth_status"),
                "live_truth_blockers": active_all_order.get("live_truth_blockers") or [],
                "buy_source_events": active_all_order.get("buy_source_events"),
                "clob_filled_buy_copy_events": active_all_order.get("clob_filled_buy_copy_events"),
                "rejected_buy_copy_events": active_all_order.get("rejected_buy_copy_events"),
            },
        )
    if "no_profit_candidate_pass" in blockers:
        add_reason(
            "no_single_candidate_passes_profit_and_live_copy_gates",
            evidence={
                "live_admission_status": decision.get("live_admission_status"),
                "live_admission_blockers": decision.get("live_admission_blockers") or [],
            },
        )

    status = "PASS" if live_ready else active_status_from_blockers(
        [str(row.get("code") or "") for row in reasons] + blockers,
        default=ANALYZE,
    )
    return {
        "schema_version": 1,
        "status": status,
        "live_ready": live_ready,
        "reason_codes": [str(row.get("code") or "") for row in reasons],
        "reasons": reasons,
        "operator_summary": (
            "A paper/replay profit result is not live-ready until the same candidate also has current-poll "
            "CLOB-backed CopyIntent truth with zero fallback/reject/miss, canonical resolution evidence, "
            "and passing train/validation/live-target metrics."
        ),
        "candidate_alignment": {
            "profit_candidate_id": best_candidate_id or None,
            "runtime_copy_candidate_id": runtime_candidate_id or None,
            "same_candidate": same_candidate,
        },
        "profitable_replay_candidate": {
            "candidate_id": best_candidate_id or None,
            "candidate_type": best_candidate.get("candidate_type"),
            "status": best_candidate.get("status"),
            "blockers": best_candidate.get("blockers") or [],
            "resolved_orders": best_summary.get("resolved_orders"),
            "roi_pct": best_summary.get("roi_pct"),
            "wr_pct": best_summary.get("wr_pct"),
            "validation_wr_pct": best_validation.get("wr_pct"),
            "pnl_usd": best_summary.get("pnl_usd"),
            "unique_windows": best_window_metrics.get("unique_windows"),
            "avg_orders_per_window": best_window_metrics.get("avg_orders_per_window"),
            "fill_evidence": best_fill,
            "resolution_evidence": best_resolution,
            "live_target_profile": best_live_target,
        },
        "runtime_copy_candidate": {
            "candidate_id": runtime_candidate_id or None,
            "source_wallet": (
                certificate_paper.get("source_wallet")
                or copy_truth.get("candidate_source_wallet")
                or decision.get("forward_candidate_source_wallet")
            ),
            "policy_id": certificate_paper.get("policy_id") or copy_truth.get("candidate_policy_id"),
            "paper_results": certificate_paper,
            "copy_truth": copy_truth,
            "candidate_forward_status": candidate_forward_status,
            "candidate_forward_top_rank": top_forward,
            "candidate_forward_top_rank_tactic_replay": top_forward_tactic,
        },
        "source_route_gate": {
            "status": route_status or None,
            **_source_route_diagnostic_fields(source_route),
        },
        "all_order_gate": {
            "status": active_all_order.get("status"),
            "live_truth_status": active_all_order.get("live_truth_status"),
            "live_truth_blockers": active_all_order.get("live_truth_blockers") or [],
        },
    }


def _candidate_forward_state_paper_result(
    path: str | Path,
    *,
    rank: int,
    runtime_proof_index: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {
            "rank": int(rank),
            "path": str(path),
            "status": "MISSING",
            "blockers": ["candidate_forward_state_missing"],
        }
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    profit_policy = summary.get("profit_policy") if isinstance(summary.get("profit_policy"), dict) else {}
    policy = profit_policy.get("policy") if isinstance(profit_policy.get("policy"), dict) else {}
    copy_efficiency = summary.get("copy_efficiency") if isinstance(summary.get("copy_efficiency"), dict) else {}
    copy_summary = copy_efficiency.get("summary") if isinstance(copy_efficiency.get("summary"), dict) else {}
    all_order = summary.get("all_order_exact_copy") if isinstance(summary.get("all_order_exact_copy"), dict) else {}
    tactic_replay = (
        all_order.get("aggressive_tactic_replay")
        if isinstance(all_order.get("aggressive_tactic_replay"), dict)
        else {}
    )
    poll_runtime = summary.get("poll_runtime") if isinstance(summary.get("poll_runtime"), dict) else {}
    current_poll = (
        summary.get("current_poll_diagnostics")
        if isinstance(summary.get("current_poll_diagnostics"), dict)
        else {}
    )
    blockers = list(copy_efficiency.get("blockers") or copy_summary.get("buy_execution_blockers") or [])
    result = {
        "rank": int(rank),
        "path": str(path),
        "status": copy_efficiency.get("status") or copy_summary.get("buy_execution_status"),
        "blockers": blockers,
        "source_wallet": profit_policy.get("candidate_source_wallet"),
        "candidate_id": profit_policy.get("candidate_id"),
        "policy_id": policy.get("policy_id") or profit_policy.get("candidate_policy_id"),
        "source_buy_events": copy_summary.get("source_buy_events"),
        "required_buy_copy_events": copy_summary.get("required_buy_copy_events"),
        "clob_filled_buy_copy_events": copy_summary.get("clob_filled_buy_copy_events"),
        "fallback_filled_buy_copy_events": copy_summary.get("fallback_filled_buy_copy_events"),
        "rejected_buy_copy_events": copy_summary.get("rejected_buy_copy_events"),
        "missed_buy_copy_events": copy_summary.get("missed_buy_copy_events"),
        "required_event_age_p95_s": copy_summary.get("required_event_age_p95_s"),
        "latest_buy_event_lag_s": copy_summary.get("latest_buy_event_lag_s"),
        "poll_runtime_status": poll_runtime.get("status"),
        "paper_only": payload.get("paper_only"),
        "live_orders_allowed": payload.get("live_orders_allowed"),
    }
    if all_order:
        result["all_order_exact_copy"] = {
            "status": all_order.get("status"),
            "active_status": all_order.get("active_status"),
            "live_truth_status": all_order.get("live_truth_status"),
            "live_truth_blockers": all_order.get("live_truth_blockers") or [],
            "source_events": all_order.get("source_events"),
            "buy_source_events": all_order.get("buy_source_events"),
            "buy_intents": all_order.get("buy_intents"),
            "clob_filled_buy_copy_events": all_order.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": all_order.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": all_order.get("rejected_buy_copy_events"),
            "copyability_rejected_buy_events": all_order.get("copyability_rejected_buy_events"),
            "execution_tactic_plan": all_order.get("execution_tactic_plan") or {},
            "paper_only": all_order.get("paper_only"),
            "live_orders_allowed": all_order.get("live_orders_allowed"),
        }
    if tactic_replay:
        result["all_order_tactic_replay"] = {
            "status": tactic_replay.get("status"),
            "role": tactic_replay.get("role"),
            "profile_id": tactic_replay.get("profile_id"),
            "recommended_tactic": tactic_replay.get("recommended_tactic"),
            "source_buy_intents": tactic_replay.get("source_buy_intents"),
            "replay_intents": tactic_replay.get("replay_intents"),
            "filled_orders": tactic_replay.get("filled_orders"),
            "rejected_orders": tactic_replay.get("rejected_orders"),
            "clob_filled_orders": tactic_replay.get("clob_filled_orders"),
            "fallback_filled_orders": tactic_replay.get("fallback_filled_orders"),
            "incremental_filled_orders_vs_strict": tactic_replay.get("incremental_filled_orders_vs_strict"),
            "incremental_source_event_ids_vs_strict": tactic_replay.get("incremental_source_event_ids_vs_strict") or [],
            "event_proofs": tactic_replay.get("event_proofs") or [],
            "strict_cost_usd": tactic_replay.get("strict_cost_usd"),
            "tactic_cost_usd": tactic_replay.get("tactic_cost_usd"),
            "cost_delta_usd": tactic_replay.get("cost_delta_usd"),
            "paper_state_path": tactic_replay.get("paper_state_path"),
            "paper_event_log_path": tactic_replay.get("paper_event_log_path"),
            "policy_validation": tactic_replay.get("policy_validation") or {},
            "pnl_attribution_status": tactic_replay.get("pnl_attribution_status"),
            "pnl_attribution_blockers": tactic_replay.get("pnl_attribution_blockers") or [],
            "paper_only": tactic_replay.get("paper_only"),
            "live_orders_allowed": tactic_replay.get("live_orders_allowed"),
            "live_admission_note": tactic_replay.get("live_admission_note"),
        }
    if current_poll:
        current_poll_summary = {
            "status": current_poll.get("status"),
            "blockers": list(current_poll.get("blockers") or []),
            "current_poll_ladder": current_poll.get("current_poll_ladder") or {},
            "zero_current_poll_root_cause": current_poll.get("zero_current_poll_root_cause"),
            "source_route_status_counts": current_poll.get("source_route_status_counts") or {},
            "source_route_class_counts": current_poll.get("source_route_class_counts") or {},
            "source_route_reset_wallets": current_poll.get("source_route_reset_wallets"),
            "source_route_error_wallets": current_poll.get("source_route_error_wallets"),
            "source_route_pass_rows": current_poll.get("source_route_pass_rows"),
            "source_route_error_rows": current_poll.get("source_route_error_rows"),
            "source_route_override_recovered_rows": current_poll.get("source_route_override_recovered_rows"),
            "source_route_degraded_recovered_rows": current_poll.get("source_route_degraded_recovered_rows"),
            "raw_source_rows_seen": current_poll.get("raw_source_rows_seen"),
            "normalized_source_rows_seen": current_poll.get("normalized_source_rows_seen"),
            "new_rows_after_dedupe": current_poll.get("new_rows_after_dedupe"),
            "new_buy_rows_after_dedupe": current_poll.get("new_buy_rows_after_dedupe"),
            "fresh_buy_rows_le_10s": current_poll.get("fresh_buy_rows_le_10s"),
            "fresh_buy_rows_le_30s": current_poll.get("fresh_buy_rows_le_30s"),
        }
        result["current_poll"] = current_poll_summary
        result["current_poll_status"] = current_poll_summary["status"]
        result["current_poll_blockers"] = current_poll_summary["blockers"]
        result["current_poll_ladder"] = current_poll_summary["current_poll_ladder"]
        result["zero_current_poll_root_cause"] = current_poll_summary["zero_current_poll_root_cause"]
        result["source_route_status_counts"] = current_poll_summary["source_route_status_counts"]
        result["source_route_class_counts"] = current_poll_summary["source_route_class_counts"]
    proof_rows = _runtime_proof_rows_for_scope(
        runtime_proof_index or {},
        candidate_id=str(result.get("candidate_id") or ""),
        policy_id=str(result.get("policy_id") or ""),
        source_wallet=str(result.get("source_wallet") or ""),
    )
    if proof_rows:
        result.update(_runtime_proof_summary(proof_rows))
        if int(result.get("required_buy_copy_events") or 0) <= 0:
            result["diagnostic_status"] = "PROOF_INDEX_PRESENT_TRACKER_SUMMARY_ZERO"
    return result


def _live_readiness_report(args: argparse.Namespace, post_audit: dict[str, Any]) -> dict[str, Any]:
    profit = load_json(args.profit_state, default={})
    if not isinstance(profit, dict):
        profit = {}
    source_route_state_path = getattr(args, "source_route_state", "")
    source_route = load_json(source_route_state_path, default={}) if source_route_state_path else {}
    if not isinstance(source_route, dict):
        source_route = {}
    strategy_direction = load_json(getattr(args, "strategy_direction_state", ""), default={})
    if not isinstance(strategy_direction, dict):
        strategy_direction = {}
    certificate = profit.get("live_readiness_certificate")
    if not isinstance(certificate, dict):
        certificate = {}
    decision = profit.get("decision") if isinstance(profit.get("decision"), dict) else {}
    candidate_forward_state = load_json(
        getattr(args, "candidate_forward_live_tracker_state", "data/research/wallet_copy_candidate_forward_live_tracking_state.json"),
        default={},
    )
    if not isinstance(candidate_forward_state, dict):
        candidate_forward_state = {}
    runtime_proof_index = load_json(
        getattr(args, "candidate_runtime_proof_index", "data/research/wallet_copy_candidate_runtime_proof_index.json"),
        default={},
    )
    if not isinstance(runtime_proof_index, dict):
        runtime_proof_index = {}
    adaptive_state = load_json(getattr(args, "adaptive_bot_state", "data/research/wallet_copy_adaptive_bot_state.json"), default={})
    if not isinstance(adaptive_state, dict):
        adaptive_state = {}
    post_checks = post_audit.get("checks") if isinstance(post_audit.get("checks"), dict) else {}
    active_tracking = (
        post_checks.get("active_hotlane_tracking_evidence")
        if isinstance(post_checks.get("active_hotlane_tracking_evidence"), dict)
        else {}
    )
    active_all_order = (
        post_checks.get("active_hotlane_all_order_exact_copy")
        if isinstance(post_checks.get("active_hotlane_all_order_exact_copy"), dict)
        else {}
    )
    adaptive_check = (
        post_checks.get("adaptive_wallet_derived_bot")
        if isinstance(post_checks.get("adaptive_wallet_derived_bot"), dict)
        else {}
    )
    candidate_forward_summary = (
        (candidate_forward_state.get("summary") or {}).get("copy_efficiency", {}).get("summary")
        if isinstance(candidate_forward_state.get("summary"), dict)
        else {}
    )
    certificate_copy_truth = certificate.get("copy_truth") if isinstance(certificate.get("copy_truth"), dict) else {}
    certificate_paper_results = (
        certificate.get("paper_results") if isinstance(certificate.get("paper_results"), dict) else {}
    )
    effective_truth_source = str(decision.get("live_tracker_truth_source") or certificate_copy_truth.get("source") or "")
    candidate_forward_summary_source = "candidate_forward_live_tracker_state"
    if effective_truth_source in {"candidate_forward", "forward_candidate"} and certificate_copy_truth:
        candidate_forward_summary = certificate_copy_truth
        candidate_forward_summary_source = "live_readiness_certificate.copy_truth"
    candidate_forward_status = decision.get("forward_candidate_truth_status")
    if candidate_forward_summary_source == "live_readiness_certificate.copy_truth":
        candidate_forward_status = (
            candidate_forward_summary.get("effective_live_tracker_truth_status")
            or decision.get("forward_candidate_truth_status")
        )
    candidate_forward_state_path = str(
        getattr(
            args,
            "candidate_forward_live_tracker_state",
            "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
        )
    )
    candidate_forward_rank_results = [
        _candidate_forward_state_paper_result(
            candidate_forward_state_path,
            rank=0,
            runtime_proof_index=runtime_proof_index,
        )
    ]
    for rank in range(1, _candidate_forward_probe_rank_count(args)):
        candidate_forward_rank_results.append(
            _candidate_forward_state_paper_result(
                _ranked_state_path(candidate_forward_state_path, rank),
                rank=rank,
                runtime_proof_index=runtime_proof_index,
            )
        )
    adaptive_summary = adaptive_state.get("summary") if isinstance(adaptive_state.get("summary"), dict) else {}
    adaptive_blockers = list(adaptive_check.get("blockers") or adaptive_state.get("blockers") or [])
    adaptive_status = adaptive_check.get("status") or adaptive_state.get("status")
    if adaptive_blockers and adaptive_status == "PASS":
        adaptive_status = "WATCH"
    all_order_blockers = []
    if not active_all_order:
        all_order_blockers.append("active_hotlane_all_order_exact_copy_missing")
    elif active_all_order.get("status") != "PASS":
        all_order_blockers.append("active_hotlane_all_order_exact_copy_not_pass")
        all_order_blockers.extend(str(row) for row in (active_all_order.get("live_truth_blockers") or []) if row)
    certificate_blockers = list(certificate.get("blockers") or [])
    decision_blockers = list(decision.get("live_admission_blockers") or [])
    source_route_status = str(source_route.get("status") or "")
    source_route_blockers = []
    if source_route_status and source_route_status != "PASS":
        source_route_blockers.append(f"source_route_{source_route_status.lower()}")
    primary_live_architecture = str(strategy_direction.get("primary_live_architecture") or "")
    layered_primary_live = primary_live_architecture == "single_wallet_copy_promotion_with_background_paper_backup_pool"
    paper_results = certificate.get("paper_results") if isinstance(certificate.get("paper_results"), dict) else {}
    single_wallet_certificate_ready = (
        bool(certificate.get("live_ready"))
        and bool(certificate.get("profitability_proven"))
        and paper_results.get("candidate_type") == "SINGLE_WALLET"
    )
    strategy_primary_ready = (
        layered_primary_live
        and bool(strategy_direction.get("live_ready"))
        and bool(strategy_direction.get("profitability_proven"))
        and not list(strategy_direction.get("live_readiness_blockers") or [])
    )
    upgrade_blockers = sorted(set(adaptive_blockers + all_order_blockers))
    blockers = sorted(
        set(
            certificate_blockers
            + decision_blockers
            + source_route_blockers
            + ([] if layered_primary_live and single_wallet_certificate_ready and strategy_primary_ready else upgrade_blockers)
        )
    )
    live_ready = bool(certificate.get("live_ready")) and not blockers
    readiness_status = (
        "LIVE_READY_BEHIND_OPERATOR_GATE" if live_ready else active_status_from_blockers(blockers, default=ANALYZE)
    )
    paper_to_live_gap = _paper_to_live_gap_report(
        profit=profit,
        certificate=certificate,
        decision=decision,
        source_route=source_route,
        active_all_order=active_all_order,
        candidate_forward_status=candidate_forward_status,
        candidate_forward_rank_results=candidate_forward_rank_results,
        blockers=blockers,
        live_ready=live_ready,
    )
    return {
        "schema_version": 1,
        "status": readiness_status,
        "profitability_proven": bool(certificate.get("profitability_proven")) and not blockers,
        "live_ready": live_ready,
        "paper_only": True,
        "live_orders_allowed": False,
        "blockers": blockers,
        "upgrade_blockers": upgrade_blockers,
        "primary_live_architecture": primary_live_architecture or None,
        "strategy_direction_live_ready": strategy_direction.get("live_ready") if layered_primary_live else None,
        "strategy_direction_profitability_proven": strategy_direction.get("profitability_proven") if layered_primary_live else None,
        "source_route": {
            "status": source_route_status or None,
            "state_path": source_route_state_path or None,
            "endpoint_statuses": _source_route_endpoint_statuses(source_route) if source_route else {},
            "next_action": source_route.get("next_action") if source_route else None,
            **_source_route_diagnostic_fields(source_route),
        },
        "paper_results": certificate.get("paper_results") or {},
        "copy_truth": certificate.get("copy_truth") or {},
        "active_hotlane_paper_results": {
            "status": active_tracking.get("status"),
            "required_buy_copy_events": active_tracking.get("required_buy_copy_events"),
            "clob_filled_buy_copy_events": active_tracking.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": active_tracking.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": active_tracking.get("rejected_buy_copy_events"),
            "missed_buy_copy_events": active_tracking.get("missed_buy_copy_events"),
            "lifecycle_missed_events": active_tracking.get("lifecycle_missed_events"),
        },
        "active_hotlane_all_order_paper_results": {
            "status": active_all_order.get("status"),
            "tracker_status": active_all_order.get("tracker_status"),
            "live_truth_status": active_all_order.get("live_truth_status"),
            "live_truth_blockers": active_all_order.get("live_truth_blockers") or [],
            "source_events": active_all_order.get("source_events"),
            "buy_source_events": active_all_order.get("buy_source_events"),
            "buy_intents": active_all_order.get("buy_intents"),
            "filled_buy_copy_events": active_all_order.get("filled_buy_copy_events"),
            "clob_filled_buy_copy_events": active_all_order.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": active_all_order.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": active_all_order.get("rejected_buy_copy_events"),
            "coverage_violations": active_all_order.get("coverage_violations"),
            "missed_lifecycle_events": active_all_order.get("missed_lifecycle_events"),
            "fill_source_counts": active_all_order.get("fill_source_counts") or {},
            "filled_fill_source_counts": active_all_order.get("filled_fill_source_counts") or {},
            "rejected_fill_source_counts": active_all_order.get("rejected_fill_source_counts") or {},
            "paper_tactic_profile_status_counts": active_all_order.get("paper_tactic_profile_status_counts") or {},
            "paper_tactic_profile_pass_events": active_all_order.get("paper_tactic_profile_pass_events") or {},
            "execution_corrections": active_all_order.get("execution_corrections") or {},
            "current_poll_execution_corrections": active_all_order.get("current_poll_execution_corrections") or {},
            "execution_tactic_plan": active_all_order.get("execution_tactic_plan") or {},
            "aggressive_tactic_replay": active_all_order.get("aggressive_tactic_replay") or {},
            "micro_batch_all_order_probe": active_all_order.get("micro_batch_all_order_probe") or {},
            "paper_state_path": active_all_order.get("paper_state_path"),
            "paper_event_log_path": active_all_order.get("paper_event_log_path"),
        },
        "candidate_forward_paper_results": {
            "status": candidate_forward_status,
            "proof_source": effective_truth_source or "candidate_forward_live_tracker_state",
            "summary_source": candidate_forward_summary_source,
            "source_wallet": decision.get("forward_candidate_source_wallet") or certificate_paper_results.get("source_wallet"),
            "candidate_id": decision.get("forward_candidate_id") or certificate_paper_results.get("candidate_id"),
            "required_buy_copy_events": candidate_forward_summary.get("required_buy_copy_events"),
            "clob_filled_buy_copy_events": candidate_forward_summary.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": candidate_forward_summary.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": candidate_forward_summary.get("rejected_buy_copy_events"),
            "missed_buy_copy_events": candidate_forward_summary.get("missed_buy_copy_events"),
            "required_event_age_p95_s": candidate_forward_summary.get("required_event_age_p95_s"),
        },
        "candidate_forward_rank_paper_results": candidate_forward_rank_results,
        "adaptive_paper_results": {
            "status": adaptive_status,
            "blockers": adaptive_blockers,
            "moves_seen": adaptive_summary.get("moves_seen"),
            "eligible_moves": adaptive_summary.get("eligible_moves"),
            "pass_signals": adaptive_summary.get("pass_signals"),
            "intents": adaptive_summary.get("intents"),
            "filled_orders": adaptive_summary.get("filled_orders"),
            "rejected_orders": adaptive_summary.get("rejected_orders"),
            "tracker_time_pass_signals": adaptive_summary.get("tracker_time_pass_signals"),
            "tracker_time_replay_intents": (adaptive_state.get("tracker_time_replay") or {}).get("summary", {}).get("intents")
            if isinstance(adaptive_state.get("tracker_time_replay"), dict)
            else None,
        },
        "live_connection_plan": {
            **(certificate.get("live_connection_plan") or {}),
            "mode": active_plan_mode(live_ready, blockers, target="PROFIT_AND_RUNTIME_PROOF"),
            "next_gate": (
                "operator_may_enable_live_execution_only_after_this_report_status_is_LIVE_READY_BEHIND_OPERATOR_GATE"
            ),
            "required_remaining_evidence": blockers,
        },
        "search_truth": certificate.get("search_truth") or {},
        "resolution_truth": certificate.get("resolution_truth") or {},
        "paper_to_live_gap": paper_to_live_gap,
        "proposal": [
            "keep the same CopyIntent path for paper and live; only the execution permission changes",
            "promote no wallet until walk-forward profit admission has PASS plus CLOB-backed current runtime copy truth",
            "treat all-order exact-copy proof as a separate live-money gate: every observed BTC 5m BUY must become CLOB-backed paper CopyIntent with no fallback/reject/miss",
            "when all-order strict copy rejects, test aggressive best-ask and micro-batch tactics in paper only and promote only tactics with measured CLOB fill plus PnL attribution",
            "use the current forward candidate and adaptive tracker-time replay as search pressure, not live truth",
        ],
    }


def _candidate_forward_tactic_backlog_actions(live_readiness: dict[str, Any]) -> list[dict[str, Any]]:
    """Create the next concrete repair when strict copy rejects but a paper tactic fills."""

    if not isinstance(live_readiness, dict):
        return []
    gap = live_readiness.get("paper_to_live_gap") if isinstance(live_readiness.get("paper_to_live_gap"), dict) else {}
    runtime = gap.get("runtime_copy_candidate") if isinstance(gap.get("runtime_copy_candidate"), dict) else {}
    top_forward = (
        runtime.get("candidate_forward_top_rank")
        if isinstance(runtime.get("candidate_forward_top_rank"), dict)
        else {}
    )
    tactic = (
        runtime.get("candidate_forward_top_rank_tactic_replay")
        if isinstance(runtime.get("candidate_forward_top_rank_tactic_replay"), dict)
        else {}
    )
    all_order = (
        top_forward.get("all_order_exact_copy")
        if isinstance(top_forward.get("all_order_exact_copy"), dict)
        else {}
    )
    execution_tactic_plan = (
        all_order.get("execution_tactic_plan")
        if isinstance(all_order.get("execution_tactic_plan"), dict)
        else {}
    )
    strict_rejected_orders = _safe_int(
        all_order.get("rejected_buy_copy_events")
        or execution_tactic_plan.get("strict_rejected_buy_orders")
    )
    copyability_rejected_orders = _safe_int(all_order.get("copyability_rejected_buy_events"))
    all_order_live_truth_status = str(all_order.get("live_truth_status") or all_order.get("status") or "")
    tactic_status = str(tactic.get("status") or "")
    replay_intents = _safe_int(tactic.get("replay_intents"))
    if (
        (strict_rejected_orders > 0 or copyability_rejected_orders > 0 or all_order_live_truth_status == CORRECTION)
        and (tactic_status != "PASS" or replay_intents <= 0)
    ):
        policy_validation = (
            tactic.get("policy_validation")
            if isinstance(tactic.get("policy_validation"), dict)
            else {}
        )
        no_actionable = (
            execution_tactic_plan.get("no_actionable_tactic_diagnostics")
            if isinstance(execution_tactic_plan.get("no_actionable_tactic_diagnostics"), dict)
            else {}
        )
        micro_summary = (
            execution_tactic_plan.get("micro_batch_summary")
            if isinstance(execution_tactic_plan.get("micro_batch_summary"), dict)
            else {}
        )
        micro_min_order_actionability = (
            execution_tactic_plan.get("micro_min_order_actionability")
            if isinstance(execution_tactic_plan.get("micro_min_order_actionability"), dict)
            else {}
        )
        flow_control_shadow_measurement_plan = (
            execution_tactic_plan.get("flow_control_shadow_measurement_plan")
            if isinstance(execution_tactic_plan.get("flow_control_shadow_measurement_plan"), dict)
            else {}
        )
        strict_copy_all_current_buys_filled = bool(
            execution_tactic_plan.get("strict_copy_all_current_buys_filled")
        )
        strict_unfilled_orders = _safe_int(execution_tactic_plan.get("strict_unfilled_buy_orders"))
        raw_tactic_repair_required = execution_tactic_plan.get("tactic_repair_required")
        tactic_repair_required = (
            bool(raw_tactic_repair_required)
            if isinstance(raw_tactic_repair_required, bool)
            else bool(strict_rejected_orders > 0 or strict_unfilled_orders > 0)
        )
        tactic_blockers = [str(row) for row in (execution_tactic_plan.get("blockers") or []) if row]
        repair_lane = str(no_actionable.get("recommended_repair_lane") or "")
        dominant_category = str(no_actionable.get("dominant_reject_category") or "")
        lifecycle_blockers: list[str] = []
        for source_blockers in (
            policy_validation.get("current_lifecycle_blockers"),
            execution_tactic_plan.get("blockers"),
            all_order.get("live_truth_blockers"),
        ):
            for row in source_blockers or []:
                blocker = str(row)
                if blocker and blocker not in lifecycle_blockers:
                    lifecycle_blockers.append(blocker)
        if (
            not tactic_repair_required
            and strict_copy_all_current_buys_filled
            and (copyability_rejected_orders > 0 or all_order_live_truth_status == CORRECTION)
        ):
            return [
                {
                    "area": "candidate-forward current-poll copyability truth",
                    "file": "src/wallet_copy/live_tracker.py",
                    "function": "LiveWalletTracker.poll_once",
                    "severity": "P1",
                    "action": (
                        "repair candidate-forward live truth at the source/copyability layer: strict CopyIntent "
                        "execution already fills every current BUY, so the next repair must explain and reduce "
                        "current-poll copyability rejects or stale source-trade/book timing, not micro/depth tactics"
                    ),
                    "verify": (
                        "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240 && "
                        "jq '.progress_action|{area,blockers,current_poll_diagnostics}' "
                        "data/research/wallet_copy_autonomous_repair_state.json"
                    ),
                    "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
                    "blockers": lifecycle_blockers
                    or [
                        "all_order_copyability_truth_not_live_ready",
                        "strict_copy_fills_current_buys_tactic_not_blocker",
                    ],
                    "candidate_id": top_forward.get("candidate_id"),
                    "source_wallet": top_forward.get("source_wallet"),
                    "policy_id": top_forward.get("policy_id"),
                    "all_order_status": all_order.get("status"),
                    "all_order_live_truth_status": all_order.get("live_truth_status"),
                    "buy_source_events": all_order.get("buy_source_events"),
                    "clob_filled_buy_copy_events": all_order.get("clob_filled_buy_copy_events"),
                    "rejected_buy_copy_events": all_order.get("rejected_buy_copy_events"),
                    "copyability_rejected_buy_events": all_order.get("copyability_rejected_buy_events"),
                    "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
                    "recommended_next_measurement": no_actionable.get("recommended_next_measurement"),
                    "strict_copy_all_current_buys_filled": strict_copy_all_current_buys_filled,
                    "tactic_repair_required": tactic_repair_required,
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ]
        shadow_incremental_events = _safe_int(
            flow_control_shadow_measurement_plan.get("incremental_pass_events_vs_strict")
        )
        if flow_control_shadow_measurement_plan and shadow_incremental_events > 0:
            shadow_blockers = [
                str(row) for row in (flow_control_shadow_measurement_plan.get("blockers") or []) if row
            ]
            shadow_verify = str(
                flow_control_shadow_measurement_plan.get("verification_command")
                or "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900"
            )
            return [
                {
                    "area": "candidate-forward flow-control shadow PnL measurement",
                    "file": "src/wallet_copy/copy_tactics.py",
                    "function": "build_copy_execution_tactic_plan",
                    "severity": "P1",
                    "progress_type": "sharper_measurement",
                    "action": (
                        "persist the flow-control-blocked profile as paper-only shadow PnL evidence; keep live "
                        "blocked because current-event replay is stale, unfillable, or otherwise gated, but stop "
                        "looping on generic diagnosis when measured profile fillability improved versus strict copy"
                    ),
                    "verify": (
                        f"{shadow_verify} && "
                        "jq '.live_readiness_report.paper_to_live_gap.runtime_copy_candidate."
                        "candidate_forward_top_rank.all_order_exact_copy.execution_tactic_plan|"
                        "{recommended_tactic,blockers,flow_control_shadow_measurement_plan,"
                        "no_actionable_tactic_diagnostics}' "
                        "data/research/wallet_copy_autonomous_repair_state.json"
                    ),
                    "next_command": shadow_verify,
                    "blockers": lifecycle_blockers
                    or shadow_blockers
                    or [
                        "flow_control_blocks_aggressive_profile_replay",
                        "paper_shadow_profile_needs_resolved_pnl_attribution",
                    ],
                    "candidate_id": top_forward.get("candidate_id"),
                    "source_wallet": top_forward.get("source_wallet"),
                    "policy_id": top_forward.get("policy_id"),
                    "all_order_status": all_order.get("status"),
                    "all_order_live_truth_status": all_order.get("live_truth_status"),
                    "buy_source_events": all_order.get("buy_source_events"),
                    "clob_filled_buy_copy_events": all_order.get("clob_filled_buy_copy_events"),
                    "rejected_buy_copy_events": all_order.get("rejected_buy_copy_events"),
                    "copyability_rejected_buy_events": all_order.get("copyability_rejected_buy_events"),
                    "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
                    "blocked_recommended_tactic": flow_control_shadow_measurement_plan.get(
                        "blocked_recommended_tactic"
                    ),
                    "shadow_profile_id": flow_control_shadow_measurement_plan.get("profile_id"),
                    "shadow_profile_pass_events": flow_control_shadow_measurement_plan.get(
                        "best_profile_pass_events"
                    ),
                    "shadow_incremental_pass_events_vs_strict": shadow_incremental_events,
                    "flow_control_next_action": flow_control_shadow_measurement_plan.get(
                        "flow_control_next_action"
                    ),
                    "skip_current_event_replay": flow_control_shadow_measurement_plan.get(
                        "skip_current_event_replay"
                    ),
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ]
        has_micro_or_depth_evidence = bool(
            tactic_repair_required
            and (
                micro_summary
            or repair_lane in {
                "paper_test_micro_aggregation_without_live_admission",
                "diagnose_exact_no_overcopy_micro_copy_before_batching",
                "test_depth_slicing_or_smaller_wallet_fraction_in_paper",
                "verify_source_trade_book_timing_before_copy",
            }
            or dominant_category in {"micro_or_min_order", "depth_or_size", "no_liquidity"}
            or any("micro_batch" in row for row in tactic_blockers)
            )
        )
        if has_micro_or_depth_evidence:
            return [
                {
                    "area": "candidate-forward micro/depth copy tactic planning",
                    "file": "src/wallet_copy/copy_tactics.py",
                    "function": "build_copy_execution_tactic_plan",
                    "severity": "P1",
                    "action": (
                        "repair candidate-forward strict-copy rejects by measuring the dominant paper-only "
                        "micro/depth/no-liquidity lane; keep live blocked until the tactic has resolved PnL "
                        "attribution and ordinary CopyIntent all-order truth has zero rejects/misses/fallbacks"
                    ),
                    "verify": (
                        "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240 && "
                        "jq '.live_readiness_report.paper_to_live_gap.runtime_copy_candidate."
                        "candidate_forward_top_rank.all_order_exact_copy.execution_tactic_plan|"
                        "{recommended_tactic,blockers,no_actionable_tactic_diagnostics,micro_batch_summary,"
                        "micro_batch_min_order_research,micro_min_order_actionability}' "
                        "data/research/wallet_copy_autonomous_repair_state.json"
                    ),
                    "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
                    "blockers": lifecycle_blockers
                    or [
                        "candidate_forward_micro_depth_tactic_plan_not_live_ready",
                        "strict_all_order_rejected_orders_present",
                    ],
                    "candidate_id": top_forward.get("candidate_id"),
                    "source_wallet": top_forward.get("source_wallet"),
                    "policy_id": top_forward.get("policy_id"),
                    "all_order_status": all_order.get("status"),
                    "all_order_live_truth_status": all_order.get("live_truth_status"),
                    "buy_source_events": all_order.get("buy_source_events"),
                    "clob_filled_buy_copy_events": all_order.get("clob_filled_buy_copy_events"),
                    "rejected_buy_copy_events": all_order.get("rejected_buy_copy_events"),
                    "copyability_rejected_buy_events": all_order.get("copyability_rejected_buy_events"),
                    "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
                    "recommended_repair_lane": no_actionable.get("recommended_repair_lane"),
                    "dominant_reject_category": no_actionable.get("dominant_reject_category"),
                    "best_profile_id": no_actionable.get("best_profile_id")
                    or execution_tactic_plan.get("best_profile_id"),
                    "best_profile_incremental_pass_events_vs_strict": no_actionable.get(
                        "best_profile_incremental_pass_events_vs_strict"
                    ),
                    "micro_batch_filled_child_events": micro_summary.get("filled_child_events"),
                    "micro_batch_rejected_child_events": micro_summary.get("rejected_child_events"),
                    "micro_batch_overcopy_usd": micro_summary.get("overcopy_usd"),
                    "micro_batch_overcopy_pct_of_exact_total": micro_summary.get(
                        "overcopy_pct_of_exact_total"
                    ),
                    "micro_batch_overcopy_per_filled_child_event_usd": micro_summary.get(
                        "overcopy_per_filled_child_event_usd"
                    ),
                    "micro_batch_improves_strict_fill_count": micro_summary.get("improves_strict_fill_count"),
                    "micro_batch_actionable_for_paper_replay": micro_summary.get("actionable_for_paper_replay"),
                    "micro_batch_research_only_not_live_admission": micro_summary.get(
                        "research_only_not_live_admission"
                    ),
                    "micro_batch_actionability_blockers": list(micro_summary.get("actionability_blockers") or []),
                    "micro_min_order_actionability": micro_min_order_actionability,
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ]
        return [
            {
                "area": "candidate-forward aggressive copy policy validation",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker._all_order_aggressive_tactic_replay",
                "severity": "P1",
                "action": (
                    "repair candidate-forward all-order strict-copy rejects by diagnosing unfillable, stale, "
                    "or sub-minimum current-poll BUYs; keep live blocked until ordinary CopyIntent all-order "
                    "truth has zero rejects/misses/fallbacks and any tactic lane has current-lifecycle action"
                ),
                "verify": (
                    "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240 && "
                    "jq '.live_readiness_report.paper_to_live_gap.runtime_copy_candidate|"
                    "{top_rank:.candidate_forward_top_rank.all_order_exact_copy,"
                    "tactic:.candidate_forward_top_rank_tactic_replay}' "
                    "data/research/wallet_copy_autonomous_repair_state.json"
                ),
                "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
                "blockers": lifecycle_blockers
                or [
                    (
                        "strict_all_order_rejected_orders_present"
                        if strict_rejected_orders > 0
                        else "all_order_copyability_truth_not_live_ready"
                    ),
                    "candidate_forward_tactic_replay_no_actionable_profile",
                ],
                "candidate_id": top_forward.get("candidate_id"),
                "source_wallet": top_forward.get("source_wallet"),
                "policy_id": top_forward.get("policy_id"),
                "all_order_status": all_order.get("status"),
                "all_order_live_truth_status": all_order.get("live_truth_status"),
                "buy_source_events": all_order.get("buy_source_events"),
                "clob_filled_buy_copy_events": all_order.get("clob_filled_buy_copy_events"),
                "rejected_buy_copy_events": all_order.get("rejected_buy_copy_events"),
                "copyability_rejected_buy_events": all_order.get("copyability_rejected_buy_events"),
                "tactic_status": tactic_status or None,
                "recommended_tactic": (
                    tactic.get("recommended_tactic")
                    or execution_tactic_plan.get("recommended_tactic")
                ),
                "current_lifecycle_status": policy_validation.get("current_lifecycle_status"),
                "policy_validation_status": policy_validation.get("status"),
            }
        ]
    if tactic.get("status") != "PASS" or _safe_int(tactic.get("replay_intents")) <= 0:
        return []
    policy_validation = (
        tactic.get("policy_validation")
        if isinstance(tactic.get("policy_validation"), dict)
        else {}
    )
    if policy_validation:
        pnl_blockers = [
            str(row)
            for row in (
                policy_validation.get("pnl_attribution_blockers")
                or policy_validation.get("blockers")
                or tactic.get("pnl_attribution_blockers")
                or []
            )
            if row
        ]
        policy_status = str(policy_validation.get("status") or tactic.get("pnl_attribution_status") or ANALYZE)
        if policy_status == "PASS":
            return []
        if policy_status != "PASS":
            return [
                {
                    "area": "candidate-forward aggressive copy policy validation",
                    "file": "src/wallet_copy/tactic_performance.py",
                    "function": "score_tactic_replay_pnl",
                    "severity": "P1",
                    "action": (
                        "continue the paper-only aggressive tactic validation lane with canonical resolved PnL "
                        "evidence; refresh BTC 5m resolutions or accumulate resolved tactic replay orders, but "
                        "keep live blocked until policy_validation.status is PASS and ordinary CopyIntent truth "
                        "also passes"
                    ),
                    "verify": (
                        "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240 && "
                        "jq '.live_readiness_report.paper_to_live_gap.runtime_copy_candidate."
                        "candidate_forward_top_rank_tactic_replay.policy_validation' "
                        "data/research/wallet_copy_autonomous_repair_state.json"
                    ),
                    "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
                    "blockers": pnl_blockers or ["candidate_forward_tactic_policy_validation_not_pass"],
                    "candidate_id": top_forward.get("candidate_id"),
                    "source_wallet": top_forward.get("source_wallet"),
                    "policy_id": top_forward.get("policy_id"),
                    "profile_id": tactic.get("profile_id"),
                    "policy_validation_status": policy_validation.get("status"),
                    "canonical_resolved_orders": (
                        (policy_validation.get("pnl_attribution") or {}).get("canonical_resolved_orders")
                        if isinstance(policy_validation.get("pnl_attribution"), dict)
                        else None
                    ),
                    "tactic_pnl_usd": (
                        (policy_validation.get("pnl_attribution") or {}).get("tactic_pnl_usd")
                        if isinstance(policy_validation.get("pnl_attribution"), dict)
                        else None
                    ),
                    "tactic_roi_pct": (
                        (policy_validation.get("pnl_attribution") or {}).get("tactic_roi_pct")
                        if isinstance(policy_validation.get("pnl_attribution"), dict)
                        else None
                    ),
                    "resolution_path": policy_validation.get("resolution_path"),
                    "resolution_rows_indexed": policy_validation.get("resolution_rows_indexed"),
                    "paper_state_path": tactic.get("paper_state_path"),
                    "paper_event_log_path": tactic.get("paper_event_log_path"),
                    "live_admission_note": tactic.get("live_admission_note"),
                }
            ]
    return [
        {
            "area": "candidate-forward aggressive copy policy validation",
            "file": "src/wallet_copy/live_tracker.py",
            "function": "LiveWalletTracker._all_order_aggressive_tactic_replay",
            "severity": "P1",
            "action": (
                "promote the measured candidate-forward aggressive best-ask replay into a paper-only policy "
                "validation lane with resolved PnL attribution and ordinary CopyIntent proof; keep live blocked "
                "until the candidate has positive resolved PnL, zero fallback/reject/miss BUYs, and "
                "candidate-specific current-poll CLOB truth"
            ),
            "verify": (
                "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240 && "
                "jq '.live_readiness_report.paper_to_live_gap.runtime_copy_candidate."
                "candidate_forward_top_rank_tactic_replay' "
                "data/research/wallet_copy_autonomous_repair_state.json"
            ),
            "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
            "blockers": [
                "strict_all_order_rejected_orders_present",
                "candidate_forward_tactic_replay_requires_pnl_validation",
            ],
            "candidate_id": top_forward.get("candidate_id"),
            "source_wallet": top_forward.get("source_wallet"),
            "policy_id": top_forward.get("policy_id"),
            "profile_id": tactic.get("profile_id"),
            "replay_intents": tactic.get("replay_intents"),
            "filled_orders": tactic.get("filled_orders"),
            "rejected_orders": tactic.get("rejected_orders"),
            "clob_filled_orders": tactic.get("clob_filled_orders"),
            "fallback_filled_orders": tactic.get("fallback_filled_orders"),
            "cost_delta_usd": tactic.get("cost_delta_usd"),
            "paper_state_path": tactic.get("paper_state_path"),
            "paper_event_log_path": tactic.get("paper_event_log_path"),
            "live_admission_note": tactic.get("live_admission_note"),
        }
    ]


def _green_semantics_report(
    live_readiness: dict[str, Any],
    source_route: dict[str, Any],
    *,
    strategy_direction: dict[str, Any] | None = None,
    workflow_status: str | None = None,
) -> dict[str, Any]:
    """Compute the only statuses that may be called globally green.

    Local PASS rows are useful diagnostics, but the operator-facing green state
    must mean the full wallet-copy and live-readiness contracts are both true.
    """

    if not isinstance(live_readiness, dict):
        live_readiness = {}
    if not isinstance(source_route, dict):
        source_route = {}
    if not isinstance(strategy_direction, dict):
        strategy_direction = {}
    primary_live_architecture = str(strategy_direction.get("primary_live_architecture") or "")
    layered_primary_live = primary_live_architecture == "single_wallet_copy_promotion_with_background_paper_backup_pool"

    def selected_all_order_gate() -> tuple[str, dict[str, Any]]:
        active_all_order = (
            live_readiness.get("active_hotlane_all_order_paper_results")
            if isinstance(live_readiness.get("active_hotlane_all_order_paper_results"), dict)
            else {}
        )
        gap = live_readiness.get("paper_to_live_gap") if isinstance(live_readiness.get("paper_to_live_gap"), dict) else {}
        runtime = gap.get("runtime_copy_candidate") if isinstance(gap.get("runtime_copy_candidate"), dict) else {}
        top_forward = (
            runtime.get("candidate_forward_top_rank")
            if isinstance(runtime.get("candidate_forward_top_rank"), dict)
            else {}
        )
        candidate_all_order = (
            top_forward.get("all_order_exact_copy")
            if isinstance(top_forward.get("all_order_exact_copy"), dict)
            else {}
        )
        candidates = (
            ("active_hotlane_all_order_paper_results", active_all_order),
            ("candidate_forward_top_rank.all_order_exact_copy", candidate_all_order),
        )
        for source_name, row in candidates:
            if row and _safe_int(row.get("buy_source_events")) > 0:
                return source_name, row
        for source_name, row in candidates:
            if row and (row.get("status") or row.get("live_truth_status")):
                return source_name, row
        return "active_hotlane_all_order_paper_results", active_all_order

    copy_blockers: list[str] = []
    bot_blockers: list[str] = []
    upgrade_copy_blockers: list[str] = []

    def add_copy(blocker: str) -> None:
        if blocker not in copy_blockers:
            copy_blockers.append(blocker)

    def add_bot(blocker: str) -> None:
        if blocker not in bot_blockers:
            bot_blockers.append(blocker)

    strategy_primary_ready = (
        layered_primary_live
        and bool(strategy_direction.get("live_ready"))
        and bool(strategy_direction.get("profitability_proven"))
        and not list(strategy_direction.get("live_readiness_blockers") or [])
    )
    live_blockers = [str(row) for row in (live_readiness.get("blockers") or []) if row]
    if strategy_primary_ready:
        live_blockers = []
    source_route_status = str(
        source_route.get("status")
        or ((live_readiness.get("source_route") or {}).get("status") if isinstance(live_readiness.get("source_route"), dict) else "")
        or ""
    )
    if source_route_status != "PASS":
        add_copy("source_route_not_pass")

    copy_truth = live_readiness.get("copy_truth") if isinstance(live_readiness.get("copy_truth"), dict) else {}
    copy_required = _safe_int(copy_truth.get("required_buy_copy_events"))
    copy_clob = _safe_int(copy_truth.get("clob_filled_buy_copy_events"))
    copy_fallback = _safe_int(copy_truth.get("fallback_filled_buy_copy_events"))
    copy_rejected = _safe_int(copy_truth.get("rejected_buy_copy_events"))
    copy_missed = _safe_int(copy_truth.get("missed_buy_copy_events"))
    if copy_required <= 0:
        add_copy("candidate_copy_truth_required_buy_events_missing")
    if copy_clob < copy_required:
        add_copy("candidate_copy_truth_clob_fill_coverage_incomplete")
    if copy_fallback > 0:
        add_copy("candidate_copy_truth_has_fallback_buy_fills")
    if copy_rejected > 0:
        add_copy("candidate_copy_truth_has_rejected_buy_events")
    if copy_missed > 0:
        add_copy("candidate_copy_truth_has_missed_buy_events")

    all_order_source, all_order = selected_all_order_gate()
    all_order_status = str(all_order.get("status") or "")
    all_order_live_truth_status = str(all_order.get("live_truth_status") or "")
    buy_source_events = _safe_int(all_order.get("buy_source_events"))
    buy_intents = _safe_int(all_order.get("buy_intents"))
    filled_buy_copy_events = _safe_int(all_order.get("filled_buy_copy_events"))
    clob_filled_buy_copy_events = _safe_int(all_order.get("clob_filled_buy_copy_events"))
    fallback_filled_buy_copy_events = _safe_int(all_order.get("fallback_filled_buy_copy_events"))
    rejected_buy_copy_events = _safe_int(all_order.get("rejected_buy_copy_events"))
    missed_buy_copy_events = _safe_int(all_order.get("missed_buy_copy_events"))

    def add_all_order_blocker(blocker: str) -> None:
        if layered_primary_live:
            if blocker not in upgrade_copy_blockers:
                upgrade_copy_blockers.append(blocker)
        else:
            add_copy(blocker)

    if all_order_status != "PASS":
        add_all_order_blocker("all_order_exact_copy_not_pass")
    if all_order_live_truth_status != "PASS":
        add_all_order_blocker("all_order_live_truth_not_pass")
    if buy_source_events <= 0:
        add_all_order_blocker("all_order_buy_source_events_missing")
    if buy_intents and buy_intents < buy_source_events:
        add_all_order_blocker("all_order_copy_intent_coverage_incomplete")
    if clob_filled_buy_copy_events < buy_source_events:
        add_all_order_blocker("all_order_clob_fill_coverage_incomplete")
    if filled_buy_copy_events and clob_filled_buy_copy_events < filled_buy_copy_events:
        add_all_order_blocker("all_order_non_clob_fill_present")
    if fallback_filled_buy_copy_events > 0:
        add_all_order_blocker("all_order_has_fallback_buy_fills")
    if rejected_buy_copy_events > 0:
        add_all_order_blocker("all_order_has_rejected_buy_events")
    if missed_buy_copy_events > 0:
        add_all_order_blocker("all_order_has_missed_buy_events")
    if all_order.get("coverage_violations"):
        add_all_order_blocker("all_order_has_coverage_violations")
    if all_order.get("missed_lifecycle_events"):
        add_all_order_blocker("all_order_has_missed_lifecycle_events")

    if layered_primary_live:
        for blocker in strategy_direction.get("copy_trading_blockers") or []:
            add_copy(str(blocker))
        if strategy_direction.get("copy_trading_green") is False and not strategy_direction.get("copy_trading_blockers"):
            add_copy("strategy_direction_copy_trading_not_green")
        for blocker in strategy_direction.get("multi_wallet_upgrade_blockers") or []:
            blocker_text = str(blocker)
            if blocker_text not in upgrade_copy_blockers:
                upgrade_copy_blockers.append(blocker_text)

    copy_trading_green = not copy_blockers
    for blocker in copy_blockers:
        add_bot(blocker)
    for blocker in live_blockers:
        add_bot(blocker)
    if layered_primary_live:
        for blocker in strategy_direction.get("live_readiness_blockers") or []:
            add_bot(str(blocker))
    profitability_proven = bool(live_readiness.get("profitability_proven")) or strategy_primary_ready
    live_ready = bool(live_readiness.get("live_ready")) or strategy_primary_ready
    if not profitability_proven:
        add_bot("profitability_not_proven")
    if not live_ready:
        add_bot("bot_not_live_ready")

    bot_green = copy_trading_green and not bot_blockers
    global_green = copy_trading_green and bot_green
    return {
        "schema_version": 1,
        "workflow_status_before_green_gate": workflow_status,
        "subsystem_pass_is_not_global_green": True,
        "copy_trading_green": copy_trading_green,
        "bot_green": bot_green,
        "global_green": global_green,
        "copy_blockers": copy_blockers,
        "bot_blockers": bot_blockers,
        "upgrade_copy_blockers": upgrade_copy_blockers,
        "source_route_status": source_route_status or None,
        "primary_live_architecture": primary_live_architecture or None,
        "strategy_direction_copy_trading_green": strategy_direction.get("copy_trading_green")
        if layered_primary_live
        else None,
        "strategy_direction_live_ready": strategy_direction.get("live_ready") if layered_primary_live else None,
        "strategy_direction_profitability_proven": strategy_direction.get("profitability_proven") if layered_primary_live else None,
        "effective_live_ready": live_ready,
        "effective_profitability_proven": profitability_proven,
        "all_order_gate_source": all_order_source,
        "copy_truth_counts": {
            "required_buy_copy_events": copy_required,
            "clob_filled_buy_copy_events": copy_clob,
            "fallback_filled_buy_copy_events": copy_fallback,
            "rejected_buy_copy_events": copy_rejected,
            "missed_buy_copy_events": copy_missed,
        },
        "all_order_counts": {
            "buy_source_events": buy_source_events,
            "buy_intents": buy_intents,
            "filled_buy_copy_events": filled_buy_copy_events,
            "clob_filled_buy_copy_events": clob_filled_buy_copy_events,
            "fallback_filled_buy_copy_events": fallback_filled_buy_copy_events,
            "rejected_buy_copy_events": rejected_buy_copy_events,
            "missed_buy_copy_events": missed_buy_copy_events,
        },
        "rule": (
            "copy-trading green follows the active layered primary live gate; multi-wallet all-order diagnostics "
            "remain upgrade blockers until separately proven; bot green requires profitable live-ready proof"
        ),
    }


def _apply_global_green_gate(status: str, green_semantics: dict[str, Any]) -> str:
    if status in {"GREEN", "PASS"} and not bool((green_semantics or {}).get("global_green")):
        return active_status_from_blockers((green_semantics or {}).get("bot_blockers") or [], default=ANALYZE)
    if bool((green_semantics or {}).get("global_green")) and status not in {"GREEN", PASS, "BUG_SUSPECT"}:
        return PASS
    return status


def _limit_pressure_report(
    args: argparse.Namespace,
    *,
    command_results: list[dict[str, Any]],
    live_readiness: dict[str, Any],
) -> dict[str, Any]:
    """Surface operational limits as explicit work, not as acceptable drift.

    A bounded probe can be useful, but it must remain visible. GREEN is only
    meaningful when the workflow either has no active artificial limit or has
    an explicit replacement path with measured coverage.
    """

    events: list[dict[str, Any]] = []
    blockers: list[str] = []

    def add(kind: str, blocker: str, **payload: Any) -> None:
        blockers.append(blocker)
        events.append({"kind": kind, "blocker": blocker, **payload})

    for row in command_results:
        name = str(row.get("name") or "")
        wall_runtime_exhausted = bool(row.get("wall_runtime_budget_exhausted"))
        if not wall_runtime_exhausted and (
            int(row.get("returncode") or 0) == 124 or "timeout after" in str(row.get("stderr_tail") or "")
        ):
            add(
                "command_timeout",
                f"{name}_timed_out",
                command=name,
                timeout_s=row.get("timeout_s"),
                next_step=(
                    "increase the parent timeout or split this command into resumable slices with persisted coverage; "
                    "do not mark the missing evidence green"
                ),
            )
        runtime_budget = row.get("runtime_budget") if isinstance(row.get("runtime_budget"), dict) else {}
        budget_status = str(runtime_budget.get("status") or "")
        if budget_status and budget_status != "UNCHANGED":
            add(
                "bounded_runtime_budget",
                f"{name}_{budget_status.lower()}",
                command=name,
                runtime_budget=runtime_budget,
                next_step=(
                    "run a longer proof lane or persist a multi-slice coverage plan until the requested runtime is "
                    "covered with equivalent evidence"
                ),
            )
        scope_mode = str(runtime_budget.get("scope_mode") or "")
        if scope_mode == "ROTATING_REGISTRY_SLICE_NOT_FULL_CYCLE":
            add(
                "rotating_registry_scope",
                "registry_sweep_rotating_slice_not_full_cycle",
                command=name,
                runtime_budget=runtime_budget,
                next_step=(
                    "continue rotation until every enabled wallet has current-poll CLOB/onchain coverage, or promote "
                    "a measured active-hotlane subset with explicit excluded-wallet evidence"
                ),
            )
        if scope_mode == "CANONICAL_PAPER_LIVE_TRACKER" and int(runtime_budget.get("iterations") or 0) < 3:
            add(
                "thin_tracker_sampling",
                "canonical_tracker_iterations_too_thin_for_readiness",
                command=name,
                runtime_budget=runtime_budget,
                next_step="increase iterations or prove an always-on guard supplies the missing current-poll samples",
            )

    if not bool(getattr(args, "deep_research", False)):
        bounded_search = {
            "max_wallets_for_search": int(getattr(args, "max_wallets_for_search", 0)),
            "max_single_wallet_candidate_intents": int(getattr(args, "max_single_wallet_candidate_intents", 0)),
            "max_multi_wallet_base_intents": int(getattr(args, "max_multi_wallet_base_intents", 0)),
        }
        active_bounds = {key: value for key, value in bounded_search.items() if value > 0}
        if active_bounds:
            add(
                "bounded_profit_search_config",
                "profit_search_config_is_bounded",
                active_bounds=active_bounds,
                next_step="rerun with zero/unbounded profit-search limits before any live-readiness claim",
            )

    unlock = getattr(args, "_live_ready_unlock_context", {})
    if isinstance(unlock, dict) and unlock.get("active"):
        deferred_ranks = int(unlock.get("deferred_candidate_forward_probe_ranks") or 0)
        if deferred_ranks > 0:
            add(
                "focused_live_ready_unlock_rank_deferral",
                "candidate_forward_probe_ranks_deferred_by_live_ready_unlock",
                original_candidate_forward_probe_ranks=unlock.get("original_candidate_forward_probe_ranks"),
                effective_candidate_forward_probe_ranks=unlock.get("effective_candidate_forward_probe_ranks"),
                deferred_candidate_forward_probe_ranks=deferred_ranks,
                candidate_id=unlock.get("candidate_id"),
                source_wallet=unlock.get("source_wallet"),
                next_step=unlock.get("next_full_coverage_command"),
            )
        if not bool(getattr(args, "skip_registry_sweep", False)):
            add(
                "focused_live_ready_unlock_registry_deferral",
                "registry_sweep_deferred_by_live_ready_unlock_focus",
                candidate_id=unlock.get("candidate_id"),
                source_wallet=unlock.get("source_wallet"),
                next_step=(
                    "run full registry rotation after the proof-led candidate unlock slice, or rerun deep research "
                    "with --disable-live-ready-unlock to prioritize broad paper backup coverage"
                ),
            )

    search_truth = live_readiness.get("search_truth") if isinstance(live_readiness.get("search_truth"), dict) else {}
    skipped_counts = (
        search_truth.get("skipped_candidate_counts")
        if isinstance(search_truth.get("skipped_candidate_counts"), dict)
        else {}
    )
    limited_counts = {str(key): int(value) for key, value in skipped_counts.items() if str(key).endswith("_limited") and int(value or 0) > 0}
    if limited_counts:
        add(
            "bounded_profit_search_evidence",
            "candidate_search_was_bounded_or_limited",
            limited_counts=limited_counts,
            next_step="remove the candidate cap and rerun profit/admission until skipped *_limited counts are zero",
        )

    status = "PASS"
    if any(event["kind"] == "command_timeout" for event in events) or any(
        str(event.get("blocker") or "").endswith("_wall_runtime_budget_exhausted")
        for event in events
    ):
        status = CORRECTION
    elif events:
        status = ANALYZE

    backlog_actions: list[dict[str, Any]] = []
    if blockers:
        backlog_actions.append(
            {
                "area": "wallet-copy limit pressure",
                "file": "scripts/run_wallet_copy_autonomous_repair.py",
                "function": "_limit_pressure_report",
                "severity": "P1" if status == CORRECTION else "P2",
                "action": (
                    "turn active time/scope/API/search limits into measured coverage or code changes; never let a "
                    "limit disappear by hiding, skipping, deleting, or narrowing the failing source"
                ),
                "verify": "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900",
                "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900",
                "blockers": sorted(set(blockers)),
                "limit_events": events[:20],
            }
        )

    return {
        "schema_version": 1,
        "status": status,
        "blockers": sorted(set(blockers)),
        "events": events,
        "backlog_actions": backlog_actions,
        "rule": (
            "limits are blockers to solve with longer runs, resumable slices, better parallelism, alternate data "
            "sources, or explicit measured replacement coverage; they are never a reason to call GREEN by removal"
        ),
    }


def _strategy_direction_backlog_actions(strategy_direction: dict[str, Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    operating_redesign = (
        strategy_direction.get("operating_redesign")
        if isinstance(strategy_direction.get("operating_redesign"), dict)
        else {}
    )
    if operating_redesign:
        next_system_action = (
            operating_redesign.get("next_system_action")
            if isinstance(operating_redesign.get("next_system_action"), dict)
            else {}
        )
        redesign_status = str(operating_redesign.get("status") or "")
        active_phase = str(next_system_action.get("phase") or operating_redesign.get("active_phase") or "unknown")
        redesign_blockers = [str(blocker) for blocker in (next_system_action.get("blockers") or []) if blocker]
        if redesign_status != PASS or redesign_blockers:
            actions.append(
                {
                    "area": "wallet-copy operating redesign",
                    "file": next_system_action.get("file") or "src/wallet_copy/strategy_selection.py",
                    "function": next_system_action.get("function") or "_operating_redesign",
                    "severity": "P1",
                    "action": next_system_action.get("action") or "execute the active operating redesign phase",
                    "verify": next_system_action.get("verify")
                    or "python3 scripts/select_wallet_copy_strategy_direction.py",
                    "next_command": next_system_action.get("next_command")
                    or next_system_action.get("verify")
                    or "python3 scripts/select_wallet_copy_strategy_direction.py",
                    "blockers": redesign_blockers or [f"operating_redesign_phase_{active_phase}_non_green"],
                    "active_phase": active_phase,
                    "phase_order": operating_redesign.get("phase_order") or [],
                    "design_principles": operating_redesign.get("design_principles") or [],
                    "development_program_next_major_change_action": operating_redesign.get(
                        "development_program_next_major_change_action"
                    ),
                    "development_research_next_change_action": operating_redesign.get(
                        "development_research_next_change_action"
                    ),
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            )

    program_review = (
        strategy_direction.get("development_program_review")
        if isinstance(strategy_direction.get("development_program_review"), dict)
        else {}
    )
    if program_review.get("full_rethink_required") is True:
        implementation_backlog = [
            row for row in (program_review.get("implementation_backlog") or []) if isinstance(row, dict)
        ]
        primary = implementation_backlog[0] if implementation_backlog else {}
        actions.append(
            {
                "area": "wallet-copy development program rethink",
                "file": primary.get("file") or "src/wallet_copy/strategy_selection.py",
                "function": primary.get("function") or "_development_program_review",
                "severity": "P1",
                "action": (
                    program_review.get("next_major_change_action")
                    or "perform a full research/development rethink before repeating the current lane"
                ),
                "verify": primary.get("verify") or "python3 scripts/select_wallet_copy_strategy_direction.py",
                "next_command": "python3 scripts/select_wallet_copy_strategy_direction.py",
                "blockers": ["development_program_full_rethink_required"],
                "strategic_traps": program_review.get("strategic_traps") or [],
                "stop_doing": program_review.get("stop_doing") or [],
                "implementation_backlog": implementation_backlog,
            }
        )

    limit_review = (
        strategy_direction.get("development_limit_review")
        if isinstance(strategy_direction.get("development_limit_review"), dict)
        else {}
    )
    limit_hit_lanes = [
        row
        for row in (limit_review.get("limit_hit_lanes") or [])
        if isinstance(row, dict) and row.get("lane_id")
    ]
    if limit_hit_lanes:
        primary = limit_hit_lanes[0]
        actions.append(
            {
                "area": "wallet-copy development lane logical limits",
                "file": "src/wallet_copy/strategy_selection.py",
                "function": "_development_limit_review",
                "severity": "P1",
                "action": (
                    "a development lane hit its logical no-improvement/same-blocker limit; reevaluate, rerank, "
                    "rebuild, rotate cohorts, or repair the copy path before repeating the same lane"
                ),
                "verify": "python3 scripts/select_wallet_copy_strategy_direction.py",
                "next_command": "python3 scripts/select_wallet_copy_strategy_direction.py",
                "blockers": [f"development_lane_logical_limit_hit_{row.get('lane_id')}" for row in limit_hit_lanes],
                "limit_hit_lanes": limit_hit_lanes,
                "next_change_action": limit_review.get("next_change_action") or primary.get("required_change_action"),
            }
        )

    decision = strategy_direction.get("decision") if isinstance(strategy_direction.get("decision"), dict) else {}
    directions = strategy_direction.get("directions") if isinstance(strategy_direction.get("directions"), list) else []
    top = directions[0] if directions and isinstance(directions[0], dict) else {}
    blockers = [str(blocker) for blocker in (top.get("blockers") or decision.get("blockers") or []) if blocker]
    if not top and not blockers:
        return actions

    runtime_proof = top.get("runtime_proof") if isinstance(top.get("runtime_proof"), dict) else {}
    wallet = str(top.get("wallet") or runtime_proof.get("source_wallet") or "")
    policy_id = str(runtime_proof.get("policy_id") or top.get("policy_id") or "")
    candidate_id = str(runtime_proof.get("candidate_id") or top.get("candidate_id") or "")
    direction_id = str(top.get("id") or decision.get("recommended_now") or "wallet_copy_strategy_direction")
    if "proof_led_candidate_not_attached_to_current_profit_rankings" in blockers:
        actions.append(
            {
                "area": "wallet-copy strategy direction",
                "file": "src/wallet_copy/profit_engine.py",
                "function": "profit candidate ranking and forward tracking queue",
                "severity": "P1",
                "action": (
                    "attach the proof-led runtime CLOB candidate to profit-engine ranking and candidate-forward "
                    "tracking instead of letting the best copyable wallet/policy stay outside the current "
                    f"profit queue; wallet={wallet}, policy_id={policy_id}, candidate_id={candidate_id}"
                ),
                "verify": (
                    "python3 scripts/run_wallet_copy_profit_engine.py "
                    "--history-state data/research/wallet_copy_history_state.json "
                    "--resolutions data/research/btc_resolutions_from_btcusdt_ticks.jsonl "
                    "--output data/research/wallet_copy_profit_engine_state.json "
                    "--max-unresolved-ratio 0.5 --slippage-bps 500 "
                    "--live-today-sprint-operator-approval-id OP-LIVE-20260703-BELA && "
                    "python3 scripts/select_wallet_copy_strategy_direction.py && "
                    "jq '.directions[0] | {id,status,wallet,runtime_proof,blockers}' "
                    "data/research/wallet_copy_strategy_direction_state.json"
                ),
                "next_command": (
                    "python3 scripts/select_wallet_copy_strategy_direction.py && "
                    "jq '.directions[0] | {id,status,wallet,runtime_proof,blockers}' "
                    "data/research/wallet_copy_strategy_direction_state.json"
                ),
                "blockers": blockers,
                "direction": direction_id,
                "source_wallet": wallet,
                "policy_id": policy_id,
                "candidate_id": candidate_id,
                "runtime_copy_proof": runtime_proof,
            }
        )
        return actions

    status = str(top.get("status") or decision.get("status") or "")
    if status in {ANALYZE, CORRECTION, WATCH}:
        actions.append(
            {
                "area": "wallet-copy strategy direction",
                "file": "src/wallet_copy/strategy_selection.py",
                "function": "build_strategy_direction_state",
                "severity": "P2",
                "action": (
                    "turn the selected non-green wallet-copy direction into a sharper measurement or code fix; "
                    f"direction={direction_id}, status={status}"
                ),
                "verify": "python3 scripts/select_wallet_copy_strategy_direction.py",
                "next_command": "python3 scripts/select_wallet_copy_strategy_direction.py",
                "blockers": blockers or [f"strategy_direction_status_{status.lower()}"],
                "direction": direction_id,
                "source_wallet": wallet,
                "policy_id": policy_id,
                "candidate_id": candidate_id,
            }
        )
    return actions


def _source_route_endpoint_statuses(source_route: dict[str, Any]) -> dict[str, str]:
    endpoints = source_route.get("endpoints") if isinstance(source_route.get("endpoints"), (dict, list)) else {}
    if isinstance(endpoints, dict):
        return {
            str(name): str(row.get("status") or "UNKNOWN")
            for name, row in endpoints.items()
            if isinstance(row, dict)
        }
    return {
        str(row.get("name") or row.get("host") or f"endpoint_{index}"): str(row.get("status") or "UNKNOWN")
        for index, row in enumerate(endpoints)
        if isinstance(row, dict)
    }


def _source_route_diagnostic_fields(source_route: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(source_route, dict):
        return {}
    keys = [
        "route_class_counts",
        "reset_by_host",
        "reset_by_transport",
        "best_nonpassing_variants",
        "direct_dns_family_counts_by_endpoint",
        "source_base_overrides",
        "source_proxy_configured",
        "source_proxy_env_var",
        "external_route_required",
        "code_route_recovery_exhausted",
        "required_operator_inputs",
    ]
    diagnostics: dict[str, Any] = {}
    for key in keys:
        if key in source_route:
            diagnostics[key] = source_route.get(key)
    return diagnostics


def _source_route_backlog_actions(source_route: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    if not isinstance(source_route, dict) or not source_route:
        return [
            {
                "area": "wallet-copy source route",
                "file": "scripts/probe_polymarket_source_routes.py",
                "function": "build_state",
                "severity": "P1",
                "action": (
                    "create fresh Data API/Gamma/CLOB source-route evidence before interpreting copyability; "
                    "missing source-route state cannot be treated as green"
                ),
                "verify": (
                    f"python3 scripts/probe_polymarket_source_routes.py --output {args.source_route_state} "
                    "--timeout-s 24 --probe-profile heartbeat --max-wall-runtime-s 180 --print"
                ),
                "next_command": (
                    f"python3 scripts/probe_polymarket_source_routes.py --output {args.source_route_state} "
                    "--timeout-s 24 --probe-profile heartbeat --max-wall-runtime-s 180 --print"
                ),
                "blockers": ["source_route_state_missing"],
            }
        ]
    status = str(source_route.get("status") or "UNKNOWN")
    if source_route_allows_measurement(status):
        return []
    endpoint_statuses = _source_route_endpoint_statuses(source_route)
    return [
        {
            "area": "wallet-copy source route",
            "file": "src/wallet_copy/http_client.py",
            "function": "PolymarketHttpClient.variants/request plus scripts/probe_polymarket_source_routes.py",
            "severity": "P1",
            "action": (
                "add or configure a measured alternate Polymarket source route for Data API, Gamma, and CLOB "
                "via POLYMARKET_SOURCE_PROXY_URL, POLYMARKET_HTTPS_PROXY, POLYMARKET_DATA_API_BASE_URL, "
                "POLYMARKET_GAMMA_API_BASE_URL, POLYMARKET_CLOB_API_BASE_URL, or an equivalent relay, then "
                "prove the source truth passes instead of calling copyability green while source-route truth is failing"
            ),
            "verify": (
                f"python3 scripts/probe_polymarket_source_routes.py --output {args.source_route_state} "
                "--timeout-s 24 --probe-profile heartbeat --max-wall-runtime-s 180 --print && "
                "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240"
            ),
            "next_command": (
                f"python3 scripts/probe_polymarket_source_routes.py --output {args.source_route_state} "
                "--timeout-s 24 --probe-profile heartbeat --max-wall-runtime-s 180 --print"
            ),
            "blockers": [f"source_route_{status.lower()}"],
            "endpoint_statuses": endpoint_statuses,
            "next_source_action": source_route.get("next_action"),
            **_source_route_diagnostic_fields(source_route),
        }
    ]


def _write_backlog(
    post_audit: dict[str, Any],
    args: argparse.Namespace,
    *,
    extra_actions: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    next_actions = post_audit.get("next_actions") if isinstance(post_audit.get("next_actions"), list) else []
    if extra_actions:
        next_actions = [*next_actions, *extra_actions]
    feedback = post_audit.get("feedback_loop") if isinstance(post_audit.get("feedback_loop"), dict) else {}
    non_green = feedback.get("non_green_checks") if isinstance(feedback.get("non_green_checks"), list) else []
    generated_at = utc_now_iso()
    existing = load_json(args.backlog_state, default={})
    items_by_key: dict[str, dict[str, Any]] = {}
    if isinstance(existing, dict):
        for row in existing.get("items") or []:
            if isinstance(row, dict):
                row_non_green = row.get("non_green_checks") if isinstance(row.get("non_green_checks"), list) else []
                item = _normalize_backlog_action(row, non_green=row_non_green, generated_at=generated_at)
                for key, value in row.items():
                    if value not in (None, "", [], {}):
                        item[key] = value
                item = _normalize_stale_status_terms(item)
                item["key"] = _backlog_key(item)
                item["id"] = item.get("id") or _backlog_id(item["key"])
                item["blockers"] = item.get("blockers") or _non_green_blockers(row_non_green)
                item["next_command"] = item.get("next_command") or item.get("verify")
                item["severity"] = item.get("severity") or (
                    "P1" if any(":FAIL" in blocker for blocker in item.get("blockers") or []) else "P2"
                )
                items_by_key[item["key"]] = item
    written: list[dict[str, Any]] = []
    active_keys: set[str] = set()
    active_groups: dict[str, list[str]] = {}
    for action in next_actions:
        if not isinstance(action, dict):
            continue
        item = _normalize_backlog_action(action, non_green=non_green, generated_at=generated_at)
        previous = items_by_key.get(str(item["key"]))
        if previous:
            item["id"] = previous.get("id") or item["id"]
            item["first_seen_at"] = previous.get("first_seen_at") or previous.get("generated_at") or generated_at
            item["seen_count"] = int(previous.get("seen_count") or 1) + 1
        else:
            item["first_seen_at"] = generated_at
            item["seen_count"] = 1
        items_by_key[str(item["key"])] = item
        active_keys.add(str(item["key"]))
        active_groups.setdefault(_backlog_group(item), []).append(str(item["key"]))
        written.append(item)
        append_jsonl(args.backlog_log, item)

    for key, item in list(items_by_key.items()):
        if key in active_keys or str(item.get("status") or "OPEN") != "OPEN":
            continue
        replacement_keys = active_groups.get(_backlog_group(item), [])
        if not replacement_keys:
            continue
        item["status"] = "SUPERSEDED"
        item["superseded_at"] = generated_at
        item["superseded_by"] = replacement_keys
        item["superseded_reason"] = "same area/file/function has a fresher code-level action from the latest audit"
        items_by_key[key] = item

    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_autonomous_backlog",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "items": sorted(items_by_key.values(), key=lambda row: str(row.get("key") or "")),
    }
    atomic_write_json(args.backlog_state, payload)
    return written


def _compact_progress_action(
    *,
    status: str,
    backlog_items: list[dict[str, Any]],
    command_results: list[dict[str, Any]],
    source_route: dict[str, Any],
) -> dict[str, Any]:
    """Return one machine-readable next action for heartbeat reporting."""

    severity_rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    open_items = [
        item
        for item in backlog_items
        if isinstance(item, dict) and str(item.get("status") or "OPEN") == "OPEN"
    ]
    if source_route_is_recovered_degraded(source_route):
        open_items = [
            item
            for item in open_items
            if str(item.get("area") or "").lower() != "wallet-copy source route"
        ]
    if open_items:
        route_status = str(source_route.get("status") or "")

        def priority(row: dict[str, Any]) -> tuple[int, int, int, str, str]:
            area = str(row.get("area") or "")
            hard_source_route = (
                route_status
                and not source_route_allows_measurement(route_status)
                and area == "wallet-copy source route"
            )
            candidate_forward_tactic = area in {
                "candidate-forward flow-control shadow PnL measurement",
                "candidate-forward micro/depth copy tactic planning",
                "candidate-forward aggressive copy policy validation",
            }
            development_program_rethink = area == "wallet-copy development program rethink"
            return (
                0 if hard_source_route else 1,
                0 if development_program_rethink else 1,
                0 if candidate_forward_tactic else 1,
                severity_rank.get(str(row.get("severity") or "P2"), 9),
                area,
                str(row.get("file") or ""),
            )

        item = sorted(
            open_items,
            key=priority,
        )[0]
        progress_type = str(item.get("progress_type") or item.get("type") or "")
        if not progress_type and str(item.get("area") or "") == "candidate-forward flow-control shadow PnL measurement":
            progress_type = "sharper_measurement"
        if not progress_type:
            progress_type = "code_level_backlog"
        action = {
            "status": "OPEN",
            "type": progress_type,
            "severity": item.get("severity") or "P2",
            "area": item.get("area"),
            "file": item.get("file"),
            "function": item.get("function"),
            "action": item.get("action"),
            "blockers": item.get("blockers") or [],
            "next_command": item.get("next_command") or item.get("verify"),
            "verify": item.get("verify"),
        }
        action.update(_source_route_diagnostic_fields(item))
        return _normalize_stale_status_terms(action)

    failed_commands = [row for row in command_results if isinstance(row, dict) and not row.get("ok")]
    if failed_commands:
        command = failed_commands[0]
        return {
            "status": "OPEN",
            "type": "sharper_measurement",
            "severity": "P1",
            "area": "wallet-copy command execution",
            "action": f"repair or rerun failed autonomous command {command.get('name')}",
            "blockers": [str(command.get("stderr_tail") or command.get("returncode") or "command_failed")[:500]],
            "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
            "verify": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
        }

    route_status = str(source_route.get("status") or "")
    if route_status and not source_route_allows_measurement(route_status):
        action = {
            "status": "OPEN",
            "type": "sharper_measurement",
            "severity": "P1",
            "area": "wallet-copy source route",
            "action": "refresh direct Polymarket Data API/Gamma/CLOB route health and keep live admission blocked until source truth passes",
            "blockers": [f"source_route_{route_status.lower()}"],
            "next_command": "python3 scripts/probe_polymarket_source_routes.py --output data/research/wallet_copy_source_route_state.json --print",
            "verify": "python3 scripts/probe_polymarket_source_routes.py --output data/research/wallet_copy_source_route_state.json --print",
        }
        action.update(_source_route_diagnostic_fields(source_route))
        return action

    if route_status and source_route_is_recovered_degraded(route_status):
        action = {
            "status": "OPEN",
            "type": "sharper_measurement",
            "severity": "P1",
            "area": "wallet-copy current-poll copyability",
            "action": (
                "use the measured recovered source route to rerun active hot-lane and candidate-forward "
                "all-order wallet-copy measurement; repair missing current-poll BUY source events instead of "
                "looping on source-route configuration"
            ),
            "blockers": [
                f"source_route_{route_status.lower()}",
                "all_order_buy_source_events_missing",
                "no_current_all_order_source_events",
            ],
            "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
            "verify": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
        }
        action.update(_source_route_diagnostic_fields(source_route))
        return action

    if status in {"GREEN", "PASS"}:
        return {
            "status": "NONE",
            "type": "none",
            "severity": None,
            "area": "wallet-copy workflow",
            "action": "no non-green progress action required",
            "blockers": [],
            "next_command": None,
            "verify": None,
        }

    return {
        "status": "OPEN",
        "type": "sharper_measurement",
        "severity": "P2",
        "area": "wallet-copy workflow",
        "action": "rerun autonomous repair to produce a bounded fix, sharper measurement, or code-level backlog item",
        "blockers": [f"workflow_status_{str(status or 'unknown').lower()}"],
        "next_command": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
        "verify": "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240",
    }


def _selected_ints(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, int]:
    selected: dict[str, int] = {}
    for key in keys:
        try:
            selected[key] = int(payload.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return selected


def _parse_utc_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _current_poll_progress_diagnostics(
    tracker_paths: list[tuple[str, str | Path]] | None = None,
    *,
    now: datetime | None = None,
    stale_after_s: float = 1800.0,
) -> dict[str, Any]:
    """Lift concrete current-poll blockers into the heartbeat progress action."""

    reference_time = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    paths = tracker_paths or [
        ("candidate_forward", ROOT / "data/research/wallet_copy_candidate_forward_live_tracking_state.json"),
        ("active_forward", ROOT / "data/research/wallet_copy_active_forward_probe_live_tracking_state.json"),
        ("active_hotlane", ROOT / "data/research/wallet_copy_active_hotlane_live_tracking_state.json"),
        ("registry_sweep", ROOT / "data/research/wallet_copy_registry_sweep_live_tracking_state.json"),
        ("canonical", ROOT / "data/research/wallet_copy_live_tracking_state.json"),
    ]
    rows: list[dict[str, Any]] = []
    for label, raw_path in paths:
        path = Path(raw_path)
        state = load_json(path, default={})
        if not isinstance(state, dict):
            continue
        summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        if not summary:
            continue
        copy_efficiency = (
            summary.get("copy_efficiency") if isinstance(summary.get("copy_efficiency"), dict) else {}
        )
        copy_summary = (
            copy_efficiency.get("summary") if isinstance(copy_efficiency.get("summary"), dict) else {}
        )
        all_order = (
            summary.get("all_order_exact_copy")
            if isinstance(summary.get("all_order_exact_copy"), dict)
            else {}
        )
        execution_tactic_plan = (
            all_order.get("execution_tactic_plan")
            if isinstance(all_order.get("execution_tactic_plan"), dict)
            else {}
        )
        no_actionable_tactic_diagnostics = (
            execution_tactic_plan.get("no_actionable_tactic_diagnostics")
            if isinstance(execution_tactic_plan.get("no_actionable_tactic_diagnostics"), dict)
            else {}
        )
        aggressive_tactic_replay = (
            all_order.get("aggressive_tactic_replay")
            if isinstance(all_order.get("aggressive_tactic_replay"), dict)
            else {}
        )
        micro_batch_probe = (
            all_order.get("micro_batch_all_order_probe")
            if isinstance(all_order.get("micro_batch_all_order_probe"), dict)
            else {}
        )
        hot_path = (
            summary.get("hot_path_adaptive")
            if isinstance(summary.get("hot_path_adaptive"), dict)
            else {}
        )
        hot_summary = hot_path.get("summary") if isinstance(hot_path.get("summary"), dict) else {}
        freshness = (
            hot_summary.get("freshness_diagnostics")
            if isinstance(hot_summary.get("freshness_diagnostics"), dict)
            else {}
        )
        current_poll = (
            summary.get("current_poll_diagnostics")
            if isinstance(summary.get("current_poll_diagnostics"), dict)
            else {}
        )
        information_source_fusion = (
            summary.get("information_source_fusion")
            if isinstance(summary.get("information_source_fusion"), dict)
            else {}
        )
        fusion_sources = (
            information_source_fusion.get("sources")
            if isinstance(information_source_fusion.get("sources"), dict)
            else {}
        )
        fusion_wallet_api = (
            fusion_sources.get("wallet_data_api")
            if isinstance(fusion_sources.get("wallet_data_api"), dict)
            else {}
        )
        fusion_clob_books = (
            fusion_sources.get("clob_books")
            if isinstance(fusion_sources.get("clob_books"), dict)
            else {}
        )
        ladder = (
            current_poll.get("current_poll_ladder")
            if isinstance(current_poll.get("current_poll_ladder"), dict)
            else {}
        )
        state_generated_at = state.get("generated_at")
        state_dt = _parse_utc_datetime(state_generated_at)
        state_age_s = None
        if state_dt is not None:
            state_age_s = max(0.0, (reference_time - state_dt).total_seconds())
        state_stale = state_age_s is not None and state_age_s > float(stale_after_s)
        row = {
            "tracker": label,
            "state_path": str(path.relative_to(ROOT)) if path.is_absolute() and path.is_relative_to(ROOT) else str(path),
            "generated_at": state_generated_at,
            "state_age_s": round(state_age_s, 3) if state_age_s is not None else None,
            "state_stale": state_stale,
            "state_stale_after_s": float(stale_after_s),
            "copy_efficiency_status": copy_efficiency.get("status"),
            "required_buy_copy_events": copy_summary.get("required_buy_copy_events"),
            "clob_filled_buy_copy_events": copy_summary.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": copy_summary.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": copy_summary.get("rejected_buy_copy_events"),
            "missed_buy_copy_events": copy_summary.get("missed_buy_copy_events"),
            "all_order_status": all_order.get("status"),
            "all_order_live_truth_status": all_order.get("live_truth_status"),
            "all_order_buy_source_events": all_order.get("buy_source_events"),
            "all_order_clob_filled_buy_copy_events": all_order.get("clob_filled_buy_copy_events"),
            "all_order_rejected_buy_copy_events": all_order.get("rejected_buy_copy_events"),
            "all_order_copyability_rejected_buy_events": all_order.get("copyability_rejected_buy_events"),
            "all_order_live_truth_blockers": list(all_order.get("live_truth_blockers") or [])[:12],
            "all_order_execution_tactic_plan": {
                "status": execution_tactic_plan.get("status"),
                "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
                "blockers": list(execution_tactic_plan.get("blockers") or [])[:12],
                "no_actionable_tactic_diagnostics": no_actionable_tactic_diagnostics,
                "paper_only": execution_tactic_plan.get("paper_only"),
                "live_orders_allowed": execution_tactic_plan.get("live_orders_allowed"),
            }
            if execution_tactic_plan
            else {},
            "paper_tactic_repair_lane": {
                "status": no_actionable_tactic_diagnostics.get("status"),
                "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
                "recommended_next_measurement": no_actionable_tactic_diagnostics.get(
                    "recommended_next_measurement"
                ),
                "dominant_reject_category": no_actionable_tactic_diagnostics.get("dominant_reject_category"),
                "recommended_repair_lane": no_actionable_tactic_diagnostics.get("recommended_repair_lane"),
                "best_profile_id": no_actionable_tactic_diagnostics.get("best_profile_id"),
                "best_profile_incremental_pass_events_vs_strict": no_actionable_tactic_diagnostics.get(
                    "best_profile_incremental_pass_events_vs_strict"
                ),
                "micro_batch_filled_child_events": no_actionable_tactic_diagnostics.get(
                    "micro_batch_filled_child_events"
                ),
                "micro_batch_rejected_child_events": no_actionable_tactic_diagnostics.get(
                    "micro_batch_rejected_child_events"
                ),
                "micro_batch_overcopy_usd": no_actionable_tactic_diagnostics.get("micro_batch_overcopy_usd"),
                "micro_batch_improves_strict_fill_count": no_actionable_tactic_diagnostics.get(
                    "micro_batch_improves_strict_fill_count"
                ),
                "micro_batch_actionable_for_paper_replay": no_actionable_tactic_diagnostics.get(
                    "micro_batch_actionable_for_paper_replay"
                ),
                "micro_batch_research_only_not_live_admission": no_actionable_tactic_diagnostics.get(
                    "micro_batch_research_only_not_live_admission"
                ),
                "micro_batch_actionability_blockers": list(
                    no_actionable_tactic_diagnostics.get("micro_batch_actionability_blockers") or []
                ),
                "micro_min_order_actionability": no_actionable_tactic_diagnostics.get(
                    "micro_min_order_actionability"
                )
                if isinstance(no_actionable_tactic_diagnostics.get("micro_min_order_actionability"), dict)
                else {},
                "reject_taxonomy": no_actionable_tactic_diagnostics.get("reject_taxonomy")
                if isinstance(no_actionable_tactic_diagnostics.get("reject_taxonomy"), dict)
                else {},
                "paper_only": no_actionable_tactic_diagnostics.get("paper_only"),
                "live_orders_allowed": no_actionable_tactic_diagnostics.get("live_orders_allowed"),
            }
            if no_actionable_tactic_diagnostics
            else {},
            "all_order_aggressive_tactic_replay": {
                "status": aggressive_tactic_replay.get("status"),
                "profile_id": aggressive_tactic_replay.get("profile_id"),
                "filled_orders": aggressive_tactic_replay.get("filled_orders"),
                "rejected_orders": aggressive_tactic_replay.get("rejected_orders"),
                "incremental_filled_orders_vs_strict": aggressive_tactic_replay.get(
                    "incremental_filled_orders_vs_strict"
                ),
                "cost_delta_usd": aggressive_tactic_replay.get("cost_delta_usd"),
                "paper_only": aggressive_tactic_replay.get("paper_only"),
                "live_orders_allowed": aggressive_tactic_replay.get("live_orders_allowed"),
            }
            if aggressive_tactic_replay
            else {},
            "all_order_micro_batch_probe": {
                "status": micro_batch_probe.get("status"),
                "source_buy_intents": micro_batch_probe.get("source_buy_intents"),
                "probe_groups": micro_batch_probe.get("probe_groups"),
                "pass_groups": micro_batch_probe.get("pass_groups"),
                "rejected_groups": micro_batch_probe.get("rejected_groups"),
                "covered_child_events": micro_batch_probe.get("covered_child_events"),
                "filled_child_events": micro_batch_probe.get("filled_child_events"),
                "rejected_child_events": micro_batch_probe.get("rejected_child_events"),
                "incremental_filled_child_events_vs_strict": micro_batch_probe.get(
                    "incremental_filled_child_events_vs_strict"
                ),
                "improves_strict_fill_count": micro_batch_probe.get("improves_strict_fill_count"),
                "actionable_for_paper_replay": micro_batch_probe.get("actionable_for_paper_replay"),
                "research_only_not_live_admission": micro_batch_probe.get("research_only_not_live_admission"),
                "actionability_blockers": list(micro_batch_probe.get("actionability_blockers") or []),
                "overcopy_usd": micro_batch_probe.get("overcopy_usd"),
                "overcopy_pct_of_exact_total": micro_batch_probe.get("overcopy_pct_of_exact_total"),
                "overcopy_per_filled_child_event_usd": micro_batch_probe.get(
                    "overcopy_per_filled_child_event_usd"
                ),
                "micro_batch_exact_no_overcopy": micro_batch_probe.get("micro_batch_exact_no_overcopy")
                if isinstance(micro_batch_probe.get("micro_batch_exact_no_overcopy"), dict)
                else {},
                "micro_batch_min_order_research": micro_batch_probe.get("micro_batch_min_order_research")
                if isinstance(micro_batch_probe.get("micro_batch_min_order_research"), dict)
                else {},
                "reject_reason_counts": micro_batch_probe.get("reject_reason_counts") or {},
                "paper_only": micro_batch_probe.get("paper_only"),
                "live_orders_allowed": micro_batch_probe.get("live_orders_allowed"),
            }
            if micro_batch_probe
            else {},
            "hot_path_status": hot_path.get("status"),
            "hot_path_blockers": list(hot_path.get("blockers") or [])[:12],
            "information_source_fusion": {
                "status": information_source_fusion.get("status"),
                "next_action": information_source_fusion.get("next_action"),
                "blockers": list(information_source_fusion.get("blockers") or [])[:12],
                "wallet_data_api_status": fusion_wallet_api.get("status"),
                "wallet_data_api_raw_rows": fusion_wallet_api.get("raw_rows"),
                "wallet_data_api_fresh_buy_rows_le_10s": fusion_wallet_api.get(
                    "fresh_buy_rows_le_10s"
                ),
                "wallet_data_api_parallel_sources": fusion_wallet_api.get("parallel_data_api_sources"),
                "wallet_data_api_trade_query_keys": fusion_wallet_api.get("trade_query_keys") or [],
                "clob_books_status": fusion_clob_books.get("status"),
                "clob_books_current_poll_ok_rows": fusion_clob_books.get("current_poll_ok_rows"),
                "clob_books_admission_window_ok_rows": fusion_clob_books.get("admission_window_ok_rows"),
            }
            if information_source_fusion
            else {},
            "current_poll_moves": hot_summary.get("current_poll_moves"),
            "pass_signals": hot_summary.get("pass_signals"),
            "runtime_fresh_buy_events_le_cap": freshness.get("runtime_fresh_buy_events_le_cap"),
            "tracker_fresh_buy_events_le_cap": freshness.get("tracker_fresh_buy_events_le_cap"),
            "source_feed_delayed": freshness.get("source_feed_delayed"),
            "latest_buy_event_lag_s": freshness.get("latest_buy_event_lag_s"),
            "freshness_transition_counts": freshness.get("freshness_transition_counts") or {},
            "top_filter_reasons_by_wallet": list(freshness.get("top_filter_reasons_by_wallet") or [])[:3],
            "zero_current_poll_root_cause": current_poll.get("zero_current_poll_root_cause"),
            "fresh_buy_loss_stage": current_poll.get("fresh_buy_loss_stage"),
            "current_poll_top_copyability_reject_reason": current_poll.get(
                "current_poll_top_copyability_reject_reason"
            ),
            "current_poll_copyability_rejected_reason_counts": current_poll.get(
                "current_poll_copyability_rejected_reason_counts"
            )
            or {},
            "current_poll_top_copyability_reject_blocker": current_poll.get(
                "current_poll_top_copyability_reject_blocker"
            ),
            "current_poll_copyability_rejected_blocker_counts": current_poll.get(
                "current_poll_copyability_rejected_blocker_counts"
            )
            or {},
            "current_poll_copyability_rejected_samples": list(
                current_poll.get("current_poll_copyability_rejected_samples") or []
            )[:5],
            "current_poll_source_trade_book_timing_rows": current_poll.get(
                "current_poll_source_trade_book_timing_rows"
            ),
            "current_poll_source_trade_book_timing_issue_counts": current_poll.get(
                "current_poll_source_trade_book_timing_issue_counts"
            )
            or {},
            "current_poll_source_trade_book_timing_samples": list(
                current_poll.get("current_poll_source_trade_book_timing_samples") or []
            )[:5],
            "current_poll_fresh_at_fetch_start_stale_at_decision_rows": current_poll.get(
                "current_poll_fresh_at_fetch_start_stale_at_decision_rows"
            ),
            "current_poll_fresh_at_fetch_start_stale_at_decision_samples": list(
                current_poll.get("current_poll_fresh_at_fetch_start_stale_at_decision_samples") or []
            )[:5],
            "current_poll_stale_before_wallet_fetch_rows": current_poll.get(
                "current_poll_stale_before_wallet_fetch_rows"
            ),
            "current_poll_stale_before_wallet_fetch_samples": list(
                current_poll.get("current_poll_stale_before_wallet_fetch_samples") or []
            )[:5],
            "current_poll_copyability_staleness_origin_counts": current_poll.get(
                "current_poll_copyability_staleness_origin_counts"
            )
            or {},
            "current_poll_ladder": _selected_ints(
                ladder,
                (
                    "wallets_polled",
                    "raw_rows",
                    "normalized_events",
                    "runtime_limited_events_skipped",
                    "runtime_limited_wallet_reports",
                    "fresh_buy_rows_le_10s",
                    "fresh_after_dedupe_buy_rows_le_10s",
                    "fresh_profit_policy_buy_rows_le_10s",
                    "fresh_copyability_buy_rows_le_10s",
                    "after_dedupe_buy_rows",
                    "profit_policy_buy_rows",
                    "copyability_buy_rows",
                    "copyability_rejected_buy_rows",
                    "copy_intents",
                    "clob_filled_orders",
                    "source_route_degraded_recovered_rows",
                ),
            ),
        }
        if any(
            [
                row["all_order_buy_source_events"],
                row["required_buy_copy_events"],
                row["current_poll_moves"],
                row["information_source_fusion"],
                row["source_feed_delayed"],
                row["current_poll_top_copyability_reject_reason"],
                row["current_poll_fresh_at_fetch_start_stale_at_decision_rows"],
                row["state_stale"],
                row["zero_current_poll_root_cause"]
                and row["zero_current_poll_root_cause"] != "raw_source_rows_zero",
            ]
        ):
            rows.append(row)

    def score(row: dict[str, Any]) -> tuple[int, int, int, str]:
        all_order_bad = str(row.get("all_order_live_truth_status") or "") in {CORRECTION, "FAIL"}
        source_delayed = row.get("source_feed_delayed") is True
        try:
            rejected = int(row.get("all_order_rejected_buy_copy_events") or 0) + int(
                (row.get("current_poll_ladder") or {}).get("copyability_rejected_buy_rows") or 0
            )
        except (TypeError, ValueError):
            rejected = 0
        return (0 if all_order_bad else 1, 0 if source_delayed else 1, -rejected, str(row.get("tracker") or ""))

    rows = sorted(rows, key=score)[:5]
    blockers: list[str] = []
    for row in rows:
        label = str(row.get("tracker") or "tracker")
        if row.get("state_stale") is True:
            blockers.append(f"{label}_state_stale")
        if row.get("source_feed_delayed") is True:
            blockers.append(f"{label}_source_feed_delayed")
        root = str(row.get("zero_current_poll_root_cause") or "")
        if root and root != "raw_source_rows_zero":
            blockers.append(f"{label}_zero_current_poll_{root}")
        fresh_stage = str(row.get("fresh_buy_loss_stage") or "")
        if fresh_stage:
            blockers.append(f"{label}_fresh_buy_loss_{fresh_stage}")
        reason = str(row.get("current_poll_top_copyability_reject_reason") or "")
        if reason:
            blockers.append(f"{label}_copyability_{reason}")
        blocker = str(row.get("current_poll_top_copyability_reject_blocker") or "")
        if blocker and blocker != reason:
            blockers.append(f"{label}_copyability_blocker_{blocker}")
        if int(row.get("current_poll_source_trade_book_timing_rows") or 0) > 0:
            blockers.append(f"{label}_source_trade_book_timing_unverified")
        if int(row.get("current_poll_fresh_at_fetch_start_stale_at_decision_rows") or 0) > 0:
            blockers.append(f"{label}_fresh_buy_staled_during_source_fetch")
        if int(row.get("current_poll_stale_before_wallet_fetch_rows") or 0) > 0:
            blockers.append(f"{label}_source_feed_pre_fetch_stale")
        live_truth = str(row.get("all_order_live_truth_status") or "")
        if live_truth and live_truth != "PASS":
            blockers.append(f"{label}_all_order_live_truth_{live_truth.lower()}")
        fusion = row.get("information_source_fusion") if isinstance(row.get("information_source_fusion"), dict) else {}
        fusion_status = str(fusion.get("status") or "")
        if fusion_status and fusion_status != "PASS":
            blockers.append(f"{label}_information_source_fusion_{fusion_status.lower()}")
        fusion_next_action = str(fusion.get("next_action") or "")
        if fusion_next_action:
            blockers.append(f"{label}_information_source_next_action_{fusion_next_action}")
    return {
        "status": WATCH if rows else ANALYZE,
        "role": "diagnostic_only_not_live_admission_truth",
        "blockers": sorted(set(blockers)),
        "tracker_rows": rows,
        "verify": (
            "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240 && "
            "jq '.progress_action.current_poll_diagnostics[]?|"
            "{tracker,generated_at,state_age_s,state_stale,fresh_buy_loss_stage,"
            "information_source_fusion,"
            "current_poll_fresh_at_fetch_start_stale_at_decision_rows,"
            "current_poll_stale_before_wallet_fetch_rows,current_poll_source_trade_book_timing_issue_counts,"
            "current_poll_source_trade_book_timing_samples,paper_tactic_repair_lane,"
            "all_order_execution_tactic_plan,all_order_micro_batch_probe}' "
            "data/research/wallet_copy_autonomous_repair_state.json"
        ),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _attach_current_poll_progress_diagnostics(action: dict[str, Any]) -> dict[str, Any]:
    area = str(action.get("area") or "").lower()
    if "current-poll" not in area and "active hot-lane" not in area and "candidate-forward" not in area:
        return action
    diagnostics = _current_poll_progress_diagnostics()
    rows = diagnostics.get("tracker_rows") if isinstance(diagnostics, dict) else []
    if not rows:
        return action
    updated = dict(action)
    diagnostic_blockers = [str(row) for row in (diagnostics.get("blockers") or []) if row]
    updated["blockers"] = sorted(
        {
            *(str(row) for row in (updated.get("blockers") or []) if row),
            *diagnostic_blockers,
        }
    )
    updated["current_poll_diagnostics"] = rows
    shadow_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        plan = row.get("all_order_execution_tactic_plan")
        if not isinstance(plan, dict):
            continue
        shadow_plan = plan.get("flow_control_shadow_measurement_plan")
        if not isinstance(shadow_plan, dict) or not shadow_plan:
            diagnostics = plan.get("no_actionable_tactic_diagnostics")
            if isinstance(diagnostics, dict):
                shadow_plan = diagnostics.get("flow_control_shadow_measurement_plan")
        if (
            isinstance(shadow_plan, dict)
            and shadow_plan
            and _safe_int(shadow_plan.get("incremental_pass_events_vs_strict")) > 0
        ):
            shadow_rows.append((row, shadow_plan))
    if shadow_rows:
        shadow_row, shadow_plan = shadow_rows[0]
        shadow_verify = str(
            shadow_plan.get("verification_command")
            or "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900"
        )
        shadow_blockers = [str(item) for item in (shadow_plan.get("blockers") or []) if item]
        updated["area"] = "candidate-forward flow-control shadow PnL measurement"
        updated["type"] = "sharper_measurement"
        updated["progress_type"] = "sharper_measurement"
        updated["file"] = "src/wallet_copy/copy_tactics.py"
        updated["function"] = "build_copy_execution_tactic_plan"
        updated["next_command"] = shadow_verify
        updated["verify"] = (
            f"{shadow_verify} && "
            "jq '.progress_action|{area,next_command,shadow_profile_id,"
            "shadow_incremental_pass_events_vs_strict,current_poll_diagnostics}' "
            "data/research/wallet_copy_autonomous_repair_state.json"
        )
        updated["shadow_tracker"] = shadow_row.get("tracker")
        updated["shadow_profile_id"] = shadow_plan.get("profile_id")
        updated["blocked_recommended_tactic"] = shadow_plan.get("blocked_recommended_tactic")
        updated["shadow_profile_pass_events"] = shadow_plan.get("best_profile_pass_events")
        updated["shadow_incremental_pass_events_vs_strict"] = shadow_plan.get(
            "incremental_pass_events_vs_strict"
        )
        updated["flow_control_next_action"] = shadow_plan.get("flow_control_next_action")
        updated["paper_only"] = True
        updated["live_orders_allowed"] = False
        updated["blockers"] = sorted(
            {
                *(str(row) for row in (updated.get("blockers") or []) if row),
                *shadow_blockers,
                "flow_control_shadow_pnl_measurement_required",
            }
        )
        updated["action"] = (
            "persist the current-poll flow-control-blocked profile as paper-only shadow PnL evidence; "
            f"tracker={shadow_row.get('tracker')}, profile={shadow_plan.get('profile_id')}, "
            f"incremental_pass_events_vs_strict={shadow_plan.get('incremental_pass_events_vs_strict')}; "
            "live remains blocked until fresh strict CopyIntent truth has zero rejects, misses, and fallback fills"
        )
    tactic_lanes = [
        {
            "tracker": row.get("tracker"),
            **(
                row.get("paper_tactic_repair_lane")
                if isinstance(row.get("paper_tactic_repair_lane"), dict)
                else {}
            ),
        }
        for row in rows
        if isinstance(row.get("paper_tactic_repair_lane"), dict) and row.get("paper_tactic_repair_lane")
    ]
    if tactic_lanes:
        updated["paper_tactic_repair_lanes"] = tactic_lanes
    updated["diagnostic_role"] = diagnostics.get("role")
    updated["diagnostic_verify"] = diagnostics.get("verify")
    if diagnostic_blockers:
        tactic_note = ""
        if tactic_lanes:
            lane_bits = []
            for lane in tactic_lanes[:3]:
                tracker = lane.get("tracker")
                category = lane.get("dominant_reject_category")
                repair_lane = lane.get("recommended_repair_lane")
                if tracker and (category or repair_lane):
                    lane_bits.append(f"{tracker}:{category or 'unknown'}->{repair_lane or 'measure_more'}")
            if lane_bits:
                tactic_note = f"; paper tactic lanes: {', '.join(lane_bits)}"
        updated["action"] = (
            f"{updated.get('action')}; dominant measured blockers: "
            f"{', '.join(diagnostic_blockers[:5])}{tactic_note}"
        )
    return updated


def _status_from(
    post_audit: dict[str, Any],
    command_results: list[dict[str, Any]],
    backlog_items: list[dict[str, Any]],
    *,
    limit_pressure: dict[str, Any] | None = None,
    strategy_direction: dict[str, Any] | None = None,
) -> str:
    limit_status = str((limit_pressure or {}).get("status") or "PASS")
    if limit_status == CORRECTION:
        return CORRECTION
    expected_limit_failures = [
        row
        for row in command_results
        if not row.get("ok") and (row.get("timed_out") or row.get("wall_runtime_budget_exhausted"))
    ]
    unexpected_failures = [
        row
        for row in command_results
        if not row.get("ok") and row not in expected_limit_failures
    ]
    if unexpected_failures:
        return "BUG_SUSPECT"
    if expected_limit_failures:
        return CORRECTION
    strategy_decision = (
        strategy_direction.get("decision") if isinstance((strategy_direction or {}).get("decision"), dict) else {}
    )
    strategy_status = str(strategy_decision.get("status") or "PASS")
    if strategy_status == CORRECTION:
        return CORRECTION
    feedback = post_audit.get("feedback_loop") if isinstance(post_audit.get("feedback_loop"), dict) else {}
    strategy_primary_live_ready = bool(
        (strategy_direction or {}).get("live_ready")
        and (strategy_direction or {}).get("profitability_proven")
        and not list((strategy_direction or {}).get("live_readiness_blockers") or [])
    )
    if feedback.get("stuck_blockers") and not strategy_primary_live_ready:
        return "BUG_SUSPECT"
    if post_audit.get("learning_status") == "GREEN" and feedback.get("status") == "PASS" and limit_status == "PASS":
        if strategy_status in {ANALYZE, WATCH}:
            return ANALYZE
        return "GREEN"
    if limit_status != "PASS":
        return ANALYZE
    if command_results or backlog_items:
        return "REPAIR"
    return "WATCH"


def main() -> int:
    args = parse_args()
    _install_parent_signal_cleanup()
    lock_handle, lock_acquired, lock_holder = _acquire_single_instance_lock(args.lock_file)
    if not lock_acquired:
        _write_lock_conflict_state(args, lock_holder)
        print(
            json.dumps(
                {
                    "state": args.state,
                    "status": "BUG_SUSPECT",
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "blockers": ["duplicate_autonomous_repair_already_running"],
                    "running_instance": lock_holder,
                    "progress_action": {
                        "area": "wallet-copy runtime process hygiene",
                        "next_command": (
                            "ps -axo pid,ppid,etime,rss,command | rg "
                            "'run_wallet_copy_autonomous_repair|run_wallet_copy_pipeline_resume|run_wallet_copy_pipeline'"
                        ),
                    },
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
        try:
            lock_handle.close()
        except Exception:
            pass
        return 2
    try:
        foreign_writers = _detect_foreign_wallet_copy_writers()
        if foreign_writers:
            _write_foreign_writer_conflict_state(args, foreign_writers)
            print(
                json.dumps(
                    {
                        "state": args.state,
                        "status": "BUG_SUSPECT",
                        "paper_only": True,
                        "live_orders_allowed": False,
                        "blockers": ["foreign_wallet_copy_state_writer_already_running"],
                        "running_wallet_copy_writers": foreign_writers,
                        "progress_action": {
                            "area": "wallet-copy runtime process hygiene",
                            "next_command": (
                                "ps -axo pid,ppid,etime,rss,command | rg "
                                "'run_wallet_copy_profit_engine|run_wallet_copy_pipeline_resume|run_wallet_copy_pipeline'"
                            ),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
            return 2
        return _main_locked(args)
    finally:
        _release_single_instance_lock(lock_handle)


def _main_locked(args: argparse.Namespace) -> int:
    mission = mission_contract()
    wall_started = time.time()
    wall_budget_s = _wall_runtime_budget_s(args)
    command_results: list[dict[str, Any]] = []
    rotation_command = _runtime_log_rotation_command(args)
    command_results.append(
        _attach_command_metadata(
            _run_command(
                rotation_command["name"],
                rotation_command["purpose"],
                rotation_command["argv"],
                acceptable_returncodes=tuple(rotation_command.get("acceptable_returncodes") or (0,)),
                timeout_s=_wall_runtime_command_timeout(
                    wall_started,
                    wall_budget_s,
                    requested_s=min(float(args.command_timeout_s), 30.0),
                    reserve_s=5.0,
                ),
            ),
            rotation_command,
        )
    )
    pre_audit = _run_audit(
        append_feedback=False,
        timeout_s=_wall_runtime_command_timeout(
            wall_started,
            wall_budget_s,
            requested_s=float(args.command_timeout_s),
            reserve_s=5.0,
        ),
    )
    if _wall_runtime_exhausted(wall_started, wall_budget_s, min_remaining_s=10.0):
        plan = []
        command_results.append(
            _wall_runtime_exhausted_result(
                name="autonomous_repair_wall_runtime",
                purpose="stop before the heartbeat process becomes an idle or memory-pressure source",
                started=wall_started,
                budget_s=wall_budget_s,
            )
        )
    else:
        plan = build_repair_plan(pre_audit, args)
    for command in plan:
        if _wall_runtime_exhausted(wall_started, wall_budget_s, min_remaining_s=10.0):
            command_results.append(
                _wall_runtime_exhausted_result(
                    name=str(command.get("name") or "autonomous_repair_wall_runtime"),
                    purpose=str(command.get("purpose") or "wall runtime budget exhausted before next repair command"),
                    started=wall_started,
                    budget_s=wall_budget_s,
                )
            )
            break
        if command.get("dynamic_factory") == "candidate_forward_tracker_commands":
            resolved_commands = _candidate_forward_tracker_commands(args)
            if not resolved_commands:
                command_results.append(
                    {
                        "name": command["name"],
                        "purpose": command.get("purpose"),
                        "ok": True,
                        "returncode": 0,
                        "skipped": True,
                        "skip_reason": "no_current_forward_tracking_candidate_in_latest_profit_state",
                    }
                )
                continue
            source_route_for_forward_slice = load_json(args.source_route_state, default={})
            if not isinstance(source_route_for_forward_slice, dict):
                source_route_for_forward_slice = {}
            skipped_forward_ranks = _route_blocked_candidate_forward_skip_results(
                args,
                resolved_commands,
                source_route_for_forward_slice,
            )
            executable_forward_commands = (
                resolved_commands[:1] if skipped_forward_ranks else resolved_commands
            )
            post_tracker_profit_refresh = _profit_command(args, name="profit_admission_after_tracker")
            post_tracker_profit_required_s = (
                _required_remaining_s_for_command(args, post_tracker_profit_refresh) or 0.0
            )
            for resolved in executable_forward_commands:
                if _wall_runtime_exhausted(wall_started, wall_budget_s, min_remaining_s=10.0):
                    command_results.append(
                        _wall_runtime_exhausted_result(
                            name=str(resolved.get("name") or "candidate_forward_tracker_measurement"),
                            purpose=str(
                                resolved.get("purpose")
                                or "wall runtime budget exhausted before candidate-forward measurement"
                            ),
                            started=wall_started,
                            budget_s=wall_budget_s,
                        )
                    )
                    break
                deferred_for_profit_refresh = _defer_candidate_forward_if_profit_refresh_would_be_starved(
                    args,
                    resolved,
                    started=wall_started,
                    budget_s=wall_budget_s,
                    profit_refresh_required_s=post_tracker_profit_required_s,
                )
                if deferred_for_profit_refresh is not None:
                    command_results.append(_attach_command_metadata(deferred_for_profit_refresh, resolved))
                    break
                deferred_result = _defer_if_insufficient_wall_runtime(
                    args,
                    resolved,
                    started=wall_started,
                    budget_s=wall_budget_s,
                )
                if deferred_result is not None:
                    command_results.append(_attach_command_metadata(deferred_result, resolved))
                    break
                result = _attach_command_metadata(
                    _run_command(
                        resolved["name"],
                        resolved["purpose"],
                        resolved["argv"],
                        acceptable_returncodes=tuple(resolved.get("acceptable_returncodes") or (0,)),
                        timeout_s=_wall_runtime_command_timeout(
                            wall_started,
                            wall_budget_s,
                            requested_s=_requested_timeout_for_command(args, resolved),
                            reserve_s=5.0,
                        ),
                    ),
                    resolved,
                )
                command_results.append(result)
                if not result.get("ok"):
                    break
            command_results.extend(skipped_forward_ranks)
            if any(row.get("wall_runtime_budget_exhausted") for row in command_results):
                break
            if command_results and not command_results[-1].get("ok"):
                break
            continue
        deferred_result = _defer_if_insufficient_wall_runtime(
            args,
            command,
            started=wall_started,
            budget_s=wall_budget_s,
        )
        if deferred_result is not None:
            command_results.append(_attach_command_metadata(deferred_result, command))
            continue
        result = _attach_command_metadata(
            _run_command(
                command["name"],
                command["purpose"],
                command["argv"],
                acceptable_returncodes=tuple(command.get("acceptable_returncodes") or (0,)),
                timeout_s=_wall_runtime_command_timeout(
                    wall_started,
                    wall_budget_s,
                    requested_s=_requested_timeout_for_command(args, command),
                    reserve_s=5.0,
                ),
            ),
            command,
        )
        command_results.append(result)
        if str(command.get("name") or "") == "source_route_probe":
            source_route_skip = _source_route_fresh_probe_blocked_heavy_work_result(args)
            if source_route_skip is not None:
                command_results.append(source_route_skip)
                break
        if not result.get("ok") and command["name"] != "paper_live_tracker_measurement":
            break

    source_route_probe_already_ok = any(
        str(row.get("name") or "") == "source_route_probe" and bool(row.get("ok"))
        for row in command_results
        if isinstance(row, dict)
    )
    for command in _final_source_truth_refresh_commands(args):
        if str(command.get("name") or "") == "source_route_probe" and source_route_probe_already_ok:
            continue
        if _wall_runtime_exhausted(wall_started, wall_budget_s, min_remaining_s=10.0):
            command_results.append(
                _wall_runtime_deferred_result(
                    name=str(command.get("name") or "final_source_truth_refresh"),
                    purpose=str(command.get("purpose") or "wall runtime budget exhausted before final source truth refresh"),
                    started=wall_started,
                    budget_s=wall_budget_s,
                    required_remaining_s=10.0,
                )
            )
            break
        if (
            str(command.get("name") or "") == "profit_admission_after_source_route"
            and _source_route_blocks_heavy_work(args)
        ):
            source_route_skip = _source_route_fresh_probe_blocked_heavy_work_result(
                args,
                name="profit_admission_after_source_route_blocked_by_fresh_source_route",
                purpose="skip final profit admission refresh while the fresh source-route probe still blocks measurement",
            )
            if source_route_skip is not None:
                command_results.append(source_route_skip)
                continue
        requested_timeout_s = min(float(args.command_timeout_s), 180.0)
        if str(command.get("name") or "") == "profit_admission_after_source_route":
            requested_timeout_s = min(requested_timeout_s, _requested_timeout_for_command(args, command))
            final_profit_reserve_s = 2.0
            if _wall_runtime_remaining_s(wall_started, wall_budget_s, reserve_s=0.0) < (
                requested_timeout_s + final_profit_reserve_s
            ):
                command_results.append(
                    _wall_runtime_deferred_result(
                        name=str(command.get("name") or "profit_admission_after_source_route"),
                        purpose=str(command.get("purpose") or "defer final profit admission refresh"),
                        started=wall_started,
                        budget_s=wall_budget_s,
                        required_remaining_s=requested_timeout_s + final_profit_reserve_s,
                    )
                )
                continue
        result = _attach_command_metadata(
            _run_command(
                command["name"],
                command["purpose"],
                command["argv"],
                acceptable_returncodes=tuple(command.get("acceptable_returncodes") or (0,)),
                timeout_s=_wall_runtime_command_timeout(
                    wall_started,
                    wall_budget_s,
                    requested_s=requested_timeout_s,
                    reserve_s=2.0 if str(command.get("name") or "") == "profit_admission_after_source_route" else 5.0,
                ),
            ),
            command,
        )
        command_results.append(result)

    post_audit = _run_audit(
        append_feedback=not args.no_append_post_audit_feedback,
        timeout_s=_wall_runtime_command_timeout(
            wall_started,
            wall_budget_s,
            requested_s=float(args.command_timeout_s),
            reserve_s=1.0,
            min_timeout_s=1.0,
        ),
    )
    source_route = load_json(args.source_route_state, default={})
    if not isinstance(source_route, dict):
        source_route = {}
    live_ready_unlock = getattr(args, "_live_ready_unlock_context", {})
    if not isinstance(live_ready_unlock, dict):
        live_ready_unlock = {}
    strategy_direction = load_json(args.strategy_direction_state, default={})
    if not isinstance(strategy_direction, dict):
        strategy_direction = {}
    relaxed_copyability_report = load_json(args.relaxed_copyability_report_state, default={})
    if not isinstance(relaxed_copyability_report, dict):
        relaxed_copyability_report = {}
    live_readiness = _live_readiness_report(args, post_audit)
    limit_pressure = _limit_pressure_report(args, command_results=command_results, live_readiness=live_readiness)
    extra_backlog_actions: list[dict[str, Any]] = []
    if isinstance(limit_pressure, dict):
        extra_backlog_actions.extend(row for row in (limit_pressure.get("backlog_actions") or []) if isinstance(row, dict))
    extra_backlog_actions.extend(_strategy_direction_backlog_actions(strategy_direction))
    extra_backlog_actions.extend(_source_route_backlog_actions(source_route, args))
    extra_backlog_actions.extend(_candidate_forward_tactic_backlog_actions(live_readiness))
    backlog_items = _write_backlog(
        post_audit,
        args,
        extra_actions=extra_backlog_actions,
    )
    status_before_green_gate = _status_from(
        post_audit,
        command_results,
        backlog_items,
        limit_pressure=limit_pressure,
        strategy_direction=strategy_direction,
    )
    green_semantics = _green_semantics_report(
        live_readiness,
        source_route,
        strategy_direction=strategy_direction,
        workflow_status=status_before_green_gate,
    )
    status = _apply_global_green_gate(status_before_green_gate, green_semantics)
    progress_action = _attach_current_poll_progress_diagnostics(
        _compact_progress_action(
            status=status,
            backlog_items=backlog_items,
            command_results=command_results,
            source_route=source_route,
        )
    )
    post_checks = post_audit.get("checks") if isinstance(post_audit.get("checks"), dict) else {}
    copy_efficiency_check = (
        post_checks.get("live_tracker_copy_efficiency")
        if isinstance(post_checks.get("live_tracker_copy_efficiency"), dict)
        else {}
    )
    live_admission_check = (
        post_checks.get("live_admission_truth")
        if isinstance(post_checks.get("live_admission_truth"), dict)
        else {}
    )
    active_hotlane_check = (
        post_checks.get("active_hotlane_scope")
        if isinstance(post_checks.get("active_hotlane_scope"), dict)
        else {}
    )
    active_forward_probe_check = (
        post_checks.get("active_forward_candidate_probe")
        if isinstance(post_checks.get("active_forward_candidate_probe"), dict)
        else {}
    )
    adaptive_check = (
        post_checks.get("adaptive_wallet_derived_bot")
        if isinstance(post_checks.get("adaptive_wallet_derived_bot"), dict)
        else {}
    )
    important_post_check_names = (
        "active_hotlane_tracking_evidence",
        "active_hotlane_paper_copy_contract",
        "active_hotlane_all_order_exact_copy",
        "active_hotlane_all_order_tactic_replay",
        "active_hotlane_hot_path_adaptive",
        "active_hotlane_single_wallet_exact_copy",
        "adaptive_single_wallet_exact_copy",
        "adaptive_wallet_derived_bot",
        "active_forward_candidate_probe",
        "live_admission_truth",
        "active_hotlane_guard_log_retention",
        "hotlane_path_isolation",
            "green_by_removal_guard",
            "limit_pressure",
        )
    important_post_checks = {
        name: post_checks.get(name)
        for name in important_post_check_names
        if isinstance(post_checks.get(name), dict)
    }
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_autonomous_repair_state",
        "generated_at": utc_now_iso(),
        "mission_contract": mission,
        "mission_contract_check": mission_contract_check(),
        "operating_framework": {
            "document": "docs/WALLET_COPY_OPERATING_FRAMEWORK.md",
            "machine_contract": "src/wallet_copy/mission.py",
            "non_deviation_rule": "wallet-copy-only BTC-5m CopyIntent workflow unless explicit operator/source-of-truth update changes it",
        },
        "status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "deep_research": bool(args.deep_research),
        "pre_audit": {
            "learning_status": pre_audit.get("learning_status"),
            "summary": pre_audit.get("summary"),
            "feedback_loop_status": (pre_audit.get("feedback_loop") or {}).get("status")
            if isinstance(pre_audit.get("feedback_loop"), dict)
            else None,
        },
        "plan": [
            {
                "name": command.get("name"),
                "purpose": command.get("purpose"),
                "acceptable_returncodes": list(command.get("acceptable_returncodes") or (0,)),
                "runtime_budget": command.get("runtime_budget"),
                "argv": command.get("argv"),
            }
            for command in plan
        ],
        "command_results": command_results,
        "commands": [
            _compact_command_summary(row)
            for row in command_results
        ],
        "post_audit": {
            "learning_status": post_audit.get("learning_status"),
            "summary": post_audit.get("summary"),
            "source_counters": post_audit.get("source_counters")
            if isinstance(post_audit.get("source_counters"), dict)
            else {},
            "jsonl_counts": post_audit.get("jsonl_counts")
            if isinstance(post_audit.get("jsonl_counts"), dict)
            else {},
            "feedback_loop_status": (post_audit.get("feedback_loop") or {}).get("status")
            if isinstance(post_audit.get("feedback_loop"), dict)
            else None,
            "feedback_loop": post_audit.get("feedback_loop")
            if isinstance(post_audit.get("feedback_loop"), dict)
            else {},
            "checks": important_post_checks,
            "copy_efficiency": copy_efficiency_check,
            "live_admission_truth": live_admission_check,
            "active_hotlane_scope": active_hotlane_check,
            "active_hotlane_tracking_evidence": important_post_checks.get("active_hotlane_tracking_evidence") or {},
            "active_hotlane_paper_copy_contract": important_post_checks.get("active_hotlane_paper_copy_contract")
            or {},
            "active_hotlane_all_order_exact_copy": important_post_checks.get("active_hotlane_all_order_exact_copy") or {},
            "active_hotlane_all_order_tactic_replay": important_post_checks.get(
                "active_hotlane_all_order_tactic_replay"
            )
            or {},
            "active_hotlane_hot_path_adaptive": important_post_checks.get("active_hotlane_hot_path_adaptive") or {},
            "active_hotlane_single_wallet_exact_copy": important_post_checks.get("active_hotlane_single_wallet_exact_copy") or {},
            "adaptive_single_wallet_exact_copy": important_post_checks.get("adaptive_single_wallet_exact_copy") or {},
            "active_hotlane_guard_log_retention": important_post_checks.get("active_hotlane_guard_log_retention") or {},
            "hotlane_path_isolation": important_post_checks.get("hotlane_path_isolation") or {},
            "active_forward_candidate_probe": active_forward_probe_check,
            "adaptive_wallet_derived_bot": adaptive_check,
            "strategy_direction": {
                "state_path": args.strategy_direction_state,
                "decision": strategy_direction.get("decision") if isinstance(strategy_direction.get("decision"), dict) else {},
                "top_direction": (
                    strategy_direction.get("directions", [{}])[0]
                    if isinstance(strategy_direction.get("directions"), list)
                    and strategy_direction.get("directions")
                    and isinstance(strategy_direction.get("directions", [{}])[0], dict)
                    else {}
                ),
                "development_research_page": strategy_direction.get("development_research_page")
                if isinstance(strategy_direction.get("development_research_page"), dict)
                else {},
                "development_limit_review": strategy_direction.get("development_limit_review")
                if isinstance(strategy_direction.get("development_limit_review"), dict)
                else {},
                "development_program_review": strategy_direction.get("development_program_review")
                if isinstance(strategy_direction.get("development_program_review"), dict)
                else {},
            },
            "copy_efficiency_scope": {
                "tracker_scope_copy_efficiency_status": copy_efficiency_check.get("copy_efficiency_status"),
                "tracker_scope_required_buy_copy_events": copy_efficiency_check.get("required_buy_copy_events"),
                "tracker_scope_clob_filled_buy_copy_events": copy_efficiency_check.get("clob_filled_buy_copy_events"),
                "active_forward_probe_status": active_forward_probe_check.get("status"),
                "active_forward_probe_blockers": active_forward_probe_check.get("blockers") or [],
                "active_forward_probe_required_buy_copy_events": active_forward_probe_check.get("required_buy_copy_events"),
                "active_forward_probe_clob_filled_buy_copy_events": active_forward_probe_check.get("clob_filled_buy_copy_events"),
                "active_forward_probe_selected": active_forward_probe_check.get("selected_probe") or {},
                "candidate_policy_copy_efficiency_status": live_admission_check.get("live_tracker_truth_status"),
                "candidate_policy_copy_efficiency_blockers": live_admission_check.get("live_tracker_truth_blockers") or [],
                "candidate_forward_copy_efficiency_status": live_admission_check.get("candidate_forward_truth_status"),
                "candidate_forward_copy_efficiency_blockers": live_admission_check.get("candidate_forward_truth_blockers") or [],
                "effective_live_tracker_truth_status": live_admission_check.get("effective_live_tracker_truth_status"),
                "live_tracker_truth_source": live_admission_check.get("live_tracker_truth_source"),
                "note": "tracker-scope PASS is not candidate-policy live admission truth",
            },
        },
        "limit_pressure": limit_pressure,
        "live_ready_unlock": live_ready_unlock,
        "green_semantics": green_semantics,
        "relaxed_copyability": {
            "state_path": args.relaxed_copyability_report_state,
            "status": relaxed_copyability_report.get("status"),
            "role": relaxed_copyability_report.get("role"),
            "paper_only": relaxed_copyability_report.get("paper_only"),
            "live_orders_allowed": relaxed_copyability_report.get("live_orders_allowed"),
            "profiles": relaxed_copyability_report.get("profiles") or [],
        },
        "progress_action": progress_action,
        "source_route": {
            "state_path": args.source_route_state,
            "status": source_route.get("status"),
            "generated_at": source_route.get("generated_at"),
            "endpoint_statuses": _source_route_endpoint_statuses(source_route),
            "next_action": source_route.get("next_action"),
            "paper_only": source_route.get("paper_only"),
            "live_orders_allowed": source_route.get("live_orders_allowed"),
            **_source_route_diagnostic_fields(source_route),
        },
        "live_readiness_report": live_readiness,
        "development_research_page": strategy_direction.get("development_research_page")
        if isinstance(strategy_direction.get("development_research_page"), dict)
        else {},
        "development_limit_review": strategy_direction.get("development_limit_review")
        if isinstance(strategy_direction.get("development_limit_review"), dict)
        else {},
        "development_program_review": strategy_direction.get("development_program_review")
        if isinstance(strategy_direction.get("development_program_review"), dict)
        else {},
        "backlog_items_written": backlog_items,
        "backlog_state": args.backlog_state,
    }
    atomic_write_json(args.state, payload, compact=True)
    live_blockers = live_readiness.get("blockers") if isinstance(live_readiness, dict) else []
    post_summary = (payload.get("post_audit") or {}).get("summary") if isinstance(payload.get("post_audit"), dict) else {}
    strategy_decision = (
        (payload.get("post_audit") or {}).get("strategy_direction") or {}
        if isinstance(payload.get("post_audit"), dict)
        else {}
    )
    if isinstance(strategy_decision, dict):
        strategy_decision = strategy_decision.get("decision") if isinstance(strategy_decision.get("decision"), dict) else {}
    print(
        json.dumps(
            {
                "state": args.state,
                "status": status,
                "status_before_green_gate": status_before_green_gate,
                "paper_only": True,
                "live_orders_allowed": False,
                "commands": [
                    _compact_command_summary(row)
                    for row in command_results
                ],
                "limit_pressure": limit_pressure,
                "live_ready_unlock": {
                    "active": live_ready_unlock.get("active") if isinstance(live_ready_unlock, dict) else False,
                    "candidate_id": live_ready_unlock.get("candidate_id") if isinstance(live_ready_unlock, dict) else None,
                    "source_wallet": live_ready_unlock.get("source_wallet") if isinstance(live_ready_unlock, dict) else None,
                    "effective_candidate_forward_probe_ranks": live_ready_unlock.get("effective_candidate_forward_probe_ranks")
                    if isinstance(live_ready_unlock, dict)
                    else None,
                    "deferred_candidate_forward_probe_ranks": live_ready_unlock.get("deferred_candidate_forward_probe_ranks")
                    if isinstance(live_ready_unlock, dict)
                    else None,
                },
                "progress_action": progress_action,
                "source_route_status": (payload.get("source_route") or {}).get("status")
                if isinstance(payload.get("source_route"), dict)
                else None,
                "green_semantics": {
                    "copy_trading_green": green_semantics.get("copy_trading_green"),
                    "bot_green": green_semantics.get("bot_green"),
                    "global_green": green_semantics.get("global_green"),
                    "copy_blockers": list(green_semantics.get("copy_blockers") or [])[:20],
                    "bot_blockers": list(green_semantics.get("bot_blockers") or [])[:20],
                },
                "live_readiness": {
                    "status": live_readiness.get("status") if isinstance(live_readiness, dict) else None,
                    "live_ready": live_readiness.get("live_ready") if isinstance(live_readiness, dict) else False,
                    "profitability_proven": live_readiness.get("profitability_proven")
                    if isinstance(live_readiness, dict)
                    else False,
                    "blockers": list(live_blockers or [])[:30],
                    "blocker_count": len(live_blockers or []),
                },
                "relaxed_copyability": {
                    "status": relaxed_copyability_report.get("status"),
                    "profiles": relaxed_copyability_report.get("profiles") or [],
                },
                "post_audit_summary": post_summary,
                "strategy_direction": {
                    "status": strategy_decision.get("status") if isinstance(strategy_decision, dict) else None,
                    "recommended_now": strategy_decision.get("recommended_now")
                    if isinstance(strategy_decision, dict)
                    else None,
                    "target_live_architecture": strategy_decision.get("target_live_architecture")
                    if isinstance(strategy_decision, dict)
                    else None,
                },
                "development_research_page": {
                    "page_id": (payload.get("development_research_page") or {}).get("page_id")
                    if isinstance(payload.get("development_research_page"), dict)
                    else None,
                    "status": (payload.get("development_research_page") or {}).get("status")
                    if isinstance(payload.get("development_research_page"), dict)
                    else None,
                    "next_change_action": (
                        ((payload.get("development_research_page") or {}).get("change_policy") or {}).get(
                            "next_change_action"
                        )
                        if isinstance(payload.get("development_research_page"), dict)
                        and isinstance((payload.get("development_research_page") or {}).get("change_policy"), dict)
                        else None
                    ),
                },
                "development_limit_review": {
                    "status": (payload.get("development_limit_review") or {}).get("status")
                    if isinstance(payload.get("development_limit_review"), dict)
                    else None,
                    "limit_hit": (payload.get("development_limit_review") or {}).get("limit_hit")
                    if isinstance(payload.get("development_limit_review"), dict)
                    else False,
                    "next_change_action": (payload.get("development_limit_review") or {}).get("next_change_action")
                    if isinstance(payload.get("development_limit_review"), dict)
                    else None,
                    "limit_hit_lanes": (payload.get("development_limit_review") or {}).get("limit_hit_lanes")
                    if isinstance(payload.get("development_limit_review"), dict)
                    else [],
                },
                "development_program_review": {
                    "status": (payload.get("development_program_review") or {}).get("status")
                    if isinstance(payload.get("development_program_review"), dict)
                    else None,
                    "full_rethink_required": (payload.get("development_program_review") or {}).get(
                        "full_rethink_required"
                    )
                    if isinstance(payload.get("development_program_review"), dict)
                    else False,
                    "next_major_change_action": (payload.get("development_program_review") or {}).get(
                        "next_major_change_action"
                    )
                    if isinstance(payload.get("development_program_review"), dict)
                    else None,
                    "strategic_traps": (payload.get("development_program_review") or {}).get("strategic_traps")
                    if isinstance(payload.get("development_program_review"), dict)
                    else [],
                    "stop_doing": (payload.get("development_program_review") or {}).get("stop_doing")
                    if isinstance(payload.get("development_program_review"), dict)
                    else [],
                },
                "backlog_items_written": len(backlog_items),
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 2 if status == "BUG_SUSPECT" else 0


if __name__ == "__main__":
    raise SystemExit(main())
