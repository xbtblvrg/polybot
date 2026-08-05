#!/usr/bin/env python3
"""Run strict active-hot-lane tracker ticks and adaptive measurement immediately.

This is a paper-only latency harness. It rotates through the active hot-lane
wallet registry in tiny slices, runs the strict CLOB-backed tracker for one
slice, then immediately runs the adaptive wallet-derived bot against the same
isolated log. The goal is to measure whether consensus can be observed while
the wallet events are still inside the <=10s live-copy evidence window.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter, deque
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.ingest import WalletHistoryClient  # noqa: E402
from src.wallet_copy.fill_model import FillModelConfig, estimate_executable_fill  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.models import CopyIntent, SizingPolicy, WalletEvent, WalletSpec, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.strategy import CopyPolicy, event_to_intent  # noqa: E402
from src.wallet_copy.source_route import (  # noqa: E402
    DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE,
    source_route_allows_live_execution,
    source_route_allows_measurement,
    source_route_probe_progress_blocker,
)


PYTHON = "python3"
MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS = 10
DIRECT_DATA_API_BASE_URL = "https://data-api.polymarket.com"
DIRECT_CLOB_BASE_URL = "https://clob.polymarket.com"
SOURCE_BASE_OVERRIDE_ENV_VARS = (
    "POLYMARKET_DATA_API_BASE_URL",
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_CLOB_API_BASE_URL",
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
)


def _bridge_direct_source_env() -> dict[str, str]:
    env = dict(os.environ)
    for key in SOURCE_BASE_OVERRIDE_ENV_VARS:
        env.pop(key, None)
    return env


@contextmanager
def _without_bridge_source_overrides():
    missing = object()
    prior = {key: os.environ.get(key, missing) for key in SOURCE_BASE_OVERRIDE_ENV_VARS}
    for key in SOURCE_BASE_OVERRIDE_ENV_VARS:
        os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in prior.items():
            if value is missing:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)


def _admission_allows_source_base_overrides(args: argparse.Namespace) -> bool:
    """Use approved recovered source routes for admission-grade measurement.

    The tracker CLI defaults to clearing Polymarket base overrides in admission
    mode so degraded paper routes cannot silently become live proof. When the
    current source-route state is explicitly live-admissible, keeping those
    measured overrides is the safer behavior: it makes the paper/live proof use
    the same approved source route that the guard is allowed to trust.
    """

    route = load_json(getattr(args, "source_route_state", ""), default={})
    return bool(isinstance(route, dict) and source_route_allows_live_execution(route))


def _source_route_uses_local_relay(args: argparse.Namespace) -> bool:
    route = load_json(getattr(args, "source_route_state", ""), default={})
    if not isinstance(route, dict):
        return False
    overrides = route.get("source_base_overrides")
    if not isinstance(overrides, dict):
        return False
    for override in overrides.values():
        if not isinstance(override, dict) or not override.get("configured"):
            continue
        host = str(override.get("base_url_host") or "")
        if not host:
            try:
                host = urlsplit(str(override.get("base_url") or "")).netloc
            except Exception:
                host = ""
        host = host.lower()
        if host.startswith("127.0.0.1") or host.startswith("localhost"):
            return True
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument("--tracker-state", default="data/research/wallet_copy_active_hotlane_live_tracking_state.json")
    parser.add_argument(
        "--tracker-event-log",
        default="data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
    )
    parser.add_argument("--paper-state", default="data/research/wallet_copy_active_hotlane_paper_state.json")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_active_hotlane_paper_events.jsonl")
    parser.add_argument(
        "--tracker-time-replay-paper-state",
        default="data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--all-order-exact-copy-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
    )
    parser.add_argument(
        "--all-order-exact-copy-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy.jsonl",
    )
    parser.add_argument(
        "--all-order-tactic-replay-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
    )
    parser.add_argument(
        "--all-order-tactic-replay-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy_aggressive_tactic_replay.jsonl",
    )
    parser.add_argument("--adaptive-state", default="data/research/wallet_copy_adaptive_bot_state.json")
    parser.add_argument("--adaptive-paper-state", default="data/research/wallet_copy_adaptive_bot_paper_state.json")
    parser.add_argument(
        "--adaptive-paper-event-log",
        default="data/research/wallet_copy_adaptive_bot_paper_events.jsonl",
    )
    parser.add_argument(
        "--adaptive-single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--adaptive-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--adaptive-tracker-time-replay-paper-state",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--adaptive-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument("--profit-policy-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--strategy-direction-state", default="data/research/wallet_copy_strategy_direction_state.json")
    parser.add_argument("--seed-history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument("--active-hotlane-source-registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--output", default="data/research/wallet_copy_hotlane_tick_state.json")
    parser.add_argument("--source-route-state", default="data/research/wallet_copy_source_route_state.json")
    parser.add_argument(
        "--autonomous-repair-command-progress-state",
        default=DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE,
    )
    parser.add_argument("--ticks", type=int, default=4)
    parser.add_argument("--wallets-per-tick", type=int, default=4)
    parser.add_argument("--parallel-wallet-fetches", type=int, default=2)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--data-api-timeout-s", type=float, default=1.0)
    parser.add_argument(
        "--data-api-trade-query-keys",
        default="user,proxyWallet",
        help=(
            "Comma-separated Data API trade query keys for this latency harness. "
            "The default keeps both user and proxyWallet routes visible so source-route "
            "degradation cannot masquerade as no wallet activity."
        ),
    )
    parser.add_argument(
        "--parallel-data-api-sources",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Fetch each wallet's Data API trade/activity sources concurrently in the latency harness. "
            "This preserves source coverage while reducing the chance that current-poll rows age past "
            "the <=10s copyability cap."
        ),
    )
    parser.add_argument("--max-poll-runtime-s", type=float, default=8.0)
    parser.add_argument("--max-runtime-s", type=float, default=15.0)
    parser.add_argument(
        "--tracker-iterations",
        type=int,
        default=6,
        help=(
            "Live-tracker polls per hot-lane tick. Multiple short polls reduce the "
            "chance that current wallet events age past the <=10s copyability cap "
            "between guard cycles."
        ),
    )
    parser.add_argument(
        "--tracker-poll-interval-s",
        type=float,
        default=0.5,
        help="Seconds between live-tracker polls inside one hot-lane tick.",
    )
    parser.add_argument("--poll-gap-s", type=float, default=0.2)
    parser.add_argument("--clob-timeout-s", type=float, default=0.8)
    parser.add_argument("--gamma-timeout-s", type=float, default=0.8)
    parser.add_argument("--market-ws-jsonl", default="data/research/clob_market_ws_events.jsonl")
    parser.add_argument("--paper-retain-orders", type=int, default=1_000)
    parser.add_argument("--paper-retain-lifecycle-events", type=int, default=3_000)
    parser.add_argument("--paper-retain-dedupe-ids", type=int, default=250_000)
    parser.add_argument("--adaptive-max-event-log-rows", type=int, default=3000)
    parser.add_argument("--adaptive-max-observation-age-s", type=float, default=30.0)
    parser.add_argument("--adaptive-max-observed-event-age-s", type=float, default=10.0)
    parser.add_argument("--adaptive-max-signal-cluster-age-s", type=float, default=8.0)
    parser.add_argument("--command-timeout-s", type=float, default=60.0)
    parser.add_argument("--stop-on-pass", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--cohort-probe-on-single-wallet", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--exact-cohort-probe", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--cohort-probe-max-wallets", type=int, default=12)
    parser.add_argument("--cohort-probe-limit", type=int, default=12)
    parser.add_argument("--cohort-probe-pages", type=int, default=2)
    parser.add_argument(
        "--force-development-bridge-probe",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Run the development-program current-poll inventory bridge cohort before the base slice. "
            "Use this for the explicit run_current_poll_inventory_bridge_burnin_and_attach_clob_truth action."
        ),
    )
    parser.add_argument(
        "--auto-refresh-stale-bridge-hotlane",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Regenerate the active-hotlane bridge cohorts when the profit engine target inventory candidate "
            "changes before a current-poll bridge burn-in."
        ),
    )
    parser.add_argument(
        "--bridge-live-feed-jsonl",
        default="data/research/wallet_copy_live_guard_wallet_events.jsonl",
        help=(
            "Evidence-only RTDS/live-feed wallet event log used to diagnose the current-poll bridge when "
            "Data API rows are unavailable. This never mutates paper/live state."
        ),
    )
    parser.add_argument("--bridge-live-feed-tail-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--bridge-live-feed-scan-limit", type=int, default=100_000)
    parser.add_argument("--bridge-live-feed-max-event-age-s", type=float, default=900.0)
    parser.add_argument(
        "--bridge-window-indexed-data-api",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Use the live pipeline WalletHistoryClient/Data-API event source as the bridge burn-in truth. "
            "This is paper-only and supersedes current-poll route repair for development bridge evidence."
        ),
    )
    parser.add_argument("--bridge-window-indexed-limit", type=int, default=50)
    parser.add_argument("--bridge-window-indexed-pages", type=int, default=1)
    parser.add_argument("--bridge-window-indexed-max-event-age-s", type=float, default=900.0)
    parser.add_argument("--bridge-window-indexed-data-api-timeout-s", type=float, default=2.0)
    parser.add_argument("--bridge-window-indexed-data-api-retries", type=int, default=1)
    parser.add_argument("--bridge-window-indexed-max-book-slippage-bps", type=float, default=150.0)
    parser.add_argument(
        "--bridge-live-feed-max-state-age-s",
        type=float,
        default=3.0,
        help="Freshness bar for a live-feed inventory window state to be considered live-admissible evidence.",
    )
    parser.add_argument(
        "--bridge-clob-book-jsonl",
        default="data/research/clob_book_snapshots_live_feed_bridge.jsonl",
        help=(
            "CLOB book snapshot JSONL used only to flag whether live-feed bridge assets have book truth. "
            "The hotlane tick can append fresh read-only snapshots for bridge assets before summarizing."
        ),
    )
    parser.add_argument("--bridge-clob-book-tail-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--bridge-clob-book-max-age-s", type=float, default=900.0)
    parser.add_argument(
        "--bridge-clob-book-auto-snapshot",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Append a bounded read-only CLOB /book snapshot for live-feed bridge token ids before burn-in summary.",
    )
    parser.add_argument("--bridge-clob-book-auto-snapshot-duration-s", type=float, default=2.0)
    parser.add_argument("--bridge-clob-book-auto-snapshot-interval-s", type=float, default=0.5)
    parser.add_argument("--bridge-clob-book-auto-snapshot-max-assets", type=int, default=20)
    return parser.parse_args()


def _run_command(
    *,
    name: str,
    argv: list[str],
    acceptable_returncodes: tuple[int, ...] = (0,),
    timeout_s: float = 60.0,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    started = time.time()
    try:
        result = subprocess.run(
            argv,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            env=env,
            timeout=max(1.0, float(timeout_s)),
        )
        returncode = int(result.returncode)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
    except subprocess.TimeoutExpired as exc:
        returncode = 124
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stderr = (stderr + "\n" if stderr else "") + f"timeout after {timeout_s}s"
    return {
        "name": name,
        "argv": argv,
        "returncode": returncode,
        "acceptable_returncodes": list(acceptable_returncodes),
        "ok": returncode in acceptable_returncodes,
        "duration_s": round(time.time() - started, 3),
        "stdout_tail": stdout[-2000:],
        "stderr_tail": stderr[-2000:],
    }


def _cleanup_dead_global_tracker_lock(
    path: str | Path = "data/research/wallet_copy_live_tracker_global.lock",
) -> dict[str, Any]:
    lock_path = Path(path)
    if not lock_path.is_absolute():
        lock_path = ROOT / lock_path
    if not lock_path.exists():
        return {"status": "NO_LOCK", "lock_path": str(lock_path)}
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"status": "UNREADABLE_LOCK", "lock_path": str(lock_path), "error": str(exc)}
    pid = payload.get("pid") if isinstance(payload, dict) else None
    if pid:
        try:
            os.kill(int(pid), 0)
            return {"status": "OWNER_ALIVE", "lock_path": str(lock_path), "pid": int(pid)}
        except ProcessLookupError:
            pass
        except PermissionError:
            return {"status": "OWNER_ALIVE_PERMISSION_UNKNOWN", "lock_path": str(lock_path), "pid": int(pid)}
        except (TypeError, ValueError):
            return {"status": "INVALID_PID", "lock_path": str(lock_path), "pid": pid}
    lock_path.unlink(missing_ok=True)
    return {
        "status": "REMOVED_DEAD_OWNER_LOCK",
        "lock_path": str(lock_path),
        "pid": pid,
        "state": payload.get("state") if isinstance(payload, dict) else None,
    }


def _remaining_wall_runtime_s(started: float, max_runtime_s: float) -> float:
    return max(0.0, float(max_runtime_s) - (time.time() - float(started)))


def _bounded_child_timeout_s(*, started: float, max_runtime_s: float, command_timeout_s: float) -> float:
    remaining_s = _remaining_wall_runtime_s(started, max_runtime_s)
    if remaining_s <= 0.0:
        return 0.0
    return max(1.0, min(float(command_timeout_s), remaining_s))


def _path_with_stem_suffix(path: str | Path, suffix: str) -> str:
    target = Path(path)
    return str(target.with_name(f"{target.stem}{suffix}{target.suffix}"))


def _cohort_probe_args(args: argparse.Namespace) -> argparse.Namespace:
    """Use isolated cohort-probe artifacts so the pinned slice cannot mask it."""

    probe_args = argparse.Namespace(**vars(args))
    for name in (
        "tracker_state",
        "tracker_event_log",
        "paper_state",
        "paper_event_log",
        "tracker_time_replay_paper_state",
        "tracker_time_replay_paper_event_log",
        "single_wallet_exact_copy_paper_state",
        "single_wallet_exact_copy_paper_event_log",
        "all_order_exact_copy_paper_state",
        "all_order_exact_copy_paper_event_log",
        "all_order_tactic_replay_paper_state",
        "all_order_tactic_replay_paper_event_log",
        "adaptive_state",
        "adaptive_paper_state",
        "adaptive_paper_event_log",
        "adaptive_single_wallet_exact_copy_paper_state",
        "adaptive_single_wallet_exact_copy_paper_event_log",
        "adaptive_tracker_time_replay_paper_state",
        "adaptive_tracker_time_replay_paper_event_log",
    ):
        value = getattr(probe_args, name, None)
        if value:
            setattr(probe_args, name, _path_with_stem_suffix(str(value), "_cohort_probe"))
    return probe_args


def build_tracker_argv(
    args: argparse.Namespace,
    *,
    max_wallets_override: int | None = None,
    parallel_wallet_fetches_override: int | None = None,
    limit_override: int | None = None,
    pages_override: int | None = None,
    wallet_addresses_override: list[str] | None = None,
    max_runtime_override_s: float | None = None,
    force_direct_source_route: bool = False,
) -> list[str]:
    targeted_current_poll_probe = bool(wallet_addresses_override)
    max_wallets = int(args.wallets_per_tick) if max_wallets_override is None else int(max_wallets_override)
    parallel_wallet_fetches = (
        int(args.parallel_wallet_fetches)
        if parallel_wallet_fetches_override is None
        else int(parallel_wallet_fetches_override)
    )
    if _source_route_uses_local_relay(args):
        parallel_wallet_fetches = min(parallel_wallet_fetches, 1)
    limit = int(args.limit) if limit_override is None else int(limit_override)
    pages = int(args.pages) if pages_override is None else int(pages_override)
    argv = [
        PYTHON,
        "scripts/run_wallet_live_tracker.py",
        "--registry",
        str(args.registry),
        "--state",
        str(args.tracker_state),
        "--event-log",
        str(args.tracker_event_log),
        "--paper-state",
        str(args.paper_state),
        "--paper-event-log",
        str(args.paper_event_log),
        "--tracker-time-replay-paper-state",
        str(args.tracker_time_replay_paper_state),
        "--tracker-time-replay-paper-event-log",
        str(args.tracker_time_replay_paper_event_log),
        "--single-wallet-exact-copy-paper-state",
        str(args.single_wallet_exact_copy_paper_state),
        "--single-wallet-exact-copy-paper-event-log",
        str(args.single_wallet_exact_copy_paper_event_log),
        "--all-order-exact-copy-paper-state",
        str(
            getattr(
                args,
                "all_order_exact_copy_paper_state",
                "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
            )
        ),
        "--all-order-exact-copy-paper-event-log",
        str(
            getattr(
                args,
                "all_order_exact_copy_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy.jsonl",
            )
        ),
        "--all-order-tactic-replay-paper-state",
        str(
            getattr(
                args,
                "all_order_tactic_replay_paper_state",
                "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
            )
        ),
        "--all-order-tactic-replay-paper-event-log",
        str(
            getattr(
                args,
                "all_order_tactic_replay_paper_event_log",
                "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy_aggressive_tactic_replay.jsonl",
            )
        ),
        "--profit-policy-state",
        str(args.profit_policy_state),
        "--strategy-direction-state",
        str(getattr(args, "strategy_direction_state", "data/research/wallet_copy_strategy_direction_state.json")),
        "--seed-before-poll",
        "--seed-history-state",
        str(args.seed_history_state),
        "--paper-retain-orders",
        str(max(1, int(getattr(args, "paper_retain_orders", 1_000)))),
        "--paper-retain-lifecycle-events",
        str(max(1, int(getattr(args, "paper_retain_lifecycle_events", 3_000)))),
        "--paper-retain-dedupe-ids",
        str(max(1, int(getattr(args, "paper_retain_dedupe_ids", 250_000)))),
        "--limit",
        str(max(1, limit)),
        "--pages",
        str(max(1, pages)),
        "--data-api-timeout-s",
        str(float(args.data_api_timeout_s)),
        "--data-api-trade-query-keys",
        str(getattr(args, "data_api_trade_query_keys", "user,proxyWallet") or "user,proxyWallet"),
        "--max-poll-runtime-s",
        str(float(args.max_poll_runtime_s)),
        "--iterations",
        "1" if targeted_current_poll_probe else str(max(1, int(getattr(args, "tracker_iterations", 6)))),
        "--poll-interval-s",
        "0.0" if targeted_current_poll_probe else str(max(0.0, float(getattr(args, "tracker_poll_interval_s", 0.5)))),
        "--max-runtime-s",
        str(float(args.max_runtime_s if max_runtime_override_s is None else max_runtime_override_s)),
        "--enable-clob-books",
        "--admission-mode",
        "--strict-mirror-coverage",
        "--track-blocked-profit-policy",
        "--profit-policy-candidate-only",
        "--no-use-profit-search-scope",
        "--max-wallets",
        str(max(1, max_wallets)),
        "--parallel-wallet-fetches",
        str(max(1, parallel_wallet_fetches)),
        "--clob-timeout-s",
        str(float(args.clob_timeout_s)),
        "--gamma-timeout-s",
        str(float(args.gamma_timeout_s)),
        "--market-ws-jsonl",
        str(getattr(args, "market_ws_jsonl", "data/research/clob_market_ws_events.jsonl")),
        "--enable-onchain-receipts",
        "--onchain-timeout-s",
        str(min(float(getattr(args, "onchain_timeout_s", 8.0)), 1.0)),
    ]
    if bool(getattr(args, "parallel_data_api_sources", True)):
        argv.append("--parallel-data-api-sources")
    if not force_direct_source_route and _admission_allows_source_base_overrides(args):
        argv.append("--allow-source-base-overrides-in-admission")
    for address in wallet_addresses_override or []:
        text = str(address or "").strip()
        if text:
            argv.extend(["--wallet-address", text])
    return argv


def build_adaptive_argv(args: argparse.Namespace, *, min_move_generated_at_ts: float | None = None) -> list[str]:
    argv = [
        PYTHON,
        "scripts/run_wallet_copy_adaptive_bot.py",
        "--live-tracking-state",
        str(args.tracker_state),
        "--live-tracking-event-log",
        str(args.tracker_event_log),
        "--output",
        str(args.adaptive_state),
        "--paper-state",
        str(args.adaptive_paper_state),
        "--paper-event-log",
        str(args.adaptive_paper_event_log),
        "--single-wallet-exact-copy-paper-state",
        str(args.adaptive_single_wallet_exact_copy_paper_state),
        "--single-wallet-exact-copy-paper-event-log",
        str(args.adaptive_single_wallet_exact_copy_paper_event_log),
        "--tracker-time-replay-paper-state",
        str(args.adaptive_tracker_time_replay_paper_state),
        "--tracker-time-replay-paper-event-log",
        str(args.adaptive_tracker_time_replay_paper_event_log),
        "--max-event-log-rows",
        str(int(args.adaptive_max_event_log_rows)),
        "--max-observation-age-s",
        str(float(args.adaptive_max_observation_age_s)),
        "--max-observed-event-age-s",
        str(float(args.adaptive_max_observed_event_age_s)),
        "--max-signal-cluster-age-s",
        str(float(args.adaptive_max_signal_cluster_age_s)),
    ]
    if min_move_generated_at_ts is not None:
        argv.extend(["--min-move-generated-at-ts", str(float(min_move_generated_at_ts))])
    return argv


def _adaptive_snapshot(path: str | Path) -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    freshness = summary.get("freshness_diagnostics") if isinstance(summary.get("freshness_diagnostics"), dict) else {}
    tracker_time_replay = (
        payload.get("tracker_time_replay") if isinstance(payload.get("tracker_time_replay"), dict) else {}
    )
    tracker_time_replay_summary = (
        tracker_time_replay.get("summary") if isinstance(tracker_time_replay.get("summary"), dict) else {}
    )
    single_wallet_exact = (
        payload.get("single_wallet_exact_copy")
        if isinstance(payload.get("single_wallet_exact_copy"), dict)
        else {}
    )
    single_wallet_exact_summary = (
        single_wallet_exact.get("summary")
        if isinstance(single_wallet_exact.get("summary"), dict)
        else {}
    )
    return {
        "status": payload.get("status"),
        "blockers": payload.get("blockers") or [],
        "moves_seen": summary.get("moves_seen"),
        "eligible_moves": summary.get("eligible_moves"),
        "signals": summary.get("signals"),
        "pass_signals": summary.get("pass_signals"),
        "intents": summary.get("intents"),
        "paper_orders": summary.get("paper_orders"),
        "filled_orders": summary.get("filled_orders"),
        "rejected_orders": summary.get("rejected_orders"),
        "tracker_time_eligible_moves": summary.get("tracker_time_eligible_moves"),
        "tracker_time_signals": summary.get("tracker_time_signals"),
        "tracker_time_pass_signals": summary.get("tracker_time_pass_signals"),
        "runtime_inventory_research_candidates": summary.get("runtime_inventory_research_candidates"),
        "tracker_time_inventory_research_candidates": summary.get("tracker_time_inventory_research_candidates"),
        "tracker_time_inventory_modes": summary.get("tracker_time_inventory_modes") or {},
        "tracker_time_replay_status": tracker_time_replay.get("status"),
        "tracker_time_replay_blockers": tracker_time_replay.get("blockers") or [],
        "tracker_time_replay_intents": tracker_time_replay_summary.get("intents"),
        "tracker_time_replay_paper_orders": tracker_time_replay_summary.get("paper_orders"),
        "tracker_time_replay_filled_orders": tracker_time_replay_summary.get("filled_orders"),
        "tracker_time_replay_rejected_orders": tracker_time_replay_summary.get("rejected_orders"),
        "single_wallet_exact_copy_status": single_wallet_exact.get("status"),
        "single_wallet_exact_copy_intents": single_wallet_exact_summary.get("intents"),
        "single_wallet_exact_copy_filled_orders": single_wallet_exact_summary.get("filled_orders"),
        "single_wallet_exact_copy_rejected_orders": single_wallet_exact_summary.get("rejected_orders"),
        "single_wallet_exact_copy_wallet_count": single_wallet_exact_summary.get("wallet_count"),
        "runtime_fresh_buy_events_le_cap": freshness.get("runtime_fresh_buy_events_le_cap"),
        "runtime_eligible_buy_events": freshness.get("runtime_eligible_buy_events"),
        "runtime_eligible_wallets": freshness.get("runtime_eligible_wallets"),
        "tracker_fresh_buy_events_le_cap": freshness.get("tracker_fresh_buy_events_le_cap"),
        "latest_buy_event_lag_s": freshness.get("latest_buy_event_lag_s"),
        "source_feed_delayed": freshness.get("source_feed_delayed"),
        "tracker_fresh_but_runtime_stale": freshness.get("tracker_fresh_but_runtime_stale"),
        "freshness_transition_counts": freshness.get("freshness_transition_counts") or {},
        "freshness_transition_rows": freshness.get("freshness_transition_rows") or [],
    }


def _tracker_snapshot(path: str | Path) -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    copy_eff = summary.get("copy_efficiency") if isinstance(summary.get("copy_efficiency"), dict) else {}
    copy_summary = copy_eff.get("summary") if isinstance(copy_eff.get("summary"), dict) else {}
    tracker_scope = summary.get("tracker_scope") if isinstance(summary.get("tracker_scope"), dict) else {}
    current_poll = (
        summary.get("current_poll_diagnostics")
        if isinstance(summary.get("current_poll_diagnostics"), dict)
        else {}
    )
    current_poll_ladder = (
        current_poll.get("current_poll_ladder")
        if isinstance(current_poll.get("current_poll_ladder"), dict)
        else {}
    )
    wallet_reports = summary.get("wallet_reports") if isinstance(summary.get("wallet_reports"), list) else []

    def _sum_wallet_report_int(key: str) -> int:
        total = 0
        for report in wallet_reports:
            if not isinstance(report, dict):
                continue
            try:
                total += int(report.get(key) or 0)
            except (TypeError, ValueError):
                continue
        return total

    if current_poll_ladder:
        current_poll_ladder = dict(current_poll_ladder)
        for key in (
            "runtime_limited_buy_events_skipped",
            "runtime_limited_fresh_buy_events_skipped",
        ):
            if key not in current_poll_ladder:
                current_poll_ladder[key] = _sum_wallet_report_int(key)
    hot_path = summary.get("hot_path_adaptive") if isinstance(summary.get("hot_path_adaptive"), dict) else {}
    hot_path_summary = hot_path.get("summary") if isinstance(hot_path.get("summary"), dict) else {}
    hot_path_freshness = (
        hot_path_summary.get("freshness_diagnostics")
        if isinstance(hot_path_summary.get("freshness_diagnostics"), dict)
        else {}
    )
    hot_path_lifecycle = hot_path.get("paper_lifecycle") if isinstance(hot_path.get("paper_lifecycle"), dict) else {}
    hot_path_inventory_lifecycle = (
        hot_path.get("inventory_paper_lifecycle")
        if isinstance(hot_path.get("inventory_paper_lifecycle"), dict)
        else {}
    )
    hot_path_single_wallet_lifecycle = (
        hot_path.get("single_wallet_exact_copy_paper_lifecycle")
        if isinstance(hot_path.get("single_wallet_exact_copy_paper_lifecycle"), dict)
        else {}
    )
    tracker_time_replay = (
        hot_path.get("tracker_time_replay") if isinstance(hot_path.get("tracker_time_replay"), dict) else {}
    )
    tracker_time_replay_summary = (
        tracker_time_replay.get("summary") if isinstance(tracker_time_replay.get("summary"), dict) else {}
    )
    tracker_time_replay_lifecycle = (
        tracker_time_replay.get("paper_lifecycle")
        if isinstance(tracker_time_replay.get("paper_lifecycle"), dict)
        else {}
    )
    return {
        "mirror_coverage_status": summary.get("mirror_coverage_status"),
        "copy_efficiency_status": copy_eff.get("status"),
        "new_wallet_events": summary.get("new_wallet_events"),
        "wallets_tracked": tracker_scope.get("wallets_tracked") or summary.get("wallets"),
        "rotation_offset": tracker_scope.get("rotation_offset"),
        "next_rotation_offset": tracker_scope.get("next_rotation_offset"),
        "source_fresh_buy_events_le_10s": copy_summary.get("source_fresh_buy_events_le_10s"),
        "current_poll_zero_current_poll_root_cause": current_poll.get("zero_current_poll_root_cause"),
        "current_poll_fresh_buy_loss_stage": current_poll.get("fresh_buy_loss_stage"),
        "current_poll_blockers": current_poll.get("blockers") or [],
        "current_poll_ladder": current_poll_ladder,
        "current_poll_source_route_status_counts": current_poll.get("source_route_status_counts") or {},
        "current_poll_source_route_class_counts": current_poll.get("source_route_class_counts") or {},
        "current_poll_source_freshness_by_source": current_poll.get("current_poll_source_freshness_by_source") or {},
        "required_buy_copy_events": copy_summary.get("required_buy_copy_events"),
        "clob_filled_buy_copy_events": copy_summary.get("clob_filled_buy_copy_events"),
        "fallback_filled_buy_copy_events": copy_summary.get("fallback_filled_buy_copy_events"),
        "rejected_buy_copy_events": copy_summary.get("rejected_buy_copy_events"),
        "missed_buy_copy_events": copy_summary.get("missed_buy_copy_events"),
        "hot_path_adaptive_status": hot_path.get("status"),
        "hot_path_adaptive_blockers": hot_path.get("blockers") or [],
        "hot_path_pass_signals": hot_path_summary.get("pass_signals"),
        "hot_path_runtime_signal_blocker_counts": hot_path_summary.get("runtime_signal_blocker_counts") or {},
        "hot_path_top_blocked_runtime_signals": hot_path_summary.get("top_blocked_runtime_signals") or [],
        "hot_path_runtime_market_outcome_counts": hot_path_summary.get("runtime_market_outcome_counts") or [],
        "hot_path_current_poll_moves": hot_path_summary.get("current_poll_moves"),
        "hot_path_intents_created": hot_path_summary.get("hot_path_intents_created"),
        "hot_path_filled_orders": hot_path_summary.get("hot_path_filled_orders"),
        "hot_path_rejected_orders": hot_path_summary.get("hot_path_rejected_orders"),
        "hot_path_paper_lifecycle_status": hot_path_lifecycle.get("status"),
        "hot_path_inventory_intents_created": hot_path_summary.get("hot_path_inventory_intents_created"),
        "hot_path_inventory_filled_orders": hot_path_summary.get("hot_path_inventory_filled_orders"),
        "hot_path_inventory_rejected_orders": hot_path_summary.get("hot_path_inventory_rejected_orders"),
        "hot_path_inventory_fill_source_counts": (
            hot_path_inventory_lifecycle.get("fill_source_counts")
            or hot_path_summary.get("hot_path_inventory_fill_source_counts")
            or {}
        ),
        "hot_path_inventory_paper_lifecycle_status": hot_path_inventory_lifecycle.get("status"),
        "hot_path_single_wallet_exact_copy_intents_created": hot_path_summary.get(
            "hot_path_single_wallet_exact_copy_intents_created"
        ),
        "hot_path_single_wallet_exact_copy_filled_orders": hot_path_summary.get(
            "hot_path_single_wallet_exact_copy_filled_orders"
        ),
        "hot_path_single_wallet_exact_copy_rejected_orders": hot_path_summary.get(
            "hot_path_single_wallet_exact_copy_rejected_orders"
        ),
        "hot_path_single_wallet_exact_copy_paper_lifecycle_status": hot_path_single_wallet_lifecycle.get("status"),
        "hot_path_runtime_fresh_buy_events_le_cap": hot_path_freshness.get("runtime_fresh_buy_events_le_cap"),
        "hot_path_runtime_eligible_wallets": hot_path_freshness.get("runtime_eligible_wallets"),
        "hot_path_runtime_inventory_research_candidates": hot_path_summary.get("runtime_inventory_research_candidates"),
        "hot_path_tracker_time_inventory_research_candidates": hot_path_summary.get(
            "tracker_time_inventory_research_candidates"
        ),
        "hot_path_tracker_time_signal_blocker_counts": hot_path_summary.get(
            "tracker_time_signal_blocker_counts"
        )
        or {},
        "hot_path_tracker_time_replay_status": tracker_time_replay.get("status"),
        "hot_path_tracker_time_replay_blockers": tracker_time_replay.get("blockers") or [],
        "hot_path_tracker_time_replay_role": tracker_time_replay.get("role"),
        "hot_path_tracker_time_replay_pass_signals": tracker_time_replay_summary.get("pass_signals"),
        "hot_path_tracker_time_replay_intents_created": tracker_time_replay_summary.get(
            "tracker_time_replay_intents_created"
        ),
        "hot_path_tracker_time_replay_filled_orders": tracker_time_replay_summary.get(
            "tracker_time_replay_filled_orders"
        ),
        "hot_path_tracker_time_replay_rejected_orders": tracker_time_replay_summary.get(
            "tracker_time_replay_rejected_orders"
        ),
        "hot_path_tracker_time_replay_recent_observed_moves": tracker_time_replay_summary.get(
            "recent_observed_moves"
        ),
        "hot_path_tracker_time_replay_eligible_moves": tracker_time_replay_summary.get("eligible_moves"),
        "hot_path_tracker_time_replay_paper_lifecycle_status": tracker_time_replay_lifecycle.get("status"),
    }


def _registry_wallet_count(path: str | Path) -> int:
    payload = load_json(path, default={})
    rows = payload.get("wallets") if isinstance(payload, dict) else payload
    return len(rows) if isinstance(rows, list) else 0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _iter_recent_jsonl_rows(
    path: str | Path,
    *,
    tail_bytes: int,
    limit: int,
) -> list[dict[str, Any]]:
    target = Path(str(path or ""))
    if not target.exists() or not target.is_file():
        return []
    rows: deque[dict[str, Any]] = deque(maxlen=max(1, int(limit)))
    size = target.stat().st_size
    start = max(0, size - max(0, int(tail_bytes)))
    with target.open("rb") as raw:
        raw.seek(start)
        if start > 0:
            raw.readline()
        for line in raw:
            if not line.strip():
                continue
            try:
                row = json.loads(line.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return list(rows)


def _bridge_probe_tracker(row: dict[str, Any]) -> dict[str, Any]:
    tracker = row.get("cohort_probe_tracker")
    if isinstance(tracker, dict):
        return tracker
    tracker = row.get("tracker")
    return tracker if isinstance(tracker, dict) else {}


def _is_development_bridge_probe(row: dict[str, Any]) -> bool:
    probe = row.get("exact_cohort_probe")
    if not isinstance(probe, dict):
        probe = row
    return bool(
        str(probe.get("source") or "") == "development_program_bridge_candidate"
        or str(probe.get("selected_source") or "") == "development_program_bridge_current_poll_inventory_burnin"
        or probe.get("development_program_bridge_focus") is True
    )


def _bridge_probe_strength(row: dict[str, Any]) -> tuple[int, ...]:
    tracker = _bridge_probe_tracker(row)
    fill_sources = (
        tracker.get("hot_path_inventory_fill_source_counts")
        if isinstance(tracker.get("hot_path_inventory_fill_source_counts"), dict)
        else {}
    )
    return (
        _int_value(tracker.get("hot_path_inventory_intents_created")),
        _int_value(tracker.get("hot_path_inventory_filled_orders")),
        _int_value(fill_sources.get("clob_book_evidence")),
        _int_value(tracker.get("hot_path_runtime_fresh_buy_events_le_cap")),
        _int_value(tracker.get("hot_path_runtime_eligible_wallets")),
        _int_value(tracker.get("hot_path_current_poll_moves") or tracker.get("new_wallet_events")),
    )


def _bridge_probe_command(row: dict[str, Any]) -> dict[str, Any]:
    command = row.get("cohort_probe_tracker_command")
    if isinstance(command, dict):
        return command
    command = row.get("tracker_command")
    return command if isinstance(command, dict) else {}


def _bridge_probe_attempt_summary(row: dict[str, Any]) -> dict[str, Any]:
    tracker = _bridge_probe_tracker(row)
    probe = row.get("exact_cohort_probe") if isinstance(row.get("exact_cohort_probe"), dict) else {}
    command = _bridge_probe_command(row)
    fill_sources = (
        tracker.get("hot_path_inventory_fill_source_counts")
        if isinstance(tracker.get("hot_path_inventory_fill_source_counts"), dict)
        else {}
    )
    ladder = tracker.get("current_poll_ladder") if isinstance(tracker.get("current_poll_ladder"), dict) else {}
    wallets = [str(wallet).lower() for wallet in probe.get("wallets") or [] if str(wallet or "").lower()]
    return {
        "tick": row.get("tick"),
        "cohort_index": probe.get("cohort_index"),
        "next_cohort_index": probe.get("next_cohort_index"),
        "rank": probe.get("rank"),
        "candidate_id": probe.get("candidate_id"),
        "wallet_count": len(wallets),
        "wallets": wallets,
        "tracker_command_ok": bool(command.get("ok")),
        "tracker_returncode": command.get("returncode"),
        "current_poll_root_cause": tracker.get("current_poll_zero_current_poll_root_cause"),
        "current_poll_blockers": tracker.get("current_poll_blockers") or [],
        "adaptive_blockers": tracker.get("hot_path_adaptive_blockers") or [],
        "raw_rows": _int_value(ladder.get("raw_rows")),
        "current_poll_moves": _int_value(tracker.get("hot_path_current_poll_moves") or tracker.get("new_wallet_events")),
        "runtime_fresh_buy_events_le_cap": _int_value(tracker.get("hot_path_runtime_fresh_buy_events_le_cap")),
        "runtime_eligible_wallets": _int_value(tracker.get("hot_path_runtime_eligible_wallets")),
        "inventory_intents_created": _int_value(tracker.get("hot_path_inventory_intents_created")),
        "inventory_filled_orders": _int_value(tracker.get("hot_path_inventory_filled_orders")),
        "inventory_rejected_orders": _int_value(tracker.get("hot_path_inventory_rejected_orders")),
        "inventory_clob_filled_orders": _int_value(fill_sources.get("clob_book_evidence")),
        "inventory_fallback_filled_orders": _int_value(fill_sources.get("source_price_plus_slippage_fallback")),
    }


def _current_poll_inventory_bridge_burnin_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    bridge_rows = [row for row in rows if isinstance(row, dict) and _is_development_bridge_probe(row)]
    if not bridge_rows:
        return {
            "status": "NOT_RUN",
            "role": "current_poll_inventory_bridge_burnin_clob_truth",
            "blockers": ["current_poll_inventory_bridge_burnin_not_run"],
            "paper_only": True,
            "live_orders_allowed": False,
        }

    best_row = max(bridge_rows, key=_bridge_probe_strength)
    best_tracker = _bridge_probe_tracker(best_row)
    best_probe = best_row.get("exact_cohort_probe") if isinstance(best_row.get("exact_cohort_probe"), dict) else {}
    fill_sources = (
        best_tracker.get("hot_path_inventory_fill_source_counts")
        if isinstance(best_tracker.get("hot_path_inventory_fill_source_counts"), dict)
        else {}
    )
    intents = _int_value(best_tracker.get("hot_path_inventory_intents_created"))
    filled = _int_value(best_tracker.get("hot_path_inventory_filled_orders"))
    rejected = _int_value(best_tracker.get("hot_path_inventory_rejected_orders"))
    clob_filled = _int_value(fill_sources.get("clob_book_evidence"))
    fallback_filled = _int_value(fill_sources.get("source_price_plus_slippage_fallback"))
    current_poll_moves = _int_value(best_tracker.get("hot_path_current_poll_moves") or best_tracker.get("new_wallet_events"))
    eligible_wallets = _int_value(best_tracker.get("hot_path_runtime_eligible_wallets"))
    fresh_buy_events = _int_value(best_tracker.get("hot_path_runtime_fresh_buy_events_le_cap"))
    attempt_rows = [_bridge_probe_attempt_summary(row) for row in bridge_rows]
    covered_wallets = sorted(
        {
            wallet
            for attempt in attempt_rows
            for wallet in attempt.get("wallets") or []
            if str(wallet or "")
        }
    )
    attempted_cohort_indices = sorted(
        {
            int(index)
            for attempt in attempt_rows
            for index in [attempt.get("cohort_index")]
            if index is not None
        }
    )

    blockers: list[str] = []
    if current_poll_moves <= 0:
        blockers.append("bridge_current_poll_moves_missing")
    if eligible_wallets < 2:
        blockers.append("bridge_current_poll_multi_wallet_truth_missing")
    if intents <= 0:
        blockers.append("bridge_inventory_intents_missing")
    if intents > 0 and filled < intents:
        blockers.append("bridge_inventory_intents_not_fully_filled")
    if intents > 0 and clob_filled < intents:
        blockers.append("bridge_candidate_specific_clob_fills_missing")
    if fallback_filled > 0:
        blockers.append("bridge_fallback_filled_orders_present")
    if rejected > 0:
        blockers.append("bridge_rejected_orders_present")

    if not blockers:
        status = "PASS"
    elif intents > 0 and (rejected > 0 or fallback_filled > 0 or (filled > 0 and clob_filled < intents)):
        status = "CORRECTION"
    else:
        status = "WATCH"

    candidate_ids = sorted(
        {
            str(probe.get("candidate_id") or "")
            for row in bridge_rows
            for probe in [row.get("exact_cohort_probe") if isinstance(row.get("exact_cohort_probe"), dict) else {}]
            if str(probe.get("candidate_id") or "")
        }
    )
    wallet_count = max(
        (
            len((row.get("exact_cohort_probe") or {}).get("wallets") or [])
            for row in bridge_rows
            if isinstance(row.get("exact_cohort_probe"), dict)
        ),
        default=0,
    )
    stop_doing = sorted(
        {
            "do_not_count_fallback_inventory_profit_as_live_admissible_progress",
            *(
                str(item)
                for row in bridge_rows
                for probe in [row.get("exact_cohort_probe") if isinstance(row.get("exact_cohort_probe"), dict) else {}]
                for item in (
                    probe.get("development_program_stop_doing")
                    if isinstance(probe.get("development_program_stop_doing"), list)
                    else []
                )
                if str(item or "")
            ),
        }
    )
    return {
        "status": status,
        "role": "current_poll_inventory_bridge_burnin_clob_truth",
        "blockers": blockers,
        "candidate_ids": candidate_ids,
        "candidate_id": candidate_ids[0] if len(candidate_ids) == 1 else None,
        "policy_id": best_probe.get("policy_id"),
        "bridge_probe_ticks": len(bridge_rows),
        "wallet_count": wallet_count,
        "attempted_cohort_indices": attempted_cohort_indices,
        "attempted_cohorts": len(attempted_cohort_indices),
        "attempted_wallet_count": len(covered_wallets),
        "covered_wallets": covered_wallets,
        "attempts_with_current_poll_moves": sum(
            1 for attempt in attempt_rows if _int_value(attempt.get("current_poll_moves")) > 0
        ),
        "attempts_with_inventory_intents": sum(
            1 for attempt in attempt_rows if _int_value(attempt.get("inventory_intents_created")) > 0
        ),
        "command_failed_attempts": sum(1 for attempt in attempt_rows if not attempt.get("tracker_command_ok")),
        "attempts": attempt_rows[:20],
        "current_poll_moves": current_poll_moves,
        "runtime_fresh_buy_events_le_cap": fresh_buy_events,
        "runtime_eligible_wallets": eligible_wallets,
        "inventory_intents_created": intents,
        "inventory_filled_orders": filled,
        "inventory_rejected_orders": rejected,
        "inventory_clob_filled_orders": clob_filled,
        "inventory_fallback_filled_orders": fallback_filled,
        "inventory_fill_source_counts": dict(sorted(fill_sources.items())),
        "candidate_specific_clob_truth_attached": bool(status == "PASS"),
        "paper_only": True,
        "live_orders_allowed": False,
        "stop_doing": stop_doing,
    }


def _bridge_candidate_wallets_from_active_hotlane(args: argparse.Namespace) -> tuple[list[str], list[str], str | None]:
    payload = load_json(getattr(args, "active_hotlane_state", ""), default={})
    if not isinstance(payload, dict):
        return [], [], None
    wallets: set[str] = set()
    candidate_ids: set[str] = set()
    policy_id: str | None = None
    for key in (
        "selected_development_program_bridge_cohorts",
        "development_program_bridge_cohorts",
        "selected_cohorts",
    ):
        rows = payload.get(key) if isinstance(payload.get(key), list) else []
        for row in rows:
            if not isinstance(row, dict) or not _is_development_bridge_probe(row):
                continue
            if row.get("candidate_id"):
                candidate_ids.add(str(row.get("candidate_id")))
            target = row.get("development_program_target_inventory")
            if isinstance(target, dict) and target.get("candidate_id"):
                candidate_ids.add(str(target.get("candidate_id")))
            if policy_id is None and row.get("policy_id"):
                policy_id = str(row.get("policy_id"))
            raw_wallets = row.get("selected_wallets") or row.get("wallets") or row.get("unique_wallets") or []
            for wallet in raw_wallets if isinstance(raw_wallets, list) else []:
                normalized = _norm_wallet(wallet)
                if normalized:
                    wallets.add(normalized)
    return sorted(wallets), sorted(candidate_ids), policy_id


def _bridge_wallet_context(
    *,
    args: argparse.Namespace,
    current_poll_bridge_burnin: dict[str, Any],
) -> tuple[list[str], list[str], str | None]:
    wallets = [
        _norm_wallet(wallet)
        for wallet in current_poll_bridge_burnin.get("covered_wallets") or []
        if _norm_wallet(wallet)
    ]
    candidate_ids = [
        str(candidate_id)
        for candidate_id in current_poll_bridge_burnin.get("candidate_ids") or []
        if str(candidate_id or "")
    ]
    candidate_id = str(current_poll_bridge_burnin.get("candidate_id") or "")
    if candidate_id and candidate_id not in candidate_ids:
        candidate_ids.append(candidate_id)
    policy_id = (
        str(current_poll_bridge_burnin.get("policy_id"))
        if current_poll_bridge_burnin.get("policy_id")
        else None
    )
    active_wallets, active_candidate_ids, active_policy_id = _bridge_candidate_wallets_from_active_hotlane(args)
    return (
        sorted(dict.fromkeys([*wallets, *active_wallets])),
        sorted(dict.fromkeys([*candidate_ids, *active_candidate_ids])),
        policy_id or active_policy_id,
    )


def _csv_values(value: Any, *, default: tuple[str, ...]) -> tuple[str, ...]:
    items = [item.strip() for item in str(value or "").split(",") if item.strip()]
    return tuple(items) or default


def _event_row_ts(event: Any) -> float:
    return _float_value(getattr(event, "event_ts", None)) or _float_value(getattr(event, "observed_ts", None))


def _policy_id_number(policy_id: str | None, pattern: str, default: float) -> float:
    match = re.search(pattern, str(policy_id or ""))
    if not match:
        return float(default)
    return _float_value(match.group(1)) or float(default)


def _window_indexed_bridge_copy_policy(policy_id: str | None) -> CopyPolicy:
    text = str(policy_id or "")
    wallet_fraction = _policy_id_number(text, r"fast_wf_([0-9]+(?:\.[0-9]+)?)", 0.05)
    max_order_usd = _policy_id_number(text, r"_cap_([0-9]+(?:\.[0-9]+)?)", 2.0)
    max_price = 1.0
    cheap_match = re.search(r"cheap_up_to_([0-9]+)", text)
    if cheap_match:
        max_price = min(1.0, max(0.01, _float_value(cheap_match.group(1)) / 100.0))
    return CopyPolicy(
        policy_id=text or "window_indexed_inventory_bridge_policy",
        strategy_family="wallet_copy_window_indexed_inventory_v1",
        allowed_assets=("BTC",),
        market_filter="btc_5m",
        min_price=0.01,
        max_price=max_price,
        sizing=SizingPolicy(
            policy_id=f"wallet_fraction_{wallet_fraction:g}_cap_{max_order_usd:g}",
            basis="wallet_usdc_fraction",
            wallet_fraction=wallet_fraction,
            max_order_usd=max_order_usd,
            min_order_usd=0.0,
        ),
    )


def _coerce_window_indexed_wallet_event(event: Any) -> WalletEvent | None:
    if isinstance(event, WalletEvent):
        return event
    condition_id = str(getattr(event, "condition_id", "") or "")
    market_slug = str(getattr(event, "market_slug", "") or "")
    outcome = str(getattr(event, "outcome", "") or "")
    token_id = str(getattr(event, "token_id", "") or "")
    price = _float_value(getattr(event, "price", 0.0))
    size = _float_value(getattr(event, "size", 0.0))
    if not condition_id or not outcome or price <= 0 or size <= 0:
        return None
    action = str(getattr(event, "action", "") or ("BUY" if bool(getattr(event, "is_buy", False)) else "")).upper()
    if not action:
        return None
    observed_ts = _float_value(getattr(event, "observed_ts", None)) or time.time()
    asset = str(getattr(event, "asset", "") or "")
    duration = str(getattr(event, "duration", "") or "")
    slug_lower = market_slug.lower()
    if not asset and ("btc" in slug_lower or "bitcoin" in slug_lower):
        asset = "BTC"
    if not duration and ("5m" in slug_lower or "5-min" in slug_lower):
        duration = "5m"
    return WalletEvent(
        event_id=str(getattr(event, "event_id", "") or ""),
        source_wallet=str(getattr(event, "source_wallet", "") or "unknown").lower(),
        wallet_name=str(getattr(event, "wallet_name", "") or "window_indexed_wallet"),
        row_type=str(getattr(event, "row_type", "") or "trade"),
        action=action,
        condition_id=condition_id,
        market_id=str(getattr(event, "market_id", "") or condition_id),
        market_slug=market_slug,
        outcome=outcome,
        price=price,
        size=size,
        usdc_size=_float_value(getattr(event, "usdc_size", 0.0)) or round(price * size, 6),
        token_id=token_id,
        event_ts=getattr(event, "event_ts", None),
        observed_ts=observed_ts,
        asset=asset,
        duration=duration,
        transaction_hash=str(getattr(event, "transaction_hash", "") or ""),
    )


def _clob_book_evidence_for_intent(
    intent: CopyIntent,
    *,
    clob: CLOBMarketClient,
    book_cache: dict[str, dict[str, Any]],
    max_slippage_bps: float,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    token_id = str(intent.token_id or "").strip()
    if not token_id:
        error = {
            "type": "missing_token_id",
            "intent_id": intent.intent_id,
            "source_event_id": intent.source_event_id,
        }
        return {
            "enabled": True,
            "status": "MISSING_TOKEN_ID",
            "token_id": token_id,
            "error": "CopyIntent has no token_id for CLOB book lookup",
        }, error

    cache_hit = token_id in book_cache
    if not cache_hit:
        started = time.perf_counter()
        try:
            book_cache[token_id] = {
                "book": clob.get_book(token_id),
                "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
                "error": None,
            }
        except Exception as exc:  # pragma: no cover - network edge cases are summarized, not raised.
            book_cache[token_id] = {
                "book": {},
                "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
                "error": {
                    "type": type(exc).__name__,
                    "message": str(exc)[-500:],
                    "token_id": token_id,
                },
            }

    cached = book_cache[token_id]
    error = cached.get("error") if isinstance(cached.get("error"), dict) else None
    if error:
        return {
            "enabled": True,
            "status": "ERROR",
            "token_id": token_id,
            "cache_key": token_id,
            "cache_hit": cache_hit,
            "fetch_duration_s": cached.get("fetch_duration_s"),
            "error": error.get("message"),
            "error_type": error.get("type"),
        }, error

    book = cached.get("book") if isinstance(cached.get("book"), dict) else {}
    clob_route_report = (
        book.get("__walletCopyClobRouteReport") if isinstance(book.get("__walletCopyClobRouteReport"), dict) else {}
    )
    summary = CLOBMarketClient.summarize_book(
        book,
        copy_size_usd=float(intent.copy_size_usd),
        source_price=float(intent.limit_price),
        max_slippage_bps=float(max_slippage_bps),
    )
    return {
        "enabled": True,
        "status": "OK",
        "token_id": token_id,
        "cache_key": token_id,
        "cache_hit": cache_hit,
        "fetch_duration_s": cached.get("fetch_duration_s") if not cache_hit else None,
        "route_status": clob_route_report.get("status"),
        "route_class": clob_route_report.get("route_class"),
        "route_report_id": clob_route_report.get("route_report_id"),
        "route_host": clob_route_report.get("host"),
        "routed_host": clob_route_report.get("routed_host"),
        "source_base_override_configured": bool(clob_route_report.get("source_base_override_configured")),
        "request_fingerprint": clob_route_report.get("request_fingerprint"),
        **summary,
    }, None


def _intent_with_clob_book_evidence(intent: CopyIntent, clob_book: dict[str, Any]) -> CopyIntent:
    metadata = dict(intent.metadata) if isinstance(intent.metadata, dict) else {}
    evidence = dict(metadata.get("live_tracking_evidence") or {})
    evidence["clob_book"] = clob_book
    metadata["live_tracking_evidence"] = evidence
    metadata["window_indexed_bridge"] = {
        "source_mode": "window_indexed_wallet_events",
        "paper_only": True,
        "live_orders_allowed": False,
    }
    return replace(intent, metadata=metadata)


def _window_indexed_paper_ledger_row(
    *,
    candidate_id: str | None,
    policy_id: str | None,
    intent: CopyIntent,
    fill_estimate: dict[str, Any],
) -> dict[str, Any]:
    requested_size = _float_value(fill_estimate.get("requested_size_usd"))
    if requested_size <= 0:
        requested_size = _float_value(intent.copy_size_usd)
    filled_size = _float_value(fill_estimate.get("filled_size_usd"))
    final_status = str(fill_estimate.get("status") or "REJECTED")
    source_intent = intent.asdict()
    order_id = stable_id(
        "wipo",
        {
            "intent_id": intent.intent_id,
            "fill_id": fill_estimate.get("fill_id"),
            "candidate_id": candidate_id,
        },
    )
    fill_source = str(fill_estimate.get("source") or "")
    return {
        "order_id": order_id,
        "paper_order_id": order_id,
        "candidate_id": candidate_id,
        "policy_id": policy_id,
        "intent_id": intent.intent_id,
        "source_event_id": intent.source_event_id,
        "source_fingerprint": (intent.metadata or {}).get("source_fingerprint"),
        "source_wallet": intent.source_wallet,
        "wallet_name": intent.wallet_name,
        "condition_id": intent.condition_id,
        "market_slug": intent.market_slug,
        "outcome": intent.outcome,
        "side": intent.side,
        "token_id": intent.token_id,
        "limit_price": round(float(intent.limit_price), 6),
        "wallet_usdc_size": round(float(intent.wallet_usdc_size), 6),
        "copy_size_usd": round(float(intent.copy_size_usd), 6),
        "requested_size_usd": round(requested_size, 6),
        "requested_shares": round(float(intent.shares), 6),
        "filled_size_usd": round(filled_size, 6),
        "filled_shares": _float_value(fill_estimate.get("filled_shares")),
        "rejected_size_usd": round(requested_size if final_status == "REJECTED" else 0.0, 6),
        "effective_price": _float_value(fill_estimate.get("effective_price")),
        "fill_ratio": _float_value(fill_estimate.get("fill_ratio")),
        "status": final_status,
        "final_status": final_status,
        "fill_source": fill_source,
        "reject_reason": fill_estimate.get("reject_reason"),
        "reject_stage": fill_estimate.get("reject_stage"),
        "clob_backed_fill_evidence": fill_source == "clob_book_evidence",
        "realized_pnl_usd": 0.0,
        "paper_only": True,
        "live_orders_allowed": False,
        "source_intent": source_intent,
        "fill_estimate": fill_estimate,
    }


def _window_indexed_continuous_paper_lane(
    *,
    candidate_id: str | None,
    policy_id: str | None,
    events: list[WalletEvent],
    blockers: list[str],
    bridge_error_count: int,
    max_book_slippage_bps: float,
    clob_timeout_s: float,
    previous_lane: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = _window_indexed_bridge_copy_policy(policy_id)
    ledger_rows: list[dict[str, Any]] = []
    clob_errors: list[dict[str, Any]] = []
    policy_filtered_events = 0
    book_cache: dict[str, dict[str, Any]] = {}
    fill_config = FillModelConfig(
        fallback_slippage_bps=0.0,
        min_fill_ratio=0.999,
        max_copy_price=0.99,
        allow_fallback_without_book=False,
    )
    sorted_events = sorted(
        events,
        key=lambda event: (_event_row_ts(event), str(getattr(event, "event_id", ""))),
        reverse=True,
    )
    clob = CLOBMarketClient(host=DIRECT_CLOB_BASE_URL, timeout_s=float(clob_timeout_s), retries=1)
    with _without_bridge_source_overrides():
        for event in sorted_events:
            wallet_event = _coerce_window_indexed_wallet_event(event)
            if wallet_event is None:
                policy_filtered_events += 1
                continue
            intent = event_to_intent(wallet_event, policy=policy, mode="paper")
            if intent is None:
                policy_filtered_events += 1
                continue
            clob_book, error = _clob_book_evidence_for_intent(
                intent,
                clob=clob,
                book_cache=book_cache,
                max_slippage_bps=max_book_slippage_bps,
            )
            if error is not None:
                clob_errors.append(
                    {
                        **error,
                        "intent_id": intent.intent_id,
                        "source_event_id": intent.source_event_id,
                    }
                )
            priced_intent = _intent_with_clob_book_evidence(intent, clob_book)
            fill_estimate = estimate_executable_fill(priced_intent, fill_config)
            ledger_rows.append(
                _window_indexed_paper_ledger_row(
                    candidate_id=candidate_id,
                    policy_id=policy_id,
                    intent=priced_intent,
                    fill_estimate=fill_estimate,
                )
            )

    paper_orders = len(ledger_rows)
    filled_rows = [row for row in ledger_rows if row.get("final_status") == "FILLED"]
    rejected_rows = [row for row in ledger_rows if row.get("final_status") == "REJECTED"]
    clob_backed_rows = [row for row in ledger_rows if row.get("clob_backed_fill_evidence") is True]
    current_tick_matching_windows = len(
        {str(row.get("market_slug") or row.get("condition_id") or "") for row in ledger_rows if row}
    )
    operation_error_count = int(bridge_error_count) + len(clob_errors)
    previous_clean = 0
    if isinstance(previous_lane, dict):
        previous_clean = int(previous_lane.get("clean_window_count") or 0)
    clean_windows = previous_clean + 1 if paper_orders > 0 and operation_error_count <= 0 else 0
    lane_blockers = list(blockers)
    if not events:
        lane_blockers.append("window_indexed_paper_lane_source_events_missing")
    if events and paper_orders <= 0:
        lane_blockers.append("window_indexed_paper_lane_copy_intents_missing")
    if clob_errors:
        lane_blockers.append("window_indexed_paper_lane_clob_book_errors")
    if paper_orders > 0 and len(clob_backed_rows) < paper_orders:
        lane_blockers.append("window_indexed_paper_lane_clob_backed_fill_evidence_incomplete")
    if clean_windows < 10:
        lane_blockers.append("window_indexed_paper_lane_clean_window_count_below_10")
    gate_counting = bool(paper_orders > 0 and clean_windows >= 10 and not lane_blockers)
    return {
        "role": "window_indexed_continuous_paper_measurement_lane",
        "flow_stage": "TRACK2",
        "paper_only": True,
        "live_orders_allowed": False,
        "candidate_id": candidate_id,
        "policy_id": policy_id,
        "copy_policy": {
            "policy_id": policy.policy_id,
            "strategy_family": policy.strategy_family,
            "min_price": policy.min_price,
            "max_price": policy.max_price,
            "sizing": policy.sizing.asdict(),
        },
        "status": "PASS" if gate_counting else "WATCH",
        "gate_counting": gate_counting,
        "acceptance_required_clean_windows": 10,
        "clean_window_count": clean_windows,
        "current_tick_matching_windows": current_tick_matching_windows,
        "bridge_error_count": bridge_error_count,
        "clob_book_error_count": len(clob_errors),
        "operation_error_count": operation_error_count,
        "source_events": len(events),
        "copy_intents_created": paper_orders,
        "policy_filtered_events": policy_filtered_events,
        "clob_book_asset_count": len(book_cache),
        "paper_orders": paper_orders,
        "paper_fills": len(filled_rows),
        "paper_rejects": len(rejected_rows),
        "realized_pnl_usd": 0.0,
        "clob_backed_order_count": len(clob_backed_rows),
        "clob_backed_fill_evidence": bool(paper_orders > 0 and len(clob_backed_rows) == paper_orders),
        "fill_source_counts": dict(sorted(Counter(str(row.get("fill_source") or "unknown") for row in ledger_rows).items())),
        "final_status_counts": dict(sorted(Counter(str(row.get("final_status") or "UNKNOWN") for row in ledger_rows).items())),
        "blockers": sorted(set(lane_blockers)),
        "clob_errors": clob_errors[:20],
        "ledger_rows": ledger_rows[:80],
        "next_action": (
            "continue consecutive clean CLOB-backed paper windows until gate_counting=true"
            if paper_orders > 0 and not clob_errors
            else "repair window-indexed CLOB-backed CopyIntent paper measurement"
        ),
    }


def _window_indexed_inventory_bridge_burnin_summary(
    *,
    args: argparse.Namespace,
    current_poll_bridge_burnin: dict[str, Any],
) -> dict[str, Any]:
    wallets, candidate_ids, policy_id = _bridge_wallet_context(
        args=args,
        current_poll_bridge_burnin=current_poll_bridge_burnin,
    )
    base = {
        "role": "window_indexed_inventory_bridge_burnin_data_api_truth",
        "source_mode": "window_indexed_wallet_events",
        "candidate_ids": candidate_ids,
        "candidate_id": candidate_ids[0] if len(candidate_ids) == 1 else None,
        "policy_id": policy_id,
        "covered_wallets": wallets,
        "wallet_count": len(wallets),
        "paper_only": True,
        "live_orders_allowed": False,
        "data_api_base_url": DIRECT_DATA_API_BASE_URL,
        "source_base_overrides_disabled": True,
    }
    if not bool(getattr(args, "bridge_window_indexed_data_api", True)):
        return {**base, "status": "DISABLED", "blockers": []}
    if not wallets:
        return {
            **base,
            "status": "NOT_RUN",
            "blockers": ["bridge_window_indexed_wallets_missing"],
            "raw_rows": 0,
            "data_api_raw_rows": 0,
        }

    now_s = time.time()
    max_event_age_s = max(
        0.0,
        float(
            getattr(
                args,
                "bridge_window_indexed_max_event_age_s",
                getattr(args, "bridge_live_feed_max_event_age_s", 900.0),
            )
        ),
    )
    since_s = now_s - max_event_age_s if max_event_age_s > 0 else 0.0
    limit = max(1, int(getattr(args, "bridge_window_indexed_limit", 50)))
    pages = max(1, int(getattr(args, "bridge_window_indexed_pages", 1)))
    timeout_s = max(
        0.25,
        float(getattr(args, "bridge_window_indexed_data_api_timeout_s", getattr(args, "data_api_timeout_s", 2.0))),
    )
    retries = max(0, int(getattr(args, "bridge_window_indexed_data_api_retries", 1)))
    trade_query_keys = _csv_values(
        getattr(args, "data_api_trade_query_keys", "user,proxyWallet"),
        default=("user", "proxyWallet"),
    )
    disabled_env_vars = sorted(key for key in SOURCE_BASE_OVERRIDE_ENV_VARS if os.getenv(key))

    wallet_reports: list[dict[str, Any]] = []
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    data_api_raw_rows = 0
    recent_rows = 0
    recent_buy_rows = 0
    wallets_with_recent_rows: set[str] = set()
    wallets_with_recent_buy_rows: set[str] = set()
    source_base_override_counts: Counter[str] = Counter()
    source_route_status_counts: Counter[str] = Counter()
    source_stop_reason_counts: Counter[str] = Counter()
    api_errors: list[dict[str, Any]] = []
    all_recent_buy_events: list[WalletEvent] = []

    with _without_bridge_source_overrides():
        for wallet in wallets:
            spec = WalletSpec(
                name=f"bridge_{wallet[-8:]}",
                address=wallet,
                data_api=DIRECT_DATA_API_BASE_URL,
            )
            client = WalletHistoryClient(spec, timeout_s=timeout_s, retries=retries)
            try:
                events = client.fetch_events(
                    limit=limit,
                    pages=pages,
                    include_activity=False,
                    parallel_sources=bool(getattr(args, "parallel_data_api_sources", True)),
                    trade_query_keys=trade_query_keys,
                )
            except Exception as exc:
                error = {
                    "wallet": wallet,
                    "type": type(exc).__name__,
                    "message": str(exc)[-500:],
                }
                api_errors.append(error)
                wallet_reports.append(
                    {
                        "wallet": wallet,
                        "status": "ERROR",
                        "raw_rows": 0,
                        "normalized_events": 0,
                        "recent_btc_5m_rows": 0,
                        "recent_buy_rows": 0,
                        "error": error,
                    }
                )
                continue

            report = client.last_fetch_report if isinstance(client.last_fetch_report, dict) else {}
            source_base_override_counts.update(
                {
                    str(key): _int_value(value)
                    for key, value in (report.get("source_base_override_counts") or {}).items()
                    if _int_value(value) > 0
                }
            )
            source_route_status_counts.update(
                str(status)
                for status in (report.get("source_route_status_by_source") or {}).values()
                if str(status or "")
            )
            source_stop_reason_counts.update(
                str(reason)
                for reason in (report.get("source_stop_reasons") or {}).values()
                if str(reason or "")
            )
            raw_rows = _int_value(report.get("raw_rows"))
            data_api_raw_rows += raw_rows

            recent_events = []
            recent_buy_events = []
            latest_ts = 0.0
            for event in events:
                row_ts = _event_row_ts(event)
                if row_ts > 0:
                    latest_ts = max(latest_ts, row_ts)
                if max_event_age_s > 0 and row_ts > 0 and row_ts < since_s:
                    continue
                recent_events.append(event)
                if getattr(event, "is_buy", False):
                    recent_buy_events.append(event)

            if recent_events:
                wallets_with_recent_rows.add(wallet)
            if recent_buy_events:
                wallets_with_recent_buy_rows.add(wallet)
                all_recent_buy_events.extend(recent_buy_events)
            recent_rows += len(recent_events)
            recent_buy_rows += len(recent_buy_events)

            for event in recent_buy_events:
                window_key = str(getattr(event, "market_slug", "") or getattr(event, "condition_id", "") or "")
                outcome = str(getattr(event, "outcome", "") or "")
                token_id = str(getattr(event, "token_id", "") or "")
                key = (window_key, outcome, token_id)
                group = groups.setdefault(
                    key,
                    {
                        "market_slug": str(getattr(event, "market_slug", "") or ""),
                        "condition_id": str(getattr(event, "condition_id", "") or ""),
                        "outcome": outcome,
                        "token_id": token_id,
                        "events": 0,
                        "wallets": set(),
                        "source_shares": 0.0,
                        "source_usd": 0.0,
                        "latest_ts": 0.0,
                    },
                )
                size = _float_value(getattr(event, "size", 0.0))
                price = _float_value(getattr(event, "price", 0.0))
                group["events"] += 1
                group["wallets"].add(wallet)
                group["source_shares"] += size
                group["source_usd"] += _float_value(getattr(event, "usdc_size", 0.0)) or (price * size)
                group["latest_ts"] = max(_float_value(group.get("latest_ts")), _event_row_ts(event))

            wallet_reports.append(
                {
                    "wallet": wallet,
                    "status": report.get("ingest_status") or "PASS",
                    "raw_rows": raw_rows,
                    "normalized_events": _int_value(report.get("normalized_events")),
                    "normalized_trade_events": _int_value(report.get("normalized_trade_events")),
                    "recent_btc_5m_rows": len(recent_events),
                    "recent_buy_rows": len(recent_buy_events),
                    "latest_event_age_s": round(max(0.0, now_s - latest_ts), 6) if latest_ts > 0 else None,
                    "trade_query_keys": list(report.get("trade_query_keys") or trade_query_keys),
                    "source_route_status_by_source": report.get("source_route_status_by_source") or {},
                    "source_stop_reasons": report.get("source_stop_reasons") or {},
                    "source_base_override_counts": report.get("source_base_override_counts") or {},
                    "api_errors": report.get("api_errors") or [],
                }
            )

    public_groups: list[dict[str, Any]] = []
    for group in groups.values():
        source_shares = _float_value(group.get("source_shares"))
        source_usd = _float_value(group.get("source_usd"))
        wallets_for_group = sorted(str(wallet) for wallet in group.get("wallets") or [] if str(wallet))
        latest_ts = _float_value(group.get("latest_ts"))
        public_groups.append(
            {
                "market_slug": group.get("market_slug"),
                "condition_id": group.get("condition_id"),
                "outcome": group.get("outcome"),
                "token_id": group.get("token_id"),
                "events": int(group.get("events") or 0),
                "wallet_count": len(wallets_for_group),
                "wallets": wallets_for_group[:8],
                "source_shares": round(source_shares, 6),
                "source_usd": round(source_usd, 6),
                "source_vwap": round(source_usd / source_shares, 8) if source_shares > 0 else None,
                "latest_age_s": round(max(0.0, now_s - latest_ts), 6) if latest_ts > 0 else None,
            }
        )
    public_groups.sort(
        key=lambda row: (
            row.get("latest_age_s") if row.get("latest_age_s") is not None else 10**9,
            -int(row.get("wallet_count") or 0),
            -int(row.get("events") or 0),
            str(row.get("market_slug") or ""),
        )
    )
    multi_wallet_groups = [row for row in public_groups if int(row.get("wallet_count") or 0) >= 2]

    blockers: list[str] = []
    if data_api_raw_rows <= 0:
        blockers.append("bridge_window_indexed_data_api_rows_missing")
    if recent_rows <= 0:
        blockers.append("bridge_window_indexed_in_window_rows_missing")
    if recent_buy_rows <= 0:
        blockers.append("bridge_window_indexed_buy_rows_missing")
    if not wallets_with_recent_rows:
        blockers.append("bridge_window_indexed_wallet_activity_missing")
    if not multi_wallet_groups:
        blockers.append("bridge_window_indexed_multi_wallet_window_missing")
    if api_errors:
        blockers.append("bridge_window_indexed_partial_data_api_error")
    status = "PASS" if not blockers else "WATCH"
    previous_output = getattr(args, "output", None)
    previous_state = load_json(str(previous_output), default={}) if previous_output else {}
    previous_lane = (
        (previous_state.get("window_indexed_inventory_bridge_burnin") or {}).get("continuous_paper_measurement_lane")
        if isinstance(previous_state, dict)
        else None
    )
    continuous_lane = _window_indexed_continuous_paper_lane(
        candidate_id=base.get("candidate_id"),
        policy_id=policy_id,
        events=all_recent_buy_events,
        blockers=blockers,
        bridge_error_count=len(api_errors),
        max_book_slippage_bps=float(getattr(args, "bridge_window_indexed_max_book_slippage_bps", 150.0)),
        clob_timeout_s=float(getattr(args, "clob_timeout_s", getattr(args, "data_api_timeout_s", 0.8))),
        previous_lane=previous_lane if isinstance(previous_lane, dict) else None,
    )
    return {
        **base,
        "status": status,
        "blockers": blockers,
        "raw_rows": recent_rows,
        "data_api_raw_rows": data_api_raw_rows,
        "matching_buy_events": recent_buy_rows,
        "wallets_with_recent_rows": len(wallets_with_recent_rows),
        "wallets_with_recent_buy_rows": len(wallets_with_recent_buy_rows),
        "inventory_window_groups": len(public_groups),
        "multi_wallet_window_groups": len(multi_wallet_groups),
        "max_event_age_s": round(max_event_age_s, 6),
        "since_s": round(since_s, 6) if since_s > 0 else None,
        "limit": limit,
        "pages": pages,
        "trade_query_keys": list(trade_query_keys),
        "source_route_status_counts": dict(sorted(source_route_status_counts.items())),
        "source_stop_reason_counts": dict(sorted(source_stop_reason_counts.items())),
        "source_base_override_counts": dict(sorted(source_base_override_counts.items())),
        "disabled_env_vars": disabled_env_vars,
        "candidate_specific_window_truth_attached": bool(recent_rows > 0 and not api_errors),
        "candidate_specific_clob_truth_attached": bool(continuous_lane.get("clob_backed_fill_evidence")),
        "continuous_paper_measurement_lane": continuous_lane,
        "wallet_reports": wallet_reports[:40],
        "groups": public_groups[:20],
    }


def _clob_truth_by_asset(
    *,
    path: str | Path,
    tail_bytes: int,
    max_age_s: float,
    now_s: float,
) -> dict[str, dict[str, Any]]:
    rows = _iter_recent_jsonl_rows(path, tail_bytes=int(tail_bytes), limit=100_000)
    truth: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("event_type") or row.get("event") or "") not in {"best_bid_ask", "book", "price_change"}:
            continue
        asset_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "").strip()
        if not asset_id:
            continue
        captured_at_s = _float_value(row.get("captured_at_s") or row.get("received_at_s"))
        if max_age_s > 0 and captured_at_s > 0 and now_s - captured_at_s > float(max_age_s):
            continue
        best_bid = row.get("best_bid")
        best_ask = row.get("best_ask")
        existing = truth.get(asset_id)
        if existing is None or captured_at_s >= _float_value(existing.get("captured_at_s")):
            truth[asset_id] = {
                "asset_id": asset_id,
                "captured_at_s": captured_at_s or None,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "source": row.get("source") or row.get("event_type") or row.get("event"),
            }
    return truth


def _live_feed_bridge_asset_evidence(
    *,
    args: argparse.Namespace,
    wallets: list[str],
) -> dict[str, Any]:
    live_feed_path = Path(str(getattr(args, "bridge_live_feed_jsonl", "") or ""))
    base = {
        "live_feed_jsonl": str(live_feed_path),
        "wallet_count": len(wallets),
        "asset_ids": [],
        "asset_count": 0,
    }
    if not wallets:
        return {**base, "status": "NO_WALLETS", "blockers": ["bridge_live_feed_wallets_missing"]}
    if not live_feed_path.exists():
        return {**base, "status": "MISSING_LIVE_FEED", "blockers": ["bridge_live_feed_jsonl_missing"]}

    now_s = time.time()
    rows = _iter_recent_jsonl_rows(
        live_feed_path,
        tail_bytes=int(getattr(args, "bridge_live_feed_tail_bytes", 64 * 1024 * 1024)),
        limit=int(getattr(args, "bridge_live_feed_scan_limit", 100_000)),
    )
    wallet_set = set(wallets)
    max_event_age_s = float(getattr(args, "bridge_live_feed_max_event_age_s", 900.0))
    stats: dict[str, dict[str, Any]] = {}
    matching_buy_events = 0
    skipped_old_rows = 0
    for row in rows:
        if row.get("event") != "wallet_copy_wallet_event":
            continue
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        wallet = _norm_wallet(row.get("source_wallet") or raw.get("proxyWallet"))
        if wallet not in wallet_set:
            continue
        action = str(row.get("action") or raw.get("side") or "").upper()
        if action != "BUY":
            continue
        observed_ts = _float_value(row.get("observed_ts") or row.get("received_at_s") or row.get("captured_at_s"))
        event_ts = _float_value(row.get("event_ts") or raw.get("timestamp"))
        row_ts = observed_ts or event_ts
        if max_event_age_s > 0 and row_ts > 0 and now_s - row_ts > max_event_age_s:
            skipped_old_rows += 1
            continue
        token_id = str(row.get("token_id") or raw.get("asset") or raw.get("tokenId") or "").strip()
        if not token_id:
            continue
        matching_buy_events += 1
        record = stats.setdefault(
            token_id,
            {
                "asset_id": token_id,
                "events": 0,
                "latest_ts": 0.0,
                "wallets": set(),
                "outcomes": set(),
                "markets": set(),
            },
        )
        record["events"] += 1
        record["latest_ts"] = max(float(record.get("latest_ts") or 0.0), row_ts)
        record["wallets"].add(wallet)
        outcome = str(row.get("outcome") or raw.get("outcome") or "")
        if outcome:
            record["outcomes"].add(outcome)
        market = str(row.get("market_slug") or row.get("event_slug") or raw.get("slug") or raw.get("eventSlug") or "")
        if market:
            record["markets"].add(market)

    ranked = sorted(
        stats.values(),
        key=lambda row: (
            -float(row.get("latest_ts") or 0.0),
            -int(row.get("events") or 0),
            str(row.get("asset_id") or ""),
        ),
    )
    max_assets = max(1, int(getattr(args, "bridge_clob_book_auto_snapshot_max_assets", 20)))
    selected = ranked[:max_assets]
    asset_ids = [str(row.get("asset_id")) for row in selected if str(row.get("asset_id") or "")]
    return {
        **base,
        "status": "READY" if asset_ids else "NO_ASSETS",
        "blockers": [] if asset_ids else ["bridge_live_feed_asset_ids_missing"],
        "scanned_rows": len(rows),
        "matching_buy_events": matching_buy_events,
        "skipped_old_rows": skipped_old_rows,
        "asset_ids": asset_ids,
        "asset_count": len(asset_ids),
        "candidate_assets": [
            {
                "asset_id": str(row.get("asset_id") or ""),
                "events": int(row.get("events") or 0),
                "wallet_count": len(row.get("wallets") or []),
                "outcomes": sorted(str(item) for item in (row.get("outcomes") or []) if str(item))[:4],
                "markets": sorted(str(item) for item in (row.get("markets") or []) if str(item))[:4],
                "latest_age_s": (
                    round(max(0.0, now_s - float(row.get("latest_ts") or 0.0)), 6)
                    if float(row.get("latest_ts") or 0.0) > 0.0
                    else None
                ),
            }
            for row in selected[:20]
        ],
    }


def _capture_live_feed_bridge_clob_truth(
    *,
    args: argparse.Namespace,
    current_poll_bridge_burnin: dict[str, Any],
    started: float | None = None,
) -> dict[str, Any]:
    wallets, candidate_ids, policy_id = _bridge_wallet_context(
        args=args,
        current_poll_bridge_burnin=current_poll_bridge_burnin,
    )
    base = {
        "role": "live_feed_inventory_bridge_clob_book_auto_snapshot",
        "enabled": bool(getattr(args, "bridge_clob_book_auto_snapshot", True)),
        "candidate_ids": candidate_ids,
        "candidate_id": candidate_ids[0] if len(candidate_ids) == 1 else None,
        "policy_id": policy_id,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    if not bool(getattr(args, "bridge_clob_book_auto_snapshot", True)):
        return {**base, "status": "DISABLED"}

    asset_evidence = _live_feed_bridge_asset_evidence(args=args, wallets=wallets)
    asset_ids = [str(asset_id) for asset_id in asset_evidence.get("asset_ids") or [] if str(asset_id)]
    if not asset_ids:
        return {
            **base,
            "status": "SKIPPED",
            "asset_evidence": asset_evidence,
            "blockers": asset_evidence.get("blockers") or ["bridge_live_feed_asset_ids_missing"],
        }

    duration_s = max(0.1, float(getattr(args, "bridge_clob_book_auto_snapshot_duration_s", 2.0)))
    interval_s = max(0.1, float(getattr(args, "bridge_clob_book_auto_snapshot_interval_s", 0.5)))
    clob_timeout_s = max(0.1, float(getattr(args, "clob_timeout_s", 0.8)))
    estimated_timeout_s = max(
        1.0,
        duration_s + len(asset_ids) * (clob_timeout_s + 0.1) + 5.0,
    )
    command_timeout_s = min(
        estimated_timeout_s,
        max(1.0, float(getattr(args, "command_timeout_s", estimated_timeout_s))),
    )
    argv = [
        PYTHON,
        "scripts/capture_clob_book_snapshots.py",
        "--asset-ids-file",
        "",
        "--output",
        str(getattr(args, "bridge_clob_book_jsonl", "") or "data/research/clob_book_snapshots_live_feed_bridge.jsonl"),
        "--clob-base-url",
        DIRECT_CLOB_BASE_URL,
        "--disable-source-base-overrides",
        "--duration-s",
        str(duration_s),
        "--interval-s",
        str(interval_s),
        "--timeout-s",
        str(clob_timeout_s),
        "--clob-retries",
        "1",
        "--max-assets",
        str(len(asset_ids)),
    ]
    for asset_id in asset_ids:
        argv.extend(["--asset-id", asset_id])
    command = _run_command(
        name="live_feed_bridge_clob_book_snapshot",
        argv=argv,
        acceptable_returncodes=(0, 2),
        timeout_s=command_timeout_s,
        env=_bridge_direct_source_env(),
    )
    returncode = int(command.get("returncode") or 0)
    if returncode == 0:
        status = "CAPTURED"
    elif returncode == 2:
        status = "NO_BOOK_ROWS"
    elif returncode == 124:
        status = "SNAPSHOT_TIMEOUT"
    else:
        status = "SNAPSHOT_ERROR"
    return {
        **base,
        "status": status,
        "asset_evidence": asset_evidence,
        "asset_ids": asset_ids,
        "asset_count": len(asset_ids),
        "output": str(getattr(args, "bridge_clob_book_jsonl", "") or "data/research/clob_book_snapshots_live_feed_bridge.jsonl"),
        "timeout_s": round(command_timeout_s, 3),
        "command": command,
    }


def _live_feed_inventory_bridge_burnin_summary(
    *,
    args: argparse.Namespace,
    current_poll_bridge_burnin: dict[str, Any],
    clob_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wallets, candidate_ids, policy_id = _bridge_wallet_context(
        args=args,
        current_poll_bridge_burnin=current_poll_bridge_burnin,
    )
    base = {
        "role": "live_feed_inventory_bridge_burnin_clob_truth",
        "candidate_ids": candidate_ids,
        "candidate_id": candidate_ids[0] if len(candidate_ids) == 1 else None,
        "policy_id": policy_id,
        "covered_wallets": wallets,
        "wallet_count": len(wallets),
        "paper_only": True,
        "live_orders_allowed": False,
    }
    if not wallets:
        return {
            **base,
            "status": "NOT_RUN",
            "blockers": ["bridge_live_feed_wallets_missing"],
            "clob_snapshot": clob_snapshot or {},
            "candidate_specific_clob_truth_attached": False,
        }

    live_feed_path = Path(str(getattr(args, "bridge_live_feed_jsonl", "") or ""))
    if not live_feed_path.exists():
        return {
            **base,
            "status": "WATCH",
            "blockers": ["bridge_live_feed_jsonl_missing"],
            "live_feed_jsonl": str(live_feed_path),
            "clob_snapshot": clob_snapshot or {},
            "candidate_specific_clob_truth_attached": False,
        }

    now_s = time.time()
    rows = _iter_recent_jsonl_rows(
        live_feed_path,
        tail_bytes=int(getattr(args, "bridge_live_feed_tail_bytes", 64 * 1024 * 1024)),
        limit=int(getattr(args, "bridge_live_feed_scan_limit", 100_000)),
    )
    wallet_set = set(wallets)
    max_event_age_s = float(getattr(args, "bridge_live_feed_max_event_age_s", 900.0))
    max_state_age_s = float(getattr(args, "bridge_live_feed_max_state_age_s", 3.0))
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    scanned_matching_rows = 0
    skipped_old_rows = 0
    skipped_non_buy_rows = 0
    for row in rows:
        if row.get("event") != "wallet_copy_wallet_event":
            continue
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        wallet = _norm_wallet(row.get("source_wallet") or raw.get("proxyWallet"))
        if wallet not in wallet_set:
            continue
        action = str(row.get("action") or raw.get("side") or "").upper()
        if action != "BUY":
            skipped_non_buy_rows += 1
            continue
        observed_ts = _float_value(row.get("observed_ts") or row.get("received_at_s") or row.get("captured_at_s"))
        event_ts = _float_value(row.get("event_ts") or raw.get("timestamp"))
        row_ts = observed_ts or event_ts
        if max_event_age_s > 0 and row_ts > 0 and now_s - row_ts > max_event_age_s:
            skipped_old_rows += 1
            continue
        market_slug = str(row.get("market_slug") or row.get("event_slug") or raw.get("slug") or raw.get("eventSlug") or "")
        condition_id = str(row.get("condition_id") or raw.get("conditionId") or "")
        outcome = str(row.get("outcome") or raw.get("outcome") or "")
        token_id = str(row.get("token_id") or raw.get("asset") or raw.get("tokenId") or "")
        price = _float_value(row.get("price") if row.get("price") is not None else raw.get("price"))
        size = _float_value(row.get("size") if row.get("size") is not None else raw.get("size"))
        if not (market_slug or condition_id) or not outcome or not token_id or price <= 0 or size <= 0:
            continue
        scanned_matching_rows += 1
        key = (wallet, market_slug or condition_id, condition_id, outcome)
        group = groups.setdefault(
            key,
            {
                "wallet": wallet,
                "market_slug": market_slug,
                "condition_id": condition_id,
                "outcome": outcome,
                "events": 0,
                "source_shares": 0.0,
                "source_usd": 0.0,
                "latest_observed_ts": 0.0,
                "latest_event_ts": 0.0,
                "token_ids": set(),
            },
        )
        group["events"] += 1
        group["source_shares"] += size
        group["source_usd"] += price * size
        group["latest_observed_ts"] = max(float(group.get("latest_observed_ts") or 0.0), observed_ts or event_ts)
        group["latest_event_ts"] = max(float(group.get("latest_event_ts") or 0.0), event_ts or observed_ts)
        group["token_ids"].add(token_id)

    clob_truth = _clob_truth_by_asset(
        path=getattr(args, "bridge_clob_book_jsonl", ""),
        tail_bytes=int(getattr(args, "bridge_clob_book_tail_bytes", 64 * 1024 * 1024)),
        max_age_s=float(getattr(args, "bridge_clob_book_max_age_s", 900.0)),
        now_s=now_s,
    )
    public_groups: list[dict[str, Any]] = []
    fresh_wallets: set[str] = set()
    clob_truth_groups = 0
    for group in groups.values():
        token_ids = sorted(str(token_id) for token_id in group.get("token_ids") or [] if str(token_id))
        latest_observed_ts = float(group.get("latest_observed_ts") or 0.0)
        latest_observed_age_s = max(0.0, now_s - latest_observed_ts) if latest_observed_ts > 0 else None
        source_shares = float(group.get("source_shares") or 0.0)
        source_usd = float(group.get("source_usd") or 0.0)
        group_has_clob_truth = any(token_id in clob_truth for token_id in token_ids)
        if group_has_clob_truth:
            clob_truth_groups += 1
        if latest_observed_age_s is not None and latest_observed_age_s <= max_state_age_s:
            fresh_wallets.add(str(group.get("wallet") or ""))
        public_groups.append(
            {
                "wallet": group.get("wallet"),
                "market_slug": group.get("market_slug"),
                "condition_id": group.get("condition_id"),
                "outcome": group.get("outcome"),
                "events": int(group.get("events") or 0),
                "source_shares": round(source_shares, 6),
                "source_usd": round(source_usd, 6),
                "source_vwap": round(source_usd / source_shares, 8) if source_shares > 0 else None,
                "latest_observed_age_s": (
                    round(float(latest_observed_age_s), 6) if latest_observed_age_s is not None else None
                ),
                "token_ids": token_ids[:4],
                "has_clob_book_truth": group_has_clob_truth,
            }
        )
    public_groups.sort(
        key=lambda group: (
            group.get("latest_observed_age_s") if group.get("latest_observed_age_s") is not None else 10**9,
            -int(group.get("events") or 0),
            str(group.get("wallet") or ""),
        )
    )
    fresh_groups = [
        group
        for group in public_groups
        if group.get("latest_observed_age_s") is not None
        and float(group.get("latest_observed_age_s") or 0.0) <= max_state_age_s
    ]
    blockers: list[str] = []
    if scanned_matching_rows <= 0:
        blockers.append("bridge_live_feed_rows_missing")
    if not fresh_groups:
        blockers.append("bridge_live_feed_fresh_inventory_state_missing")
    if len(fresh_wallets) < 2:
        blockers.append("bridge_live_feed_multi_wallet_truth_missing")
    if clob_truth_groups <= 0:
        blockers.append("bridge_live_feed_clob_truth_missing")
    status = "PASS" if not blockers else "WATCH"
    return {
        **base,
        "status": status,
        "blockers": blockers,
        "live_feed_jsonl": str(live_feed_path),
        "clob_book_jsonl": str(getattr(args, "bridge_clob_book_jsonl", "") or ""),
        "scanned_rows": len(rows),
        "matching_buy_events": scanned_matching_rows,
        "skipped_old_rows": skipped_old_rows,
        "skipped_non_buy_rows": skipped_non_buy_rows,
        "inventory_window_groups": len(public_groups),
        "fresh_inventory_groups": len(fresh_groups),
        "fresh_wallet_count": len(fresh_wallets),
        "max_event_age_s": round(max_event_age_s, 6),
        "max_state_age_s": round(max_state_age_s, 6),
        "clob_truth_asset_count": len(clob_truth),
        "clob_truth_groups": clob_truth_groups,
        "candidate_specific_clob_truth_attached": bool(status == "PASS"),
        "clob_snapshot": clob_snapshot or {},
        "groups": public_groups[:20],
    }


def _cohort_wallets(row: dict[str, Any], *, max_wallets: int) -> list[str]:
    raw_wallets = row.get("selected_wallets") or row.get("wallets") or row.get("unique_wallets") or []
    wallets: list[str] = []
    if isinstance(raw_wallets, list):
        for value in raw_wallets:
            address = str(value or "").lower().strip()
            if address.startswith("0x") and address not in wallets:
                wallets.append(address)
    return wallets[: max(1, int(max_wallets))]


def _probe_cohort_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, tuple[str, ...]]] = set()
    for key in (
        "selected_cohorts",
        "recent_live_activity_cohorts",
        "selected_recent_live_activity_cohorts",
        "development_program_bridge_cohorts",
        "selected_development_program_bridge_cohorts",
    ):
        values = payload.get(key) if isinstance(payload.get(key), list) else []
        for row in values:
            if not isinstance(row, dict):
                continue
            raw_wallets = row.get("selected_wallets") or row.get("wallets") or row.get("unique_wallets") or []
            identity_wallets = tuple(
                dict.fromkeys(
                    str(value or "").lower().strip()
                    for value in raw_wallets
                    if str(value or "").lower().strip().startswith("0x")
                )
            )
            identity = (
                str(row.get("source") or ""),
                str(row.get("candidate_id") or ""),
                str(row.get("rank") or ""),
                identity_wallets,
            )
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(row)
    return rows


def _next_probe_cohort_index(args: argparse.Namespace) -> int:
    payload = load_json(getattr(args, "output", ""), default={})
    if not isinstance(payload, dict):
        return 0
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    try:
        return max(0, int(summary.get("next_exact_cohort_probe_index") or 0))
    except (TypeError, ValueError):
        return 0


def _has_probe_rotation_state(args: argparse.Namespace) -> bool:
    path = Path(str(getattr(args, "output", "") or ""))
    if not path.exists():
        return False
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return False
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return "next_exact_cohort_probe_index" in summary


def _previous_probe_needs_bridge_burnin(args: argparse.Namespace) -> bool:
    path = Path(str(getattr(args, "output", "") or ""))
    if not path.exists():
        return False
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return False
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    latest_tracker = summary.get("latest_tracker") if isinstance(summary.get("latest_tracker"), dict) else {}
    blockers = {str(blocker) for blocker in payload.get("blockers") or []}
    tracker_blockers = {str(blocker) for blocker in latest_tracker.get("hot_path_adaptive_blockers") or []}
    no_current_signal = (
        int(latest_tracker.get("hot_path_current_poll_moves") or 0) <= 0
        and int(latest_tracker.get("new_wallet_events") or 0) <= 0
        and int(latest_tracker.get("required_buy_copy_events") or 0) <= 0
    )
    repeated_watch = bool(
        blockers.intersection({"no_adaptive_pass_signal", "no_current_poll_moves"})
        or tracker_blockers.intersection({"no_current_poll_moves", "no_rolling_window_moves"})
    )
    return no_current_signal and repeated_watch


def _selected_probe_cohort(args: argparse.Namespace, *, tick_index: int, max_wallets: int) -> dict[str, Any]:
    if not bool(getattr(args, "exact_cohort_probe", True)):
        return {"status": "DISABLED", "wallets": []}
    payload = load_json(getattr(args, "active_hotlane_state", ""), default={})
    cohorts = _probe_cohort_rows(payload) if isinstance(payload, dict) else []
    if not isinstance(cohorts, list) or not cohorts:
        return {"status": "MISSING", "wallets": []}
    eligible: list[dict[str, Any]] = []
    recent_live_eligible: list[dict[str, Any]] = []
    bridge_eligible: list[dict[str, Any]] = []
    for row in cohorts:
        if not isinstance(row, dict):
            continue
        wallets = _cohort_wallets(row, max_wallets=max_wallets)
        if len(wallets) < 2:
            continue
        cohort = {**row, "wallets": wallets}
        eligible.append(cohort)
        source = str(row.get("source") or "")
        selected_source = str(row.get("selected_source") or "")
        if (
            source == "recent_live_activity"
            or selected_source == "recent_live_activity_current_poll_probe"
            or row.get("role") == "current_poll_recent_activity_cohort_only_not_live_admission_truth"
        ):
            recent_live_eligible.append(cohort)
        if (
            source == "development_program_bridge_candidate"
            or selected_source == "development_program_bridge_current_poll_inventory_burnin"
            or row.get("development_program_bridge_focus") is True
        ):
            bridge_eligible.append(cohort)
    if not eligible:
        return {"status": "NO_MULTI_WALLET_COHORT", "wallets": []}
    # Bootstrap the development bridge when there is no previous probe rotation
    # state. If the prior tick already proved a no-current-poll/no-proof loop,
    # return to bridge burn-in instead of repeating a short recent-live slice.
    # Bridge cohorts still stay ahead of stale tracker-time replay.
    if bridge_eligible and (not _has_probe_rotation_state(args) or _previous_probe_needs_bridge_burnin(args)):
        rotation_pool = bridge_eligible
    else:
        rotation_pool = recent_live_eligible or bridge_eligible or eligible
    selected_index = (_next_probe_cohort_index(args) + int(tick_index)) % len(rotation_pool)
    selected = rotation_pool[selected_index]
    return {
        "status": "PASS",
        "wallets": selected["wallets"],
        "candidate_id": selected.get("candidate_id"),
        "market_slug": selected.get("market_slug"),
        "outcome": selected.get("outcome"),
        "source": selected.get("source"),
        "role": selected.get("role"),
        "rank": selected.get("rank"),
        "cohort_index": selected_index,
        "next_cohort_index": (selected_index + 1) % len(rotation_pool),
        "eligible_cohorts": len(rotation_pool),
        "all_eligible_cohorts": len(eligible),
        "recent_live_eligible_cohorts": len(recent_live_eligible),
        "bridge_eligible_cohorts": len(bridge_eligible),
    }


def _development_program_bridge_probe_available(args: argparse.Namespace) -> bool:
    if not bool(getattr(args, "exact_cohort_probe", True)):
        return False
    payload = load_json(getattr(args, "active_hotlane_state", ""), default={})
    if not isinstance(payload, dict):
        return False
    for row in _probe_cohort_rows(payload):
        if not isinstance(row, dict):
            continue
        source = str(row.get("source") or "")
        selected_source = str(row.get("selected_source") or "")
        if (
            source != "development_program_bridge_candidate"
            and selected_source != "development_program_bridge_current_poll_inventory_burnin"
            and row.get("development_program_bridge_focus") is not True
        ):
            continue
        wallets = _cohort_wallets(
            row,
            max_wallets=max(2, int(getattr(args, "cohort_probe_max_wallets", 12))),
        )
        if len(wallets) >= 2:
            return True
    return False


def _development_program_bridge_probe_should_run_first(args: argparse.Namespace) -> bool:
    return bool(
        _development_program_bridge_probe_available(args)
        and (
            bool(getattr(args, "force_development_bridge_probe", False))
            or _previous_probe_needs_bridge_burnin(args)
        )
    )


def _development_bridge_target_candidate_id(args: argparse.Namespace) -> str:
    payload = load_json(getattr(args, "profit_policy_state", ""), default={})
    if not isinstance(payload, dict):
        return ""
    bridge = payload.get("development_program_bridge")
    if not isinstance(bridge, dict) or bridge.get("active") is not True:
        return ""
    target = bridge.get("target_inventory") if isinstance(bridge.get("target_inventory"), dict) else {}
    return str(target.get("candidate_id") or "")


def _active_hotlane_bridge_candidate_ids(args: argparse.Namespace) -> list[str]:
    payload = load_json(getattr(args, "active_hotlane_state", ""), default={})
    if not isinstance(payload, dict):
        return []
    candidate_ids: set[str] = set()

    def add(value: Any) -> None:
        candidate_id = str(value or "")
        if candidate_id:
            candidate_ids.add(candidate_id)

    summary = (
        payload.get("development_program_bridge_summary")
        if isinstance(payload.get("development_program_bridge_summary"), dict)
        else {}
    )
    add(summary.get("development_program_bridge_candidate_id"))
    for key in (
        "selected_development_program_bridge_cohorts",
        "development_program_bridge_cohorts",
        "selected_cohorts",
    ):
        rows = payload.get(key) if isinstance(payload.get(key), list) else []
        for row in rows:
            if not isinstance(row, dict) or not _is_development_bridge_probe(row):
                continue
            add(row.get("candidate_id"))
            target = (
                row.get("development_program_target_inventory")
                if isinstance(row.get("development_program_target_inventory"), dict)
                else {}
            )
            add(target.get("candidate_id"))
    return sorted(candidate_ids)


def _development_bridge_hotlane_refresh_plan(args: argparse.Namespace) -> dict[str, Any]:
    target_candidate_id = _development_bridge_target_candidate_id(args)
    active_candidate_ids = _active_hotlane_bridge_candidate_ids(args)
    if not target_candidate_id:
        status = "NO_DEVELOPMENT_BRIDGE_TARGET"
        refresh_required = False
    elif active_candidate_ids == [target_candidate_id]:
        status = "UP_TO_DATE"
        refresh_required = False
    else:
        status = "STALE_ACTIVE_HOTLANE_BRIDGE_TARGET"
        refresh_required = True
    return {
        "status": status,
        "refresh_required": refresh_required,
        "target_candidate_id": target_candidate_id or None,
        "active_hotlane_candidate_ids": active_candidate_ids,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _refresh_development_bridge_active_hotlane_if_stale(args: argparse.Namespace) -> dict[str, Any]:
    plan = _development_bridge_hotlane_refresh_plan(args)
    if not plan.get("refresh_required"):
        return plan
    if not bool(getattr(args, "auto_refresh_stale_bridge_hotlane", True)):
        return {
            **plan,
            "refresh_success": False,
            "refresh_skipped_reason": "auto_refresh_stale_bridge_hotlane_disabled",
        }
    argv = [
        PYTHON,
        "scripts/select_wallet_copy_active_hotlane.py",
        "--registry",
        str(getattr(args, "active_hotlane_source_registry", "configs/wallet_copy/wallets.json")),
        "--active-hotlane-live-tracking-state",
        str(getattr(args, "tracker_state", "data/research/wallet_copy_active_hotlane_live_tracking_state.json")),
        "--history-state",
        str(getattr(args, "seed_history_state", "data/research/wallet_copy_history_state.json")),
        "--profit-state",
        str(getattr(args, "profit_policy_state", "data/research/wallet_copy_profit_engine_state.json")),
        "--strategy-direction-state",
        str(getattr(args, "strategy_direction_state", "data/research/wallet_copy_strategy_direction_state.json")),
        "--adaptive-state",
        str(getattr(args, "adaptive_state", "data/research/wallet_copy_adaptive_bot_state.json")),
        "--hotlane-tick-state",
        str(getattr(args, "output", "data/research/wallet_copy_hotlane_tick_state.json")),
        "--output-registry",
        str(getattr(args, "registry", "data/research/wallet_copy_active_hotlane_registry.json")),
        "--output",
        str(getattr(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json")),
    ]
    result = _run_command(
        name="refresh_stale_development_bridge_active_hotlane",
        argv=argv,
        acceptable_returncodes=(0, 2),
        timeout_s=min(30.0, max(1.0, float(getattr(args, "command_timeout_s", 60.0)))),
    )
    post_refresh = _development_bridge_hotlane_refresh_plan(args)
    refresh_success = bool(result.get("ok") and not post_refresh.get("refresh_required"))
    return {
        **plan,
        "refresh_command": result,
        "post_refresh_status": post_refresh.get("status"),
        "post_refresh_candidate_ids": post_refresh.get("active_hotlane_candidate_ids"),
        "refresh_success": refresh_success,
    }


def _aggregate_counter(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        values = (row.get("tracker") or {}).get(field)
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            counts[str(key)] += int(value or 0)
    return dict(sorted(counts.items()))


def _aggregate_adaptive_freshness_counter(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        adaptive = row.get("adaptive") if isinstance(row.get("adaptive"), dict) else {}
        diagnostics = adaptive.get("freshness_diagnostics") if isinstance(adaptive.get("freshness_diagnostics"), dict) else {}
        values = adaptive.get(field) if isinstance(adaptive.get(field), dict) else diagnostics.get(field)
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            counts[str(key)] += int(value or 0)
    return dict(sorted(counts.items()))


def _top_adaptive_freshness_transitions(
    rows: list[dict[str, Any]],
    *,
    limit: int = 20,
    max_observed_event_age_s: float | None = None,
) -> list[dict[str, Any]]:
    transition_rows: list[dict[str, Any]] = []
    for row in rows:
        adaptive = row.get("adaptive") if isinstance(row.get("adaptive"), dict) else {}
        diagnostics = adaptive.get("freshness_diagnostics") if isinstance(adaptive.get("freshness_diagnostics"), dict) else {}
        values = adaptive.get("freshness_transition_rows")
        if not isinstance(values, list):
            values = diagnostics.get("freshness_transition_rows")
        if not isinstance(values, list):
            continue
        measurement = str(row.get("active_measurement") or "slice")
        for value in values:
            if not isinstance(value, dict):
                continue
            normalized = {"active_measurement": measurement, **value}
            try:
                tracker_age = normalized.get("tracker_event_age_s")
                runtime_age = normalized.get("runtime_event_age_s")
                if (
                    normalized.get("runtime_minus_tracker_age_s") is None
                    and tracker_age is not None
                    and runtime_age is not None
                ):
                    normalized["runtime_minus_tracker_age_s"] = round(
                        max(0.0, float(runtime_age) - float(tracker_age)),
                        6,
                    )
                if (
                    normalized.get("tracker_latency_budget_remaining_s") is None
                    and max_observed_event_age_s is not None
                    and tracker_age is not None
                ):
                    normalized["tracker_latency_budget_remaining_s"] = round(
                        float(max_observed_event_age_s) - float(tracker_age),
                        6,
                    )
            except (TypeError, ValueError):
                pass
            transition_rows.append(normalized)
    transition_rows.sort(
        key=lambda item: (
            str(item.get("transition_reason") or ""),
            str(item.get("source_wallet") or ""),
            str(item.get("market_slug") or ""),
            str(item.get("outcome") or ""),
        )
    )
    return transition_rows[:limit]


def _child_command_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    tracker_started = 0
    adaptive_started = 0
    tracker_ok = 0
    adaptive_ok = 0
    cohort_tracker_started = 0
    cohort_adaptive_started = 0
    cohort_tracker_ok = 0
    cohort_adaptive_ok = 0

    for row in rows:
        tracker_command = row.get("tracker_command")
        if isinstance(tracker_command, dict):
            tracker_started += 1
            tracker_ok += int(bool(tracker_command.get("ok")))
        adaptive_command = row.get("adaptive_command")
        if isinstance(adaptive_command, dict):
            adaptive_started += 1
            adaptive_ok += int(bool(adaptive_command.get("ok")))
        cohort_tracker_command = row.get("cohort_probe_tracker_command")
        if isinstance(cohort_tracker_command, dict):
            tracker_started += 1
            cohort_tracker_started += 1
            ok = int(bool(cohort_tracker_command.get("ok")))
            tracker_ok += ok
            cohort_tracker_ok += ok
        cohort_adaptive_command = row.get("cohort_probe_adaptive_command")
        if isinstance(cohort_adaptive_command, dict):
            adaptive_started += 1
            cohort_adaptive_started += 1
            ok = int(bool(cohort_adaptive_command.get("ok")))
            adaptive_ok += ok
            cohort_adaptive_ok += ok

    return {
        "tracker_children_started": tracker_started,
        "adaptive_children_started": adaptive_started,
        "tracker_children_ok": tracker_ok,
        "adaptive_children_ok": adaptive_ok,
        "cohort_probe_tracker_children_started": cohort_tracker_started,
        "cohort_probe_adaptive_children_started": cohort_adaptive_started,
        "cohort_probe_tracker_children_ok": cohort_tracker_ok,
        "cohort_probe_adaptive_children_ok": cohort_adaptive_ok,
    }


def _tracker_strength(row: dict[str, Any]) -> tuple[int, ...]:
    return (
        int(str(row.get("hot_path_adaptive_status") or "") == "PASS"),
        int(row.get("hot_path_pass_signals") or 0),
        int(row.get("hot_path_intents_created") or 0),
        int(row.get("hot_path_inventory_intents_created") or 0),
        int(row.get("hot_path_inventory_filled_orders") or 0),
        int(row.get("hot_path_single_wallet_exact_copy_intents_created") or 0),
        int(row.get("hot_path_single_wallet_exact_copy_filled_orders") or 0),
        int(row.get("hot_path_tracker_time_replay_pass_signals") or 0),
        int(row.get("hot_path_tracker_time_replay_intents_created") or 0),
        int(row.get("hot_path_tracker_time_replay_filled_orders") or 0),
        int(row.get("hot_path_runtime_eligible_wallets") or 0),
        int(row.get("hot_path_runtime_fresh_buy_events_le_cap") or 0),
    )


def _cohort_probe_decision(
    *,
    args: argparse.Namespace,
    tracker: dict[str, Any],
    adaptive: dict[str, Any],
) -> tuple[bool, str]:
    if not bool(args.cohort_probe_on_single_wallet):
        return False, "none"
    tracker_blockers = [str(item) for item in tracker.get("hot_path_adaptive_blockers") or []]
    blocker_counts = (
        tracker.get("hot_path_runtime_signal_blocker_counts")
        if isinstance(tracker.get("hot_path_runtime_signal_blocker_counts"), dict)
        else {}
    )
    partial_consensus_blocked = bool(
        int(tracker.get("hot_path_runtime_eligible_wallets") or 0) >= 2
        and int(tracker.get("hot_path_pass_signals") or 0) <= 0
        and (
            int(tracker.get("hot_path_runtime_inventory_research_candidates") or 0) > 0
            or any(
                int(blocker_counts.get(reason) or 0) > 0
                for reason in {
                    "insufficient_agreeing_wallets",
                    "directional_dominance_below_minimum",
                    "price_spread_too_wide",
                    "signal_score_below_minimum",
                }
            )
            or any(
                reason in tracker_blockers
                for reason in {
                    "runtime_inventory_candidate_research_only",
                    "no_hot_path_adaptive_pass_signal",
                }
            )
        )
    )
    if partial_consensus_blocked:
        return True, "partial_consensus_blocked"
    if (
        int(tracker.get("hot_path_runtime_fresh_buy_events_le_cap") or 0) > 0
        and int(tracker.get("hot_path_runtime_eligible_wallets") or 0) < 2
    ):
        return True, "single_wallet_runtime_fresh"
    tracker_time_replay_filled = int(adaptive.get("tracker_time_replay_filled_orders") or 0)
    tracker_time_replay_intents = int(adaptive.get("tracker_time_replay_intents") or 0)
    runtime_truth_missing = "tracker_time_replay_available_but_runtime_truth_missing" in {
        str(item) for item in adaptive.get("blockers") or []
    }
    if _development_program_bridge_probe_available(args):
        return True, "development_program_bridge_needs_current_poll_probe"
    if (
        tracker_time_replay_filled > 0
        and tracker_time_replay_filled >= tracker_time_replay_intents
        and int(adaptive.get("runtime_eligible_wallets") or 0) < 2
        and (
            bool(adaptive.get("source_feed_delayed"))
            or bool(adaptive.get("tracker_fresh_but_runtime_stale"))
            or runtime_truth_missing
        )
    ):
        return True, "tracker_time_replay_needs_current_poll_probe"
    return False, "none"


def _cohort_probe_can_run_after_base_commands(
    *,
    reason: str,
    tracker_result: dict[str, Any],
    adaptive_result: dict[str, Any],
) -> bool:
    if not adaptive_result.get("ok"):
        return False
    if tracker_result.get("ok"):
        return True
    if reason != "development_program_bridge_needs_current_poll_probe":
        return False
    returncode = int(tracker_result.get("returncode") or 0)
    return returncode in {2, 124}


def _source_route_blocked_state(args: argparse.Namespace) -> dict[str, Any] | None:
    probe_blocker = source_route_probe_progress_blocker(
        getattr(args, "autonomous_repair_command_progress_state", None)
    )
    route = load_json(getattr(args, "source_route_state", ""), default={})
    if probe_blocker:
        status = "SOURCE_ROUTE_PROBE_RUNNING"
        route = {
            **probe_blocker,
            "route_class_counts": {},
            "source_proxy_configured": None,
            "external_route_required": None,
            "next_action": "pause heavy hot-lane children until the fresh source-route probe finishes",
        }
    elif not isinstance(route, dict):
        route = {}
        status = "SOURCE_ROUTE_UNKNOWN"
    else:
        status = str(route.get("status") or "SOURCE_ROUTE_UNKNOWN")
    if source_route_allows_measurement(status):
        return None
    preview_wallets = max(1, int(getattr(args, "cohort_probe_max_wallets", 4)))
    probe_preview = _selected_probe_cohort(args, tick_index=0, max_wallets=preview_wallets)
    if probe_preview.get("status") == "PASS":
        probe_preview = {
            **probe_preview,
            "poll_blocked_reason": f"source_route_{status.lower()}",
            "role": probe_preview.get("role") or "route_blocked_exact_cohort_poll_preview",
        }
    return {
        "kind": "wallet_copy_hotlane_tick_state",
        "status": "SOURCE_ROUTE_BLOCKED_SLEEP",
        "blockers": [f"source_route_{status.lower()}"],
        "source_route": {
            "status": status,
            "route_class_counts": route.get("route_class_counts") or {},
            "source_proxy_configured": route.get("source_proxy_configured"),
            "external_route_required": route.get("external_route_required"),
            "generated_at": route.get("generated_at"),
            "next_action": route.get("next_action"),
        },
        "summary": {
            "ticks_requested": max(1, int(getattr(args, "ticks", 1))),
            "ticks_completed": 0,
            "source_route_status": status,
            "tracker_children_started": 0,
            "adaptive_children_started": 0,
            "blocked_cohort_probe_preview_status": probe_preview.get("status"),
            "blocked_cohort_probe_preview_wallets": len(probe_preview.get("wallets") or []),
        },
        "blocked_cohort_probe_preview": probe_preview,
        "ticks": [],
        "paper_only": True,
        "live_orders_allowed": False,
    }


def main() -> int:
    args = parse_args()
    bridge_hotlane_refresh = _refresh_development_bridge_active_hotlane_if_stale(args)
    if bridge_hotlane_refresh.get("refresh_required") and not bridge_hotlane_refresh.get("refresh_success"):
        blockers = ["development_bridge_active_hotlane_target_stale"]
        state = {
            "kind": "wallet_copy_hotlane_tick_state",
            "generated_at": utc_now_iso(),
            "status": "WATCH",
            "blockers": blockers,
            "development_bridge_active_hotlane_refresh": bridge_hotlane_refresh,
            "summary": {
                "ticks_requested": max(1, int(getattr(args, "ticks", 1))),
                "ticks_completed": 0,
                "development_bridge_active_hotlane_refresh": bridge_hotlane_refresh,
            },
            "ticks": [],
            "paper_only": True,
            "live_orders_allowed": False,
        }
        atomic_write_json(args.output, state)
        print(
            json.dumps(
                {
                    "state": str(args.output),
                    "status": state["status"],
                    "blockers": blockers,
                    "summary": state["summary"],
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    route_blocked = _source_route_blocked_state(args)
    if route_blocked:
        route_blocked["development_bridge_active_hotlane_refresh"] = bridge_hotlane_refresh
        route_blocked["summary"]["development_bridge_active_hotlane_refresh"] = bridge_hotlane_refresh
        live_feed_bridge_clob_snapshot = _capture_live_feed_bridge_clob_truth(
            args=args,
            current_poll_bridge_burnin={
                "status": "NOT_RUN",
                "blockers": ["current_poll_inventory_bridge_burnin_not_run_source_route_blocked"],
            },
        )
        live_feed_bridge_burnin = _live_feed_inventory_bridge_burnin_summary(
            args=args,
            current_poll_bridge_burnin={
                "status": "NOT_RUN",
                "blockers": ["current_poll_inventory_bridge_burnin_not_run_source_route_blocked"],
            },
            clob_snapshot=live_feed_bridge_clob_snapshot,
        )
        window_indexed_bridge_burnin = _window_indexed_inventory_bridge_burnin_summary(
            args=args,
            current_poll_bridge_burnin={
                "status": "NOT_RUN",
                "blockers": ["current_poll_inventory_bridge_burnin_not_run_source_route_blocked"],
            },
        )
        route_blocked["live_feed_bridge_clob_snapshot"] = live_feed_bridge_clob_snapshot
        route_blocked["live_feed_inventory_bridge_burnin"] = live_feed_bridge_burnin
        route_blocked["window_indexed_inventory_bridge_burnin"] = window_indexed_bridge_burnin
        route_blocked["summary"]["live_feed_bridge_clob_snapshot"] = live_feed_bridge_clob_snapshot
        route_blocked["summary"]["live_feed_inventory_bridge_burnin"] = live_feed_bridge_burnin
        route_blocked["summary"]["window_indexed_inventory_bridge_burnin"] = window_indexed_bridge_burnin
        atomic_write_json(args.output, route_blocked)
        print(
            json.dumps(
                {
                    "state": str(args.output),
                    "status": route_blocked["status"],
                    "blockers": route_blocked["blockers"],
                    "summary": route_blocked["summary"],
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    ticks_requested = max(1, int(args.ticks))
    tick_rows: list[dict[str, Any]] = []
    status = "WATCH"
    blockers = ["no_adaptive_pass_signal"]
    started = time.time()
    for tick_index in range(ticks_requested):
        if _development_program_bridge_probe_should_run_first(args):
            registry_wallets = _registry_wallet_count(args.registry)
            cohort_probe_wallets = max(
                int(args.wallets_per_tick),
                min(max(1, int(args.cohort_probe_max_wallets)), max(1, registry_wallets)),
            )
            exact_probe = _selected_probe_cohort(
                args,
                tick_index=tick_index,
                max_wallets=cohort_probe_wallets,
            )
            exact_probe_wallets = exact_probe.get("wallets") if isinstance(exact_probe.get("wallets"), list) else []
            if len(exact_probe_wallets) >= 2:
                cohort_probe_wallets = len(exact_probe_wallets)
            if len(exact_probe_wallets) >= 2:
                probe_args = _cohort_probe_args(args)
                child_timeout_s = _bounded_child_timeout_s(
                    started=started,
                    max_runtime_s=float(args.max_runtime_s),
                    command_timeout_s=float(args.command_timeout_s),
                )
                probe_tracker_batch_started_at = time.time()
                if child_timeout_s <= 0.0:
                    blockers = ["hotlane_tick_runtime_exhausted"]
                    break
                probe_tracker_result = _run_command(
                    name="active_hotlane_bridge_first_cohort_probe_tracker_tick",
                    argv=build_tracker_argv(
                        probe_args,
                        max_wallets_override=cohort_probe_wallets,
                        parallel_wallet_fetches_override=max(
                            int(probe_args.parallel_wallet_fetches),
                            cohort_probe_wallets,
                        ),
                        limit_override=max(int(args.limit), int(args.cohort_probe_limit)),
                        pages_override=max(int(args.pages), int(args.cohort_probe_pages)),
                        wallet_addresses_override=exact_probe_wallets,
                        max_runtime_override_s=child_timeout_s,
                        force_direct_source_route=True,
                    ),
                    acceptable_returncodes=(0, 2),
                    timeout_s=child_timeout_s,
                    env=_bridge_direct_source_env(),
                )
                child_timeout_s = _bounded_child_timeout_s(
                    started=started,
                    max_runtime_s=float(args.max_runtime_s),
                    command_timeout_s=float(args.command_timeout_s),
                )
                if child_timeout_s <= 0.0:
                    probe_adaptive_result = {
                        "name": "adaptive_after_bridge_first_cohort_probe_tick",
                        "argv": build_adaptive_argv(
                            probe_args,
                            min_move_generated_at_ts=probe_tracker_batch_started_at,
                        ),
                        "returncode": 124,
                        "acceptable_returncodes": [0, 2],
                        "ok": False,
                        "duration_s": 0.0,
                        "stdout_tail": "",
                        "stderr_tail": "timeout before adaptive_after_bridge_first_cohort_probe_tick: parent max runtime exhausted",
                    }
                    blockers = ["hotlane_tick_runtime_exhausted"]
                else:
                    probe_adaptive_result = _run_command(
                        name="adaptive_after_bridge_first_cohort_probe_tick",
                        argv=build_adaptive_argv(
                            probe_args,
                            min_move_generated_at_ts=probe_tracker_batch_started_at,
                        ),
                        acceptable_returncodes=(0, 2),
                        timeout_s=child_timeout_s,
                    )
                probe_tracker = _tracker_snapshot(probe_args.tracker_state)
                probe_adaptive = _adaptive_snapshot(probe_args.adaptive_state)
                tick_rows.append(
                    {
                        "tick": tick_index + 1,
                        "generated_at": utc_now_iso(),
                        "active_measurement": "cohort_probe",
                        "tracker_command": probe_tracker_result,
                        "adaptive_command": probe_adaptive_result,
                        "cohort_probe_tracker_command": probe_tracker_result,
                        "cohort_probe_adaptive_command": probe_adaptive_result,
                        "cohort_probe_triggered": True,
                        "cohort_probe_reason": "development_program_bridge_first_current_poll_probe",
                        "exact_cohort_probe": exact_probe,
                        "tracker": probe_tracker,
                        "adaptive": probe_adaptive,
                    }
                )
                if blockers == ["hotlane_tick_runtime_exhausted"]:
                    break
                continue
        child_timeout_s = _bounded_child_timeout_s(
            started=started,
            max_runtime_s=float(args.max_runtime_s),
            command_timeout_s=float(args.command_timeout_s),
        )
        if child_timeout_s <= 0.0:
            blockers = ["hotlane_tick_runtime_exhausted"]
            break
        tracker_batch_started_at = time.time()
        tracker_result = _run_command(
            name="active_hotlane_tracker_tick",
            argv=build_tracker_argv(args),
            acceptable_returncodes=(0, 2),
            timeout_s=child_timeout_s,
        )
        child_timeout_s = _bounded_child_timeout_s(
            started=started,
            max_runtime_s=float(args.max_runtime_s),
            command_timeout_s=float(args.command_timeout_s),
        )
        if child_timeout_s <= 0.0:
            tick_rows.append(
                {
                    "tick": tick_index + 1,
                    "generated_at": utc_now_iso(),
                    "tracker_command": tracker_result,
                    "adaptive_command": {
                        "name": "adaptive_after_tracker_tick",
                        "argv": build_adaptive_argv(args, min_move_generated_at_ts=tracker_batch_started_at),
                        "returncode": 124,
                        "acceptable_returncodes": [0, 2],
                        "ok": False,
                        "duration_s": 0.0,
                        "stdout_tail": "",
                        "stderr_tail": "timeout before adaptive_after_tracker_tick: parent max runtime exhausted",
                    },
                    "cohort_probe_triggered": False,
                    "cohort_probe_reason": "parent_max_runtime_exhausted",
                    "tracker": _tracker_snapshot(args.tracker_state),
                    "adaptive": _adaptive_snapshot(args.adaptive_state),
                }
            )
            blockers = ["hotlane_tick_runtime_exhausted"]
            break
        adaptive_result = _run_command(
            name="adaptive_after_tracker_tick",
            argv=build_adaptive_argv(args, min_move_generated_at_ts=tracker_batch_started_at),
            acceptable_returncodes=(0, 2),
            timeout_s=child_timeout_s,
        )
        tracker = _tracker_snapshot(args.tracker_state)
        adaptive = _adaptive_snapshot(args.adaptive_state)
        cohort_probe_triggered, cohort_probe_reason = _cohort_probe_decision(
            args=args,
            tracker=tracker,
            adaptive=adaptive,
        )
        row = {
            "tick": tick_index + 1,
            "generated_at": utc_now_iso(),
            "tracker_command": tracker_result,
            "adaptive_command": adaptive_result,
            "cohort_probe_triggered": cohort_probe_triggered,
            "cohort_probe_reason": cohort_probe_reason,
            "tracker": tracker,
            "adaptive": adaptive,
        }
        if cohort_probe_triggered and _cohort_probe_can_run_after_base_commands(
            reason=cohort_probe_reason,
            tracker_result=tracker_result,
            adaptive_result=adaptive_result,
        ):
            registry_wallets = _registry_wallet_count(args.registry)
            cohort_probe_wallets = max(
                int(args.wallets_per_tick),
                min(max(1, int(args.cohort_probe_max_wallets)), max(1, registry_wallets)),
            )
            exact_probe = _selected_probe_cohort(
                args,
                tick_index=tick_index,
                max_wallets=cohort_probe_wallets,
            )
            exact_probe_wallets = exact_probe.get("wallets") if isinstance(exact_probe.get("wallets"), list) else []
            if len(exact_probe_wallets) >= 2:
                cohort_probe_wallets = len(exact_probe_wallets)
            cohort_probe_limit = max(int(args.limit), int(args.cohort_probe_limit))
            cohort_probe_pages = max(int(args.pages), int(args.cohort_probe_pages))
            bridge_probe_direct_source = _is_development_bridge_probe(exact_probe)
            probe_tracker_batch_started_at = time.time()
            probe_args = _cohort_probe_args(args)
            child_timeout_s = _bounded_child_timeout_s(
                started=started,
                max_runtime_s=float(args.max_runtime_s),
                command_timeout_s=float(args.command_timeout_s),
            )
            if child_timeout_s <= 0.0:
                probe_tracker_result = {
                    "name": "active_hotlane_cohort_probe_tracker_tick",
                    "argv": build_tracker_argv(
                        probe_args,
                        max_wallets_override=cohort_probe_wallets,
                        parallel_wallet_fetches_override=max(
                            int(probe_args.parallel_wallet_fetches),
                            cohort_probe_wallets,
                        ),
                        limit_override=cohort_probe_limit,
                        pages_override=cohort_probe_pages,
                        wallet_addresses_override=exact_probe_wallets,
                        max_runtime_override_s=child_timeout_s,
                        force_direct_source_route=bridge_probe_direct_source,
                    ),
                    "returncode": 124,
                    "acceptable_returncodes": [0, 2],
                    "ok": False,
                    "duration_s": 0.0,
                    "stdout_tail": "",
                    "stderr_tail": "timeout before active_hotlane_cohort_probe_tracker_tick: parent max runtime exhausted",
                }
                probe_adaptive_result = {
                    "name": "adaptive_after_cohort_probe_tick",
                    "argv": build_adaptive_argv(probe_args, min_move_generated_at_ts=probe_tracker_batch_started_at),
                    "returncode": 124,
                    "acceptable_returncodes": [0, 2],
                    "ok": False,
                    "duration_s": 0.0,
                    "stdout_tail": "",
                    "stderr_tail": "timeout before adaptive_after_cohort_probe_tick: parent max runtime exhausted",
                }
                probe_tracker = _tracker_snapshot(probe_args.tracker_state)
                probe_adaptive = _adaptive_snapshot(probe_args.adaptive_state)
                blockers = ["hotlane_tick_runtime_exhausted"]
            else:
                probe_tracker_result = _run_command(
                    name="active_hotlane_cohort_probe_tracker_tick",
                    argv=build_tracker_argv(
                        probe_args,
                        max_wallets_override=cohort_probe_wallets,
                        parallel_wallet_fetches_override=max(
                            int(probe_args.parallel_wallet_fetches),
                            cohort_probe_wallets,
                        ),
                        limit_override=cohort_probe_limit,
                        pages_override=cohort_probe_pages,
                        wallet_addresses_override=exact_probe_wallets,
                        max_runtime_override_s=child_timeout_s,
                        force_direct_source_route=bridge_probe_direct_source,
                    ),
                    acceptable_returncodes=(0, 2),
                    timeout_s=child_timeout_s,
                    env=_bridge_direct_source_env() if bridge_probe_direct_source else None,
                )
                child_timeout_s = _bounded_child_timeout_s(
                    started=started,
                    max_runtime_s=float(args.max_runtime_s),
                    command_timeout_s=float(args.command_timeout_s),
                )
                if child_timeout_s <= 0.0:
                    probe_adaptive_result = {
                        "name": "adaptive_after_cohort_probe_tick",
                        "argv": build_adaptive_argv(
                            probe_args,
                            min_move_generated_at_ts=probe_tracker_batch_started_at,
                        ),
                        "returncode": 124,
                        "acceptable_returncodes": [0, 2],
                        "ok": False,
                        "duration_s": 0.0,
                        "stdout_tail": "",
                        "stderr_tail": "timeout before adaptive_after_cohort_probe_tick: parent max runtime exhausted",
                    }
                    blockers = ["hotlane_tick_runtime_exhausted"]
                else:
                    probe_adaptive_result = _run_command(
                        name="adaptive_after_cohort_probe_tick",
                        argv=build_adaptive_argv(probe_args, min_move_generated_at_ts=probe_tracker_batch_started_at),
                        acceptable_returncodes=(0, 2),
                        timeout_s=child_timeout_s,
                    )
                probe_tracker = _tracker_snapshot(probe_args.tracker_state)
                probe_adaptive = _adaptive_snapshot(probe_args.adaptive_state)
            row.update(
                {
                    "active_measurement": "cohort_probe",
                    "cohort_probe_tracker_state": probe_args.tracker_state,
                    "cohort_probe_tracker_event_log": probe_args.tracker_event_log,
                    "cohort_probe_adaptive_state": probe_args.adaptive_state,
                    "cohort_probe_wallets": cohort_probe_wallets,
                    "cohort_probe_limit": cohort_probe_limit,
                    "cohort_probe_pages": cohort_probe_pages,
                    "exact_cohort_probe": exact_probe,
                    "cohort_probe_tracker_command": probe_tracker_result,
                    "cohort_probe_adaptive_command": probe_adaptive_result,
                    "cohort_probe_tracker": probe_tracker,
                    "cohort_probe_adaptive": probe_adaptive,
                }
            )
            if not probe_tracker_result["ok"] or not probe_adaptive_result["ok"]:
                tracker_result = probe_tracker_result
                adaptive_result = probe_adaptive_result
                tracker = probe_tracker
                adaptive = probe_adaptive
                row["tracker"] = tracker
                row["adaptive"] = adaptive
            elif _tracker_strength(probe_tracker) >= _tracker_strength(tracker):
                tracker_result = probe_tracker_result
                adaptive_result = probe_adaptive_result
                tracker = probe_tracker
                adaptive = probe_adaptive
                row["active_measurement"] = "cohort_probe"
                row["tracker"] = tracker
                row["adaptive"] = adaptive
        tick_rows.append(row)
        if not tracker_result["ok"] or not adaptive_result["ok"]:
            blockers = ["hotlane_tick_command_failed"]
            break
        if str(adaptive.get("status") or "") == "PASS" and int(adaptive.get("pass_signals") or 0) > 0:
            status = "PASS"
            blockers = []
            if bool(args.stop_on_pass):
                break
        if (
            status != "PASS"
            and str(tracker.get("hot_path_adaptive_status") or "") == "PASS"
            and (
                int(tracker.get("hot_path_pass_signals") or 0) > 0
                or int(tracker.get("hot_path_inventory_intents_created") or 0) > 0
            )
        ):
            blockers = []
            status = "PASS"
            if bool(args.stop_on_pass):
                break
        if (
            status != "PASS"
            and str(adaptive.get("single_wallet_exact_copy_status") or "") == "PASS"
            and int(adaptive.get("single_wallet_exact_copy_intents") or 0) > 0
            and int(adaptive.get("single_wallet_exact_copy_filled_orders") or 0)
            >= int(adaptive.get("single_wallet_exact_copy_intents") or 0)
            and int(adaptive.get("single_wallet_exact_copy_rejected_orders") or 0) == 0
        ):
            if int(adaptive.get("single_wallet_exact_copy_intents") or 0) >= MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS:
                blockers = []
                status = "PASS"
                if bool(args.stop_on_pass):
                    break
            else:
                blockers = ["single_wallet_live_promotion_required_buy_copy_events_below_10"]
                status = "WATCH"
        if tick_index < ticks_requested - 1:
            time.sleep(max(0.0, float(args.poll_gap_s)))

    latest_adaptive = tick_rows[-1]["adaptive"] if tick_rows else {}
    latest_tracker = tick_rows[-1]["tracker"] if tick_rows else {}
    if status != "PASS":
        best_runtime_fresh = max(
            int((row.get("adaptive") or {}).get("runtime_fresh_buy_events_le_cap") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_runtime_wallets = max(
            int((row.get("adaptive") or {}).get("runtime_eligible_wallets") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_runtime_inventory = max(
            int((row.get("adaptive") or {}).get("runtime_inventory_research_candidates") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_pass = max(
            int((row.get("tracker") or {}).get("hot_path_pass_signals") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_fresh = max(
            int((row.get("tracker") or {}).get("hot_path_runtime_fresh_buy_events_le_cap") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_wallets = max(
            int((row.get("tracker") or {}).get("hot_path_runtime_eligible_wallets") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_inventory = max(
            int((row.get("tracker") or {}).get("hot_path_runtime_inventory_research_candidates") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_inventory_intents = max(
            int((row.get("tracker") or {}).get("hot_path_inventory_intents_created") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_inventory_filled = max(
            int((row.get("tracker") or {}).get("hot_path_inventory_filled_orders") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_single_wallet_intents = max(
            int((row.get("tracker") or {}).get("hot_path_single_wallet_exact_copy_intents_created") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_single_wallet_filled = max(
            int((row.get("tracker") or {}).get("hot_path_single_wallet_exact_copy_filled_orders") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_hot_path_single_wallet_rejected = max(
            int((row.get("tracker") or {}).get("hot_path_single_wallet_exact_copy_rejected_orders") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_tracker_time_replay_pass = max(
            int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_pass_signals") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_tracker_time_replay_filled = max(
            int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_filled_orders") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_single_wallet_intents = max(
            int((row.get("adaptive") or {}).get("single_wallet_exact_copy_intents") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        best_single_wallet_filled = max(
            int((row.get("adaptive") or {}).get("single_wallet_exact_copy_filled_orders") or 0)
            for row in tick_rows
        ) if tick_rows else 0
        if best_hot_path_pass > 0:
            blockers = ["hot_path_adaptive_pass_in_prior_tick_not_final_state"]
        elif best_hot_path_inventory_intents > 0 and best_hot_path_inventory_filled >= best_hot_path_inventory_intents:
            blockers = ["hot_path_inventory_pass_in_prior_tick_not_final_state"]
        elif (
            best_hot_path_single_wallet_intents > 0
            and best_hot_path_single_wallet_filled >= best_hot_path_single_wallet_intents
            and best_hot_path_single_wallet_rejected == 0
        ):
            if best_hot_path_single_wallet_intents >= MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS:
                blockers = []
                status = "PASS"
            else:
                blockers = ["hot_path_single_wallet_live_promotion_required_buy_copy_events_below_10"]
        elif best_hot_path_fresh > 0 and best_hot_path_wallets >= 2:
            blockers = ["hot_path_runtime_multi_wallet_partial_without_pass_signal"]
        elif best_tracker_time_replay_pass > 0 and best_tracker_time_replay_filled > 0:
            blockers = ["tracker_time_replay_passed_but_current_poll_truth_missing"]
        elif best_hot_path_inventory > 0:
            blockers = ["hot_path_runtime_inventory_candidate_without_paper_lifecycle"]
        elif best_hot_path_fresh > 0 and best_hot_path_wallets < 2:
            blockers = ["hot_path_runtime_fresh_but_single_wallet_only"]
        elif best_single_wallet_intents > 0 and best_single_wallet_filled >= best_single_wallet_intents:
            if best_single_wallet_intents >= MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS:
                blockers = []
                status = "PASS"
            else:
                blockers = ["single_wallet_live_promotion_required_buy_copy_events_below_10"]
        elif best_runtime_fresh > 0 and best_runtime_wallets < 2:
            blockers = ["runtime_fresh_but_single_wallet_only"]
        elif best_runtime_fresh > 0:
            blockers = ["runtime_fresh_without_adaptive_pass_signal"]
        elif best_runtime_inventory > 0:
            blockers = ["runtime_inventory_candidate_without_adaptive_pass_signal"]
        elif latest_adaptive.get("source_feed_delayed") is True:
            blockers = ["source_feed_delayed"]
        elif latest_adaptive.get("tracker_fresh_but_runtime_stale") is True:
            blockers = ["tracker_fresh_but_runtime_stale"]
        else:
            blockers = list(latest_adaptive.get("blockers") or blockers)
    exact_probe_rows = [
        row.get("exact_cohort_probe")
        for row in tick_rows
        if isinstance(row.get("exact_cohort_probe"), dict)
        and (row.get("exact_cohort_probe") or {}).get("status") == "PASS"
    ]
    next_exact_cohort_probe_index = (
        exact_probe_rows[-1].get("next_cohort_index")
        if exact_probe_rows
        else _next_probe_cohort_index(args)
    )
    global_tracker_lock_cleanup = _cleanup_dead_global_tracker_lock()
    bridge_burnin = _current_poll_inventory_bridge_burnin_summary(tick_rows)
    window_indexed_bridge_burnin = _window_indexed_inventory_bridge_burnin_summary(
        args=args,
        current_poll_bridge_burnin=bridge_burnin,
    )
    live_feed_bridge_clob_snapshot = _capture_live_feed_bridge_clob_truth(
        args=args,
        current_poll_bridge_burnin=bridge_burnin,
        started=started,
    )
    live_feed_bridge_burnin = _live_feed_inventory_bridge_burnin_summary(
        args=args,
        current_poll_bridge_burnin=bridge_burnin,
        clob_snapshot=live_feed_bridge_clob_snapshot,
    )
    state = {
        "kind": "wallet_copy_hotlane_tick_state",
        "generated_at": utc_now_iso(),
        "status": status,
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
        "current_poll_inventory_bridge_burnin": bridge_burnin,
        "window_indexed_inventory_bridge_burnin": window_indexed_bridge_burnin,
        "live_feed_bridge_clob_snapshot": live_feed_bridge_clob_snapshot,
        "live_feed_inventory_bridge_burnin": live_feed_bridge_burnin,
        "development_bridge_active_hotlane_refresh": bridge_hotlane_refresh,
        "summary": {
            "ticks_requested": ticks_requested,
            "ticks_completed": len(tick_rows),
            "elapsed_s": round(time.time() - started, 3),
            **_child_command_counts(tick_rows),
            "current_poll_inventory_bridge_burnin": bridge_burnin,
            "window_indexed_inventory_bridge_burnin": window_indexed_bridge_burnin,
            "live_feed_bridge_clob_snapshot": live_feed_bridge_clob_snapshot,
            "live_feed_inventory_bridge_burnin": live_feed_bridge_burnin,
            "development_bridge_active_hotlane_refresh": bridge_hotlane_refresh,
            "wallets_per_tick": int(args.wallets_per_tick),
            "cohort_probe_enabled": bool(args.cohort_probe_on_single_wallet),
            "exact_cohort_probe_enabled": bool(getattr(args, "exact_cohort_probe", True)),
            "force_development_bridge_probe": bool(getattr(args, "force_development_bridge_probe", False)),
            "cohort_probe_max_wallets": int(args.cohort_probe_max_wallets),
            "cohort_probe_limit": int(args.cohort_probe_limit),
            "cohort_probe_pages": int(args.cohort_probe_pages),
            "cohort_probe_ticks": sum(1 for row in tick_rows if row.get("cohort_probe_triggered") is True),
            "exact_cohort_probe_ticks": sum(
                1
                for row in tick_rows
                if (row.get("exact_cohort_probe") or {}).get("status") == "PASS"
            ),
            "next_exact_cohort_probe_index": next_exact_cohort_probe_index,
            "exact_cohort_probe_eligible_cohorts": (
                exact_probe_rows[-1].get("eligible_cohorts") if exact_probe_rows else None
            ),
            "active_measurement_counts": dict(
                sorted(
                    {
                        key: sum(1 for row in tick_rows if str(row.get("active_measurement") or "slice") == key)
                        for key in {"slice", "cohort_probe"}
                    }.items()
                )
            ),
            "best_runtime_fresh_buy_events_le_cap": max(
                int((row.get("adaptive") or {}).get("runtime_fresh_buy_events_le_cap") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_runtime_eligible_wallets": max(
                int((row.get("adaptive") or {}).get("runtime_eligible_wallets") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_runtime_inventory_research_candidates": max(
                int((row.get("adaptive") or {}).get("runtime_inventory_research_candidates") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_adaptive_tracker_time_replay_intents": max(
                int((row.get("adaptive") or {}).get("tracker_time_replay_intents") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_adaptive_tracker_time_replay_filled_orders": max(
                int((row.get("adaptive") or {}).get("tracker_time_replay_filled_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_adaptive_tracker_time_replay_rejected_orders": max(
                int((row.get("adaptive") or {}).get("tracker_time_replay_rejected_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_single_wallet_exact_copy_intents": max(
                int((row.get("tracker") or {}).get("hot_path_single_wallet_exact_copy_intents_created") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_single_wallet_exact_copy_filled_orders": max(
                int((row.get("tracker") or {}).get("hot_path_single_wallet_exact_copy_filled_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_single_wallet_exact_copy_rejected_orders": max(
                int((row.get("tracker") or {}).get("hot_path_single_wallet_exact_copy_rejected_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_pass_signals": max(
                int((row.get("tracker") or {}).get("hot_path_pass_signals") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_intents_created": max(
                int((row.get("tracker") or {}).get("hot_path_intents_created") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_filled_orders": max(
                int((row.get("tracker") or {}).get("hot_path_filled_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_rejected_orders": max(
                int((row.get("tracker") or {}).get("hot_path_rejected_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_inventory_intents_created": max(
                int((row.get("tracker") or {}).get("hot_path_inventory_intents_created") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_inventory_filled_orders": max(
                int((row.get("tracker") or {}).get("hot_path_inventory_filled_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_inventory_rejected_orders": max(
                int((row.get("tracker") or {}).get("hot_path_inventory_rejected_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_runtime_fresh_buy_events_le_cap": max(
                int((row.get("tracker") or {}).get("hot_path_runtime_fresh_buy_events_le_cap") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_runtime_eligible_wallets": max(
                int((row.get("tracker") or {}).get("hot_path_runtime_eligible_wallets") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_runtime_inventory_research_candidates": max(
                int((row.get("tracker") or {}).get("hot_path_runtime_inventory_research_candidates") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_runtime_signal_blocker_counts": _aggregate_counter(
                tick_rows,
                "hot_path_runtime_signal_blocker_counts",
            ),
            "adaptive_freshness_transition_counts": _aggregate_adaptive_freshness_counter(
                tick_rows,
                "freshness_transition_counts",
            ),
            "top_adaptive_freshness_transitions": _top_adaptive_freshness_transitions(
                tick_rows,
                max_observed_event_age_s=float(args.adaptive_max_observed_event_age_s),
            ),
            "best_hot_path_tracker_time_replay_pass_signals": max(
                int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_pass_signals") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_tracker_time_replay_intents_created": max(
                int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_intents_created") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_tracker_time_replay_filled_orders": max(
                int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_filled_orders") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_tracker_time_replay_eligible_moves": max(
                int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_eligible_moves") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "best_hot_path_tracker_time_replay_recent_observed_moves": max(
                int((row.get("tracker") or {}).get("hot_path_tracker_time_replay_recent_observed_moves") or 0)
                for row in tick_rows
            ) if tick_rows else 0,
            "latest_tracker": latest_tracker,
            "latest_adaptive": latest_adaptive,
            "global_tracker_lock_cleanup": global_tracker_lock_cleanup,
        },
        "ticks": tick_rows[-20:],
    }
    atomic_write_json(args.output, state)
    print(json.dumps({"state": str(args.output), "status": status, "blockers": blockers, "summary": state["summary"]}, indent=2, sort_keys=True))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
