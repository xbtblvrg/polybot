#!/usr/bin/env python3
"""Track registered BTC 5m wallets and copy new moves into paper.

This is a live tracker, not a live order submitter. It polls wallet-attributed
truth sources, enriches moves with CLOB/onchain evidence where configured, and
applies the same CopyIntent path to paper.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.live_tracker import (
    EmptyTrackerPollRejected,
    LiveTrackerConfig,
    LiveWalletTracker,
    _write_tracker_state_with_non_regression,
)
from src.wallet_copy.mission import mission_contract


ADMISSION_SOURCE_ROUTE_ENV_VARS = (
    "POLYMARKET_DATA_API_BASE_URL",
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_CLOB_API_BASE_URL",
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
)


class _RuntimeLimitExceeded(TimeoutError):
    """Raised when a single tracker poll exceeds the CLI runtime budget."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _load_existing_state(path: str) -> dict:
    try:
        candidate = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return candidate if isinstance(candidate, dict) else {}


def _poll_with_runtime_alarm(tracker: LiveWalletTracker, remaining_s: float) -> dict:
    """Run poll_once with a hard wall-clock cap when POSIX timers are available."""

    if remaining_s <= 0 or not hasattr(signal, "setitimer"):
        return tracker.poll_once()

    def _raise_timeout(signum, frame):  # noqa: ANN001, ARG001
        raise _RuntimeLimitExceeded(f"tracker poll exceeded remaining runtime budget {remaining_s:.3f}s")

    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _raise_timeout)
    old_timer = signal.setitimer(signal.ITIMER_REAL, max(0.001, float(remaining_s)))
    try:
        return tracker.poll_once()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, old_handler)
        if old_timer and old_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, old_timer[0], old_timer[1])


def _minimum_remaining_runtime_for_next_poll(
    args: argparse.Namespace,
    *,
    effective_max_poll_runtime_s: float,
) -> float:
    """Avoid starting another poll when only enough time remains to corrupt current-poll evidence."""

    poll_cap_s = max(0.0, float(effective_max_poll_runtime_s))
    if poll_cap_s <= 0:
        return 0.0
    try:
        data_api_timeout_s = max(0.0, float(getattr(args, "data_api_timeout_s", 0.0)))
    except (TypeError, ValueError):
        data_api_timeout_s = 0.0
    try:
        data_api_retries = max(1, int(getattr(args, "data_api_retries", 1)))
    except (TypeError, ValueError):
        data_api_retries = 1
    source_route_floor_s = data_api_timeout_s * min(3, data_api_retries) + 2.0
    return min(poll_cap_s, max(5.0, source_route_floor_s))


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _copy_efficiency_summary(state: dict) -> dict:
    summary = state.get("summary") if isinstance(state, dict) else {}
    copy_efficiency = summary.get("copy_efficiency") if isinstance(summary, dict) else {}
    candidate = copy_efficiency.get("summary") if isinstance(copy_efficiency, dict) else {}
    return candidate if isinstance(candidate, dict) else {}


def _clob_filled_market_windows(state: dict) -> int:
    summary = state.get("summary") if isinstance(state, dict) else {}
    copy_efficiency = summary.get("copy_efficiency") if isinstance(summary, dict) else {}
    rows = []
    if isinstance(copy_efficiency, dict):
        rows.extend(row for row in copy_efficiency.get("event_scores") or [] if isinstance(row, dict))
    rows.extend(row for row in state.get("admission_evidence_window") or [] if isinstance(row, dict))
    windows: set[str] = set()
    for row in rows:
        score = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else row
        if str(score.get("wallet_action") or row.get("action") or "").upper() != "BUY":
            continue
        if score.get("copy_status") != "COPIED_FILLED":
            continue
        if score.get("fill_source") != "clob_book_evidence":
            continue
        market_slug = str(score.get("market_slug") or row.get("market_slug") or "")
        if market_slug:
            windows.add(market_slug)
    return len(windows)


def _admission_evidence_stop_condition(state: dict, args: argparse.Namespace) -> dict:
    """Stop bounded paper burn-in once strict BUY CopyIntent proof exists."""

    if not bool(getattr(args, "stop_on_admission_evidence", False)):
        return {"enabled": False, "reached": False}
    summary = _copy_efficiency_summary(state)
    required = _safe_int(summary.get("required_buy_copy_events"))
    clob_filled = _safe_int(summary.get("clob_filled_buy_copy_events"))
    fallback_filled = _safe_int(summary.get("fallback_filled_buy_copy_events"))
    rejected = _safe_int(summary.get("rejected_buy_copy_events"))
    missed = _safe_int(summary.get("missed_buy_copy_events"))
    min_required = max(1, _safe_int(getattr(args, "stop_min_required_buy_copy_events", 1), 1))
    min_clob_filled = max(1, _safe_int(getattr(args, "stop_min_clob_filled_buy_copy_events", 1), 1))
    min_clob_filled_market_windows = max(
        0,
        _safe_int(getattr(args, "stop_min_clob_filled_market_windows", 0), 0),
    )
    clob_filled_market_windows = _clob_filled_market_windows(state)
    blockers: list[str] = []
    if required < min_required:
        blockers.append("required_buy_copy_events_below_stop_threshold")
    if clob_filled < min_clob_filled:
        blockers.append("clob_filled_buy_copy_events_below_stop_threshold")
    if (
        min_clob_filled_market_windows > 0
        and clob_filled_market_windows < min_clob_filled_market_windows
    ):
        blockers.append("clob_filled_market_windows_below_stop_threshold")
    if clob_filled < required:
        blockers.append("not_all_required_buys_clob_filled")
    if fallback_filled > 0:
        blockers.append("fallback_filled_buy_copy_events_present")
    if rejected > 0:
        blockers.append("rejected_buy_copy_events_present")
    if missed > 0:
        blockers.append("missed_buy_copy_events_present")
    return {
        "enabled": True,
        "reached": not blockers,
        "role": "paper_only_burnin_stop_not_live_admission",
        "min_required_buy_copy_events": min_required,
        "min_clob_filled_buy_copy_events": min_clob_filled,
        "min_clob_filled_market_windows": min_clob_filled_market_windows,
        "required_buy_copy_events": required,
        "clob_filled_buy_copy_events": clob_filled,
        "clob_filled_market_windows": clob_filled_market_windows,
        "fallback_filled_buy_copy_events": fallback_filled,
        "rejected_buy_copy_events": rejected,
        "missed_buy_copy_events": missed,
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _disable_source_base_overrides_for_admission(args: argparse.Namespace) -> dict:
    """Force admission proof onto direct source routes unless explicitly allowed."""

    enabled = bool(getattr(args, "admission_mode", False)) and not bool(
        getattr(args, "allow_source_base_overrides_in_admission", False)
    )
    prior_values: dict[str, str] = {}
    cleared: list[str] = []
    if enabled:
        for env_var in ADMISSION_SOURCE_ROUTE_ENV_VARS:
            value = os.environ.get(env_var)
            if value:
                prior_values[env_var] = value
                os.environ[env_var] = ""
                cleared.append(env_var)
            elif env_var in os.environ:
                os.environ[env_var] = ""
    return {
        "enabled": enabled,
        "allow_source_base_overrides_in_admission": bool(
            getattr(args, "allow_source_base_overrides_in_admission", False)
        ),
        "cleared_env_vars": cleared,
        "prior_configured_env_vars": sorted(prior_values),
        "role": "admission_direct_source_route_guard_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--event-log", default="data/research/wallet_copy_live_tracking_events.jsonl")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_paper_events.jsonl")
    parser.add_argument(
        "--tracker-time-replay-paper-state",
        default="data/research/wallet_copy_live_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_live_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_live_tracker_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_live_tracker_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument("--all-order-exact-copy-paper-state", default="")
    parser.add_argument("--all-order-exact-copy-paper-event-log", default="")
    parser.add_argument("--all-order-tactic-replay-paper-state", default="")
    parser.add_argument("--all-order-tactic-replay-paper-event-log", default="")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--data-api-timeout-s", type=float, default=8.0)
    parser.add_argument(
        "--data-api-retries",
        type=int,
        default=3,
        help=(
            "Measured Polymarket source-route retry count per wallet API request. "
            "Use 1 for latency probes when the route is known to reset; default keeps audit retries."
        ),
    )
    parser.add_argument(
        "--data-api-trade-query-keys",
        default="user,proxyWallet",
        help=(
            "Comma-separated /trades query keys to poll. Use 'user' for the latency-critical "
            "hot-copy lane; keep 'user,proxyWallet' for full audit lanes."
        ),
    )
    parser.add_argument(
        "--include-activity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Fetch /activity lifecycle rows during tracker polls. Disable only for latency-critical "
            "candidate-only BUY copyability probes; full audit lanes should keep activity enabled."
        ),
    )
    parser.add_argument(
        "--max-poll-runtime-s",
        type=float,
        default=0.0,
        help="Hard cap for one poll_once cycle; stops before the next wallet/event when the cap is reached.",
    )
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--poll-interval-s", type=float, default=1.0)
    parser.add_argument(
        "--max-runtime-s",
        type=float,
        default=0.0,
        help="Stop after this many seconds even if iterations remain; 0 disables the total runtime cap.",
    )
    parser.add_argument(
        "--stop-on-admission-evidence",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "For bounded paper-only burn-in probes, keep polling until strict current CopyIntent BUY proof appears "
            "or the runtime budget expires. This only stops measurement; it does not grant live admission."
        ),
    )
    parser.add_argument("--stop-min-required-buy-copy-events", type=int, default=1)
    parser.add_argument("--stop-min-clob-filled-buy-copy-events", type=int, default=1)
    parser.add_argument("--stop-min-clob-filled-market-windows", type=int, default=0)
    parser.add_argument(
        "--global-tracker-lock",
        default="data/research/wallet_copy_live_tracker_global.lock",
        help="Serializes memory-heavy live tracker runs; a busy lock exits rc=2 with explicit state evidence.",
    )
    parser.add_argument("--use-global-tracker-lock", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--wallet-fraction", type=float, default=0.05)
    parser.add_argument("--max-order-usd", type=float, default=2.0)
    parser.add_argument("--min-order-usd", type=float, default=0.0)
    parser.add_argument("--profit-policy-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--strategy-direction-state", default="data/research/wallet_copy_strategy_direction_state.json")
    parser.add_argument(
        "--intent-time-copyability-proof-state",
        default="data/research/wallet_copy_intent_time_copyability_proof_state.json",
        help="Live-guard intent-time proof sidecar merged into tracker admission evidence.",
    )
    parser.add_argument("--clob-host", default="https://clob.polymarket.com")
    parser.add_argument("--gamma-host", default="https://gamma-api.polymarket.com")
    parser.add_argument("--polygon-rpc-url", default="https://polygon-bor-rpc.publicnode.com")
    parser.add_argument("--enable-clob-books", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--enable-onchain-receipts", action="store_true")
    parser.add_argument("--clob-timeout-s", type=float, default=5.0)
    parser.add_argument("--gamma-timeout-s", type=float, default=5.0)
    parser.add_argument("--onchain-timeout-s", type=float, default=8.0)
    parser.add_argument("--market-ws-jsonl", default="")
    parser.add_argument(
        "--rtds-activity-jsonl",
        default="",
        help=(
            "Canonical wallet-attributed RTDS activity JSONL. Its independent byte cursor is stored "
            "in this tracker's state; first attach starts at EOF to prevent historical replay."
        ),
    )
    parser.add_argument("--market-ws-lookback-s", type=float, default=120.0)
    parser.add_argument("--preconfirm-match-window-s", type=float, default=8.0)
    parser.add_argument("--preconfirm-price-tolerance", type=float, default=0.01)
    parser.add_argument("--max-book-slippage-bps", type=float, default=150.0)
    parser.add_argument("--max-copy-efficiency-latency-s", type=float, default=10.0)
    parser.add_argument("--max-copy-efficiency-slippage-bps", type=float, default=500.0)
    parser.add_argument("--require-clob-book-evidence-for-efficiency", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--enable-copyability-gate", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--admission-mode",
        action="store_true",
        help=(
            "Require admission-grade copyability/CLOB/strict guards. Use no-gate tracker runs "
            "only as research experiments, never as live-admission evidence."
        ),
    )
    parser.add_argument(
        "--allow-source-base-overrides-in-admission",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Paper-only research escape hatch. Admission-mode proof normally clears Polymarket "
            "base/proxy override env vars so live-readiness evidence uses direct Data/Gamma/CLOB routes."
        ),
    )
    parser.add_argument("--max-copyability-event-age-s", type=float, default=10.0)
    parser.add_argument("--max-wallet-fetch-duration-s", type=float, default=2.0)
    parser.add_argument("--min-copyability-clob-fill-ratio", type=float, default=0.999)
    parser.add_argument("--strict-mirror-coverage", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--seed-history-state", default="")
    parser.add_argument("--seed-before-poll", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--seed-max-events", type=int, default=0)
    parser.add_argument("--seed-lookback-s", type=float, default=0.0)
    parser.add_argument("--paper-retain-orders", type=int, default=1_000)
    parser.add_argument("--paper-retain-lifecycle-events", type=int, default=3_000)
    parser.add_argument("--paper-retain-dedupe-ids", type=int, default=250_000)
    parser.add_argument("--wallet-address", action="append", default=[])
    parser.add_argument("--wallet-name", action="append", default=[])
    parser.add_argument("--max-wallets", type=int, default=0)
    parser.add_argument(
        "--parallel-wallet-fetches",
        type=int,
        default=1,
        help="Fetch wallet history concurrently before sequential CLOB/paper processing; paper-only latency helper.",
    )
    parser.add_argument(
        "--parallel-data-api-sources",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Fetch /trades user, /trades proxyWallet, and /activity concurrently inside each wallet poll. "
            "Keeps all sources while reducing hot-path source roundtrip latency."
        ),
    )
    parser.add_argument("--use-profit-search-scope", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--track-blocked-profit-policy",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Paper-only: forward-measure the current best blocked profit candidate policy. "
            "This stamps CopyIntents with the candidate policy id but never grants live admission by itself."
        ),
    )
    parser.add_argument(
        "--profit-policy-candidate-only",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Paper-only proof helper: when a profit-policy candidate is pinned, poll only that candidate wallet "
            "instead of spending the hot-path runtime on registry rotation."
        ),
    )
    return parser.parse_args()


def _acquire_global_tracker_lock(args: argparse.Namespace):
    if not bool(getattr(args, "use_global_tracker_lock", True)):
        return None
    lock_path = Path(str(args.global_tracker_lock))
    if not lock_path.is_absolute():
        lock_path = ROOT / lock_path
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.seek(0)
        raw_holder = lock_handle.read().strip()
        try:
            holder = json.loads(raw_holder) if raw_holder else {}
        except json.JSONDecodeError:
            holder = {"raw": raw_holder}
        owner_pid = holder.get("pid") if isinstance(holder, dict) else None
        runtime = {
            "status": "GLOBAL_TRACKER_LOCK_BUSY",
            "lock_path": str(lock_path),
            "pid": owner_pid or os.getpid(),
            "pid_role": "lock_owner" if owner_pid else "blocked_process",
            "blocked_pid": os.getpid(),
            "lock_owner_pid": owner_pid,
            "lock_owner_started_at": holder.get("started_at") if isinstance(holder, dict) else None,
            "lock_owner_state": holder.get("state") if isinstance(holder, dict) else None,
            "message": "another memory-heavy wallet live tracker is already running; this run is blocked, not green",
        }
        state = _load_existing_state(args.state)
        state.update(
            {
                "generated_at": _utc_now(),
                "kind": state.get("kind") or "wallet_copy_live_tracker_state",
                "paper_only": True,
                "live_orders_allowed": False,
                "runtime_limit": runtime,
            }
        )
        summary = state.setdefault("summary", {})
        if isinstance(summary, dict):
            summary["runtime_limit"] = runtime
            summary["global_tracker_lock_status"] = "BUSY"
        _write_tracker_state_with_non_regression(args.state, state)
        print(json.dumps({"state": args.state, "summary": summary}, indent=2, sort_keys=True, default=str))
        lock_handle.close()
        return False
    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(json.dumps({"pid": os.getpid(), "state": args.state, "started_at": _utc_now()}, sort_keys=True) + "\n")
    lock_handle.flush()
    return lock_handle


def _release_global_tracker_lock(lock_handle) -> None:
    if lock_handle is None:
        return
    raw_name = str(getattr(lock_handle, "name", "") or "")
    lock_path = Path(raw_name) if raw_name else None
    try:
        if lock_path and lock_path.exists() and lock_path.is_file():
            lock_path.unlink(missing_ok=True)
    finally:
        lock_handle.close()


def _clear_stale_cli_runtime_limit(state: dict) -> bool:
    """Drop a prior CLI runtime blocker after a non-limited tracker poll succeeds."""
    if not isinstance(state, dict):
        return False
    changed = False
    if "runtime_limit" in state:
        state.pop("runtime_limit", None)
        changed = True
    summary = state.get("summary")
    if isinstance(summary, dict):
        for key in ("runtime_limit", "runtime_limit_status", "global_tracker_lock_status"):
            if key in summary:
                summary.pop(key, None)
                changed = True
    return changed


def _admission_mode_errors(args: argparse.Namespace) -> list[str]:
    if not bool(getattr(args, "admission_mode", False)):
        return []
    errors: list[str] = []
    if not bool(args.enable_copyability_gate):
        errors.append("admission_mode_requires_copyability_gate")
    if not bool(args.enable_clob_books):
        errors.append("admission_mode_requires_clob_books")
    if not bool(args.require_clob_book_evidence_for_efficiency):
        errors.append("admission_mode_requires_clob_evidence")
    if not bool(args.strict_mirror_coverage):
        errors.append("admission_mode_requires_strict_mirror_coverage")
    runtime_phase = mission_contract().get("current_runtime_phase_contract")
    runtime_phase = runtime_phase if isinstance(runtime_phase, dict) else {}
    profitability_filter = runtime_phase.get("profitability_filter_contract")
    profitability_filter = profitability_filter if isinstance(profitability_filter, dict) else {}
    max_live_event_age_s = float(profitability_filter.get("max_event_age_s") or 10.0)
    if float(args.max_copyability_event_age_s) > max_live_event_age_s:
        errors.append("admission_mode_max_copyability_event_age_too_loose")
    if float(args.max_copy_efficiency_latency_s) > 10.0:
        errors.append("admission_mode_max_copy_efficiency_latency_too_loose")
    if float(args.max_wallet_fetch_duration_s) > 2.0:
        errors.append("admission_mode_max_wallet_fetch_duration_too_loose")
    if float(args.min_copyability_clob_fill_ratio) < 0.999:
        errors.append("admission_mode_min_clob_fill_ratio_too_loose")
    trade_query_keys = {
        item.strip()
        for item in str(getattr(args, "data_api_trade_query_keys", "") or "").split(",")
        if item.strip()
    }
    if "user" not in trade_query_keys:
        errors.append("admission_mode_requires_user_trade_query_source")
    return errors


def main() -> int:
    args = parse_args()
    admission_errors = _admission_mode_errors(args)
    if admission_errors:
        print(
            json.dumps(
                {
                    "status": "CONFIG_REJECTED",
                    "admission_mode": True,
                    "errors": admission_errors,
                    "live_orders_allowed": False,
                    "paper_only": True,
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    source_route_guard = _disable_source_base_overrides_for_admission(args)
    global_tracker_lock = _acquire_global_tracker_lock(args)
    if global_tracker_lock is False:
        return 2
    max_runtime_s = max(0.0, float(args.max_runtime_s))
    requested_max_poll_runtime_s = max(0.0, float(args.max_poll_runtime_s))
    effective_max_poll_runtime_s = requested_max_poll_runtime_s
    if max_runtime_s > 0:
        effective_max_poll_runtime_s = (
            min(requested_max_poll_runtime_s, max_runtime_s)
            if requested_max_poll_runtime_s > 0
            else max_runtime_s
        )
    cfg = LiveTrackerConfig(
        registry_path=args.registry,
        state_path=args.state,
        event_log_path=args.event_log,
        paper_state_path=args.paper_state,
        paper_event_log_path=args.paper_event_log,
        tracker_time_replay_paper_state_path=args.tracker_time_replay_paper_state,
        tracker_time_replay_paper_event_log_path=args.tracker_time_replay_paper_event_log,
        single_wallet_exact_copy_paper_state_path=args.single_wallet_exact_copy_paper_state,
        single_wallet_exact_copy_paper_event_log_path=args.single_wallet_exact_copy_paper_event_log,
        all_order_exact_copy_paper_state_path=args.all_order_exact_copy_paper_state,
        all_order_exact_copy_paper_event_log_path=args.all_order_exact_copy_paper_event_log,
        all_order_tactic_replay_paper_state_path=args.all_order_tactic_replay_paper_state,
        all_order_tactic_replay_paper_event_log_path=args.all_order_tactic_replay_paper_event_log,
        resolutions_path=args.resolutions,
        data_api_limit=args.limit,
        data_api_pages=args.pages,
        data_api_timeout_s=args.data_api_timeout_s,
        data_api_retries=args.data_api_retries,
        include_activity=args.include_activity,
        trade_query_keys=tuple(
            item.strip()
            for item in str(args.data_api_trade_query_keys or "").split(",")
            if item.strip()
        ),
        max_poll_runtime_s=effective_max_poll_runtime_s,
        parallel_data_api_sources=args.parallel_data_api_sources,
        clob_host=args.clob_host,
        gamma_host=args.gamma_host,
        polygon_rpc_url=args.polygon_rpc_url,
        enable_clob_books=args.enable_clob_books,
        enable_onchain_receipts=args.enable_onchain_receipts,
        clob_timeout_s=args.clob_timeout_s,
        gamma_timeout_s=args.gamma_timeout_s,
        onchain_timeout_s=args.onchain_timeout_s,
        market_ws_jsonl_path=args.market_ws_jsonl,
        rtds_activity_jsonl_path=args.rtds_activity_jsonl,
        market_ws_lookback_s=args.market_ws_lookback_s,
        preconfirm_match_window_s=args.preconfirm_match_window_s,
        preconfirm_price_tolerance=args.preconfirm_price_tolerance,
        max_book_slippage_bps=args.max_book_slippage_bps,
        max_copy_efficiency_latency_s=args.max_copy_efficiency_latency_s,
        max_copy_efficiency_slippage_bps=args.max_copy_efficiency_slippage_bps,
        require_clob_book_evidence_for_efficiency=args.require_clob_book_evidence_for_efficiency,
        enable_copyability_gate=args.enable_copyability_gate,
        admission_mode=args.admission_mode,
        max_copyability_event_age_s=args.max_copyability_event_age_s,
        max_wallet_fetch_duration_s=args.max_wallet_fetch_duration_s,
        min_copyability_clob_fill_ratio=args.min_copyability_clob_fill_ratio,
        wallet_fraction=args.wallet_fraction,
        max_order_usd=args.max_order_usd,
        min_order_usd=args.min_order_usd,
        profit_policy_state_path=args.profit_policy_state,
        strategy_direction_state_path=args.strategy_direction_state,
        intent_time_copyability_proof_state_path=args.intent_time_copyability_proof_state,
        strict_mirror_coverage=args.strict_mirror_coverage,
        paper_retain_orders=max(1, int(args.paper_retain_orders)),
        paper_retain_lifecycle_events=max(1, int(args.paper_retain_lifecycle_events)),
        paper_retain_dedupe_ids=max(1, int(args.paper_retain_dedupe_ids)),
        seed_history_state_path=args.seed_history_state,
        seed_before_poll=args.seed_before_poll,
        seed_max_events=args.seed_max_events,
        seed_lookback_s=args.seed_lookback_s,
        wallet_address_allowlist=tuple(args.wallet_address or []),
        wallet_name_allowlist=tuple(args.wallet_name or []),
        max_wallets=args.max_wallets,
        parallel_wallet_fetches=args.parallel_wallet_fetches,
        use_profit_search_scope=args.use_profit_search_scope,
        track_blocked_profit_policy=args.track_blocked_profit_policy,
        profit_policy_candidate_only=args.profit_policy_candidate_only,
        allow_source_base_overrides_in_admission=args.allow_source_base_overrides_in_admission,
    )
    tracker = LiveWalletTracker(cfg)
    state = {}
    iterations = max(1, int(args.iterations))
    started_ts = time.time()
    runtime_limited = False
    hard_runtime_exceeded = False
    stop_condition: dict = {}
    runtime_limit_message = "tracker stopped by CLI runtime cap before completing all requested iterations"
    for index in range(iterations):
        elapsed_s = time.time() - started_ts
        if max_runtime_s > 0 and elapsed_s >= max_runtime_s:
            runtime_limited = True
            break
        remaining_s = max_runtime_s - elapsed_s if max_runtime_s > 0 else 0.0
        if max_runtime_s > 0 and state:
            minimum_next_poll_s = _minimum_remaining_runtime_for_next_poll(
                args,
                effective_max_poll_runtime_s=effective_max_poll_runtime_s,
            )
            if remaining_s < minimum_next_poll_s:
                runtime_limited = True
                runtime_limit_message = (
                    "tracker stopped before starting another poll because remaining runtime budget "
                    f"{remaining_s:.3f}s is below minimum next-poll budget {minimum_next_poll_s:.3f}s"
                )
                break
        try:
            state = _poll_with_runtime_alarm(tracker, remaining_s)
        except EmptyTrackerPollRejected as exc:
            print(
                json.dumps(
                    {
                        "status": "EMPTY_POLL_REJECTED",
                        "state": args.state,
                        "quarantine_path": exc.quarantine_path,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            _release_global_tracker_lock(global_tracker_lock)
            return 3
        except _RuntimeLimitExceeded as exc:
            runtime_limited = True
            hard_runtime_exceeded = True
            runtime_limit_message = str(exc)
            state = _load_existing_state(args.state)
            break
        stop_condition = _admission_evidence_stop_condition(state, args)
        if stop_condition.get("reached"):
            break
        if max_runtime_s > 0 and time.time() - started_ts >= max_runtime_s and index < iterations - 1:
            runtime_limited = True
            break
        if index < iterations - 1:
            sleep_s = max(0.0, float(args.poll_interval_s))
            if max_runtime_s > 0:
                remaining_s = max_runtime_s - (time.time() - started_ts)
                sleep_s = min(sleep_s, max(0.0, remaining_s))
            time.sleep(sleep_s)
    if stop_condition and isinstance(state, dict):
        state["generated_at"] = _utc_now()
        state["kind"] = state.get("kind") or "wallet_copy_live_tracker_state"
        state["paper_only"] = True
        state["live_orders_allowed"] = False
        state["admission_evidence_stop_condition"] = stop_condition
        summary = state.setdefault("summary", {})
        if isinstance(summary, dict):
            summary["admission_evidence_stop_condition"] = stop_condition
            summary["admission_source_route_guard"] = source_route_guard
        _write_tracker_state_with_non_regression(args.state, state)
    if runtime_limited and isinstance(state, dict):
        prior_summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        existing_blockers = prior_summary.get("blockers") if isinstance(prior_summary.get("blockers"), list) else []
        runtime_blocker = "tracker_hard_runtime_timeout" if hard_runtime_exceeded else "tracker_runtime_limit_reached"
        blockers = [str(item) for item in existing_blockers]
        if runtime_blocker not in blockers:
            blockers.append(runtime_blocker)
        runtime = {
            "status": "HARD_TIMEOUT" if hard_runtime_exceeded else "LIMIT_REACHED",
            "max_runtime_s": max_runtime_s,
            "elapsed_s": round(time.time() - started_ts, 3),
            "requested_iterations": iterations,
            "completed_iterations": index,
            "progress_status": "PARTIAL_STATE_PERSISTED",
            "blockers": blockers,
            "state_path": args.state,
            "message": runtime_limit_message,
        }
        state["generated_at"] = _utc_now()
        state["kind"] = state.get("kind") or "wallet_copy_live_tracker_state"
        state["paper_only"] = True
        state["live_orders_allowed"] = False
        state["runtime_limit"] = runtime
        summary = state.setdefault("summary", {})
        if isinstance(summary, dict):
            summary["runtime_limit"] = runtime
            summary["status"] = summary.get("status") or ("ANALYZE" if hard_runtime_exceeded else "WATCH")
            summary["blockers"] = blockers
            summary["runtime_limit_status"] = runtime["status"]
            summary["admission_source_route_guard"] = source_route_guard
        _write_tracker_state_with_non_regression(args.state, state)
    elif _clear_stale_cli_runtime_limit(state):
        summary = state.setdefault("summary", {}) if isinstance(state, dict) else {}
        if isinstance(summary, dict):
            summary["admission_source_route_guard"] = source_route_guard
        _write_tracker_state_with_non_regression(args.state, state)
    elif isinstance(state, dict):
        summary = state.setdefault("summary", {})
        if isinstance(summary, dict):
            summary["admission_source_route_guard"] = source_route_guard
        _write_tracker_state_with_non_regression(args.state, state)
    summary = state.get("summary") if isinstance(state, dict) else {}
    print(json.dumps({"state": args.state, "summary": summary}, indent=2, sort_keys=True, default=str))
    exit_code = 0
    if args.strict_mirror_coverage and isinstance(summary, dict) and summary.get("mirror_coverage_status") == "FAIL":
        exit_code = 2
    all_order_exact_copy = summary.get("all_order_exact_copy") if isinstance(summary, dict) else {}
    if (
        exit_code == 0
        and args.strict_mirror_coverage
        and isinstance(all_order_exact_copy, dict)
        and all_order_exact_copy.get("status") == "FAIL"
    ):
        exit_code = 2
    copy_efficiency = summary.get("copy_efficiency") if isinstance(summary, dict) else {}
    if (
        exit_code == 0
        and args.strict_mirror_coverage
        and isinstance(copy_efficiency, dict)
        and copy_efficiency.get("status") == "FAIL"
    ):
        exit_code = 2
    if hard_runtime_exceeded:
        exit_code = 2
    _release_global_tracker_lock(global_tracker_lock)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
