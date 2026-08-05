#!/usr/bin/env python3
"""Run the slow-market paper qualification lane.

This orchestrates ranking plus bounded paper measurement.  It is paper-only:
it writes research artifacts and never creates CopyIntents or live orders.
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


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402

DEFAULT_STATUS = "data/research/slow_market_paper_qualification_status.json"
DEFAULT_LOCK = "data/research/slow_market_paper_qualification.lock"
DEFAULT_RANKING = "data/research/slow_market_candidate_ranking_20260705.json"
DEFAULT_LANE = "data/research/slow_market_paper_lane_state.json"
DEFAULT_MEASUREMENT = "data/research/slow_market_paper_measurement_state.json"
DEFAULT_EVENTS = "data/research/slow_market_paper_events.jsonl"
DEFAULT_RTDS = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_ABANDONED_WALLETS = "data/research/slow_market_abandoned_wallets.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", default=DEFAULT_STATUS)
    parser.add_argument("--lock", default=DEFAULT_LOCK)
    parser.add_argument("--ranking-output", default=DEFAULT_RANKING)
    parser.add_argument("--lane-state", default=DEFAULT_LANE)
    parser.add_argument("--measurement-state", default=DEFAULT_MEASUREMENT)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--tail-bytes", type=int, default=256_000_000)
    parser.add_argument("--ranking-max-rows", type=int, default=250_000)
    parser.add_argument("--measurement-scan-limit", type=int, default=100_000)
    parser.add_argument("--measurement-max-events", type=int, default=250)
    parser.add_argument("--max-book-fetches", type=int, default=60)
    parser.add_argument("--command-timeout-s", type=float, default=150.0)
    parser.add_argument("--clob-base-url", default="https://clob.polymarket.com")
    parser.add_argument("--clob-timeout-s", type=float, default=0.75)
    parser.add_argument("--policy-id", default="slow_market_mission5_v1")
    parser.add_argument("--abandoned-wallets-state", default=DEFAULT_ABANDONED_WALLETS)
    parser.add_argument("--kill-pnl-usd", type=float, default=-50.0)
    parser.add_argument("--promotion-min-resolved-fills", type=int, default=50)
    parser.add_argument("--ignore-prior-state", action="store_true")
    return parser.parse_args()


def _abs(path: str) -> Path:
    target = Path(path)
    return target if target.is_absolute() else ROOT / target


def _load_json(path: str | Path) -> dict[str, Any]:
    target = _abs(str(path))
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = _abs(str(path))
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(target)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _run(cmd: list[str], *, timeout_s: float, env: dict[str, str] | None = None) -> dict[str, Any]:
    started = time.time()
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(1.0, float(timeout_s)),
            check=False,
        )
        return {
            "cmd": cmd,
            "returncode": int(completed.returncode),
            "ok": int(completed.returncode) == 0,
            "duration_s": round(time.time() - started, 3),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": cmd,
            "returncode": 124,
            "ok": False,
            "duration_s": round(time.time() - started, 3),
            "timeout_s": float(timeout_s),
            "stdout_tail": str(exc.stdout or "")[-4000:],
            "stderr_tail": str(exc.stderr or "")[-4000:],
        }


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_from_any(payload: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _selected_wallets(ranking: dict[str, Any]) -> list[str]:
    wallets: list[str] = []
    for row in ranking.get("ranked_wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42 and wallet not in wallets:
            wallets.append(wallet)
    return wallets


def _qualification_decision(args: argparse.Namespace, measurement_summary: dict[str, Any]) -> dict[str, Any]:
    pnl_usd = _num(measurement_summary.get("paper_pnl_usd"))
    resolved_fills = _int_from_any(
        measurement_summary,
        (
            "resolved_fills",
            "resolved_orders",
            "resolved_filled_orders",
            "paper_resolved_fills",
            "paper_resolved_orders",
        ),
    )
    kill_pnl = float(args.kill_pnl_usd)
    min_resolved = int(args.promotion_min_resolved_fills)
    reasons: list[str] = []
    status = "WATCH"
    if pnl_usd <= kill_pnl:
        status = "ABANDON_CURRENT_SELECTION"
        reasons.append("paper_pnl_at_or_below_kill_line")
    if resolved_fills >= min_resolved and pnl_usd <= 0.0:
        status = "ABANDON_CURRENT_SELECTION"
        reasons.append("resolved_fill_gate_non_positive_pnl")
    return {
        "flow_stage": "OBSERVE/PROMOTE/ROTATE",
        "status": status,
        "paper_pnl_usd": round(pnl_usd, 6),
        "kill_pnl_usd": kill_pnl,
        "resolved_fills_observed": resolved_fills,
        "promotion_min_resolved_fills": min_resolved,
        "reasons": reasons,
        "next_action": (
            "write selected wallets to abandoned state and rerank next pass"
            if status == "ABANDON_CURRENT_SELECTION"
            else "continue paper qualification until resolved positive gate or kill line"
        ),
    }


def _record_abandoned_wallets(
    path: str | Path,
    *,
    wallets: list[str],
    decision: dict[str, Any],
    generated_at: float,
) -> dict[str, Any]:
    existing = _load_json(path)
    rows_by_wallet: dict[str, dict[str, Any]] = {}
    for row in existing.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            rows_by_wallet[wallet] = dict(row)
    for wallet in wallets:
        rows_by_wallet[wallet] = {
            "wallet": wallet,
            "abandoned_at_s": round(generated_at, 6),
            "reason": ",".join(decision.get("reasons") or []),
            "paper_pnl_usd": decision.get("paper_pnl_usd"),
        }
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_slow_market_abandoned_wallets",
        "flow_stage": "OBSERVE/PROMOTE/ROTATE",
        "updated_at_s": round(generated_at, 6),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(generated_at)),
        "wallets": [rows_by_wallet[key] for key in sorted(rows_by_wallet)],
        "latest_decision": decision,
    }
    _write_json(path, payload)
    return payload


def _main_locked(args: argparse.Namespace) -> int:
    started = time.time()
    commands: list[dict[str, Any]] = []
    ranking_cmd = [
        sys.executable,
        "scripts/build_slow_market_candidate_ranking.py",
        "--output",
        args.ranking_output,
        "--lane-output",
        args.lane_state,
        "--limit",
        str(int(args.limit)),
        "--tail-bytes",
        str(int(args.tail_bytes)),
        "--max-rows",
        str(int(args.ranking_max_rows)),
        "--excluded-wallets-state",
        args.abandoned_wallets_state,
    ]
    commands.append(_run(ranking_cmd, timeout_s=float(args.command_timeout_s)))
    post_ranking = _load_json(args.ranking_output)
    post_lane = _load_json(args.lane_state)
    prior_measurement = _load_json(args.measurement_state)
    selection_wallets = _selected_wallets(post_lane) or _selected_wallets(post_ranking)
    prior_wallets = _selected_wallets(prior_measurement)
    selection_changed_from_prior = bool(
        selection_wallets and prior_wallets and set(selection_wallets) != set(prior_wallets)
    )

    measurement_cmd = [
        sys.executable,
        "scripts/run_top10_broad_paper_lane.py",
        "--lane-state",
        args.lane_state,
        "--rtds-jsonl",
        args.rtds_jsonl,
        "--polygon-jsonl",
        "",
        "--output",
        args.measurement_state,
        "--event-log",
        args.event_log,
        "--clob-base-url",
        args.clob_base_url,
        "--clob-timeout-s",
        str(float(args.clob_timeout_s)),
        "--policy-id",
        args.policy_id,
        "--scan-limit",
        str(int(args.measurement_scan_limit)),
        "--max-events",
        str(int(args.measurement_max_events)),
        "--max-book-fetches",
        str(int(args.max_book_fetches)),
        "--floor-copy-size-to-min-order",
        "--buy-events-only",
    ]
    if args.ignore_prior_state or selection_changed_from_prior:
        measurement_cmd.append("--ignore-prior-state")
    measurement_env = os.environ.copy()
    measurement_env["POLYMARKET_CLOB_API_BASE_URL"] = ""
    commands.append(
        _run(
            measurement_cmd,
            timeout_s=float(args.command_timeout_s),
            env=measurement_env,
        )
    )

    ranking = _load_json(args.ranking_output)
    measurement = _load_json(args.measurement_state)
    failed = [row for row in commands if not bool(row.get("ok"))]
    measurement_summary = measurement.get("summary") if isinstance(measurement.get("summary"), dict) else {}
    qualification_decision = _qualification_decision(args, measurement_summary)
    abandoned_state: dict[str, Any] = {}
    if not failed and qualification_decision.get("status") == "ABANDON_CURRENT_SELECTION":
        abandoned_state = _record_abandoned_wallets(
            args.abandoned_wallets_state,
            wallets=_selected_wallets(ranking),
            decision=qualification_decision,
            generated_at=time.time(),
        )
    abandoned_state_for_summary = abandoned_state or _load_json(args.abandoned_wallets_state)
    status_value = "ERROR" if failed else str(qualification_decision.get("status") or "WATCH")
    status = {
        "schema_version": 1,
        "kind": "wallet_copy_slow_market_paper_qualification_status",
        "flow_stage": "OBSERVE/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "started_s": round(started, 6),
        "finished_s": round(time.time(), 6),
        "duration_s": round(time.time() - started, 3),
        "status": status_value,
        "failed_commands": len(failed),
        "outputs": {
            "ranking_output": args.ranking_output,
            "lane_state": args.lane_state,
            "measurement_state": args.measurement_state,
            "event_log": args.event_log,
            "abandoned_wallets_state": args.abandoned_wallets_state,
        },
        "summary": {
            "selected_count": int((ranking.get("summary") or {}).get("selected_count") or 0),
            "selected_recent_buy_events": int((ranking.get("summary") or {}).get("selected_recent_buy_events") or 0),
            "measurement_wallets": int(measurement_summary.get("wallets") or 0),
            "measurement_buy_events": int(measurement_summary.get("buy_events") or 0),
            "measurement_copyable_buy_events": int(measurement_summary.get("copyable_buy_events") or 0),
            "measurement_paper_pnl_usd": measurement_summary.get("paper_pnl_usd"),
            "wallets_with_buy_sample": int(measurement_summary.get("wallets_with_buy_sample") or 0),
            "wallets_with_realtime_events": int(measurement_summary.get("wallets_with_realtime_events") or 0),
            "measurement_status": measurement.get("status") or "",
            "abandoned_wallet_count": len(abandoned_state_for_summary.get("wallets") or []),
            "selection_changed_from_prior_measurement": selection_changed_from_prior,
        },
        "qualification_decision": qualification_decision,
        "commands": commands,
        "next_action": (
            "rerank next pass with abandoned wallets excluded; live path remains untouched"
            if status_value == "ABANDON_CURRENT_SELECTION"
            else (
                "continue paper qualification until >=50 resolved paper fills are positive at our prices; "
                "then promote through the single live guard only after Fable gate"
            )
        ),
    }
    _write_json(args.status, status)
    print(json.dumps(status, indent=2, sort_keys=True))
    return 1 if failed else 0


def main() -> int:
    args = parse_args()
    lock_path = _abs(args.lock)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    reclaimed_stale_lock = False
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            try:
                existing_pid = int(lock_path.read_text().strip())
            except (OSError, ValueError):
                existing_pid = 0
            if existing_pid and not _pid_alive(existing_pid):
                lock_path.unlink(missing_ok=True)
                reclaimed_stale_lock = True
                continue
            print(json.dumps({"status": "ALREADY_RUNNING", "lock": str(lock_path), "pid": existing_pid}, sort_keys=True))
            return 0
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(str(os.getpid()) + "\n")
        code = _main_locked(args)
        if reclaimed_stale_lock:
            status = _load_json(args.status)
            if status:
                status["reclaimed_stale_lock"] = True
                _write_json(args.status, status)
        return code
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
