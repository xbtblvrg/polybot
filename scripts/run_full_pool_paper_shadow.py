#!/usr/bin/env python3
"""Run the wallet-copy full-pool paper-shadow learning batch.

This orchestrator is paper/research only. It writes separate full-pool
artifacts, never creates CopyIntents, and never touches the live guard.
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
DEFAULT_STATUS = "data/research/wallet_copy_full_pool_paper_shadow_status.json"
DEFAULT_LOCK = "data/research/wallet_copy_full_pool_paper_shadow.lock"
DEFAULT_LANE = "data/research/wallet_copy_full_pool_broad_paper_lane_state.json"
DEFAULT_MEASUREMENT = "data/research/wallet_copy_full_pool_broad_paper_measurement_state.json"
DEFAULT_EVENTS = "data/research/wallet_copy_full_pool_broad_paper_events.jsonl"
DEFAULT_SHORTLIST = "data/research/active_set_expansion_full_pool_shortlist.json"
DEFAULT_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_RESOLUTION_OUTPUT = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_RESOLUTION_SUMMARY = "data/research/btc_resolutions_full_pool_gamma_summary.json"
DEFAULT_CANDIDATE_ALLOWLIST = "data/research/wallet_copy_full_universe_copyability_latest.json"
DEFAULT_WIDE_EXACT_STATE = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_WIDE_EXACT_LEDGER = "data/research/wide_exact_policy_paper_orders.jsonl"
DEFAULT_WIDE_STANDINGS = "data/research/wide_candidate_standings_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", default=DEFAULT_STATUS)
    parser.add_argument("--lock", default=DEFAULT_LOCK)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--candidate-allowlist-state", default=DEFAULT_CANDIDATE_ALLOWLIST)
    parser.add_argument("--candidate-allowlist-status", default="READY_QUEUE")
    parser.add_argument("--lane-state", default=DEFAULT_LANE)
    parser.add_argument("--measurement-state", default=DEFAULT_MEASUREMENT)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--shortlist-output", default=DEFAULT_SHORTLIST)
    parser.add_argument("--replay-output", default=DEFAULT_REPLAY)
    parser.add_argument("--activity-jsonl", default="data/research/polygon_orderfilled_ws_capture.jsonl")
    parser.add_argument("--rtds-jsonl", default="data/research/rtds_wallet_activity_latest.jsonl")
    parser.add_argument("--market-category", default="btc_5m")
    parser.add_argument("--policy-id", default="full_pool_shadow_mission5_btc5m")
    parser.add_argument("--scan-limit", type=int, default=500_000)
    parser.add_argument("--max-events", type=int, default=2_000)
    parser.add_argument("--max-book-fetches", type=int, default=200)
    parser.add_argument("--replay-every-minutes", type=float, default=60.0)
    parser.add_argument("--replay-max-clob-fetches", type=int, default=500)
    parser.add_argument("--resolution-output", default=DEFAULT_RESOLUTION_OUTPUT)
    parser.add_argument("--resolution-summary-output", default=DEFAULT_RESOLUTION_SUMMARY)
    parser.add_argument("--resolution-max-replay-slugs", type=int, default=80)
    parser.add_argument("--resolution-max-windows", type=int, default=250)
    parser.add_argument("--resolution-max-wall-runtime-s", type=float, default=180.0)
    parser.add_argument("--resolution-timeout-s", type=float, default=8.0)
    parser.add_argument("--timeout-s", type=float, default=900.0)
    parser.add_argument("--skip-replay", action="store_true")
    parser.add_argument("--skip-resolution-refresh", action="store_true")
    parser.add_argument("--wide-run-id", default="")
    parser.add_argument("--wide-polygon-jsonl", default="")
    parser.add_argument("--wide-alpha-report", default="")
    parser.add_argument("--wide-manifest", default="")
    parser.add_argument("--wide-exact-state", default=DEFAULT_WIDE_EXACT_STATE)
    parser.add_argument("--wide-exact-ledger", default=DEFAULT_WIDE_EXACT_LEDGER)
    parser.add_argument("--wide-standings", default=DEFAULT_WIDE_STANDINGS)
    return parser.parse_args()


def _abs(path: str) -> Path:
    target = Path(path)
    return target if target.is_absolute() else ROOT / target


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


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


def _run(cmd: list[str], *, timeout_s: float, ok_returncodes: tuple[int, ...] = (0,)) -> dict[str, Any]:
    started = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=float(timeout_s),
        check=False,
    )
    return {
        "cmd": cmd,
        "returncode": proc.returncode,
        "ok": int(proc.returncode) in ok_returncodes,
        "duration_s": round(time.time() - started, 3),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


def _should_replay(status: dict[str, Any], *, every_minutes: float) -> bool:
    if every_minutes <= 0:
        return True
    last = status.get("last_replay_started_s")
    try:
        last_s = float(last)
    except (TypeError, ValueError):
        return True
    return time.time() - last_s >= every_minutes * 60.0


def _replay_unresolved_market_slugs(path: Path, *, max_slugs: int) -> list[str]:
    payload = _load_json(path)
    slugs: list[str] = []
    seen: set[str] = set()
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        orders = replay.get("replay_orders") if isinstance(replay.get("replay_orders"), list) else []
        for order in orders:
            if not isinstance(order, dict):
                continue
            status = str(order.get("final_status") or order.get("status") or "").upper()
            if status != "FILLED":
                continue
            if order.get("resolved") is True or order.get("pnl_usd") is not None:
                continue
            slug = str(order.get("market_slug") or "")
            if not slug.startswith("btc-updown-5m-") or slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
            if len(slugs) >= max(0, int(max_slugs)):
                return slugs
    return slugs


def _main_locked(args: argparse.Namespace) -> int:
    status_path = _abs(args.status)
    prior_status = _load_json(status_path)
    commands: list[dict[str, Any]] = []
    started = time.time()
    due_replay = (not args.skip_replay) and _should_replay(
        prior_status,
        every_minutes=float(args.replay_every_minutes),
    )

    build_base = [
        sys.executable,
        "scripts/build_top10_broad_paper_lane.py",
        "--activity-jsonl",
        args.activity_jsonl,
        "--limit",
        str(int(args.limit)),
        "--candidate-allowlist-state",
        args.candidate_allowlist_state,
        "--candidate-allowlist-status",
        args.candidate_allowlist_status,
        "--output",
        args.lane_state,
    ]
    commands.append(_run(build_base, timeout_s=float(args.timeout_s)))

    measurement_cmd = [
        sys.executable,
        "scripts/run_top10_broad_paper_lane.py",
        "--lane-state",
        args.lane_state,
        "--polygon-jsonl",
        args.activity_jsonl,
        "--rtds-jsonl",
        args.rtds_jsonl if _abs(args.rtds_jsonl).exists() else "",
        "--output",
        args.measurement_state,
        "--event-log",
        args.event_log,
        "--policy-id",
        args.policy_id,
        "--scan-limit",
        str(int(args.scan_limit)),
        "--max-events",
        str(int(args.max_events)),
        "--max-book-fetches",
        str(int(args.max_book_fetches)),
        "--floor-copy-size-to-min-order",
        "--buy-events-only",
        "--market-category",
        args.market_category,
    ]
    commands.append(_run(measurement_cmd, timeout_s=float(args.timeout_s)))

    rebuild_cmd = [
        sys.executable,
        "scripts/build_top10_broad_paper_lane.py",
        "--activity-jsonl",
        args.activity_jsonl,
        "--measurement-state",
        args.measurement_state,
        "--limit",
        str(int(args.limit)),
        "--candidate-allowlist-state",
        args.candidate_allowlist_state,
        "--candidate-allowlist-status",
        args.candidate_allowlist_status,
        "--output",
        args.lane_state,
    ]
    commands.append(_run(rebuild_cmd, timeout_s=float(args.timeout_s)))

    shortlist_cmd = [
        sys.executable,
        "scripts/build_active_set_expansion_shortlist.py",
        "--top-n",
        "100",
        "--output",
        args.shortlist_output,
    ]
    commands.append(_run(shortlist_cmd, timeout_s=float(args.timeout_s)))

    replay_started_s = prior_status.get("last_replay_started_s")
    if due_replay:
        replay_started_s = time.time()
        replay_cmd = [
            sys.executable,
            "scripts/replay_discover_live_band_candidates.py",
            "--output",
            args.replay_output,
            "--max-clob-fetches",
            str(int(args.replay_max_clob_fetches)),
            "--rescore-stored-orders",
            "--capture-unresolved-clob-books",
        ]
        commands.append(_run(replay_cmd, timeout_s=float(args.timeout_s)))

    replay_resolution_slugs = _replay_unresolved_market_slugs(
        _abs(args.replay_output),
        max_slugs=int(args.resolution_max_replay_slugs),
    )
    if not args.skip_resolution_refresh:
        resolution_cmd = [
            sys.executable,
            "scripts/refresh_btc_5m_resolutions_from_gamma.py",
            "--ledger",
            "data/research/wallet_copy_live_execution_state.json",
            "--existing",
            args.resolution_output,
            "--output",
            args.resolution_output,
            "--summary-output",
            args.resolution_summary_output,
            "--merge-existing",
            "--max-windows",
            str(int(args.resolution_max_windows)),
            "--max-wall-runtime-s",
            str(float(args.resolution_max_wall_runtime_s)),
            "--timeout-s",
            str(float(args.resolution_timeout_s)),
        ]
        for slug in replay_resolution_slugs:
            resolution_cmd.extend(["--market-slug", slug])
        commands.append(
            _run(
                resolution_cmd,
                timeout_s=float(args.timeout_s),
                ok_returncodes=(0, 2),
            )
        )

    wide_enabled = bool(
        args.wide_run_id and args.wide_polygon_jsonl and args.wide_alpha_report and args.wide_manifest
    )
    if wide_enabled:
        reconcile_cmd = [
            sys.executable,
            "scripts/reconcile_wide_exact_policy_paper.py",
            "--run-id",
            args.wide_run_id,
            "--polygon-jsonl",
            args.wide_polygon_jsonl,
            "--alpha-report",
            args.wide_alpha_report,
            "--manifest",
            args.wide_manifest,
            "--resolutions",
            args.resolution_output,
            "--state",
            args.wide_exact_state,
            "--ledger",
            args.wide_exact_ledger,
        ]
        commands.append(_run(reconcile_cmd, timeout_s=float(args.timeout_s)))
        standings_cmd = [
            sys.executable,
            "scripts/build_wide_candidate_standings.py",
            "--alpha",
            args.wide_alpha_report,
            "--measurement",
            args.wide_exact_state,
            "--output",
            args.wide_standings,
        ]
        commands.append(_run(standings_cmd, timeout_s=float(args.timeout_s)))

    lane_state = _load_json(_abs(args.lane_state))
    measurement_state = _load_json(_abs(args.measurement_state))
    shortlist_state = _load_json(_abs(args.shortlist_output))
    replay_state = _load_json(_abs(args.replay_output))
    resolution_state = _load_json(_abs(args.resolution_summary_output))
    failed = [row for row in commands if not bool(row.get("ok"))]
    status = {
        "schema_version": 1,
        "kind": "wallet_copy_full_pool_paper_shadow_status",
        "paper_only": True,
        "live_orders_allowed": False,
        "started_s": round(started, 6),
        "finished_s": round(time.time(), 6),
        "duration_s": round(time.time() - started, 3),
        "status": "ERROR" if failed else "OK",
        "failed_commands": len(failed),
        "last_replay_started_s": replay_started_s,
        "replay_due_this_run": bool(due_replay),
        "outputs": {
            "lane_state": args.lane_state,
            "measurement_state": args.measurement_state,
            "event_log": args.event_log,
            "shortlist_output": args.shortlist_output,
            "replay_output": args.replay_output,
            "resolution_output": args.resolution_output,
            "resolution_summary_output": args.resolution_summary_output,
            "wide_exact_state": args.wide_exact_state if wide_enabled else "",
            "wide_exact_ledger": args.wide_exact_ledger if wide_enabled else "",
            "wide_standings": args.wide_standings if wide_enabled else "",
        },
        "summary": {
            "lane_selected_wallets": int((lane_state.get("selection") or {}).get("selected_wallets") or 0),
            "lane_candidate_wallets": int((lane_state.get("selection") or {}).get("candidate_wallets") or 0),
            "measurement_wallets": int((measurement_state.get("summary") or {}).get("wallets") or 0),
            "measurement_buy_events": int((measurement_state.get("summary") or {}).get("buy_events") or 0),
            "measurement_copyable_buy_events": int(
                (measurement_state.get("summary") or {}).get("copyable_buy_events") or 0
            ),
            "shortlist_top_count": int((shortlist_state.get("summary") or {}).get("top_count") or 0),
            "shortlist_pool": int((shortlist_state.get("summary") or {}).get("pool_after_active_set_exclusion") or 0),
            "replay_candidates": int((replay_state.get("replay_summary") or {}).get("candidate_count") or 0),
            "replay_promotable": int((replay_state.get("replay_summary") or {}).get("promotable_replays") or 0),
            "resolution_replay_slugs_requested": len(replay_resolution_slugs),
            "resolution_requested_windows": int(resolution_state.get("requested_windows") or 0),
            "resolution_fetched_canonical_rows": int(resolution_state.get("fetched_canonical_rows") or 0),
            "resolution_failed_count": int(resolution_state.get("failed_count") or 0),
            "resolution_status": str(resolution_state.get("status") or ""),
        },
        "commands": commands,
    }
    _write_json(status_path, status)
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
            existing_pid_text = ""
            try:
                existing_pid_text = lock_path.read_text().strip()
                existing_pid = int(existing_pid_text)
            except (OSError, ValueError):
                existing_pid = 0
            if existing_pid and not _pid_alive(existing_pid):
                lock_path.unlink(missing_ok=True)
                reclaimed_stale_lock = True
                continue
            print(
                json.dumps(
                    {
                        "status": "ALREADY_RUNNING",
                        "lock": str(lock_path),
                        "pid": existing_pid_text,
                    },
                    sort_keys=True,
                )
            )
            return 0
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(str(os.getpid()) + "\n")
        exit_code = _main_locked(args)
        if reclaimed_stale_lock:
            status_path = _abs(args.status)
            status = _load_json(status_path)
            if status:
                status["reclaimed_stale_lock"] = True
                _write_json(status_path, status)
        return exit_code
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
