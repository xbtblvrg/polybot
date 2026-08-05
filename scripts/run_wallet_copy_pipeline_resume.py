#!/usr/bin/env python3
"""Run one resumable wallet-copy history/paper slice.

Deep proof cannot depend on a single all-wallet, all-page history pull. This
wrapper advances one deterministic Data API offset per run, merges the fetched
events into the canonical history state, and records the next offset. A
heartbeat can therefore keep making coverage progress without hiding that full
coverage is still incomplete.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


PYTHON = "python3"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallets-config", default="configs/wallet_copy/wallets.json")
    parser.add_argument(
        "--wallet",
        default="",
        help="Optional single wallet override for proof-led targeted coverage; when set, wallets-config is not used.",
    )
    parser.add_argument("--wallet-name", default="")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--wallet-event-log", default="data/research/wallet_copy_events.jsonl")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_paper_events.jsonl")
    parser.add_argument("--resume-state", default="data/research/wallet_copy_pipeline_resume_state.json")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pages-per-run", type=int, default=1)
    parser.add_argument("--offset", type=int, default=-1)
    parser.add_argument(
        "--wallet-batch-size",
        type=int,
        default=4,
        help=(
            "Number of registry wallets to process per child pipeline call. "
            "Use 1 only when a degraded source route needs maximum checkpoint granularity."
        ),
    )
    parser.add_argument(
        "--no-adaptive-wallet-batch-size",
        action="store_true",
        help="Ignore a persisted smaller batch-size recommendation after a timeout.",
    )
    parser.add_argument(
        "--checkpoint-reserve-s",
        type=float,
        default=15.0,
        help="Stop before the parent timeout when checkpointed progress exists instead of losing the slice.",
    )
    parser.add_argument(
        "--min-child-timeout-s",
        type=float,
        default=30.0,
        help="Do not start a new registry wallet batch unless this much child runtime remains after the reserve.",
    )
    parser.add_argument("--wallet-fraction", type=float, default=0.05)
    parser.add_argument("--max-order-usd", type=float, default=2.0)
    parser.add_argument("--policy-id", default="leaderboard_crypto_exact_copy_all_buys")
    parser.add_argument("--command-timeout-s", type=float, default=900.0)
    parser.add_argument("--reset-resume", action="store_true")
    return parser.parse_args()


def _load_resume(path: str | Path, *, reset: bool) -> dict[str, Any]:
    if reset:
        return {}
    payload = load_json(path, default={})
    return payload if isinstance(payload, dict) else {}


def _current_offset(args: argparse.Namespace, state: dict[str, Any]) -> int:
    if int(args.offset) >= 0:
        return int(args.offset)
    return max(0, int(state.get("next_offset") or 0))


def _current_wallet_index(state: dict[str, Any], offset: int) -> int:
    if int(state.get("offset") or 0) != int(offset):
        return 0
    return max(0, int(state.get("next_wallet_index") or 0))


def _effective_wallet_batch_size(args: argparse.Namespace, state: dict[str, Any]) -> int:
    requested = max(1, int(args.wallet_batch_size))
    if bool(getattr(args, "no_adaptive_wallet_batch_size", False)):
        return requested
    recommended = int(state.get("recommended_next_wallet_batch_size") or 0)
    if recommended > 0:
        return max(1, min(requested, recommended))
    return requested


def _pipeline_stdout_payload(stdout: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _slice_completion(
    history_state: dict[str, Any],
    offset: int,
    *,
    current_wallet_count: int | None = None,
) -> dict[str, Any]:
    ingest = history_state.get("ingest") if isinstance(history_state.get("ingest"), dict) else {}
    reports = [row for row in ingest.get("wallet_reports") or [] if isinstance(row, dict)]
    matching_reports = [
        row
        for row in reports
        if int(row.get("last_offset") or row.get("offset") or 0) == int(offset)
    ]
    if current_wallet_count is None:
        slice_reports = matching_reports
    elif current_wallet_count > 0:
        slice_reports = matching_reports[-int(current_wallet_count) :]
    else:
        slice_reports = []
    page_limit_rows = [
        row
        for row in slice_reports
        if "history_page_limit_reached" in {str(item) for item in row.get("blockers") or []}
    ]
    normalized_events = sum(int(row.get("normalized_events") or 0) for row in slice_reports)
    return {
        "offset": int(offset),
        "wallet_reports": len(slice_reports),
        "matching_wallet_reports_total": len(matching_reports),
        "normalized_events": normalized_events,
        "page_limit_wallets": len(page_limit_rows),
        "coverage_complete_hint": bool(slice_reports) and not page_limit_rows,
    }


def _matching_report_count(history_state: dict[str, Any], offset: int) -> int:
    ingest = history_state.get("ingest") if isinstance(history_state.get("ingest"), dict) else {}
    reports = [row for row in ingest.get("wallet_reports") or [] if isinstance(row, dict)]
    return len(
        [
            row
            for row in reports
            if int(row.get("last_offset") or row.get("offset") or 0) == int(offset)
        ]
    )


def _load_wallet_rows(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("wallets") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict) and bool(row.get("enabled", True))]


def _write_batch_wallet_config(rows: list[dict[str, Any]]) -> str:
    data_dir = ROOT / "data" / "research"
    data_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        prefix=".wallet_copy_batch_",
        suffix=".json",
        dir=str(data_dir),
        delete=False,
    )
    with handle:
        json.dump({"wallets": rows}, handle, sort_keys=True)
        handle.write("\n")
    return handle.name


def _base_pipeline_argv(args: argparse.Namespace, *, limit: int, pages: int, offset: int) -> list[str]:
    return [
        PYTHON,
        "scripts/run_wallet_copy_pipeline.py",
        "--limit",
        str(limit),
        "--pages",
        str(pages),
        "--offset",
        str(offset),
        "--history-state",
        args.history_state,
        "--wallet-event-log",
        args.wallet_event_log,
        "--paper-state",
        args.paper_state,
        "--paper-event-log",
        args.paper_event_log,
        "--wallet-fraction",
        str(float(args.wallet_fraction)),
        "--max-order-usd",
        str(float(args.max_order_usd)),
        "--policy-id",
        args.policy_id,
        "--merge-history-state",
    ]


def _run_child(argv: list[str], *, timeout_s: float) -> Any:
    try:
        return subprocess.run(
            argv,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=max(1.0, float(timeout_s)),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        class TimeoutResult:
            returncode = 124
            stdout = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")

        return TimeoutResult()


def _run_single_wallet_override(
    args: argparse.Namespace,
    *,
    limit: int,
    pages: int,
    offset: int,
    timeout_s: float,
) -> tuple[Any, list[dict[str, Any]]]:
    wallet_name = str(args.wallet_name or "").strip() or f"wallet_{str(args.wallet)[2:10]}"
    argv = _base_pipeline_argv(args, limit=limit, pages=pages, offset=offset)
    argv[2:2] = ["--wallet", str(args.wallet).strip(), "--wallet-name", wallet_name]
    result = _run_child(argv, timeout_s=timeout_s)
    return result, [{"argv": argv, "returncode": int(result.returncode), "timed_out": int(result.returncode) == 124}]


def _run_registry_batches(
    args: argparse.Namespace,
    *,
    limit: int,
    pages: int,
    offset: int,
    timeout_s: float,
    start_wallet_index: int,
) -> tuple[Any, list[dict[str, Any]], int, int, bool]:
    wallet_rows = _load_wallet_rows(args.wallets_config)
    wallet_count = len(wallet_rows)
    wallet_index = min(max(0, int(start_wallet_index)), wallet_count)
    batch_size = max(1, int(args.wallet_batch_size))
    reserve_s = max(1.0, float(args.checkpoint_reserve_s))
    min_child_timeout_s = max(1.0, float(args.min_child_timeout_s))
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    child_results: list[dict[str, Any]] = []
    processed_wallets = 0
    stopped_for_budget = False
    last_result: Any | None = None

    while wallet_index < wallet_count:
        remaining = deadline - time.monotonic()
        if remaining <= reserve_s + min_child_timeout_s and processed_wallets > 0:
            stopped_for_budget = True
            break
        if remaining <= 1.0:
            break
        batch_rows = wallet_rows[wallet_index : wallet_index + batch_size]
        batch_config = _write_batch_wallet_config(batch_rows)
        argv = _base_pipeline_argv(args, limit=limit, pages=pages, offset=offset)
        argv[2:2] = ["--wallets-config", batch_config]
        child_timeout_s = max(1.0, min(remaining - 0.5, remaining - reserve_s if processed_wallets > 0 else remaining))
        started = time.monotonic()
        try:
            result = _run_child(argv, timeout_s=child_timeout_s)
        finally:
            try:
                Path(batch_config).unlink()
            except FileNotFoundError:
                pass
        elapsed_s = round(max(0.0, time.monotonic() - started), 6)
        last_result = result
        stdout_payload = _pipeline_stdout_payload(result.stdout or "")
        stdout_wallet_count = len(
            [row for row in stdout_payload.get("wallet_results") or [] if isinstance(row, dict)]
        )
        child_results.append(
            {
                "argv": argv,
                "returncode": int(result.returncode),
                "timed_out": int(result.returncode) == 124,
                "elapsed_s": elapsed_s,
                "wallet_index_start": wallet_index,
                "wallet_index_end": wallet_index + len(batch_rows),
                "wallets_requested": len(batch_rows),
                "stdout_wallet_count": stdout_wallet_count,
                "stdout_tail": (result.stdout or "")[-1000:],
                "stderr_tail": (result.stderr or "")[-1000:],
            }
        )
        if int(result.returncode) != 0:
            return result, child_results, wallet_index, wallet_count, stopped_for_budget
        wallet_index += len(batch_rows)
        processed_wallets += max(stdout_wallet_count, len(batch_rows))
        resume_state_path = getattr(args, "resume_state", None)
        if resume_state_path:
            atomic_write_json(
                resume_state_path,
                {
                    "schema_version": 1,
                    "kind": "wallet_copy_pipeline_resume_state",
                    "generated_at": utc_now_iso(),
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "status": "ANALYZE",
                    "blockers": [
                        "history_wallet_registry_slice_incomplete",
                        "history_wallet_registry_partial_checkpoint",
                    ],
                    "offset": offset,
                    "next_offset": offset,
                    "wallet_count": wallet_count,
                    "wallet_index": int(start_wallet_index),
                    "next_wallet_index": wallet_index,
                    "wallet_batch_size": batch_size,
                    "requested_wallet_batch_size": max(1, int(getattr(args, "requested_wallet_batch_size", batch_size))),
                    "adaptive_wallet_batch_size_applied": batch_size
                    < max(1, int(getattr(args, "requested_wallet_batch_size", batch_size))),
                    "limit": limit,
                    "pages_per_run": pages,
                    "current_wallet_count": processed_wallets,
                    "stdout_wallet_count": stdout_wallet_count,
                    "persisted_wallet_count": processed_wallets,
                    "history_state": args.history_state,
                    "paper_state": args.paper_state,
                    "scope": {
                        "mode": "registry_slice",
                        "wallet": None,
                        "wallet_name": None,
                        "policy_id": args.policy_id,
                    },
                    "command": {
                        "argv": [PYTHON, "scripts/run_wallet_copy_pipeline_resume.py"],
                        "child_commands": child_results,
                        "returncode": 0,
                        "child_returncode": int(result.returncode),
                        "timed_out": False,
                        "timeout_s": timeout_s,
                        "stdout_tail": (result.stdout or "")[-4000:],
                        "stderr_tail": (result.stderr or "")[-4000:],
                    },
                },
            )

    class AggregateResult:
        returncode = 0 if (wallet_index >= wallet_count or processed_wallets > 0) else 124
        stdout = json.dumps({"wallet_results": [{} for _ in range(processed_wallets)]})
        stderr = "" if last_result is None else str(getattr(last_result, "stderr", "") or "")

    return AggregateResult(), child_results, wallet_index, wallet_count, stopped_for_budget


def main() -> int:
    args = parse_args()
    resume_state = _load_resume(args.resume_state, reset=bool(args.reset_resume))
    offset = _current_offset(args, resume_state)
    wallet_start_index = _current_wallet_index(resume_state, offset)
    requested_wallet_batch_size = max(1, int(args.wallet_batch_size))
    args.requested_wallet_batch_size = requested_wallet_batch_size
    args.wallet_batch_size = _effective_wallet_batch_size(args, resume_state)
    limit = max(1, int(args.limit))
    pages = max(1, int(args.pages_per_run))
    timed_out = False
    timeout_s = max(1.0, float(args.command_timeout_s))
    before_history_state = load_json(args.history_state, default={})
    if not isinstance(before_history_state, dict):
        before_history_state = {}
    before_matching_reports = _matching_report_count(before_history_state, offset)
    if str(args.wallet or "").strip():
        wallet_count = 1
        result, child_results = _run_single_wallet_override(
            args,
            limit=limit,
            pages=pages,
            offset=offset,
            timeout_s=timeout_s,
        )
        wallet_index = 1 if int(result.returncode) == 0 else 0
        stopped_for_budget = False
    else:
        result, child_results, wallet_index, wallet_count, stopped_for_budget = _run_registry_batches(
            args,
            limit=limit,
            pages=pages,
            offset=offset,
            timeout_s=timeout_s,
            start_wallet_index=wallet_start_index,
        )
    timed_out = any(bool(row.get("timed_out")) for row in child_results)

    stdout_payload = _pipeline_stdout_payload(result.stdout or "")
    stdout_wallet_count = len(
        [row for row in stdout_payload.get("wallet_results") or [] if isinstance(row, dict)]
    )
    history_state = load_json(args.history_state, default={})
    if not isinstance(history_state, dict):
        history_state = {}
    after_matching_reports = _matching_report_count(history_state, offset)
    successful_child_wallet_count = sum(
        int(row.get("stdout_wallet_count") or 0)
        for row in child_results
        if int(row.get("returncode") or 0) == 0
    )
    persisted_wallet_count = max(0, after_matching_reports - before_matching_reports, successful_child_wallet_count)
    current_wallet_count = stdout_wallet_count
    if current_wallet_count == 0 and persisted_wallet_count > 0:
        current_wallet_count = persisted_wallet_count
    completion_wallet_count = wallet_count if wallet_index >= wallet_count and wallet_count > 0 else current_wallet_count
    completion = _slice_completion(history_state, offset, current_wallet_count=completion_wallet_count)
    if result.returncode == 0 and wallet_index >= wallet_count:
        next_offset = 0 if completion.get("coverage_complete_hint") else offset + limit * pages
        next_wallet_index = 0
        status = "PASS" if completion.get("coverage_complete_hint") else "ANALYZE"
        blockers = [] if status == "PASS" else ["history_pagination_more_offsets_required"]
    elif result.returncode == 0 and persisted_wallet_count > 0:
        next_offset = offset
        next_wallet_index = wallet_index
        status = "ANALYZE"
        blockers = ["history_wallet_registry_slice_incomplete"]
        if stopped_for_budget:
            blockers.append("history_wallet_registry_checkpoint_budget_exhausted")
    elif result.returncode != 0 and persisted_wallet_count > 0:
        next_offset = offset
        next_wallet_index = wallet_index
        status = "ANALYZE"
        blockers = ["history_wallet_registry_slice_incomplete"]
        if timed_out:
            blockers.append("history_wallet_batch_timeout_after_checkpoint")
    else:
        next_offset = offset
        next_wallet_index = wallet_index
        status = "CORRECTION"
        blockers = ["history_slice_command_timeout" if timed_out else "history_slice_command_failed"]
    reported_returncode = 0 if status == "ANALYZE" and persisted_wallet_count > 0 else int(result.returncode)
    recommended_next_wallet_batch_size = 0
    if result.returncode != 0 and persisted_wallet_count > 0 and timed_out:
        recommended_next_wallet_batch_size = max(1, int(args.wallet_batch_size) // 2)
    next_wallet_batch_size = recommended_next_wallet_batch_size or max(1, int(args.wallet_batch_size))

    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_pipeline_resume_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "status": status,
        "blockers": blockers,
        "offset": offset,
        "next_offset": next_offset,
        "wallet_count": wallet_count,
        "wallet_index": wallet_start_index,
        "next_wallet_index": next_wallet_index,
        "wallet_batch_size": max(1, int(args.wallet_batch_size)),
        "requested_wallet_batch_size": requested_wallet_batch_size,
        "adaptive_wallet_batch_size_applied": int(args.wallet_batch_size) < requested_wallet_batch_size,
        "recommended_next_wallet_batch_size": recommended_next_wallet_batch_size,
        "limit": limit,
        "pages_per_run": pages,
        "slice": completion,
        "current_wallet_count": current_wallet_count,
        "stdout_wallet_count": stdout_wallet_count,
        "persisted_wallet_count": persisted_wallet_count,
        "history_state": args.history_state,
        "paper_state": args.paper_state,
        "scope": {
            "mode": "single_wallet" if str(args.wallet or "").strip() else "registry_slice",
            "wallet": str(args.wallet or "").strip() or None,
            "wallet_name": str(args.wallet_name or "").strip() or None,
            "policy_id": args.policy_id,
        },
        "command": {
            "argv": child_results[0]["argv"] if len(child_results) == 1 else [PYTHON, "scripts/run_wallet_copy_pipeline_resume.py"],
            "child_commands": child_results,
            "returncode": reported_returncode,
            "child_returncode": int(result.returncode),
            "timed_out": timed_out,
            "timeout_s": timeout_s,
            "stdout_tail": (result.stdout or "")[-4000:],
            "stderr_tail": (result.stderr or "")[-4000:],
        },
        "next_command": (
            f"python3 scripts/run_wallet_copy_pipeline_resume.py --wallets-config {args.wallets_config} "
            f"--limit {limit} --pages-per-run {pages} --wallet-batch-size {next_wallet_batch_size} "
            f"--command-timeout-s {int(timeout_s)}"
        ),
    }
    atomic_write_json(args.resume_state, payload)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return int(reported_returncode)


if __name__ == "__main__":
    raise SystemExit(main())
