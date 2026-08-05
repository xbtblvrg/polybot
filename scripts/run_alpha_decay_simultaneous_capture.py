#!/usr/bin/env python3
"""Run a paired Polygon-fill and CLOB-book capture, then rebuild alpha decay.

This is LEARN/OBSERVE evidence only. It starts the existing Polygon
OrderFilled probe and the CLOB book snapshotter in the same wall-clock window,
then runs report_alpha_decay.py against the paired outputs so execution-profile
coverage can be audited without manually stitching files together.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json  # noqa: E402


DEFAULT_CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_POLYGON_WSS_FALLBACK_URLS = ("wss://polygon.drpc.org",)


@dataclass(frozen=True)
class CapturePaths:
    run_id: str
    polygon_jsonl: str
    clob_jsonl: str
    alpha_report: str
    state: str
    event_log: str
    detection_report: str
    asset_ids_output: str
    log_dir: str


@dataclass(frozen=True)
class CommandResult:
    name: str
    argv: list[str]
    returncode: int
    duration_s: float
    stdout_log: str | None = None
    stderr_log: str | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--duration-s", type=float, default=1800.0)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--polygon-jsonl", default="")
    parser.add_argument("--orderfilled-fanout-socket", default="")
    parser.add_argument("--clob-jsonl", default="")
    parser.add_argument(
        "--alpha-report",
        default="",
        help="Run-scoped output by default so paper capture cannot overwrite the live-consumed alpha report.",
    )
    parser.add_argument("--state", default="")
    parser.add_argument("--event-log", default="data/research/alpha_decay_simultaneous_capture_events.jsonl")
    parser.add_argument("--detection-report", default="data/research/detection_latency_report.json")
    parser.add_argument("--asset-ids-output", default="data/research/alpha_decay_target_asset_ids.json")
    parser.add_argument("--log-dir", default="data/research/alpha_decay_capture_logs")
    parser.add_argument("--polygon-rpc-url", default="")
    parser.add_argument("--polygon-wss-url", default="")
    parser.add_argument(
        "--polygon-wss-fallback-url",
        action="append",
        default=[
            item.strip()
            for item in os.getenv("POLYGON_WSS_FALLBACK_URLS", ",".join(DEFAULT_POLYGON_WSS_FALLBACK_URLS)).split(",")
            if item.strip()
        ],
        help="Extra Polygon WSS endpoints tried after --polygon-wss-url on subscribe/connect failure.",
    )
    parser.add_argument("--polygon-lookback-blocks", type=int, default=40)
    parser.add_argument("--polygon-timeout-s", type=float, default=10.0)
    parser.add_argument("--polygon-ws-retry-s", type=float, default=5.0)
    parser.add_argument("--registry", action="append", default=[])
    parser.add_argument("--active-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=0.75)
    parser.add_argument("--clob-retries", type=int, default=1)
    parser.add_argument(
        "--asset-ids-file",
        default="data/research/alpha_decay_target_asset_ids.json",
        help="Hot-reloaded explicit BTC5m roster; explicit ids precede Polygon-mined assets.",
    )
    parser.add_argument("--snapshot-interval-s", type=float, default=1.0)
    parser.add_argument("--asset-refresh-s", type=float, default=2.0)
    parser.add_argument("--max-assets", type=int, default=25)
    parser.add_argument("--polygon-scan-limit", type=int, default=250_000)
    parser.add_argument("--polygon-max-age-s", type=float, default=30.0)
    parser.add_argument(
        "--polygon-disable-default-registry",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Pass --disable-default-registry to the Polygon probe for isolated active-registry captures.",
    )
    parser.add_argument(
        "--polygon-registry-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When mining Polygon rows for snapshot targets, keep registered/selected wallet rows only.",
    )
    parser.add_argument(
        "--http-backfill-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use same-run Polygon eth_getLogs rows as LEARN/OBSERVE seed evidence when WS rows are absent or delayed.",
    )
    parser.add_argument(
        "--disable-source-base-overrides",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Clear POLYMARKET_CLOB_API_BASE_URL in the snapshot child so direct CLOB truth is measured.",
    )
    parser.add_argument("--alpha-sample-limit", type=int, default=5000)
    parser.add_argument(
        "--history-state",
        default="data/research/wallet_copy_live_guard_hot_history_state.json",
        help="Explicit current history input passed to report_alpha_decay.py for fail-closed freshness grading.",
    )
    parser.add_argument("--profile-min-fills", type=int, default=20)
    parser.add_argument("--profile-horizon-s", type=float, default=2.0)
    parser.add_argument("--startup-delay-s", type=float, default=3.0)
    parser.add_argument("--process-timeout-buffer-s", type=float, default=45.0)
    return parser.parse_args()


def _capture_paths(args: argparse.Namespace) -> CapturePaths:
    run_id = str(args.run_id or "").strip() or _run_id()
    polygon_jsonl = str(args.polygon_jsonl or f"data/research/polygon_orderfilled_ws_capture_alpha_decay_{run_id}.jsonl")
    clob_jsonl = str(args.clob_jsonl or f"data/research/clob_book_snapshots_alpha_decay_{run_id}.jsonl")
    state = str(args.state or f"data/research/alpha_decay_simultaneous_capture_state_{run_id}.json")
    return CapturePaths(
        run_id=run_id,
        polygon_jsonl=polygon_jsonl,
        clob_jsonl=clob_jsonl,
        alpha_report=str(args.alpha_report or f"data/research/alpha_decay_report_{run_id}.json"),
        state=state,
        event_log=str(args.event_log),
        detection_report=str(args.detection_report),
        asset_ids_output=str(args.asset_ids_output),
        log_dir=str(args.log_dir),
    )


def _base_python(args: argparse.Namespace) -> str:
    return str(args.python or sys.executable)


def _polygon_sources(args: argparse.Namespace) -> list[str]:
    sources = ["polygon_ws"]
    if bool(args.http_backfill_fallback):
        sources.append("polygon_http_getLogs_tail")
    return sources


def _polygon_command(args: argparse.Namespace, paths: CapturePaths) -> list[str]:
    argv = [
        _base_python(args),
        "scripts/probe_polygon_orderfilled_ws.py",
        "--duration-s",
        str(float(args.duration_s)),
        "--output",
        paths.polygon_jsonl,
        "--report",
        paths.detection_report,
        "--lookback-blocks",
        str(int(args.polygon_lookback_blocks)),
        "--timeout-s",
        str(float(args.polygon_timeout_s)),
        "--ws-retry-s",
        str(float(args.polygon_ws_retry_s)),
        "--active-registry",
        str(args.active_registry),
    ]
    for registry in args.registry or []:
        argv.extend(["--registry", str(registry)])
    if str(args.polygon_rpc_url or "").strip():
        argv.extend(["--polygon-rpc-url", str(args.polygon_rpc_url)])
    if str(args.polygon_wss_url or "").strip():
        argv.extend(["--polygon-wss-url", str(args.polygon_wss_url)])
    for url in getattr(args, "polygon_wss_fallback_url", None) or []:
        if str(url).strip():
            argv.extend(["--polygon-wss-fallback-url", str(url).strip()])
    if bool(getattr(args, "polygon_disable_default_registry", False)):
        argv.append("--disable-default-registry")
    if str(getattr(args, "orderfilled_fanout_socket", "") or "").strip():
        argv.extend(
            [
                "--orderfilled-fanout-socket",
                str(args.orderfilled_fanout_socket),
            ]
        )
    return argv


def _clob_command(args: argparse.Namespace, paths: CapturePaths) -> list[str]:
    argv = [
        _base_python(args),
        "scripts/capture_clob_book_snapshots.py",
        "--duration-s",
        str(float(args.duration_s)),
        "--output",
        paths.clob_jsonl,
        "--asset-ids-file",
        str(args.asset_ids_file or ""),
        "--polygon-jsonl",
        paths.polygon_jsonl,
        "--polygon-scan-limit",
        str(int(args.polygon_scan_limit)),
        "--polygon-max-age-s",
        str(float(args.polygon_max_age_s)),
        "--asset-refresh-s",
        str(float(args.asset_refresh_s)),
        "--interval-s",
        str(float(args.snapshot_interval_s)),
        "--timeout-s",
        str(float(args.clob_timeout_s)),
        "--clob-retries",
        str(int(args.clob_retries)),
        "--max-assets",
        str(int(args.max_assets)),
        "--clob-base-url",
        str(args.clob_base_url),
    ]
    for source in _polygon_sources(args):
        argv.extend(["--polygon-source", source])
    if bool(args.disable_source_base_overrides):
        argv.append("--disable-source-base-overrides")
    if not bool(args.polygon_registry_only):
        argv.append("--no-polygon-registry-only")
    return argv


def _alpha_command(args: argparse.Namespace, paths: CapturePaths) -> list[str]:
    argv = [
        _base_python(args),
        "scripts/report_alpha_decay.py",
        "--polygon-jsonl",
        paths.polygon_jsonl,
        "--clob-jsonl",
        paths.clob_jsonl,
        "--report",
        paths.alpha_report,
        "--asset-ids-output",
        paths.asset_ids_output,
        "--history-state",
        str(args.history_state),
        "--sample-limit",
        str(int(args.alpha_sample_limit)),
        "--profile-horizon-s",
        str(float(args.profile_horizon_s)),
        "--profile-min-fills",
        str(int(args.profile_min_fills)),
    ]
    for source in _polygon_sources(args):
        argv.extend(["--fill-source", source])
    return argv


def _read_tail(path: Path, limit: int = 4000) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - limit))
            return handle.read(limit).decode("utf-8", "replace")
    except OSError:
        return ""


def _start_logged_process(name: str, argv: list[str], paths: CapturePaths) -> tuple[subprocess.Popen[Any], Any, Any, Path, Path, float]:
    log_dir = Path(paths.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{paths.run_id}_{name}.stdout.log"
    stderr_path = log_dir / f"{paths.run_id}_{name}.stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    started = time.time()
    process = subprocess.Popen(argv, cwd=ROOT, stdout=stdout_handle, stderr=stderr_handle, text=True)
    return process, stdout_handle, stderr_handle, stdout_path, stderr_path, started


def _finish_logged_process(
    name: str,
    argv: list[str],
    process: subprocess.Popen[Any],
    stdout_handle: Any,
    stderr_handle: Any,
    stdout_path: Path,
    stderr_path: Path,
    started: float,
    *,
    timeout_s: float,
) -> CommandResult:
    try:
        returncode = process.wait(timeout=max(1.0, float(timeout_s)))
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            returncode = process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            process.kill()
            returncode = process.wait(timeout=10.0)
    finally:
        stdout_handle.close()
        stderr_handle.close()
    return CommandResult(
        name=name,
        argv=argv,
        returncode=int(returncode),
        duration_s=round(time.time() - started, 6),
        stdout_log=str(stdout_path),
        stderr_log=str(stderr_path),
        stdout_tail=_read_tail(stdout_path),
        stderr_tail=_read_tail(stderr_path),
    )


def _run_foreground(name: str, argv: list[str], *, timeout_s: float) -> CommandResult:
    started = time.time()
    try:
        completed = subprocess.run(
            argv,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=max(1.0, float(timeout_s)),
            check=False,
        )
        return CommandResult(
            name=name,
            argv=argv,
            returncode=int(completed.returncode),
            duration_s=round(time.time() - started, 6),
            stdout_tail=completed.stdout[-4000:],
            stderr_tail=completed.stderr[-4000:],
        )
    except subprocess.TimeoutExpired as exc:
        return CommandResult(
            name=name,
            argv=argv,
            returncode=124,
            duration_s=round(time.time() - started, 6),
            stdout_tail=str(exc.stdout or "")[-4000:],
            stderr_tail=str(exc.stderr or "")[-4000:],
        )


def _accepted_returncode(
    name: str,
    returncode: int,
    *,
    duration_s: float = 0.0,
    expected_duration_s: float | None = None,
) -> tuple[bool, str | None]:
    if returncode == 0:
        return True, None
    if name == "clob_books" and returncode == 2:
        return True, "clob_snapshot_no_available_books_evidence"
    if name == "polygon_fills" and returncode == 3:
        return True, "polygon_ws_no_rows_or_timeout_evidence"
    if (
        name == "polygon_fills"
        and returncode == -15
        and expected_duration_s is not None
        and duration_s >= max(1.0, float(expected_duration_s))
    ):
        return True, "polygon_timebox_complete_sigterm"
    return False, None


def _annotate_command(
    result: CommandResult,
    *,
    expected_duration_s: float | None = None,
) -> dict[str, Any]:
    ok, reason = _accepted_returncode(
        result.name,
        int(result.returncode),
        duration_s=float(result.duration_s),
        expected_duration_s=expected_duration_s,
    )
    return {
        **result.asdict(),
        "ok": ok,
        "accepted_non_green_evidence": reason is not None,
        "accepted_non_green_reason": reason,
        "evidence_status": "WATCH" if reason else ("PASS" if ok else "FAIL"),
    }


def _alpha_summary(report_path: str) -> dict[str, Any]:
    report = load_json(report_path, default={})
    if not isinstance(report, dict):
        return {}
    alpha = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else {}
    profiles = report.get("execution_profiles") if isinstance(report.get("execution_profiles"), dict) else {}
    return {
        "report": report_path,
        "updated_at": report.get("updated_at"),
        "alpha_status": alpha.get("status"),
        "fills_total": alpha.get("fills_total"),
        "fill_source_counts": alpha.get("fill_source_counts") if isinstance(alpha.get("fill_source_counts"), dict) else {},
        "fills_with_any_book_coverage": alpha.get("fills_with_any_book_coverage"),
        "overlapping_fill_book_assets": alpha.get("overlapping_fill_book_assets"),
        "fills_on_book_assets": alpha.get("fills_on_book_assets"),
        "alpha_blockers": alpha.get("blockers") if isinstance(alpha.get("blockers"), list) else [],
        "execution_profile_status": profiles.get("status"),
        "eligible_profile_count": profiles.get("eligible_profile_count"),
        "profile_count": profiles.get("profile_count"),
        "profile_blockers": profiles.get("blockers") if isinstance(profiles.get("blockers"), list) else [],
        "next_action": profiles.get("next_action") or alpha.get("next_action"),
    }


def _classify_state(command_rows: list[dict[str, Any]], alpha_summary: dict[str, Any]) -> str:
    if any(not bool(row.get("ok")) for row in command_rows):
        return "CORRECTION"
    if str(alpha_summary.get("execution_profile_status") or "").upper() == "PASS":
        return "PASS"
    if str(alpha_summary.get("alpha_status") or "").upper() == "PASS":
        return "ANALYZE"
    return "WATCH"


def main() -> int:
    args = parse_args()
    paths = _capture_paths(args)
    Path(paths.polygon_jsonl).parent.mkdir(parents=True, exist_ok=True)
    Path(paths.clob_jsonl).parent.mkdir(parents=True, exist_ok=True)

    polygon_argv = _polygon_command(args, paths)
    clob_argv = _clob_command(args, paths)
    alpha_argv = _alpha_command(args, paths)
    timeout_s = max(1.0, float(args.duration_s) + float(args.process_timeout_buffer_s))

    started_at = utc_now_iso()
    append_jsonl(
        paths.event_log,
        {
            "event": "alpha_decay_simultaneous_capture_started",
            "flow_stages": ["LEARN", "OBSERVE"],
            "run_id": paths.run_id,
            "started_at": started_at,
            "polygon_jsonl": paths.polygon_jsonl,
            "clob_jsonl": paths.clob_jsonl,
            "duration_s": float(args.duration_s),
        },
    )

    polygon = _start_logged_process("polygon_fills", polygon_argv, paths)
    time.sleep(max(0.0, float(args.startup_delay_s)))
    clob = _start_logged_process("clob_books", clob_argv, paths)

    polygon_result = _finish_logged_process(
        "polygon_fills",
        polygon_argv,
        polygon[0],
        polygon[1],
        polygon[2],
        polygon[3],
        polygon[4],
        polygon[5],
        timeout_s=timeout_s,
    )
    clob_result = _finish_logged_process(
        "clob_books",
        clob_argv,
        clob[0],
        clob[1],
        clob[2],
        clob[3],
        clob[4],
        clob[5],
        timeout_s=timeout_s,
    )
    alpha_result = _run_foreground("alpha_decay_report", alpha_argv, timeout_s=max(30.0, float(args.process_timeout_buffer_s)))

    command_rows = [
        _annotate_command(
            row,
            expected_duration_s=float(args.duration_s) if row.name == "polygon_fills" else None,
        )
        for row in (polygon_result, clob_result, alpha_result)
    ]
    summary = _alpha_summary(paths.alpha_report)
    status = _classify_state(command_rows, summary)
    state = {
        "schema_version": 1,
        "kind": "alpha_decay_simultaneous_capture_state",
        "flow_stages": ["LEARN", "OBSERVE"],
        "status": status,
        "run_id": paths.run_id,
        "started_at": started_at,
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "decision": "measure whether same-window Polygon fills and CLOB books provide execution-profile coverage",
        "threshold": "execution_profiles.status PASS creates promotable LEARN evidence; WATCH/ANALYZE keeps capture/profile repair in BUILD mode",
        "fill_source_policy": {
            "sources": _polygon_sources(args),
            "http_backfill_fallback": bool(args.http_backfill_fallback),
            "note": "polygon_http_getLogs rows are LEARN/OBSERVE seed evidence only; they do not authorize live trigger substitution.",
        },
        "paths": asdict(paths),
        "commands": command_rows,
        "alpha_summary": summary,
        "next_action": (
            "feed eligible profiles into promotion selector"
            if status == "PASS"
            else summary.get("next_action")
            or "continue simultaneous capture on active liquid assets until fill/book coverage is sufficient"
        ),
    }
    atomic_write_json(paths.state, state)
    append_jsonl(paths.event_log, {"event": "alpha_decay_simultaneous_capture_finished", **state})
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if status in {"PASS", "ANALYZE", "WATCH"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
