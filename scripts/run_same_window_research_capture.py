#!/usr/bin/env python3
"""Capture Polygon, CLOB, and Data API evidence in one research window."""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--duration-s", type=float, default=7500.0)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--queue", default="data/research/wallet_copy_full_universe_copyability_research_latest.json")
    parser.add_argument("--top10-lane", default="data/research/wallet_copy_top10_broad_paper_lane_state.json")
    parser.add_argument(
        "--clearance-packets",
        default="data/research/ranked_queue_clearance_packets_latest.json",
        help="Active exact-policy packet set; every packet wallet is included in the capture union.",
    )
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output-dir", default="data/research/same_window_capture")
    parser.add_argument("--poll-interval-s", type=float, default=15.0)
    parser.add_argument("--lock-file", default="data/research/same_window_capture/.capture.lock")
    parser.add_argument("--allow-dirty-run-dir", action="store_true")
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def selected_wallets(
    queue: dict[str, Any],
    lane: dict[str, Any],
    clearance_packets: dict[str, Any] | None = None,
) -> list[str]:
    wallets = {
        _norm_wallet(row.get("wallet"))
        for row in (queue.get("ranked_queue") or [])
        if isinstance(row, dict)
    }
    wallets.update(
        _norm_wallet(row.get("wallet"))
        for row in (lane.get("ranked_wallets") or [])
        if isinstance(row, dict)
    )
    wallets.update(
        _norm_wallet(row.get("wallet"))
        for row in ((clearance_packets or {}).get("packets") or [])
        if isinstance(row, dict)
    )
    return sorted(wallet for wallet in wallets if wallet)


def _registry(wallets: list[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "wallet_copy_registry",
        "generated_at": utc_now_iso(),
        "order_basis": "same_window_ranked_queue9_plus_top10",
        "wallets": [
            {
                "name": f"same_window_{wallet[-10:]}",
                "address": wallet,
                "enabled": True,
                "market_filter": "btc_5m",
                "asset_allowlist": ["BTC"],
                "data_api": "https://data-api.polymarket.com",
                "tags": ["paper_only", "same_window_capture"],
                "notes": "Fable-directed ranked_queue plus top10 same-window research capture.",
            }
            for wallet in wallets
        ],
    }


def _run_logged(argv: list[str], log_path: Path, *, timeout_s: float) -> dict[str, Any]:
    started = time.time()
    with log_path.open("w", encoding="utf-8") as log:
        result = subprocess.run(argv, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=timeout_s, check=False)
    return {"argv": argv, "returncode": result.returncode, "duration_s": round(time.time() - started, 6), "log": str(log_path)}


def _capture_gates(alpha: dict[str, Any], whale: dict[str, Any], top10: dict[str, Any]) -> dict[str, bool]:
    capture_windows = alpha.get("capture_windows") if isinstance(alpha.get("capture_windows"), dict) else {}
    return {
        "alpha_overlap_s_gt_0": float(capture_windows.get("overlap_s") or 0.0) > 0.0,
        "alpha_book_coverage_gt_0": int(alpha.get("fills_with_any_book_coverage") or 0) > 0,
        "top10_rows_scanned_gt_0": int(((top10.get("source") or {}).get("rows_scanned") or 0)) > 0,
        "whale_windows_gt_0": int(((whale.get("coverage") or {}).get("windows") or (whale.get("summary") or {}).get("windows") or 0)) > 0,
    }


def dirty_capture_paths(output_dir: Path) -> list[str]:
    sentinels = [output_dir / "same_window_capture_state.json", *output_dir.glob("*.jsonl")]
    return sorted(str(path) for path in sentinels if path.exists())


def main() -> int:
    args = parse_args()
    lock_path = Path(args.lock_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(
            json.dumps(
                {
                    "status": "HELD",
                    "lock_file": str(lock_path),
                    "retry_policy": "terminal_success_prevents_launchd_retry_loop",
                },
                sort_keys=True,
            )
        )
        return 0
    run_id = str(args.run_id or "").strip() or _run_id()
    output_dir = Path(args.output_dir) / run_id
    dirty_paths = dirty_capture_paths(output_dir)
    if dirty_paths and not args.allow_dirty_run_dir:
        print(
            json.dumps(
                {
                    "status": "DIRTY_RUN_DIR_REFUSED",
                    "run_id": run_id,
                    "dirty_paths": dirty_paths,
                    "retry_policy": "terminal_success_prevents_launchd_failure_relaunch_reuse",
                },
                sort_keys=True,
            )
        )
        return 0
    output_dir.mkdir(parents=True, exist_ok=True)
    queue = load_json(args.queue, default={})
    lane = load_json(args.top10_lane, default={})
    clearance_packets = load_json(args.clearance_packets, default={})
    wallets = selected_wallets(queue, lane, clearance_packets)
    if not wallets:
        raise SystemExit("ranked queue plus top10 selection is empty")

    paths = {
        "registry": output_dir / "selected_wallet_registry.json",
        "alpha_state": output_dir / "alpha_capture_state.json",
        "alpha_report": output_dir / "alpha_decay_report.json",
        "polygon": output_dir / "polygon_orderfilled.jsonl",
        "clob": output_dir / "clob_books.jsonl",
        "dataapi_capture_state": output_dir / "dataapi_capture_state.json",
        "dataapi_poll_state": output_dir / "dataapi_poll_state.json",
        "dataapi_history": output_dir / "dataapi_history.json",
        "dataapi_index": output_dir / "dataapi_history_index.json",
        "dataapi_events": output_dir / "dataapi_wallet_events.jsonl",
        "dataapi_first_seen": output_dir / "dataapi_first_seen.jsonl",
        "dataapi_watermarks": output_dir / "dataapi_watermarks.json",
        "whale": output_dir / "whale_consensus.json",
        "top10": output_dir / "top10_measurement.json",
        "top10_events": output_dir / "top10_events.jsonl",
        "state": output_dir / "same_window_capture_state.json",
    }
    atomic_write_json(paths["registry"], _registry(wallets))

    alpha_cmd = [
        str(args.python), "scripts/run_alpha_decay_simultaneous_capture.py",
        "--run-id", run_id, "--duration-s", str(float(args.duration_s)),
        "--polygon-jsonl", str(paths["polygon"]), "--clob-jsonl", str(paths["clob"]),
        "--alpha-report", str(paths["alpha_report"]), "--state", str(paths["alpha_state"]),
        "--active-registry", str(paths["registry"]), "--polygon-disable-default-registry",
        "--max-assets", "64", "--alpha-sample-limit", "250000",
    ]
    dataapi_cmd = [
        str(args.python), "scripts/capture_dataapi_wallet_events.py",
        "--duration-s", str(float(args.duration_s)), "--poll-interval-s", str(float(args.poll_interval_s)),
        "--history-state", str(paths["dataapi_history"]), "--history-window-index", str(paths["dataapi_index"]),
        "--wallet-event-log", str(paths["dataapi_events"]), "--dataapi-first-seen-jsonl", str(paths["dataapi_first_seen"]),
        "--observation-watermark-state", str(paths["dataapi_watermarks"]), "--poll-state", str(paths["dataapi_poll_state"]),
        "--capture-state", str(paths["dataapi_capture_state"]),
    ]
    for wallet in wallets:
        dataapi_cmd.extend(["--source-wallet", wallet])

    started_at = utc_now_iso()
    started_monotonic = time.monotonic()
    alpha_log = (output_dir / "alpha_capture.log").open("w", encoding="utf-8")
    dataapi_log = (output_dir / "dataapi_capture.log").open("w", encoding="utf-8")
    alpha = subprocess.Popen(alpha_cmd, cwd=ROOT, stdout=alpha_log, stderr=subprocess.STDOUT)
    dataapi = subprocess.Popen(dataapi_cmd, cwd=ROOT, stdout=dataapi_log, stderr=subprocess.STDOUT)
    atomic_write_json(
        paths["state"],
        {
            "schema_version": 1,
            "kind": "same_window_three_source_research_capture",
            "flow_stages": ["DISCOVER", "LEARN", "OBSERVE", "PROMOTE"],
            "status": "RUNNING",
            "run_id": run_id,
            "started_at": started_at,
            "requested_duration_s": float(args.duration_s),
            "selected_wallets": wallets,
            "paper_only": True,
            "live_orders_allowed": False,
            "child_pids": {"alpha_pair": alpha.pid, "dataapi": dataapi.pid},
            "lock_file": str(lock_path),
            "paths": {key: str(value) for key, value in paths.items()},
        },
    )
    timeout_s = max(120.0, float(args.duration_s) + 180.0)
    try:
        alpha_rc = alpha.wait(timeout=timeout_s)
        dataapi_rc = dataapi.wait(timeout=timeout_s)
    finally:
        for process in (alpha, dataapi):
            if process.poll() is None:
                process.terminate()
        alpha_log.close()
        dataapi_log.close()

    whale_cmd = [str(args.python), "scripts/backtest_whale_consensus_v1.py", "--polygon-jsonl", str(paths["polygon"]), "--resolutions", str(args.resolutions), "--output", str(paths["whale"])]
    top10_cmd = [
        str(args.python), "scripts/run_top10_broad_paper_lane.py", "--lane-state", str(args.top10_lane),
        "--rtds-jsonl", str(paths["dataapi_events"]), "--output", str(paths["top10"]),
        "--event-log", str(paths["top10_events"]), "--scan-limit", "250000", "--max-events", "5000",
        "--floor-copy-size-to-min-order", "--buy-events-only", "--market-category", "btc_5m",
        "--ignore-prior-state", "--disable-source-base-overrides",
    ]
    whale_result = _run_logged(whale_cmd, output_dir / "whale_consensus.log", timeout_s=600.0)
    top10_result = _run_logged(top10_cmd, output_dir / "top10_measurement.log", timeout_s=900.0)
    alpha_report = load_json(paths["alpha_report"], default={})
    alpha = alpha_report.get("alpha_decay") if isinstance(alpha_report, dict) else {}
    whale = load_json(paths["whale"], default={})
    top10 = load_json(paths["top10"], default={})
    completed_at = utc_now_iso()
    capture_state = {
        "schema_version": 1,
        "kind": "same_window_three_source_research_capture",
        "flow_stages": ["DISCOVER", "LEARN", "OBSERVE", "PROMOTE"],
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "requested_duration_s": float(args.duration_s),
        "duration_actual_s": round(time.monotonic() - started_monotonic, 6),
        "selected_wallets": wallets,
        "paper_only": True,
        "live_orders_allowed": False,
        "capture_returncodes": {"alpha_pair": alpha_rc, "dataapi": dataapi_rc},
        "consumer_results": {"whale": whale_result, "top10": top10_result},
        "gates": _capture_gates(alpha or {}, whale, top10),
        "paths": {key: str(value) for key, value in paths.items()},
    }
    returncodes = [alpha_rc, dataapi_rc, whale_result["returncode"], top10_result["returncode"]]
    capture_state["gate_status"] = "PASS" if all(capture_state["gates"].values()) else "ANALYZE"
    capture_state["status"] = "COMPLETED" if all(code == 0 for code in returncodes) else "FAILED"
    atomic_write_json(paths["state"], capture_state)
    print(json.dumps(capture_state, indent=2, sort_keys=True))
    return 0 if capture_state["status"] == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
