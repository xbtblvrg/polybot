#!/usr/bin/env python3
"""Quantify candidate signal coverage for no-copy-signal BTC windows.

Flow stage: LIVE/PROMOTE/LEARN. This report answers whether a queued wallet
would add enough distinct BTC 5m windows that the current active roster did not
cover, while separating watcher/trade-window coverage from actual live
CopyIntent coverage.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.scorecard import load_fresh_scorecard


DEFAULT_CANDIDATE = "0xac0586732786905d285959613f1813bc89246729"
DEFAULT_FOCUS_WALLET = "0xad825954d08beba32f74b594821f4251460c3df1"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_WINDOW_INDEX = "data/research/wallet_copy_history_window_index.json"
DEFAULT_LIVE_EXECUTION = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_WATCHER_GAP_REPORT = "data/research/wallet_copy_no_copy_signal_watcher_gap_latest.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_no_copy_signal_candidate_coverage_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-wallet", default=DEFAULT_CANDIDATE)
    parser.add_argument("--focus-wallet", default=DEFAULT_FOCUS_WALLET)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--window-index", default=DEFAULT_WINDOW_INDEX)
    parser.add_argument("--live-execution", default=DEFAULT_LIVE_EXECUTION)
    parser.add_argument("--watcher-gap-report", default=DEFAULT_WATCHER_GAP_REPORT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--trigger-threshold", type=int, default=10)
    parser.add_argument("--direction-id", default="2026-07-08T02:11Z-fable-no-copy-coverage")
    parser.add_argument("--end-ts", type=float, default=0.0)
    parser.add_argument("--window-label", default="")
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text[:42] if text.startswith("0x") and len(text) >= 42 else ""


def _scorecard_window(scorecard: dict[str, Any]) -> tuple[float, float]:
    window = scorecard.get("window") if isinstance(scorecard.get("window"), dict) else {}
    return float(num(window.get("start_ts"), 0.0)), float(num(window.get("end_ts"), 0.0))


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z") if ts else ""


def _no_copy_windows(scorecard: dict[str, Any], *, start_ts: float, end_ts: float) -> set[int]:
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    missed = (
        volume.get("missed_window_attribution")
        if isinstance(volume.get("missed_window_attribution"), dict)
        else {}
    )
    rows = missed.get("rows") if isinstance(missed.get("rows"), list) else []
    return {
        int(num(row.get("window_start_s"), 0.0))
        for row in rows
        if (
            isinstance(row, dict)
            and row.get("attribution") == "no_copy_signal"
            and start_ts <= float(num(row.get("window_start_s"), 0.0)) < end_ts
        )
    }


def _active_wallets(guard_state: dict[str, Any]) -> list[str]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    wallets: list[str] = []
    for member in members:
        if isinstance(member, dict) and (wallet := _norm_wallet(member.get("source_wallet"))):
            wallets.append(wallet)
    return wallets


def _wallet_windows(
    window_index: dict[str, Any],
    wallet: str,
    *,
    start_ts: float,
    end_ts: float,
) -> set[int]:
    wallet = _norm_wallet(wallet)
    windows = window_index.get("windows") if isinstance(window_index.get("windows"), dict) else {}
    out: set[int] = set()
    for key, wallets in windows.items():
        try:
            window_start = int(key)
        except (TypeError, ValueError):
            continue
        if not (start_ts <= window_start < end_ts):
            continue
        if isinstance(wallets, dict) and wallet in wallets:
            out.add(window_start)
    return out


def _union_wallet_windows(
    window_index: dict[str, Any],
    wallets: list[str],
    *,
    start_ts: float,
    end_ts: float,
) -> set[int]:
    normalized = {_norm_wallet(wallet) for wallet in wallets if _norm_wallet(wallet)}
    windows = window_index.get("windows") if isinstance(window_index.get("windows"), dict) else {}
    out: set[int] = set()
    for key, indexed_wallets in windows.items():
        try:
            window_start = int(key)
        except (TypeError, ValueError):
            continue
        if not (start_ts <= window_start < end_ts):
            continue
        if isinstance(indexed_wallets, dict) and normalized.intersection(indexed_wallets.keys()):
            out.add(window_start)
    return out


def _int_set(values: Any) -> set[int]:
    if not isinstance(values, list):
        return set()
    out: set[int] = set()
    for value in values:
        try:
            out.add(int(value))
        except (TypeError, ValueError):
            continue
    return out


def _watcher_gap_roster_coverage(
    watcher_gap_report: dict[str, Any],
    no_copy_windows: set[int],
) -> tuple[set[int], dict[str, Any]]:
    summary = (
        watcher_gap_report.get("summary")
        if isinstance(watcher_gap_report.get("summary"), dict)
        else {}
    )
    covered = _int_set(
        summary.get("active_set_watcher_trade_no_copy_signal_windows_covered_starts")
        or summary.get("active_set_no_copy_signal_windows_covered_starts")
    )
    if not covered:
        return set(), {}
    covered_no_copy = no_copy_windows.intersection(covered)
    if not covered_no_copy:
        return set(), {}
    return covered_no_copy, {
        "source": "watcher_gap_report",
        "active_set_btc_trade_windows_total": int(
            num(
                summary.get("active_set_watcher_trade_btc_windows_total")
                or summary.get("active_set_btc_trade_windows_total"),
                0.0,
            )
        ),
    }


def _btc_window_start_from_slug(slug: Any) -> int | None:
    text = str(slug or "")
    if not text.startswith("btc-updown-5m-"):
        return None
    marker = text.rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


def _source_wallet_from_order(row: dict[str, Any]) -> str:
    source_event = row.get("source_event") if isinstance(row.get("source_event"), dict) else {}
    source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    trade_decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
    wallet_copy = trade_decision.get("wallet_copy") if isinstance(trade_decision.get("wallet_copy"), dict) else {}
    return _norm_wallet(
        row.get("source_wallet")
        or row.get("copy_source_wallet")
        or source_event.get("source_wallet")
        or source_event.get("wallet")
        or source_intent.get("source_wallet")
        or wallet_copy.get("source_wallet")
    )


def _copyintent_windows(
    live_execution: dict[str, Any],
    wallets: list[str],
    *,
    start_ts: float,
    end_ts: float,
) -> set[int]:
    normalized = {_norm_wallet(wallet) for wallet in wallets if _norm_wallet(wallet)}
    rows = live_execution.get("live_orders")
    if not isinstance(rows, list):
        rows = live_execution.get("orders") if isinstance(live_execution.get("orders"), list) else []
    out: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_wallet = _source_wallet_from_order(row)
        if source_wallet not in normalized:
            continue
        source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
        window_start = _btc_window_start_from_slug(row.get("market_slug") or source_intent.get("market_slug"))
        if window_start is None or not (start_ts <= window_start < end_ts):
            continue
        out.add(window_start)
    return out


def _live_order_counts(live_execution: dict[str, Any], wallet: str) -> dict[str, int]:
    wallet = _norm_wallet(wallet)
    rows = live_execution.get("live_orders")
    if not isinstance(rows, list):
        rows = live_execution.get("orders") if isinstance(live_execution.get("orders"), list) else []
    counts = {"orders": 0, "fills": 0, "rejects": 0, "submitted": 0}
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_wallet = _source_wallet_from_order(row)
        if source_wallet != wallet:
            continue
        counts["orders"] += 1
        status = str(row.get("final_status") or "").upper()
        if status == "FILLED":
            counts["fills"] += 1
        elif status == "REJECTED":
            counts["rejects"] += 1
        elif status == "SUBMITTED":
            counts["submitted"] += 1
    return counts


def _wallet_summary(
    wallet: str,
    wallet_windows: set[int],
    no_copy_windows: set[int],
    uncovered_no_copy: set[int],
    threshold: int,
) -> dict[str, Any]:
    covered_no_copy = sorted(no_copy_windows.intersection(wallet_windows))
    covered_uncovered = sorted(uncovered_no_copy.intersection(wallet_windows))
    return {
        "wallet": wallet,
        "btc_trade_windows": len(wallet_windows),
        "no_copy_signal_windows_with_wallet_signal": len(covered_no_copy),
        "uncovered_no_copy_signal_windows_with_wallet_signal": len(covered_uncovered),
        "trigger_threshold_distinct_uncovered_no_copy_windows": int(threshold),
        "trigger_met": len(covered_uncovered) >= int(threshold),
        "sample_uncovered_no_copy_windows": covered_uncovered[:20],
        "signal_coverage_source": "window_index",
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    scorecard = load_fresh_scorecard(args.scorecard)
    guard_state = load_json(args.guard_state, default={})
    window_index = load_json(args.window_index, default={})
    live_execution = load_json(args.live_execution, default={})
    watcher_gap_report = load_json(args.watcher_gap_report, default={})
    scorecard = scorecard if isinstance(scorecard, dict) else {}
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    window_index = window_index if isinstance(window_index, dict) else {}
    live_execution = live_execution if isinstance(live_execution, dict) else {}
    watcher_gap_report = watcher_gap_report if isinstance(watcher_gap_report, dict) else {}

    start_ts, scorecard_end_ts = _scorecard_window(scorecard)
    end_ts = float(args.end_ts) if float(getattr(args, "end_ts", 0.0) or 0.0) > 0 else scorecard_end_ts
    if end_ts <= 0:
        end_ts = time.time()
    active_wallets = _active_wallets(guard_state)
    candidate_wallet = _norm_wallet(args.candidate_wallet)
    focus_wallet = _norm_wallet(args.focus_wallet)
    no_copy_windows = _no_copy_windows(scorecard, start_ts=start_ts, end_ts=end_ts)
    roster_windows = _union_wallet_windows(window_index, active_wallets, start_ts=start_ts, end_ts=end_ts)
    watcher_roster_no_copy_covered, watcher_meta = _watcher_gap_roster_coverage(
        watcher_gap_report,
        no_copy_windows,
    )
    watcher_no_copy_covered = watcher_roster_no_copy_covered or no_copy_windows.intersection(roster_windows)
    copyintent_windows = _copyintent_windows(
        live_execution,
        active_wallets,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    copyintent_no_copy_covered = no_copy_windows.intersection(copyintent_windows)
    copyintent_uncovered_no_copy = no_copy_windows - copyintent_no_copy_covered
    roster_without_focus = [
        wallet for wallet in active_wallets if wallet != focus_wallet
    ]
    roster_without_focus_windows = _union_wallet_windows(
        window_index,
        roster_without_focus,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    watcher_uncovered_no_copy = no_copy_windows - watcher_no_copy_covered

    candidate_windows = _wallet_windows(window_index, candidate_wallet, start_ts=start_ts, end_ts=end_ts)
    focus_windows = _wallet_windows(window_index, focus_wallet, start_ts=start_ts, end_ts=end_ts)
    focus_incremental_no_copy = sorted(no_copy_windows.intersection(focus_windows - roster_without_focus_windows))

    return {
        "generated_at": utc_now_iso(),
        "kind": "wallet_copy_no_copy_signal_candidate_coverage",
        "flow_stage": "LIVE/PROMOTE/LEARN",
        "direction_id": str(args.direction_id),
        "window_label": str(getattr(args, "window_label", "") or ""),
        "field_semantics": {
            "watcher_trade_coverage": "active roster source wallet had a BTC 5m trade window in the history/watch data",
            "copyintent_coverage": "live execution emitted a CopyIntent/order for an active roster source wallet in the BTC 5m window",
            "trigger_basis": "candidate source trade windows inside no_copy_signal windows that active roster CopyIntent coverage did not cover",
        },
        "inputs": {
            "scorecard": str(args.scorecard),
            "guard_state": str(args.guard_state),
            "window_index": str(args.window_index),
            "live_execution": str(args.live_execution),
            "watcher_gap_report": str(args.watcher_gap_report),
            "end_ts_override": float(getattr(args, "end_ts", 0.0) or 0.0),
        },
        "window": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_iso": _iso(start_ts),
            "end_iso": _iso(end_ts),
        },
        "post_admission_roster": {
            "active_wallets": len(active_wallets),
            "watcher_trade_windows": (
                watcher_meta.get("active_set_btc_trade_windows_total")
                if watcher_meta
                else len(roster_windows)
            ),
            "no_copy_signal_windows_total": len(no_copy_windows),
            "watcher_trade_no_copy_signal_windows": len(watcher_no_copy_covered),
            "watcher_trade_no_copy_signal_windows_uncovered": len(watcher_uncovered_no_copy),
            "watcher_trade_coverage_source": watcher_meta.get("source") if watcher_meta else "window_index",
            "copyintent_windows": len(copyintent_windows),
            "copyintent_no_copy_signal_windows": len(copyintent_no_copy_covered),
            "copyintent_no_copy_signal_windows_missing": len(copyintent_uncovered_no_copy),
            "copyintent_coverage_source": "live_execution_orders",
            "sample_copyintent_missing_no_copy_windows": sorted(copyintent_uncovered_no_copy)[:20],
            "covered_definition": "copyintent coverage means a live execution order/CopyIntent exists for an active roster wallet in the BTC 5m window; watcher_trade coverage only means an active roster wallet had a BTC trade-window signal.",
        },
        "focus_wallet": {
            **_wallet_summary(
                focus_wallet,
                focus_windows,
                no_copy_windows,
                copyintent_uncovered_no_copy,
                int(args.trigger_threshold),
            ),
            "incremental_no_copy_windows_vs_roster_without_focus": len(focus_incremental_no_copy),
            "sample_incremental_no_copy_windows": focus_incremental_no_copy[:20],
            "live_counts": _live_order_counts(live_execution, focus_wallet),
        },
        "candidate": {
            **_wallet_summary(
                candidate_wallet,
                candidate_windows,
                no_copy_windows,
                copyintent_uncovered_no_copy,
                int(args.trigger_threshold),
            ),
            "queue_rank": 2,
            "live_counts": _live_order_counts(live_execution, candidate_wallet),
            "action": (
                "ADMIT_HALF_SIZE_PREAUTHORIZED"
                if len(copyintent_uncovered_no_copy.intersection(candidate_windows)) >= int(args.trigger_threshold)
                else "HOLD_UNTIL_QUALIFICATION_REFRESH_WINDOWS_GE_8"
            ),
        },
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps(report["candidate"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
