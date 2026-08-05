#!/usr/bin/env python3
"""Sample no-copy-signal BTC windows for active-set wallet activity.

Flow stage: LIVE/PROMOTE/SELF-DEV. The report answers Fable's volume
question: are no_copy_signal windows watcher misses, or did the active set
actually have no BTC-5m trades in those windows?
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.scorecard import load_fresh_scorecard


DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_no_copy_signal_watcher_gap_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text[:42] if text.startswith("0x") and len(text) >= 42 else ""


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "")
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _sample_evenly(rows: list[dict[str, Any]], sample_size: int) -> list[dict[str, Any]]:
    if sample_size <= 0 or len(rows) <= sample_size:
        return rows
    if sample_size == 1:
        return [rows[0]]
    last = len(rows) - 1
    indexes = [round(i * last / (sample_size - 1)) for i in range(sample_size)]
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    for index in indexes:
        if index not in seen:
            out.append(rows[index])
            seen.add(index)
    return out


def _active_wallets(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    out: list[dict[str, Any]] = []
    for member in members:
        if not isinstance(member, dict):
            continue
        wallet = _norm_wallet(member.get("source_wallet"))
        if not wallet:
            continue
        out.append(
            {
                "wallet": wallet,
                "candidate_id": str(member.get("candidate_id") or ""),
                "policy_id": str(member.get("policy_id") or ""),
                "is_current_cycle_member": bool(member.get("is_current_cycle_member")),
            }
        )
    return out


def _scorecard_window(scorecard: dict[str, Any]) -> tuple[float, float]:
    window = scorecard.get("window") if isinstance(scorecard.get("window"), dict) else {}
    start = num(window.get("start_ts"), 0.0)
    end = num(window.get("end_ts"), 0.0)
    return float(start), float(end)


def _no_copy_windows(scorecard: dict[str, Any]) -> list[dict[str, Any]]:
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    missed = (
        volume.get("missed_window_attribution")
        if isinstance(volume.get("missed_window_attribution"), dict)
        else {}
    )
    rows = missed.get("rows") if isinstance(missed.get("rows"), list) else []
    out = [row for row in rows if isinstance(row, dict) and row.get("attribution") == "no_copy_signal"]
    return sorted(out, key=lambda row: num(row.get("window_start_s"), 0.0))


def _fetch_wallet_trades(
    wallet: str,
    *,
    start_ts: float,
    end_ts: float,
    limit: int,
    max_pages: int,
    timeout_s: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    for page in range(max_pages):
        query = urllib.parse.urlencode({"user": wallet, "takerOnly": "false", "limit": int(limit), "offset": page * int(limit)})
        url = f"https://data-api.polymarket.com/trades?{query}"
        urls.append(url)
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError) as exc:
            return rows, {
                "pages": page + 1,
                "truncated": False,
                "errored": True,
                "error": f"{type(exc).__name__}: {exc}",
                "urls": urls,
            }
        page_rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        for row in page_rows:
            ts = _parse_ts(row.get("timestamp"))
            if start_ts <= ts < end_ts:
                rows.append(row)
        if len(page_rows) < int(limit):
            return rows, {"pages": page + 1, "truncated": False, "urls": urls}
        oldest = min((_parse_ts(row.get("timestamp")) for row in page_rows), default=0.0)
        if oldest and oldest < start_ts:
            return rows, {"pages": page + 1, "truncated": False, "urls": urls}
    return rows, {"pages": int(max_pages), "truncated": True, "urls": urls}


def _btc_window_start_from_slug(slug: str) -> int | None:
    text = str(slug or "")
    if not text.startswith("btc-updown-5m-"):
        return None
    marker = text.rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


def classify_sampled_windows(
    sampled_windows: list[dict[str, Any]],
    wallet_trades: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    trades_by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for wallet, trades in wallet_trades.items():
        for trade in trades:
            slug_start = _btc_window_start_from_slug(str(trade.get("slug") or ""))
            if slug_start is None:
                continue
            row = dict(trade)
            row["_wallet"] = wallet
            trades_by_window[int(slug_start)].append(row)

    rows: list[dict[str, Any]] = []
    for window in sampled_windows:
        start = int(num(window.get("window_start_s"), 0.0))
        trades = trades_by_window.get(start, [])
        wallets = sorted({_norm_wallet(trade.get("proxyWallet") or trade.get("_wallet")) for trade in trades})
        rows.append(
            {
                "window_start_s": start,
                "market_slug": str(window.get("market_slug") or f"btc-updown-5m-{start}"),
                "active_wallet_trade_count": len(trades),
                "active_wallets_with_trades": [wallet for wallet in wallets if wallet],
                "classification": "WATCHER_GAP" if trades else "COVERAGE_GAP",
                "sample_trades": [
                    {
                        "wallet": _norm_wallet(trade.get("proxyWallet") or trade.get("_wallet")),
                        "tx": str(trade.get("transactionHash") or ""),
                        "side": str(trade.get("side") or ""),
                        "outcome": str(trade.get("outcome") or ""),
                        "size": num(trade.get("size"), 0.0),
                        "price": num(trade.get("price"), 0.0),
                        "timestamp": _parse_ts(trade.get("timestamp")),
                    }
                    for trade in trades[:10]
                ],
            }
        )
    return rows


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    scorecard = load_fresh_scorecard(args.scorecard)
    guard_state = load_json(args.guard_state, default={})
    scorecard = scorecard if isinstance(scorecard, dict) else {}
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    start_ts, end_ts = _scorecard_window(scorecard)
    no_copy_rows = _no_copy_windows(scorecard)
    sampled = _sample_evenly(no_copy_rows, int(args.sample_size))
    active_wallets = _active_wallets(guard_state)
    wallet_trades: dict[str, list[dict[str, Any]]] = {}
    fetch_meta: dict[str, Any] = {}
    for member in active_wallets:
        wallet = str(member["wallet"])
        trades, meta = _fetch_wallet_trades(
            wallet,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=int(args.limit),
            max_pages=int(args.max_pages),
            timeout_s=float(args.timeout_s),
        )
        wallet_trades[wallet] = trades
        fetch_meta[wallet] = {key: value for key, value in meta.items() if key != "urls"}
    rows = classify_sampled_windows(sampled, wallet_trades)
    counts = Counter(row["classification"] for row in rows)
    no_copy_window_starts = {
        int(start)
        for row in no_copy_rows
        if (start := num(row.get("window_start_s"), 0.0))
    }
    per_wallet_coverage: list[dict[str, Any]] = []
    active_no_copy_covered: set[int] = set()
    for member in active_wallets:
        wallet = str(member["wallet"])
        btc_trade_windows = {
            int(start)
            for trade in wallet_trades.get(wallet, [])
            if (start := _btc_window_start_from_slug(str(trade.get("slug") or ""))) is not None
        }
        no_copy_covered = sorted(btc_trade_windows & no_copy_window_starts)
        active_no_copy_covered.update(no_copy_covered)
        per_wallet_coverage.append(
            {
                "wallet": wallet,
                "candidate_id": member["candidate_id"],
                "policy_id": member["policy_id"],
                "btc_trade_windows": len(btc_trade_windows),
                "no_copy_signal_windows_covered": len(no_copy_covered),
                "sample_no_copy_signal_windows_covered": no_copy_covered[:20],
            }
        )
    wallet_trade_windows = {
        int(start)
        for trades in wallet_trades.values()
        for trade in trades
        if (start := _btc_window_start_from_slug(str(trade.get("slug") or ""))) is not None
    }
    return {
        "generated_at": utc_now_iso(),
        "kind": "wallet_copy_no_copy_signal_watcher_gap",
        "flow_stage": "LIVE/PROMOTE/SELF-DEV",
        "inputs": {"scorecard": str(args.scorecard), "guard_state": str(args.guard_state)},
        "window": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_iso": datetime.fromtimestamp(start_ts, tz=UTC).isoformat().replace("+00:00", "Z") if start_ts else "",
            "end_iso": datetime.fromtimestamp(end_ts, tz=UTC).isoformat().replace("+00:00", "Z") if end_ts else "",
        },
        "summary": {
            "no_copy_signal_windows_total": len(no_copy_rows),
            "sampled_windows": len(rows),
            "watcher_gap_windows": int(counts.get("WATCHER_GAP", 0)),
            "coverage_gap_windows": int(counts.get("COVERAGE_GAP", 0)),
            "watcher_gap_pct": round(100.0 * int(counts.get("WATCHER_GAP", 0)) / len(rows), 6) if rows else 0.0,
            "active_set_wallets": len(active_wallets),
            "coverage_semantics": "watcher_trade: active roster source wallet had a BTC 5m trade window; this is not CopyIntent coverage",
            "active_set_watcher_trade_btc_windows_total": len(wallet_trade_windows),
            "active_set_watcher_trade_no_copy_signal_windows_covered": len(active_no_copy_covered),
            "active_set_watcher_trade_no_copy_signal_windows_uncovered": max(
                0,
                len(no_copy_window_starts) - len(active_no_copy_covered),
            ),
            "active_set_watcher_trade_no_copy_signal_windows_covered_starts": sorted(active_no_copy_covered),
            "active_set_watcher_trade_no_copy_signal_windows_uncovered_starts": sorted(
                no_copy_window_starts - active_no_copy_covered
            ),
            "active_set_btc_trade_windows_total": len(wallet_trade_windows),
            "active_set_no_copy_signal_windows_covered": len(active_no_copy_covered),
            "active_set_no_copy_signal_windows_uncovered": max(0, len(no_copy_window_starts) - len(active_no_copy_covered)),
            "active_set_no_copy_signal_windows_covered_starts": sorted(active_no_copy_covered),
            "active_set_no_copy_signal_windows_uncovered_starts": sorted(no_copy_window_starts - active_no_copy_covered),
            "classification": "watcher_gap_if_active_wallet_trade_exists_inside_sampled_no_copy_signal_window",
        },
        "active_wallets": active_wallets,
        "per_wallet_coverage": per_wallet_coverage,
        "trade_fetch": fetch_meta,
        "rows": rows,
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
