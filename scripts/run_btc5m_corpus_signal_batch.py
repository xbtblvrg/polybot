#!/usr/bin/env python3
"""Run BTC-5m corpus signal studies over the RTDS firehose.

Flow stage: LEARN/PROMOTE. This is offline evidence only: it reads persisted
RTDS and resolution files, writes an EV report, and never touches live or paper
execution state.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_RTDS = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = "data/research/btc5m_corpus_signal_batch_20260707.json"

OUTCOMES = ("Up", "Down")
FLOW_CUTOFFS_S = (15.0, 30.0, 60.0, 120.0)
FLOW_MIN_USD = (25.0, 50.0, 100.0, 250.0)
DOMINANCE = (0.55, 0.60, 0.65, 0.70)
ENTRY_CAPS = (0.45, 0.50, 0.60, 0.75)
WALLET_PRICE_CAPS = (0.40, 0.50, 0.60, 0.75)
FIXED_ENTRY_PRICES = (0.45, 0.50, 0.55)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--state", default="data/research/btc5m_corpus_signal_batch_state.json")
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--max-bytes", type=int, default=0, help="0 means scan the whole RTDS file.")
    parser.add_argument("--max-lines", type=int, default=0, help="0 means no line limit.")
    parser.add_argument("--max-samples", type=int, default=20)
    return parser.parse_args()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return ""


def _outcome(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"up", "yes"}:
        return "Up"
    if text in {"down", "no"}:
        return "Down"
    return ""


def _winner(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text == "UP":
        return "Up"
    if text == "DOWN":
        return "Down"
    return ""


def _opposite(outcome: str) -> str:
    return "Down" if outcome == "Up" else "Up" if outcome == "Down" else ""


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _load_resolutions(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    diagnostics: Counter[str] = Counter()
    with path.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            diagnostics["rows_seen"] += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                diagnostics["bad_json"] += 1
                continue
            if str(row.get("asset") or "").upper() != "BTC":
                diagnostics["non_btc"] += 1
                continue
            if str(row.get("window_type") or "").lower() != "5m":
                diagnostics["non_5m"] += 1
                continue
            slug = str(row.get("market_slug") or "")
            winner = _winner(row.get("direction"))
            if not slug.startswith("btc-updown-5m-") or not winner:
                diagnostics["unusable_btc5m"] += 1
                continue
            start = int(_num(row.get("window_start_unix_ts") or slug.rsplit("-", 1)[-1], 0.0))
            rows[slug] = {
                "market_slug": slug,
                "winner": winner,
                "window_start_s": start,
                "hour_utc": datetime.fromtimestamp(start, tz=UTC).hour,
            }
            diagnostics["accepted"] += 1
    starts = [int(row["window_start_s"]) for row in rows.values()]
    return rows, {
        "path": str(path),
        "diagnostics": dict(sorted(diagnostics.items())),
        "resolved_windows": len(rows),
        "min_window_start_s": min(starts) if starts else None,
        "max_window_start_s": max(starts) if starts else None,
    }


def _active_wallets(path: Path) -> dict[str, dict[str, Any]]:
    guard = _load_json(path, {})
    active = guard.get("active_set") if isinstance(guard, dict) and isinstance(guard.get("active_set"), dict) else {}
    members = active.get("members") if isinstance(active.get("members"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for member in members:
        if not isinstance(member, dict):
            continue
        wallet = _norm_wallet(member.get("source_wallet") or member.get("wallet"))
        if not wallet:
            continue
        out[wallet] = {
            "candidate_id": member.get("candidate_id"),
            "policy_id": member.get("policy_id"),
            "status": member.get("status"),
        }
    return out


def _event_from_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("event") != "rtds_trade_event":
        return None
    slug = str(row.get("market_slug") or row.get("raw", {}).get("slug") or "")
    if not slug.startswith("btc-updown-5m-"):
        return None
    outcome = _outcome(row.get("outcome") or row.get("raw", {}).get("outcome"))
    side = str(row.get("side") or row.get("raw", {}).get("side") or "").upper()
    price = _num(row.get("price"))
    size = _num(row.get("size"))
    event_ts = _num(row.get("event_ts") or row.get("raw", {}).get("timestamp"))
    wallet = _norm_wallet(row.get("source_wallet") or row.get("raw", {}).get("proxyWallet"))
    if not outcome or side not in {"BUY", "SELL"} or price <= 0.0 or size <= 0.0 or event_ts <= 0.0:
        return None
    return {
        "market_slug": slug,
        "outcome": outcome,
        "side": side,
        "price": price,
        "size": size,
        "usd": price * size,
        "event_ts": event_ts,
        "wallet": wallet,
    }


def _blank_agg() -> dict[str, Any]:
    return {
        "event_count": 0,
        "buy_usd": defaultdict(lambda: defaultdict(float)),
        "sell_usd": defaultdict(lambda: defaultdict(float)),
        "buy_size": defaultdict(lambda: defaultdict(float)),
        "buy_price_size": defaultdict(lambda: defaultdict(float)),
        "sell_size": defaultdict(lambda: defaultdict(float)),
        "sell_price_size": defaultdict(lambda: defaultdict(float)),
        "unique_wallets": set(),
    }


def _entry_pnl(outcome: str, winner: str, entry_price: float, order_usd: float) -> float:
    if entry_price <= 0.0:
        return 0.0
    shares = order_usd / entry_price
    return round((shares if outcome == winner else 0.0) - order_usd, 6)


def _blank_metric() -> dict[str, Any]:
    return {"trades": 0, "wins": 0, "cost_usd": 0.0, "pnl_usd": 0.0}


def _update_metric(metric: dict[str, Any], pnl: float, cost_usd: float) -> None:
    metric["trades"] += 1
    metric["cost_usd"] = round(float(metric["cost_usd"]) + cost_usd, 6)
    metric["pnl_usd"] = round(float(metric["pnl_usd"]) + pnl, 6)
    if pnl > 0.0:
        metric["wins"] += 1


def _add_result(
    grid: dict[str, dict[str, Any]],
    key: str,
    pnl: float,
    sample: dict[str, Any],
    max_samples: int,
    *,
    partition: str = "test",
) -> None:
    row = grid.setdefault(
        key,
        {
            "trades": 0,
            "wins": 0,
            "cost_usd": 0.0,
            "pnl_usd": 0.0,
            "train": _blank_metric(),
            "test": _blank_metric(),
            "samples": [],
        },
    )
    cost_usd = float(sample.get("cost_usd") or 0.0)
    _update_metric(row, pnl, cost_usd)
    if partition not in {"train", "test"}:
        partition = "test"
    _update_metric(row[partition], pnl, cost_usd)
    if len(row["samples"]) < max_samples:
        row["samples"].append({**sample, "partition": partition})


def _finalize_grid(grid: dict[str, dict[str, Any]], *, min_trades_for_positive: int = 30) -> dict[str, Any]:
    rows = []
    for key, row in grid.items():
        trades = int(row.get("trades") or 0)
        cost = float(row.get("cost_usd") or 0.0)
        pnl = float(row.get("pnl_usd") or 0.0)
        test = row.get("test") if isinstance(row.get("test"), dict) else _blank_metric()
        train = row.get("train") if isinstance(row.get("train"), dict) else _blank_metric()
        out = dict(row)
        out["key"] = key
        out["wr_pct"] = round(100.0 * int(row.get("wins") or 0) / trades, 6) if trades else 0.0
        out["roi_pct"] = round(100.0 * pnl / cost, 6) if cost > 0 else 0.0
        for label, metric in (("train", train), ("test", test)):
            label_trades = int(metric.get("trades") or 0)
            label_cost = float(metric.get("cost_usd") or 0.0)
            label_pnl = float(metric.get("pnl_usd") or 0.0)
            metric["wr_pct"] = round(100.0 * int(metric.get("wins") or 0) / label_trades, 6) if label_trades else 0.0
            metric["roi_pct"] = round(100.0 * label_pnl / label_cost, 6) if label_cost > 0 else 0.0
            out[label] = metric
        out["oos_trades"] = int(test.get("trades") or 0)
        out["oos_pnl_usd"] = round(float(test.get("pnl_usd") or 0.0), 6)
        out["oos_roi_pct"] = round(float(test.get("roi_pct") or 0.0), 6)
        rows.append(out)
    rows.sort(key=lambda item: (item["oos_roi_pct"], item["oos_pnl_usd"], item["oos_trades"]), reverse=True)
    best = rows[0] if rows else {}
    status = "NO_SAMPLES"
    if rows:
        status = (
            "POSITIVE_OOS_REGION"
            if int(best.get("oos_trades") or 0) >= min_trades_for_positive
            and float(best.get("oos_pnl_usd") or 0.0) > 0
            else "KILL_NO_POSITIVE_OOS_REGION"
        )
    return {"status": status, "best": best, "grid": rows[:200]}


def _split_start_s(resolutions: dict[str, dict[str, Any]], train_fraction: float = 0.70) -> int:
    starts = sorted(int(row["window_start_s"]) for row in resolutions.values())
    if not starts:
        return 0
    index = min(len(starts) - 1, max(0, int(len(starts) * train_fraction)))
    return starts[index]


def _partition(window_start_s: int | float, split_start_s: int) -> str:
    return "train" if split_start_s and float(window_start_s) < float(split_start_s) else "test"


def scan_corpus(
    *,
    rtds_path: Path,
    resolutions: dict[str, dict[str, Any]],
    active_wallets: dict[str, dict[str, Any]],
    max_bytes: int,
    max_lines: int,
    order_usd: float,
    tick_size: float,
    state_path: Path,
    split_start_s: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, dict[str, Any]]]:
    windows: dict[str, dict[str, Any]] = defaultdict(_blank_agg)
    active_wallet_grid: dict[str, dict[str, Any]] = {}
    diagnostics: Counter[str] = Counter()
    bytes_seen = 0
    file_size = rtds_path.stat().st_size if rtds_path.exists() else 0
    state_every = 250_000
    with rtds_path.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            diagnostics["lines_seen"] += 1
            bytes_seen += len(line.encode("utf-8", errors="ignore"))
            if max_lines and diagnostics["lines_seen"] > max_lines:
                diagnostics["line_limit_reached"] += 1
                break
            if max_bytes and bytes_seen > max_bytes:
                diagnostics["byte_limit_reached"] += 1
                break
            if "rtds_trade_event" not in line:
                diagnostics["non_trade_line"] += 1
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                diagnostics["bad_json"] += 1
                continue
            event = _event_from_row(raw)
            if event is None:
                diagnostics["unusable_trade"] += 1
                continue
            resolved = resolutions.get(event["market_slug"])
            if resolved is None:
                diagnostics["unresolved_or_out_of_scope"] += 1
                continue
            offset = float(event["event_ts"]) - float(resolved["window_start_s"])
            if offset < 0.0 or offset >= 300.0:
                diagnostics["outside_window"] += 1
                continue
            agg = windows[event["market_slug"]]
            agg["event_count"] += 1
            if event["wallet"]:
                agg["unique_wallets"].add(event["wallet"])
            for cutoff in FLOW_CUTOFFS_S:
                if offset > cutoff:
                    continue
                side_key = "buy" if event["side"] == "BUY" else "sell"
                agg[f"{side_key}_usd"][cutoff][event["outcome"]] += float(event["usd"])
                agg[f"{side_key}_size"][cutoff][event["outcome"]] += float(event["size"])
                agg[f"{side_key}_price_size"][cutoff][event["outcome"]] += float(event["price"]) * float(event["size"])
            wallet = event["wallet"]
            if event["side"] == "BUY" and wallet in active_wallets:
                winner = str(resolved["winner"])
                for cutoff in FLOW_CUTOFFS_S:
                    if offset > cutoff:
                        continue
                    for cap in WALLET_PRICE_CAPS:
                        entry = min(0.99, float(event["price"]) + tick_size)
                        if entry > cap:
                            continue
                        key = f"wallet={wallet}|cutoff={int(cutoff)}|cap={cap:.2f}"
                        pnl = _entry_pnl(event["outcome"], winner, entry, order_usd)
                        _add_result(
                            active_wallet_grid,
                            key,
                            pnl,
                            {
                                "market_slug": event["market_slug"],
                                "wallet": wallet,
                                "outcome": event["outcome"],
                                "winner": winner,
                                "offset_s": round(offset, 3),
                                "entry_price": round(entry, 6),
                                "cost_usd": order_usd,
                                "pnl_usd": pnl,
                            },
                            8,
                            partition=_partition(resolved["window_start_s"], split_start_s),
                        )
            diagnostics["accepted_trade"] += 1
            if diagnostics["lines_seen"] % state_every == 0:
                atomic_write_json(
                    state_path,
                    {
                        "kind": "btc5m_corpus_signal_batch_state",
                        "flow_stage": "LEARN/PROMOTE",
                        "paper_only": True,
                        "live_orders_allowed": False,
                        "updated_at": _utc_now(),
                        "rtds_jsonl": str(rtds_path),
                        "file_size_bytes": file_size,
                        "bytes_seen": bytes_seen,
                        "progress_pct": round(100.0 * bytes_seen / file_size, 6) if file_size else 0.0,
                        "diagnostics": dict(sorted(diagnostics.items())),
                        "resolved_windows_with_events": len(windows),
                    },
                )
    return windows, {
        "rtds_jsonl": str(rtds_path),
        "file_size_bytes": file_size,
        "bytes_seen": bytes_seen,
        "progress_pct": round(100.0 * bytes_seen / file_size, 6) if file_size else 0.0,
        "diagnostics": dict(sorted(diagnostics.items())),
        "resolved_windows_with_events": len(windows),
    }, active_wallet_grid


def _avg_price(agg: dict[str, Any], side: str, cutoff: float, outcome: str) -> float:
    size = float(agg[f"{side}_size"][cutoff][outcome])
    if size <= 0.0:
        return 0.0
    return float(agg[f"{side}_price_size"][cutoff][outcome]) / size


def _flow_grids(
    windows: dict[str, dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    order_usd: float,
    tick_size: float,
    max_samples: int,
    split_start_s: int,
) -> dict[str, Any]:
    e4: dict[str, dict[str, Any]] = {}
    e9: dict[str, dict[str, Any]] = {}
    e12: dict[str, dict[str, Any]] = {}
    e13: dict[str, dict[str, Any]] = {}
    for slug, agg in windows.items():
        winner = resolutions[slug]["winner"]
        partition = _partition(resolutions[slug]["window_start_s"], split_start_s)
        for cutoff in FLOW_CUTOFFS_S:
            buy = {outcome: float(agg["buy_usd"][cutoff][outcome]) for outcome in OUTCOMES}
            sell = {outcome: float(agg["sell_usd"][cutoff][outcome]) for outcome in OUTCOMES}
            total_buy = buy["Up"] + buy["Down"]
            total_sell = sell["Up"] + sell["Down"]
            if total_buy > 0.0:
                dominant = "Up" if buy["Up"] >= buy["Down"] else "Down"
                dominance = buy[dominant] / total_buy
                avg = _avg_price(agg, "buy", cutoff, dominant)
                for min_flow in FLOW_MIN_USD:
                    if total_buy < min_flow:
                        continue
                    for min_dom in DOMINANCE:
                        if dominance < min_dom:
                            continue
                        for cap in ENTRY_CAPS:
                            entry = min(0.99, avg + tick_size)
                            if entry <= 0.0 or entry > cap:
                                continue
                            pnl = _entry_pnl(dominant, winner, entry, order_usd)
                            sample = {
                                "market_slug": slug,
                                "outcome": dominant,
                                "winner": winner,
                                "cutoff_s": cutoff,
                                "total_buy_usd": round(total_buy, 6),
                                "dominance": round(dominance, 6),
                                "entry_price": round(entry, 6),
                                "cost_usd": order_usd,
                                "pnl_usd": pnl,
                            }
                            key = f"cutoff={int(cutoff)}|flow>={min_flow:.0f}|dom>={min_dom:.2f}|cap={cap:.2f}"
                            _add_result(e4, key, pnl, sample, max_samples, partition=partition)
                            _add_result(e12, key, pnl, sample, max_samples, partition=partition)
                            fade = _opposite(dominant)
                            fade_avg = _avg_price(agg, "buy", cutoff, fade)
                            fade_entry = min(0.99, (fade_avg if fade_avg > 0.0 else 1.0 - avg) + tick_size)
                            if avg >= 0.55 and fade_entry > 0.0 and fade_entry <= cap:
                                fade_pnl = _entry_pnl(fade, winner, fade_entry, order_usd)
                                fade_sample = dict(sample, outcome=fade, entry_price=round(fade_entry, 6), pnl_usd=fade_pnl)
                                _add_result(
                                    e9,
                                    key + "|dominant_price>=0.55",
                                    fade_pnl,
                                    fade_sample,
                                    max_samples,
                                    partition=partition,
                                )
            if total_sell > 0.0:
                sold = "Up" if sell["Up"] >= sell["Down"] else "Down"
                inverse = _opposite(sold)
                dominance = sell[sold] / total_sell
                sell_avg = _avg_price(agg, "sell", cutoff, sold)
                inverse_avg = _avg_price(agg, "buy", cutoff, inverse)
                entry = min(0.99, (inverse_avg if inverse_avg > 0.0 else 1.0 - sell_avg) + tick_size)
                for min_flow in FLOW_MIN_USD:
                    if total_sell < min_flow:
                        continue
                    for min_dom in DOMINANCE:
                        if dominance < min_dom:
                            continue
                        for cap in ENTRY_CAPS:
                            if entry <= 0.0 or entry > cap:
                                continue
                            pnl = _entry_pnl(inverse, winner, entry, order_usd)
                            sample = {
                                "market_slug": slug,
                                "sold_outcome": sold,
                                "outcome": inverse,
                                "winner": winner,
                                "cutoff_s": cutoff,
                                "total_sell_usd": round(total_sell, 6),
                                "dominance": round(dominance, 6),
                                "entry_price": round(entry, 6),
                                "cost_usd": order_usd,
                                "pnl_usd": pnl,
                            }
                            key = f"cutoff={int(cutoff)}|sell>={min_flow:.0f}|dom>={min_dom:.2f}|cap={cap:.2f}"
                            _add_result(e13, key, pnl, sample, max_samples, partition=partition)
    return {
        "E4_inventory_flow": _finalize_grid(e4),
        "E6_whale_flow_recalibration": _finalize_grid(e4),
        "E9_early_overreaction_fade": _finalize_grid(e9),
        "E12_trade_flow_imbalance_proxy": _finalize_grid(e12),
        "E13_whale_exit_inverse": _finalize_grid(e13),
    }


def _cross_window_momentum(
    resolutions: dict[str, dict[str, Any]],
    order_usd: float,
    max_samples: int,
    split_start_s: int,
) -> dict[str, Any]:
    momentum: dict[str, dict[str, Any]] = {}
    contrarian: dict[str, dict[str, Any]] = {}
    ordered = sorted(resolutions.values(), key=lambda row: int(row["window_start_s"]))
    prev: dict[str, Any] | None = None
    for row in ordered:
        if prev is None:
            prev = row
            continue
        if int(row["window_start_s"]) - int(prev["window_start_s"]) != 300:
            prev = row
            continue
        for entry in FIXED_ENTRY_PRICES:
            partition = _partition(row["window_start_s"], split_start_s)
            outcome = str(prev["winner"])
            pnl = _entry_pnl(outcome, str(row["winner"]), entry, order_usd)
            sample = {
                "market_slug": row["market_slug"],
                "previous_winner": outcome,
                "winner": row["winner"],
                "entry_price": entry,
                "cost_usd": order_usd,
                "pnl_usd": pnl,
            }
            _add_result(momentum, f"entry={entry:.2f}", pnl, sample, max_samples, partition=partition)
            inverse = _opposite(outcome)
            inv_pnl = _entry_pnl(inverse, str(row["winner"]), entry, order_usd)
            inv_sample = dict(sample, outcome=inverse, pnl_usd=inv_pnl)
            _add_result(contrarian, f"entry={entry:.2f}", inv_pnl, inv_sample, max_samples, partition=partition)
        prev = row
    result = _finalize_grid(momentum)
    result["contrarian"] = _finalize_grid(contrarian)
    return result


def _hour_seasonality(
    resolutions: dict[str, dict[str, Any]],
    order_usd: float,
    max_samples: int,
    split_start_s: int,
) -> dict[str, Any]:
    grid: dict[str, dict[str, Any]] = {}
    history: dict[int, Counter[str]] = defaultdict(Counter)
    ordered = sorted(resolutions.values(), key=lambda row: int(row["window_start_s"]))
    for row in ordered:
        hour = int(row["hour_utc"])
        prior = history[hour]
        if sum(prior.values()) >= 10:
            predicted = "Up" if prior["Up"] >= prior["Down"] else "Down"
            for entry in FIXED_ENTRY_PRICES:
                pnl = _entry_pnl(predicted, str(row["winner"]), entry, order_usd)
                sample = {
                    "market_slug": row["market_slug"],
                    "hour_utc": hour,
                    "prior_up": prior["Up"],
                    "prior_down": prior["Down"],
                    "outcome": predicted,
                    "winner": row["winner"],
                    "entry_price": entry,
                    "cost_usd": order_usd,
                    "pnl_usd": pnl,
                }
                _add_result(
                    grid,
                    f"hour={hour}|entry={entry:.2f}",
                    pnl,
                    sample,
                    max_samples,
                    partition=_partition(row["window_start_s"], split_start_s),
                )
        history[hour][str(row["winner"])] += 1
    return _finalize_grid(grid)


def _input_gap(name: str, reason: str, next_action: str) -> dict[str, Any]:
    return {
        "status": "INPUT_GAP",
        "kill_verdict": "NOT_TESTED",
        "reason": reason,
        "next_action": next_action,
        "grid": [],
        "best": {},
        "name": name,
    }


def main() -> int:
    args = parse_args()
    rtds_path = Path(args.rtds_jsonl)
    resolutions, resolution_summary = _load_resolutions(Path(args.resolutions))
    split_start = _split_start_s(resolutions)
    active = _active_wallets(Path(args.guard_state))
    windows, scan_summary, active_wallet_grid = scan_corpus(
        rtds_path=rtds_path,
        resolutions=resolutions,
        active_wallets=active,
        max_bytes=int(args.max_bytes),
        max_lines=int(args.max_lines),
        order_usd=float(args.order_usd),
        tick_size=float(args.tick_size),
        state_path=Path(args.state),
        split_start_s=split_start,
    )
    flow = _flow_grids(
        windows,
        resolutions,
        order_usd=float(args.order_usd),
        tick_size=float(args.tick_size),
        max_samples=int(args.max_samples),
        split_start_s=split_start,
    )
    report = {
        "kind": "btc5m_corpus_signal_batch",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now(),
        "inputs": {
            "rtds_jsonl": str(rtds_path),
            "resolutions": str(args.resolutions),
            "guard_state": str(args.guard_state),
            "order_usd": float(args.order_usd),
            "tick_size": float(args.tick_size),
            "max_bytes": int(args.max_bytes),
            "max_lines": int(args.max_lines),
            "train_fraction": 0.70,
            "split_start_s": split_start,
        },
        "corpus_availability": {
            "rtds_size_bytes": rtds_path.stat().st_size if rtds_path.exists() else 0,
            "rtds_size_gib": round((rtds_path.stat().st_size if rtds_path.exists() else 0) / (1024.0**3), 6),
            "data_research_size_gib": round(
                sum(path.stat().st_size for path in (ROOT / "data/research").glob("**/*") if path.is_file()) / (1024.0**3),
                6,
            ),
        },
        "resolution_summary": resolution_summary,
        "scan_summary": scan_summary,
        "active_wallets": active,
        "studies": {
            **flow,
            "COPY_DRIP_DENSITY_active_members": _finalize_grid(active_wallet_grid, min_trades_for_positive=10),
            "E10_fair_value_model": _input_gap(
                "E10_fair_value_model",
                "Raw per-second BTC spot path and volatility features are not present in the RTDS corpus file.",
                "reuse/fetch Binance kline/tick cache and join by window for fair-value modelling",
            ),
            "E11_cross_window_momentum": _cross_window_momentum(
                resolutions,
                float(args.order_usd),
                int(args.max_samples),
                split_start,
            ),
            "E14_hour_seasonality": _hour_seasonality(
                resolutions,
                float(args.order_usd),
                int(args.max_samples),
                split_start,
            ),
            "E15_cross_asset_spillover": _input_gap(
                "E15_cross_asset_spillover",
                "No ETH 5m resolution/input stream was found in the BTC resolution file.",
                "locate or build ETH 5m resolution and spot cache before testing cross-asset spillover",
            ),
            "LP_maker_rewards": _input_gap(
                "LP_maker_rewards",
                "Reward terms and inventory toxicity data are not encoded in the RTDS trade stream.",
                "join CLOB reward parameters and E5 maker fill/toxicity rows for the EOD LP verdict",
            ),
        },
    }
    atomic_write_json(Path(args.output), report)
    atomic_write_json(
        Path(args.state),
        {
            "kind": "btc5m_corpus_signal_batch_state",
            "flow_stage": "LEARN/PROMOTE",
            "paper_only": True,
            "live_orders_allowed": False,
            "updated_at": _utc_now(),
            "status": "COMPLETE",
            "output": str(args.output),
            "scan_summary": scan_summary,
        },
    )
    best_bits = []
    for name, study in report["studies"].items():
        best = study.get("best") if isinstance(study, dict) else {}
        best_bits.append(f"{name}:{study.get('status')}:{best.get('trades', 0) if isinstance(best, dict) else 0}")
    print("btc5m_corpus_signal_batch", " ".join(best_bits[:8]), f"output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
