#!/usr/bin/env python3
"""Score wallets by whether early commitment is followable.

Flow stage: LEARN. Followability asks whether a wallet's first-60s side and
size predict its own final window position and the resolved outcome. High
scores become candidates for native follow engines that ladder ahead of the
wallet's continuation instead of chasing fills.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_strategy_decompiler_intake import (
    _default_resolutions_path,
    _float,
    _load_json,
    _load_resolutions,
    _norm_outcome,
    _parse_ts,
    _utc_now_iso,
)


DEFAULT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_followability_leaderboard_latest.json"


def _window_start(row: dict[str, Any]) -> float:
    explicit = _parse_ts(row.get("window_start_s"))
    if explicit > 0:
        return explicit
    slug = str(row.get("market_slug") or "")
    match = re.search(r"-(\d{10})(?:$|[^0-9])", slug)
    if match:
        return float(match.group(1))
    ts = _parse_ts(row.get("event_ts") or row.get("observed_ts"))
    return math.floor(ts / 300.0) * 300.0 if ts > 0 else 0.0


def _stake(row: dict[str, Any]) -> float:
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    return _float(row.get("usdc_size"), 0.0) or (price * size if price > 0 and size > 0 else 0.0)


def _event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    wallet = str(row.get("source_wallet") or "").lower()
    condition = str(row.get("condition_id") or "")
    market = str(row.get("market_slug") or "")
    return wallet, condition, market


def _final_side(stake_by_side: dict[str, float]) -> str:
    if not stake_by_side:
        return ""
    return max(stake_by_side.items(), key=lambda item: (item[1], item[0]))[0]


def _iso(ts: float | None) -> str | None:
    if not ts or ts <= 0:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _wallet_stats(wallet: str) -> dict[str, Any]:
    return {
        "wallet": wallet,
        "eligible_windows": 0,
        "early_final_same_side_windows": 0,
        "early_win_windows": 0,
        "continuation_windows": 0,
        "early_stake_usd": 0.0,
        "final_stake_usd": 0.0,
        "continuation_same_side_usd": 0.0,
        "first_event_ts": None,
        "latest_event_ts": None,
        "conditions": set(),
        "markets": set(),
        "examples": [],
    }


def _add_window(stats: dict[str, Any], row: dict[str, Any]) -> None:
    stats["eligible_windows"] += 1
    stats["early_final_same_side_windows"] += int(row["early_side"] == row["final_side"])
    stats["early_win_windows"] += int(row["early_side"] == row["winner"])
    stats["continuation_windows"] += int(row["continuation_same_side_usd"] > 0)
    stats["early_stake_usd"] += row["early_stake_usd"]
    stats["final_stake_usd"] += row["final_stake_usd"]
    stats["continuation_same_side_usd"] += row["continuation_same_side_usd"]
    ts = row.get("first_event_ts")
    if ts:
        stats["first_event_ts"] = ts if stats["first_event_ts"] is None else min(stats["first_event_ts"], ts)
        stats["latest_event_ts"] = ts if stats["latest_event_ts"] is None else max(stats["latest_event_ts"], ts)
    stats["conditions"].add(row["condition_id"])
    stats["markets"].add(row["market_slug"])
    if len(stats["examples"]) < 8:
        stats["examples"].append(row)


def _finalize_wallet(stats: dict[str, Any]) -> dict[str, Any]:
    windows = int(stats["eligible_windows"])
    predictiveness = (stats["early_final_same_side_windows"] / windows) if windows else 0.0
    win_rate = (stats["early_win_windows"] / windows) if windows else 0.0
    continuation_avg = (stats["continuation_same_side_usd"] / windows) if windows else 0.0
    continuation_rate = (stats["continuation_windows"] / windows) if windows else 0.0
    score = predictiveness * continuation_avg * win_rate
    return {
        "wallet": stats["wallet"],
        "eligible_windows": windows,
        "unique_conditions": len(stats["conditions"]),
        "unique_markets": len(stats["markets"]),
        "early_final_same_side_windows": int(stats["early_final_same_side_windows"]),
        "early_side_predictiveness_pct": round(predictiveness * 100.0, 6),
        "early_win_windows": int(stats["early_win_windows"]),
        "early_win_rate_pct": round(win_rate * 100.0, 6),
        "continuation_windows": int(stats["continuation_windows"]),
        "continuation_rate_pct": round(continuation_rate * 100.0, 6),
        "early_stake_usd": round(float(stats["early_stake_usd"]), 6),
        "final_stake_usd": round(float(stats["final_stake_usd"]), 6),
        "continuation_same_side_usd": round(float(stats["continuation_same_side_usd"]), 6),
        "avg_continuation_same_side_usd": round(continuation_avg, 6),
        "followability_score": round(score, 6),
        "first_event_ts": _iso(stats["first_event_ts"]),
        "latest_event_ts": _iso(stats["latest_event_ts"]),
        "engine_candidate": "early_commitment_drip_bombardment",
        "rule_sketch": (
            "If wallet's first-60s dominant side appears, ladder same side for the "
            "remaining window at or below its early weighted average price; paper gate required."
        ),
        "examples": stats["examples"],
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    history = _load_json(root / args.history, {})
    resolutions_path = root / args.resolutions if args.resolutions else _default_resolutions_path(root)
    winners = _load_resolutions(resolutions_path)
    events = history.get("events") if isinstance(history.get("events"), list) else []
    newest_source_event_ts = max(
        (_parse_ts(row.get("event_ts") or row.get("observed_ts")) for row in events if isinstance(row, dict)),
        default=0.0,
    )
    now_ts = datetime.now(tz=UTC).timestamp()
    newest_source_event_age_s = max(0.0, now_ts - newest_source_event_ts) if newest_source_event_ts else None
    source_is_frozen_d97 = Path(args.history).name == "wallet_copy_history_state.json"
    source_fresh = bool(
        not source_is_frozen_d97
        and newest_source_event_age_s is not None
        and newest_source_event_age_s <= 86400.0
    )
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    skipped = {"non_buy": 0, "unresolved": 0, "missing_wallet": 0, "bad_timing": 0, "bad_stake_or_side": 0}

    for row in events:
        if not isinstance(row, dict):
            continue
        if str(row.get("action") or "").upper() != "BUY":
            skipped["non_buy"] += 1
            continue
        wallet, condition, market = _event_key(row)
        if not wallet:
            skipped["missing_wallet"] += 1
            continue
        winner = winners.get(market) or winners.get(condition)
        if not winner:
            skipped["unresolved"] += 1
            continue
        side = _norm_outcome(row.get("outcome"))
        stake = _stake(row)
        event_ts = _parse_ts(row.get("event_ts") or row.get("observed_ts"))
        window_start = _window_start(row)
        if event_ts <= 0 or window_start <= 0:
            skipped["bad_timing"] += 1
            continue
        offset_s = event_ts - window_start
        if offset_s < 0 or offset_s >= 300:
            skipped["bad_timing"] += 1
            continue
        if not side or stake <= 0:
            skipped["bad_stake_or_side"] += 1
            continue
        grouped[(wallet, condition, market)].append(
            {
                "wallet": wallet,
                "condition_id": condition,
                "market_slug": market,
                "winner": winner,
                "side": side,
                "stake_usd": stake,
                "price": _float(row.get("price"), 0.0),
                "event_ts": event_ts,
                "offset_s": offset_s,
            }
        )

    wallet_rows: dict[str, dict[str, Any]] = {}
    window_rows = 0
    for (wallet, condition, market), rows in grouped.items():
        early = [row for row in rows if row["offset_s"] <= float(args.early_window_s)]
        if not early:
            continue
        early_by_side: dict[str, float] = defaultdict(float)
        final_by_side: dict[str, float] = defaultdict(float)
        early_price_weighted: dict[str, float] = defaultdict(float)
        first_event_ts = min(row["event_ts"] for row in rows)
        for row in rows:
            final_by_side[row["side"]] += row["stake_usd"]
        for row in early:
            early_by_side[row["side"]] += row["stake_usd"]
            early_price_weighted[row["side"]] += row["stake_usd"] * row["price"]
        early_side = _final_side(early_by_side)
        final_side = _final_side(final_by_side)
        if not early_side or early_by_side[early_side] < float(args.min_early_stake_usd):
            continue
        early_stake = early_by_side[early_side]
        final_stake = sum(final_by_side.values())
        same_side_final = final_by_side.get(early_side, 0.0)
        continuation = max(0.0, same_side_final - early_stake)
        window_row = {
            "wallet": wallet,
            "condition_id": condition,
            "market_slug": market,
            "winner": rows[0]["winner"],
            "early_side": early_side,
            "final_side": final_side,
            "early_stake_usd": round(early_stake, 6),
            "final_stake_usd": round(final_stake, 6),
            "continuation_same_side_usd": round(continuation, 6),
            "early_weighted_avg_price": round(early_price_weighted[early_side] / early_stake, 6),
            "first_event_ts": first_event_ts,
            "first_event_iso": _iso(first_event_ts),
        }
        stats = wallet_rows.setdefault(wallet, _wallet_stats(wallet))
        _add_window(stats, window_row)
        window_rows += 1

    leaderboard = [
        _finalize_wallet(stats)
        for stats in wallet_rows.values()
        if int(stats["eligible_windows"]) >= int(args.min_windows)
    ]
    leaderboard.sort(
        key=lambda row: (
            float(row["followability_score"]),
            int(row["eligible_windows"]),
            float(row["early_win_rate_pct"]),
        ),
        reverse=True,
    )
    selected = leaderboard[: int(args.top_n)] if source_fresh else []
    return {
        "kind": "wallet_copy_followability_leaderboard",
        "flow_stage": "LEARN",
        "generated_at": _utc_now_iso(),
        "status": "PASS_CURRENT_SOURCE" if source_fresh else "STALE_SOURCE_FAIL_CLOSED",
        "promotion_grade": source_fresh,
        "history_state": args.history,
        "source_freshness": {
            "newest_source_event_ts": newest_source_event_ts or None,
            "newest_source_event_age_s": round(newest_source_event_age_s, 6)
            if newest_source_event_age_s is not None
            else None,
            "freshness_limit_s": 86400.0,
            "source_is_frozen_d97": source_is_frozen_d97,
            "pass": source_fresh,
        },
        "resolutions": str(resolutions_path.relative_to(root)) if resolutions_path.is_relative_to(root) else str(resolutions_path),
        "criteria": {
            "top_n": int(args.top_n),
            "early_window_s": float(args.early_window_s),
            "min_windows": int(args.min_windows),
            "min_early_stake_usd": float(args.min_early_stake_usd),
            "score_formula": "early_side_predictiveness * avg_continuation_same_side_usd * early_win_rate",
        },
        "summary": {
            "events_scanned": len(events),
            "resolved_wallet_condition_windows": len(grouped),
            "early_commitment_windows": window_rows,
            "wallets_scored": len(leaderboard),
            "selected_wallets": len(selected),
            "skipped": skipped,
        },
        "selected_wallets": selected,
        "leaderboard": leaderboard,
        "promotion_path": (
            "selected wallets become paper-only follow-engine candidates; standard 50-fill gates still apply"
            if source_fresh
            else "no promotion use: source freshness gate failed closed"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--early-window-s", type=float, default=60.0)
    parser.add_argument("--min-windows", type=int, default=20)
    parser.add_argument("--min-early-stake-usd", type=float, default=0.1)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
