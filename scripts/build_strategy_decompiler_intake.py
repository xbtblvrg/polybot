#!/usr/bin/env python3
"""Build the first-stage wallet set for strategy decompiler modeling."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_strategy_decompiler_intake_latest.json"


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return _float(text, 0.0)


def _norm_outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"UP", "YES"}:
        return "UP"
    if text in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _winner_from_resolution(row: dict[str, Any]) -> str:
    direction = str(row.get("direction") or "").upper()
    if direction in {"UP", "DOWN"}:
        return direction
    winner = str(row.get("winner") or row.get("resolved_outcome") or "").upper()
    if winner in {"UP", "YES"}:
        return "UP"
    if winner in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _default_resolutions_path(root: Path) -> Path:
    candidates = [path for path in (root / "data" / "research").glob("btc_resolutions_*.jsonl") if path.is_file()]
    if not candidates:
        return root / "data" / "research" / "btc_resolutions_from_btcusdt_ticks.jsonl"
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _load_resolutions(path: Path) -> dict[str, str]:
    winners: dict[str, str] = {}
    if not path.exists():
        return winners
    with path.open() as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            winner = _winner_from_resolution(row)
            if not winner:
                continue
            for key in (row.get("market_slug"), row.get("condition_id")):
                text = str(key or "")
                if text:
                    winners[text] = winner
    return winners


def _new_wallet(wallet: str) -> dict[str, Any]:
    return {
        "wallet": wallet,
        "resolved_buy_events": 0,
        "wins": 0,
        "stake_usd": 0.0,
        "pnl_usd": 0.0,
        "first_event_ts": None,
        "latest_event_ts": None,
        "markets": set(),
        "conditions": set(),
        "prices": [],
    }


def _add_event(row: dict[str, Any], winner: str, stats: dict[str, Any]) -> None:
    price = _float(row.get("price"))
    if price <= 0.0 or price >= 1.0:
        return
    outcome = _norm_outcome(row.get("outcome"))
    if not outcome:
        return
    stake = _float(row.get("usdc_size"), 0.0) or max(price * _float(row.get("size")), 0.0)
    if stake <= 0.0:
        stake = 1.0
    win = outcome == winner
    unit_pnl = (1.0 / price - 1.0) if win else -1.0
    ts = _parse_ts(row.get("event_ts") or row.get("observed_ts"))
    stats["resolved_buy_events"] += 1
    stats["wins"] += int(win)
    stats["stake_usd"] += stake
    stats["pnl_usd"] += unit_pnl * stake
    if ts:
        stats["first_event_ts"] = ts if stats["first_event_ts"] is None else min(stats["first_event_ts"], ts)
        stats["latest_event_ts"] = ts if stats["latest_event_ts"] is None else max(stats["latest_event_ts"], ts)
    if row.get("market_slug"):
        stats["markets"].add(str(row.get("market_slug")))
    if row.get("condition_id"):
        stats["conditions"].add(str(row.get("condition_id")))
    stats["prices"].append(price)


def _finalize(stats: dict[str, Any]) -> dict[str, Any]:
    count = int(stats["resolved_buy_events"])
    wins = int(stats["wins"])
    stake = float(stats["stake_usd"])
    pnl = float(stats["pnl_usd"])
    first_ts = stats["first_event_ts"]
    latest_ts = stats["latest_event_ts"]
    span_days = ((latest_ts - first_ts) / 86400.0) if first_ts and latest_ts and latest_ts >= first_ts else 0.0
    prices = stats["prices"]
    return {
        "wallet": stats["wallet"],
        "resolved_buy_events": count,
        "wins": wins,
        "win_rate_pct": round((wins / count * 100.0), 6) if count else None,
        "stake_usd": round(stake, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / stake * 100.0), 6) if stake else None,
        "unique_markets": len(stats["markets"]),
        "unique_conditions": len(stats["conditions"]),
        "span_days": round(span_days, 6),
        "avg_price": round(sum(prices) / len(prices), 6) if prices else None,
        "first_event_ts": datetime.fromtimestamp(first_ts, tz=UTC).isoformat().replace("+00:00", "Z") if first_ts else None,
        "latest_event_ts": datetime.fromtimestamp(latest_ts, tz=UTC).isoformat().replace("+00:00", "Z") if latest_ts else None,
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    history = _load_json(root / args.history, {})
    resolutions_path = root / args.resolutions if args.resolutions else _default_resolutions_path(root)
    winners = _load_resolutions(resolutions_path)
    wallet_stats: dict[str, dict[str, Any]] = {}
    skipped = {"unresolved": 0, "non_buy": 0, "missing_wallet": 0}

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
    for row in events:
        if not isinstance(row, dict):
            continue
        if str(row.get("action") or "").upper() != "BUY":
            skipped["non_buy"] += 1
            continue
        winner = winners.get(str(row.get("market_slug") or "")) or winners.get(str(row.get("condition_id") or ""))
        if not winner:
            skipped["unresolved"] += 1
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if not wallet:
            skipped["missing_wallet"] += 1
            continue
        stats = wallet_stats.setdefault(wallet, _new_wallet(wallet))
        _add_event(row, winner, stats)

    rows = [_finalize(stats) for stats in wallet_stats.values()]
    eligible = [
        row
        for row in rows
        if int(row["resolved_buy_events"]) >= args.min_resolved_buys
        and int(row["unique_conditions"]) >= args.min_unique_conditions
        and float(row["pnl_usd"]) > 0.0
        and float(row["roi_pct"] or 0.0) > 0.0
    ]
    eligible.sort(
        key=lambda row: (
            float(row["pnl_usd"]),
            float(row["roi_pct"] or 0.0),
            int(row["resolved_buy_events"]),
            int(row["unique_conditions"]),
        ),
        reverse=True,
    )
    selected = eligible[: args.top_n] if source_fresh else []
    return {
        "kind": "wallet_copy_strategy_decompiler_intake",
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
            "top_n": args.top_n,
            "min_resolved_buys": args.min_resolved_buys,
            "min_unique_conditions": args.min_unique_conditions,
            "requires_positive_pnl": True,
            "requires_positive_roi": True,
            "wash_filter_hint": "unique_conditions gate plus resolved-only BTC-5m BUY events; deeper wash filtering belongs in model pass",
        },
        "summary": {
            "events_scanned": len(events),
            "wallets_seen": len(wallet_stats),
            "eligible_wallets": len(eligible),
            "selected_wallets": len(selected),
            "skipped": skipped,
        },
        "selected_wallets": selected,
        "modeling_plan": {
            "target": "next_action_from_observable_state",
            "first_models": ["shallow_decision_tree", "logistic_regression"],
            "validation": "walk_forward_out_of_sample_per_wallet",
            "feature_families": [
                "time_in_5m_window",
                "spot_delta_lookbacks",
                "book_bid_ask_spread_and_depth",
                "prior_wallet_flow",
                "entry_price_band",
                "side_and_outcome_pressure",
            ],
            "promotion_rule": "predictable_rule_engines_enter_paper_only race, then standard 50-fill evidence gates",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-resolved-buys", type=int, default=50)
    parser.add_argument("--min-unique-conditions", type=int, default=10)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": args.output, "summary": report["summary"]}, sort_keys=True))


if __name__ == "__main__":
    main()
