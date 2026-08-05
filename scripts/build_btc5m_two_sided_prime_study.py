#!/usr/bin/env python3
"""Build the BTC5M two-sided prime study.

Flow stage: LEARN/PROMOTE. This is paper-only deterministic evidence for the
operator's two-sided hypothesis: pair-sum arb frequency, intra-window scalp EV,
and two-sided wallet replay/split. It reads persisted history and resolutions,
writes an EV/day report, and never touches the live guard.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import defaultdict, deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_OUTPUT = "data/research/btc5m_two_sided_prime_study_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--train-fraction", type=float, default=0.70)
    parser.add_argument("--min-oos-trades", type=int, default=5)
    parser.add_argument("--top-wallets", type=int, default=25)
    return parser.parse_args()


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        raw = float(value)
        return raw / 1000.0 if raw > 10_000_000_000 else raw
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _window_start(row: dict[str, Any]) -> int:
    for key in ("window_start_s", "window_start", "market_start"):
        parsed = int(_parse_ts(row.get(key)) or 0)
        if parsed > 0:
            return parsed
    slug = str(row.get("market_slug") or row.get("event_slug") or row.get("slug") or "")
    match = re.search(r"btc-updown-5m-(\d{10})", slug)
    if match:
        return int(match.group(1))
    ts = _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp"))
    return int(ts // 300 * 300) if ts > 0 else 0


def _is_btc5m(row: dict[str, Any]) -> bool:
    slug = str(row.get("market_slug") or row.get("event_slug") or row.get("slug") or "").lower()
    if re.search(r"btc-updown-5m-\d{10}", slug):
        return True
    title = " ".join(str(row.get(key) or "") for key in ("title", "question", "series")).lower()
    return "btc" in title and ("5m" in title or "5 minute" in title or "5-minute" in title)


def _outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"UP", "YES"}:
        return "UP"
    if text in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _winner_from_row(row: dict[str, Any]) -> str:
    return _outcome(row.get("direction") or row.get("winner") or row.get("resolved_outcome"))


def _stake(row: dict[str, Any]) -> float:
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    return _float(row.get("usdc_size"), 0.0) or (price * size if price > 0 and size > 0 else 0.0)


def _resolutions_path(root: Path, explicit: str) -> Path:
    if explicit:
        path = Path(explicit)
        return path if path.is_absolute() else root / path
    candidates = [path for path in (root / "data" / "research").glob("btc_resolutions_*.jsonl") if path.is_file()]
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else root / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def _load_resolutions(path: Path) -> dict[str, dict[str, Any]]:
    winners: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return winners
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            winner = _winner_from_row(row)
            if not winner:
                continue
            window_start = int(_parse_ts(row.get("window_start_unix_ts") or row.get("window_start_s")) or 0)
            market_slug = str(row.get("market_slug") or "")
            if window_start <= 0 and market_slug:
                match = re.search(r"btc-updown-5m-(\d{10})", market_slug)
                if match:
                    window_start = int(match.group(1))
            payload = {"winner": winner, "window_start_s": window_start, "market_slug": market_slug}
            for key in (market_slug, row.get("condition_id")):
                text = str(key or "")
                if text:
                    winners[text] = payload
    return winners


def _split_start_s(starts: set[int], fraction: float) -> int:
    ordered = sorted(start for start in starts if start > 0)
    if not ordered:
        return 0
    index = min(len(ordered) - 1, max(0, int(len(ordered) * fraction)))
    return ordered[index]


def _partition(window_start_s: int, split_start_s: int) -> str:
    return "train" if split_start_s and window_start_s < split_start_s else "test"


def _new_metric() -> dict[str, Any]:
    return {"trades": 0, "wins": 0, "cost_usd": 0.0, "pnl_usd": 0.0, "start_s": None, "end_s": None}


def _add_metric(metric: dict[str, Any], *, pnl: float, cost_usd: float, window_start_s: int) -> None:
    metric["trades"] = int(metric.get("trades") or 0) + 1
    metric["wins"] = int(metric.get("wins") or 0) + int(pnl > 0.0)
    metric["cost_usd"] = round(float(metric.get("cost_usd") or 0.0) + cost_usd, 6)
    metric["pnl_usd"] = round(float(metric.get("pnl_usd") or 0.0) + pnl, 6)
    if window_start_s > 0:
        metric["start_s"] = window_start_s if metric.get("start_s") is None else min(int(metric["start_s"]), window_start_s)
        metric["end_s"] = window_start_s if metric.get("end_s") is None else max(int(metric["end_s"]), window_start_s)


def _finalize_metric(metric: dict[str, Any]) -> dict[str, Any]:
    trades = int(metric.get("trades") or 0)
    wins = int(metric.get("wins") or 0)
    cost = float(metric.get("cost_usd") or 0.0)
    pnl = float(metric.get("pnl_usd") or 0.0)
    start = metric.get("start_s")
    end = metric.get("end_s")
    span_days = 0.0
    if start is not None and end is not None:
        span_days = max((float(end) - float(start) + 300.0) / 86400.0, 300.0 / 86400.0)
    ev_days = max(span_days, 1.0)
    return {
        "trades": trades,
        "wins": wins,
        "wr_pct": round(100.0 * wins / trades, 6) if trades else 0.0,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(100.0 * pnl / cost, 6) if cost > 0 else 0.0,
        "span_days": round(span_days, 6),
        "ev_per_day_usd": round(pnl / ev_days, 6),
    }


def _new_study() -> dict[str, Any]:
    return {"total": _new_metric(), "train": _new_metric(), "test": _new_metric(), "samples": []}


def _add_study(study: dict[str, Any], *, pnl: float, cost_usd: float, window_start_s: int, sample: dict[str, Any]) -> None:
    partition = _partition(window_start_s, int(study.get("split_start_s") or 0))
    _add_metric(study["total"], pnl=pnl, cost_usd=cost_usd, window_start_s=window_start_s)
    _add_metric(study[partition], pnl=pnl, cost_usd=cost_usd, window_start_s=window_start_s)
    if len(study["samples"]) < 20:
        study["samples"].append({**sample, "partition": partition})


def _finalize_study(
    study: dict[str, Any],
    *,
    mechanism_id: str,
    family: str,
    paper_lane_id: str,
    min_oos_trades: int,
    evidence_pointer: str,
    proposed_funding_size_usd: float,
) -> dict[str, Any]:
    total = _finalize_metric(study["total"])
    train = _finalize_metric(study["train"])
    test = _finalize_metric(study["test"])
    has_train = int(train["trades"]) > 0
    holdout_passed = has_train and int(test["trades"]) >= min_oos_trades and float(test["pnl_usd"]) > 0.0
    if int(total["trades"]) <= 0:
        status = "NO_SAMPLES"
    elif holdout_passed:
        status = "HOLDOUT_PASS"
    elif not has_train and int(test["trades"]) >= min_oos_trades and float(test["pnl_usd"]) > 0.0:
        status = "NEEDS_TRAIN_HOLDOUT"
    elif int(test["trades"]) < min_oos_trades:
        status = "NEEDS_MORE_OOS"
    else:
        status = "NEGATIVE_OOS"
    return {
        "mechanism_id": mechanism_id,
        "family": family,
        "paper_lane_id": paper_lane_id,
        "status": status,
        "holdout_passed": holdout_passed,
        "ev_per_day_usd": test["ev_per_day_usd"],
        "oos_pnl_usd": test["pnl_usd"],
        "oos_trades": test["trades"],
        "total": total,
        "train": train,
        "test": test,
        "samples": study["samples"],
        "evidence_pointer": evidence_pointer,
        "proposed_funding_size_usd": proposed_funding_size_usd if holdout_passed else 0.0,
    }


def _event(row: dict[str, Any], winners: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if not _is_btc5m(row):
        return None
    action = str(row.get("action") or row.get("side") or row.get("raw", {}).get("side") or "").strip().upper()
    if action not in {"BUY", "SELL"}:
        return None
    wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet") or row.get("proxyWallet") or row.get("raw", {}).get("proxyWallet"))
    if not wallet:
        return None
    outcome = _outcome(row.get("outcome") or row.get("raw", {}).get("outcome"))
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    if not outcome or price <= 0.0 or price >= 1.0 or size <= 0.0:
        return None
    market_slug = str(row.get("market_slug") or row.get("event_slug") or row.get("slug") or row.get("raw", {}).get("slug") or "")
    condition_id = str(row.get("condition_id") or row.get("market_id") or row.get("raw", {}).get("conditionId") or "")
    resolved = winners.get(market_slug) or winners.get(condition_id)
    if not resolved:
        return None
    window_start = int(resolved.get("window_start_s") or _window_start(row))
    if window_start <= 0:
        return None
    return {
        "wallet": wallet,
        "action": action,
        "outcome": outcome,
        "price": price,
        "size": size,
        "stake_usd": _stake(row),
        "market_slug": market_slug,
        "condition_id": condition_id,
        "winner": str(resolved["winner"]),
        "window_start_s": window_start,
        "event_ts": _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp")),
    }


def _wallet_stats_template(wallet: str) -> dict[str, Any]:
    return {
        "wallet": wallet,
        "two_sided_windows": 0,
        "buy_events": 0,
        "up_source_pnl_usd": 0.0,
        "down_source_pnl_usd": 0.0,
        "source_pnl_usd": 0.0,
        "copy_replay": {"total": _new_metric(), "train": _new_metric(), "test": _new_metric()},
        "markets": set(),
    }


def _winner_pnl(outcome: str, winner: str, price: float, size: float) -> float:
    return (size if outcome == winner else 0.0) - price * size


def _copy_pnl(outcome: str, winner: str, entry_price: float, order_usd: float) -> float:
    if entry_price <= 0.0:
        return 0.0
    shares = order_usd / entry_price
    return (shares if outcome == winner else 0.0) - order_usd


def _run_fifo_scalp(events: list[dict[str, Any]], *, tick_size: float, order_usd: float, study: dict[str, Any]) -> None:
    lots: dict[tuple[str, str], deque[dict[str, float]]] = defaultdict(deque)
    for event in sorted(events, key=lambda item: (float(item["event_ts"] or 0.0), item["action"] != "BUY")):
        key = (event["wallet"], event["outcome"])
        if event["action"] == "BUY":
            entry = min(0.99, float(event["price"]) + tick_size)
            if entry <= 0.0:
                continue
            shares = min(float(event["size"]), order_usd / entry)
            lots[key].append({"shares": shares, "entry": entry})
            continue
        exit_price = max(0.01, float(event["price"]) - tick_size)
        remaining = float(event["size"])
        while remaining > 0 and lots[key]:
            lot = lots[key][0]
            matched = min(float(lot["shares"]), remaining)
            pnl = (exit_price - float(lot["entry"])) * matched
            cost = float(lot["entry"]) * matched
            _add_study(
                study,
                pnl=pnl,
                cost_usd=cost,
                window_start_s=int(event["window_start_s"]),
                sample={
                    "market_slug": event["market_slug"],
                    "wallet": event["wallet"],
                    "outcome": event["outcome"],
                    "entry_price": round(float(lot["entry"]), 6),
                    "exit_price": round(exit_price, 6),
                    "matched_shares": round(matched, 6),
                    "pnl_usd": round(pnl, 6),
                },
            )
            lot["shares"] = float(lot["shares"]) - matched
            remaining -= matched
            if float(lot["shares"]) <= 1e-9:
                lots[key].popleft()


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    history = load_json(root / args.history, default={})
    resolutions = _resolutions_path(root, args.resolutions)
    winners = _load_resolutions(resolutions)
    events = []
    diagnostics = defaultdict(int)
    raw_events = history.get("events") if isinstance(history.get("events"), list) else []
    newest_source_event_ts = max(
        (_parse_ts(row.get("event_ts") or row.get("observed_ts")) for row in raw_events if isinstance(row, dict)),
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
    for row in raw_events:
        if not isinstance(row, dict):
            continue
        parsed = _event(row, winners)
        if parsed is None:
            diagnostics["skipped_unusable"] += 1
            continue
        events.append(parsed)
        diagnostics["accepted_events"] += 1
    starts = {int(event["window_start_s"]) for event in events}
    split_start = _split_start_s(starts, float(args.train_fraction))

    pair_study = _new_study()
    scalp_study = _new_study()
    pair_study["split_start_s"] = split_start
    scalp_study["split_start_s"] = split_start

    by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_wallet_market: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_market[event["market_slug"]].append(event)
        by_wallet_market[(event["wallet"], event["market_slug"])].append(event)

    paired_markets = 0
    pair_candidates = 0
    for slug, market_events in by_market.items():
        _run_fifo_scalp(market_events, tick_size=float(args.tick_size), order_usd=float(args.order_usd), study=scalp_study)
        buy_size = defaultdict(float)
        buy_price_size = defaultdict(float)
        window_start = int(market_events[0]["window_start_s"])
        for event in market_events:
            if event["action"] != "BUY":
                continue
            buy_size[event["outcome"]] += float(event["size"])
            buy_price_size[event["outcome"]] += float(event["price"]) * float(event["size"])
        if buy_size["UP"] <= 0 or buy_size["DOWN"] <= 0:
            continue
        paired_markets += 1
        avg_up = buy_price_size["UP"] / buy_size["UP"]
        avg_down = buy_price_size["DOWN"] / buy_size["DOWN"]
        pair_sum = avg_up + avg_down
        if pair_sum >= 1.0 - float(args.tick_size):
            continue
        pair_candidates += 1
        pnl = float(args.order_usd) * (1.0 - pair_sum) / pair_sum
        _add_study(
            pair_study,
            pnl=pnl,
            cost_usd=float(args.order_usd),
            window_start_s=window_start,
            sample={
                "market_slug": slug,
                "pair_sum": round(pair_sum, 6),
                "avg_up": round(avg_up, 6),
                "avg_down": round(avg_down, 6),
                "pnl_usd": round(pnl, 6),
            },
        )

    wallet_rows: dict[str, dict[str, Any]] = {}
    for (wallet, slug), wallet_events in by_wallet_market.items():
        buys = [event for event in wallet_events if event["action"] == "BUY"]
        outcomes = {event["outcome"] for event in buys}
        if not {"UP", "DOWN"}.issubset(outcomes):
            continue
        stats = wallet_rows.setdefault(wallet, _wallet_stats_template(wallet))
        stats["two_sided_windows"] += 1
        stats["markets"].add(slug)
        for event in buys:
            source_pnl = _winner_pnl(event["outcome"], event["winner"], float(event["price"]), float(event["size"]))
            stats["buy_events"] += 1
            stats["source_pnl_usd"] += source_pnl
            stats[f"{event['outcome'].lower()}_source_pnl_usd"] += source_pnl
            entry = min(0.99, float(event["price"]) + float(args.tick_size))
            copy_pnl = _copy_pnl(event["outcome"], event["winner"], entry, float(args.order_usd))
            partition = _partition(int(event["window_start_s"]), split_start)
            _add_metric(stats["copy_replay"]["total"], pnl=copy_pnl, cost_usd=float(args.order_usd), window_start_s=int(event["window_start_s"]))
            _add_metric(stats["copy_replay"][partition], pnl=copy_pnl, cost_usd=float(args.order_usd), window_start_s=int(event["window_start_s"]))

    finalized_wallets = []
    raw_wallet_replays: dict[str, dict[str, dict[str, Any]]] = {}
    for stats in wallet_rows.values():
        raw_wallet_replays[str(stats["wallet"])] = stats["copy_replay"]
        total = _finalize_metric(stats["copy_replay"]["total"])
        train = _finalize_metric(stats["copy_replay"]["train"])
        test = _finalize_metric(stats["copy_replay"]["test"])
        finalized_wallets.append(
            {
                "wallet": stats["wallet"],
                "two_sided_windows": int(stats["two_sided_windows"]),
                "buy_events": int(stats["buy_events"]),
                "unique_markets": len(stats["markets"]),
                "source_pnl_usd": round(float(stats["source_pnl_usd"]), 6),
                "up_source_pnl_usd": round(float(stats["up_source_pnl_usd"]), 6),
                "down_source_pnl_usd": round(float(stats["down_source_pnl_usd"]), 6),
                "source_net_positive": float(stats["source_pnl_usd"]) > 0.0,
                "copy_replay": {"total": total, "train": train, "test": test},
            }
        )
    finalized_wallets.sort(
        key=lambda row: (
            float(row["copy_replay"]["test"]["ev_per_day_usd"]),
            float(row["copy_replay"]["test"]["pnl_usd"]),
            int(row["two_sided_windows"]),
        ),
        reverse=True,
    )

    copy_study = _new_study()
    copy_study["split_start_s"] = split_start
    best_wallet = finalized_wallets[0] if finalized_wallets else {}
    if best_wallet:
        raw_replay = raw_wallet_replays.get(str(best_wallet["wallet"]), {})
        for label in ("total", "train", "test"):
            metric = raw_replay.get(label) if isinstance(raw_replay.get(label), dict) else {}
            copy_study[label].update(metric)
    output_rel = args.output
    mechanism_rows = [
        _finalize_study(
            pair_study,
            mechanism_id="structural-pair-sum-arb",
            family="structural",
            paper_lane_id="paper_struct_pair_sum_arb",
            min_oos_trades=int(args.min_oos_trades),
            evidence_pointer=f"{output_rel}#pair_sum_arb",
            proposed_funding_size_usd=1.0,
        ),
        _finalize_study(
            scalp_study,
            mechanism_id="structural-intra-window-scalp",
            family="structural",
            paper_lane_id="paper_struct_intra_window_scalp",
            min_oos_trades=int(args.min_oos_trades),
            evidence_pointer=f"{output_rel}#intra_window_scalp",
            proposed_funding_size_usd=1.0,
        ),
        _finalize_study(
            copy_study,
            mechanism_id="copy-two-sided-inventory",
            family="copy",
            paper_lane_id="paper_copy_two_sided_inventory",
            min_oos_trades=int(args.min_oos_trades),
            evidence_pointer=f"{output_rel}#wallet_two_sided_replay",
            proposed_funding_size_usd=2.0,
        ),
    ]
    mechanism_rows.sort(key=lambda row: (bool(row["holdout_passed"]), float(row["ev_per_day_usd"]), int(row["oos_trades"])), reverse=True)

    return {
        "schema_version": 1,
        "kind": "btc5m_two_sided_prime_study",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now_iso(),
        "status": "PASS_CURRENT_SOURCE" if source_fresh else "STALE_SOURCE_FAIL_CLOSED",
        "promotion_grade": source_fresh,
        "inputs": {
            "history": args.history,
            "resolutions": str(resolutions.relative_to(root)) if resolutions.is_relative_to(root) else str(resolutions),
            "order_usd": float(args.order_usd),
            "tick_size": float(args.tick_size),
            "train_fraction": float(args.train_fraction),
            "split_start_s": split_start,
            "newest_source_event_ts": newest_source_event_ts or None,
            "newest_source_event_age_s": round(newest_source_event_age_s, 6)
            if newest_source_event_age_s is not None
            else None,
            "freshness_limit_s": 86400.0,
            "source_is_frozen_d97": source_is_frozen_d97,
            "source_freshness_pass": source_fresh,
        },
        "summary": {
            "accepted_events": len(events),
            "resolved_windows": len(starts),
            "paired_markets": paired_markets,
            "pair_sum_candidates": pair_candidates,
            "pair_sum_frequency_pct": round(100.0 * pair_candidates / paired_markets, 6) if paired_markets else 0.0,
            "two_sided_wallets": len(finalized_wallets),
            "top_mechanism": mechanism_rows[0]["mechanism_id"] if mechanism_rows else "",
            "top_ev_per_day_usd": mechanism_rows[0]["ev_per_day_usd"] if mechanism_rows else 0.0,
            "diagnostics": dict(sorted(diagnostics.items())),
        },
        "mechanism_rows": mechanism_rows,
        "pair_sum_arb": mechanism_rows[[row["mechanism_id"] for row in mechanism_rows].index("structural-pair-sum-arb")],
        "intra_window_scalp": mechanism_rows[[row["mechanism_id"] for row in mechanism_rows].index("structural-intra-window-scalp")],
        "wallet_two_sided_replay": mechanism_rows[[row["mechanism_id"] for row in mechanism_rows].index("copy-two-sided-inventory")],
        "wallet_two_sided_rows": finalized_wallets[: max(1, int(args.top_wallets))],
        "best_two_sided_wallet": finalized_wallets[0] if finalized_wallets else {},
        "next_action": (
            "feed mechanism_rows into the unified BTC5M morning ranked table; fund only through Fable and the single live guard"
            if source_fresh
            else "historical prior only; rebuild from current age-stamped source before promotion use"
        ),
    }


def main() -> int:
    args = parse_args()
    report = build_report(ROOT, args)
    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    atomic_write_json(output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
