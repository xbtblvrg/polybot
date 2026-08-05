#!/usr/bin/env python3
"""Backtest E7 spot-vs-open calibration and an E6 opposite-flow exit variant.

Flow stage: LEARN/PROMOTE. This is offline research only: it reads existing
resolution/RTDS data, may fetch Binance 1m klines for the same resolved BTC-5m
windows, and writes a JSON report. It never creates CopyIntents and never
touches live or paper execution state.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.refresh_btc_5m_resolutions_from_history import fetch_1m_klines  # noqa: E402
from scripts.run_e6_whale_net_flow_paper_lane import build_e6_signals, load_recent_buy_events  # noqa: E402
from src.wallet_copy.models import num  # noqa: E402
from src.wallet_copy.performance import load_resolutions as load_resolution_index, score_order  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import load_json  # noqa: E402


DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_RTDS = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_E5_PAPER_STATE = "data/research/maker_first_btc5m_paper_state.json"
DEFAULT_E6_PAPER_STATE = "data/research/e6_whale_net_flow_paper_state.json"
DEFAULT_OUTPUT = "data/research/e7_spot_open_e6_exit_backtest_20260706.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--e5-paper-state", default=DEFAULT_E5_PAPER_STATE)
    parser.add_argument("--e6-paper-state", default=DEFAULT_E6_PAPER_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--max-windows", type=int, default=900)
    parser.add_argument("--e7-threshold-bps", default="5,10,15,20,30,40,50")
    parser.add_argument("--e7-entry-prices", default="0.70,0.80,0.90")
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--e6-scan-limit", type=int, default=250_000)
    parser.add_argument("--e6-scan-max-bytes", type=int, default=256_000_000)
    parser.add_argument("--e6-max-feed-events", type=int, default=20_000)
    parser.add_argument("--e6-signal-window-s", type=float, default=120.0)
    parser.add_argument("--e6-min-flow-usd", type=float, default=20.0)
    parser.add_argument("--e6-min-dominance", type=float, default=0.65)
    parser.add_argument("--e6-exit-min-flow-usd", type=float, default=20.0)
    parser.add_argument("--e6-exit-min-dominance", type=float, default=0.60)
    parser.add_argument("--tick-size", type=float, default=0.01)
    return parser.parse_args()


def _csv_floats(text: str) -> list[float]:
    return [float(item.strip()) for item in str(text).split(",") if item.strip()]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _load_resolutions(path: str | Path, *, max_windows: int) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with Path(path).open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("asset") or "").upper() != "BTC":
                continue
            if str(row.get("window_type") or "").lower() != "5m":
                continue
            slug = str(row.get("market_slug") or "")
            if not slug.startswith("btc-updown-5m-"):
                continue
            direction = str(row.get("direction") or "").upper()
            if direction not in {"UP", "DOWN"}:
                continue
            rows[slug] = row
    ordered = sorted(
        rows.values(),
        key=lambda row: int(num(row.get("window_start_unix_ts") or str(row.get("market_slug")).rsplit("-", 1)[-1], 0)),
    )
    if max_windows > 0:
        ordered = ordered[-max_windows:]
    return {str(row["market_slug"]): row for row in ordered}


def _window_start(row: dict[str, Any]) -> int:
    return int(num(row.get("window_start_unix_ts") or str(row.get("market_slug")).rsplit("-", 1)[-1], 0))


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    trades = len(rows)
    wins = sum(1 for row in rows if num(row.get("pnl_usd")) > 0)
    cost = sum(num(row.get("cost_usd")) for row in rows)
    pnl = sum(num(row.get("pnl_usd")) for row in rows)
    return {
        "trades": trades,
        "wins": wins,
        "losses": trades - wins,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "wr_pct": round(100.0 * wins / trades, 6) if trades else 0.0,
        "roi_pct": round(100.0 * pnl / cost, 6) if cost else 0.0,
    }


def _paper_orders(path: str | Path) -> list[dict[str, Any]]:
    state = load_json(path, default={})
    if not isinstance(state, dict):
        return []
    return [row for row in state.get("orders") or [] if isinstance(row, dict)]


def _source_metadata(order: dict[str, Any], key: str) -> dict[str, Any]:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    value = metadata.get(key)
    return value if isinstance(value, dict) else {}


def _status_is_filled(order: dict[str, Any]) -> bool:
    return str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"


def _direction_to_outcome(value: Any) -> str:
    direction = str(value or "").upper()
    return "Up" if direction == "UP" else "Down" if direction == "DOWN" else ""


def _run_e7_book_verified(
    args: argparse.Namespace,
    *,
    resolutions: dict[str, dict[str, Any]],
    klines: dict[int, list[Any]],
    thresholds_bps: list[float],
    entry_prices: list[float],
) -> dict[str, Any]:
    grid: dict[tuple[float, float], list[dict[str, Any]]] = {(t, p): [] for t in thresholds_bps for p in entry_prices}
    diagnostics: Counter[str] = Counter()
    orders = _paper_orders(args.e5_paper_state)
    for order in orders:
        signal = _source_metadata(order, "e5_maker_first_btc5m_v1")
        if not signal:
            diagnostics["missing_e5_signal"] += 1
            continue
        slug = str(signal.get("market_slug") or order.get("market_slug") or "")
        row = resolutions.get(slug)
        if row is None:
            diagnostics["missing_resolution"] += 1
            continue
        start = _window_start(row)
        quote_ts = num(signal.get("quote_ts") or signal.get("source_observed_ts") or order.get("submitted_at"), 0.0)
        offset_s = quote_ts - float(start)
        if offset_s < 210.0 or offset_s > 240.0:
            diagnostics["outside_t_minus_90_60"] += 1
            continue
        open_row = klines.get(start)
        signal_row = klines.get(start + 240)
        if open_row is None or signal_row is None:
            diagnostics["missing_kline"] += 1
            continue
        open_price = float(open_row[1])
        signal_price = float(signal_row[1])
        if open_price <= 0 or signal_price <= 0:
            diagnostics["bad_price"] += 1
            continue
        delta_bps = (signal_price - open_price) / open_price * 10_000.0
        predicted = "Up" if delta_bps > 0 else "Down" if delta_bps < 0 else ""
        if not predicted:
            diagnostics["tie_signal"] += 1
            continue
        outcome = str(signal.get("outcome") or order.get("outcome") or "")
        if predicted != outcome:
            diagnostics["book_for_opposite_side_unavailable"] += 1
            continue
        top = signal.get("top_of_book") if isinstance(signal.get("top_of_book"), dict) else {}
        if str(top.get("status") or "") != "OK" or not str(top.get("book_hash") or ""):
            diagnostics["missing_book_evidence"] += 1
            continue
        if str(top.get("instant_fill_status") or "") != "PASS" or num(top.get("fillable_usd"), 0.0) < float(args.order_usd):
            diagnostics["insufficient_book_depth"] += 1
            continue
        entry_price = num(top.get("avg_fill_price"), 0.0) or num(top.get("best_ask"), 0.0)
        if entry_price <= 0:
            diagnostics["missing_entry_price"] += 1
            continue
        winner = _direction_to_outcome(row.get("direction"))
        if not winner:
            diagnostics["missing_winner"] += 1
            continue
        for threshold in thresholds_bps:
            if abs(delta_bps) < threshold:
                continue
            for entry_cap in entry_prices:
                if entry_price > entry_cap:
                    continue
                shares = float(args.order_usd) / entry_price
                pnl = (shares if predicted == winner else 0.0) - float(args.order_usd)
                grid[(threshold, entry_cap)].append(
                    {
                        "market_slug": slug,
                        "window_start_s": start,
                        "predicted": predicted,
                        "winner": winner,
                        "delta_bps_at_t_minus_60": round(delta_bps, 6),
                        "entry_cap": entry_cap,
                        "entry_price": round(entry_price, 6),
                        "book_hash": top.get("book_hash"),
                        "fillable_usd": round(num(top.get("fillable_usd"), 0.0), 6),
                        "cost_usd": round(float(args.order_usd), 6),
                        "pnl_usd": round(pnl, 6),
                    }
                )
    rows = []
    for (threshold, entry_cap), trades in grid.items():
        rows.append(
            {
                "threshold_bps": threshold,
                "entry_cap": entry_cap,
                **_summarize(trades),
                "samples": trades[:20],
            }
        )
    rows.sort(key=lambda item: (item["roi_pct"], item["pnl_usd"], item["trades"]), reverse=True)
    return {
        "status": "BOOK_VERIFIED_COMPLETE" if rows and rows[0]["trades"] else "NO_BOOK_VERIFIED_E7_ENTRIES",
        "rule": "persisted E5 top_of_book only; T-90..T-60; predicted side must match book side; instant_fill_status PASS must cover order_usd",
        "input_e5_orders": len(orders),
        "diagnostics": dict(sorted(diagnostics.items())),
        "best": rows[0] if rows else {},
        "grid": rows,
    }


def run_e7(args: argparse.Namespace, resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not resolutions:
        return {"status": "NO_RESOLUTIONS"}
    starts = [_window_start(row) for row in resolutions.values()]
    klines = fetch_1m_klines(args.symbol, min(starts), max(starts) + 300, float(args.timeout_s))
    thresholds_bps = _csv_floats(args.e7_threshold_bps)
    entry_prices = _csv_floats(args.e7_entry_prices)
    grid: dict[tuple[float, float], list[dict[str, Any]]] = {(t, p): [] for t in thresholds_bps for p in entry_prices}
    diagnostics: Counter[str] = Counter()
    for slug, row in sorted(resolutions.items(), key=lambda item: _window_start(item[1])):
        start = _window_start(row)
        open_row = klines.get(start)
        signal_row = klines.get(start + 240)
        if open_row is None or signal_row is None:
            diagnostics["missing_kline"] += 1
            continue
        open_price = float(open_row[1])
        signal_price = float(signal_row[1])
        if open_price <= 0 or signal_price <= 0:
            diagnostics["bad_price"] += 1
            continue
        delta_bps = (signal_price - open_price) / open_price * 10_000.0
        predicted = "UP" if delta_bps > 0 else "DOWN" if delta_bps < 0 else "TIE"
        winner = str(row.get("direction") or "").upper()
        if predicted == "TIE":
            diagnostics["tie_signal"] += 1
            continue
        for threshold in thresholds_bps:
            if abs(delta_bps) < threshold:
                continue
            for entry_price in entry_prices:
                shares = float(args.order_usd) / entry_price
                pnl = (shares if predicted == winner else 0.0) - float(args.order_usd)
                grid[(threshold, entry_price)].append(
                    {
                        "market_slug": slug,
                        "window_start_s": start,
                        "predicted": predicted,
                        "winner": winner,
                        "delta_bps_at_t_minus_60": round(delta_bps, 6),
                        "entry_price_assumption": entry_price,
                        "cost_usd": round(float(args.order_usd), 6),
                        "pnl_usd": round(pnl, 6),
                    }
                )
    rows = []
    for (threshold, entry_price), trades in grid.items():
        summary = _summarize(trades)
        rows.append(
            {
                "threshold_bps": threshold,
                "entry_price_assumption": entry_price,
                **summary,
                "samples": trades[:20],
            }
        )
    rows.sort(key=lambda item: (item["roi_pct"], item["pnl_usd"], item["trades"]), reverse=True)
    book_verified = _run_e7_book_verified(
        args,
        resolutions=resolutions,
        klines=klines,
        thresholds_bps=thresholds_bps,
        entry_prices=entry_prices,
    )
    return {
        "status": "CALIBRATION_ONLY_BOOK_DEPTH_NOT_VERIFIED",
        "book_depth_gate": "E7 is not paper/live-ready until CLOB book depth at T-90..T-60 is replayed or captured; this grid uses conservative fixed entry price assumptions.",
        "input_windows": len(resolutions),
        "kline_rows": len(klines),
        "diagnostics": dict(sorted(diagnostics.items())),
        "best": rows[0] if rows else {},
        "grid": rows,
        "book_verified": book_verified,
    }


def _winner(resolutions: dict[str, dict[str, Any]], market_slug: str) -> str:
    direction = str((resolutions.get(market_slug) or {}).get("direction") or "").upper()
    return "Up" if direction == "UP" else "Down" if direction == "DOWN" else ""


def _exit_for_signal(
    *,
    signal: dict[str, Any],
    events: list[Any],
    min_flow_usd: float,
    min_dominance: float,
    tick_size: float,
) -> dict[str, Any]:
    outcome = str(signal.get("outcome") or "")
    opposite = "Down" if outcome == "Up" else "Up"
    cumulative = {"Up": 0.0, "Down": 0.0}
    price_numer = {"Up": 0.0, "Down": 0.0}
    size_denom = {"Up": 0.0, "Down": 0.0}
    for event in sorted(events, key=lambda item: (item.event_ts, item.observed_ts, item.event_id)):
        if float(event.event_ts) <= num(signal.get("event_ts")):
            continue
        if event.side != "BUY":
            continue
        usd = float(event.source_usd)
        cumulative[event.outcome] += usd
        price_numer[event.outcome] += float(event.price) * float(event.size)
        size_denom[event.outcome] += float(event.size)
        total = cumulative["Up"] + cumulative["Down"]
        if total <= 0:
            continue
        dominance = cumulative[opposite] / total
        if cumulative[opposite] >= float(min_flow_usd) and dominance >= float(min_dominance):
            avg_price = price_numer[opposite] / max(size_denom[opposite], 1e-9)
            return {
                "exit_price": min(0.99, avg_price + float(tick_size)),
                "exit_ts": float(event.event_ts),
                "opposite_flow_usd": cumulative[opposite],
                "opposite_dominance": dominance,
            }
    return {}


def compare_e6_exit_orders(
    *,
    orders: list[dict[str, Any]],
    events: list[Any],
    resolution_index: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    by_market: dict[str, list[Any]] = defaultdict(list)
    for event in events:
        by_market[event.market_slug].append(event)
    hold_rows: list[dict[str, Any]] = []
    exit_rows: list[dict[str, Any]] = []
    exit_hits = 0
    skipped: Counter[str] = Counter()
    for order in orders:
        if not _status_is_filled(order):
            skipped["not_filled"] += 1
            continue
        signal = _source_metadata(order, "e6_whale_net_flow_v1")
        if not signal:
            skipped["missing_e6_signal"] += 1
            continue
        scored = score_order(order, resolution_index)
        if not scored.get("resolved"):
            skipped["unresolved"] += 1
            continue
        slug = str(scored.get("market_slug") or order.get("market_slug") or "")
        outcome = str(scored.get("outcome") or order.get("outcome") or "")
        winner = str(scored.get("winner") or "")
        order_usd = num(scored.get("cost_usd"), num(order.get("filled_size_usd"), 0.0))
        shares = num(scored.get("shares"), num(order.get("filled_shares"), 0.0))
        entry_price = order_usd / shares if shares > 0 else max(0.01, num(order.get("limit_price"), 0.0))
        hold_pnl = num(scored.get("pnl_usd"), 0.0)
        hold_rows.append(
            {
                "market_slug": slug,
                "outcome": outcome,
                "winner": winner,
                "entry_price": round(entry_price, 6),
                "cost_usd": order_usd,
                "pnl_usd": hold_pnl,
            }
        )

        exit_info = _exit_for_signal(
            signal=signal,
            events=by_market.get(slug, []),
            min_flow_usd=float(args.e6_exit_min_flow_usd),
            min_dominance=float(args.e6_exit_min_dominance),
            tick_size=float(args.tick_size),
        )
        exit_price = num(exit_info.get("exit_price"), 0.0)
        exit_source = "rtds_opposite_flow" if exit_price > 0 else None
        if exit_price > 0:
            exit_hits += 1
            pnl = shares * (1.0 - exit_price) - order_usd
        else:
            pnl = hold_pnl
        exit_rows.append(
            {
                "market_slug": slug,
                "outcome": outcome,
                "winner": winner,
                "entry_price": round(entry_price, 6),
                "exit_price": round(exit_price, 6) if exit_price else None,
                "exit_ts": exit_info.get("exit_ts") if exit_info else None,
                "exit_source": exit_source,
                "opposite_flow_usd": round(num(exit_info.get("opposite_flow_usd"), 0.0), 6) if exit_info else None,
                "opposite_dominance": round(num(exit_info.get("opposite_dominance"), 0.0), 6) if exit_info else None,
                "cost_usd": order_usd + (shares * exit_price if exit_price else 0.0),
                "pnl_usd": pnl,
            }
        )
    hold_summary = _summarize(hold_rows)
    exit_summary = _summarize(exit_rows)
    return {
        "status": "COMPARISON_COMPLETE" if hold_rows else "INCONCLUSIVE_NO_E6_PAPER_ORDERS",
        "exit_rule": {
            "opposite_flow_usd_gte": float(args.e6_exit_min_flow_usd),
            "opposite_dominance_gte": float(args.e6_exit_min_dominance),
            "exit_price": "opposite weighted avg price + one tick; buy opposite shares equal to entry shares",
        },
        "input_paper_orders": len(orders),
        "skipped_orders": dict(sorted(skipped.items())),
        "exit_hits": exit_hits,
        "hold_summary": hold_summary,
        "exit_summary": exit_summary,
        "delta_exit_minus_hold_pnl_usd": round(exit_summary["pnl_usd"] - hold_summary["pnl_usd"], 6),
        "samples": exit_rows[:20],
    }


def run_e6_exit(args: argparse.Namespace, resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    events, feed_diag = load_recent_buy_events(
        args.rtds_jsonl,
        scan_limit=int(args.e6_scan_limit),
        scan_max_bytes=int(args.e6_scan_max_bytes),
        max_feed_events=int(args.e6_max_feed_events),
        max_event_age_s=0.0,
        now_ts=0.0,
    )
    reconstructed_signals, reconstructed_diag = build_e6_signals(
        events,
        signal_window_s=float(args.e6_signal_window_s),
        min_flow_usd=float(args.e6_min_flow_usd),
        min_dominance=float(args.e6_min_dominance),
        order_usd=8.0,
        tick_size=float(args.tick_size),
        slippage_ticks=1,
        max_signals=0,
    )
    comparison = compare_e6_exit_orders(
        orders=_paper_orders(args.e6_paper_state),
        events=events,
        resolution_index=load_resolution_index(args.resolutions),
        args=args,
    )
    comparison["feed_diagnostics"] = feed_diag
    comparison["rebuilt_signal_count"] = len(reconstructed_signals)
    comparison["rebuilt_signal_diagnostics"] = reconstructed_diag
    comparison["resolution_windows"] = len(resolutions)
    return comparison


def main() -> int:
    args = parse_args()
    resolutions = _load_resolutions(args.resolutions, max_windows=int(args.max_windows))
    report = {
        "schema_version": 1,
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now(),
        "inputs": {
            "resolutions": args.resolutions,
            "rtds_jsonl": args.rtds_jsonl,
            "resolution_windows": len(resolutions),
        },
        "e7_spot_vs_open": run_e7(args, resolutions),
        "e6_exit_variant": run_e6_exit(args, resolutions),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    e7 = report["e7_spot_vs_open"].get("best") or {}
    e6 = report["e6_exit_variant"]
    print(
        "e7_e6_exit_backtest",
        f"e7_best_trades={e7.get('trades', 0)}",
        f"e7_best_roi_pct={e7.get('roi_pct', 0.0)}",
        f"e6_hold_pnl={e6.get('hold_summary', {}).get('pnl_usd', 0.0)}",
        f"e6_exit_pnl={e6.get('exit_summary', {}).get('pnl_usd', 0.0)}",
        f"output={output}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
