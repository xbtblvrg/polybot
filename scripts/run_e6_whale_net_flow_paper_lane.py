#!/usr/bin/env python3
"""Run the E6 whale-side net-flow paper lane on recent BTC-5m RTDS flow.

Flow stage: OBSERVE/LEARN. This is paper-only and emits the same CopyIntent
contract used by live wallet copy, but never enables live order submission.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_whale_consensus_paper_lane import (  # noqa: E402
    DEFAULT_RTDS_JSONL,
    WhaleConsensusFeedEvent,
    _feed_event_from_rtds,
    _iter_recent_jsonl,
)
from src.wallet_copy.models import CopyIntent, num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.paper import PaperExecutionConfig, PaperWalletCopyEngine  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402
from scripts.score_e6_whale_net_flow_paper_lane import (  # noqa: E402
    DEFAULT_RESOLUTIONS,
    build_scored_state,
)


DEFAULT_BACKTEST_REPORT = "data/research/e6_whale_side_backtest_20260705.json"
DEFAULT_STATE = "data/research/e6_whale_net_flow_paper_lane_state.json"
DEFAULT_SIGNAL_EVENTS = "data/research/e6_whale_net_flow_paper_lane_events.jsonl"
DEFAULT_PAPER_STATE = "data/research/e6_whale_net_flow_paper_state.json"
DEFAULT_PAPER_EVENTS = "data/research/e6_whale_net_flow_paper_events.jsonl"

LANE_ID = "e6_whale_net_flow_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backtest-report", default=DEFAULT_BACKTEST_REPORT)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--signal-event-log", default=DEFAULT_SIGNAL_EVENTS)
    parser.add_argument("--paper-state", default=DEFAULT_PAPER_STATE)
    parser.add_argument("--paper-event-log", default=DEFAULT_PAPER_EVENTS)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--no-fetch-missing-gamma-resolutions", action="store_true")
    parser.add_argument("--resolution-timeout-s", type=float, default=8.0)
    parser.add_argument("--user-agent", default="Mozilla/5.0")
    parser.add_argument("--scan-limit", type=int, default=50_000)
    parser.add_argument("--scan-max-bytes", type=int, default=192_000_000)
    parser.add_argument("--max-feed-events", type=int, default=5_000)
    parser.add_argument("--max-signals", type=int, default=16)
    parser.add_argument("--max-event-age-s", type=float, default=900.0)
    parser.add_argument("--signal-window-s", type=float, default=120.0)
    parser.add_argument("--min-flow-usd", type=float, default=20.0)
    parser.add_argument("--min-dominance", type=float, default=0.65)
    parser.add_argument("--order-usd", type=float, default=8.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--slippage-ticks", type=int, default=1)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--min-fill-ratio", type=float, default=0.999)
    parser.add_argument("--reset-paper-state", action="store_true")
    parser.add_argument("--no-apply-paper", action="store_true")
    return parser.parse_args()


def load_recent_buy_events(
    path: str,
    *,
    scan_limit: int,
    scan_max_bytes: int,
    max_feed_events: int,
    max_event_age_s: float,
    now_ts: float,
) -> tuple[list[WhaleConsensusFeedEvent], dict[str, int]]:
    rows, diagnostics = _iter_recent_jsonl(path, limit=scan_limit, max_bytes=scan_max_bytes)
    counts = Counter(diagnostics)
    events: list[WhaleConsensusFeedEvent] = []
    seen: set[str] = set()
    for row in rows:
        event = _feed_event_from_rtds(row)
        if event is None:
            counts["not_rtds_btc5m_trade"] += 1
            continue
        if event.side != "BUY":
            counts["non_buy_trade"] += 1
            continue
        if event.event_id in seen:
            counts["duplicate_event"] += 1
            continue
        age_s = max(0.0, float(now_ts) - float(event.observed_ts))
        if max_event_age_s > 0 and age_s > float(max_event_age_s):
            counts["stale_observed_event"] += 1
            continue
        seen.add(event.event_id)
        events.append(event)
        if len(events) >= max(1, int(max_feed_events)):
            break
    counts["accepted_buy_events"] = len(events)
    return events, dict(sorted(counts.items()))


def _weighted_avg_price(events: list[WhaleConsensusFeedEvent]) -> float:
    shares = sum(max(0.0, float(event.size)) for event in events)
    if shares <= 0:
        return 0.0
    return sum(float(event.price) * float(event.size) for event in events) / shares


def build_e6_signals(
    events: list[WhaleConsensusFeedEvent],
    *,
    signal_window_s: float,
    min_flow_usd: float,
    min_dominance: float,
    order_usd: float,
    tick_size: float,
    slippage_ticks: int,
    max_signals: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_market: dict[str, list[WhaleConsensusFeedEvent]] = defaultdict(list)
    diagnostics: Counter[str] = Counter()
    for event in events:
        offset_s = float(event.event_ts) - float(event.window_start_s)
        if offset_s < 0 or offset_s > float(signal_window_s):
            diagnostics["outside_signal_window"] += 1
            continue
        by_market[event.market_slug].append(event)

    signals: list[dict[str, Any]] = []
    for market_slug, rows in sorted(by_market.items()):
        rows_by_outcome: dict[str, list[WhaleConsensusFeedEvent]] = defaultdict(list)
        for row in rows:
            rows_by_outcome[row.outcome].append(row)
        outcome_flow = {
            outcome: sum(float(event.source_usd) for event in outcome_rows)
            for outcome, outcome_rows in rows_by_outcome.items()
        }
        total_flow = sum(outcome_flow.values())
        if total_flow <= 0:
            diagnostics["zero_flow_window"] += 1
            continue
        outcome = max(outcome_flow, key=lambda item: (outcome_flow[item], item))
        dominant_flow = float(outcome_flow[outcome])
        dominance = dominant_flow / total_flow if total_flow > 0 else 0.0
        if dominant_flow < float(min_flow_usd):
            diagnostics["dominant_flow_below_threshold"] += 1
            continue
        if dominance < float(min_dominance):
            diagnostics["dominance_below_threshold"] += 1
            continue
        outcome_rows = sorted(rows_by_outcome[outcome], key=lambda item: (item.event_ts, item.observed_ts, item.event_id))
        trigger = outcome_rows[-1]
        avg_price = _weighted_avg_price(outcome_rows)
        limit_price = min(0.99, max(0.01, avg_price + float(tick_size) * int(slippage_ticks)))
        signal_id = stable_id(
            "e6",
            {
                "market_slug": market_slug,
                "outcome": outcome,
                "signal_window_s": round(float(signal_window_s), 6),
                "min_flow_usd": round(float(min_flow_usd), 6),
                "min_dominance": round(float(min_dominance), 6),
                "slippage_ticks": int(slippage_ticks),
                "order_usd": round(float(order_usd), 6),
            },
        )
        source_wallets = sorted({event.source_wallet for event in outcome_rows})
        signals.append(
            {
                "schema_version": 1,
                "event": "e6_whale_net_flow_signal",
                "flow_stage": "OBSERVE",
                "copy_model": "e6_whale_side",
                "lane": LANE_ID,
                "signal_id": signal_id,
                "generated_at": utc_now_iso(),
                "market_slug": market_slug,
                "condition_id": trigger.condition_id,
                "outcome": outcome,
                "side": "YES" if outcome == "Up" else "NO",
                "avg_price": round(avg_price, 6),
                "limit_price": round(limit_price, 6),
                "dominant_flow_usd": round(dominant_flow, 6),
                "total_flow_usd": round(total_flow, 6),
                "dominance": round(dominance, 6),
                "signal_window_s": round(float(signal_window_s), 6),
                "min_flow_usd": round(float(min_flow_usd), 6),
                "min_dominance": round(float(min_dominance), 6),
                "order_usd": round(float(order_usd), 6),
                "tick_size": round(float(tick_size), 6),
                "slippage_ticks": int(slippage_ticks),
                "source_event_id": trigger.event_id,
                "source_wallet": trigger.source_wallet,
                "source_wallets": source_wallets,
                "source_wallet_count": len(source_wallets),
                "event_count": len(outcome_rows),
                "market_buy_event_count": len(rows),
                "window_start_s": trigger.window_start_s,
                "event_ts": trigger.event_ts,
                "observed_ts": trigger.observed_ts,
                "seconds_from_open": round(float(trigger.event_ts) - float(trigger.window_start_s), 6),
                "token_id": trigger.token_id,
                "transaction_hash": trigger.transaction_hash,
                "outcome_flow_usd": {key: round(value, 6) for key, value in sorted(outcome_flow.items())},
                "sample_events": [
                    {
                        "event_id": event.event_id,
                        "wallet": event.source_wallet,
                        "price": round(float(event.price), 6),
                        "size": round(float(event.size), 6),
                        "source_usd": round(float(event.source_usd), 6),
                        "event_ts": event.event_ts,
                    }
                    for event in outcome_rows[-20:]
                ],
            }
        )
        if max_signals and len(signals) >= int(max_signals):
            break
    diagnostics["windows"] = len(by_market)
    diagnostics["signals"] = len(signals)
    return signals, dict(sorted(diagnostics.items()))


def e6_signal_to_intent(signal: dict[str, Any]) -> CopyIntent:
    order_usd = max(0.0, num(signal.get("order_usd"), 8.0))
    price = max(0.000001, num(signal.get("limit_price"), 0.5))
    metadata = {
        "copy_model": "e6_whale_side",
        "e6_whale_net_flow_v1": signal,
        "row_type": "e6_whale_net_flow_signal",
        "source_fingerprint": str(signal.get("signal_id") or ""),
        "live_candidate_member": False,
        "promotion_gate": {
            "resolved_paper_fills_required": 50,
            "requires_positive_pnl": True,
            "scale_after_resolved_paper_fills": 150,
        },
    }
    return CopyIntent(
        intent_id=stable_id("ci", {"e6_whale_net_flow_signal_id": signal.get("signal_id")}),
        source_wallet="E6_WHALE_SIDE",
        wallet_name=LANE_ID,
        source_event_id=str(signal.get("signal_id") or ""),
        condition_id=str(signal.get("condition_id") or ""),
        market_slug=str(signal.get("market_slug") or ""),
        outcome=str(signal.get("outcome") or ""),
        side=str(signal.get("side") or ""),
        limit_price=round(price, 6),
        wallet_usdc_size=round(order_usd, 6),
        copy_size_usd=round(order_usd, 6),
        shares=round(order_usd / price, 6) if price > 0 else 0.0,
        observed_ts=num(signal.get("observed_ts")),
        strategy_family=LANE_ID,
        policy_id=(
            f"{LANE_ID}_flow{str(signal.get('min_flow_usd')).replace('.', 'p')}"
            f"_dom{str(signal.get('min_dominance')).replace('.', 'p')}"
            f"_w{str(signal.get('signal_window_s')).replace('.', 'p')}"
        ),
        sizing_policy_id=f"fixed_usd_{str(round(order_usd, 6)).replace('.', 'p')}",
        mode="paper",
        action="BUY",
        order_type="PAPER_SOURCE_FILL",
        token_id=str(signal.get("token_id") or ""),
        event_ts=num(signal.get("event_ts")) or None,
        api_latency_s=max(0.0, num(signal.get("observed_ts")) - num(signal.get("event_ts"))),
        live_orders_allowed=False,
        reason="E6 whale-side first-120s net-flow paper signal",
        metadata=metadata,
    )


def build_state(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = time.time()
    prior_state = load_json(args.state, default={})
    prior_state = prior_state if isinstance(prior_state, dict) else {}
    events, feed_diagnostics = load_recent_buy_events(
        args.rtds_jsonl,
        scan_limit=int(args.scan_limit),
        scan_max_bytes=int(args.scan_max_bytes),
        max_feed_events=int(args.max_feed_events),
        max_event_age_s=float(args.max_event_age_s),
        now_ts=now_ts,
    )
    signals, signal_diagnostics = build_e6_signals(
        events,
        signal_window_s=float(args.signal_window_s),
        min_flow_usd=float(args.min_flow_usd),
        min_dominance=float(args.min_dominance),
        order_usd=float(args.order_usd),
        tick_size=float(args.tick_size),
        slippage_ticks=int(args.slippage_ticks),
        max_signals=int(args.max_signals),
    )
    intents = [e6_signal_to_intent(signal) for signal in signals]
    paper_state: dict[str, Any] = {}
    if not args.no_apply_paper:
        engine = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=str(args.paper_state),
                event_log_path=str(args.paper_event_log),
                fill_model="e6_whale_net_flow_source_fill_v1",
                fallback_slippage_bps=float(args.slippage_bps),
                min_fill_ratio=float(args.min_fill_ratio),
                allow_fallback_without_book=True,
                reset_existing_state=bool(args.reset_paper_state),
                retain_orders=50_000,
                retain_lifecycle_events=150_000,
            )
        )
        paper_state = engine.apply_intents(intents)
    else:
        paper_state = load_json(args.paper_state, default={})
        paper_state = paper_state if isinstance(paper_state, dict) else {}

    previous_ids = {str(row) for row in prior_state.get("signal_ids") or [] if row}
    new_signal_rows = [signal for signal in signals if str(signal.get("signal_id") or "") not in previous_ids]
    append_jsonl_many(args.signal_event_log, new_signal_rows)

    signal_ids = sorted(previous_ids | {str(signal.get("signal_id") or "") for signal in signals if signal.get("signal_id")})
    backtest_report = load_json(args.backtest_report, default={})
    state = {
        "schema_version": 1,
        "kind": "e6_whale_net_flow_paper_lane_state",
        "lane": LANE_ID,
        "flow_stage": "OBSERVE",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "updated_at": utc_now_iso(),
        "parameters": {
            "signal_window_s": float(args.signal_window_s),
            "min_flow_usd": float(args.min_flow_usd),
            "min_dominance": float(args.min_dominance),
            "order_usd": float(args.order_usd),
            "tick_size": float(args.tick_size),
            "slippage_ticks": int(args.slippage_ticks),
            "max_event_age_s": float(args.max_event_age_s),
            "scan_limit": int(args.scan_limit),
            "max_feed_events": int(args.max_feed_events),
            "resolutions": args.resolutions,
        },
        "backtest_evidence": {
            "path": args.backtest_report,
            "summary": backtest_report.get("summary") if isinstance(backtest_report, dict) else None,
            "parameters": backtest_report.get("parameters") if isinstance(backtest_report, dict) else None,
        },
        "diagnostics": {
            "feed": feed_diagnostics,
            "signals": signal_diagnostics,
            "new_signal_rows": len(new_signal_rows),
        },
        "current_signals": signals,
        "current_intents": [intent.asdict() for intent in intents],
        "signal_ids": signal_ids[-250_000:],
        "summary": {
            "accepted_buy_events": feed_diagnostics.get("accepted_buy_events", 0),
            "windows": signal_diagnostics.get("windows", 0),
            "signals": len(signals),
            "cumulative_signal_count": len(signal_ids),
            "new_signal_rows": len(new_signal_rows),
            "paper_orders": (paper_state.get("summary") or {}).get("paper_orders") if isinstance(paper_state, dict) else 0,
            "filled_orders": (paper_state.get("summary") or {}).get("filled_orders") if isinstance(paper_state, dict) else 0,
            "live_orders_allowed": False,
            "paper_only": True,
        },
        "promotion_gate": {},
    }
    state = build_scored_state(
        lane_state=state,
        paper_state=paper_state,
        resolutions_path=args.resolutions,
        fetch_missing_gamma=not bool(args.no_fetch_missing_gamma_resolutions),
        append_fetched_resolutions=True,
        timeout_s=float(args.resolution_timeout_s),
        user_agent=str(args.user_agent),
    )
    atomic_write_json(args.state, state)
    return state


def main() -> int:
    args = parse_args()
    state = build_state(args)
    print(
        {
            "lane": state.get("lane"),
            "updated_at": state.get("updated_at"),
            "signals": state.get("summary", {}).get("signals"),
            "cumulative_signal_count": state.get("summary", {}).get("cumulative_signal_count"),
            "paper_orders": state.get("summary", {}).get("paper_orders"),
            "paper_only": True,
            "live_orders_allowed": False,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
