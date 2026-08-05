#!/usr/bin/env python3
"""Run the paper-only whale consensus lane on recent realtime BTC-5m flow.

Flow stage: OBSERVE/LEARN. This reads realtime RTDS rows, emits paper
CopyIntents tagged copy_model=consensus, and applies them to a separate paper
ledger with CLOB book evidence required. It never submits live orders.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backtest_whale_consensus_v1 import (  # noqa: E402
    DEFAULT_POLYGON_JSONL,
    DEFAULT_RESOLUTIONS,
    iter_fill_events,
    load_token_map,
    profile_weight_rows_before,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.paper import PaperExecutionConfig, PaperWalletCopyEngine  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


DEFAULT_BACKTEST_REPORT = "data/research/whale_consensus_v1_backtest_latest.json"
DEFAULT_RTDS_JSONL = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_STATE = "data/research/whale_consensus_paper_lane_state.json"
DEFAULT_SIGNAL_EVENTS = "data/research/whale_consensus_paper_lane_events.jsonl"
DEFAULT_PAPER_STATE = "data/research/whale_consensus_paper_state.json"
DEFAULT_PAPER_EVENTS = "data/research/whale_consensus_paper_events.jsonl"
DEFAULT_CLOB_BASE = "http://127.0.0.1:8787/clob"
DEFAULT_RELAXED_THRESHOLD = 0.5
DEFAULT_RELAXED_PMAX = 0.55
DEFAULT_DECISION_DEADLINE_UTC = "2026-07-06T20:00:00Z"


@dataclass(frozen=True)
class WhaleConsensusFeedEvent:
    source_wallet: str
    market_slug: str
    condition_id: str
    outcome: str
    side: str
    price: float
    size: float
    event_ts: float
    observed_ts: float
    token_id: str
    transaction_hash: str
    event_id: str

    @property
    def window_start_s(self) -> float:
        return _btc_5m_window_start_s(self.market_slug) or 0.0

    @property
    def source_usd(self) -> float:
        return max(0.0, float(self.price) * float(self.size))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backtest-report", default=DEFAULT_BACKTEST_REPORT)
    parser.add_argument("--polygon-jsonl", default=DEFAULT_POLYGON_JSONL)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--signal-event-log", default=DEFAULT_SIGNAL_EVENTS)
    parser.add_argument("--paper-state", default=DEFAULT_PAPER_STATE)
    parser.add_argument("--paper-event-log", default=DEFAULT_PAPER_EVENTS)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--scan-limit", type=int, default=25_000)
    parser.add_argument("--scan-max-bytes", type=int, default=128_000_000)
    parser.add_argument("--max-history-rows", type=int, default=0)
    parser.add_argument("--max-feed-events", type=int, default=2_500)
    parser.add_argument("--max-signals", type=int, default=8)
    parser.add_argument("--max-event-age-s", type=float, default=300.0)
    parser.add_argument("--threshold", type=float, default=0.0)
    parser.add_argument("--pmax", type=float, default=0.0)
    parser.add_argument("--top-k", type=int, default=0)
    parser.add_argument("--order-usd", type=float, default=0.0)
    parser.add_argument("--max-entry-offset-s", type=float, default=0.0)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--min-fill-ratio", type=float, default=0.999)
    parser.add_argument("--decision-deadline-utc", default=DEFAULT_DECISION_DEADLINE_UTC)
    parser.add_argument("--reset-paper-state", action="store_true")
    parser.add_argument("--no-apply-paper", action="store_true")
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _btc_5m_window_start_s(market_slug: str) -> float | None:
    slug = str(market_slug or "")
    if not slug.startswith("btc-updown-5m-"):
        return None
    marker = slug.rsplit("-", 1)[-1]
    if not marker.isdigit():
        return None
    return float(marker)


def _iter_recent_jsonl(path: str, *, limit: int, max_bytes: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    target = Path(path)
    diagnostics: Counter[str] = Counter()
    if not target.exists():
        diagnostics["missing_input"] += 1
        return [], dict(diagnostics)
    chunks: list[bytes] = []
    with target.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        scanned = 0
        while position > 0 and scanned < max(1, int(max_bytes)):
            size = min(1_048_576, position, max(1, int(max_bytes)) - scanned)
            position -= size
            handle.seek(position)
            chunks.append(handle.read(size))
            scanned += size
    rows: list[dict[str, Any]] = []
    for raw in reversed(b"".join(reversed(chunks)).splitlines()):
        diagnostics["lines_seen"] += 1
        try:
            row = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            diagnostics["bad_json"] += 1
            continue
        if not isinstance(row, dict):
            diagnostics["non_object"] += 1
            continue
        rows.append(row)
        if len(rows) >= max(1, int(limit)):
            break
    diagnostics["rows_loaded"] = len(rows)
    return list(reversed(rows)), dict(diagnostics)


def _feed_event_from_rtds(row: dict[str, Any]) -> WhaleConsensusFeedEvent | None:
    if row.get("event") != "rtds_trade_event":
        return None
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    market_slug = str(row.get("market_slug") or raw.get("slug") or raw.get("eventSlug") or "")
    if _btc_5m_window_start_s(market_slug) is None:
        return None
    wallet = _norm_wallet(row.get("source_wallet") or row.get("proxyWallet"))
    if not wallet:
        return None
    outcome_raw = str(row.get("outcome") or raw.get("outcome") or "").strip().lower()
    if outcome_raw in {"up", "yes"}:
        outcome = "Up"
    elif outcome_raw in {"down", "no"}:
        outcome = "Down"
    else:
        return None
    side = str(row.get("side") or raw.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return None
    price = num(row.get("price") or raw.get("price"))
    size = num(row.get("size") or raw.get("size"))
    event_ts = num(row.get("event_ts") or row.get("timestamp") or raw.get("timestamp"))
    observed_ts = num(row.get("received_at_s") or row.get("captured_at_s") or event_ts)
    token_id = str(row.get("asset") or row.get("asset_id") or raw.get("asset") or "")
    tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
    if price <= 0.0 or size <= 0.0 or event_ts <= 0.0 or observed_ts <= 0.0 or not token_id:
        return None
    event_id = str(row.get("event_id") or "") or stable_id(
        "wce",
        {
            "wallet": wallet,
            "tx": tx,
            "market_slug": market_slug,
            "outcome": outcome,
            "side": side,
            "price": round(price, 8),
            "size": round(size, 8),
            "event_ts": event_ts,
        },
    )
    return WhaleConsensusFeedEvent(
        source_wallet=wallet,
        market_slug=market_slug,
        condition_id=str(row.get("condition_id") or row.get("conditionId") or raw.get("conditionId") or ""),
        outcome=outcome,
        side=side,
        price=price,
        size=size,
        event_ts=event_ts,
        observed_ts=observed_ts,
        token_id=token_id,
        transaction_hash=tx,
        event_id=event_id,
    )


def load_recent_feed_events(
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
        observed_age_s = max(0.0, now_ts - event.observed_ts)
        if max_event_age_s > 0 and observed_age_s > float(max_event_age_s):
            counts["stale_observed_event"] += 1
            continue
        if event.event_id in seen:
            counts["duplicate_event"] += 1
            continue
        seen.add(event.event_id)
        events.append(event)
        if len(events) >= max(1, int(max_feed_events)):
            break
    counts["accepted_feed_events"] = len(events)
    return events, dict(sorted(counts.items()))


def _default_params(backtest_report: dict[str, Any], args: argparse.Namespace, prior_state: dict[str, Any] | None = None) -> dict[str, Any]:
    params = backtest_report.get("parameters") if isinstance(backtest_report.get("parameters"), dict) else {}
    prior_params = prior_state.get("parameters") if isinstance(prior_state, dict) and isinstance(prior_state.get("parameters"), dict) else {}
    threshold = (
        float(args.threshold)
        if args.threshold > 0
        else num(prior_params.get("threshold"), DEFAULT_RELAXED_THRESHOLD)
    )
    pmax = (
        float(args.pmax)
        if args.pmax > 0
        else max(num(prior_params.get("pmax"), DEFAULT_RELAXED_PMAX), DEFAULT_RELAXED_PMAX)
    )
    top_k = int(args.top_k) if args.top_k > 0 else int(num(params.get("top_k"), 20))
    order_usd = float(args.order_usd) if args.order_usd > 0 else num(params.get("order_usd"), 1.0)
    max_entry_offset_s = (
        float(args.max_entry_offset_s)
        if args.max_entry_offset_s > 0
        else num(params.get("max_entry_offset_s"), 240.0)
    )
    return {
        "threshold": threshold,
        "pmax": pmax,
        "top_k": top_k,
        "order_usd": order_usd,
        "max_entry_offset_s": max_entry_offset_s,
        "slippage_bps": float(args.slippage_bps),
        "min_fill_ratio": float(args.min_fill_ratio),
    }


def _baseline_params(backtest_report: dict[str, Any]) -> dict[str, float]:
    best = backtest_report.get("best") if isinstance(backtest_report.get("best"), dict) else {}
    return {
        "threshold": num(best.get("threshold"), 0.5),
        "pmax": num(best.get("pmax"), 0.45),
    }


def _is_relaxed_signal(signal: dict[str, Any], baseline: dict[str, float]) -> bool:
    threshold = num(signal.get("threshold"), baseline["threshold"])
    pmax = num(signal.get("pmax"), baseline["pmax"])
    return threshold < float(baseline["threshold"]) or pmax > float(baseline["pmax"])


def _signal_snapshot(signal: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(signal, dict):
        return None
    return {
        "signal_id": signal.get("signal_id"),
        "generated_at": signal.get("generated_at"),
        "market_slug": signal.get("market_slug"),
        "outcome": signal.get("outcome"),
        "price": signal.get("price"),
        "threshold": signal.get("threshold"),
        "pmax": signal.get("pmax"),
        "signal_strength": signal.get("signal_strength"),
        "clob_instant_fill_status": signal.get("clob_instant_fill_status"),
        "clob_blocking_reason": signal.get("clob_blocking_reason"),
    }


def _load_signal_event_evidence(path: str, *, baseline: dict[str, float]) -> dict[str, Any]:
    target = Path(path)
    rows_by_id: dict[str, dict[str, Any]] = {}
    if target.exists():
        for raw in target.read_text(errors="replace").splitlines():
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict) or row.get("event") != "whale_consensus_signal":
                continue
            signal_id = str(row.get("signal_id") or "")
            if signal_id:
                rows_by_id[signal_id] = row
    rows = sorted(
        rows_by_id.values(),
        key=lambda row: (str(row.get("generated_at") or ""), num(row.get("event_ts")), str(row.get("signal_id") or "")),
    )
    relaxed_rows = [row for row in rows if _is_relaxed_signal(row, baseline)]
    fillable_rows = [row for row in rows if str(row.get("clob_instant_fill_status") or "").upper() == "PASS"]
    return {
        "signal_events": len(rows),
        "relaxed_signal_events": len(relaxed_rows),
        "fillable_signal_events": len(fillable_rows),
        "latest_signal": _signal_snapshot(rows[-1] if rows else None),
        "latest_relaxed_signal": _signal_snapshot(relaxed_rows[-1] if relaxed_rows else None),
    }


def _profile_weights_from_history(
    *,
    polygon_jsonl: str,
    resolutions: str,
    window_start_s: float,
    top_k: int,
    max_rows: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    token_map = load_token_map(Path(resolutions))
    events, diagnostics = iter_fill_events(Path(polygon_jsonl), token_map, max_rows=max_rows)
    weights = profile_weight_rows_before(events, window_start_s, limit=top_k)
    return weights, {
        "source": "polygon_history_scan",
        "token_map_entries": len(token_map),
        "diagnostics": diagnostics,
        "events": len(events),
    }


def load_profile_weights(
    *,
    backtest_report: dict[str, Any],
    current_window_start_s: float,
    top_k: int,
    polygon_jsonl: str,
    resolutions: str,
    max_history_rows: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = backtest_report.get("latest_profile_weights")
    if isinstance(rows, list) and rows:
        normalized = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            wallet = _norm_wallet(row.get("wallet"))
            weight = num(row.get("weight"))
            if wallet and weight > 0:
                normalized.append({**row, "wallet": wallet, "weight": weight})
        normalized.sort(key=lambda item: (num(item.get("weight")), num(item.get("pnl_usd")), str(item.get("wallet"))), reverse=True)
        return normalized[:top_k], {
            "source": "backtest_report_latest_profile_weights",
            "available_weights": len(normalized),
            "profile_window_start_s": backtest_report.get("latest_profile_window_start_s"),
        }
    return _profile_weights_from_history(
        polygon_jsonl=polygon_jsonl,
        resolutions=resolutions,
        window_start_s=current_window_start_s,
        top_k=top_k,
        max_rows=max_history_rows,
    )


def build_live_consensus_signals(
    events: list[WhaleConsensusFeedEvent],
    profile_weights: list[dict[str, Any]],
    *,
    threshold: float,
    pmax: float,
    top_k: int,
    order_usd: float,
    max_entry_offset_s: float,
    max_signals: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    selected_weights = {
        _norm_wallet(row.get("wallet")): num(row.get("weight"))
        for row in profile_weights[: max(1, int(top_k))]
        if _norm_wallet(row.get("wallet")) and num(row.get("weight")) > 0
    }
    by_window: dict[str, list[WhaleConsensusFeedEvent]] = defaultdict(list)
    for event in events:
        by_window[event.market_slug].append(event)

    diagnostics: Counter[str] = Counter()
    signals: list[dict[str, Any]] = []
    for market_slug, rows in sorted(by_window.items()):
        rows = sorted(rows, key=lambda item: (item.event_ts, item.observed_ts, item.event_id))
        accum = {"Up": 0.0, "Down": 0.0}
        contributors: dict[str, dict[str, Any]] = {}
        fired = False
        for event in rows:
            weight = selected_weights.get(event.source_wallet)
            if weight is None:
                diagnostics["wallet_not_in_top_profile"] += 1
                continue
            offset_s = event.event_ts - event.window_start_s
            if offset_s < 0 or offset_s > float(max_entry_offset_s):
                diagnostics["outside_entry_window"] += 1
                continue
            direction = 1.0 if event.side == "BUY" else -1.0
            signed_flow = direction * float(event.size) * float(weight)
            accum[event.outcome] += signed_flow
            contributor = contributors.setdefault(
                event.source_wallet,
                {"wallet": event.source_wallet, "events": 0, "weighted_net": 0.0, "source_usd": 0.0},
            )
            contributor["events"] += 1
            contributor["weighted_net"] = round(num(contributor.get("weighted_net")) + signed_flow, 9)
            contributor["source_usd"] = round(num(contributor.get("source_usd")) + event.source_usd, 6)
            signal_outcome = "Up" if accum["Up"] >= accum["Down"] else "Down"
            signal_strength = accum[signal_outcome]
            if signal_strength < float(threshold):
                diagnostics["signal_below_threshold"] += 1
                continue
            if event.side != "BUY":
                diagnostics["trigger_row_not_buy"] += 1
                continue
            if event.outcome != signal_outcome:
                diagnostics["trigger_row_not_signal_outcome"] += 1
                continue
            if event.price > float(pmax):
                diagnostics["trigger_price_above_pmax"] += 1
                continue
            signal_id = stable_id(
                "wcs",
                {
                    "market_slug": market_slug,
                    "outcome": signal_outcome,
                    "event_id": event.event_id,
                    "threshold": round(float(threshold), 9),
                    "pmax": round(float(pmax), 9),
                    "top_k": int(top_k),
                },
            )
            signals.append(
                {
                    "schema_version": 1,
                    "flow_stage": "OBSERVE",
                    "copy_model": "consensus",
                    "lane": "whale_consensus_v1",
                    "signal_id": signal_id,
                    "market_slug": market_slug,
                    "condition_id": event.condition_id,
                    "outcome": signal_outcome,
                    "side": "YES" if signal_outcome == "Up" else "NO",
                    "price": round(event.price, 6),
                    "source_event_id": event.event_id,
                    "source_wallet": event.source_wallet,
                    "source_wallet_weight": round(float(weight), 9),
                    "source_wallets": sorted(contributors),
                    "contributors": sorted(contributors.values(), key=lambda item: item["weighted_net"], reverse=True),
                    "signal_strength": round(signal_strength, 9),
                    "accum_up": round(accum["Up"], 9),
                    "accum_down": round(accum["Down"], 9),
                    "threshold": round(float(threshold), 9),
                    "pmax": round(float(pmax), 9),
                    "top_k": int(top_k),
                    "order_usd": round(float(order_usd), 6),
                    "max_entry_offset_s": round(float(max_entry_offset_s), 6),
                    "event_ts": event.event_ts,
                    "observed_ts": event.observed_ts,
                    "window_start_s": event.window_start_s,
                    "seconds_from_open": round(offset_s, 6),
                    "token_id": event.token_id,
                    "transaction_hash": event.transaction_hash,
                }
            )
            fired = True
            break
        if not fired:
            diagnostics["window_without_signal"] += 1
        if max_signals and len(signals) >= int(max_signals):
            break
    diagnostics["signals"] = len(signals)
    diagnostics["profile_wallets"] = len(selected_weights)
    diagnostics["windows"] = len(by_window)
    return signals, dict(sorted(diagnostics.items()))


def _clob_summary_for_signal(
    signal: dict[str, Any],
    *,
    clob: CLOBMarketClient,
    order_usd: float,
    slippage_bps: float,
) -> dict[str, Any]:
    token_id = str(signal.get("token_id") or "")
    if not token_id:
        return {"status": "ERROR", "error_type": "MissingTokenId", "error": "signal has no token_id"}
    try:
        book = clob.get_book(token_id)
        summary = CLOBMarketClient.summarize_book(
            book,
            copy_size_usd=float(order_usd),
            source_price=float(signal.get("price") or 0.0),
            max_slippage_bps=float(slippage_bps),
        )
        route_report = clob.last_route_report if isinstance(clob.last_route_report, dict) else {}
        return {
            "status": "OK",
            "token_id": token_id,
            **summary,
            "route_report": route_report,
        }
    except Exception as exc:  # noqa: BLE001 - paper lane should persist route failures as evidence.
        primary_route_report = clob.last_route_report if isinstance(clob.last_route_report, dict) else {}
        primary_error = {"error_type": type(exc).__name__, "error": str(exc), "route_report": primary_route_report}
        try:
            response = requests.get(
                f"{CLOBMarketClient.DIRECT_CLOB_HOST}/book",
                params={"token_id": token_id},
                headers={"Accept": "application/json", "User-Agent": "wallet-consensus-paper-lane/1.0"},
                timeout=float(clob.timeout_s),
            )
            response.raise_for_status()
            direct_book = response.json()
            direct_book = direct_book if isinstance(direct_book, dict) else {}
            summary = CLOBMarketClient.summarize_book(
                direct_book,
                copy_size_usd=float(order_usd),
                source_price=float(signal.get("price") or 0.0),
                max_slippage_bps=float(slippage_bps),
            )
            return {
                "status": "OK",
                "token_id": token_id,
                **summary,
                "direct_clob_fallback": True,
                "primary_error": primary_error,
                "route_report": {
                    "status": "PASS",
                    "route_class": "DIRECT_CLOB_FALLBACK",
                    "routed_host": "clob.polymarket.com",
                },
            }
        except Exception as fallback_exc:  # noqa: BLE001 - preserve both route failures.
            return {
                "status": "ERROR",
                "token_id": token_id,
                "error_type": type(fallback_exc).__name__,
                "error": str(fallback_exc),
                "primary_error": primary_error,
                "route_report": primary_route_report,
            }


def consensus_signal_to_intent(signal: dict[str, Any], *, clob_book: dict[str, Any] | None = None) -> CopyIntent:
    order_usd = max(0.0, num(signal.get("order_usd"), 1.0))
    price = max(0.000001, num(signal.get("price"), 0.5))
    metadata = {
        "copy_model": "consensus",
        "whale_consensus_v1": signal,
        "row_type": "whale_consensus_signal",
        "source_fingerprint": str(signal.get("signal_id") or ""),
    }
    if clob_book is not None:
        metadata["live_tracking_evidence"] = {"clob_book": clob_book}
    return CopyIntent(
        intent_id=stable_id("ci", {"whale_consensus_signal_id": signal.get("signal_id")}),
        source_wallet="CONSENSUS",
        wallet_name="whale_consensus_v1",
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
        strategy_family="whale_consensus_v1",
        policy_id=(
            f"whale_consensus_v1_s{str(signal.get('threshold')).replace('.', 'p')}"
            f"_pmax{str(signal.get('pmax')).replace('.', 'p')}_top{int(num(signal.get('top_k'), 20))}"
        ),
        sizing_policy_id=f"fixed_usd_{str(round(order_usd, 6)).replace('.', 'p')}",
        mode="paper",
        action="BUY",
        order_type="PAPER_SOURCE_FILL",
        token_id=str(signal.get("token_id") or ""),
        event_ts=num(signal.get("event_ts")) or None,
        api_latency_s=max(0.0, num(signal.get("observed_ts")) - num(signal.get("event_ts"))),
        live_orders_allowed=False,
        reason="whale consensus live-feed paper signal",
        metadata=metadata,
    )


def _summarize_consensus_orders(paper_state: dict[str, Any], *, baseline: dict[str, float] | None = None) -> dict[str, Any]:
    orders = [
        row
        for row in paper_state.get("orders") or []
        if isinstance(row, dict)
        and str(row.get("source_wallet") or "").lower() == "consensus"
        and str((row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}).get("strategy_family") or "")
        == "whale_consensus_v1"
    ]
    status_counts = Counter(str(row.get("final_status") or "UNKNOWN") for row in orders)
    fill_sources = Counter()
    reject_reasons = Counter()
    relaxed_counts: Counter[str] = Counter()
    for row in orders:
        source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
        fill = source_intent.get("fill_estimate") if isinstance(source_intent.get("fill_estimate"), dict) else {}
        fill_sources[str(fill.get("source") or "unknown")] += 1
        reason = str(fill.get("reject_reason") or "")
        if reason:
            reject_reasons[reason] += 1
        signal = {}
        metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
        if isinstance(metadata.get("whale_consensus_v1"), dict):
            signal = metadata["whale_consensus_v1"]
        if baseline and signal and _is_relaxed_signal(signal, baseline):
            relaxed_counts[str(row.get("final_status") or "UNKNOWN")] += 1
    return {
        "orders": len(orders),
        "filled_orders": int(status_counts.get("FILLED", 0)),
        "rejected_orders": int(status_counts.get("REJECTED", 0)),
        "relaxed_orders": sum(relaxed_counts.values()),
        "relaxed_filled_orders": int(relaxed_counts.get("FILLED", 0)),
        "relaxed_rejected_orders": int(relaxed_counts.get("REJECTED", 0)),
        "status_counts": dict(sorted(status_counts.items())),
        "fill_source_counts": dict(sorted(fill_sources.items())),
        "reject_reason_counts": dict(sorted(reject_reasons.items())),
        "latest_order_ts": max((str(row.get("updated_at") or "") for row in orders), default=None),
    }


def main() -> int:
    args = parse_args()
    now_ts = time.time()
    backtest_report = load_json(args.backtest_report, default={})
    backtest_report = backtest_report if isinstance(backtest_report, dict) else {}
    prior_state = load_json(args.state, default={})
    prior_state = prior_state if isinstance(prior_state, dict) else {}
    params = _default_params(backtest_report, args, prior_state=prior_state)
    baseline = _baseline_params(backtest_report)

    feed_events, feed_diagnostics = load_recent_feed_events(
        args.rtds_jsonl,
        scan_limit=args.scan_limit,
        scan_max_bytes=args.scan_max_bytes,
        max_feed_events=args.max_feed_events,
        max_event_age_s=args.max_event_age_s,
        now_ts=now_ts,
    )
    current_window_start = max((event.window_start_s for event in feed_events), default=float(int(now_ts // 300) * 300))
    profile_weights, profile_diagnostics = load_profile_weights(
        backtest_report=backtest_report,
        current_window_start_s=current_window_start,
        top_k=int(params["top_k"]),
        polygon_jsonl=args.polygon_jsonl,
        resolutions=args.resolutions,
        max_history_rows=args.max_history_rows,
    )
    signals, signal_diagnostics = build_live_consensus_signals(
        feed_events,
        profile_weights,
        threshold=float(params["threshold"]),
        pmax=float(params["pmax"]),
        top_k=int(params["top_k"]),
        order_usd=float(params["order_usd"]),
        max_entry_offset_s=float(params["max_entry_offset_s"]),
        max_signals=int(args.max_signals),
    )

    clob = CLOBMarketClient(host=args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    intents: list[CopyIntent] = []
    signal_rows: list[dict[str, Any]] = []
    clob_counts: Counter[str] = Counter()
    for signal in signals:
        clob_book = _clob_summary_for_signal(
            signal,
            clob=clob,
            order_usd=float(params["order_usd"]),
            slippage_bps=float(params["slippage_bps"]),
        )
        clob_counts[str(clob_book.get("instant_fill_status") or clob_book.get("status") or "UNKNOWN")] += 1
        intent = consensus_signal_to_intent(signal, clob_book=clob_book)
        intents.append(intent)
        signal_rows.append(
            {
                "event": "whale_consensus_signal",
                "generated_at": utc_now_iso(),
                "intent_id": intent.intent_id,
                "clob_book_status": clob_book.get("status"),
                "clob_instant_fill_status": clob_book.get("instant_fill_status"),
                "clob_blocking_reason": clob_book.get("blocking_reason"),
                **signal,
            }
        )

    paper_state: dict[str, Any] = load_json(args.paper_state, default={})
    if not args.no_apply_paper:
        paper_engine = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=args.paper_state,
                event_log_path=args.paper_event_log,
                fill_model="whale_consensus_clob_fill_v1",
                fallback_slippage_bps=float(params["slippage_bps"]),
                min_fill_ratio=float(params["min_fill_ratio"]),
                allow_fallback_without_book=False,
                reset_existing_state=bool(args.reset_paper_state),
                retain_orders=25_000,
                retain_lifecycle_events=75_000,
            )
        )
        paper_state = paper_engine.apply_intents(intents)

    prior_signal_ids = {
        str(item)
        for item in ((prior_state if isinstance(prior_state, dict) else {}).get("signal_ids") or [])
        if item
    }
    new_signal_rows = [row for row in signal_rows if str(row.get("signal_id") or "") not in prior_signal_ids]
    append_jsonl_many(args.signal_event_log, new_signal_rows)

    cumulative_signals = _load_signal_event_evidence(args.signal_event_log, baseline=baseline)
    paper_summary = _summarize_consensus_orders(paper_state if isinstance(paper_state, dict) else {}, baseline=baseline)
    current_scan_status = "PAPER_SIGNAL_APPLIED" if intents else "ANALYZE_NO_CONSENSUS_SIGNAL"
    if intents and paper_summary.get("filled_orders", 0) <= 0:
        current_scan_status = "ANALYZE_CONSENSUS_SIGNAL_NOT_FILLABLE"
    relaxed_active = float(params["threshold"]) < baseline["threshold"] or float(params["pmax"]) > baseline["pmax"]
    status = current_scan_status
    if not intents and cumulative_signals["relaxed_signal_events"] > 0:
        status = "RELAXED_SIGNAL_EVIDENCE_ACCUMULATING"
    state = {
        "schema_version": 1,
        "kind": "whale_consensus_paper_lane_state",
        "flow_stage": "OBSERVE",
        "lane": "whale_consensus_v1",
        "copy_model": "consensus",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": status,
        "decision_deadline_utc": str(args.decision_deadline_utc or ""),
        "generated_at": utc_now_iso(),
        "inputs": {
            "backtest_report": args.backtest_report,
            "rtds_jsonl": args.rtds_jsonl,
            "polygon_jsonl": args.polygon_jsonl,
            "resolutions": args.resolutions,
            "clob_base_url": args.clob_base_url,
        },
        "parameters": params,
        "parameter_source": {
            "threshold": "cli" if args.threshold > 0 else ("prior_state" if prior_state.get("parameters") else "backtest_best"),
            "pmax": "cli" if args.pmax > 0 else ("prior_state" if prior_state.get("parameters") else "backtest_best"),
        },
        "relaxed_lane": {
            "active": relaxed_active,
            "baseline": baseline,
            "current": {"threshold": params["threshold"], "pmax": params["pmax"]},
            "relaxation_reason": "Fable 2026-07-05T15:32:55Z paper-only one-grid-notch relaxation",
            "current_scan_status": current_scan_status,
            "current_scan_signals": len(signal_rows),
            "cumulative_signal_events": cumulative_signals["signal_events"],
            "cumulative_relaxed_signal_events": cumulative_signals["relaxed_signal_events"],
            "cumulative_relaxed_paper_orders": paper_summary["relaxed_orders"],
            "cumulative_relaxed_paper_fills": paper_summary["relaxed_filled_orders"],
            "latest_relaxed_signal": cumulative_signals["latest_relaxed_signal"],
            "early_kill_check_at": "2026-07-06T15:00:00Z",
            "first_live_allocation_decision_at": str(args.decision_deadline_utc or ""),
        },
        "backtest_best": backtest_report.get("best") if isinstance(backtest_report.get("best"), dict) else {},
        "profile_weights": profile_weights,
        "profile_diagnostics": profile_diagnostics,
        "feed": {
            "events": len(feed_events),
            "latest_window_start_s": current_window_start,
            "diagnostics": feed_diagnostics,
        },
        "signals": signal_rows,
        "signal_ids": sorted({*(prior_signal_ids), *(str(row.get("signal_id") or "") for row in signal_rows)}),
        "cumulative_signals": cumulative_signals,
        "intents": [intent.asdict() for intent in intents],
        "diagnostics": {
            "signal": signal_diagnostics,
            "clob": dict(sorted(clob_counts.items())),
            "new_signal_events_appended": len(new_signal_rows),
            "paper_applied": not args.no_apply_paper,
        },
        "paper": {
            "state_path": args.paper_state,
            "event_log_path": args.paper_event_log,
            "summary": paper_summary,
        },
        "decision": {
            "status": status,
            "promotion_allowed": False,
            "deadline_utc": str(args.decision_deadline_utc or ""),
            "next_action": (
                "collect resolved paper PnL for consensus fills before promotion"
                if intents or cumulative_signals["relaxed_signal_events"] > 0
                else "keep paper lane running on fresh RTDS BTC-5m flow until a top-profile consensus signal appears"
            ),
        },
    }
    atomic_write_json(args.state, state)
    print(
        "whale_consensus_paper_lane",
        f"status={status}",
        f"feed_events={len(feed_events)}",
        f"signals={len(signals)}",
        f"intents={len(intents)}",
        f"paper_orders={paper_summary.get('orders', 0)}",
        f"filled={paper_summary.get('filled_orders', 0)}",
        f"state={args.state}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
