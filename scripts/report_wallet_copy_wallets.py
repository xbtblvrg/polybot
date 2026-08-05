#!/usr/bin/env python3
"""Build a wallet-level wallet-copy coverage and performance report.

The report intentionally separates four truths:

- registry coverage: wallets configured for research/copying
- history coverage: wallet-attributed source events ingested
- profit replay: historical candidate ROI/WR/PnL from the profit engine
- paper tracking: our CopyIntent paper lifecycle results and verification evidence
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.performance import load_resolutions, score_paper_state
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.tactic_performance import score_tactic_replay_pnl


TARGET_SOURCE_HIGH_ROI_PCT = 20.0
TARGET_SOURCE_HIGH_WR_PCT = 70.0
TARGET_SOURCE_HIGH_LEADERBOARD_PNL_USD = 10_000.0
TARGET_PAPER_MIN_RESOLVED_ORDERS = 100
TARGET_PAPER_MIN_ROI_PCT = 5.0
TARGET_PAPER_MIN_WR_PCT = 70.0
TARGET_ACTIVE_POLICY_COPY_COVERAGE_PCT = 95.0
TARGET_ACTIVE_ALL_ORDER_FILL_RATE_PCT = 99.0
TARGET_ACTIVE_ALL_ORDER_REJECT_RATE_MAX_PCT = 5.0


def _max_full_state_load_bytes() -> int:
    raw = os.getenv("WALLET_COPY_REPORT_MAX_FULL_STATE_BYTES", "").strip()
    if not raw:
        return 64 * 1024 * 1024
    try:
        return max(1, int(raw))
    except ValueError:
        return 64 * 1024 * 1024


def _extract_tail_value_for_key(
    path: str | Path,
    key: str,
    *,
    max_bytes: int = 8 * 1024 * 1024,
) -> Any:
    target = Path(path)
    if not target.exists():
        return None
    try:
        size = target.stat().st_size
        with target.open("rb") as handle:
            handle.seek(max(0, size - max(1, int(max_bytes))))
            text = handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return None

    marker = f'"{key}"'
    start = text.rfind(marker)
    if start < 0:
        return None
    colon = text.find(":", start + len(marker))
    if colon < 0:
        return None
    index = colon + 1
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text) or text[index] not in "{[":
        return None

    opener = text[index]
    closer = "}" if opener == "{" else "]"
    depth = 0
    in_string = False
    escaped = False
    for pos in range(index, len(text)):
        char = text[pos]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[index : pos + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _extract_tail_object_for_key(path: str | Path, key: str, *, max_bytes: int = 8 * 1024 * 1024) -> dict[str, Any]:
    value = _extract_tail_value_for_key(path, key, max_bytes=max_bytes)
    return value if isinstance(value, dict) else {}


def _extract_tail_list_for_key(path: str | Path, key: str, *, max_bytes: int = 8 * 1024 * 1024) -> list[Any]:
    value = _extract_tail_value_for_key(path, key, max_bytes=max_bytes)
    return value if isinstance(value, list) else []


def _large_state_stub(path: str | Path, *, size_bytes: int) -> dict[str, Any]:
    summary = _extract_tail_object_for_key(path, "summary")
    wallets = _extract_tail_object_for_key(path, "wallets")
    wallet_results = _extract_tail_list_for_key(path, "wallet_results")
    stub: dict[str, Any] = {
        "_wallet_report_large_state_stub": True,
        "_file_size_bytes": int(size_bytes),
        "_reason": "state file exceeds bounded wallet-report full-load limit",
    }
    if summary:
        stub["summary"] = summary
    if wallets:
        stub["wallets"] = wallets
    if wallet_results:
        stub["wallet_results"] = wallet_results
    return stub


def _load_report_state(path: str | Path, *, default: Any = None) -> dict[str, Any]:
    target = Path(path)
    try:
        size = target.stat().st_size
    except OSError:
        size = 0
    if size > _max_full_state_load_bytes():
        return _large_state_stub(path, size_bytes=size)
    data = load_json(path, default={} if default is None else default)
    return data if isinstance(data, dict) else {}


def _state_load_metadata(path: str | Path, payload: dict[str, Any]) -> dict[str, Any]:
    target = Path(path)
    try:
        size = target.stat().st_size
    except OSError:
        size = 0
    return {
        "path": str(target),
        "size_bytes": int(size),
        "bounded_large_state": bool(payload.get("_wallet_report_large_state_stub")),
        "has_summary": isinstance(payload.get("summary"), dict),
        "wallets": len(payload.get("wallets") or {}) if isinstance(payload.get("wallets"), dict) else 0,
        "wallet_results": len(payload.get("wallet_results") or [])
        if isinstance(payload.get("wallet_results"), list)
        else 0,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--active-hotlane-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--canonical-tracking-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--active-tracking-state", default="data/research/wallet_copy_active_hotlane_live_tracking_state.json")
    parser.add_argument("--canonical-event-log", default="data/research/wallet_copy_live_tracking_events.jsonl")
    parser.add_argument("--active-event-log", default="data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl")
    parser.add_argument("--registry-sweep-state", default="data/research/wallet_copy_registry_sweep_live_tracking_state.json")
    parser.add_argument("--registry-sweep-event-log", default="data/research/wallet_copy_registry_sweep_live_tracking_events.jsonl")
    parser.add_argument("--registry-sweep-paper-state", default="data/research/wallet_copy_registry_sweep_paper_state.json")
    parser.add_argument(
        "--active-all-order-tactic-replay-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
    )
    parser.add_argument(
        "--active-all-order-exact-copy-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
    )
    parser.add_argument("--output", default="data/research/wallet_copy_wallet_analysis_state.json")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument(
        "--print-mode",
        choices=("full", "summary", "none"),
        default="full",
        help="Control stdout size; the output state file is always written in full.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Alias for --print-mode none, kept for low-noise automation callers.",
    )
    return parser.parse_args()


def _addr(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text.startswith("0x") and len(text) >= 42:
            return text[:42]
    return ""


def _first_addr(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        address = _addr(value)
        if address:
            return address
    return ""


def _wallet_from_nested(row: dict[str, Any]) -> str:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    copy_efficiency = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    tracking = row.get("tracking_evidence") if isinstance(row.get("tracking_evidence"), dict) else {}
    wallet_api = tracking.get("wallet_api") if isinstance(tracking.get("wallet_api"), dict) else {}
    raw = wallet_event.get("raw") if isinstance(wallet_event.get("raw"), dict) else {}
    return (
        _first_addr(wallet_event, ("source_wallet", "wallet", "wallet_address", "proxyWallet", "proxy_wallet"))
        or _first_addr(copy_efficiency, ("source_wallet",))
        or _first_addr(wallet_api, ("requested_wallet", "raw_proxy_wallet"))
        or _first_addr(raw, ("proxyWallet", "proxy_wallet", "wallet"))
        or _first_addr(row, ("source_wallet", "wallet", "wallet_address", "proxyWallet", "proxy_wallet"))
    )


def _wallet_label(row: dict[str, Any]) -> str:
    return str(
        row.get("name")
        or row.get("wallet_name")
        or row.get("label")
        or row.get("id")
        or row.get("user_name")
        or ""
    )


def _parse_generated_at(payload: dict[str, Any]) -> float | None:
    value = payload.get("generated_at") or payload.get("updated_at")
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _summarize_numbers(summary: dict[str, Any] | None) -> dict[str, Any]:
    summary = summary if isinstance(summary, dict) else {}
    return {
        "orders": int(num(summary.get("orders"), 0)),
        "resolved_orders": int(num(summary.get("resolved_orders"), 0)),
        "wins": int(num(summary.get("wins"), 0)),
        "losses": int(num(summary.get("losses"), 0)),
        "pnl_usd": round(num(summary.get("pnl_usd")), 6),
        "roi_pct": round(num(summary.get("roi_pct")), 6),
        "wr_pct": round(num(summary.get("wr_pct")), 6),
        "cost_usd": round(num(summary.get("cost_usd")), 6),
        "unresolved_ratio": round(num(summary.get("unresolved_ratio")), 6),
        "unique_windows": int(num(summary.get("unique_windows"), 0)),
    }


def _pct(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(max(0.0, float(numerator)) / float(denominator) * 100.0, 6)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * max(0.0, min(100.0, float(pct))) / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return round(ordered[lower] * (1.0 - weight) + ordered[upper] * weight, 6)


def _status_count_total(counts: dict[str, Any], statuses: set[str]) -> int:
    return sum(int(num(count)) for status, count in counts.items() if str(status) in statuses)


def _paper_metric_status(row: dict[str, Any]) -> str:
    paper_orders = int(num(row.get("paper_orders"), 0))
    resolved = int(num(row.get("paper_resolved_orders"), 0))
    unresolved = int(num(row.get("paper_unresolved_orders"), 0))
    history_events = int(num(row.get("history_events"), 0))
    tracking_events = (
        int(num(row.get("canonical_tracking_source_events"), 0))
        + int(num(row.get("active_tracking_source_events"), 0))
        + int(num(row.get("registry_sweep_tracking_source_events"), 0))
    )
    if paper_orders <= 0:
        return "MISSING"
    if resolved <= 0:
        return "PARTIAL_UNRESOLVED"
    if history_events <= 0 and tracking_events <= 0:
        return "PARTIAL_NO_SOURCE_HISTORY"
    if unresolved / max(1, paper_orders) > 0.5:
        return "PARTIAL_UNRESOLVED_HEAVY"
    if resolved < 100:
        return "PARTIAL_SMALL_SAMPLE"
    return "COMPLETE"


def _profit_metric_pair(row: dict[str, Any], key: str) -> tuple[float | None, str]:
    profit = row.get("profit_replay") if isinstance(row.get("profit_replay"), dict) else {}
    for basis in ("validation", "replay", "raw_baseline"):
        summary = profit.get(basis) if isinstance(profit.get(basis), dict) else {}
        value = summary.get(key)
        resolved = int(num(summary.get("resolved_orders"), 0))
        unique_windows = int(num(summary.get("unique_windows"), 0))
        if isinstance(value, (int, float)) and (resolved > 0 or unique_windows > 0 or float(value) != 0.0):
            return round(float(value), 6), basis
    return None, "missing"


def _best_leaderboard_pnl(row: dict[str, Any]) -> tuple[float, str]:
    pnl_by_period = row.get("leaderboard_pnl_by_period")
    if not isinstance(pnl_by_period, dict):
        return 0.0, "missing"
    best_period = ""
    best_pnl = 0.0
    for period, value in pnl_by_period.items():
        pnl = num(value)
        if pnl > best_pnl:
            best_period = str(period)
            best_pnl = pnl
    return round(best_pnl, 6), best_period or "missing"


def _top_counter_item(counts: Any) -> tuple[str | None, int]:
    if not isinstance(counts, dict) or not counts:
        return None, 0
    key, value = sorted(((str(k), int(num(v))) for k, v in counts.items()), key=lambda item: item[1], reverse=True)[0]
    return key, value


def _copy_edge_loss_reasons(
    *,
    source_high: bool,
    current_tracking_seen: bool,
    active_buy_events: int,
    active_policy_copy_coverage_pct: float | None,
    active_all_order_reject_rate_pct: float | None,
    dominant_copyability_reason: str | None,
    dominant_reject_reason: str | None,
    dominant_reject_blocking_reason: str | None,
    paper_resolved: int,
    paper_profitable: bool,
    paper_high_quality: bool,
    active_exact_copy_clean: bool,
    policy_copy_coverage_clean: bool,
) -> list[str]:
    """Explain where profitable source-wallet edge is lost in our copy path."""

    if not source_high:
        return ["source_edge_not_high_confidence"]

    reasons: list[str] = []
    if not current_tracking_seen:
        reasons.append("no_current_active_buy_tracking")
    if active_buy_events > 0 and not policy_copy_coverage_clean:
        reasons.append("policy_copy_coverage_below_target")
    if str(dominant_copyability_reason or "") in {"event_age_above_cap", "fetch_duration_above_cap"}:
        reasons.append("freshness_latency_above_copy_cap")
    if str(dominant_copyability_reason or "") in {"best_ask_above_slippage_cap", "no_ask_liquidity"}:
        reasons.append("copyability_slippage_or_liquidity")
    if (
        str(dominant_reject_reason or "") in {"clob_price_above_slippage_cap", "clob_no_ask_liquidity"}
        or str(dominant_reject_blocking_reason or "") in {
            "price_above_slippage_cap",
            "no_ask_liquidity",
            "insufficient_depth_within_slippage_cap",
        }
        or (active_all_order_reject_rate_pct or 0.0) > TARGET_ACTIVE_ALL_ORDER_REJECT_RATE_MAX_PCT
    ):
        reasons.append("all_order_slippage_or_liquidity_rejects")
    if not active_exact_copy_clean:
        reasons.append("all_order_exact_copy_not_clean")
    if paper_resolved <= 0:
        reasons.append("paper_resolution_missing_or_unresolved")
    elif paper_resolved < TARGET_PAPER_MIN_RESOLVED_ORDERS:
        reasons.append("paper_resolved_sample_below_target")
    elif not paper_profitable:
        reasons.append("copied_paper_not_profitable")
    elif not paper_high_quality:
        reasons.append("copied_paper_quality_below_target")

    ordered: list[str] = []
    for reason in reasons:
        if reason not in ordered:
            ordered.append(reason)
    return ordered or ["no_edge_loss_detected"]


def build_source_vs_paper_copy_quality(row: dict[str, Any]) -> dict[str, Any]:
    """Classify whether a profitable source wallet is transferring to our paper copy."""

    source_roi, source_roi_basis = _profit_metric_pair(row, "roi_pct")
    source_wr, source_wr_basis = _profit_metric_pair(row, "wr_pct")
    leaderboard_pnl, leaderboard_period = _best_leaderboard_pnl(row)
    paper_orders = int(num(row.get("paper_orders"), 0))
    paper_resolved = int(num(row.get("paper_resolved_orders"), 0))
    paper_roi = num(row.get("paper_roi_pct"), 0.0)
    paper_wr = num(row.get("paper_wr_pct"), 0.0)
    paper_pnl = num(row.get("paper_total_realized_plus_resolved_pnl_usd"), row.get("paper_pnl_usd", 0.0))
    tactic_replay_orders = int(num(row.get("active_tactic_replay_paper_orders"), 0))
    tactic_replay_resolved = int(num(row.get("active_tactic_replay_paper_resolved_orders"), 0))
    tactic_replay_roi = num(row.get("active_tactic_replay_paper_roi_pct"), 0.0)
    tactic_replay_wr = num(row.get("active_tactic_replay_paper_wr_pct"), 0.0)
    tactic_replay_pnl = num(row.get("active_tactic_replay_paper_pnl_usd"), 0.0)
    tactic_pnl_attr = row.get("active_tactic_replay_pnl_attribution") if isinstance(row.get("active_tactic_replay_pnl_attribution"), dict) else {}
    tactic_pnl_status = str(tactic_pnl_attr.get("status") or "")

    active_buy_events = int(num(row.get("active_tracking_buy_events"), 0))
    active_copied_buy_events = int(num(row.get("active_tracking_copied_buy_events"), 0))
    active_all_order_copied = int(num(row.get("active_tracking_all_order_copied_buy_events"), 0))
    active_all_order_rejected = int(num(row.get("active_tracking_all_order_rejected_buy_events"), 0))
    active_all_order_copyability_rejected = int(
        num(row.get("active_tracking_all_order_copyability_rejected_buy_events"), 0)
    )
    registry_sweep_buy_events = int(num(row.get("registry_sweep_tracking_buy_events"), 0))
    registry_sweep_all_order_copied = int(num(row.get("registry_sweep_tracking_all_order_copied_buy_events"), 0))
    registry_sweep_all_order_rejected = int(num(row.get("registry_sweep_tracking_all_order_rejected_buy_events"), 0))

    active_policy_copy_coverage_pct = _pct(active_copied_buy_events, active_buy_events)
    active_all_order_attempts = active_all_order_copied + active_all_order_rejected
    active_all_order_fill_rate_pct = _pct(active_all_order_copied, active_all_order_attempts)
    active_all_order_reject_rate_pct = _pct(active_all_order_rejected, active_all_order_attempts)
    registry_sweep_all_order_attempts = registry_sweep_all_order_copied + registry_sweep_all_order_rejected
    registry_sweep_all_order_fill_rate_pct = _pct(registry_sweep_all_order_copied, registry_sweep_all_order_attempts)
    registry_sweep_all_order_reject_rate_pct = _pct(
        registry_sweep_all_order_rejected,
        registry_sweep_all_order_attempts,
    )
    dominant_copyability_reason, dominant_copyability_count = _top_counter_item(row.get("active_copyability_reason_counts"))
    dominant_reject_reason, dominant_reject_count = _top_counter_item(row.get("active_all_order_reject_reason_counts"))
    dominant_reject_blocking_reason, dominant_reject_blocking_count = _top_counter_item(
        row.get("active_all_order_reject_blocking_reason_counts")
    )

    source_high = (
        (source_roi is not None and source_roi >= TARGET_SOURCE_HIGH_ROI_PCT)
        or (source_wr is not None and source_wr >= TARGET_SOURCE_HIGH_WR_PCT)
        or leaderboard_pnl >= TARGET_SOURCE_HIGH_LEADERBOARD_PNL_USD
    )
    paper_profitable = paper_pnl > 0.0 and paper_roi > 0.0
    paper_high_quality = (
        paper_resolved >= TARGET_PAPER_MIN_RESOLVED_ORDERS
        and paper_pnl > 0.0
        and paper_roi >= TARGET_PAPER_MIN_ROI_PCT
        and paper_wr >= TARGET_PAPER_MIN_WR_PCT
    )
    tactic_replay_profitable = (
        tactic_replay_resolved >= TARGET_PAPER_MIN_RESOLVED_ORDERS
        and tactic_replay_pnl > 0.0
        and tactic_replay_roi >= TARGET_PAPER_MIN_ROI_PCT
        and tactic_replay_wr >= TARGET_PAPER_MIN_WR_PCT
    )
    current_tracking_seen = active_buy_events > 0
    active_exact_copy_clean = (
        active_all_order_attempts > 0
        and active_all_order_rejected == 0
        and active_all_order_copyability_rejected == 0
        and (active_all_order_fill_rate_pct or 0.0) >= TARGET_ACTIVE_ALL_ORDER_FILL_RATE_PCT
    )
    policy_copy_coverage_clean = (
        active_buy_events > 0
        and active_copied_buy_events > 0
        and (active_policy_copy_coverage_pct or 0.0) >= TARGET_ACTIVE_POLICY_COPY_COVERAGE_PCT
    )

    blockers: list[str] = []
    if source_high and paper_orders <= 0:
        blockers.append("source_high_but_no_paper_orders")
    if source_high and 0 < paper_resolved < TARGET_PAPER_MIN_RESOLVED_ORDERS:
        blockers.append("source_high_but_paper_sample_below_100_resolved")
    if source_high and paper_orders > 0 and paper_roi < TARGET_PAPER_MIN_ROI_PCT:
        blockers.append("source_high_but_paper_roi_below_5pct")
    if source_high and paper_orders > 0 and paper_wr < TARGET_PAPER_MIN_WR_PCT:
        blockers.append("source_high_but_paper_wr_below_70pct")
    if source_high and paper_orders > 0 and paper_pnl <= 0.0:
        blockers.append("source_high_but_paper_pnl_not_positive")
    if source_high and active_buy_events <= 0:
        blockers.append("source_high_but_no_current_active_buy_tracking")
    if (
        source_high
        and active_buy_events > 0
        and (active_policy_copy_coverage_pct or 0.0) < TARGET_ACTIVE_POLICY_COPY_COVERAGE_PCT
    ):
        blockers.append("policy_copy_coverage_below_95pct")
    if source_high and active_all_order_attempts <= 0:
        blockers.append("all_order_exact_copy_not_measured_currently")
    if source_high and active_all_order_rejected > 0:
        blockers.append("all_order_exact_copy_has_rejected_buys")
    if source_high and (active_all_order_reject_rate_pct or 0.0) > TARGET_ACTIVE_ALL_ORDER_REJECT_RATE_MAX_PCT:
        blockers.append("all_order_exact_copy_reject_rate_above_5pct")
    if source_high and tactic_replay_profitable and active_all_order_rejected > 0:
        blockers.append("tactic_replay_profitable_but_not_ordinary_exact_copy_truth")
    if source_high and tactic_replay_orders > 0 and tactic_pnl_status != "PASS":
        blockers.append("tactic_replay_pnl_attribution_missing")

    if source_high and (
        "source_high_but_no_paper_orders" in blockers
        or "source_high_but_paper_sample_below_100_resolved" in blockers
        or "source_high_but_no_current_active_buy_tracking" in blockers
        or "all_order_exact_copy_not_measured_currently" in blockers
    ):
        status = "ANALYZE"
    elif source_high and blockers:
        status = "CORRECTION"
    elif source_high and paper_high_quality and active_exact_copy_clean and policy_copy_coverage_clean:
        status = "PASS"
    elif source_high and paper_profitable:
        status = "WATCH"
        blockers.append("paper_profitable_but_copy_quality_not_live_ready")
    elif source_high:
        status = "CORRECTION"
    elif paper_high_quality:
        status = "WATCH"
        blockers.append("paper_positive_but_source_profit_proxy_not_high")
    else:
        status = "ANALYZE"
        blockers.append("insufficient_source_or_paper_edge")

    paper_minus_source_roi = round(paper_roi - source_roi, 6) if source_roi is not None else None
    paper_minus_source_wr = round(paper_wr - source_wr, 6) if source_wr is not None else None
    live_candidate_score = (
        (source_roi or 0.0) * 2.0
        + (source_wr or 0.0)
        + max(0.0, paper_roi) * 2.0
        + max(0.0, paper_wr)
        + max(0.0, paper_pnl) / 10.0
        + (active_policy_copy_coverage_pct or 0.0)
        + (active_all_order_fill_rate_pct or 0.0)
        - (active_all_order_reject_rate_pct or 0.0) * 3.0
    )
    if status == "PASS":
        live_candidate_score += 500.0
    elif status == "CORRECTION":
        live_candidate_score -= 200.0
    elif status == "ANALYZE":
        live_candidate_score -= 50.0
    edge_loss_reasons = _copy_edge_loss_reasons(
        source_high=source_high,
        current_tracking_seen=current_tracking_seen,
        active_buy_events=active_buy_events,
        active_policy_copy_coverage_pct=active_policy_copy_coverage_pct,
        active_all_order_reject_rate_pct=active_all_order_reject_rate_pct,
        dominant_copyability_reason=dominant_copyability_reason,
        dominant_reject_reason=dominant_reject_reason,
        dominant_reject_blocking_reason=dominant_reject_blocking_reason,
        paper_resolved=paper_resolved,
        paper_profitable=paper_profitable,
        paper_high_quality=paper_high_quality,
        active_exact_copy_clean=active_exact_copy_clean,
        policy_copy_coverage_clean=policy_copy_coverage_clean,
    )

    return {
        "status": status,
        "blockers": blockers,
        "copy_edge_loss_primary_reason": edge_loss_reasons[0],
        "copy_edge_loss_reasons": edge_loss_reasons,
        "source_high_confidence": source_high,
        "source_proxy_roi_pct": source_roi,
        "source_proxy_roi_basis": source_roi_basis,
        "source_proxy_wr_pct": source_wr,
        "source_proxy_wr_basis": source_wr_basis,
        "source_leaderboard_best_pnl_usd": leaderboard_pnl,
        "source_leaderboard_best_period": leaderboard_period,
        "paper_profitable": paper_profitable,
        "paper_high_quality": paper_high_quality,
        "paper_orders": paper_orders,
        "paper_resolved_orders": paper_resolved,
        "paper_roi_pct": round(paper_roi, 6),
        "paper_wr_pct": round(paper_wr, 6),
        "paper_pnl_usd": round(paper_pnl, 6),
        "active_tactic_replay_paper_profitable": tactic_replay_profitable,
        "active_tactic_replay_paper_orders": tactic_replay_orders,
        "active_tactic_replay_paper_resolved_orders": tactic_replay_resolved,
        "active_tactic_replay_paper_roi_pct": round(tactic_replay_roi, 6),
        "active_tactic_replay_paper_wr_pct": round(tactic_replay_wr, 6),
        "active_tactic_replay_paper_pnl_usd": round(tactic_replay_pnl, 6),
        "active_tactic_replay_pnl_attribution_status": tactic_pnl_status or None,
        "active_tactic_replay_pnl_attribution_blockers": tactic_pnl_attr.get("blockers") or [],
        "active_tactic_replay_pnl_delta_vs_strict_usd": tactic_pnl_attr.get("pnl_delta_vs_strict_usd"),
        "active_tactic_replay_cost_delta_usd": tactic_pnl_attr.get("cost_delta_usd"),
        "active_tactic_replay_p95_effective_price_delta_bps": tactic_pnl_attr.get("p95_effective_price_delta_bps"),
        "paper_minus_source_roi_pct": paper_minus_source_roi,
        "paper_minus_source_wr_pct": paper_minus_source_wr,
        "current_tracking_seen": current_tracking_seen,
        "active_buy_events": active_buy_events,
        "active_policy_copied_buy_events": active_copied_buy_events,
        "active_policy_copy_coverage_pct": active_policy_copy_coverage_pct,
        "active_all_order_copied_buy_events": active_all_order_copied,
        "active_all_order_rejected_buy_events": active_all_order_rejected,
        "active_all_order_copyability_rejected_buy_events": active_all_order_copyability_rejected,
        "active_all_order_fill_rate_pct": active_all_order_fill_rate_pct,
        "active_all_order_reject_rate_pct": active_all_order_reject_rate_pct,
        "registry_sweep_buy_events": registry_sweep_buy_events,
        "registry_sweep_all_order_copied_buy_events": registry_sweep_all_order_copied,
        "registry_sweep_all_order_rejected_buy_events": registry_sweep_all_order_rejected,
        "registry_sweep_all_order_fill_rate_pct": registry_sweep_all_order_fill_rate_pct,
        "registry_sweep_all_order_reject_rate_pct": registry_sweep_all_order_reject_rate_pct,
        "dominant_active_copyability_reason": dominant_copyability_reason,
        "dominant_active_copyability_reason_count": dominant_copyability_count,
        "dominant_active_all_order_reject_reason": dominant_reject_reason,
        "dominant_active_all_order_reject_reason_count": dominant_reject_count,
        "dominant_active_all_order_reject_blocking_reason": dominant_reject_blocking_reason,
        "dominant_active_all_order_reject_blocking_reason_count": dominant_reject_blocking_count,
        "active_all_order_reject_required_slippage_bps_summary": row.get(
            "active_all_order_reject_required_slippage_bps_summary"
        )
        if isinstance(row.get("active_all_order_reject_required_slippage_bps_summary"), dict)
        else {},
        "active_paper_tactic_profile_pass_counts": row.get("active_paper_tactic_profile_pass_counts")
        if isinstance(row.get("active_paper_tactic_profile_pass_counts"), dict)
        else {},
        "live_candidate_score": round(live_candidate_score, 6),
    }


def _candidate_rank_key(candidate: dict[str, Any]) -> tuple[int, float, int, float]:
    status_rank = 1 if candidate.get("status") == "PASS" else 0
    validation = candidate.get("validation_summary") if isinstance(candidate.get("validation_summary"), dict) else {}
    summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
    return (
        status_rank,
        num(validation.get("roi_pct"), num(summary.get("roi_pct"))),
        int(num(validation.get("resolved_orders"), num(summary.get("resolved_orders")))),
        num(candidate.get("profit_score")),
    )


def _load_jsonl_tracking(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    by_wallet: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "source_events": 0,
            "buy_events": 0,
            "copied_buy_events": 0,
            "policy_copied_buy_events": 0,
            "all_order_copied_buy_events": 0,
            "all_order_rejected_buy_events": 0,
            "all_order_copyability_rejected_buy_events": 0,
            "filtered_events": 0,
            "rejected_buy_copy_events": 0,
            "fallback_filled_buy_copy_events": 0,
            "clob_filled_buy_copy_events": 0,
            "tx_hashes": set(),
            "onchain_status_counts": Counter(),
            "copy_action_counts": Counter(),
            "copyability_reason_counts": Counter(),
            "all_order_reject_reason_counts": Counter(),
            "all_order_reject_stage_counts": Counter(),
            "all_order_reject_blocking_reason_counts": Counter(),
            "all_order_reject_blocker_counts": Counter(),
            "all_order_reject_required_slippage_bps": [],
            "paper_tactic_profile_pass_counts": Counter(),
            "latest_event_ts": 0.0,
            "fresh_le_10s": 0,
            "fresh_le_30s": 0,
        }
    )
    status_counts: Counter[str] = Counter()
    rows = 0
    if not p.exists():
        return {"path": str(p), "rows": 0, "wallets": {}, "onchain_status_counts": {}}
    with p.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            rows += 1
            wallet = _wallet_from_nested(row)
            if not wallet:
                continue
            entry = by_wallet[wallet]
            entry["source_events"] += 1
            wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
            action = str(row.get("action") or wallet_event.get("action") or "").upper()
            copy_action = str(row.get("copy_action") or (row.get("mirror_result") or {}).get("copy_action") or "")
            entry["copy_action_counts"][copy_action] += 1
            copy_eff = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
            copyability_reason = str(copy_eff.get("copyability_reason") or copy_eff.get("missed_copy_reason") or "")
            if copyability_reason:
                entry["copyability_reason_counts"][copyability_reason] += 1
            if action == "BUY":
                entry["buy_events"] += 1
            if copy_action in {"COPY_BUY_TO_PAPER", "COPY_LIFECYCLE_TO_PAPER"}:
                if action == "BUY":
                    entry["copied_buy_events"] += 1
                    entry["policy_copied_buy_events"] += 1
            if copy_action == "COPY_BUY_REJECTED_BY_FILL_MODEL":
                entry["rejected_buy_copy_events"] += 1
            if copy_action == "FILTERED_NO_COPY":
                entry["filtered_events"] += 1
            all_order = row.get("all_order_exact_copy") if isinstance(row.get("all_order_exact_copy"), dict) else {}
            all_order_mirror_status = str(all_order.get("mirror_status") or "")
            all_order_copy_action = str(all_order.get("copy_action") or "")
            if action == "BUY" and all_order_mirror_status == "MIRRORED_BUY_TO_PAPER":
                entry["all_order_copied_buy_events"] += 1
            if action == "BUY" and (
                all_order_mirror_status
                in {
                    "MIRRORED_BUY_TO_PAPER_REJECTED_BY_FILL_MODEL",
                    "COPY_BUY_REJECTED_BY_FILL_MODEL",
                    "REJECTED",
                }
                or all_order_copy_action == "COPY_BUY_REJECTED_BY_FILL_MODEL"
            ):
                entry["all_order_rejected_buy_events"] += 1
                fill_estimate = all_order.get("fill_estimate") if isinstance(all_order.get("fill_estimate"), dict) else {}
                reject_reason = str(fill_estimate.get("reject_reason") or all_order.get("reason") or "unknown")
                reject_stage = str(fill_estimate.get("reject_stage") or "unknown")
                reject_details = (
                    fill_estimate.get("reject_details")
                    if isinstance(fill_estimate.get("reject_details"), dict)
                    else {}
                )
                blocking_reason = str(reject_details.get("blocking_reason") or "unknown")
                if reject_reason:
                    entry["all_order_reject_reason_counts"][reject_reason] += 1
                if reject_stage:
                    entry["all_order_reject_stage_counts"][reject_stage] += 1
                if blocking_reason:
                    entry["all_order_reject_blocking_reason_counts"][blocking_reason] += 1
                for blocker in fill_estimate.get("blockers") or []:
                    if blocker:
                        entry["all_order_reject_blocker_counts"][str(blocker)] += 1
                details = copy_eff.get("copyability_details") if isinstance(copy_eff.get("copyability_details"), dict) else {}
                min_slippage = details.get("min_slippage_to_fill_bps")
                if isinstance(min_slippage, (int, float)):
                    entry["all_order_reject_required_slippage_bps"].append(float(min_slippage))
            if action == "BUY" and all_order_mirror_status == "FILTERED_BY_COPYABILITY_POLICY":
                entry["all_order_copyability_rejected_buy_events"] += 1
            tracking = row.get("tracking_evidence") if isinstance(row.get("tracking_evidence"), dict) else {}
            tactic_profiles = (
                tracking.get("paper_tactic_fillability")
                if isinstance(tracking.get("paper_tactic_fillability"), dict)
                else {}
            )
            if action == "BUY":
                for profile_id, profile in tactic_profiles.items():
                    if not isinstance(profile, dict):
                        continue
                    profile_status = str(profile.get("status") or profile.get("instant_fill_status") or "")
                    if profile_status == "PASS" or profile.get("instant_fill_status") == "PASS":
                        entry["paper_tactic_profile_pass_counts"][str(profile_id)] += 1
            mirror = row.get("mirror_result") if isinstance(row.get("mirror_result"), dict) else {}
            fill_source = str(mirror.get("fill_source") or "")
            if fill_source == "source_price_plus_slippage_fallback":
                entry["fallback_filled_buy_copy_events"] += 1
            if fill_source == "clob_book_evidence":
                entry["clob_filled_buy_copy_events"] += 1
            tx_hash = str(
                row.get("tx_hash")
                or wallet_event.get("transaction_hash")
                or (wallet_event.get("raw") or {}).get("transactionHash")
                or ""
            ).lower()
            if tx_hash.startswith("0x"):
                entry["tx_hashes"].add(tx_hash)
            onchain = tracking.get("onchain") if isinstance(tracking.get("onchain"), dict) else {}
            onchain_status = str(onchain.get("status") or "MISSING")
            entry["onchain_status_counts"][onchain_status] += 1
            status_counts[onchain_status] += 1
            event_ts = num(row.get("event_ts") or wallet_event.get("event_ts"), 0)
            entry["latest_event_ts"] = max(num(entry["latest_event_ts"]), event_ts)
            lag = None
            if isinstance(copy_eff.get("api_latency_s"), (int, float)):
                lag = num(copy_eff.get("api_latency_s"))
            elif isinstance(wallet_event.get("api_latency_s"), (int, float)):
                lag = num(wallet_event.get("api_latency_s"))
            if lag is not None:
                if lag <= 10:
                    entry["fresh_le_10s"] += 1
                if lag <= 30:
                    entry["fresh_le_30s"] += 1
    wallets: dict[str, Any] = {}
    for wallet, entry in by_wallet.items():
        slippage_values = [float(value) for value in entry["all_order_reject_required_slippage_bps"]]
        wallets[wallet] = {
            **{
                k: v
                for k, v in entry.items()
                if k
                not in {
                    "tx_hashes",
                    "onchain_status_counts",
                    "copy_action_counts",
                    "copyability_reason_counts",
                    "all_order_reject_reason_counts",
                    "all_order_reject_stage_counts",
                    "all_order_reject_blocking_reason_counts",
                    "all_order_reject_blocker_counts",
                    "all_order_reject_required_slippage_bps",
                    "paper_tactic_profile_pass_counts",
                }
            },
            "tx_hash_count": len(entry["tx_hashes"]),
            "onchain_status_counts": dict(entry["onchain_status_counts"]),
            "copy_action_counts": dict(entry["copy_action_counts"]),
            "copyability_reason_counts": dict(entry["copyability_reason_counts"]),
            "all_order_reject_reason_counts": dict(entry["all_order_reject_reason_counts"]),
            "all_order_reject_stage_counts": dict(entry["all_order_reject_stage_counts"]),
            "all_order_reject_blocking_reason_counts": dict(entry["all_order_reject_blocking_reason_counts"]),
            "all_order_reject_blocker_counts": dict(entry["all_order_reject_blocker_counts"]),
            "all_order_reject_required_slippage_bps_summary": {
                "count": len(slippage_values),
                "min": round(min(slippage_values), 6) if slippage_values else None,
                "p50": _percentile(slippage_values, 50.0),
                "p95": _percentile(slippage_values, 95.0),
                "max": round(max(slippage_values), 6) if slippage_values else None,
            },
            "paper_tactic_profile_pass_counts": dict(entry["paper_tactic_profile_pass_counts"]),
        }
    return {"path": str(p), "rows": rows, "wallets": wallets, "onchain_status_counts": dict(status_counts)}


def _history_by_wallet(history: dict[str, Any]) -> dict[str, Any]:
    generated_ts = _parse_generated_at(history) or datetime.now(tz=timezone.utc).timestamp()
    out: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "history_state_registered": False,
            "history_events": 0,
            "history_event_rows_attributed": 0,
            "history_buys": 0,
            "history_buy_count_basis": "events",
            "history_sells": 0,
            "history_lifecycle_events": 0,
            "history_copy_intents": 0,
            "latest_event_ts": 0.0,
            "latest_event_lag_s": None,
        }
    )
    for row in history.get("wallet_results") or []:
        if not isinstance(row, dict):
            continue
        wallet_meta = row.get("wallet") if isinstance(row.get("wallet"), dict) else {}
        wallet = _first_addr(wallet_meta, ("address", "wallet", "wallet_address"))
        if not wallet:
            continue
        entry = out[wallet]
        entry["history_state_registered"] = True
        entry["history_events"] = int(num(row.get("events"), 0))
        entry["history_copy_intents"] = int(num(row.get("copy_intents"), 0))
        entry["latest_event_ts"] = max(num(entry.get("latest_event_ts"), 0), num(row.get("latest_event_ts"), 0))
        if not entry.get("wallet_name"):
            entry["wallet_name"] = _wallet_label(wallet_meta)
    for wallet_meta in history.get("wallets") or []:
        if not isinstance(wallet_meta, dict):
            continue
        wallet = _first_addr(wallet_meta, ("address", "wallet", "wallet_address"))
        if not wallet:
            continue
        entry = out[wallet]
        entry["history_state_registered"] = True
        if not entry.get("wallet_name"):
            entry["wallet_name"] = _wallet_label(wallet_meta)
    for row in history.get("events") or []:
        if not isinstance(row, dict):
            continue
        wallet = _first_addr(row, ("source_wallet", "wallet", "wallet_address")) or _first_addr(
            row.get("raw") if isinstance(row.get("raw"), dict) else {},
            ("proxyWallet", "wallet"),
        )
        if not wallet:
            continue
        entry = out[wallet]
        entry["history_event_rows_attributed"] += 1
        if not entry.get("history_state_registered"):
            entry["history_events"] += 1
        action = str(row.get("action") or "").upper()
        if action == "BUY":
            entry["history_buys"] += 1
        elif action == "SELL":
            entry["history_sells"] += 1
        elif action in {"REDEEM", "MERGE", "SPLIT"}:
            entry["history_lifecycle_events"] += 1
        entry["latest_event_ts"] = max(num(entry["latest_event_ts"]), num(row.get("event_ts"), 0))
    for row in history.get("copy_intents") or []:
        if not isinstance(row, dict):
            continue
        wallet = _first_addr(row, ("source_wallet", "wallet", "wallet_address"))
        if wallet:
            if not out[wallet].get("history_state_registered"):
                out[wallet]["history_copy_intents"] += 1
    for wallet, entry in out.items():
        if int(num(entry.get("history_buys"), 0)) <= 0 and int(num(entry.get("history_copy_intents"), 0)) > 0:
            entry["history_buys"] = int(num(entry.get("history_copy_intents"), 0))
            entry["history_buy_count_basis"] = "wallet_results_copy_intents_proxy"
        latest = num(entry.get("latest_event_ts"), 0)
        entry["latest_event_lag_s"] = round(generated_ts - latest, 6) if latest > 0 else None
    return dict(out)


def _leaderboard_by_wallet(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for row in payload.get("candidate_wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _first_addr(row, ("address", "proxy_wallet", "wallet"))
        if not wallet:
            continue
        out[wallet] = {
            "leaderboard_name": row.get("user_name") or row.get("name"),
            "leaderboard_ranks": row.get("ranks") if isinstance(row.get("ranks"), dict) else {},
            "leaderboard_pnl_by_period": row.get("pnl_by_period") if isinstance(row.get("pnl_by_period"), dict) else {},
            "leaderboard_volume_by_period": row.get("vol_by_period") if isinstance(row.get("vol_by_period"), dict) else {},
        }
    return out


def _profit_by_wallet(payload: dict[str, Any]) -> dict[str, Any]:
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in payload.get("ranked_candidates") or []:
        if not isinstance(candidate, dict):
            continue
        metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
        wallet = _addr(metadata.get("source_wallet"))
        if wallet:
            candidates[wallet].append(candidate)
    out: dict[str, Any] = {}
    for wallet, rows in candidates.items():
        best = sorted(rows, key=_candidate_rank_key, reverse=True)[0]
        policy = best.get("policy") if isinstance(best.get("policy"), dict) else {}
        metadata = best.get("metadata") if isinstance(best.get("metadata"), dict) else {}
        out[wallet] = {
            "profit_candidate_count": len(rows),
            "best_candidate_id": best.get("candidate_id"),
            "best_candidate_status": best.get("status"),
            "best_candidate_blockers": best.get("blockers") or [],
            "best_policy_id": policy.get("policy_id"),
            "wallet_name": metadata.get("wallet_name"),
            "replay": _summarize_numbers(best.get("summary") if isinstance(best.get("summary"), dict) else {}),
            "validation": _summarize_numbers(
                best.get("validation_summary") if isinstance(best.get("validation_summary"), dict) else {}
            ),
            "raw_baseline": _summarize_numbers(
                best.get("raw_baseline_summary") if isinstance(best.get("raw_baseline_summary"), dict) else {}
            ),
            "fill_evidence_summary": best.get("fill_evidence_summary")
            if isinstance(best.get("fill_evidence_summary"), dict)
            else {},
            "resolution_evidence_summary": best.get("resolution_evidence_summary")
            if isinstance(best.get("resolution_evidence_summary"), dict)
            else {},
        }
    return out


def _paper_by_wallet(paper_state: dict[str, Any], resolutions: str) -> dict[str, Any]:
    if paper_state.get("_wallet_report_large_state_stub"):
        out: dict[str, Any] = {}
        wallets = paper_state.get("wallets") if isinstance(paper_state.get("wallets"), dict) else {}
        for wallet_raw, card in wallets.items():
            if not isinstance(card, dict):
                continue
            wallet = _addr(wallet_raw)
            if not wallet:
                wallet = _first_addr(card, ("source_wallet", "wallet", "wallet_address", "proxyWallet", "proxy_wallet"))
            if not wallet:
                continue
            orders = int(num(card.get("orders") or card.get("paper_orders"), 0))
            resolved = int(num(card.get("resolved_orders") or card.get("paper_resolved_orders"), 0))
            unresolved = int(
                num(
                    card.get("unresolved_orders")
                    or card.get("paper_unresolved_orders")
                    or max(0, orders - resolved),
                    0,
                )
            )
            pnl = round(num(card.get("pnl_usd") or card.get("paper_pnl_usd")), 6)
            cost = round(num(card.get("cost_usd") or card.get("filled_size_usd") or card.get("paper_cost_usd")), 6)
            roi = round(num(card.get("roi_pct") or card.get("paper_roi_pct")), 6)
            wr = round(num(card.get("wr_pct") or card.get("paper_wr_pct")), 6)
            out[wallet] = {
                "paper_orders": orders,
                "paper_resolved_orders": resolved,
                "paper_unresolved_orders": unresolved,
                "paper_wins": int(num(card.get("wins") or card.get("paper_wins"), 0)),
                "paper_losses": int(num(card.get("losses") or card.get("paper_losses"), 0)),
                "paper_pnl_usd": pnl,
                "paper_expiry_pnl_usd": pnl,
                "paper_roi_pct": roi,
                "paper_wr_pct": wr,
                "paper_cost_usd": cost,
                "paper_lifecycle_realized_events": int(num(card.get("lifecycle_events"), 0)),
                "paper_lifecycle_realized_pnl_usd": 0.0,
                "paper_lifecycle_cost_removed_usd": 0.0,
                "paper_total_realized_plus_resolved_pnl_usd": pnl,
                "paper_total_realized_plus_resolved_roi_pct": roi,
                "paper_avg_api_latency_s": card.get("avg_api_latency_s"),
                "paper_max_api_latency_s": card.get("max_api_latency_s"),
                "paper_metrics_source": "bounded_large_state_wallet_aggregate",
                "paper_metrics_complete": resolved > 0 and ("pnl_usd" in card or "paper_pnl_usd" in card),
            }
        return out

    report = score_paper_state(paper_state, resolutions_path=resolutions)
    lifecycle_by_wallet: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "paper_lifecycle_realized_events": 0,
            "paper_lifecycle_proceeds_usd": 0.0,
            "paper_lifecycle_cost_removed_usd": 0.0,
            "paper_lifecycle_realized_pnl_usd": 0.0,
        }
    )
    for event in paper_state.get("lifecycle_events") or []:
        if not isinstance(event, dict):
            continue
        wallet = _addr(event.get("source_wallet"))
        reduction = event.get("position_reduction") if isinstance(event.get("position_reduction"), dict) else {}
        if not wallet or "realized_pnl_usd" not in reduction:
            continue
        entry = lifecycle_by_wallet[wallet]
        entry["paper_lifecycle_realized_events"] += 1
        entry["paper_lifecycle_proceeds_usd"] += num(reduction.get("proceeds_usd"))
        entry["paper_lifecycle_cost_removed_usd"] += num(reduction.get("cost_removed_usd"))
        entry["paper_lifecycle_realized_pnl_usd"] += num(reduction.get("realized_pnl_usd"))
    out: dict[str, Any] = {}
    for card in report.get("wallet_scorecards") or []:
        if not isinstance(card, dict):
            continue
        wallet = _addr(card.get("source_wallet"))
        if not wallet:
            continue
        lifecycle = lifecycle_by_wallet.get(wallet, {})
        lifecycle_pnl = round(num(lifecycle.get("paper_lifecycle_realized_pnl_usd")), 6)
        lifecycle_cost = round(num(lifecycle.get("paper_lifecycle_cost_removed_usd")), 6)
        expiry_pnl = round(num(card.get("pnl_usd")), 6)
        expiry_cost = round(num(card.get("cost_usd")), 6)
        total_pnl = round(expiry_pnl + lifecycle_pnl, 6)
        total_cost = round(expiry_cost + lifecycle_cost, 6)
        out[wallet] = {
            "paper_orders": int(num(card.get("orders"), 0)),
            "paper_resolved_orders": int(num(card.get("resolved_orders"), 0)),
            "paper_unresolved_orders": int(num(card.get("unresolved_orders"), 0)),
            "paper_wins": int(num(card.get("wins"), 0)),
            "paper_losses": int(num(card.get("losses"), 0)),
            "paper_pnl_usd": expiry_pnl,
            "paper_expiry_pnl_usd": expiry_pnl,
            "paper_roi_pct": round(num(card.get("roi_pct")), 6),
            "paper_wr_pct": round(num(card.get("wr_pct")), 6),
            "paper_cost_usd": expiry_cost,
            "paper_lifecycle_realized_events": int(num(lifecycle.get("paper_lifecycle_realized_events"), 0)),
            "paper_lifecycle_realized_pnl_usd": lifecycle_pnl,
            "paper_lifecycle_cost_removed_usd": lifecycle_cost,
            "paper_total_realized_plus_resolved_pnl_usd": total_pnl,
            "paper_total_realized_plus_resolved_roi_pct": round(total_pnl / total_cost * 100.0, 6)
            if total_cost > 0
            else 0.0,
            "paper_avg_api_latency_s": card.get("avg_api_latency_s"),
            "paper_max_api_latency_s": card.get("max_api_latency_s"),
        }
    for wallet, lifecycle in lifecycle_by_wallet.items():
        if wallet in out:
            continue
        lifecycle_pnl = round(num(lifecycle.get("paper_lifecycle_realized_pnl_usd")), 6)
        lifecycle_cost = round(num(lifecycle.get("paper_lifecycle_cost_removed_usd")), 6)
        out[wallet] = {
            "paper_orders": 0,
            "paper_resolved_orders": 0,
            "paper_unresolved_orders": 0,
            "paper_wins": 0,
            "paper_losses": 0,
            "paper_pnl_usd": 0.0,
            "paper_expiry_pnl_usd": 0.0,
            "paper_roi_pct": 0.0,
            "paper_wr_pct": 0.0,
            "paper_cost_usd": 0.0,
            "paper_lifecycle_realized_events": int(num(lifecycle.get("paper_lifecycle_realized_events"), 0)),
            "paper_lifecycle_realized_pnl_usd": lifecycle_pnl,
            "paper_lifecycle_cost_removed_usd": lifecycle_cost,
            "paper_total_realized_plus_resolved_pnl_usd": lifecycle_pnl,
            "paper_total_realized_plus_resolved_roi_pct": round(lifecycle_pnl / lifecycle_cost * 100.0, 6)
            if lifecycle_cost > 0
            else 0.0,
            "paper_avg_api_latency_s": None,
            "paper_max_api_latency_s": None,
        }
    return out


def _orders_by_wallet(paper_state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if paper_state.get("_wallet_report_large_state_stub"):
        return by_wallet
    for row in paper_state.get("orders") or []:
        if not isinstance(row, dict):
            continue
        wallet = _addr(row.get("source_wallet"))
        if not wallet:
            source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            wallet = _addr(source_intent.get("source_wallet"))
        if wallet:
            by_wallet[wallet].append(row)
    return by_wallet


def _tactic_pnl_attribution_by_wallet(
    strict_state: dict[str, Any],
    tactic_state: dict[str, Any],
    resolutions_path: str,
) -> dict[str, dict[str, Any]]:
    if strict_state.get("_wallet_report_large_state_stub") or tactic_state.get("_wallet_report_large_state_stub"):
        strict_wallets = strict_state.get("wallets") if isinstance(strict_state.get("wallets"), dict) else {}
        tactic_wallets = tactic_state.get("wallets") if isinstance(tactic_state.get("wallets"), dict) else {}
        wallets = {_addr(wallet) for wallet in set(strict_wallets) | set(tactic_wallets)}
        wallets.discard("")
        out: dict[str, dict[str, Any]] = {}
        for wallet in sorted(wallets):
            strict_card = strict_wallets.get(wallet) if isinstance(strict_wallets.get(wallet), dict) else {}
            tactic_card = tactic_wallets.get(wallet) if isinstance(tactic_wallets.get(wallet), dict) else {}
            out[wallet] = {
                "status": "ANALYZE",
                "blockers": ["large_state_bounded_tactic_pnl_attribution"],
                "strict_orders": int(num(strict_card.get("orders"), 0)),
                "tactic_orders": int(num(tactic_card.get("orders"), 0)),
                "bounded_large_state": True,
            }
        return out

    resolutions = load_resolutions(resolutions_path)
    strict_by_wallet = _orders_by_wallet(strict_state)
    tactic_by_wallet = _orders_by_wallet(tactic_state)
    return {
        wallet: score_tactic_replay_pnl(
            strict_by_wallet.get(wallet, []),
            tactic_by_wallet.get(wallet, []),
            resolutions,
        )
        for wallet in sorted(set(strict_by_wallet) | set(tactic_by_wallet))
    }


def _active_status(row: dict[str, Any]) -> str:
    if row.get("active_hotlane"):
        return "ACTIVE_HOTLANE"
    if num(row.get("active_tracking_source_events"), 0) > 0:
        return "ACTIVE_TRACKED_HISTORICAL_LOG"
    if num(row.get("registry_sweep_tracking_source_events"), 0) > 0:
        return "REGISTRY_SWEEP_TRACKED"
    if num(row.get("canonical_tracking_source_events"), 0) > 0:
        return "CANONICAL_TRACKED_HISTORICAL_LOG"
    lag = row.get("history_latest_event_lag_s")
    if isinstance(lag, (int, float)) and lag <= 3600:
        return "HISTORY_ACTIVE_1H"
    if isinstance(lag, (int, float)) and lag <= 86400:
        return "HISTORY_ACTIVE_24H"
    if num(row.get("history_events"), 0) > 0:
        return "HISTORY_STALE_OR_NOT_CURRENTLY_POLLED"
    return "NO_HISTORY_INGESTED"


def _compact_wallet_row(row: dict[str, Any]) -> dict[str, Any]:
    copy_quality = row.get("source_vs_paper_copy_quality") if isinstance(row.get("source_vs_paper_copy_quality"), dict) else {}
    return {
        "wallet": row.get("wallet"),
        "name": row.get("wallet_name"),
        "active_status": row.get("active_status"),
        "copy_quality_status": copy_quality.get("status"),
        "copy_quality_blockers": copy_quality.get("blockers", []),
        "copy_quality_score": copy_quality.get("live_candidate_score"),
        "copy_edge_loss_primary_reason": copy_quality.get("copy_edge_loss_primary_reason"),
        "copy_edge_loss_reasons": copy_quality.get("copy_edge_loss_reasons", []),
        "source_high_confidence": copy_quality.get("source_high_confidence"),
        "source_proxy_roi_pct": copy_quality.get("source_proxy_roi_pct"),
        "source_proxy_wr_pct": copy_quality.get("source_proxy_wr_pct"),
        "source_leaderboard_best_pnl_usd": copy_quality.get("source_leaderboard_best_pnl_usd"),
        "paper_minus_source_roi_pct": copy_quality.get("paper_minus_source_roi_pct"),
        "paper_minus_source_wr_pct": copy_quality.get("paper_minus_source_wr_pct"),
        "active_policy_copy_coverage_pct": copy_quality.get("active_policy_copy_coverage_pct"),
        "active_all_order_fill_rate_pct": copy_quality.get("active_all_order_fill_rate_pct"),
        "active_all_order_reject_rate_pct": copy_quality.get("active_all_order_reject_rate_pct"),
        "dominant_active_copyability_reason": copy_quality.get("dominant_active_copyability_reason"),
        "dominant_active_all_order_reject_reason": copy_quality.get("dominant_active_all_order_reject_reason"),
        "dominant_active_all_order_reject_blocking_reason": copy_quality.get(
            "dominant_active_all_order_reject_blocking_reason"
        ),
        "active_all_order_reject_required_slippage_bps_summary": copy_quality.get(
            "active_all_order_reject_required_slippage_bps_summary"
        ),
        "active_paper_tactic_profile_pass_counts": copy_quality.get("active_paper_tactic_profile_pass_counts"),
        "history_events": row.get("history_events", 0),
        "history_buys": row.get("history_buys", 0),
        "history_buy_count_basis": row.get("history_buy_count_basis"),
        "metric_status": row.get("metric_status"),
        "paper_orders": row.get("paper_orders", 0),
        "paper_resolved_orders": row.get("paper_resolved_orders", 0),
        "paper_unresolved_orders": row.get("paper_unresolved_orders", 0),
        "paper_resolved_ratio_pct": row.get("paper_resolved_ratio_pct"),
        "history_to_paper_order_coverage_pct": row.get("history_to_paper_order_coverage_pct"),
        "paper_roi_pct": row.get("paper_roi_pct"),
        "paper_wr_pct": row.get("paper_wr_pct"),
        "paper_pnl_usd": row.get("paper_pnl_usd"),
        "paper_lifecycle_realized_pnl_usd": row.get("paper_lifecycle_realized_pnl_usd"),
        "paper_total_realized_plus_resolved_pnl_usd": row.get(
            "paper_total_realized_plus_resolved_pnl_usd"
        ),
        "active_tactic_replay_paper_profitable": copy_quality.get("active_tactic_replay_paper_profitable"),
        "active_tactic_replay_paper_orders": copy_quality.get("active_tactic_replay_paper_orders"),
        "active_tactic_replay_paper_resolved_orders": copy_quality.get(
            "active_tactic_replay_paper_resolved_orders"
        ),
        "active_tactic_replay_paper_roi_pct": copy_quality.get("active_tactic_replay_paper_roi_pct"),
        "active_tactic_replay_paper_wr_pct": copy_quality.get("active_tactic_replay_paper_wr_pct"),
        "active_tactic_replay_paper_pnl_usd": copy_quality.get("active_tactic_replay_paper_pnl_usd"),
        "replay_status": (row.get("profit_replay") or {}).get("best_candidate_status"),
        "replay_roi_pct": (row.get("profit_replay") or {}).get("replay", {}).get("roi_pct"),
        "replay_wr_pct": (row.get("profit_replay") or {}).get("replay", {}).get("wr_pct"),
        "replay_pnl_usd": (row.get("profit_replay") or {}).get("replay", {}).get("pnl_usd"),
        "validation_roi_pct": (row.get("profit_replay") or {}).get("validation", {}).get("roi_pct"),
        "validation_wr_pct": (row.get("profit_replay") or {}).get("validation", {}).get("wr_pct"),
        "leaderboard_pnl_by_period": row.get("leaderboard_pnl_by_period"),
        "active_tracking_buy_events": row.get("active_tracking_buy_events"),
        "active_tracking_copied_buy_events": row.get("active_tracking_copied_buy_events"),
        "active_tracking_all_order_copied_buy_events": row.get("active_tracking_all_order_copied_buy_events"),
        "active_tracking_all_order_rejected_buy_events": row.get("active_tracking_all_order_rejected_buy_events"),
        "active_tracking_all_order_copyability_rejected_buy_events": row.get(
            "active_tracking_all_order_copyability_rejected_buy_events"
        ),
        "active_copyability_reason_counts": row.get("active_copyability_reason_counts"),
        "active_all_order_reject_reason_counts": row.get("active_all_order_reject_reason_counts"),
        "active_all_order_reject_blocking_reason_counts": row.get("active_all_order_reject_blocking_reason_counts"),
        "active_all_order_reject_required_slippage_bps_summary": row.get(
            "active_all_order_reject_required_slippage_bps_summary"
        ),
        "active_paper_tactic_profile_pass_counts": row.get("active_paper_tactic_profile_pass_counts"),
        "onchain_confirmed_tx_count": row.get("onchain_confirmed_tx_count"),
        "onchain_error_count": row.get("onchain_error_count"),
        "active_onchain_status_counts": row.get("active_onchain_status_counts"),
        "registry_sweep_tracking_buy_events": row.get("registry_sweep_tracking_buy_events"),
        "registry_sweep_tracking_all_order_copied_buy_events": row.get(
            "registry_sweep_tracking_all_order_copied_buy_events"
        ),
        "registry_sweep_tracking_all_order_rejected_buy_events": row.get(
            "registry_sweep_tracking_all_order_rejected_buy_events"
        ),
        "registry_sweep_all_order_reject_reason_counts": row.get("registry_sweep_all_order_reject_reason_counts"),
        "registry_sweep_all_order_reject_required_slippage_bps_summary": row.get(
            "registry_sweep_all_order_reject_required_slippage_bps_summary"
        ),
        "registry_sweep_paper_tactic_profile_pass_counts": row.get("registry_sweep_paper_tactic_profile_pass_counts"),
        "registry_sweep_paper_roi_pct": row.get("registry_sweep_paper_roi_pct"),
        "registry_sweep_paper_pnl_usd": row.get("registry_sweep_paper_pnl_usd"),
        "registry_sweep_onchain_status_counts": row.get("registry_sweep_onchain_status_counts"),
    }


def _copy_surface_metrics(row: dict[str, Any], prefix: str, *, label: str) -> dict[str, Any]:
    buy_events = int(num(row.get(f"{prefix}_tracking_buy_events"), 0))
    copied = int(num(row.get(f"{prefix}_tracking_all_order_copied_buy_events"), 0))
    rejected = int(num(row.get(f"{prefix}_tracking_all_order_rejected_buy_events"), 0))
    attempted = copied + rejected
    denominator = buy_events if buy_events > 0 else attempted
    return {
        "surface": label,
        "buy_events": buy_events,
        "copied_buy_events": copied,
        "rejected_buy_events": rejected,
        "attempted_buy_events": attempted,
        "fill_rate_pct": _pct(copied, denominator),
        "reject_rate_pct": _pct(rejected, denominator),
    }


def _copyable_surface_rank_key(surface: dict[str, Any]) -> tuple[float, int, int]:
    return (
        num(surface.get("fill_rate_pct"), -1.0),
        int(num(surface.get("copied_buy_events"), 0)),
        int(num(surface.get("buy_events"), 0)),
    )


def _copyable_wallet_row(row: dict[str, Any]) -> dict[str, Any]:
    copy_quality = row.get("source_vs_paper_copy_quality") if isinstance(row.get("source_vs_paper_copy_quality"), dict) else {}
    surfaces = [
        _copy_surface_metrics(row, "active", label="active_hotlane_or_active_forward"),
        _copy_surface_metrics(row, "registry_sweep", label="registry_sweep_rotation"),
        _copy_surface_metrics(row, "canonical", label="canonical_tracking"),
    ]
    observed_surfaces = [surface for surface in surfaces if int(num(surface.get("buy_events"), 0)) > 0]
    best_surface = max(observed_surfaces, key=_copyable_surface_rank_key) if observed_surfaces else {}
    active_surface = surfaces[0]
    registry_surface = surfaces[1]
    canonical_surface = surfaces[2]
    source_pnl = copy_quality.get("source_leaderboard_best_pnl_usd")
    if source_pnl is None:
        leaderboard_pnl = row.get("leaderboard_pnl_by_period") if isinstance(row.get("leaderboard_pnl_by_period"), dict) else {}
        source_pnl = max((num(value) for value in leaderboard_pnl.values()), default=0.0)
    return {
        "wallet": row.get("wallet"),
        "name": row.get("wallet_name"),
        "active_status": row.get("active_status"),
        "best_surface": best_surface.get("surface"),
        "best_surface_buy_events": best_surface.get("buy_events", 0),
        "best_surface_copied_buy_events": best_surface.get("copied_buy_events", 0),
        "best_surface_rejected_buy_events": best_surface.get("rejected_buy_events", 0),
        "best_surface_fill_rate_pct": best_surface.get("fill_rate_pct"),
        "active_buy_events": active_surface.get("buy_events", 0),
        "active_copied_buy_events": active_surface.get("copied_buy_events", 0),
        "active_rejected_buy_events": active_surface.get("rejected_buy_events", 0),
        "active_fill_rate_pct": active_surface.get("fill_rate_pct"),
        "active_fresh_le_10s": int(num(row.get("active_tracking_fresh_le_10s"), 0)),
        "active_fresh_le_30s": int(num(row.get("active_tracking_fresh_le_30s"), 0)),
        "registry_sweep_buy_events": registry_surface.get("buy_events", 0),
        "registry_sweep_copied_buy_events": registry_surface.get("copied_buy_events", 0),
        "registry_sweep_rejected_buy_events": registry_surface.get("rejected_buy_events", 0),
        "registry_sweep_fill_rate_pct": registry_surface.get("fill_rate_pct"),
        "canonical_buy_events": canonical_surface.get("buy_events", 0),
        "canonical_copied_buy_events": canonical_surface.get("copied_buy_events", 0),
        "canonical_rejected_buy_events": canonical_surface.get("rejected_buy_events", 0),
        "canonical_fill_rate_pct": canonical_surface.get("fill_rate_pct"),
        "history_buys": int(num(row.get("history_buys"), 0)),
        "paper_orders": int(num(row.get("paper_orders"), 0)),
        "paper_resolved_orders": int(num(row.get("paper_resolved_orders"), 0)),
        "paper_roi_pct": row.get("paper_roi_pct"),
        "paper_wr_pct": row.get("paper_wr_pct"),
        "source_high_confidence": bool(copy_quality.get("source_high_confidence")),
        "source_leaderboard_best_pnl_usd": round(num(source_pnl), 6),
        "copy_quality_status": copy_quality.get("status"),
        "copy_edge_loss_primary_reason": copy_quality.get("copy_edge_loss_primary_reason"),
        "dominant_active_copyability_reason": copy_quality.get("dominant_active_copyability_reason"),
        "dominant_active_all_order_reject_reason": copy_quality.get("dominant_active_all_order_reject_reason"),
    }


def _copyable_wallet_rank_key(row: dict[str, Any], *, current_only: bool = False) -> tuple[int, float, int, int, float]:
    card = _copyable_wallet_row(row)
    if current_only:
        return (
            1 if int(num(card.get("active_buy_events"), 0)) > 0 else 0,
            num(card.get("active_fill_rate_pct"), -1.0),
            int(num(card.get("active_copied_buy_events"), 0)),
            int(num(card.get("active_buy_events"), 0)),
            num(card.get("source_leaderboard_best_pnl_usd"), 0.0),
        )
    return (
        1 if int(num(card.get("best_surface_buy_events"), 0)) > 0 else 0,
        num(card.get("best_surface_fill_rate_pct"), -1.0),
        int(num(card.get("best_surface_copied_buy_events"), 0)),
        int(num(card.get("best_surface_buy_events"), 0)),
        num(card.get("source_leaderboard_best_pnl_usd"), 0.0),
    )


def main() -> int:
    args = parse_args()
    registry = _load_report_state(args.registry)
    active_registry = _load_report_state(args.active_hotlane_registry)
    history_state = _load_report_state(args.history_state)
    profit_state = _load_report_state(args.profit_state)
    paper_state = _load_report_state(args.paper_state)
    leaderboard_state = _load_report_state(args.leaderboard_state)
    canonical_tracking_state = _load_report_state(args.canonical_tracking_state)
    active_tracking_state = _load_report_state(args.active_tracking_state)
    registry_sweep_state = _load_report_state(args.registry_sweep_state)

    registry_wallets: dict[str, dict[str, Any]] = {}
    for row in registry.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _first_addr(row, ("address", "wallet", "wallet_address", "proxyWallet", "proxy_wallet"))
        if not wallet:
            continue
        registry_wallets[wallet] = {
            "wallet": wallet,
            "wallet_name": _wallet_label(row),
            "registry_enabled": row.get("enabled", True) is not False,
            "registry_tags": row.get("tags") if isinstance(row.get("tags"), list) else [],
        }
    active_wallets = {
        _first_addr(row, ("address", "wallet", "wallet_address", "proxyWallet", "proxy_wallet"))
        for row in active_registry.get("wallets") or []
        if isinstance(row, dict)
    }
    active_wallets.discard("")

    history = _history_by_wallet(history_state)
    profit = _profit_by_wallet(profit_state)
    paper = _paper_by_wallet(paper_state, args.resolutions)
    registry_sweep_paper_state = _load_report_state(args.registry_sweep_paper_state)
    registry_sweep_paper = _paper_by_wallet(registry_sweep_paper_state, args.resolutions)
    active_all_order_exact_copy_paper_state = _load_report_state(args.active_all_order_exact_copy_paper_state)
    active_all_order_tactic_replay_paper_state = _load_report_state(args.active_all_order_tactic_replay_paper_state)
    active_tactic_replay_paper = _paper_by_wallet(active_all_order_tactic_replay_paper_state, args.resolutions)
    active_tactic_replay_pnl_attribution_by_wallet = _tactic_pnl_attribution_by_wallet(
        active_all_order_exact_copy_paper_state,
        active_all_order_tactic_replay_paper_state,
        args.resolutions,
    )
    leaderboard = _leaderboard_by_wallet(leaderboard_state)
    canonical_tracking = _load_jsonl_tracking(args.canonical_event_log)
    active_tracking = _load_jsonl_tracking(args.active_event_log)
    registry_sweep_tracking = _load_jsonl_tracking(args.registry_sweep_event_log)

    all_wallets = (
        set(registry_wallets)
        | set(history)
        | set(profit)
        | set(paper)
        | set(registry_sweep_paper)
        | set(active_tactic_replay_paper)
        | set(leaderboard)
    )
    rows: list[dict[str, Any]] = []
    for wallet in sorted(all_wallets):
        row = {
            "wallet": wallet,
            "wallet_name": (
                registry_wallets.get(wallet, {}).get("wallet_name")
                or profit.get(wallet, {}).get("wallet_name")
                or leaderboard.get(wallet, {}).get("leaderboard_name")
                or ""
            ),
            "registry_enabled": registry_wallets.get(wallet, {}).get("registry_enabled", False),
            "active_hotlane": wallet in active_wallets,
            **history.get(wallet, {}),
            **leaderboard.get(wallet, {}),
            "profit_replay": profit.get(wallet, {}),
            **paper.get(wallet, {}),
        }
        canonical = canonical_tracking.get("wallets", {}).get(wallet, {})
        active = active_tracking.get("wallets", {}).get(wallet, {})
        registry_sweep = registry_sweep_tracking.get("wallets", {}).get(wallet, {})
        sweep_paper = registry_sweep_paper.get(wallet, {})
        tactic_paper = active_tactic_replay_paper.get(wallet, {})
        tactic_pnl_attribution = active_tactic_replay_pnl_attribution_by_wallet.get(wallet, {})
        row.update(
            {
                "canonical_tracking_source_events": int(num(canonical.get("source_events"), 0)),
                "canonical_tracking_buy_events": int(num(canonical.get("buy_events"), 0)),
                "canonical_tracking_copied_buy_events": int(num(canonical.get("copied_buy_events"), 0)),
                "canonical_tracking_policy_copied_buy_events": int(num(canonical.get("policy_copied_buy_events"), 0)),
                "canonical_tracking_all_order_copied_buy_events": int(num(canonical.get("all_order_copied_buy_events"), 0)),
                "canonical_tracking_all_order_rejected_buy_events": int(num(canonical.get("all_order_rejected_buy_events"), 0)),
                "canonical_tracking_all_order_copyability_rejected_buy_events": int(
                    num(canonical.get("all_order_copyability_rejected_buy_events"), 0)
                ),
                "canonical_onchain_status_counts": canonical.get("onchain_status_counts", {}),
                "canonical_tx_hash_count": int(num(canonical.get("tx_hash_count"), 0)),
                "active_tracking_source_events": int(num(active.get("source_events"), 0)),
                "active_tracking_buy_events": int(num(active.get("buy_events"), 0)),
                "active_tracking_copied_buy_events": int(num(active.get("copied_buy_events"), 0)),
                "active_tracking_policy_copied_buy_events": int(num(active.get("policy_copied_buy_events"), 0)),
                "active_tracking_all_order_copied_buy_events": int(num(active.get("all_order_copied_buy_events"), 0)),
                "active_tracking_all_order_rejected_buy_events": int(num(active.get("all_order_rejected_buy_events"), 0)),
                "active_tracking_all_order_copyability_rejected_buy_events": int(
                    num(active.get("all_order_copyability_rejected_buy_events"), 0)
                ),
                "active_tracking_fresh_le_10s": int(num(active.get("fresh_le_10s"), 0)),
                "active_tracking_fresh_le_30s": int(num(active.get("fresh_le_30s"), 0)),
                "active_onchain_status_counts": active.get("onchain_status_counts", {}),
                "active_copyability_reason_counts": active.get("copyability_reason_counts", {}),
                "active_all_order_reject_reason_counts": active.get("all_order_reject_reason_counts", {}),
                "active_all_order_reject_stage_counts": active.get("all_order_reject_stage_counts", {}),
                "active_all_order_reject_blocking_reason_counts": active.get(
                    "all_order_reject_blocking_reason_counts", {}
                ),
                "active_all_order_reject_blocker_counts": active.get("all_order_reject_blocker_counts", {}),
                "active_all_order_reject_required_slippage_bps_summary": active.get(
                    "all_order_reject_required_slippage_bps_summary", {}
                ),
                "active_paper_tactic_profile_pass_counts": active.get("paper_tactic_profile_pass_counts", {}),
                "active_tx_hash_count": int(num(active.get("tx_hash_count"), 0)),
                "registry_sweep_tracking_source_events": int(num(registry_sweep.get("source_events"), 0)),
                "registry_sweep_tracking_buy_events": int(num(registry_sweep.get("buy_events"), 0)),
                "registry_sweep_tracking_policy_copied_buy_events": int(
                    num(registry_sweep.get("policy_copied_buy_events"), 0)
                ),
                "registry_sweep_tracking_all_order_copied_buy_events": int(
                    num(registry_sweep.get("all_order_copied_buy_events"), 0)
                ),
                "registry_sweep_tracking_all_order_rejected_buy_events": int(
                    num(registry_sweep.get("all_order_rejected_buy_events"), 0)
                ),
                "registry_sweep_tracking_all_order_copyability_rejected_buy_events": int(
                    num(registry_sweep.get("all_order_copyability_rejected_buy_events"), 0)
                ),
                "registry_sweep_onchain_status_counts": registry_sweep.get("onchain_status_counts", {}),
                "registry_sweep_copyability_reason_counts": registry_sweep.get("copyability_reason_counts", {}),
                "registry_sweep_all_order_reject_reason_counts": registry_sweep.get("all_order_reject_reason_counts", {}),
                "registry_sweep_all_order_reject_required_slippage_bps_summary": registry_sweep.get(
                    "all_order_reject_required_slippage_bps_summary", {}
                ),
                "registry_sweep_paper_tactic_profile_pass_counts": registry_sweep.get(
                    "paper_tactic_profile_pass_counts", {}
                ),
                "registry_sweep_tx_hash_count": int(num(registry_sweep.get("tx_hash_count"), 0)),
                "registry_sweep_paper_orders": int(num(sweep_paper.get("paper_orders"), 0)),
                "registry_sweep_paper_resolved_orders": int(num(sweep_paper.get("paper_resolved_orders"), 0)),
                "registry_sweep_paper_pnl_usd": sweep_paper.get("paper_pnl_usd"),
                "registry_sweep_paper_roi_pct": sweep_paper.get("paper_roi_pct"),
                "registry_sweep_paper_wr_pct": sweep_paper.get("paper_wr_pct"),
                "active_tactic_replay_paper_orders": int(num(tactic_paper.get("paper_orders"), 0)),
                "active_tactic_replay_paper_resolved_orders": int(
                    num(tactic_paper.get("paper_resolved_orders"), 0)
                ),
                "active_tactic_replay_paper_pnl_usd": tactic_paper.get("paper_pnl_usd"),
                "active_tactic_replay_paper_roi_pct": tactic_paper.get("paper_roi_pct"),
                "active_tactic_replay_paper_wr_pct": tactic_paper.get("paper_wr_pct"),
                "active_tactic_replay_pnl_attribution": tactic_pnl_attribution,
            }
        )
        paper_orders = int(num(row.get("paper_orders"), 0))
        paper_resolved = int(num(row.get("paper_resolved_orders"), 0))
        paper_unresolved = int(num(row.get("paper_unresolved_orders"), 0))
        history_events = int(num(row.get("history_events"), 0))
        history_buys = int(num(row.get("history_buys"), 0))
        canonical_onchain = row.get("canonical_onchain_status_counts") if isinstance(row.get("canonical_onchain_status_counts"), dict) else {}
        active_onchain = row.get("active_onchain_status_counts") if isinstance(row.get("active_onchain_status_counts"), dict) else {}
        sweep_onchain = (
            row.get("registry_sweep_onchain_status_counts")
            if isinstance(row.get("registry_sweep_onchain_status_counts"), dict)
            else {}
        )
        all_onchain_counts = Counter()
        all_onchain_counts.update({str(k): int(num(v)) for k, v in canonical_onchain.items()})
        all_onchain_counts.update({str(k): int(num(v)) for k, v in active_onchain.items()})
        all_onchain_counts.update({str(k): int(num(v)) for k, v in sweep_onchain.items()})
        row["paper_resolved_ratio_pct"] = _pct(paper_resolved, paper_orders)
        row["paper_unresolved_ratio_pct"] = _pct(paper_unresolved, paper_orders)
        row["history_to_paper_order_coverage_pct"] = _pct(paper_orders, history_buys or history_events)
        row["paper_to_history_buy_coverage_pct"] = row["history_to_paper_order_coverage_pct"]
        row["history_coverage_status"] = "PRESENT" if history_events > 0 else "MISSING"
        row["paper_coverage_status"] = "PRESENT" if paper_orders > 0 else "MISSING"
        row["onchain_confirmed_tx_count"] = _status_count_total(dict(all_onchain_counts), {"CONFIRMED"})
        row["onchain_error_count"] = _status_count_total(dict(all_onchain_counts), {"ERROR"})
        row["onchain_enabled_count"] = _status_count_total(
            dict(all_onchain_counts),
            set(all_onchain_counts) - {"SKIPPED", "MISSING", ""},
        )
        row["metric_status"] = _paper_metric_status(row)
        row["active_status"] = _active_status(row)
        row["source_vs_paper_copy_quality"] = build_source_vs_paper_copy_quality(row)
        rows.append(row)

    rows.sort(
        key=lambda row: (
            num((row.get("source_vs_paper_copy_quality") or {}).get("live_candidate_score"), 0.0),
            row.get("active_hotlane") is True,
            num((row.get("profit_replay") or {}).get("validation", {}).get("roi_pct")),
            num(row.get("paper_resolved_orders"), 0),
            num(row.get("history_buys"), 0),
        ),
        reverse=True,
    )

    status_counts = Counter(str(row.get("active_status") or "") for row in rows)
    metric_status_counts = Counter(str(row.get("metric_status") or "") for row in rows)
    copy_quality_status_counts = Counter(
        str((row.get("source_vs_paper_copy_quality") or {}).get("status") or "") for row in rows
    )
    copy_edge_loss_reason_counts = Counter(
        str(reason)
        for row in rows
        for reason in ((row.get("source_vs_paper_copy_quality") or {}).get("copy_edge_loss_reasons") or [])
        if reason
    )
    copy_edge_loss_primary_reason_counts = Counter(
        str((row.get("source_vs_paper_copy_quality") or {}).get("copy_edge_loss_primary_reason") or "")
        for row in rows
        if (row.get("source_vs_paper_copy_quality") or {}).get("copy_edge_loss_primary_reason")
    )
    high_source_wallets = [
        row for row in rows if bool((row.get("source_vs_paper_copy_quality") or {}).get("source_high_confidence"))
    ]
    copy_quality_pass_wallets = [
        row for row in high_source_wallets if (row.get("source_vs_paper_copy_quality") or {}).get("status") == "PASS"
    ]
    copy_quality_correction_wallets = [
        row
        for row in high_source_wallets
        if (row.get("source_vs_paper_copy_quality") or {}).get("status") == "CORRECTION"
    ]
    top_live_copy_candidates = [
        row
        for row in rows
        if (row.get("source_vs_paper_copy_quality") or {}).get("status") in {"PASS", "WATCH"}
        and bool((row.get("source_vs_paper_copy_quality") or {}).get("source_high_confidence"))
    ][:10]
    rows_with_copy_surface = [
        row
        for row in rows
        if (
            int(num(row.get("active_tracking_buy_events"), 0)) > 0
            or int(num(row.get("registry_sweep_tracking_buy_events"), 0)) > 0
            or int(num(row.get("canonical_tracking_buy_events"), 0)) > 0
        )
    ]
    top_copyable_wallets = sorted(
        rows_with_copy_surface,
        key=lambda row: _copyable_wallet_rank_key(row),
        reverse=True,
    )[:20]
    top_current_copyable_wallets = sorted(
        [row for row in rows if int(num(row.get("active_tracking_buy_events"), 0)) > 0],
        key=lambda row: _copyable_wallet_rank_key(row, current_only=True),
        reverse=True,
    )[:20]
    top_current_copyable_wallets_large_sample = sorted(
        [row for row in rows if int(num(row.get("active_tracking_buy_events"), 0)) >= 50],
        key=lambda row: _copyable_wallet_rank_key(row, current_only=True),
        reverse=True,
    )[:20]
    source_high_rows = [
        row for row in rows if bool((row.get("source_vs_paper_copy_quality") or {}).get("source_high_confidence"))
    ]
    top_source_current_copyable_wallets = sorted(
        [row for row in source_high_rows if int(num(row.get("active_tracking_buy_events"), 0)) > 0],
        key=lambda row: _copyable_wallet_rank_key(row, current_only=True),
        reverse=True,
    )[:20]
    top_source_current_copyable_wallets_large_sample = sorted(
        [row for row in source_high_rows if int(num(row.get("active_tracking_buy_events"), 0)) >= 50],
        key=lambda row: _copyable_wallet_rank_key(row, current_only=True),
        reverse=True,
    )[:20]
    no_history_state = [row["wallet"] for row in rows if row.get("history_state_registered") is not True]
    no_history_events = [row["wallet"] for row in rows if int(num(row.get("history_events"), 0)) <= 0]
    no_paper = [row["wallet"] for row in rows if int(num(row.get("paper_orders"), 0)) <= 0]
    incomplete_metrics = [row["wallet"] for row in rows if str(row.get("metric_status") or "") != "COMPLETE"]
    onchain_enabled_rows = 0
    onchain_confirmed_rows = 0
    for tracking in (canonical_tracking, active_tracking):
        for status, count in (tracking.get("onchain_status_counts") or {}).items():
            if status not in {"SKIPPED", "MISSING"}:
                onchain_enabled_rows += int(count)
            if status == "CONFIRMED":
                onchain_confirmed_rows += int(count)
    registry_sweep_onchain_enabled_rows = 0
    registry_sweep_onchain_confirmed_rows = 0
    for status, count in (registry_sweep_tracking.get("onchain_status_counts") or {}).items():
        if status not in {"SKIPPED", "MISSING"}:
            registry_sweep_onchain_enabled_rows += int(count)
        if status == "CONFIRMED":
            registry_sweep_onchain_confirmed_rows += int(count)
    registry_sweep_summary = (
        registry_sweep_state.get("summary") if isinstance(registry_sweep_state.get("summary"), dict) else {}
    )
    registry_sweep_scope = (
        registry_sweep_summary.get("tracker_scope")
        if isinstance(registry_sweep_summary.get("tracker_scope"), dict)
        else {}
    )
    active_tracking_summary = (
        active_tracking_state.get("summary") if isinstance(active_tracking_state.get("summary"), dict) else {}
    )
    active_all_order = (
        active_tracking_summary.get("all_order_exact_copy")
        if isinstance(active_tracking_summary.get("all_order_exact_copy"), dict)
        else {}
    )
    registry_sweep_all_order = (
        registry_sweep_summary.get("all_order_exact_copy")
        if isinstance(registry_sweep_summary.get("all_order_exact_copy"), dict)
        else {}
    )
    registry_sweep_enabled_wallets = int(
        num(registry_sweep_scope.get("registry_enabled_wallets"), len(registry_wallets))
    )
    registry_sweep_wallets_per_slice = int(
        num(registry_sweep_scope.get("max_wallets"), len(registry_sweep_scope.get("tracked_wallets") or []))
    )
    registry_sweep_slices_needed = (
        (registry_sweep_enabled_wallets + max(1, registry_sweep_wallets_per_slice) - 1)
        // max(1, registry_sweep_wallets_per_slice)
    )

    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_wallet_analysis_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "inputs": {
            "registry": args.registry,
            "active_hotlane_registry": args.active_hotlane_registry,
            "history_state": args.history_state,
            "profit_state": args.profit_state,
            "paper_state": args.paper_state,
            "resolutions": args.resolutions,
            "leaderboard_state": args.leaderboard_state,
            "canonical_tracking_state": args.canonical_tracking_state,
            "active_tracking_state": args.active_tracking_state,
            "canonical_event_log": args.canonical_event_log,
            "active_event_log": args.active_event_log,
            "registry_sweep_state": args.registry_sweep_state,
            "registry_sweep_event_log": args.registry_sweep_event_log,
            "registry_sweep_paper_state": args.registry_sweep_paper_state,
            "active_all_order_tactic_replay_paper_state": args.active_all_order_tactic_replay_paper_state,
            "state_loads": {
                "history_state": _state_load_metadata(args.history_state, history_state),
                "paper_state": _state_load_metadata(args.paper_state, paper_state),
                "registry_sweep_paper_state": _state_load_metadata(
                    args.registry_sweep_paper_state,
                    registry_sweep_paper_state,
                ),
                "active_all_order_exact_copy_paper_state": _state_load_metadata(
                    args.active_all_order_exact_copy_paper_state,
                    active_all_order_exact_copy_paper_state,
                ),
                "active_all_order_tactic_replay_paper_state": _state_load_metadata(
                    args.active_all_order_tactic_replay_paper_state,
                    active_all_order_tactic_replay_paper_state,
                ),
            },
        },
        "summary": {
            "registry_wallets": len(registry_wallets),
            "registry_enabled_wallets": sum(1 for row in registry_wallets.values() if row.get("registry_enabled")),
            "active_hotlane_wallets": len(active_wallets),
            "history_state_wallets": sum(1 for row in rows if row.get("history_state_registered") is True),
            "history_event_wallets": sum(1 for row in rows if int(num(row.get("history_events"), 0)) > 0),
            "history_event_attributed_wallets": sum(
                1 for row in rows if int(num(row.get("history_event_rows_attributed"), 0)) > 0
            ),
            "wallets_without_history_state": len(no_history_state),
            "wallets_without_history_events": len(no_history_events),
            "profit_wallets": len(profit),
            "paper_wallets": len(paper),
            "wallets_without_paper_orders": len(no_paper),
            "leaderboard_wallets": len(leaderboard),
            "canonical_tracking_wallets": len(canonical_tracking.get("wallets", {})),
            "active_tracking_wallets": len(active_tracking.get("wallets", {})),
            "canonical_tracking_rows": canonical_tracking.get("rows", 0),
            "active_tracking_rows": active_tracking.get("rows", 0),
            "registry_sweep_tracking_wallets": len(registry_sweep_tracking.get("wallets", {})),
            "registry_sweep_tracking_rows": registry_sweep_tracking.get("rows", 0),
            "registry_sweep_paper_wallets": len(registry_sweep_paper),
            "active_tactic_replay_paper_wallets": len(active_tactic_replay_paper),
            "active_tactic_replay_profitable_wallets": sum(
                1
                for row in rows
                if bool((row.get("source_vs_paper_copy_quality") or {}).get("active_tactic_replay_paper_profitable"))
            ),
            "registry_sweep_onchain_enabled_rows": registry_sweep_onchain_enabled_rows,
            "registry_sweep_onchain_confirmed_rows": registry_sweep_onchain_confirmed_rows,
            "active_status_counts": dict(status_counts),
            "metric_status_counts": dict(metric_status_counts),
            "source_vs_paper_copy_quality_status_counts": dict(copy_quality_status_counts),
            "copy_edge_loss_primary_reason_counts": dict(copy_edge_loss_primary_reason_counts),
            "copy_edge_loss_reason_counts": dict(copy_edge_loss_reason_counts),
            "high_source_wallets": len(high_source_wallets),
            "high_source_wallets_with_copy_quality_pass": len(copy_quality_pass_wallets),
            "high_source_wallets_with_copy_quality_correction": len(copy_quality_correction_wallets),
            "best_copyable_wallet": _copyable_wallet_row(top_copyable_wallets[0]) if top_copyable_wallets else {},
            "best_current_copyable_wallet": _copyable_wallet_row(top_current_copyable_wallets[0])
            if top_current_copyable_wallets
            else {},
            "best_current_copyable_wallet_large_sample": _copyable_wallet_row(
                top_current_copyable_wallets_large_sample[0]
            )
            if top_current_copyable_wallets_large_sample
            else {},
            "best_source_current_copyable_wallet": _copyable_wallet_row(
                top_source_current_copyable_wallets[0]
            )
            if top_source_current_copyable_wallets
            else {},
            "best_source_current_copyable_wallet_large_sample": _copyable_wallet_row(
                top_source_current_copyable_wallets_large_sample[0]
            )
            if top_source_current_copyable_wallets_large_sample
            else {},
            "copy_quality_targets": {
                "source_high_roi_pct": TARGET_SOURCE_HIGH_ROI_PCT,
                "source_high_wr_pct": TARGET_SOURCE_HIGH_WR_PCT,
                "source_high_leaderboard_pnl_usd": TARGET_SOURCE_HIGH_LEADERBOARD_PNL_USD,
                "paper_min_resolved_orders": TARGET_PAPER_MIN_RESOLVED_ORDERS,
                "paper_min_roi_pct": TARGET_PAPER_MIN_ROI_PCT,
                "paper_min_wr_pct": TARGET_PAPER_MIN_WR_PCT,
                "active_policy_copy_coverage_target_pct": TARGET_ACTIVE_POLICY_COPY_COVERAGE_PCT,
                "active_all_order_fill_rate_target_pct": TARGET_ACTIVE_ALL_ORDER_FILL_RATE_PCT,
                "active_all_order_reject_rate_max_pct": TARGET_ACTIVE_ALL_ORDER_REJECT_RATE_MAX_PCT,
                "live_architecture_target": "MULTI_WALLET_INVENTORY",
                "live_target_avg_orders_per_window": 2.0,
            },
            "onchain_enabled_rows": onchain_enabled_rows,
            "onchain_confirmed_rows": onchain_confirmed_rows,
            "canonical_onchain_status_counts": canonical_tracking.get("onchain_status_counts", {}),
            "active_onchain_status_counts": active_tracking.get("onchain_status_counts", {}),
            "registry_sweep_onchain_status_counts": registry_sweep_tracking.get("onchain_status_counts", {}),
            "canonical_tracking_state_live_orders_allowed": canonical_tracking_state.get("live_orders_allowed"),
            "active_tracking_state_live_orders_allowed": active_tracking_state.get("live_orders_allowed"),
            "registry_sweep_state_live_orders_allowed": registry_sweep_state.get("live_orders_allowed"),
            "registry_sweep_scope": {
                "rotation_enabled": bool(registry_sweep_scope.get("rotation_enabled")),
                "registry_enabled_wallets": registry_sweep_enabled_wallets,
                "wallets_tracked_this_slice": int(num(registry_sweep_scope.get("wallets_tracked"), 0)),
                "max_wallets_per_slice": registry_sweep_wallets_per_slice,
                "rotation_offset": registry_sweep_scope.get("rotation_offset"),
                "next_rotation_offset": registry_sweep_scope.get("next_rotation_offset"),
                "estimated_slices_for_full_cycle": registry_sweep_slices_needed,
                "scope_reason": registry_sweep_scope.get("scope_reason"),
            },
            "active_all_order_tactic_profile_status_counts": active_all_order.get(
                "paper_tactic_profile_status_counts", {}
            ),
            "active_all_order_tactic_profile_pass_events": active_all_order.get(
                "paper_tactic_profile_pass_events", {}
            ),
            "active_all_order_micro_batch_probe": active_all_order.get("micro_batch_all_order_probe", {}),
            "registry_sweep_all_order_tactic_profile_status_counts": registry_sweep_all_order.get(
                "paper_tactic_profile_status_counts", {}
            ),
            "registry_sweep_all_order_tactic_profile_pass_events": registry_sweep_all_order.get(
                "paper_tactic_profile_pass_events", {}
            ),
            "registry_sweep_all_order_micro_batch_probe": registry_sweep_all_order.get(
                "micro_batch_all_order_probe", {}
            ),
            "paper_state_live_orders_allowed": paper_state.get("live_orders_allowed"),
        },
        "coverage_gaps": {
            "wallets_without_history_state": no_history_state,
            "wallets_without_history_events": no_history_events,
            "wallets_without_paper_orders": no_paper,
            "wallets_without_complete_metrics": incomplete_metrics,
            "not_active_hotlane_wallets": sorted(set(registry_wallets) - active_wallets),
        },
        "top_live_copy_candidates": [_compact_wallet_row(row) for row in top_live_copy_candidates],
        "top_copyable_wallets": [_copyable_wallet_row(row) for row in top_copyable_wallets],
        "top_current_copyable_wallets": [_copyable_wallet_row(row) for row in top_current_copyable_wallets],
        "top_current_copyable_wallets_large_sample": [
            _copyable_wallet_row(row) for row in top_current_copyable_wallets_large_sample
        ],
        "top_source_current_copyable_wallets": [
            _copyable_wallet_row(row) for row in top_source_current_copyable_wallets
        ],
        "top_source_current_copyable_wallets_large_sample": [
            _copyable_wallet_row(row) for row in top_source_current_copyable_wallets_large_sample
        ],
        "high_source_copy_corrections": [_compact_wallet_row(row) for row in copy_quality_correction_wallets[:20]],
        "top_wallets": [_compact_wallet_row(row) for row in rows[:20]],
        "wallets": rows,
    }
    atomic_write_json(args.output, payload)

    printed = {
        "output": args.output,
        "summary": payload["summary"],
    }
    if args.quiet or str(args.print_mode) == "none":
        return 0
    if str(args.print_mode) == "full":
        top_rows = rows[: max(0, int(args.top))]
        printed["top_live_copy_candidates"] = payload["top_live_copy_candidates"]
        printed["top_copyable_wallets"] = payload["top_copyable_wallets"][: max(0, int(args.top))]
        printed["top_current_copyable_wallets"] = payload["top_current_copyable_wallets"][
            : max(0, int(args.top))
        ]
        printed["top_current_copyable_wallets_large_sample"] = payload[
            "top_current_copyable_wallets_large_sample"
        ][: max(0, int(args.top))]
        printed["top_source_current_copyable_wallets"] = payload[
            "top_source_current_copyable_wallets"
        ][: max(0, int(args.top))]
        printed["top_source_current_copyable_wallets_large_sample"] = payload[
            "top_source_current_copyable_wallets_large_sample"
        ][: max(0, int(args.top))]
        printed["high_source_copy_corrections"] = payload["high_source_copy_corrections"][: max(0, int(args.top))]
        printed["top_wallets"] = [_compact_wallet_row(row) for row in top_rows]
    print(json.dumps(printed, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
