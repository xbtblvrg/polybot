#!/usr/bin/env python3
"""Classify today's live wallet-copy rejects for Fable RULING 22c.

Flow stage: LIVE/MEASURE. This is a read-only attribution packet. It never
changes live eligibility, caps, thresholds, CopyIntent construction, or order
submission.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import price_bucket as pnl_price_bucket  # noqa: E402
from src.wallet_copy.pnl_truth import score_order as score_pnl_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_2026-07-17.json"
DEFAULT_OUTPUT = "data/research/live_order_reject_attribution_latest.json"
RULING_ID = "2026-07-17T13:52Z-fable-ruling25-negative-fill-maker-recovery-pnl"
FAK_NO_MATCH_RULING_ID = "2026-07-17T13:45Z-fable-ruling24b-fak-no-match-attribution"
PRICE_BAND_RULING_ID = "2026-07-17T14:19Z-fable-ruling26-price-band-tranche-roi"
PRICE_BAND_FIX_GATE_MIN_FILLS = 15
PRICE_BAND_FIX_GATE_MAX_ROI_PCT = -20.0


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _resolve(root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else root / path


def _default_resolutions_path(root: Path = ROOT) -> str:
    candidates = [path for path in (root / "data" / "research").glob("btc_resolutions_*.jsonl") if path.is_file()]
    if not candidates:
        return DEFAULT_RESOLUTIONS
    newest = max(candidates, key=lambda path: path.stat().st_mtime)
    return str(newest.relative_to(root)) if newest.is_relative_to(root) else str(newest)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _parse_ts(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _market_window_start_s(market_slug: Any) -> float | None:
    try:
        return float(str(market_slug).rsplit("-", 1)[-1])
    except (TypeError, ValueError):
        return None


def _submitted_ts(order: dict[str, Any], payload: dict[str, Any] | None = None) -> float | None:
    payload = payload or _last_payload(order)
    latency = _as_dict(payload.get("wallet_copy_latency_budget")) or _as_dict(order.get("latency_budget"))
    for value in (latency.get("submit_sent_ts"), latency.get("exchange_ack_ts")):
        ts = _parse_ts(value)
        if ts is not None:
            return ts
    for row in _as_list(order.get("lifecycle")):
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "") == "LIVE_SUBMITTED":
            ts = _parse_ts(row.get("ts"))
            if ts is not None:
                return ts
    return _parse_ts(order.get("submitted_at"))


def _is_day_order(order: dict[str, Any], day: str) -> bool:
    return str(order.get("submitted_at") or order.get("updated_at") or "").startswith(f"{day}T")


def _status(order: dict[str, Any]) -> str:
    return str(order.get("final_status") or order.get("status") or "").upper()


def _last_payload(order: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for row in _as_list(order.get("lifecycle")):
        if not isinstance(row, dict):
            continue
        row_payload = row.get("payload")
        if isinstance(row_payload, dict):
            payload = row_payload
    return payload


def _live_intent_metadata(order: dict[str, Any]) -> dict[str, Any]:
    parity = _as_dict(order.get("parity_capsule"))
    live_intent = _as_dict(parity.get("live_intent"))
    metadata = _as_dict(live_intent.get("metadata"))
    if metadata:
        return metadata
    decision = _as_dict(parity.get("live_trade_decision"))
    wallet_copy = _as_dict(decision.get("wallet_copy"))
    return _as_dict(wallet_copy.get("metadata"))


def _payload_text(payload: dict[str, Any]) -> str:
    return " ".join(str(payload.get(key) or "") for key in ("error_class", "error", "status", "final_status")).lower()


def _reject_reason(order: dict[str, Any]) -> str:
    payload = _last_payload(order)
    error_class = str(payload.get("error_class") or order.get("reject_reason") or "").strip()
    message = str(payload.get("error") or "").lower()
    lifecycle_message = " ".join(str(row.get("message") or "") for row in _as_list(order.get("lifecycle")) if isinstance(row, dict)).lower()
    if error_class:
        return error_class
    if "maker fallback order canceled" in lifecycle_message or "window end without a fill" in lifecycle_message:
        return "maker_window_end_no_fill"
    if "no orders found to match" in message:
        return "fak_no_match"
    return "unknown_reject"


def _reason_class(reason: str, payload: dict[str, Any]) -> str:
    text = f"{reason} {_payload_text(payload)}"
    if "precision_cap" in text or "policy cap" in text or "price_above" in text or "slippage" in text:
        return "price_band_or_policy_cap"
    if "no orders found to match" in text or "fak_no_match" in text or "no_ask_liquidity" in text:
        return "best_ask_timeout_or_no_match"
    if "min" in text and ("size" in text or "tranche" in text or "order" in text):
        return "min_size"
    if "balance" in text or "allowance" in text or "insufficient" in text:
        return "balance_or_allowance"
    if "closed" in text or "canceled" in text or "window end" in text or "window_end" in text or "market-state" in text:
        return "market_state_or_window_end"
    return "other"


def _forgone_usd(order: dict[str, Any]) -> float:
    payload = _last_payload(order)
    for key in ("size_usd", "market_order_amount_usd", "filled_size_usd"):
        value = _num(payload.get(key), -1.0)
        if value > 0:
            return round(value, 6)
    for key in ("market_order_amount_usd", "amount_usd", "cost_usd"):
        value = _num(order.get(key), -1.0)
        if value > 0:
            return round(value, 6)
    expected = _as_dict(order.get("expected_fee_gate"))
    value = _num(expected.get("estimated_response_cost_usd"), -1.0)
    return round(value, 6) if value > 0 else 0.0


def _filled_usd(order: dict[str, Any]) -> float:
    payload = _last_payload(order)
    for key in ("filled_size_usd", "response_cost_usd", "market_order_amount_usd", "size_usd"):
        value = _num(payload.get(key), -1.0)
        if value > 0:
            return round(value, 6)
    for key in ("filled_size_usd", "cost_usd", "amount_usd"):
        value = _num(order.get(key), -1.0)
        if value > 0:
            return round(value, 6)
    return 0.0


def _entry_price(order: dict[str, Any]) -> float:
    payload = _last_payload(order)
    chase = _as_dict(payload.get("wallet_copy_chase"))
    for value in (
        payload.get("response_fill_price"),
        payload.get("entry_price"),
        chase.get("effective_limit_price"),
        order.get("limit_price"),
    ):
        price = _num(value, -1.0)
        if price > 0:
            return round(price, 6)
    return 0.0


def _tranche_type(order: dict[str, Any]) -> str:
    maker = _as_dict(order.get("maker_fallback"))
    if str(maker.get("parent_order_id") or "").strip():
        return "maker_recovery_fill"
    payload = _last_payload(order)
    role = str(order.get("execution_role") or payload.get("execution_role") or "").strip().lower()
    if role == "maker" or bool(order.get("maker")) or bool(payload.get("maker")):
        return "maker_unlinked_fill"
    return "direct_taker_fill"


def _outcome_side(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"YES", "UP"}:
        return "YES"
    if text in {"NO", "DOWN"}:
        return "NO"
    return text


def _empty_pnl_metric() -> dict[str, Any]:
    return {
        "fills": 0,
        "resolved_fills": 0,
        "negative_fills": 0,
        "positive_fills": 0,
        "zero_fills": 0,
        "cost_usd": 0.0,
        "payout_usd": 0.0,
        "pnl_usd": 0.0,
        "avg_pnl_usd": 0.0,
        "roi_pct": 0.0,
        "negative_rate_pct": 0.0,
    }


def _summarize_pnl_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return _empty_pnl_metric()
    cost = sum(_num(row.get("cost_usd")) for row in rows)
    payout = sum(_num(row.get("payout_usd")) for row in rows)
    pnl = sum(_num(row.get("pnl_usd")) for row in rows)
    negative = sum(1 for row in rows if _num(row.get("pnl_usd")) < 0)
    positive = sum(1 for row in rows if _num(row.get("pnl_usd")) > 0)
    zero = len(rows) - negative - positive
    return {
        "fills": len(rows),
        "resolved_fills": len(rows),
        "negative_fills": negative,
        "positive_fills": positive,
        "zero_fills": zero,
        "cost_usd": round(cost, 6),
        "payout_usd": round(payout, 6),
        "pnl_usd": round(pnl, 6),
        "avg_pnl_usd": round(pnl / len(rows), 6),
        "roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "negative_rate_pct": round((negative / len(rows)) * 100.0, 6),
    }


def _summaries_by(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "unknown")].append(row)
    return {
        group: _summarize_pnl_rows(group_rows)
        for group, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    }


def _maker_vs_direct_summary(all_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_tranche = _summaries_by(all_rows, "tranche_type")
    maker = by_tranche.get("maker_recovery_fill", _empty_pnl_metric())
    direct = by_tranche.get("direct_taker_fill", _empty_pnl_metric())
    return {
        "maker_recovery_fills": maker["fills"],
        "direct_taker_fills": direct["fills"],
        "maker_recovery_avg_pnl_usd": maker["avg_pnl_usd"],
        "direct_taker_avg_pnl_usd": direct["avg_pnl_usd"],
        "maker_minus_direct_avg_pnl_usd": round(
            float(maker["avg_pnl_usd"]) - float(direct["avg_pnl_usd"]), 6
        ),
        "maker_recovery_roi_pct": maker["roi_pct"],
        "direct_taker_roi_pct": direct["roi_pct"],
        "maker_recovery_negative_rate_pct": maker["negative_rate_pct"],
        "direct_taker_negative_rate_pct": direct["negative_rate_pct"],
        "measurement_only_no_fix": True,
    }


def _price_band_tranche_gate(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    packet: dict[str, dict[str, Any]] = {}
    for group, metric in _summaries_by(rows, "price_band_tranche_type").items():
        passes = (
            int(metric["resolved_fills"]) >= PRICE_BAND_FIX_GATE_MIN_FILLS
            and float(metric["roi_pct"]) <= PRICE_BAND_FIX_GATE_MAX_ROI_PCT
        )
        packet[group] = {
            **metric,
            "fix_candidate_gate": {
                "min_resolved_fills": PRICE_BAND_FIX_GATE_MIN_FILLS,
                "max_roi_pct": PRICE_BAND_FIX_GATE_MAX_ROI_PCT,
                "passes": passes,
            },
        }
    return packet


def _book_state_at_submit(order: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    metadata = _live_intent_metadata(order)
    gate = _as_dict(metadata.get("inventory_best_ask_gate"))
    chase = _as_dict(payload.get("wallet_copy_chase"))
    limit_price = _num(
        chase.get("effective_limit_price"),
        _num(payload.get("entry_price"), _num(order.get("limit_price"), 0.0)),
    )
    best_ask = _num(gate.get("best_ask"), 0.0)
    displayed_size = _num(gate.get("best_ask_size"), -1.0)
    if best_ask <= 0:
        verdict = "unknown"
    elif limit_price > 0 and best_ask <= limit_price + 1e-9:
        verdict = "ask_at_or_inside_limit"
    else:
        verdict = "ask_above_limit"
    return {
        "source": "parity_capsule.live_intent.metadata.inventory_best_ask_gate",
        "status": gate.get("status"),
        "reason": gate.get("reason"),
        "best_ask": round(best_ask, 6) if best_ask > 0 else None,
        "limit_price": round(limit_price, 6) if limit_price > 0 else None,
        "displayed_size": round(displayed_size, 6) if displayed_size >= 0 else None,
        "displayed_size_available": displayed_size >= 0,
        "route": gate.get("book_route_winner") or gate.get("book_route_primary"),
        "maker_fallback_candidate": bool(gate.get("maker_fallback_candidate")),
        "verdict": verdict,
    }


def _same_window_outcome(order: dict[str, Any], all_day_orders: list[dict[str, Any]]) -> dict[str, Any]:
    slug = str(order.get("market_slug") or "")
    outcome = str(order.get("outcome") or "")
    submit_ts = _submitted_ts(order)
    fills = [
        row
        for row in all_day_orders
        if row is not order
        and str(row.get("market_slug") or "") == slug
        and str(row.get("outcome") or "") == outcome
        and _status(row) == "FILLED"
    ]
    fills_after = [row for row in fills if submit_ts is None or (_submitted_ts(row) or 0.0) >= submit_ts]
    return {
        "same_window_fill_count": len(fills),
        "same_window_fill_count_after_reject_submit": len(fills_after),
        "same_window_filled_usd": round(sum(_filled_usd(row) for row in fills), 6),
        "same_window_eventual_fill": bool(fills),
        "forgone_estimate_overstated_by_same_window_fill": bool(fills),
    }


def _maker_fallback_links(order: dict[str, Any], maker_by_parent_order_id: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    linked = maker_by_parent_order_id.get(str(order.get("order_id") or ""), [])
    return {
        "engaged": bool(linked),
        "linked_order_ids": [row.get("order_id") for row in linked],
        "linked_intent_ids": [row.get("intent_id") for row in linked],
        "linked_final_statuses": [row.get("final_status") or row.get("status") for row in linked],
    }


def _event_scheduler_markets(guard_state: dict[str, Any]) -> set[str]:
    scheduler = _as_dict(guard_state.get("event_triggered_cycle_scheduler"))
    markets = set()
    for key in ("source_event", "last_trigger"):
        row = _as_dict(scheduler.get(key))
        market = str(row.get("market_slug") or "").strip()
        if market:
            markets.add(market)
    return markets


def _is_scheduler_subset(order: dict[str, Any], scheduler_markets: set[str]) -> bool:
    if str(order.get("market_slug") or "") in scheduler_markets:
        return True
    payload = _last_payload(order)
    return str(payload.get("market_slug") or "") in scheduler_markets


def _class_rows(orders: list[dict[str, Any]], scheduler_markets: set[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    class_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    class_forgone: defaultdict[str, float] = defaultdict(float)
    scheduler_counts: Counter[str] = Counter()
    scheduler_forgone: defaultdict[str, float] = defaultdict(float)
    examples: list[dict[str, Any]] = []

    for order in orders:
        payload = _last_payload(order)
        reason = _reject_reason(order)
        klass = _reason_class(reason, payload)
        forgone = _forgone_usd(order)
        is_scheduler = _is_scheduler_subset(order, scheduler_markets)
        class_counts[klass] += 1
        reason_counts[reason] += 1
        class_forgone[klass] += forgone
        if is_scheduler:
            scheduler_counts[klass] += 1
            scheduler_forgone[klass] += forgone
        if len(examples) < 12:
            examples.append(
                {
                    "submitted_at": order.get("submitted_at"),
                    "market_slug": order.get("market_slug"),
                    "intent_id": order.get("intent_id"),
                    "execution_role": order.get("execution_role"),
                    "reason": reason,
                    "class": klass,
                    "forgone_usd": forgone,
                    "scheduler_subset": is_scheduler,
                    "error": payload.get("error"),
                }
            )

    classes = {
        klass: {
            "count": count,
            "forgone_usd_estimate": round(class_forgone[klass], 6),
        }
        for klass, count in sorted(class_counts.items(), key=lambda item: (-item[1], item[0]))
    }
    scheduler = {
        klass: {
            "count": count,
            "forgone_usd_estimate": round(scheduler_forgone[klass], 6),
        }
        for klass, count in sorted(scheduler_counts.items(), key=lambda item: (-item[1], item[0]))
    }
    summary = {
        "rejects": len(orders),
        "classes": classes,
        "reason_counts": dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "forgone_usd_estimate": round(sum(class_forgone.values()), 6),
        "scheduler_triggered_subset": {
            "market_slugs": sorted(scheduler_markets),
            "rejects": sum(scheduler_counts.values()),
            "classes": scheduler,
            "forgone_usd_estimate": round(sum(scheduler_forgone.values()), 6),
        },
    }
    return summary, examples


def _fak_no_match_analysis(all_day_orders: list[dict[str, Any]], rejects: list[dict[str, Any]]) -> dict[str, Any]:
    maker_by_parent_order_id: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in all_day_orders:
        maker = _as_dict(order.get("maker_fallback"))
        parent = str(maker.get("parent_order_id") or "")
        if parent:
            maker_by_parent_order_id[parent].append(order)

    rows: list[dict[str, Any]] = []
    book_counts: Counter[str] = Counter()
    latency_values: list[float] = []
    maker_count = 0
    same_window_fill_count = 0
    net_unrecovered_forgone_usd = 0.0
    unrecovered_rows = 0
    race_loss_usd = 0.0
    for order in rejects:
        payload = _last_payload(order)
        reason = _reject_reason(order)
        if reason != "fak_no_match":
            continue
        submit_ts = _submitted_ts(order, payload)
        window_start = _market_window_start_s(order.get("market_slug"))
        close_ts = window_start + 300.0 if window_start is not None else None
        submit_to_close_s = round(close_ts - submit_ts, 6) if submit_ts is not None and close_ts is not None else None
        if submit_to_close_s is not None:
            latency_values.append(submit_to_close_s)
        book_state = _book_state_at_submit(order, payload)
        book_counts[str(book_state["verdict"])] += 1
        maker_link = _maker_fallback_links(order, maker_by_parent_order_id)
        if maker_link["engaged"]:
            maker_count += 1
        same_window = _same_window_outcome(order, all_day_orders)
        if same_window["same_window_eventual_fill"]:
            same_window_fill_count += 1
        else:
            unrecovered_rows += 1
            net_unrecovered_forgone_usd += _forgone_usd(order)
            if book_state["verdict"] == "ask_at_or_inside_limit":
                race_loss_usd += _forgone_usd(order)
        rows.append(
            {
                "submitted_at": order.get("submitted_at"),
                "market_slug": order.get("market_slug"),
                "outcome": order.get("outcome"),
                "intent_id": order.get("intent_id"),
                "order_id": order.get("order_id"),
                "forgone_usd": _forgone_usd(order),
                "submit_to_window_close_s": submit_to_close_s,
                "latency_budget": {
                    "submit_sent_ts": (_as_dict(payload.get("wallet_copy_latency_budget")) or _as_dict(order.get("latency_budget"))).get("submit_sent_ts"),
                    "exchange_ack_ts": (_as_dict(payload.get("wallet_copy_latency_budget")) or _as_dict(order.get("latency_budget"))).get("exchange_ack_ts"),
                    "submit_sent_to_exchange_ack_s": (
                        _as_dict((_as_dict(payload.get("wallet_copy_latency_budget")) or _as_dict(order.get("latency_budget"))).get("hops"))
                    ).get("submit_sent_to_exchange_ack_s"),
                },
                "book_state_at_submit": book_state,
                "maker_fallback_link": maker_link,
                "same_window_outcome": same_window,
            }
        )

    latency_sorted = sorted(latency_values)
    p50 = latency_sorted[len(latency_sorted) // 2] if latency_sorted else None
    return {
        "flow_stage": "LIVE/MEASURE",
        "ruling_id": FAK_NO_MATCH_RULING_ID,
        "count": len(rows),
        "rows": rows,
        "summary": {
            "count": len(rows),
            "fak_no_match_rejects": len(rows),
            "forgone_usd_estimate": round(sum(row["forgone_usd"] for row in rows), 6),
            "submit_to_window_close_s_avg": round(sum(latency_values) / len(latency_values), 6) if latency_values else None,
            "submit_to_window_close_s_p50": round(p50, 6) if p50 is not None else None,
            "book_state_verdict_counts": dict(sorted(book_counts.items())),
            "maker_fallback_engaged": maker_count,
            "same_window_eventual_fill": same_window_fill_count,
            "same_window_eventual_fill_count": same_window_fill_count,
            "unrecovered_rows": unrecovered_rows,
            "net_unrecovered_forgone_usd": round(net_unrecovered_forgone_usd, 6),
            "race_loss_usd": round(race_loss_usd, 6),
            "live_path_mutated": False,
            "threshold_change": False,
        },
    }


def _fill_payload(order: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for row in _as_list(order.get("lifecycle")):
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "") == "LIVE_FILLED" and isinstance(row.get("payload"), dict):
            payload = row["payload"]
    return payload or _last_payload(order)


def _price_band(price: float) -> str:
    if price < 0.25:
        return "00_lt_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_gte_70"


def _negative_fill_pnl_analysis(
    all_day_orders: list[dict[str, Any]],
    fak_analysis: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    reject_by_order_id = {
        str(order.get("order_id") or ""): order
        for order in all_day_orders
        if _status(order) == "REJECTED" and str(order.get("order_id") or "")
    }
    fak_recovery_keys = {
        (str(row.get("market_slug") or ""), str(row.get("outcome") or ""))
        for row in _as_list(fak_analysis.get("rows"))
        if _as_dict(row.get("same_window_outcome")).get("same_window_eventual_fill")
    }
    rows: list[dict[str, Any]] = []
    resolved_rows: list[dict[str, Any]] = []
    unresolved_fills = 0

    for order in all_day_orders:
        if _status(order) != "FILLED":
            continue
        event = score_pnl_order(order, resolutions)
        if not event.get("resolved"):
            unresolved_fills += 1
            continue
        payload = _fill_payload(order)
        entry_price = _entry_price(order) or _num(event.get("limit_price"), 0.0)
        execution_role = str(order.get("execution_role") or payload.get("execution_role") or "")
        same_window_recovery = (str(order.get("market_slug") or ""), str(order.get("outcome") or "")) in fak_recovery_keys
        tranche_type = _tranche_type(order)
        maker = _as_dict(order.get("maker_fallback"))
        parent_order_id = str(maker.get("parent_order_id") or "").strip()
        parent_reject = reject_by_order_id.get(parent_order_id, {})
        pnl = _num(event.get("pnl_usd"), 0.0)
        cost = _num(event.get("cost_usd"), _filled_usd(order))
        price_band = event.get("price_bucket") or (pnl_price_bucket(entry_price) if entry_price else _price_band(entry_price))
        row = {
            "submitted_at": order.get("submitted_at"),
            "market_slug": order.get("market_slug"),
            "intent_id": order.get("intent_id"),
            "order_id": order.get("order_id"),
            "source_wallet": event.get("source_wallet"),
            "wallet": event.get("source_wallet"),
            "policy_id": event.get("lane"),
            "price_band": price_band,
            "entry_price": round(entry_price, 6) if entry_price else None,
            "resolution_winner": event.get("winner"),
            "entry_side": event.get("side"),
            "entry_outcome": order.get("outcome"),
            "entry_outcome_side": _outcome_side(order.get("outcome")),
            "cost_usd": round(cost, 6),
            "payout_usd": round(_num(event.get("payout_usd"), 0.0), 6),
            "pnl_usd": round(pnl, 6),
            "roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
            "tranche_type": tranche_type,
            "execution_role": execution_role or None,
            "same_window_recovery_after_fak_no_match": same_window_recovery,
            "maker_parent_order_id": parent_order_id or None,
            "parent_reject_reason": _reject_reason(parent_reject) if parent_reject else None,
            "cost_basis_source": event.get("cost_basis_source"),
        }
        row["price_band_tranche_type"] = f"{price_band}|{tranche_type}"
        resolved_rows.append(row)
        if pnl < 0:
            rows.append(row)

    return {
        "flow_stage": "LIVE/MEASURE",
        "ruling_id": PRICE_BAND_RULING_ID,
        "summary": {
            "counts_basis": "resolved_only_canonical_pnl_truth_events_joined_by_order_id",
            "previous_ruling_id": RULING_ID,
            "resolved_fills": len(resolved_rows),
            "resolved_fills_joined": len(resolved_rows),
            "unresolved_fills": unresolved_fills,
            "negative_fills": len(rows),
            "negative_pnl_usd": round(sum(row["pnl_usd"] for row in rows), 6),
            "all_resolved_pnl": _summarize_pnl_rows(resolved_rows),
            "aggregate_by_tranche_type": _summaries_by(resolved_rows, "tranche_type"),
            "aggregate_by_price_band_tranche_type": _price_band_tranche_gate(resolved_rows),
            "negative_by_tranche_type": _summaries_by(rows, "tranche_type"),
            "negative_by_wallet": _summaries_by(rows, "source_wallet"),
            "negative_by_policy": _summaries_by(rows, "policy_id"),
            "negative_by_price_band": _summaries_by(rows, "price_band"),
            "maker_recovery_vs_direct_taker": _maker_vs_direct_summary(resolved_rows),
            "live_path_mutated": False,
            "threshold_change": False,
        },
        "rows": rows,
    }


def build_report(
    live_state: dict[str, Any],
    guard_state: dict[str, Any],
    *,
    day: str,
    generated_at: str,
    scorecard_state: dict[str, Any] | None = None,
    resolutions: dict[str, dict[str, Any]] | None = None,
    live_state_path: str = DEFAULT_LIVE_STATE,
    guard_state_path: str = DEFAULT_GUARD_STATE,
    resolutions_path: str = DEFAULT_RESOLUTIONS,
    scorecard_path: str = DEFAULT_SCORECARD,
) -> dict[str, Any]:
    all_day_orders = [
        row for row in _as_list(live_state.get("orders")) if isinstance(row, dict) and _is_day_order(row, day)
    ]
    rejects = [row for row in all_day_orders if _status(row) == "REJECTED"]
    fills = [row for row in all_day_orders if _status(row) == "FILLED"]
    scheduler_markets = _event_scheduler_markets(guard_state)
    summary, examples = _class_rows(rejects, scheduler_markets)
    fak_analysis = _fak_no_match_analysis(all_day_orders, rejects)
    negative_fill_analysis = _negative_fill_pnl_analysis(all_day_orders, fak_analysis, resolutions or {})
    summary.update(
        {
            "day": day,
            "orders": len(all_day_orders),
            "fills": len(fills),
            "reject_rate_pct": round((len(rejects) / len(all_day_orders) * 100.0), 6) if all_day_orders else 0.0,
            "dominant_class": next(iter(summary["classes"]), None),
            "flow_stage": "LIVE/MEASURE",
            "threshold_change": False,
            "live_path_mutated": False,
        }
    )
    return {
        "schema_version": 1,
        "kind": "live_order_reject_attribution",
        "flow_stage": "LIVE/MEASURE",
        "ruling_id": RULING_ID,
        "generated_at": generated_at,
        "live_path_mutated": False,
        "threshold_change": False,
        "source": {
            "live_state_path": live_state_path,
            "guard_state_path": guard_state_path,
            "resolutions_path": resolutions_path,
            "scorecard_path": scorecard_path,
            "guard_pid": guard_state.get("pid"),
            "guard_generated_at": guard_state.get("generated_at"),
        },
        "summary": summary,
        "examples": examples,
        "fak_no_match_analysis": fak_analysis,
        "negative_fill_pnl_analysis": negative_fill_analysis,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--resolutions", default=_default_resolutions_path())
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    live_path = _resolve(ROOT, args.live_state)
    guard_path = _resolve(ROOT, args.guard_state)
    resolutions_path = _resolve(ROOT, args.resolutions)
    scorecard_path = _resolve(ROOT, args.scorecard)
    output_path = _resolve(ROOT, args.output)
    packet = build_report(
        load_json(live_path, default={}) or {},
        load_json(guard_path, default={}) or {},
        day=args.day,
        generated_at=_utc_now_iso(),
        scorecard_state=load_json(scorecard_path, default={}) or {},
        resolutions=load_resolutions(resolutions_path),
        live_state_path=str(live_path.relative_to(ROOT)) if live_path.is_relative_to(ROOT) else str(live_path),
        guard_state_path=str(guard_path.relative_to(ROOT)) if guard_path.is_relative_to(ROOT) else str(guard_path),
        resolutions_path=str(resolutions_path.relative_to(ROOT)) if resolutions_path.is_relative_to(ROOT) else str(resolutions_path),
        scorecard_path=str(scorecard_path.relative_to(ROOT)) if scorecard_path.is_relative_to(ROOT) else str(scorecard_path),
    )
    atomic_write_json(output_path, packet)
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
