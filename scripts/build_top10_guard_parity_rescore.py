#!/usr/bin/env python3
"""Rescore the top-10 clearance replay under live-guard parity semantics."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_REPLAY = "data/research/wallet_copy_top10_watch_clearance_replay.json"
DEFAULT_CLEARANCE = "data/research/wallet_copy_top10_watch_clearance_summary.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_top10_guard_parity_rescore.json"

STRICT_SLIPPAGE_BPS = 250.0
MARGINAL_MULTIPLIER = 1.6
CONFIGURED_DRIFT_BUFFER_PRICE = 0.05
APPLIED_INVENTORY_TICK_BUFFER_PRICE = 0.01
DRIP_MIN_TRANCHE_USD = 1.0
DRIP_MAX_TRANCHE_USD = 2.5
MAKER_FALLBACK_PRICE_CEILING = 0.50
REALISTIC_COPY_LATENCY_S = 5.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--clearance-summary", default=DEFAULT_CLEARANCE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--strict-slippage-bps", type=float, default=STRICT_SLIPPAGE_BPS)
    parser.add_argument("--marginal-multiplier", type=float, default=MARGINAL_MULTIPLIER)
    parser.add_argument("--configured-drift-buffer-price", type=float, default=CONFIGURED_DRIFT_BUFFER_PRICE)
    parser.add_argument("--applied-drift-buffer-price", type=float, default=APPLIED_INVENTORY_TICK_BUFFER_PRICE)
    parser.add_argument("--drip-min-tranche-usd", type=float, default=DRIP_MIN_TRANCHE_USD)
    parser.add_argument("--drip-max-tranche-usd", type=float, default=DRIP_MAX_TRANCHE_USD)
    parser.add_argument("--maker-fallback-price-ceiling", type=float, default=MAKER_FALLBACK_PRICE_CEILING)
    parser.add_argument("--copy-latency-s", type=float, default=REALISTIC_COPY_LATENCY_S)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _slug_start(slug: str) -> int | None:
    marker = str(slug or "").rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


def _slug_close(slug: str) -> int | None:
    start = _slug_start(slug)
    return start + 300 if start is not None else None


def _source_event_ts(order: dict[str, Any]) -> float | None:
    source = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    for key in ("event_ts", "observed_ts"):
        value = num(source.get(key), 0.0)
        if value > 0:
            return float(value)
    return None


def _book_ts_s(details: dict[str, Any]) -> float | None:
    value = str(details.get("book_timestamp") or "").strip()
    if not value:
        return None
    raw = num(value, 0.0)
    if raw <= 0:
        return None
    return float(raw / 1000.0 if raw > 10_000_000_000 else raw)


def _fill_estimate(order: dict[str, Any]) -> dict[str, Any]:
    return order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}


def _reject_details(order: dict[str, Any]) -> dict[str, Any]:
    fill = _fill_estimate(order)
    details = fill.get("reject_details") if isinstance(fill.get("reject_details"), dict) else {}
    if details:
        return details
    book = fill.get("book") if isinstance(fill.get("book"), dict) else {}
    return book


def _is_rejected(order: dict[str, Any]) -> bool:
    fill = _fill_estimate(order)
    return str(order.get("final_status") or "").upper() == "REJECTED" or str(fill.get("status") or "").upper() == "REJECTED"


def _is_filled(order: dict[str, Any]) -> bool:
    fill = _fill_estimate(order)
    return str(order.get("final_status") or "").upper() == "FILLED" or str(fill.get("status") or "").upper() == "FILLED"


def _blocking_reason(order: dict[str, Any]) -> str:
    fill = _fill_estimate(order)
    details = _reject_details(order)
    reason = (
        details.get("blocking_reason")
        or fill.get("reject_reason")
        or fill.get("reject_stage")
        or "missing_clob_book_evidence"
    )
    text = str(reason or "").removeprefix("clob_")
    return text or "missing_clob_book_evidence"


def _required_slippage_bps(details: dict[str, Any]) -> float | None:
    source_price = num(details.get("source_price"), 0.0)
    best_ask = num(details.get("best_ask"), 0.0)
    if source_price <= 0 or best_ask <= 0:
        return None
    return round(((best_ask / source_price) - 1.0) * 10000.0, 6)


def classify_reject(
    order: dict[str, Any],
    *,
    strict_slippage_bps: float = STRICT_SLIPPAGE_BPS,
    marginal_multiplier: float = MARGINAL_MULTIPLIER,
    configured_drift_buffer_price: float = CONFIGURED_DRIFT_BUFFER_PRICE,
    applied_drift_buffer_price: float = APPLIED_INVENTORY_TICK_BUFFER_PRICE,
    maker_fallback_price_ceiling: float = MAKER_FALLBACK_PRICE_CEILING,
    copy_latency_s: float = REALISTIC_COPY_LATENCY_S,
) -> dict[str, Any]:
    """Classify a replay reject without changing live/admission policy."""

    details = _reject_details(order)
    reason = _blocking_reason(order)
    required_bps = _required_slippage_bps(details)
    source_price = num(details.get("source_price"), 0.0)
    best_ask = num(details.get("best_ask"), 0.0)
    market_slug = str(order.get("market_slug") or ((order.get("source_intent") or {}).get("market_slug") if isinstance(order.get("source_intent"), dict) else "") or "")
    close_ts = _slug_close(market_slug)
    event_ts = _source_event_ts(order)
    book_ts = _book_ts_s(details)
    live_at_source_plus_latency = (
        bool(close_ts and event_ts) and float(event_ts) + float(copy_latency_s) <= float(close_ts)
    )
    close_minus_source_trade_s = round(float(close_ts) - float(event_ts), 6) if close_ts and event_ts else None
    book_snapshot_lag_s = round(float(book_ts) - float(event_ts), 6) if book_ts and event_ts else None
    book_after_close = bool(close_ts and book_ts and float(book_ts) >= float(close_ts))
    convergence_limit_price = (
        round(min(0.99, source_price + float(applied_drift_buffer_price)), 6)
        if source_price > 0
        else None
    )
    maker_fallback_candidate = bool(
        live_at_source_plus_latency
        and source_price > 0
        and convergence_limit_price is not None
        and convergence_limit_price <= float(maker_fallback_price_ceiling) + 1e-9
        and (best_ask <= 0 or best_ask > convergence_limit_price + 1e-9)
    )
    base = {
        "configured_drift_buffer_price": float(configured_drift_buffer_price),
        "applied_drift_buffer_price": float(applied_drift_buffer_price),
        "convergence_limit_price": convergence_limit_price,
        "maker_fallback_price_ceiling": float(maker_fallback_price_ceiling),
        "maker_fallback_candidate": maker_fallback_candidate,
        "market_slug": market_slug,
        "source_event_ts": event_ts,
        "market_close_ts": close_ts,
        "market_close_minus_source_trade_s": close_minus_source_trade_s,
        "book_ts": book_ts,
        "book_snapshot_lag_s": book_snapshot_lag_s,
        "live_at_source_plus_latency": live_at_source_plus_latency,
        "book_after_close": book_after_close,
    }

    if reason == "price_above_slippage_cap":
        threshold = float(strict_slippage_bps) * float(marginal_multiplier)
        category = "marginal_slippage" if required_bps is not None and required_bps <= threshold else "deep_slippage"
        drift_buffer_taker_fillable = bool(
            convergence_limit_price is not None
            and best_ask > 0
            and best_ask <= convergence_limit_price + 1e-9
        )
        return {
            **base,
            "category": category,
            "reason": reason,
            "required_slippage_bps": required_bps,
            "strict_slippage_bps": float(strict_slippage_bps),
            "marginal_threshold_bps": round(threshold, 6),
            "drift_buffer_taker_fillable": drift_buffer_taker_fillable,
            "parity_taker_fillable": drift_buffer_taker_fillable,
            "maker_fallback_maybe_fill": maker_fallback_candidate,
        }

    if reason in {"no_ask_liquidity", "book_unavailable_or_market_closed", "missing_clob_book_evidence"}:
        replay_artifact = bool(live_at_source_plus_latency and (book_after_close or book_ts is None))
        category = "maker_fallback_fillable" if maker_fallback_candidate else "structural_no_book"
        return {
            **base,
            "category": category,
            "reason": reason,
            "required_slippage_bps": required_bps,
            "strict_slippage_bps": float(strict_slippage_bps),
            "drift_buffer_taker_fillable": False,
            "parity_taker_fillable": False,
            "maker_fallback_maybe_fill": category == "maker_fallback_fillable",
            "replay_artifact_book_expired": replay_artifact,
        }

    return {
        **base,
        "category": "structural_no_book",
        "reason": reason,
        "required_slippage_bps": required_bps,
        "strict_slippage_bps": float(strict_slippage_bps),
        "drift_buffer_taker_fillable": False,
        "parity_taker_fillable": False,
        "maker_fallback_maybe_fill": False,
    }


def _clearance_rows(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if wallet:
            out[wallet] = row
    return out


def _wallet_row(
    candidate: dict[str, Any],
    *,
    clearance_row: dict[str, Any],
    strict_slippage_bps: float,
    marginal_multiplier: float,
    configured_drift_buffer_price: float,
    applied_drift_buffer_price: float,
    maker_fallback_price_ceiling: float,
    copy_latency_s: float,
) -> dict[str, Any]:
    wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
    replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
    orders = replay.get("replay_orders") if isinstance(replay.get("replay_orders"), list) else []
    filled_orders = [order for order in orders if isinstance(order, dict) and _is_filled(order)]
    rejected_orders = [order for order in orders if isinstance(order, dict) and _is_rejected(order)]
    classifications = [
        classify_reject(
            order,
            strict_slippage_bps=strict_slippage_bps,
            marginal_multiplier=marginal_multiplier,
            configured_drift_buffer_price=configured_drift_buffer_price,
            applied_drift_buffer_price=applied_drift_buffer_price,
            maker_fallback_price_ceiling=maker_fallback_price_ceiling,
            copy_latency_s=copy_latency_s,
        )
        for order in rejected_orders
        if isinstance(order, dict)
    ]
    category_counts = Counter(str(row.get("category") or "") for row in classifications)
    reason_counts = Counter(str(row.get("reason") or "") for row in classifications)
    parity_taker_extra = sum(1 for row in classifications if row.get("parity_taker_fillable"))
    maker_maybe = sum(1 for row in classifications if row.get("maker_fallback_maybe_fill"))
    maker_candidates = sum(1 for row in classifications if row.get("maker_fallback_candidate"))
    replay_denominator = len(filled_orders) + len(rejected_orders)
    parity_taker_count = len(filled_orders) + parity_taker_extra
    parity_maybe_count = parity_taker_count + maker_maybe
    source_live = sum(1 for row in classifications if row.get("live_at_source_plus_latency"))
    replay_artifacts = sum(1 for row in classifications if row.get("replay_artifact_book_expired"))
    return {
        "wallet": wallet,
        "candidate_id": candidate.get("candidate_id") or clearance_row.get("candidate_id") or "",
        "clearance_status": clearance_row.get("clearance_status") or "",
        "existing_taker_copyable_rate_pct": clearance_row.get("copyable_rate_pct"),
        "existing_taker_copyable_buy_events": int(clearance_row.get("copyable_buy_events") or 0),
        "existing_buy_events": int(clearance_row.get("buy_events") or 0),
        "replay_orders_scored": replay_denominator,
        "strict_replay_fills": len(filled_orders),
        "replay_rejects_classified": len(classifications),
        "reject_category_counts": dict(sorted(category_counts.items())),
        "reject_reason_counts": dict(sorted(reason_counts.items())),
        "parity_taker_fillable_orders": parity_taker_count,
        "maker_fallback_candidate_rejects": maker_candidates,
        "parity_maker_maybe_fill_orders": maker_maybe,
        "copyable_rate_pct_parity_taker": round((parity_taker_count / replay_denominator) * 100.0, 6)
        if replay_denominator
        else None,
        "copyable_rate_pct_parity_taker_plus_maker_maybe": round((parity_maybe_count / replay_denominator) * 100.0, 6)
        if replay_denominator
        else None,
        "source_live_at_trade_time_rejects": source_live,
        "replay_artifact_book_expired_rejects": replay_artifacts,
        "sample_rejects": classifications[:25],
        "rejects": classifications,
    }


def build_report(
    *,
    replay_payload: dict[str, Any],
    clearance_summary: dict[str, Any],
    strict_slippage_bps: float = STRICT_SLIPPAGE_BPS,
    marginal_multiplier: float = MARGINAL_MULTIPLIER,
    configured_drift_buffer_price: float = CONFIGURED_DRIFT_BUFFER_PRICE,
    applied_drift_buffer_price: float = APPLIED_INVENTORY_TICK_BUFFER_PRICE,
    drip_min_tranche_usd: float = DRIP_MIN_TRANCHE_USD,
    drip_max_tranche_usd: float = DRIP_MAX_TRANCHE_USD,
    maker_fallback_price_ceiling: float = MAKER_FALLBACK_PRICE_CEILING,
    copy_latency_s: float = REALISTIC_COPY_LATENCY_S,
) -> dict[str, Any]:
    clearance_by_wallet = _clearance_rows(clearance_summary)
    candidates = [
        row
        for row in (replay_payload.get("candidates") if isinstance(replay_payload.get("candidates"), list) else [])
        if isinstance(row, dict)
    ]
    rows = [
        _wallet_row(
            candidate,
            clearance_row=clearance_by_wallet.get(_norm_wallet(candidate.get("wallet") or candidate.get("source_wallet")), {}),
            strict_slippage_bps=strict_slippage_bps,
            marginal_multiplier=marginal_multiplier,
            configured_drift_buffer_price=configured_drift_buffer_price,
            applied_drift_buffer_price=applied_drift_buffer_price,
            maker_fallback_price_ceiling=maker_fallback_price_ceiling,
            copy_latency_s=copy_latency_s,
        )
        for candidate in candidates
    ]
    reject_category_counts = Counter()
    reject_reason_counts = Counter()
    for row in rows:
        reject_category_counts.update(row.get("reject_category_counts") or {})
        reject_reason_counts.update(row.get("reject_reason_counts") or {})
    partial_wallets = [
        row
        for row in rows
        if str(row.get("clearance_status")) == "ANALYZE_PARTIAL_SAMPLE"
    ]
    partial_reaches_copyable_sample = [
        row["wallet"]
        for row in partial_wallets
        if int(row.get("parity_taker_fillable_orders") or 0) >= 5
        or int((row.get("reject_category_counts") or {}).get("deep_slippage") or 0) > 0
    ]
    return {
        "schema_version": 1,
        "kind": "wallet_copy_top10_guard_parity_rescore",
        "flow_stage": "PROMOTE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "status": "PASS",
        "parity_semantics": {
            "strict_slippage_bps": float(strict_slippage_bps),
            "marginal_multiplier": float(marginal_multiplier),
            "marginal_slippage_threshold_bps": round(float(strict_slippage_bps) * float(marginal_multiplier), 6),
            "configured_max_drift_buffer_price": float(configured_drift_buffer_price),
            "applied_inventory_tick_buffer_price": float(applied_drift_buffer_price),
            "drip_min_tranche_usd": float(drip_min_tranche_usd),
            "drip_max_tranche_usd": float(drip_max_tranche_usd),
            "maker_fallback_price_ceiling": float(maker_fallback_price_ceiling),
            "copy_latency_s": float(copy_latency_s),
            "maker_fallback_is_maybe_fill": True,
            "note": "configured max buffer is the guard arg; applied inventory/drip transform caps this to one tick before best-ask gate",
        },
        "summary": {
            "candidate_count": len(rows),
            "replay_rejects_classified": sum(int(row.get("replay_rejects_classified") or 0) for row in rows),
            "strict_replay_fills": sum(int(row.get("strict_replay_fills") or 0) for row in rows),
            "parity_taker_fillable_orders": sum(int(row.get("parity_taker_fillable_orders") or 0) for row in rows),
            "maker_fallback_candidate_rejects": sum(int(row.get("maker_fallback_candidate_rejects") or 0) for row in rows),
            "parity_maker_maybe_fill_orders": sum(int(row.get("parity_maker_maybe_fill_orders") or 0) for row in rows),
            "reject_category_counts": dict(sorted(reject_category_counts.items())),
            "reject_reason_counts": dict(sorted(reject_reason_counts.items())),
            "all_rejects_classified": sum(int(row.get("replay_rejects_classified") or 0) for row in rows) == sum(
                sum(int(count) for count in (row.get("reject_category_counts") or {}).values()) for row in rows
            ),
            "source_live_at_trade_time_rejects": sum(int(row.get("source_live_at_trade_time_rejects") or 0) for row in rows),
            "replay_artifact_book_expired_rejects": sum(int(row.get("replay_artifact_book_expired_rejects") or 0) for row in rows),
            "partial_sample_wallets": [row["wallet"] for row in partial_wallets],
            "partial_sample_wallets_with_decisive_parity_evidence": partial_reaches_copyable_sample,
        },
        "rows": rows,
        "next": "use parity taker counts as measurement correction only; maker maybe-fill stays separate from admission arithmetic",
    }


def main() -> int:
    args = parse_args()
    payload = build_report(
        replay_payload=load_json(args.replay, default={}),
        clearance_summary=load_json(args.clearance_summary, default={}),
        strict_slippage_bps=float(args.strict_slippage_bps),
        marginal_multiplier=float(args.marginal_multiplier),
        configured_drift_buffer_price=float(args.configured_drift_buffer_price),
        applied_drift_buffer_price=float(args.applied_drift_buffer_price),
        drip_min_tranche_usd=float(args.drip_min_tranche_usd),
        drip_max_tranche_usd=float(args.drip_max_tranche_usd),
        maker_fallback_price_ceiling=float(args.maker_fallback_price_ceiling),
        copy_latency_s=float(args.copy_latency_s),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
