#!/usr/bin/env python3
"""Audit self-feed-only candidate fills against the live guard path.

Flow stage: LIVE/SELF-DEV. This is the confirming instrument for self-feed
rows that are visible in our wallet Data API feed but absent from the live
ledger. The audit is deliberately conservative: Data API-only evidence is not
enough to confirm a non-guard fill.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASH_LEDGER = "data/research/wallet_copy_recon_window_cash_ledger_latest.json"
DEFAULT_SELF_FEED = "data/research/wallet_copy_self_feed_vs_ledger_latest.json"
DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_guard_fill_recording_audit_latest.json"
RULED_RESIDUAL_USD = -5.27


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _tx(value: Any) -> str:
    return str(value or "").strip().lower()


def _norm_outcome(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"up", "yes"}:
        return "up"
    if text in {"down", "no"}:
        return "down"
    return text


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return _num(text, 0.0)


def _flatten_order_strings(value: Any, *, parent_key: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key).lower()
            if any(fragment in key_text for fragment in ("tx", "transaction")):
                if isinstance(item, str):
                    found.append(_tx(item))
                elif isinstance(item, list):
                    found.extend(_tx(part) for part in item if isinstance(part, str))
            found.extend(_flatten_order_strings(item, parent_key=key_text))
    elif isinstance(value, list):
        for item in value:
            found.extend(_flatten_order_strings(item, parent_key=parent_key))
    return [item for item in found if item.startswith("0x")]


def _flatten_order_ids(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key).lower()
            if key_text in {"order_id", "orderid", "clob_order_id"}:
                if isinstance(item, str):
                    found.append(_tx(item))
            found.extend(_flatten_order_ids(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_flatten_order_ids(item))
    return [item for item in found if item.startswith("0x")]


def _live_fill_payload(order: dict[str, Any]) -> dict[str, Any]:
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    for item in reversed(lifecycle):
        if not isinstance(item, dict) or "FILLED" not in str(item.get("status") or "").upper():
            continue
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        return payload
    return {}


def _order_trace(order: dict[str, Any]) -> dict[str, Any]:
    tx_hashes = sorted(set(_flatten_order_strings(order)))
    order_ids = sorted(set(_flatten_order_ids(order)))
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    latency = order.get("latency_budget") if isinstance(order.get("latency_budget"), dict) else {}
    submitted_ts = _parse_ts(order.get("submitted_at"))
    source_ts = _num(latency.get("source_fill_block_ts"), 0.0) or _parse_ts(source_intent.get("event_ts"))
    fill_payload = _live_fill_payload(order)
    return {
        "order_id": order.get("order_id"),
        "intent_id": order.get("intent_id"),
        "status": order.get("status") or order.get("final_status"),
        "submitted_at": order.get("submitted_at"),
        "submitted_ts": submitted_ts,
        "source_fill_block_ts": source_ts,
        "condition_id": order.get("condition_id") or source_intent.get("condition_id"),
        "market_slug": order.get("market_slug") or source_intent.get("market_slug"),
        "outcome": order.get("outcome") or source_intent.get("outcome"),
        "source_wallet": order.get("source_wallet") or source_intent.get("source_wallet"),
        "order_ids": order_ids,
        "tx_hashes": tx_hashes,
        "live_filled_lifecycle": bool(fill_payload),
        "response_fill_price": fill_payload.get("response_fill_price") or fill_payload.get("avg_price"),
        "response_fill_size_shares": fill_payload.get("response_fill_size_shares")
        or fill_payload.get("taking_amount")
        or fill_payload.get("matched_shares")
        or fill_payload.get("size_matched"),
        "response_filled_size_usd": fill_payload.get("response_filled_size_usd")
        or fill_payload.get("making_amount")
        or fill_payload.get("filled_size_usd"),
    }


def _self_feed_by_tx(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in report.get("self_missing_ledger_rows") if isinstance(report.get("self_missing_ledger_rows"), list) else []:
        if not isinstance(row, dict):
            continue
        tx = _tx(row.get("tx"))
        item = row.get("self_feed") if isinstance(row.get("self_feed"), dict) else {}
        if tx:
            out[tx] = item
    return out


def _sample_rows(
    rows: list[dict[str, Any]],
    *,
    sample_size: int,
    seed: int,
    all_candidates: bool,
) -> list[tuple[str, dict[str, Any]]]:
    true_rows = [row for row in rows if row.get("classification") == "true_unrecorded_fill_candidate"]
    if all_candidates:
        return [("full_population", row) for row in true_rows]
    top = sorted(true_rows, key=lambda row: _num(row.get("cost_usd")), reverse=True)[: min(3, sample_size)]
    top_txs = {_tx(row.get("tx")) for row in top}
    remaining = [row for row in true_rows if _tx(row.get("tx")) not in top_txs]
    rng = random.Random(seed)
    random_rows = rng.sample(remaining, k=min(max(0, sample_size - len(top)), len(remaining)))
    selected = [("top_cost", row) for row in top] + [("seeded_random", row) for row in random_rows]
    return selected


def _matching_orders(
    order_traces: list[dict[str, Any]],
    row: dict[str, Any],
    self_item: dict[str, Any],
    *,
    time_tolerance_s: float,
) -> dict[str, list[dict[str, Any]]]:
    tx = _tx(row.get("tx"))
    condition_ids = {str(item) for item in (row.get("condition_ids") or self_item.get("condition_ids") or [])}
    outcomes = {_norm_outcome(item) for item in (row.get("outcomes") or self_item.get("outcomes") or [])}
    event_ts = _num(self_item.get("min_event_ts"), 0.0) or _num(self_item.get("max_event_ts"), 0.0)
    order_ids = {_tx(item) for item in (self_item.get("order_ids") or []) if _tx(item)}
    exact_tx: list[dict[str, Any]] = []
    exact_order_id: list[dict[str, Any]] = []
    near_condition: list[dict[str, Any]] = []
    same_condition: list[dict[str, Any]] = []
    for order in order_traces:
        if tx and tx in set(order.get("tx_hashes") or []):
            exact_tx.append(order)
        if order_ids and order_ids.intersection(set(order.get("order_ids") or [])):
            exact_order_id.append(order)
        condition_match = str(order.get("condition_id") or "") in condition_ids if condition_ids else False
        outcome_match = _norm_outcome(order.get("outcome")) in outcomes if outcomes else True
        if condition_match and outcome_match:
            same_condition.append(order)
            order_ts = _num(order.get("source_fill_block_ts"), 0.0) or _num(order.get("submitted_ts"), 0.0)
            if event_ts <= 0 or order_ts <= 0 or abs(order_ts - event_ts) <= time_tolerance_s:
                near_condition.append(order)
    return {
        "exact_tx": exact_tx,
        "exact_order_id": exact_order_id,
        "near_condition": near_condition,
        "same_condition": same_condition,
    }


def _match_quality(order: dict[str, Any], self_item: dict[str, Any]) -> dict[str, Any]:
    self_size = _num(self_item.get("size"), 0.0)
    self_price = _num(self_item.get("avg_price"), 0.0)
    self_cost = _num(self_item.get("cost_usd"), 0.0)
    order_size = _num(order.get("response_fill_size_shares"), 0.0)
    order_price = _num(order.get("response_fill_price"), 0.0)
    order_cost = _num(order.get("response_filled_size_usd"), 0.0)
    quantity_available = order_size > 0.0
    price_available = order_price > 0.0
    cost_available = order_cost > 0.0
    return {
        "order_id": order.get("order_id"),
        "order_quantities_unavailable": not quantity_available,
        "order_price_unavailable": not price_available,
        "order_cost_unavailable": not cost_available,
        "share_delta_abs": round(abs(self_size - order_size), 6) if self_size and quantity_available else None,
        "price_delta_abs": round(abs(self_price - order_price), 6) if self_price and price_available else None,
        "cost_delta_abs": round(abs(self_cost - order_cost), 6) if self_cost and cost_available else None,
        "self_size": self_size or None,
        "order_size": order_size or None,
        "self_price": self_price or None,
        "order_price": order_price or None,
        "self_cost_usd": self_cost or None,
        "order_cost_usd": order_cost or None,
    }


def _best_quality(orders: list[dict[str, Any]], self_item: dict[str, Any]) -> dict[str, Any]:
    qualities = [_match_quality(order, self_item) for order in orders]
    if not qualities:
        return {}
    return min(
        qualities,
        key=lambda item: (
            _num(item.get("share_delta_abs"), 1_000_000.0),
            _num(item.get("cost_delta_abs"), 1_000_000.0),
            _num(item.get("price_delta_abs"), 1_000_000.0),
        ),
    )


def _within_tolerance(quality: dict[str, Any], *, share_tolerance: float, price_tolerance: float) -> bool:
    if not quality:
        return False
    if quality.get("order_quantities_unavailable") or quality.get("order_price_unavailable"):
        return False
    share_delta = quality.get("share_delta_abs")
    price_delta = quality.get("price_delta_abs")
    share_ok = share_delta is None or _num(share_delta, 1_000_000.0) <= share_tolerance
    price_ok = price_delta is None or _num(price_delta, 1_000_000.0) <= price_tolerance
    return share_ok and price_ok


def _classify_trace(
    matches: dict[str, list[dict[str, Any]]],
    self_item: dict[str, Any],
    *,
    share_tolerance: float,
    price_tolerance: float,
) -> tuple[str, str, dict[str, Any]]:
    exact_guard = [row for row in matches["exact_tx"] + matches["exact_order_id"] if str(row.get("status") or "").upper() == "FILLED"]
    if exact_guard:
        return (
            "b1_guard_filled_ledger_write_or_join_missed",
            "guard_order_exact_tx_or_order_id_match",
            _best_quality(exact_guard, self_item),
        )
    guard_near = [row for row in matches["near_condition"] if str(row.get("status") or "").upper() == "FILLED"]
    if guard_near:
        quality = _best_quality(guard_near, self_item)
        if quality.get("order_quantities_unavailable") or quality.get("order_price_unavailable"):
            reason = "guard_filled_same_condition_outcome_near_time_order_quantities_unavailable"
        elif _within_tolerance(quality, share_tolerance=share_tolerance, price_tolerance=price_tolerance):
            reason = "guard_filled_same_condition_outcome_near_time_within_price_qty_tolerance"
        else:
            reason = "guard_filled_same_condition_outcome_near_time_amount_or_price_mismatch"
        return "b3_join_scope_artifact", reason, quality
    guard_same_condition = [
        row for row in matches["same_condition"] if str(row.get("status") or "").upper() == "FILLED"
    ]
    if guard_same_condition:
        return (
            "b3_join_scope_artifact",
            "guard_filled_same_condition_outcome_outside_time_tolerance",
            _best_quality(guard_same_condition, self_item),
        )
    sources = {str(item) for item in (self_item.get("sources") or [])}
    order_ids = [str(item) for item in (self_item.get("order_ids") or []) if str(item or "")]
    independent = bool(order_ids) or any("polygon" in source.lower() or "orderfilled" in source.lower() for source in sources)
    if independent:
        return "b2_non_guard_fill_candidate", "independent_self_feed_order_or_onchain_source_without_guard_trace", {}
    return "b3_join_scope_artifact", "data_api_only_self_feed_without_guard_or_onchain_proof", {}


def _scorecard_reconciliation(scorecard: dict[str, Any]) -> dict[str, float]:
    since = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    canonical = _num(since.get("canonical_pnl_usd") or chain.get("canonical_pnl_usd"), 0.0)
    actual = _num(
        since.get("actual_delta_vs_baseline_usd")
        or since.get("actual_account_delta_vs_baseline_usd")
        or chain.get("actual_delta_usd"),
        0.0,
    )
    spread = _num(chain.get("cash_delta_vs_expected_identity_usd"), canonical - actual)
    return {
        "canonical_pnl_usd": round(canonical, 6),
        "actual_delta_usd": round(actual, 6),
        "canonical_minus_actual_spread_usd": round(canonical - actual, 6),
        "cash_identity_delta_usd": round(spread, 6),
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    cash = _load_json(root / args.cash_ledger, {})
    self_feed = _load_json(root / args.self_feed_report, {})
    live_state = _load_json(root / args.live_state, {})
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(root / args.scorecard)
    rows = cash.get("rows") if isinstance(cash.get("rows"), list) else []
    self_by_tx = _self_feed_by_tx(self_feed)
    order_traces = [_order_trace(order) for order in (live_state.get("orders") or []) if isinstance(order, dict)]
    selected = _sample_rows(
        rows,
        sample_size=int(args.sample_size),
        seed=int(args.seed),
        all_candidates=bool(args.all_candidates),
    )
    sample_rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    pnl_by_class: Counter[str] = Counter()
    cost_by_class: Counter[str] = Counter()

    for sample_reason, row in selected:
        tx = _tx(row.get("tx"))
        self_item = self_by_tx.get(tx, {})
        matches = _matching_orders(order_traces, row, self_item, time_tolerance_s=float(args.time_tolerance_s))
        audit_class, audit_reason, match_quality = _classify_trace(
            matches,
            self_item,
            share_tolerance=float(args.share_tolerance),
            price_tolerance=float(args.price_tolerance),
        )
        cost = _num(row.get("cost_usd"), 0.0)
        pnl = _num(row.get("pnl_usd"), 0.0)
        counts[audit_class] += 1
        pnl_by_class[audit_class] += pnl
        cost_by_class[audit_class] += cost
        sample_rows.append(
            {
                "tx": tx,
                "sample_reason": sample_reason,
                "cash_ledger_classification": row.get("classification"),
                "audit_classification": audit_class,
                "audit_reason": audit_reason,
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
                "market_slugs": row.get("market_slugs") or [],
                "condition_ids": row.get("condition_ids") or [],
                "outcomes": row.get("outcomes") or [],
                "self_feed": {
                    "sources": self_item.get("sources") if isinstance(self_item.get("sources"), list) else [],
                    "order_ids": self_item.get("order_ids") if isinstance(self_item.get("order_ids"), list) else [],
                    "min_event_ts": self_item.get("min_event_ts"),
                    "max_event_ts": self_item.get("max_event_ts"),
                    "avg_price": self_item.get("avg_price"),
                    "size": self_item.get("size"),
                },
                "trace_counts": {key: len(value) for key, value in matches.items()},
                "best_match_quality": match_quality,
                "matched_guard_orders": {
                    key: [
                        {
                            "order_id": item.get("order_id"),
                            "intent_id": item.get("intent_id"),
                            "status": item.get("status"),
                            "submitted_at": item.get("submitted_at"),
                            "source_fill_block_ts": item.get("source_fill_block_ts"),
                            "response_fill_price": item.get("response_fill_price"),
                            "response_fill_size_shares": item.get("response_fill_size_shares"),
                            "response_filled_size_usd": item.get("response_filled_size_usd"),
                            "tx_hashes": item.get("tx_hashes"),
                        }
                        for item in value[:5]
                    ]
                    for key, value in matches.items()
                    if value
                },
            }
        )

    score_recon = _scorecard_reconciliation(scorecard)
    sample_b1 = round(pnl_by_class["b1_guard_filled_ledger_write_or_join_missed"], 6)
    sample_b2 = round(pnl_by_class["b2_non_guard_fill_candidate"], 6)
    b3_effect = 0.0
    actual = score_recon["actual_delta_usd"]
    unexplained = round(actual - (float(args.ruled_residual_usd) + sample_b1 + sample_b2 + b3_effect), 6)
    true_total_pnl = _num((cash.get("summary") or {}).get("pnl_by_class_usd", {}).get("true_unrecorded_fill_candidate"), 0.0)
    current_spread = score_recon["canonical_minus_actual_spread_usd"]
    spread_if_added = round(score_recon["canonical_pnl_usd"] + true_total_pnl - actual, 6)

    return {
        "schema_version": 1,
        "kind": "wallet_copy_guard_fill_recording_audit",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "paper_only": False,
        "live_orders_allowed": False,
        "inputs": {
            "cash_ledger": args.cash_ledger,
            "self_feed_report": args.self_feed_report,
            "live_state": args.live_state,
            "scorecard": args.scorecard,
        },
        "criteria": {
            "requested_sample_size": None if bool(args.all_candidates) else int(args.sample_size),
            "selected_rows": len(selected),
            "full_population": bool(args.all_candidates),
            "sample_size_basis": (
                "full_population_overrides_sample_size"
                if bool(args.all_candidates)
                else "bounded_sample"
            ),
            "sample_selection": (
                "all_true_unrecorded_fill_candidates"
                if bool(args.all_candidates)
                else "top_3_by_cost_plus_seeded_random_true_unrecorded_fill_candidates"
            ),
            "seed": None if bool(args.all_candidates) else int(args.seed),
            "time_tolerance_s": float(args.time_tolerance_s),
            "share_tolerance": float(args.share_tolerance),
            "price_tolerance": float(args.price_tolerance),
            "tolerance_basis": "exact_match_placeholder; use a wider tolerance only for optional duplicate-quality reruns",
            "b2_requires_independent_order_id_or_onchain_source": True,
        },
        "summary": {
            "true_candidate_population": sum(1 for row in rows if row.get("classification") == "true_unrecorded_fill_candidate"),
            "sample_size": len(sample_rows),
            "audit_classification_counts": dict(sorted(counts.items())),
            "sample_cost_by_audit_class_usd": {key: round(value, 6) for key, value in sorted(cost_by_class.items())},
            "sample_pnl_by_audit_class_usd": {key: round(value, 6) for key, value in sorted(pnl_by_class.items())},
            "b1_count": int(counts["b1_guard_filled_ledger_write_or_join_missed"]),
            "b2_count": int(counts["b2_non_guard_fill_candidate"]),
            "b3_count": int(counts["b3_join_scope_artifact"]),
            "immediate_notify_required": int(counts["b2_non_guard_fill_candidate"]) > 0,
            "audit_scope": "full_population" if bool(args.all_candidates) else "bounded_sample_not_census",
        },
        "reconciliation_equation": {
            "formula": "actual_delta = ruled_residual + sample_b1_effect + sample_b2_effect + b3_accounting_effect + unexplained",
            "actual_delta_usd": actual,
            "ruled_residual_usd": round(float(args.ruled_residual_usd), 6),
            "sample_b1_effect_usd": sample_b1,
            "sample_b2_effect_usd": sample_b2,
            "b3_accounting_effect_usd": b3_effect,
            "unexplained_usd": unexplained,
            "effect_scope": "full candidate population" if bool(args.all_candidates) else "sample effects only; full b1/b2 effect requires census or backfill",
        },
        "spread_identity": {
            **score_recon,
            "true_candidate_full_pnl_usd": round(true_total_pnl, 6),
            "spread_if_true_candidates_added_to_canonical_usd": spread_if_added,
            "spread_widening_if_added_usd": round(spread_if_added - current_spread, 6),
            "interpretation": (
                "positive self-feed-only candidate PnL would widen the canonical-vs-actual spread, "
                "so sampled Data API-only rows are treated as join-scope artifacts unless independent guard/onchain proof appears"
            ),
        },
        "rows": sample_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cash-ledger", default=DEFAULT_CASH_LEDGER)
    parser.add_argument("--self-feed-report", default=DEFAULT_SELF_FEED)
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--all-candidates", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--seed", type=int, default=20260707)
    parser.add_argument("--time-tolerance-s", type=float, default=180.0)
    parser.add_argument("--share-tolerance", type=float, default=0.000001)
    parser.add_argument("--price-tolerance", type=float, default=0.000001)
    parser.add_argument("--ruled-residual-usd", type=float, default=RULED_RESIDUAL_USD)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": args.output, "summary": report["summary"], "equation": report["reconciliation_equation"]}, sort_keys=True))


if __name__ == "__main__":
    main()
