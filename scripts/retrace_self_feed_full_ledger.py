#!/usr/bin/env python3
"""Retrace self-feed-only candidates against the full live ledger."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLASSIFICATION = "data/research/wallet_copy_recon_window_cash_ledger_latest.json"
DEFAULT_SELF_FEED = "data/research/wallet_copy_self_feed_vs_ledger_latest.json"
DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_self_feed_full_ledger_retrace_latest.json"
RULED_RESIDUAL_USD = -5.27


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _norm(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == "yes":
        return "up"
    if text == "no":
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
        return _num(text)


def _flatten(value: Any, needles: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key).lower()
            if any(needle in key_text for needle in needles):
                if isinstance(item, str):
                    found.append(item.strip().lower())
                elif isinstance(item, list):
                    found.extend(str(part).strip().lower() for part in item if str(part or ""))
            found.extend(_flatten(item, needles))
    elif isinstance(value, list):
        for item in value:
            found.extend(_flatten(item, needles))
    return [item for item in found if item.startswith("0x")]


def _last_live_fill(order: dict[str, Any]) -> dict[str, Any]:
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    for item in reversed(lifecycle):
        if isinstance(item, dict) and "FILLED" in str(item.get("status") or "").upper():
            payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
            return payload
    return {}


def _ledger_row(order: dict[str, Any]) -> dict[str, Any]:
    payload = _last_live_fill(order)
    latency = order.get("latency_budget") if isinstance(order.get("latency_budget"), dict) else {}
    cost = (
        _num(order.get("actual_trade_cost_usd"))
        or _num(order.get("response_filled_size_usd"))
        or _num(payload.get("response_filled_size_usd"))
        or _num(payload.get("filled_size_usd"))
        or _num(payload.get("making_amount"))
        or _num((payload.get("after") or {}).get("filled_size_usd") if isinstance(payload.get("after"), dict) else None)
    )
    shares = (
        _num(order.get("response_fill_size_shares"))
        or _num(payload.get("response_fill_size_shares"))
        or _num(payload.get("matched_shares"))
        or _num(payload.get("size_matched"))
        or _num((payload.get("after") or {}).get("size_matched") if isinstance(payload.get("after"), dict) else None)
        or _num(payload.get("taking_amount"))
        or _num(order.get("shares"))
    )
    price = (
        _num(payload.get("avg_price"))
        or _num(payload.get("response_fill_price"))
        or _num(order.get("limit_price"))
        or (cost / shares if shares else 0.0)
    )
    return {
        "order_id": str(order.get("order_id") or "").lower(),
        "intent_id": order.get("intent_id"),
        "status": str(order.get("final_status") or order.get("status") or "").upper(),
        "condition_id": str(order.get("condition_id") or "").lower(),
        "market_slug": str(order.get("market_slug") or ""),
        "outcome": _norm(order.get("outcome")),
        "submitted_at": order.get("submitted_at"),
        "event_ts": _num(latency.get("source_fill_block_ts")) or _parse_ts(order.get("submitted_at")),
        "cost_usd": round(cost, 6),
        "shares": round(shares, 6),
        "price": round(price, 6),
        "tx_hashes": sorted(set(_flatten(order, ("tx", "transaction")))),
    }


def _self_by_tx(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in report.get("self_missing_ledger_rows") if isinstance(report.get("self_missing_ledger_rows"), list) else []:
        if isinstance(row, dict) and row.get("tx"):
            out[str(row["tx"]).lower()] = row.get("self_feed") if isinstance(row.get("self_feed"), dict) else {}
    return out


def _event_ts(self_item: dict[str, Any]) -> float:
    return _num(self_item.get("min_event_ts")) or _num(self_item.get("max_event_ts"))


def _nearest(row: dict[str, Any], self_item: dict[str, Any], ledger_rows: list[dict[str, Any]]) -> dict[str, Any]:
    condition_ids = {str(item).lower() for item in (row.get("condition_ids") or self_item.get("condition_ids") or [])}
    outcomes = {_norm(item) for item in (row.get("outcomes") or self_item.get("outcomes") or [])}
    markets = {str(item) for item in (row.get("market_slugs") or self_item.get("market_slugs") or [])}
    row_cost = _num(row.get("cost_usd")) or _num(self_item.get("cost_usd"))
    row_shares = _num(self_item.get("size"))
    row_price = _num(self_item.get("avg_price")) or (row_cost / row_shares if row_shares else 0.0)
    event_ts = _event_ts(self_item)
    candidates: list[dict[str, Any]] = []
    for ledger in ledger_rows:
        if ledger["status"] != "FILLED":
            continue
        condition_match = ledger["condition_id"] in condition_ids if condition_ids else False
        market_match = ledger["market_slug"] in markets if markets else False
        if not (condition_match or market_match):
            continue
        outcome_match = ledger["outcome"] in outcomes if outcomes else True
        time_delta = abs(ledger["event_ts"] - event_ts) if ledger["event_ts"] and event_ts else 0.0
        share_delta = abs(ledger["shares"] - row_shares) if row_shares and ledger["shares"] else None
        price_delta = abs(ledger["price"] - row_price) if row_price and ledger["price"] else None
        cost_delta = abs(ledger["cost_usd"] - row_cost) if row_cost and ledger["cost_usd"] else None
        candidates.append(
            {
                **ledger,
                "condition_match": condition_match,
                "market_match": market_match,
                "outcome_match": outcome_match,
                "time_delta_s": round(time_delta, 6),
                "share_delta": round(share_delta, 6) if share_delta is not None else None,
                "price_delta": round(price_delta, 6) if price_delta is not None else None,
                "cost_delta_usd": round(cost_delta, 6) if cost_delta is not None else None,
            }
        )
    candidates.sort(
        key=lambda item: (
            not item["outcome_match"],
            item["time_delta_s"],
            item["cost_delta_usd"] if item["cost_delta_usd"] is not None else 999999.0,
        )
    )
    return candidates[0] if candidates else {}


def _has_guard_evidence(row: dict[str, Any], self_item: dict[str, Any], orders: list[dict[str, Any]], tolerance_s: float) -> bool:
    condition_ids = {str(item).lower() for item in (row.get("condition_ids") or self_item.get("condition_ids") or [])}
    markets = {str(item) for item in (row.get("market_slugs") or self_item.get("market_slugs") or [])}
    event_ts = _event_ts(self_item)
    for order in orders:
        condition_match = order["condition_id"] in condition_ids if condition_ids else False
        market_match = order["market_slug"] in markets if markets else False
        if not (condition_match or market_match):
            continue
        if not event_ts or not order["event_ts"] or abs(order["event_ts"] - event_ts) <= tolerance_s:
            return True
    return False


def _classify(row: dict[str, Any], self_item: dict[str, Any], nearest: dict[str, Any], guard_evidence: bool, args: argparse.Namespace) -> tuple[str, str]:
    if nearest:
        if not nearest["outcome_match"]:
            return "b3_opposite_side_or_merge_artifact", "full_ledger_same_market_or_condition_opposite_outcome"
        share_ok = nearest["share_delta"] is None or nearest["share_delta"] <= float(args.share_tolerance)
        price_ok = nearest["price_delta"] is None or nearest["price_delta"] <= float(args.price_tolerance)
        time_ok = nearest["time_delta_s"] <= float(args.time_tolerance_s)
        if share_ok and price_ok and time_ok:
            return "b3_duplicate_full_ledger_match", "full_ledger_condition_outcome_time_size_price_match"
        return "b3_join_scope_artifact_size_price_or_time_mismatch", "full_ledger_condition_outcome_match_with_size_price_or_time_delta"
    independent = bool(self_item.get("order_ids")) or any("polygon" in str(src).lower() for src in self_item.get("sources", []))
    if guard_evidence:
        return "b1_confirmed_guard_evidence_no_full_ledger_fill", "guard_market_window_evidence_present_but_no_full_ledger_fill_match"
    if independent:
        return "b2_suspect_non_guard_fill", "independent_onchain_or_order_id_source_without_guard_evidence"
    return "b3_data_api_only_no_guard_or_onchain_proof", "data_api_only_candidate_without_guard_or_onchain_proof"


def _first_num(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    classification = _load_json(root / args.classification, {})
    self_feed = _load_json(root / args.self_feed_report, {})
    live_state = _load_json(root / args.live_state, {})
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(root / args.scorecard)
    rows = [
        row for row in classification.get("rows", [])
        if isinstance(row, dict) and row.get("classification") == "true_unrecorded_fill_candidate"
    ]
    self_by_tx = _self_by_tx(self_feed)
    ledger_rows = [_ledger_row(order) for order in live_state.get("orders", []) if isinstance(order, dict)]
    counts: Counter[str] = Counter()
    pnl_by_class: Counter[str] = Counter()
    cost_by_class: Counter[str] = Counter()
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        tx = str(row.get("tx") or "").lower()
        self_item = self_by_tx.get(tx, {})
        nearest = _nearest(row, self_item, ledger_rows)
        guard_evidence = _has_guard_evidence(row, self_item, ledger_rows, float(args.time_tolerance_s))
        klass, reason = _classify(row, self_item, nearest, guard_evidence, args)
        counts[klass] += 1
        cost_by_class[klass] += _num(row.get("cost_usd"))
        pnl_by_class[klass] += _num(row.get("pnl_usd"))
        out_rows.append(
            {
                "tx": tx,
                "classification": klass,
                "reason": reason,
                "cost_usd": row.get("cost_usd"),
                "pnl_usd": row.get("pnl_usd"),
                "market_slugs": row.get("market_slugs") or [],
                "condition_ids": row.get("condition_ids") or [],
                "outcomes": row.get("outcomes") or [],
                "self_feed": {
                    "sources": self_item.get("sources") or [],
                    "order_ids": self_item.get("order_ids") or [],
                    "avg_price": self_item.get("avg_price"),
                    "size": self_item.get("size"),
                    "event_ts": _event_ts(self_item),
                },
                "guard_evidence_same_window": guard_evidence,
                "nearest_full_ledger_fill": nearest,
            }
        )
    since_topup = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    actual = _first_num(
        since_topup.get("actual_delta_vs_baseline_usd"),
        since_topup.get("actual_account_delta_vs_baseline_usd"),
        chain.get("actual_delta_usd"),
    )
    b1_effect = pnl_by_class["b1_confirmed_guard_evidence_no_full_ledger_fill"]
    b2_effect = pnl_by_class["b2_suspect_non_guard_fill"]
    duplicate_effect = 0.0
    overlay = (
        since_topup.get("self_feed_reconciliation_overlay")
        if isinstance(since_topup.get("self_feed_reconciliation_overlay"), dict)
        else {}
    )
    cash_residual = (
        since_topup.get("cash_diff_reconciliation_residual")
        if isinstance(since_topup.get("cash_diff_reconciliation_residual"), dict)
        else scorecard.get("cash_diff_reconciliation_residual")
        if isinstance(scorecard.get("cash_diff_reconciliation_residual"), dict)
        else {}
    )
    b3_join_scope_effect = _num(overlay.get("overlay_delta_usd"))
    account_value_residual = _num(cash_residual.get("residual_usd"))
    unexplained = round(
        actual
        - float(args.ruled_residual_usd)
        - b1_effect
        - b2_effect
        - duplicate_effect
        - b3_join_scope_effect
        - account_value_residual,
        6,
    )
    equation_status = "RECONCILED" if abs(unexplained) < 1.0 else "MISMATCH"
    return {
        "kind": "wallet_copy_self_feed_full_ledger_retrace",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "inputs": {
            "classification": args.classification,
            "self_feed_report": args.self_feed_report,
            "live_state": args.live_state,
            "scorecard": args.scorecard,
            "time_tolerance_s": float(args.time_tolerance_s),
            "share_tolerance": float(args.share_tolerance),
            "price_tolerance": float(args.price_tolerance),
        },
        "summary": {
            "candidate_total": len(rows),
            "class_counts": dict(sorted(counts.items())),
            "cost_by_class_usd": {key: round(value, 6) for key, value in sorted(cost_by_class.items())},
            "pnl_by_class_usd": {key: round(value, 6) for key, value in sorted(pnl_by_class.items())},
            "b1_confirmed_count": int(counts["b1_confirmed_guard_evidence_no_full_ledger_fill"]),
            "b2_suspect_count": int(counts["b2_suspect_non_guard_fill"]),
            "immediate_notify_required": int(counts["b2_suspect_non_guard_fill"]) > 0,
        },
        "reconciliation_equation": {
            "formula": "actual_delta = ruled_residual + confirmed_b1_effect + confirmed_b2_effect + duplicate_effect + b3_join_scope_effect + account_value_residual + unexplained",
            "status": equation_status,
            "actual_delta_usd": round(actual, 6),
            "ruled_residual_usd": round(float(args.ruled_residual_usd), 6),
            "confirmed_b1_effect_usd": round(b1_effect, 6),
            "confirmed_b2_effect_usd": round(b2_effect, 6),
            "duplicate_effect_usd": duplicate_effect,
            "b3_join_scope_effect_usd": round(b3_join_scope_effect, 6),
            "account_value_residual_usd": round(account_value_residual, 6),
            "unexplained_usd": unexplained,
            "target_abs_unexplained_lt_usd": 1.0,
        },
        "backfill_gate": {
            "allowed": int(counts["b1_confirmed_guard_evidence_no_full_ledger_fill"]) > 0,
            "rule": "backfill only confirmed b1 groups; never backfill duplicate or candidate-level rows",
        },
        "rows": out_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--classification", default=DEFAULT_CLASSIFICATION)
    parser.add_argument("--self-feed-report", default=DEFAULT_SELF_FEED)
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--time-tolerance-s", type=float, default=300.0)
    parser.add_argument("--share-tolerance", type=float, default=0.75)
    parser.add_argument("--price-tolerance", type=float, default=0.03)
    parser.add_argument("--ruled-residual-usd", type=float, default=RULED_RESIDUAL_USD)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": args.output, "summary": report["summary"], "equation": report["reconciliation_equation"]}, sort_keys=True))


if __name__ == "__main__":
    main()
