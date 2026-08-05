#!/usr/bin/env python3
"""Backfill exact actual trade costs onto today's live ledger fills.

Flow stage: LIVE/SELF-DEV. This is intentionally conservative: Data API trade
records do not expose our CLOB order id, so only one-order transaction groups
are annotated as exact actual cost. Missing-tx fills are counted for the
tx-hash coverage defect and left on fallback accounting.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import report_today_fill_cash_diff as fill_diff
try:
    from py_clob_client_v2.clob_types import TradeParams
except ImportError:  # pragma: no cover - depends on operator env package name
    try:
        from py_clob_client.clob_types import TradeParams
    except ImportError:  # pragma: no cover - unit-test/mock environment
        class TradeParams:  # type: ignore[no-redef]
            def __init__(self, **kwargs: Any) -> None:
                self.__dict__.update(kwargs)

from src.config import Config
from src.trade_executor import TradeExecutor
from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.pnl_truth import order_intended_cost, order_shares
from src.wallet_copy.store import atomic_write_json, json_file_lock, load_json


DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_actual_trade_cost_backfill_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="", help="UTC day YYYY-MM-DD; default is today UTC.")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--no-write", action="store_true", help="Build the report without mutating the ledger.")
    return parser.parse_args()


def _is_day_fill(order: dict[str, Any], *, start_ts: float, end_ts: float) -> bool:
    status = str(order.get("final_status") or order.get("status") or "").upper()
    ts = fill_diff._parse_ts(order.get("submitted_at") or order.get("updated_at"))
    return status == "FILLED" and start_ts <= ts < end_ts


def _associated_trade_ids(order: dict[str, Any]) -> list[str]:
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    cancel = result.get("wallet_copy_maker_cancel") if isinstance(result.get("wallet_copy_maker_cancel"), dict) else {}
    values: list[Any] = []
    containers = [result, cancel]
    for key in ("before", "after"):
        status = cancel.get(key)
        if isinstance(status, dict):
            containers.append(status)
            raw = status.get("raw")
            if isinstance(raw, dict):
                containers.append(raw)
    for container in containers:
        raw = container.get("associate_trades") if isinstance(container, dict) else None
        if isinstance(raw, list):
            values.extend(raw)
        elif raw:
            values.append(raw)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _actual_cost_from_clob_trade(order_id: str, trade: dict[str, Any]) -> dict[str, Any] | None:
    order_id_l = str(order_id or "").lower()
    maker_orders = trade.get("maker_orders") if isinstance(trade.get("maker_orders"), list) else []
    for maker_order in maker_orders:
        if not isinstance(maker_order, dict):
            continue
        if str(maker_order.get("order_id") or "").lower() != order_id_l:
            continue
        shares = num(maker_order.get("matched_amount"), 0.0)
        price = num(maker_order.get("price"), 0.0)
        if shares <= 0 or price <= 0:
            return None
        tx = str(trade.get("transaction_hash") or trade.get("transactionHash") or "").strip().lower()
        return {
            "actual_cost_usd": round(shares * price, 6),
            "actual_size_shares": round(shares, 6),
            "matched_shares": round(shares, 6),
            "price": round(price, 6),
            "tx": tx,
            "trade_id": str(trade.get("id") or ""),
            "source": "clob_associate_trade_by_order_id",
        }
    return None


async def fetch_clob_associated_trade_costs(orders: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    wanted: dict[str, list[str]] = {}
    for order in orders:
        order_id = str(order.get("order_id") or "").lower()
        trade_ids = _associated_trade_ids(order)
        if order_id and trade_ids:
            wanted[order_id] = trade_ids
    if not wanted:
        return {}, {"status": "NO_ASSOCIATED_TRADES", "orders_with_associate_trade_ids": 0}
    executor = TradeExecutor(Config())
    await executor.initialize()
    out: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    fetched_trade_ids = 0
    for order_id, trade_ids in sorted(wanted.items()):
        for trade_id in trade_ids:
            try:
                rows = executor.client.get_trades(TradeParams(id=trade_id), only_first_page=True)
                fetched_trade_ids += 1
            except Exception as exc:  # pragma: no cover - live CLOB edge
                errors.append({"trade_id": trade_id, "error": str(exc)})
                continue
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                actual = _actual_cost_from_clob_trade(order_id, row)
                if actual:
                    out[order_id] = actual
                    break
            if order_id in out:
                break
    return out, {
        "status": "OK" if not errors else "PARTIAL",
        "orders_with_associate_trade_ids": len(wanted),
        "fetched_trade_ids": fetched_trade_ids,
        "matched_order_costs": len(out),
        "errors": errors[:10],
    }


ACTUAL_COST_FIELDS = (
    "actual_trade_cost_usd",
    "actual_trade_cost_source",
    "actual_trade_cost_key",
    "actual_trade_cost_trade_id",
    "actual_trade_cost_updated_at",
    "actual_trade_cost_normalization",
    "actual_trade_record_size_shares",
    "intended_cost_usd",
    "price_improvement_usd",
    "actual_trade_cost_rejected_reason",
)


def _clear_actual_cost_fields(order: dict[str, Any]) -> None:
    for key in ACTUAL_COST_FIELDS:
        order.pop(key, None)
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for key in ACTUAL_COST_FIELDS:
        result.pop(key, None)
    if result:
        order["trade_result"] = result


def _normalized_actual_cost_payload(
    order: dict[str, Any],
    actual: dict[str, Any],
    *,
    tx: str,
    updated_at: str,
    source: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    intended = order_intended_cost(order)
    fill_shares = order_shares(order)
    actual_cost = num(actual.get("actual_cost_usd"), 0.0)
    actual_size = num(actual.get("actual_size_shares") or actual.get("matched_shares"), 0.0)
    if actual_cost <= 0:
        return None, {"reason": "actual_cost_missing_or_zero", "intended_cost_usd": round(intended, 6)}
    normalized_cost = actual_cost
    normalization = "exact_trade_size"
    if actual_size > 0 and fill_shares > 0 and actual_size + 1e-6 < fill_shares:
        normalized_cost = round((actual_cost / actual_size) * fill_shares, 6)
        normalization = "scaled_partial_trade_price_to_fill_size"
    elif actual_size <= 0 or fill_shares <= 0:
        normalization = "size_unavailable"
    improvement = round(intended - normalized_cost, 6) if intended > 0 else 0.0
    if intended > 0 and (normalization == "size_unavailable" or improvement > 0.5 * intended):
        return None, {
            "reason": "suspect_actual_improvement_or_missing_size",
            "intended_cost_usd": round(intended, 6),
            "raw_actual_cost_usd": round(actual_cost, 6),
            "normalized_actual_cost_usd": round(normalized_cost, 6),
            "fill_shares": round(fill_shares, 6),
            "actual_size_shares": round(actual_size, 6),
            "price_improvement_usd": improvement,
        }
    return (
        {
            "actual_trade_cost_usd": round(normalized_cost, 6),
            "actual_trade_cost_source": source,
            "actual_trade_cost_key": tx or str(actual.get("trade_id") or ""),
            "actual_trade_cost_trade_id": str(actual.get("trade_id") or ""),
            "actual_trade_cost_updated_at": updated_at,
            "actual_trade_cost_normalization": normalization,
            "actual_trade_record_size_shares": round(actual_size, 6),
            "intended_cost_usd": round(intended, 6),
            "price_improvement_usd": improvement,
        },
        {
            "reason": "",
            "intended_cost_usd": round(intended, 6),
            "raw_actual_cost_usd": round(actual_cost, 6),
            "normalized_actual_cost_usd": round(normalized_cost, 6),
            "fill_shares": round(fill_shares, 6),
            "actual_size_shares": round(actual_size, 6),
            "normalization": normalization,
        },
    )


def _mark_rejected_actual(order: dict[str, Any], rejection: dict[str, Any], *, updated_at: str) -> None:
    payload = {
        "actual_trade_cost_rejected_reason": str(rejection.get("reason") or "suspect_actual_cost"),
        "actual_trade_cost_updated_at": updated_at,
        "intended_cost_usd": rejection.get("intended_cost_usd"),
        "price_improvement_usd": 0.0,
    }
    order.update(payload)
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    result.update(payload)
    order["trade_result"] = result


def annotate_actual_trade_costs(
    ledger: dict[str, Any],
    actual_by_tx: dict[str, dict[str, Any]],
    *,
    start_ts: float,
    end_ts: float,
    updated_at: str,
    actual_by_order_id: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    orders = [row for row in ledger.get("orders") or [] if isinstance(row, dict)]
    day_fills = [row for row in orders if _is_day_fill(row, start_ts=start_ts, end_ts=end_ts)]
    for order in day_fills:
        _clear_actual_cost_fields(order)
    by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_tx: list[dict[str, Any]] = []
    for order in day_fills:
        tx = fill_diff._order_tx(order)
        if tx:
            by_tx[tx].append(order)
        else:
            missing_tx.append(order)

    annotated_rows: list[dict[str, Any]] = []
    ambiguous_tx_groups = 0
    no_trade_record_tx_groups = 0
    annotated_order_ids: set[str] = set()
    partial_scaled_annotations = 0
    annotation_rejected_suspect = 0
    rejected_rows: list[dict[str, Any]] = []
    for tx, tx_orders in sorted(by_tx.items()):
        actual = actual_by_tx.get(tx)
        if not actual:
            no_trade_record_tx_groups += 1
            continue
        if len(tx_orders) != 1:
            ambiguous_tx_groups += 1
            continue
        order = tx_orders[0]
        payload, normalization = _normalized_actual_cost_payload(
            order,
            actual,
            tx=tx,
            updated_at=updated_at,
            source="data_api_trades_by_tx_hash",
        )
        if payload is None:
            annotation_rejected_suspect += 1
            _mark_rejected_actual(order, normalization, updated_at=updated_at)
            rejected_rows.append({"order_id": str(order.get("order_id") or ""), "tx": tx, **normalization})
            continue
        if payload["actual_trade_cost_normalization"] == "scaled_partial_trade_price_to_fill_size":
            partial_scaled_annotations += 1
        order.update(payload)
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        result.update(payload)
        order["trade_result"] = result
        annotated_order_ids.add(str(order.get("order_id") or "").lower())
        annotated_rows.append(
            {
                "order_id": str(order.get("order_id") or ""),
                "tx": tx,
                "market_slug": str(order.get("market_slug") or ""),
                "intended_cost_usd": payload["intended_cost_usd"],
                "actual_trade_cost_usd": payload["actual_trade_cost_usd"],
                "price_improvement_usd": payload["price_improvement_usd"],
                "normalization": payload["actual_trade_cost_normalization"],
            }
        )

    clob_order_annotations = 0
    for order in day_fills:
        order_id = str(order.get("order_id") or "").lower()
        if not order_id or order_id in annotated_order_ids:
            continue
        actual = (actual_by_order_id or {}).get(order_id)
        if not actual:
            continue
        tx = str(actual.get("tx") or "").lower()
        payload, normalization = _normalized_actual_cost_payload(
            order,
            actual,
            tx=tx,
            updated_at=updated_at,
            source=str(actual.get("source") or "clob_associate_trade_by_order_id"),
        )
        if payload is None:
            annotation_rejected_suspect += 1
            _mark_rejected_actual(order, normalization, updated_at=updated_at)
            rejected_rows.append({"order_id": str(order.get("order_id") or ""), "tx": tx, **normalization})
            continue
        if payload["actual_trade_cost_normalization"] == "scaled_partial_trade_price_to_fill_size":
            partial_scaled_annotations += 1
        order.update(payload)
        if tx:
            order["transaction_hash"] = tx
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        result.update(payload)
        if tx:
            existing = result.get("tx_hashes") if isinstance(result.get("tx_hashes"), list) else []
            result["tx_hashes"] = list(dict.fromkeys([*existing, tx]))
        order["trade_result"] = result
        annotated_order_ids.add(order_id)
        clob_order_annotations += 1
        annotated_rows.append(
            {
                "order_id": str(order.get("order_id") or ""),
                "tx": tx,
                "market_slug": str(order.get("market_slug") or ""),
                "intended_cost_usd": payload["intended_cost_usd"],
                "actual_trade_cost_usd": payload["actual_trade_cost_usd"],
                "price_improvement_usd": payload["price_improvement_usd"],
                "source": payload["actual_trade_cost_source"],
                "normalization": payload["actual_trade_cost_normalization"],
            }
        )

    refreshed_fills_with_tx = 0
    for order in day_fills:
        if fill_diff._order_tx(order):
            refreshed_fills_with_tx += 1
    fills_with_tx = sum(len(rows) for rows in by_tx.values())
    summary = {
        "flow_stage": "LIVE/SELF-DEV",
        "updated_at": updated_at,
        "fills_total": len(day_fills),
        "fills_with_tx_before_backfill": fills_with_tx,
        "fills_with_tx": refreshed_fills_with_tx,
        "fills_missing_tx": len(day_fills) - refreshed_fills_with_tx,
        "fill_tx_coverage_pct": round(100.0 * refreshed_fills_with_tx / len(day_fills), 6) if day_fills else 0.0,
        "actual_cost_annotated_fills": len(annotated_rows),
        "clob_order_id_annotated_fills": clob_order_annotations,
        "partial_scaled_annotations": partial_scaled_annotations,
        "annotation_rejected_suspect": annotation_rejected_suspect,
        "ambiguous_tx_groups": ambiguous_tx_groups,
        "no_trade_record_tx_groups": no_trade_record_tx_groups,
        "sum_price_improvement_usd": round(sum(num(row.get("price_improvement_usd"), 0.0) for row in annotated_rows), 6),
        "classification": "exact_actual_cost_for_single_order_tx_groups; missing_tx_fills_left_on_fallback_cost",
    }
    ledger["actual_trade_cost_backfill"] = summary
    return {"summary": summary, "rows": annotated_rows, "rejected_rows": rejected_rows[:50]}


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    day_start, day_end = fill_diff._day_window(str(args.day or ""))
    user = fill_diff._load_dotenv_value("POLYMARKET_PROXY")
    trades, trade_fetch = fill_diff._fetch_data_api_trades(
        user=user,
        day_start_ts=day_start.timestamp(),
        day_end_ts=day_end.timestamp(),
        limit=int(args.limit),
        max_pages=int(args.max_pages),
        timeout_s=float(args.timeout_s),
    )
    actual_by_tx = fill_diff._actual_trade_summary(trades, {})
    ledger_preview = load_json(args.ledger, default={})
    ledger_preview = ledger_preview if isinstance(ledger_preview, dict) else {}
    day_orders_preview = [
        row
        for row in ledger_preview.get("orders") or []
        if isinstance(row, dict) and _is_day_fill(row, start_ts=day_start.timestamp(), end_ts=day_end.timestamp())
    ]
    actual_by_order_id, clob_fetch = asyncio.run(fetch_clob_associated_trade_costs(day_orders_preview))
    updated_at = utc_now_iso()
    if args.no_write:
        ledger = ledger_preview
        backfill = annotate_actual_trade_costs(
            ledger,
            actual_by_tx,
            start_ts=day_start.timestamp(),
            end_ts=day_end.timestamp(),
            updated_at=updated_at,
            actual_by_order_id=actual_by_order_id,
        )
    else:
        with json_file_lock(args.ledger):
            ledger = load_json(args.ledger, default={})
            ledger = ledger if isinstance(ledger, dict) else {}
            backfill = annotate_actual_trade_costs(
                ledger,
                actual_by_tx,
                start_ts=day_start.timestamp(),
                end_ts=day_end.timestamp(),
                updated_at=updated_at,
                actual_by_order_id=actual_by_order_id,
            )
            atomic_write_json(args.ledger, ledger)
    return {
        "generated_at": updated_at,
        "kind": "wallet_copy_actual_trade_cost_backfill",
        "flow_stage": "LIVE/SELF-DEV",
        "day_utc": day_start.date().isoformat(),
        "inputs": {
            "ledger": str(args.ledger),
            "trade_source": "https://data-api.polymarket.com/trades",
            "write_ledger": not bool(args.no_write),
        },
        "trade_fetch": trade_fetch,
        "clob_associated_trade_fetch": clob_fetch,
        **backfill,
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
