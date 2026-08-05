#!/usr/bin/env python3
"""Reconcile pending embedded fees from Polygon pUSD receipt debits.

Writes a sidecar instead of rewriting the live guard-owned ledger.  It also
extends the canonical receipt-cost artifact consumed by daily_scorecard.py.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.fees import POLYMARKET_EMBEDDED_FEE_RATE, expected_polymarket_buy_fee_usd
from src.wallet_copy.models import parse_ts
from src.wallet_copy.pnl_truth import order_response_cost, order_shares


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
PUSD_TOKEN = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
DEFAULT_RPC = "https://polygon.drpc.org"
DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_REPORT = "data/research/wallet_copy_realized_fee_receipts_latest.json"
DEFAULT_COSTS = "data/research/wallet_copy_cash_flow_replay_tx_match_realized_fee_latest.json"
LEGACY_COSTS = "data/research/wallet_copy_cash_flow_replay_tx_match_2026-07-06T1838Z.json"


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tx_hashes(order: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    trade = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    values.extend(trade.get("tx_hashes") or [])
    details = trade.get("details") if isinstance(trade.get("details"), dict) else {}
    values.extend(details.get("transactionsHashes") or [])
    for event in order.get("lifecycle") or []:
        if not isinstance(event, dict):
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        event_details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
        values.extend(event_details.get("transactionsHashes") or [])
    out: list[str] = []
    for value in values:
        text = str(value or "").lower()
        if text.startswith("0x") and len(text) == 66 and text not in out:
            out.append(text)
    return out


def _pusd_debits(receipt: dict[str, Any]) -> dict[str, float]:
    by_sender: dict[str, int] = {}
    for log in receipt.get("logs") or []:
        if not isinstance(log, dict) or str(log.get("address") or "").lower() != PUSD_TOKEN:
            continue
        topics = log.get("topics") if isinstance(log.get("topics"), list) else []
        if len(topics) < 3 or str(topics[0]).lower() != TRANSFER_TOPIC:
            continue
        sender_topic = str(topics[1] or "").lower().removeprefix("0x")
        if len(sender_topic) != 64:
            continue
        sender = "0x" + sender_topic[-40:]
        try:
            amount = int(str(log.get("data") or "0x0"), 16)
        except ValueError:
            continue
        by_sender[sender] = by_sender.get(sender, 0) + amount
    return {sender: amount / 1_000_000 for sender, amount in by_sender.items() if amount > 0}


def _receipt_cost(receipt: dict[str, Any], *, expected_total_usd: float, tolerance_usd: float = 0.01) -> tuple[float, str]:
    if str(receipt.get("status") or "").lower() not in {"0x1", "1"}:
        raise ValueError("receipt status is not successful")
    candidates = _pusd_debits(receipt)
    if not candidates:
        raise ValueError("receipt has no pUSD debit")
    sender, amount = min(candidates.items(), key=lambda item: (abs(item[1] - expected_total_usd), item[0]))
    if abs(amount - expected_total_usd) > tolerance_usd:
        raise ValueError("no pUSD debit matches expected total within tolerance")
    return amount, sender


def _rpc_receipt(rpc_url: str, tx_hash: str, *, timeout_s: float = 20.0) -> dict[str, Any]:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": [tx_hash]}).encode()
    request = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "polymarket-agent-receipt-reconciler/1"},
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        body = json.loads(response.read())
    receipt = body.get("result") if isinstance(body, dict) else None
    if not isinstance(receipt, dict):
        raise ValueError(f"missing receipt for {tx_hash}")
    return receipt


def _rpc_receipts_batch(
    rpc_url: str, tx_hashes: list[str], *, timeout_s: float = 30.0, chunk_size: int = 75
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Fetch receipts in JSON-RPC batches; return successes and named gaps."""

    receipts: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    for offset in range(0, len(tx_hashes), max(1, chunk_size)):
        chunk = tx_hashes[offset : offset + max(1, chunk_size)]
        payload = [
            {"jsonrpc": "2.0", "id": idx, "method": "eth_getTransactionReceipt", "params": [tx]}
            for idx, tx in enumerate(chunk)
        ]
        request = urllib.request.Request(
            rpc_url,
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "polymarket-agent-receipt-reconciler/2",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                body = json.loads(response.read())
        except Exception as exc:  # network coverage is published, never hidden
            for tx in chunk:
                errors[tx] = f"batch_fetch_error:{type(exc).__name__}:{exc}"
            continue
        by_id = {int(row.get("id")): row for row in body if isinstance(row, dict)} if isinstance(body, list) else {}
        for idx, tx in enumerate(chunk):
            row = by_id.get(idx) or {}
            receipt = row.get("result")
            if isinstance(receipt, dict):
                receipts[tx] = receipt
            else:
                errors[tx] = f"missing_receipt:{row.get('error') or 'null_result'}"
    return receipts, errors


def reconcile_fee_coverage(
    ledger: dict[str, Any],
    *,
    receipts: dict[str, dict[str, Any]],
    receipt_errors: dict[str, str],
    start_iso: str,
) -> list[dict[str, Any]]:
    """Measure receipt fees without interpolating across uncovered fills."""

    start_ts = parse_ts(start_iso)
    rows: list[dict[str, Any]] = []
    for index, order in enumerate(ledger.get("orders") or []):
        if not isinstance(order, dict):
            continue
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        submitted_ts = parse_ts(order.get("submitted_at"))
        if start_ts is not None and (submitted_ts is None or submitted_ts < start_ts):
            continue
        response_cost = order_response_cost(order)
        shares = order_shares(order)
        price = response_cost / shares if response_cost > 0.0 and shares > 0.0 else float(order.get("limit_price") or 0.0)
        expected_fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        tx_hashes = _tx_hashes(order)
        base = {
            "order_index": index,
            "order_id": str(order.get("order_id") or "").lower(),
            "submitted_at": order.get("submitted_at"),
            "market_slug": order.get("market_slug"),
            "response_cost_usd": round(response_cost, 6),
            "shares": round(shares, 6),
            "price": round(price, 6),
            "expected_fee_usd": round(expected_fee, 6),
        }
        if response_cost <= 0.0 or shares <= 0.0 or not (0.0 < price < 1.0):
            rows.append({**base, "status": "COVERAGE_GAP_MISSING_RESPONSE_FILL_BASIS"})
            continue
        if len(tx_hashes) != 1:
            rows.append({**base, "status": "COVERAGE_GAP_TX_HASH_COUNT", "tx_hash_count": len(tx_hashes)})
            continue
        tx = tx_hashes[0]
        receipt = receipts.get(tx)
        if receipt is None:
            rows.append({**base, "tx": tx, "status": "COVERAGE_GAP_RECEIPT", "reason": receipt_errors.get(tx)})
            continue
        debits = _pusd_debits(receipt)
        if not debits:
            rows.append({**base, "tx": tx, "status": "COVERAGE_GAP_NO_PUSD_DEBIT"})
            continue
        _sender, total_cost = min(
            debits.items(), key=lambda item: (abs(item[1] - response_cost - expected_fee), item[0])
        )
        realized_fee = total_cost - response_cost
        denominator = shares * price * (1.0 - price)
        fitted_rate = realized_fee / denominator if denominator > 0.0 else None
        rows.append(
            {
                **base,
                "tx": tx,
                "receipt_total_cost_usd": round(total_cost, 6),
                "realized_fee_usd": round(realized_fee, 6),
                "realized_minus_expected_fee_usd": round(realized_fee - expected_fee, 6),
                "receipt_measured_fee_rate": round(fitted_rate, 9) if fitted_rate is not None else None,
                "status": "PASS_RECEIPT_MEASURED",
                "realized_fee_source": "tx_receipt_pusd_debit",
            }
        )
    return rows


def reconcile_pending_fees(
    ledger: dict[str, Any],
    *,
    receipt_loader,
    order_indices: set[int] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, order in enumerate(ledger.get("orders") or []):
        if not isinstance(order, dict) or (order_indices is not None and index not in order_indices):
            continue
        comparison = order.get("expected_vs_realized_fee") if isinstance(order.get("expected_vs_realized_fee"), dict) else {}
        if comparison.get("realized_fee_usd") is not None or str(comparison.get("status") or "") != "PENDING_RECEIPT":
            continue
        response_cost = _num(comparison.get("response_cost_usd"))
        expected_fee = _num(comparison.get("response_expected_fee_usd"))
        tx_hashes = _tx_hashes(order)
        if response_cost is None or expected_fee is None or len(tx_hashes) != 1:
            continue
        tx_hash = tx_hashes[0]
        receipt_cost, _sender = _receipt_cost(
            receipt_loader(tx_hash),
            expected_total_usd=response_cost + expected_fee,
        )
        realized_fee = receipt_cost - response_cost
        delta = realized_fee - expected_fee
        rows.append(
            {
                "order_index": index,
                "order_id": str(order.get("order_id") or "").lower(),
                "tx": tx_hash,
                "submitted_at": order.get("submitted_at"),
                "market_slug": order.get("market_slug"),
                "response_cost_usd": round(response_cost, 6),
                "receipt_total_cost_usd": round(receipt_cost, 6),
                "realized_fee_usd": round(realized_fee, 6),
                "response_expected_fee_usd": round(expected_fee, 6),
                "realized_minus_expected_fee_usd": round(delta, 6),
                "status": "PASS_RECEIPT_RECONCILED" if abs(delta) < 0.01 else "REVIEW_FEE_DELTA",
                "realized_fee_source": "tx_receipt_pusd_debit",
            }
        )
    return rows


def _merge_cost_rows(existing: dict[str, Any], reconciled: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in existing.get("rows") or [] if isinstance(row, dict)]
    by_tx = {str(row.get("tx") or "").lower(): dict(row) for row in rows if str(row.get("tx") or "")}
    for row in reconciled:
        by_tx[row["tx"]] = {
            "ledger_cost": row["response_cost_usd"],
            "market_slug": row.get("market_slug"),
            "orders": 1,
            "out_minus_cost": row["realized_fee_usd"],
            "pUSD_in": 0.0,
            "pUSD_out": row["receipt_total_cost_usd"],
            "sample_order": row["order_id"],
            "submitted_at": row.get("submitted_at"),
            "tx": row["tx"],
        }
    merged = sorted(by_tx.values(), key=lambda row: (str(row.get("submitted_at") or ""), str(row.get("tx") or "")))
    return {
        "rows": merged,
        "summary": {
            "tx_count": len(merged),
            "pUSD_out_sum": round(sum(float(row.get("pUSD_out") or 0) for row in merged), 6),
            "ledger_cost_sum": round(sum(float(row.get("ledger_cost") or 0) for row in merged), 6),
            "out_minus_cost_sum": round(sum(float(row.get("out_minus_cost") or 0) for row in merged), 6),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--rpc-url", default=DEFAULT_RPC)
    parser.add_argument("--output", default=DEFAULT_REPORT)
    parser.add_argument("--receipt-costs-output", default=DEFAULT_COSTS)
    parser.add_argument("--base-receipt-costs", default=LEGACY_COSTS)
    parser.add_argument("--order-index", type=int, action="append")
    parser.add_argument("--start-iso", default="2026-07-05T12:55:00Z")
    parser.add_argument("--rpc-batch-size", type=int, default=75)
    args = parser.parse_args()
    now = datetime.now(timezone.utc).isoformat()
    ledger = load_json(args.ledger, default={})
    existing_report = load_json(args.output, default={})
    cached_measured = {
        str(row.get("order_id") or ""): row
        for row in existing_report.get("rows") or []
        if isinstance(row, dict) and row.get("status") == "PASS_RECEIPT_MEASURED"
    }
    cached_txs = {str(row.get("tx") or "") for row in cached_measured.values() if row.get("tx")}
    start_ts = parse_ts(args.start_iso)
    eligible_orders = [
        order
        for order in ledger.get("orders") or []
        if isinstance(order, dict)
        and str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
        and (start_ts is None or (parse_ts(order.get("submitted_at")) or 0.0) >= start_ts)
    ]
    tx_hashes = sorted(
        {tx for order in eligible_orders for tx in _tx_hashes(order) if tx not in cached_txs}
    )
    receipts, receipt_errors = _rpc_receipts_batch(
        args.rpc_url, tx_hashes, chunk_size=args.rpc_batch_size
    )
    rows = reconcile_fee_coverage(
        ledger,
        receipts=receipts,
        receipt_errors=receipt_errors,
        start_iso=args.start_iso,
    )
    rows = [
        cached_measured.get(str(row.get("order_id") or ""), row)
        if row.get("status") == "COVERAGE_GAP_RECEIPT"
        else row
        for row in rows
    ]
    measured = [row for row in rows if row.get("status") == "PASS_RECEIPT_MEASURED"]
    rates = [float(row["receipt_measured_fee_rate"]) for row in measured]
    residuals = [float(row["realized_minus_expected_fee_usd"]) for row in measured]
    status_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
    fit_rows = [
        row for row in measured if abs(float(row.get("realized_minus_expected_fee_usd") or 0.0)) <= 0.01
    ]
    zero_fee_rows = [row for row in measured if abs(float(row.get("realized_fee_usd") or 0.0)) <= 0.000001]
    mismatch_rows = [row for row in measured if row not in fit_rows and row not in zero_fee_rows]
    report = {
        "schema_version": 1,
        "kind": "wallet_copy_realized_fee_receipts",
        "flow_stage": "LIVE/LEARN",
        "generated_at": now,
        "ledger_rewrite": False,
        "ledger_rewrite_reason": "live guard owns the ledger; receipt sidecar is joined by order_id/tx",
        "coverage_start_iso": args.start_iso,
        "rows": rows,
        "summary": {
            "since_topup_filled_rows": len(rows),
            "receipt_measured_rows": len(measured),
            "coverage_gap_rows": len(rows) - len(measured),
            "status_counts": status_counts,
            "realized_fee_usd_measured": round(sum(float(row["realized_fee_usd"]) for row in measured), 6),
            "expected_fee_usd_measured": round(sum(float(row["expected_fee_usd"]) for row in measured), 6),
            "absolute_model_residual_usd": round(sum(abs(value) for value in residuals), 6),
            "receipt_measured_fee_rate": {
                "target": POLYMARKET_EMBEDDED_FEE_RATE,
                "mean": round(statistics.fmean(rates), 9) if rates else None,
                "median": round(statistics.median(rates), 9) if rates else None,
                "minimum": round(min(rates), 9) if rates else None,
                "maximum": round(max(rates), 9) if rates else None,
            },
            "coverage_verdict": "PARTIAL_DO_NOT_INTERPOLATE" if len(measured) < len(rows) else "COMPLETE",
            "model_classification": {
                "fits_rate_within_0_01_usd": len(fit_rows),
                "zero_fee_receipts": len(zero_fee_rows),
                "other_receipt_response_mismatch": len(mismatch_rows),
                "rule": "receipt debit is authoritative per row; never interpolate into coverage gaps",
            },
            "measured_first_submitted_at": min(
                (str(row.get("submitted_at") or "") for row in measured), default=None
            ),
            "measured_last_submitted_at": max(
                (str(row.get("submitted_at") or "") for row in measured), default=None
            ),
        },
    }
    atomic_write_json(args.output, report)
    costs_path = Path(args.receipt_costs_output)
    base_path = costs_path if costs_path.exists() else Path(args.base_receipt_costs)
    legacy_rows = [
        {
            **row,
            "response_expected_fee_usd": row.get("expected_fee_usd"),
        }
        for row in measured
    ]
    atomic_write_json(costs_path, _merge_cost_rows(load_json(base_path, default={}), legacy_rows))
    print(json.dumps({"output": args.output, "receipt_costs": str(costs_path), "summary": report["summary"]}, sort_keys=True))
    return 0 if measured else 2


if __name__ == "__main__":
    raise SystemExit(main())
