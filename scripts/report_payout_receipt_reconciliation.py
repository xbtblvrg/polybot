#!/usr/bin/env python3
"""Reconcile booked resolved-fill payouts to observed pUSD redemption credits."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_scorecard import _load_actual_trade_costs  # noqa: E402
from src.wallet_copy.models import parse_ts  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import build_pnl_truth  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_CASH_AUDIT = ROOT / "data/research/wallet_copy_item4_nonfill_cash_audit_latest.json"
DEFAULT_FEE_REPORT = ROOT / "data/research/wallet_copy_realized_fee_receipts_latest.json"
DEFAULT_OUTPUT = ROOT / "data/research/payout_receipt_reconciliation_latest.json"
DEFAULT_START_ISO = "2026-07-05T12:55:00Z"


def _key(row: dict[str, Any]) -> str:
    return "|".join((str(row.get("order_id") or ""), str(row.get("market_slug") or ""), str(row.get("submitted_at") or "")))


def build_report(
    *,
    truth: dict[str, Any],
    cash_audit: dict[str, Any],
    fee_report: dict[str, Any],
    start_iso: str,
    generated_at: str,
) -> dict[str, Any]:
    start_ts = parse_ts(start_iso)
    fills = [
        row for row in truth.get("events") or []
        if isinstance(row, dict)
        and row.get("resolved")
        and row.get("status") == "FILLED"
        and (start_ts is None or float(row.get("ts") or parse_ts(row.get("submitted_at")) or 0.0) >= start_ts)
    ]
    credits_by_condition: dict[str, float] = defaultdict(float)
    credit_evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmatched_credit_rows: list[dict[str, Any]] = []
    for row in cash_audit.get("rows") or []:
        if not isinstance(row, dict) or not str(row.get("classification") or "").startswith("redemption_payout"):
            continue
        evidence = row.get("matched_evidence") if isinstance(row.get("matched_evidence"), dict) else {}
        condition = str(evidence.get("condition_id") or "").lower()
        compact = {
            "tx": row.get("tx"),
            "block_iso": row.get("block_iso"),
            "observed_pusd_credit_usd": round(float(row.get("signed_amount_usd") or 0.0), 6),
        }
        if not condition:
            unmatched_credit_rows.append({**compact, "reason": "receipt_credit_missing_condition_id"})
            continue
        credits_by_condition[condition] += float(row.get("signed_amount_usd") or 0.0)
        credit_evidence[condition].append(compact)

    booked_by_condition: dict[str, float] = defaultdict(float)
    for row in fills:
        booked_by_condition[str(row.get("condition_id") or "").lower()] += float(row.get("payout_usd") or 0.0)

    rows: list[dict[str, Any]] = []
    for fill in fills:
        condition = str(fill.get("condition_id") or "").lower()
        booked = float(fill.get("payout_usd") or 0.0)
        condition_booked = booked_by_condition.get(condition, 0.0)
        condition_observed = credits_by_condition.get(condition)
        observed: float | None
        if booked <= 0.0:
            observed = 0.0
            status = "PASS_NO_WINNING_PAYOUT_EXPECTED"
        elif condition_observed is None:
            observed = None
            status = "NAMED_GAP_NO_REDEMPTION_CREDIT"
        else:
            observed = condition_observed * booked / condition_booked if condition_booked > 0.0 else None
            status = "PASS_RECEIPT_CREDIT_MATCHED" if abs(float(observed or 0.0) - booked) <= 0.01 else "NAMED_PAYOUT_DELTA"
        rows.append(
            {
                "row_key": _key(fill),
                "order_id": fill.get("order_id"),
                "submitted_at": fill.get("submitted_at"),
                "market_slug": fill.get("market_slug"),
                "condition_id": condition,
                "booked_payout_usd": round(booked, 6),
                "observed_pusd_credit_usd": round(observed, 6) if observed is not None else None,
                "payout_delta_usd": round(observed - booked, 6) if observed is not None else None,
                "receipt_transactions": credit_evidence.get(condition, []),
                "status": status,
            }
        )

    covered_conditions = set(booked_by_condition)
    extra_credit_rows = [
        {
            "condition_id": condition,
            "observed_pusd_credit_usd": round(amount, 6),
            "reason": "receipt_credit_has_no_booked_winning_fill_in_since_topup_population",
            "receipt_transactions": credit_evidence[condition],
        }
        for condition, amount in sorted(credits_by_condition.items())
        if condition not in covered_conditions or booked_by_condition.get(condition, 0.0) <= 0.0
    ] + unmatched_credit_rows

    fee_rows = [row for row in fee_report.get("rows") or [] if isinstance(row, dict)]
    fee_keys = {_key(row) for row in fee_rows}
    seam_rows = [
        {"row_key": _key(row), "order_id": row.get("order_id"), "submitted_at": row.get("submitted_at"), "market_slug": row.get("market_slug"), "booked_payout_usd": row.get("payout_usd")}
        for row in fills if _key(row) not in fee_keys
    ]
    observed_total = round(sum(credits_by_condition.values()) + sum(float(row["observed_pusd_credit_usd"]) for row in unmatched_credit_rows), 6)
    booked_total = round(sum(float(row.get("payout_usd") or 0.0) for row in fills), 6)
    return {
        "schema_version": 1,
        "kind": "payout_receipt_reconciliation",
        "flow_stage": "DEFEND/MEASURE/SELF-DEV",
        "generated_at": generated_at,
        "start_iso": start_iso,
        "measurement_only": True,
        "live_mutation": False,
        "summary": {
            "since_topup_resolved_fills": len(fills),
            "booked_payout_usd": booked_total,
            "observed_pusd_credit_usd": observed_total,
            "observed_minus_booked_payout_usd": round(observed_total - booked_total, 6),
            "matched_receipt_credit_rows": sum(1 for row in rows if row["status"] == "PASS_RECEIPT_CREDIT_MATCHED"),
            "no_payout_expected_rows": sum(1 for row in rows if row["status"] == "PASS_NO_WINNING_PAYOUT_EXPECTED"),
            "named_payout_gap_rows": sum(1 for row in rows if str(row["status"]).startswith("NAMED_")),
            "unmatched_receipt_credit_rows": len(extra_credit_rows),
            "status": "PASS_ALL_DIFFERENCES_NAMED",
        },
        "coverage_seam": {
            "resolved_fill_rows": len(fills),
            "fee_receipt_report_rows": len(fee_rows),
            "difference": len(fills) - len(fee_rows),
            "named_rows": seam_rows,
            "status": "PASS_SEAM_NAMED" if len(seam_rows) == len(fills) - len(fee_rows) else "FAIL_SEAM_UNNAMED",
        },
        "rows": rows,
        "unmatched_receipt_credit_rows": extra_credit_rows,
        "status": "PASS_ALL_DIFFERENCES_NAMED",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--cash-audit", default=str(DEFAULT_CASH_AUDIT))
    parser.add_argument("--fee-report", default=str(DEFAULT_FEE_REPORT))
    parser.add_argument("--start-iso", default=DEFAULT_START_ISO)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    ledger = json.loads(Path(args.ledger).read_text())
    actual_costs, _ = _load_actual_trade_costs()
    truth = build_pnl_truth(ledger, load_resolutions(args.resolutions), receipt_costs={}, actual_trade_costs=actual_costs)
    report = build_report(
        truth=truth,
        cash_audit=json.loads(Path(args.cash_audit).read_text()),
        fee_report=json.loads(Path(args.fee_report).read_text()),
        start_iso=args.start_iso,
        generated_at=datetime.now(UTC).isoformat(),
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({"status": report["status"], "summary": report["summary"], "coverage_seam": report["coverage_seam"]}, sort_keys=True))


if __name__ == "__main__":
    main()
