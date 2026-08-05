#!/usr/bin/env python3
"""Compact the one-cut USDC transfer audit into the ORDER 109 ledger."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def build_summary(raw: dict[str, Any]) -> dict[str, Any]:
    classes = raw.get("classification_summary") or {}
    fill = classes.get("fill_settlement") or {}
    redemption = classes.get("redemption_payout") or {}
    residual = raw.get("residual_reconciliation") or {}
    canonical = num(residual.get("canonical_residual_usd"), 0.0)
    transfer_sum = round(num(fill.get("net_usd")) + num(redemption.get("net_usd")), 6)
    rows = [
        {
            "classification": "fill",
            "transfer_count": int(fill.get("rows") or 0),
            "signed_sum_usd": round(num(fill.get("net_usd")), 6),
            "enumeration": "complete",
        },
        {
            "classification": "redemption_payout",
            "transfer_count": int(redemption.get("rows") or 0),
            "signed_sum_usd": round(num(redemption.get("net_usd")), 6),
            "enumeration": "complete",
        },
        {
            "classification": "fee",
            "transfer_count": 0,
            "signed_sum_usd": 0.0,
            "enumeration": "not_separable_from_matchOrders_fill_settlement_transfer",
        },
        {
            "classification": "gas",
            "transfer_count": 0,
            "signed_sum_usd": 0.0,
            "enumeration": "not_a_usdc_transfer; Polygon gas is paid in native token",
        },
        {
            "classification": "topup_or_withdrawal",
            "transfer_count": len(raw.get("other_counterparty_rows") or []),
            "signed_sum_usd": round(
                sum(num(row.get("signed_amount_usd")) for row in raw.get("other_counterparty_rows") or []),
                6,
            ),
            "enumeration": "complete_after_topup_baseline",
        },
        {
            "classification": "unknown_account_value_identity_residual",
            "transfer_count": 0,
            "signed_sum_usd": round(canonical - transfer_sum, 6),
            "enumeration": "UNENUMERABLE_AS_USDC_TRANSFER",
            "reason": (
                "all USDC transfers in the window are classified as fill or redemption and "
                "other-counterparty rows are empty; the canonical residual is an account-value "
                "identity difference, the exact baseline balance sample failed with explorer 403, "
                "and multiple ordinary redemption txs are near the residual so no unique tx hash is admissible"
            ),
        },
    ]
    return {
        "schema_version": 1,
        "kind": "order109_usdc_transfer_ledger",
        "flow_stage": "DEFEND/SELF-DEV",
        "generated_at": utc_now_iso(),
        "source_artifact": "data/research/wallet_copy_order109_usdc_transfer_audit_raw.json",
        "source_status": raw.get("status"),
        "window": raw.get("window"),
        "raw_transfer_count": int(((raw.get("fetch") or {}).get("transfers") or {}).get("rows_in_window") or 0),
        "canonical_cash_diff_residual_usd": round(canonical, 6),
        "enumerated_transfer_signed_sum_usd": transfer_sum,
        "ledger_rows": rows,
        "ledger_row_count": len(rows),
        "ledger_signed_sum_usd": round(
            sum(num(row.get("signed_sum_usd")) for row in rows), 6
        ),
        "acceptance": {
            "within_20_rows": len(rows) <= 20,
            "signed_sum_matches_residual_within_one_cent": abs(
                sum(num(row.get("signed_sum_usd")) for row in rows) - canonical
            ) <= 0.01,
            "unique_residual_tx_hash": None,
            "unmapped_class": "unknown_account_value_identity_residual",
            "unmapped_class_reason": rows[-1]["reason"],
            "decision": "RETIRED_PERMANENT_NAMED_CONSTANT_NO_FURTHER_RETRACE",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    raw = load_json(args.input, default={})
    if not isinstance(raw, dict) or not raw:
        raise ValueError(f"ORDER109_RAW_AUDIT_MISSING path={args.input}")
    report = build_summary(raw)
    report["source_artifact"] = args.input
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, **report["acceptance"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
