#!/usr/bin/env python3
"""Classify self-feed trades missing from the live ledger."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SELF_FEED_REPORT = "data/research/wallet_copy_self_feed_vs_ledger_latest.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_recon_window_cash_ledger_latest.json"


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


def _tx(value: Any) -> str:
    return str(value or "").lower()


def _split_companion_map(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for group in report.get("probable_split_fill_groups") if isinstance(report.get("probable_split_fill_groups"), list) else []:
        if not isinstance(group, dict):
            continue
        for row in group.get("missing_companion_self_feed_txs") if isinstance(group.get("missing_companion_self_feed_txs"), list) else []:
            if not isinstance(row, dict):
                continue
            tx = _tx(row.get("tx"))
            if tx:
                out[tx] = group
    return out


def _scorecard_recon(scorecard: dict[str, Any]) -> dict[str, Any]:
    truth = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    totals = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
    since = totals.get("since_topup") if isinstance(totals.get("since_topup"), dict) else {}
    return {
        "reconciliation_status": truth.get("status") or truth.get("reconciliation_status"),
        "reconciliation_start_iso": truth.get("reconciliation_start_iso"),
        "actual_delta_usd": since.get("actual_delta_usd") or truth.get("actual_delta_usd"),
        "canonical_pnl_usd": since.get("canonical_pnl_usd"),
        "spread_usd": round(_num(since.get("canonical_pnl_usd")) - _num(since.get("actual_delta_usd") or truth.get("actual_delta_usd")), 6),
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    self_feed = _load_json(root / args.self_feed_report, {})
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(root / args.scorecard)
    split_by_tx = _split_companion_map(self_feed)
    pnl_rows = (
        (self_feed.get("self_feed_missing_ledger_pnl") or {}).get("rows")
        if isinstance(self_feed.get("self_feed_missing_ledger_pnl"), dict)
        else []
    )
    pnl_by_tx = {_tx(row.get("tx")): row for row in pnl_rows if isinstance(row, dict)}
    rows: list[dict[str, Any]] = []
    classification_counts: Counter[str] = Counter()
    cost_by_class: Counter[str] = Counter()
    pnl_by_class: Counter[str] = Counter()

    missing_rows = (
        self_feed.get("self_missing_ledger_rows")
        if isinstance(self_feed.get("self_missing_ledger_rows"), list)
        else []
    )
    for row in missing_rows:
        if not isinstance(row, dict):
            continue
        tx = _tx(row.get("tx"))
        item = row.get("self_feed") if isinstance(row.get("self_feed"), dict) else {}
        split = split_by_tx.get(tx)
        if split:
            classification = "join_key_defect_probable_split_fill"
            evidence = {
                "ledger_tx": split.get("ledger_tx"),
                "ledger_order_ids": split.get("ledger_order_ids") if isinstance(split.get("ledger_order_ids"), list) else [],
                "split_cost_delta_usd": split.get("cost_delta_usd"),
            }
        else:
            classification = "true_unrecorded_fill_candidate"
            evidence = {
                "reason": "self_feed_tx_absent_from_live_ledger_and_not_explained_by_split_companion",
                "sources": item.get("sources") if isinstance(item.get("sources"), list) else [],
            }
        pnl_row = pnl_by_tx.get(tx, {})
        cost = _num(item.get("cost_usd"))
        pnl = _num(pnl_row.get("pnl_usd") if isinstance(pnl_row, dict) else None)
        classification_counts[classification] += 1
        cost_by_class[classification] += cost
        pnl_by_class[classification] += pnl
        rows.append(
            {
                "tx": tx,
                "classification": classification,
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
                "payout_usd": round(_num(pnl_row.get("payout_usd") if isinstance(pnl_row, dict) else None), 6),
                "resolved": bool(pnl_row.get("resolved")) if isinstance(pnl_row, dict) and "resolved" in pnl_row else None,
                "market_slugs": item.get("market_slugs") if isinstance(item.get("market_slugs"), list) else [],
                "condition_ids": item.get("condition_ids") if isinstance(item.get("condition_ids"), list) else [],
                "outcomes": item.get("outcomes") if isinstance(item.get("outcomes"), list) else [],
                "evidence": evidence,
            }
        )

    ledger_rows = self_feed.get("ledger_rows") if isinstance(self_feed.get("ledger_rows"), list) else []
    amount_mismatches = [
        row for row in ledger_rows if isinstance(row, dict) and row.get("status") == "AMOUNT_MISMATCH"
    ]
    split_ledger_txs = {_tx(group.get("ledger_tx")) for group in self_feed.get("probable_split_fill_groups", []) if isinstance(group, dict)}
    amount_mismatch_txs = {_tx(row.get("tx")) for row in amount_mismatches}
    amount_mismatch_split_overlap = sorted(tx for tx in amount_mismatch_txs if tx in split_ledger_txs)

    summary = {
        "self_feed_missing_ledger_rows": len(rows),
        "join_key_defect_probable_split_fill": int(classification_counts["join_key_defect_probable_split_fill"]),
        "true_unrecorded_fill_candidate": int(classification_counts["true_unrecorded_fill_candidate"]),
        "cost_by_class_usd": {key: round(value, 6) for key, value in sorted(cost_by_class.items())},
        "pnl_by_class_usd": {key: round(value, 6) for key, value in sorted(pnl_by_class.items())},
        "amount_mismatch_tx_groups": len(amount_mismatches),
        "probable_split_fill_groups": len(split_by_tx),
        "amount_mismatch_split_overlap": len(amount_mismatch_split_overlap),
        "p0_guard_fill_recording_audit_required": int(classification_counts["true_unrecorded_fill_candidate"]) > 0,
    }
    return {
        "kind": "wallet_copy_recon_window_cash_ledger_classification",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "inputs": {
            "self_feed_report": args.self_feed_report,
            "scorecard": args.scorecard,
        },
        "window": self_feed.get("window") if isinstance(self_feed.get("window"), dict) else {},
        "scorecard_reconciliation": _scorecard_recon(scorecard),
        "summary": summary,
        "rows": rows,
        "amount_mismatch_split_overlap_txs": amount_mismatch_split_overlap,
        "source_summary": self_feed.get("summary") if isinstance(self_feed.get("summary"), dict) else {},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-feed-report", default=DEFAULT_SELF_FEED_REPORT)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": args.output, "summary": report["summary"]}, sort_keys=True))


if __name__ == "__main__":
    main()
