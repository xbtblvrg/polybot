#!/usr/bin/env python3
"""Trace a bounded sample of self-feed trades missing from the live ledger."""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLASSIFICATION = "data/research/wallet_copy_recon_window_cash_ledger_latest.json"
DEFAULT_SELF_FEED = "data/research/wallet_copy_self_feed_vs_ledger_latest.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_self_feed_missing_trace_latest.json"


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


def _first_num(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _overlap(left: list[Any], right: list[Any]) -> bool:
    return bool({str(item) for item in left if str(item)} & {str(item) for item in right if str(item)})


def _sample_rows(rows: list[dict[str, Any]], *, sample_size: int, top_cost: int, seed: int) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: _num(row.get("cost_usd")), reverse=True)
    selected = ordered[: max(0, top_cost)]
    selected_txs = {str(row.get("tx") or "") for row in selected}
    rest = [row for row in rows if str(row.get("tx") or "") not in selected_txs]
    rng = random.Random(seed)
    rng.shuffle(rest)
    selected.extend(rest[: max(0, sample_size - len(selected))])
    return selected[:sample_size]


def _ledger_candidates(row: dict[str, Any], ledger_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for ledger_row in ledger_rows:
        if not isinstance(ledger_row, dict):
            continue
        ledger = ledger_row.get("ledger") if isinstance(ledger_row.get("ledger"), dict) else {}
        self_feed = ledger_row.get("self_feed") if isinstance(ledger_row.get("self_feed"), dict) else {}
        basis = ledger or self_feed
        if not basis:
            continue
        condition_match = _overlap(row.get("condition_ids") or [], basis.get("condition_ids") or [])
        market_match = _overlap(row.get("market_slugs") or [], basis.get("market_slugs") or [])
        outcome_match = _overlap(row.get("outcomes") or [], basis.get("outcomes") or [])
        if not (condition_match or market_match):
            continue
        cost_delta = abs(_num(row.get("cost_usd")) - _num(basis.get("cost_usd")))
        out.append(
            {
                "tx": ledger_row.get("tx"),
                "status": ledger_row.get("status"),
                "condition_or_market_match": bool(condition_match or market_match),
                "outcome_match": bool(outcome_match),
                "cost_delta_usd": round(cost_delta, 6),
                "ledger_order_ids": basis.get("order_ids") if isinstance(basis.get("order_ids"), list) else [],
            }
        )
    out.sort(key=lambda item: (not item["outcome_match"], item["cost_delta_usd"]))
    return out[:5]


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    classification = _load_json(root / args.classification, {})
    self_feed = _load_json(root / args.self_feed_report, {})
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(root / args.scorecard)
    rows = [
        row
        for row in classification.get("rows", [])
        if isinstance(row, dict) and row.get("classification") == "true_unrecorded_fill_candidate"
    ]
    sample = _sample_rows(rows, sample_size=int(args.sample_size), top_cost=int(args.top_cost), seed=int(args.seed))
    ledger_rows = self_feed.get("ledger_rows") if isinstance(self_feed.get("ledger_rows"), list) else []
    traced: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    cost_by_class: Counter[str] = Counter()
    pnl_by_class: Counter[str] = Counter()

    for row in sample:
        candidates = _ledger_candidates(row, ledger_rows)
        strong_match = next((item for item in candidates if item["outcome_match"] and item["cost_delta_usd"] <= 0.05), None)
        if strong_match:
            trace_class = "b3_join_scope_artifact_same_market_outcome_cost_match"
            evidence = {"matched_ledger_candidate": strong_match, "candidate_count": len(candidates)}
        elif candidates:
            trace_class = "b3_join_scope_artifact_same_market_needs_manual_review"
            evidence = {"nearest_ledger_candidates": candidates}
        else:
            trace_class = "b1_recording_defect_candidate_no_same_market_ledger_match"
            evidence = {"reason": "no ledger row with matching condition/market found in self-feed diff artifact"}
        counts[trace_class] += 1
        cost_by_class[trace_class] += _num(row.get("cost_usd"))
        pnl_by_class[trace_class] += _num(row.get("pnl_usd"))
        traced.append({**row, "trace_classification": trace_class, "trace_evidence": evidence})

    scorecard_recon = classification.get("scorecard_reconciliation")
    scorecard_recon = scorecard_recon if isinstance(scorecard_recon, dict) else {}
    since_topup = scorecard.get("since_topup_truth")
    since_topup = since_topup if isinstance(since_topup, dict) else {}
    chain_recon = scorecard.get("chain_reconciliation")
    chain_recon = chain_recon if isinstance(chain_recon, dict) else {}
    actual_delta = _first_num(
        scorecard_recon.get("actual_delta_usd"),
        since_topup.get("actual_delta_vs_baseline_usd"),
        since_topup.get("actual_account_delta_vs_baseline_usd"),
        chain_recon.get("actual_delta_vs_baseline_usd"),
    )
    ruled_residual = _num(args.ruled_residual_usd)
    b1_effect = sum(_num(row.get("pnl_usd")) for row in traced if str(row.get("trace_classification", "")).startswith("b1_"))
    b2_effect = 0.0
    b3_effect = sum(_num(row.get("pnl_usd")) for row in traced if str(row.get("trace_classification", "")).startswith("b3_"))
    unexplained = round(actual_delta - ruled_residual - b1_effect - b2_effect - b3_effect, 6)
    return {
        "kind": "wallet_copy_self_feed_missing_trace",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "inputs": {
            "classification": args.classification,
            "self_feed_report": args.self_feed_report,
            "scorecard": args.scorecard,
            "sample_size": int(args.sample_size),
            "top_cost": int(args.top_cost),
            "seed": int(args.seed),
        },
        "coverage": {
            "true_unrecorded_candidates_total": len(rows),
            "sampled": len(traced),
            "sample_is_census": len(traced) == len(rows),
        },
        "summary": {
            "trace_counts": dict(sorted(counts.items())),
            "cost_by_trace_class_usd": {key: round(value, 6) for key, value in sorted(cost_by_class.items())},
            "pnl_by_trace_class_usd": {key: round(value, 6) for key, value in sorted(pnl_by_class.items())},
            "b2_non_guard_fill_confirmed": 0,
            "immediate_notify_required": False,
        },
        "gap_equation": {
            "actual_delta_usd": actual_delta,
            "ruled_residual_usd": ruled_residual,
            "sample_b1_effect_usd": round(b1_effect, 6),
            "sample_b2_effect_usd": round(b2_effect, 6),
            "sample_b3_effect_usd": round(b3_effect, 6),
            "sample_unexplained_usd": unexplained,
            "equation": "actual_delta = ruled_residual + sample_b1_effect + sample_b2_effect + sample_b3_effect + sample_unexplained",
        },
        "rows": traced,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--classification", default=DEFAULT_CLASSIFICATION)
    parser.add_argument("--self-feed-report", default=DEFAULT_SELF_FEED)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--top-cost", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260707)
    parser.add_argument("--ruled-residual-usd", type=float, default=-5.27)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": args.output, "summary": report["summary"], "coverage": report["coverage"]}, sort_keys=True))


if __name__ == "__main__":
    main()
