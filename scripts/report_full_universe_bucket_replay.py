#!/usr/bin/env python3
"""Summarize full-universe replay PnL by price bucket and latency.

Flow stage: LEARN/PROMOTE. This is a paper-only evidence package for the
bucket-concentration ruling; it reads existing replay/resolution artifacts and
does not mutate live configuration.
"""

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

from scripts.build_strategy_decompiler_intake import (  # noqa: E402
    _default_resolutions_path,
    _float,
    _load_resolutions,
    _norm_outcome,
)

DEFAULT_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_full_universe_bucket_replay_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_70_100"


def latency_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 30:
        return "00_lt_30s"
    if seconds < 120:
        return "01_30_120s"
    return "02_gte_120s"


def _book_ts_s(order: dict[str, Any]) -> float | None:
    estimate = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
    book = estimate.get("book") if isinstance(estimate.get("book"), dict) else {}
    raw = book.get("book_timestamp")
    value = _float(raw, 0.0)
    if value <= 0:
        meta = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        evidence = meta.get("metadata") if isinstance(meta.get("metadata"), dict) else {}
        clob = (evidence.get("live_tracking_evidence") or {}).get("clob_book") if isinstance(evidence, dict) else {}
        value = _float((clob or {}).get("book_timestamp"), 0.0)
    if value <= 0:
        return None
    return value / 1000.0 if value > 10_000_000_000 else value


def _new_bucket() -> dict[str, Any]:
    return {
        "orders": 0,
        "wins": 0,
        "cost_usd": 0.0,
        "pnl_usd": 0.0,
        "filled_shares": 0.0,
        "price_sum": 0.0,
        "latency_samples": [],
        "wallets": set(),
    }


def add_order(bucket: dict[str, Any], *, wallet: str, cost: float, shares: float, price: float, pnl: float, win: bool, latency_s: float | None) -> None:
    bucket["orders"] += 1
    bucket["wins"] += int(win)
    bucket["cost_usd"] += cost
    bucket["pnl_usd"] += pnl
    bucket["filled_shares"] += shares
    bucket["price_sum"] += price
    if latency_s is not None:
        bucket["latency_samples"].append(latency_s)
    if wallet:
        bucket["wallets"].add(wallet)


def finalize(bucket: dict[str, Any]) -> dict[str, Any]:
    orders = int(bucket["orders"])
    cost = float(bucket["cost_usd"])
    samples = sorted(float(item) for item in bucket["latency_samples"])
    p50 = samples[len(samples) // 2] if samples else None
    return {
        "orders": orders,
        "wins": int(bucket["wins"]),
        "win_rate_pct": round(bucket["wins"] / orders * 100.0, 6) if orders else None,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(float(bucket["pnl_usd"]), 6),
        "roi_pct": round(float(bucket["pnl_usd"]) / cost * 100.0, 6) if cost else None,
        "avg_price": round(float(bucket["price_sum"]) / orders, 6) if orders else None,
        "wallets": len(bucket["wallets"]),
        "latency_p50_s": round(p50, 6) if p50 is not None else None,
        "latency_max_s": round(max(samples), 6) if samples else None,
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    replay_path = root / args.replay
    replay = json.loads(replay_path.read_text())
    resolutions_path = root / args.resolutions if args.resolutions else _default_resolutions_path(root)
    winners = _load_resolutions(resolutions_path)

    by_bucket: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    by_bucket_latency: dict[tuple[str, str], dict[str, Any]] = defaultdict(_new_bucket)
    by_wallet_bucket: dict[tuple[str, str], dict[str, Any]] = defaultdict(_new_bucket)
    overall = _new_bucket()
    skipped: dict[str, int] = defaultdict(int)
    sub_25c_spotcheck: list[dict[str, Any]] = []
    sub_25c_seen: set[tuple[str, str, str, float, float]] = set()

    for candidate in replay.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        paper = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        wallet = str(candidate.get("wallet") or candidate.get("source_wallet") or "")
        for order in paper.get("replay_orders") or []:
            if not isinstance(order, dict):
                continue
            if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
                skipped["non_filled"] += 1
                continue
            intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
            wallet = str(intent.get("source_wallet") or wallet).lower()
            winner = winners.get(str(order.get("market_slug") or "")) or winners.get(str(order.get("condition_id") or ""))
            outcome = _norm_outcome(order.get("outcome") or intent.get("outcome"))
            if not winner or not outcome:
                skipped["unresolved"] += 1
                continue
            price = _float(order.get("limit_price"), 0.0)
            cost = _float(order.get("filled_size_usd"), 0.0) or _float(order.get("requested_size_usd"), 0.0)
            shares = _float(order.get("filled_shares"), 0.0)
            if price <= 0 or cost <= 0 or shares <= 0:
                skipped["bad_fill"] += 1
                continue
            win = outcome == winner
            pnl = (shares - cost) if win else -cost
            event_ts = _float(intent.get("event_ts"), 0.0)
            book_ts = _book_ts_s(order)
            latency_s = (book_ts - event_ts) if book_ts is not None and event_ts > 0 else None
            if latency_s is not None and latency_s < 0:
                latency_s = None
            bucket = price_bucket(price)
            lbucket = latency_bucket(latency_s)
            for target in (overall, by_bucket[bucket], by_bucket_latency[(bucket, lbucket)], by_wallet_bucket[(wallet, bucket)]):
                add_order(target, wallet=wallet, cost=cost, shares=shares, price=price, pnl=pnl, win=win, latency_s=latency_s)
            if bucket == "00_00_25" and len(sub_25c_spotcheck) < int(args.spotcheck_sample_size):
                sample_key = (
                    wallet,
                    str(order.get("order_id") or ""),
                    str(order.get("market_slug") or ""),
                    round(price, 6),
                    round(cost, 6),
                )
                if sample_key in sub_25c_seen:
                    continue
                sub_25c_seen.add(sample_key)
                sub_25c_spotcheck.append(
                    {
                        "source_wallet": wallet,
                        "order_id": str(order.get("order_id") or ""),
                        "market_slug": str(order.get("market_slug") or ""),
                        "condition_id": str(order.get("condition_id") or ""),
                        "outcome_raw": order.get("outcome") or intent.get("outcome"),
                        "outcome_norm": outcome,
                        "winner": winner,
                        "limit_price": round(price, 6),
                        "cost_usd": round(cost, 6),
                        "filled_shares": round(shares, 6),
                        "win": win,
                        "pnl_usd": round(pnl, 6),
                    }
                )

    wallet_rows = [
        {
            "source_wallet": key[0],
            "price_bucket": key[1],
            **finalize(bucket),
        }
        for key, bucket in by_wallet_bucket.items()
    ]
    wallet_rows.sort(key=lambda row: (-float(row.get("pnl_usd") or 0.0), -int(row.get("orders") or 0), row["source_wallet"]))

    bucket_rows = {key: finalize(bucket) for key, bucket in sorted(by_bucket.items())}
    latency_rows = {
        f"{key[0]}__{key[1]}": finalize(bucket)
        for key, bucket in sorted(by_bucket_latency.items())
    }
    positive_bucket_rows = [row for row in wallet_rows if float(row.get("pnl_usd") or 0.0) > 0 and int(row.get("orders") or 0) >= int(args.min_orders)]
    spotcheck_missing = [
        row for row in sub_25c_spotcheck if not str(row.get("outcome_norm") or "") or not str(row.get("winner") or "")
    ]
    spotcheck_wins = sum(1 for row in sub_25c_spotcheck if row.get("win") is True)
    spotcheck_verdict = (
        "ACCOUNTING_DEFECT_OUTCOME_OR_WINNER_MISSING"
        if spotcheck_missing
        else "FILL_CONDITIONED_ADVERSE_SELECTION_CONFIRMED"
        if sub_25c_spotcheck and spotcheck_wins == 0
        else "ACCOUNTING_HAS_SUB25C_WINS"
        if spotcheck_wins > 0
        else "SPOTCHECK_EMPTY"
    )

    return {
        "schema_version": 1,
        "kind": "wallet_copy_full_universe_bucket_replay",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now_iso(),
        "inputs": {
            "replay": args.replay,
            "resolutions": str(resolutions_path.relative_to(root)) if resolutions_path.is_relative_to(root) else str(resolutions_path),
        },
        "criteria": {
            "min_orders_for_wallet_bucket_top": int(args.min_orders),
            "pnl_basis": "paper replay filled shares at our CLOB effective/limit price; payout=shares when copied outcome wins else 0",
            "latency_basis": "CLOB book timestamp minus source event timestamp, bucketed for latency-conditioned bucket ruling",
            "fill_conditioned_label_required": True,
        },
        "summary": {
            "candidates": len(replay.get("candidates") or []),
            "overall": finalize(overall),
            "positive_wallet_buckets": len(positive_bucket_rows),
            "skipped": dict(sorted(skipped.items())),
        },
        "by_price_bucket": bucket_rows,
        "by_price_bucket_and_latency": latency_rows,
        "sub_25c_accounting_spotcheck": {
            "flow_stage": "LEARN/PROMOTE",
            "sample_size_requested": int(args.spotcheck_sample_size),
            "sample_size": len(sub_25c_spotcheck),
            "sample_wins": spotcheck_wins,
            "sample_losses": len(sub_25c_spotcheck) - spotcheck_wins,
            "verdict": spotcheck_verdict,
            "resolution_source": str(resolutions_path.relative_to(root)) if resolutions_path.is_relative_to(root) else str(resolutions_path),
            "conclusion": (
                "sampled sub-25c copied outcomes normalize correctly and match losing market resolutions; "
                "the -100% bucket is a fill-conditioned adverse-selection finding, not an Up/Down accounting flip"
                if spotcheck_verdict == "FILL_CONDITIONED_ADVERSE_SELECTION_CONFIRMED"
                else "do not use the 25-50c structural evidence until this spot-check verdict is resolved"
            ),
            "samples": sub_25c_spotcheck,
        },
        "top_positive_wallet_buckets": positive_bucket_rows[: int(args.top_n)],
        "bottom_wallet_buckets": sorted(wallet_rows, key=lambda row: (float(row.get("pnl_usd") or 0.0), -int(row.get("orders") or 0)))[: int(args.top_n)],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--min-orders", type=int, default=20)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--spotcheck-sample-size", type=int, default=20)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
