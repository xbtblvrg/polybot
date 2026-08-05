#!/usr/bin/env python3
"""Score frozen f418 FAK no-match intents against delayed L2 depth, paper-only."""

from __future__ import annotations

import argparse
import bisect
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
POLICY = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
ACTIVATION_UTC = "2026-07-24T09:22:03Z"
DELAYS_MS = (0, 100, 250, 500, 1000, 1500)
FEE_RATE = 0.069997697


def _ts(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _resolution_index(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in rows:
        winner = str(row.get("winning_outcome") or row.get("direction") or "").lower()
        if winner not in {"up", "down"}:
            continue
        for key in ("condition_id", "market", "market_slug"):
            value = str(row.get(key) or "").lower()
            if value:
                out[value] = winner
    return out


def _book_index(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("event_type") != "best_bid_ask":
            continue
        asset = str(row.get("asset_id") or "")
        observed = _ts(row.get("captured_at_s") or row.get("captured_at_iso"))
        asks = row.get("asks")
        if not asset or observed is None or not isinstance(asks, list):
            continue
        levels = []
        for level in asks:
            try:
                price, size = float(level["price"]), float(level["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if 0 <= price <= 1 and size > 0:
                levels.append({"price": price, "size": size})
        out[asset].append({"ts": observed, "asks": sorted(levels, key=lambda item: item["price"])})
    for points in out.values():
        points.sort(key=lambda item: item["ts"])
    return dict(out)


def _future_book(points: list[dict[str, Any]], target: float, tolerance_s: float) -> dict[str, Any] | None:
    if not points:
        return None
    index = bisect.bisect_left([point["ts"] for point in points], target)
    if index >= len(points):
        return None
    point = points[index]
    return point if point["ts"] - target <= tolerance_s else None


def _consume(asks: list[dict[str, float]], shares: float, limit: float) -> tuple[bool, float]:
    remaining, cost = shares, 0.0
    for level in asks:
        if level["price"] > limit + 1e-12:
            break
        take = min(remaining, level["size"])
        cost += take * level["price"]
        remaining -= take
        if remaining <= 1e-9:
            return True, cost
    return False, 0.0


def _opportunities(rows: Iterable[dict[str, Any]], activation_ts: float) -> tuple[list[dict[str, Any]], int]:
    out, seen, violations = [], set(), 0
    for row in rows:
        if row.get("event") != "wallet_copy_live_order" or row.get("final_status") != "REJECTED":
            continue
        lifecycle = row.get("lifecycle") if isinstance(row.get("lifecycle"), list) else []
        rejected = next(
            (
                item
                for item in lifecycle
                if isinstance(item, dict)
                and item.get("status") == "LIVE_REJECTED"
                and isinstance(item.get("payload"), dict)
                and item["payload"].get("error_class") == "fak_no_match"
            ),
            None,
        )
        if rejected is None:
            continue
        payload = rejected["payload"]
        latency = payload.get("wallet_copy_latency_budget")
        latency = latency if isinstance(latency, dict) else row.get("latency_budget")
        latency = latency if isinstance(latency, dict) else {}
        observed = _ts(latency.get("exchange_ack_ts") or rejected.get("ts"))
        error = str(payload.get("error_class") or "").lower()
        if observed is None or observed < activation_ts or "fak_no_match" not in error:
            continue
        profile = payload.get("wallet_copy_execute_live_profile")
        profile = profile if isinstance(profile, dict) else payload
        intent = str(row.get("intent_id") or payload.get("intent_id") or "")
        if not intent or intent in seen:
            continue
        parity = row.get("parity_capsule") if isinstance(row.get("parity_capsule"), dict) else {}
        live_intent = parity.get("live_intent") if isinstance(parity.get("live_intent"), dict) else {}
        metadata = live_intent.get("metadata") if isinstance(live_intent.get("metadata"), dict) else {}
        source = str(row.get("source_wallet") or live_intent.get("source_wallet") or metadata.get("source_wallet") or "").lower()
        policy = str(row.get("policy_id") or metadata.get("policy_id") or POLICY)
        asset = str(payload.get("market_id") or live_intent.get("market_id") or "")
        slug = str(row.get("market_slug") or profile.get("market_slug") or "")
        condition = str(row.get("condition_id") or profile.get("condition_id") or "")
        outcome = str(row.get("outcome") or profile.get("outcome") or "").lower()
        try:
            shares = float(payload.get("order_size") or row.get("requested_shares") or 0)
            limit = float(payload.get("entry_price") or row.get("limit_price") or 0)
            usd = float(payload.get("size_usd") or row.get("requested_size_usd") or shares * limit)
        except (TypeError, ValueError):
            violations += 1
            continue
        parity_ok = source == F418 and policy == POLICY and asset and 0 < limit <= 0.5 and 0 < usd <= 1.000001
        violations += int(not parity_ok)
        if not parity_ok:
            continue
        seen.add(intent)
        out.append({"intent_id": intent, "ts": observed, "asset_id": asset, "market_slug": slug,
                    "condition_id": condition, "outcome": outcome, "shares": shares, "limit_price": limit,
                    "order_size_usd": usd})
    return sorted(out, key=lambda row: (row["ts"], row["intent_id"])), violations


def build_report(*, event_rows: Iterable[dict[str, Any]], book_rows: Iterable[dict[str, Any]],
                 resolution_rows: Iterable[dict[str, Any]], generated_at: str,
                 activation_utc: str = ACTIVATION_UTC, max_book_lag_s: float = 0.35,
                 min_resolved_opportunities: int = 50) -> dict[str, Any]:
    activation = _ts(activation_utc)
    if activation is None:
        raise ValueError("invalid activation timestamp")
    books, resolutions = _book_index(book_rows), _resolution_index(resolution_rows)
    opportunities, violations = _opportunities(event_rows, activation)
    scored = []
    bin_counts = {str(delay): {"covered": 0, "executable": 0} for delay in DELAYS_MS}
    for opportunity in opportunities:
        first_delay, fill_cost = None, 0.0
        samples = {}
        for delay in DELAYS_MS:
            point = _future_book(books.get(opportunity["asset_id"], []), opportunity["ts"] + delay / 1000, max_book_lag_s)
            executable, cost = (False, 0.0) if point is None else _consume(
                point["asks"], opportunity["shares"], opportunity["limit_price"]
            )
            if point is not None:
                bin_counts[str(delay)]["covered"] += 1
            if executable:
                bin_counts[str(delay)]["executable"] += 1
                if first_delay is None:
                    first_delay, fill_cost = delay, cost
            samples[str(delay)] = {"covered": point is not None, "executable": executable}
        winner = resolutions.get(opportunity["condition_id"].lower()) or resolutions.get(opportunity["market_slug"].lower())
        pnl = None
        if winner and first_delay is not None:
            payout = opportunity["shares"] if opportunity["outcome"] == winner else 0.0
            price = fill_cost / opportunity["shares"]
            pnl = payout - fill_cost - FEE_RATE * opportunity["shares"] * price * (1 - price)
        scored.append({**opportunity, "first_executable_delay_ms": first_delay, "resolved": bool(winner),
                       "post_fee_pnl_usd": None if pnl is None else round(pnl, 6), "samples": samples})
    resolved = [row for row in scored if row["resolved"]]
    split = int(len(resolved) * 0.8)
    pnl_rows = [row for row in resolved if row["post_fee_pnl_usd"] is not None]
    aggregate = sum(row["post_fee_pnl_usd"] for row in pnl_rows)
    holdout = sum(row["post_fee_pnl_usd"] for row in resolved[split:] if row["post_fee_pnl_usd"] is not None)
    immediate = bin_counts["0"]["executable"]
    delayed = sum(1 for row in scored if row["first_executable_delay_ms"] not in {None, 0})
    ready = len(resolved) >= min_resolved_opportunities
    passed = ready and aggregate > 0 and holdout > 0 and delayed > immediate and violations == 0
    return {
        "schema_version": 1, "kind": "fak_depth_persistence_timing_shadow", "flow_stage": "LEARN/LIVE-READY",
        "generated_at": generated_at, "activation_utc": activation_utc,
        "experiment_id": "copy-fak-depth-persistence-timing-shadow", "source_wallet": F418,
        "frozen_policy_id": POLICY, "delay_bins_ms": list(DELAYS_MS), "status": "GATE_PASS" if passed else "ACCRUING" if not ready else "GATE_FAIL",
        "coverage": {"opportunities": len(scored), "resolved_opportunities": len(resolved), "minimum_resolved": min_resolved_opportunities},
        "bin_counts": bin_counts, "metrics": {"immediate_executable": immediate, "delayed_first_executable": delayed,
        "aggregate_post_fee_pnl_usd": round(aggregate, 6), "holdout_post_fee_pnl_usd": round(holdout, 6)},
        "gate": {"ready": ready, "passed": passed, "requirements": ["positive aggregate post-fee PnL",
        "positive chronological holdout", "delayed conversion uplift versus immediate FAK", "zero parity violations"]},
        "parity": {"violations": violations, "copy_intents_frozen": True}, "live_mutation": False,
        "paper_only": True, "rows": scored[-200:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--books", default="data/research/fak_depth_persistence_books.jsonl")
    parser.add_argument("--resolutions", default="data/research/polymarket_btc5m_resolutions.jsonl")
    parser.add_argument("--output", default="data/research/fak_depth_persistence_timing_shadow_latest.json")
    args = parser.parse_args()
    generated_at = datetime.now().astimezone().isoformat()
    report = build_report(event_rows=_jsonl(ROOT / args.events), book_rows=_jsonl(ROOT / args.books),
                          resolution_rows=_jsonl(ROOT / args.resolutions), generated_at=generated_at)
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
