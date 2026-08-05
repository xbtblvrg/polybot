#!/usr/bin/env python3
"""Spot-check sub-25c replay fills against canonical BTC resolutions.

Flow stage: LEARN/PROMOTE. This is paper-only accounting evidence for the
25-50c admission ruling; it reads replay and resolution artifacts and never
mutates live configuration.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_full_universe_bucket_replay import DEFAULT_REPLAY, price_bucket  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.models import num  # noqa: E402

DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/sub25_bucket_accounting_spot_check_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _norm_outcome(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"up", "yes"}:
        return "Up"
    if text in {"down", "no"}:
        return "Down"
    return ""


def _token_matches_outcome(order: dict[str, Any], resolution: dict[str, Any] | None) -> bool | None:
    if not isinstance(resolution, dict):
        return None
    token_id = str(order.get("token_id") or "")
    outcome = _norm_outcome(order.get("outcome"))
    if not token_id or not outcome:
        return None
    expected = str(resolution.get("yes_token") if outcome == "Up" else resolution.get("no_token") or "")
    if not expected:
        return None
    return token_id == expected


def _resolution_for_scored(scored: dict[str, Any]) -> dict[str, Any] | None:
    resolution = scored.get("resolution")
    return resolution if isinstance(resolution, dict) else None


def _iter_sub25_filled_orders(replay: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    for candidate in replay.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = str(candidate.get("wallet") or candidate.get("source_wallet") or "").lower()
        paper = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        for order in paper.get("replay_orders") or []:
            if not isinstance(order, dict):
                continue
            if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
                continue
            if price_bucket(num(order.get("limit_price"))) != "00_00_25":
                continue
            rows.append((wallet, order))
    return rows


def _pick_spread_sample(rows: list[tuple[str, dict[str, Any]]], sample_size: int) -> list[tuple[str, dict[str, Any]]]:
    selected: list[tuple[str, dict[str, Any]]] = []
    seen_markets: set[str] = set()
    seen_wallets: set[str] = set()
    ordered = sorted(
        rows,
        key=lambda item: (
            str(item[1].get("market_slug") or ""),
            str(item[0] or ""),
            str(item[1].get("order_id") or item[1].get("intent_id") or ""),
        ),
    )
    for wallet, order in ordered:
        market = str(order.get("market_slug") or order.get("condition_id") or "")
        if market in seen_markets:
            continue
        selected.append((wallet, order))
        seen_markets.add(market)
        if wallet:
            seen_wallets.add(wallet)
        if len(selected) >= sample_size:
            return selected
    for wallet, order in ordered:
        key = str(order.get("order_id") or order.get("intent_id") or "")
        if any(str(existing.get("order_id") or existing.get("intent_id") or "") == key for _, existing in selected):
            continue
        selected.append((wallet, order))
        if wallet:
            seen_wallets.add(wallet)
        if len(selected) >= sample_size:
            break
    return selected


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    replay_path = root / args.replay
    resolutions_path = root / args.resolutions
    replay = json.loads(replay_path.read_text())
    resolutions = load_resolutions(resolutions_path)
    rows = _iter_sub25_filled_orders(replay)
    sample = _pick_spread_sample(rows, int(args.sample_size))
    checks: list[dict[str, Any]] = []
    wins = 0
    token_mismatches = 0
    unresolved = 0
    for wallet, order in sample:
        scored = score_order(order, resolutions)
        resolution = _resolution_for_scored(scored)
        token_match = _token_matches_outcome(order, resolutions.get(str(order.get("condition_id") or "")) or resolution)
        if scored.get("resolved") is not True:
            unresolved += 1
        if scored.get("win") is True:
            wins += 1
        if token_match is False:
            token_mismatches += 1
        checks.append(
            {
                "order_id": order.get("order_id"),
                "intent_id": order.get("intent_id"),
                "source_wallet": wallet,
                "market_slug": order.get("market_slug"),
                "condition_id": order.get("condition_id"),
                "outcome": order.get("outcome"),
                "token_id": order.get("token_id"),
                "limit_price": round(num(order.get("limit_price")), 6),
                "cost_usd": scored.get("cost_usd"),
                "shares": scored.get("shares"),
                "resolved": bool(scored.get("resolved")),
                "winner": scored.get("winner"),
                "win": scored.get("win"),
                "pnl_usd": scored.get("pnl_usd"),
                "token_matches_outcome": token_match,
                "resolution_source": (resolution or {}).get("source"),
                "resolution_research_only": bool((resolution or {}).get("research_only")),
            }
        )
    resolved = len(checks) - unresolved
    conclusion = "SUB25_ZERO_WIN_CONFIRMED_SAMPLE"
    if unresolved:
        conclusion = "SUB25_SAMPLE_HAS_UNRESOLVED_ROWS"
    if token_mismatches:
        conclusion = "ACCOUNTING_DEFECT_TOKEN_OUTCOME_MISMATCH"
    elif wins:
        conclusion = "ACCOUNTING_HAS_SUB25_WINS_IN_SAMPLE"
    return {
        "schema_version": 1,
        "kind": "sub25_bucket_accounting_spot_check",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now_iso(),
        "inputs": {
            "replay": args.replay,
            "resolutions": args.resolutions,
            "sample_size_requested": int(args.sample_size),
        },
        "summary": {
            "sub25_filled_orders_available": len(rows),
            "sampled_orders": len(checks),
            "sampled_unique_markets": len({str(row.get("market_slug") or "") for row in checks}),
            "sampled_unique_wallets": len({str(row.get("source_wallet") or "") for row in checks}),
            "resolved_orders": resolved,
            "unresolved_orders": unresolved,
            "wins": wins,
            "losses": resolved - wins,
            "token_outcome_mismatches": token_mismatches,
            "conclusion": conclusion,
            "gate": "PASS" if conclusion == "SUB25_ZERO_WIN_CONFIRMED_SAMPLE" else "ANALYZE",
        },
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
