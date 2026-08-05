#!/usr/bin/env python3
"""Replay market-scan wallets from remote wallet trade history.

Flow stage: DISCOVER/LEARN/PROMOTE. Paper/research only: this script reads the
market-wide intake queue, fetches per-wallet data-api history via ``user=``, and
scores resolved crypto-5m BUY trades against the local resolution registry.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import build_wallet_market_scan_intake as intake  # noqa: E402
from scripts.build_strategy_decompiler_intake import (  # noqa: E402
    _default_resolutions_path,
    _float,
    _load_resolutions,
    _norm_outcome,
    _parse_ts,
)
from src.wallet_copy.http_client import PolymarketHttpClient, PolymarketRouteError  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


TRADES_URL = "https://data-api.polymarket.com/trades"
DEFAULT_MARKET_SCAN = "data/research/wallet_market_scan_ranked.json"
DEFAULT_OUTPUT = "data/research/wallet_market_cohort_replay_latest.json"
DEFAULT_STATE = "data/research/wallet_market_cohort_replay_state.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-scan", default=DEFAULT_MARKET_SCAN)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--wallet-limit", type=int, default=25)
    parser.add_argument("--history-limit", type=int, default=500)
    parser.add_argument("--max-pages-per-wallet", type=int, default=8)
    parser.add_argument("--lookback-days", type=float, default=7.0)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--sleep-s", type=float, default=0.08)
    parser.add_argument("--max-wall-runtime-s", type=float, default=240.0)
    parser.add_argument("--min-resolved-buys", type=int, default=12)
    parser.add_argument("--min-unique-markets", type=int, default=8)
    parser.add_argument("--min-paper-pnl-usd", type=float, default=0.0)
    parser.add_argument("--reset-state", action="store_true")
    return parser.parse_args()


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _iso(ts: float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _trade_key(row: dict[str, Any]) -> str:
    tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").strip().lower()
    if tx:
        return tx
    return "|".join(
        [
            str(row.get("proxyWallet") or ""),
            str(row.get("slug") or row.get("marketSlug") or ""),
            str(row.get("timestamp") or ""),
            str(row.get("side") or ""),
            str(row.get("outcome") or ""),
            str(row.get("size") or ""),
            str(row.get("price") or ""),
        ]
    )


def _stake(row: dict[str, Any]) -> float:
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    return max(0.0, price * size)


def _fetch_wallet_page(
    client: PolymarketHttpClient,
    *,
    wallet: str,
    limit: int,
    offset: int,
    timeout_s: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    response = None
    route_report: dict[str, Any] = {}
    for attempt in range(2):
        response = client.request(
            "GET",
            TRADES_URL,
            params={"user": wallet, "takerOnly": "false", "limit": int(limit), "offset": int(offset)},
            request_role="wallet_market_cohort_replay_user_trades",
            timeout_s=float(timeout_s),
        )
        route_report = intake._response_report(response)
        if not (int(offset) > 0 and int(response.status_code) in {502, 503} and attempt == 0):
            break
        time.sleep(0.2)
    assert response is not None
    route_report = intake._response_report(response)
    if int(offset) > 0 and int(response.status_code) in {400, 502, 503}:
        route_report["route_class"] = "DATA_API_USER_PAGINATION_CAP"
        route_report["pagination_cap_reached"] = True
        route_report["pagination_cap_status_code"] = int(response.status_code)
        return [], route_report
    response.raise_for_status()
    payload = response.json()
    rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
    return rows, route_report


def _ranked_wallets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("ranked_wallets") if isinstance(payload.get("ranked_wallets"), list) else []
    wallets = [row for row in rows if isinstance(row, dict) and _norm_wallet(row.get("wallet") or row.get("source_wallet"))]
    wallets.sort(
        key=lambda row: (
            -int(row.get("crypto5m_trade_count") or 0),
            -float(row.get("rank_score") or 0.0),
            _norm_wallet(row.get("wallet") or row.get("source_wallet")),
        )
    )
    return wallets


def _new_wallet_stats(wallet_row: dict[str, Any]) -> dict[str, Any]:
    wallet = _norm_wallet(wallet_row.get("wallet") or wallet_row.get("source_wallet"))
    return {
        "wallet": wallet,
        "intake_rank_score": wallet_row.get("rank_score"),
        "intake_crypto5m_trades": int(wallet_row.get("crypto5m_trade_count") or 0),
        "history_rows_seen": 0,
        "crypto5m_rows_seen": 0,
        "copyable_buy_events": 0,
        "resolved_copyable_events": 0,
        "unresolved_copyable_events": 0,
        "wins": 0,
        "stake_usd": 0.0,
        "paper_pnl_usd": 0.0,
        "unique_markets": set(),
        "unique_conditions": set(),
        "first_trade_ts": None,
        "latest_trade_ts": None,
        "last_offset": 0,
        "pagination_cap_reached": False,
        "sample_trades": [],
        "seen_trade_keys": set(),
    }


def _add_trade(stats: dict[str, Any], row: dict[str, Any], winners: dict[str, str], *, cutoff_ts: float) -> None:
    ts = _parse_ts(row.get("timestamp"))
    if ts > 0 and ts < cutoff_ts:
        return
    stats["history_rows_seen"] += 1
    stats["first_trade_ts"] = ts if ts and stats["first_trade_ts"] is None else (
        min(float(stats["first_trade_ts"]), ts) if ts else stats["first_trade_ts"]
    )
    stats["latest_trade_ts"] = max(float(stats["latest_trade_ts"] or 0.0), ts) if ts else stats["latest_trade_ts"]
    if not intake.is_crypto_5m_trade(row):
        return
    stats["crypto5m_rows_seen"] += 1
    if str(row.get("side") or "").upper() != "BUY":
        return
    key = _trade_key(row)
    seen = stats["seen_trade_keys"]
    if key in seen:
        return
    seen.add(key)
    price = _float(row.get("price"), 0.0)
    stake = _stake(row)
    outcome = _norm_outcome(row.get("outcome"))
    if price <= 0.0 or price >= 1.0 or stake <= 0.0 or not outcome:
        return
    slug = str(row.get("slug") or row.get("marketSlug") or "").strip().lower()
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip()
    stats["copyable_buy_events"] += 1
    if slug:
        stats["unique_markets"].add(slug)
    if condition_id:
        stats["unique_conditions"].add(condition_id)
    winner = winners.get(slug) or winners.get(condition_id)
    if not winner:
        stats["unresolved_copyable_events"] += 1
    else:
        win = outcome == winner
        stats["resolved_copyable_events"] += 1
        stats["wins"] += int(win)
        stats["stake_usd"] += stake
        stats["paper_pnl_usd"] += ((1.0 / price - 1.0) if win else -1.0) * stake
    samples = stats["sample_trades"]
    if len(samples) < 5:
        samples.append(
            {
                "slug": slug,
                "side": "BUY",
                "outcome": outcome,
                "price": round(price, 6),
                "stake_usd": round(stake, 6),
                "timestamp": ts,
                "resolved": bool(winner),
                "winner": winner or "",
            }
        )


def _finalize_wallet(stats: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    resolved = int(stats["resolved_copyable_events"])
    stake = float(stats["stake_usd"])
    pnl = float(stats["paper_pnl_usd"])
    roi = (pnl / stake * 100.0) if stake else None
    unique_markets = len(stats["unique_markets"])
    live_ready = (
        resolved >= int(args.min_resolved_buys)
        and unique_markets >= int(args.min_unique_markets)
        and pnl > float(args.min_paper_pnl_usd)
    )
    if live_ready:
        status = "LIVE_READY_SHADOW_PICK"
        next_action = "add to shadow accrual and standard paper-to-live gate queue"
    elif resolved and pnl > 0:
        status = "SHADOW_POSITIVE_THIN_SAMPLE"
        next_action = "continue remote-history replay until sample clears live-ready thresholds"
    elif int(stats["copyable_buy_events"]) > 0:
        status = "WATCH_ONLY_PENDING_POSITIVE_REPLAY"
        next_action = "continue replay; do not promote until positive resolved copy-PnL"
    else:
        status = "WATCH_ONLY_NO_COPYABLE_BUYS"
        next_action = "keep in ranked intake; no promotion"
    return {
        "wallet": stats["wallet"],
        "status": status,
        "live_ready": live_ready,
        "intake_rank_score": stats["intake_rank_score"],
        "intake_crypto5m_trades": stats["intake_crypto5m_trades"],
        "history_rows_seen": int(stats["history_rows_seen"]),
        "crypto5m_rows_seen": int(stats["crypto5m_rows_seen"]),
        "copyable_buy_events": int(stats["copyable_buy_events"]),
        "resolved_copyable_events": resolved,
        "unresolved_copyable_events": int(stats["unresolved_copyable_events"]),
        "wins": int(stats["wins"]),
        "win_rate_pct": round((int(stats["wins"]) / resolved * 100.0), 6) if resolved else None,
        "stake_usd": round(stake, 6),
        "paper_pnl_usd": round(pnl, 6),
        "roi_pct": round(roi, 6) if roi is not None else None,
        "unique_markets": unique_markets,
        "unique_conditions": len(stats["unique_conditions"]),
        "first_trade_ts": _iso(stats["first_trade_ts"]),
        "latest_trade_ts": _iso(stats["latest_trade_ts"]),
        "last_offset": int(stats["last_offset"]),
        "pagination_cap_reached": bool(stats["pagination_cap_reached"]),
        "sample_trades": stats["sample_trades"],
        "next_action": next_action,
    }


def build_report(root: Path, args: argparse.Namespace, *, now_ts: float | None = None) -> dict[str, Any]:
    generated_at = _utc_now_iso()
    if now_ts is None:
        now_ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00")).timestamp()
    cutoff_ts = float(now_ts) - max(0.0, float(args.lookback_days) * 86400.0)
    market_scan = load_json(root / args.market_scan, default={})
    market_scan = market_scan if isinstance(market_scan, dict) else {}
    ranked_wallets = _ranked_wallets(market_scan)
    state = {} if bool(args.reset_state) else load_json(root / args.state, default={})
    state = state if isinstance(state, dict) else {}
    completed = {str(item) for item in state.get("completed_wallets") or []}
    selected = [row for row in ranked_wallets if _norm_wallet(row.get("wallet") or row.get("source_wallet")) not in completed]
    selected = selected[: max(0, int(args.wallet_limit))]

    resolutions_path = root / args.resolutions if str(args.resolutions or "").strip() else _default_resolutions_path(root)
    winners = _load_resolutions(resolutions_path)
    client = PolymarketHttpClient(
        timeout_s=float(args.timeout_s),
        retries=1,
        user_agent="wallet-market-cohort-replay/1.0",
    )
    started = time.monotonic()
    route_classes: Counter[str] = Counter()
    errors: list[dict[str, Any]] = []
    wallets: list[dict[str, Any]] = []
    budget_exhausted = False

    for wallet_row in selected:
        if float(args.max_wall_runtime_s) > 0 and time.monotonic() - started >= float(args.max_wall_runtime_s):
            budget_exhausted = True
            break
        stats = _new_wallet_stats(wallet_row)
        wallet = stats["wallet"]
        for page in range(max(0, int(args.max_pages_per_wallet))):
            offset = page * int(args.history_limit)
            if float(args.max_wall_runtime_s) > 0 and time.monotonic() - started >= float(args.max_wall_runtime_s):
                budget_exhausted = True
                break
            try:
                rows, route_report = _fetch_wallet_page(
                    client,
                    wallet=wallet,
                    limit=int(args.history_limit),
                    offset=offset,
                    timeout_s=float(args.timeout_s),
                )
            except (PolymarketRouteError, requests.RequestException, ValueError) as exc:
                errors.append({"wallet": wallet, "offset": offset, "error": f"{type(exc).__name__}: {exc}"})
                break
            route_class = str(route_report.get("route_class") or "UNKNOWN")
            route_classes[route_class] += 1
            stats["last_offset"] = offset
            if bool(route_report.get("pagination_cap_reached")):
                stats["pagination_cap_reached"] = True
                break
            if not rows:
                break
            oldest_ts = 0.0
            for row in rows:
                if isinstance(row, dict):
                    row_ts = _parse_ts(row.get("timestamp"))
                    oldest_ts = row_ts if row_ts and oldest_ts <= 0 else min(oldest_ts, row_ts) if row_ts else oldest_ts
                    _add_trade(stats, row, winners, cutoff_ts=cutoff_ts)
            if len(rows) < int(args.history_limit) or (oldest_ts and oldest_ts < cutoff_ts):
                break
            if float(args.sleep_s) > 0:
                time.sleep(float(args.sleep_s))
        wallets.append(_finalize_wallet(stats, args))
        completed.add(wallet)
        if budget_exhausted:
            break

    prior_output = load_json(root / args.output, default={})
    prior_wallets = prior_output.get("wallets") if isinstance(prior_output, dict) and isinstance(prior_output.get("wallets"), list) else []
    merged: dict[str, dict[str, Any]] = {
        _norm_wallet(row.get("wallet")): row for row in prior_wallets if isinstance(row, dict) and _norm_wallet(row.get("wallet"))
    }
    for row in wallets:
        merged[_norm_wallet(row.get("wallet"))] = row
    all_wallets = sorted(
        merged.values(),
        key=lambda row: (
            0 if row.get("live_ready") else 1,
            -float(row.get("paper_pnl_usd") or -1_000_000.0),
            -int(row.get("resolved_copyable_events") or 0),
            str(row.get("wallet") or ""),
        ),
    )
    status_counts = Counter(str(row.get("status") or "UNKNOWN") for row in all_wallets)
    shadow_positive = [
        row
        for row in all_wallets
        if int(row.get("resolved_copyable_events") or 0) > 0 and float(row.get("paper_pnl_usd") or 0.0) > 0.0
    ]
    live_ready_picks = [row for row in all_wallets if bool(row.get("live_ready"))]
    summary = {
        "cohort_size": len(all_wallets),
        "batch_wallets": len(wallets),
        "ranked_queue_size": len(ranked_wallets),
        "cohort_shadow_positive": len(shadow_positive),
        "live_ready_picks": len(live_ready_picks),
        "status_counts": dict(sorted(status_counts.items())),
        "route_class_counts": dict(sorted(route_classes.items())),
        "budget_exhausted": budget_exhausted,
        "errors": len(errors),
        "top_live_ready_wallet": live_ready_picks[0]["wallet"] if live_ready_picks else "",
        "top_shadow_positive_wallet": shadow_positive[0]["wallet"] if shadow_positive else "",
    }
    return {
        "schema_version": 1,
        "kind": "wallet_market_cohort_replay",
        "flow_stage": "DISCOVER/LEARN/PROMOTE",
        "status": "BATCH_REPLAY_COMPLETE" if not errors else "BATCH_REPLAY_WITH_ERRORS",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "inputs": {
            "market_scan": args.market_scan,
            "resolutions": str(resolutions_path.relative_to(root)) if resolutions_path.is_relative_to(root) else str(resolutions_path),
            "endpoint": TRADES_URL,
            "query_mode": "data-api user=<wallet> with offset batches; server startDate/endDate ignored in live probe",
        },
        "criteria": {
            "lookback_days": float(args.lookback_days),
            "min_resolved_buys": int(args.min_resolved_buys),
            "min_unique_markets": int(args.min_unique_markets),
            "min_paper_pnl_usd": float(args.min_paper_pnl_usd),
        },
        "summary": summary,
        "errors": errors,
        "wallets": all_wallets,
        "live_ready_picks": live_ready_picks,
        "shadow_positive_wallets": shadow_positive,
        "next_action": "append each live_ready_pick to shadow accrual queue; continue ranked wallet replay until cohort queue exhausted",
    }


def main() -> int:
    args = parse_args()
    root = ROOT
    report = build_report(root, args)
    atomic_write_json(root / args.output, report)
    prior_state = {} if bool(args.reset_state) else load_json(root / args.state, default={})
    prior_completed = {str(item) for item in (prior_state or {}).get("completed_wallets") or []}
    prior_completed.update(str(row.get("wallet")) for row in report.get("wallets") or [] if isinstance(row, dict))
    atomic_write_json(
        root / args.state,
        {
            "schema_version": 1,
            "generated_at": report.get("generated_at"),
            "completed_wallets": sorted(wallet for wallet in prior_completed if _norm_wallet(wallet)),
            "last_summary": report.get("summary"),
        },
    )
    print(json.dumps({"output": args.output, "summary": report.get("summary")}, sort_keys=True))
    return 0 if int((report.get("summary") or {}).get("errors") or 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
