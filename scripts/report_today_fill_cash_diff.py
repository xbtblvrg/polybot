#!/usr/bin/env python3
"""Compare today's ledger fills to wallet trade records by transaction hash."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.pnl_truth import build_pnl_truth, winner_from_resolution
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.scorecard import load_fresh_scorecard


DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_today_fill_cash_diff_latest.json"
DEFAULT_ITEM4_CASH_AUDIT = "data/research/wallet_copy_item4_nonfill_cash_audit_latest.json"
DEFAULT_SELF_FEED_OVERLAY_REFRESH = "data/research/self_feed_overlay_refresh_latest.json"
RESIDUAL_TREND_LIMIT = 50


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="", help="UTC day YYYY-MM-DD; default is today UTC.")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    return parser.parse_args()


def _load_dotenv_value(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    env_path = ROOT / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _day_window(day: str) -> tuple[datetime, datetime]:
    if day:
        start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    else:
        now = datetime.now(tz=UTC)
        start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "")
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _order_tx(order: dict[str, Any]) -> str:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    tx_hashes = trade_result.get("tx_hashes") if isinstance(trade_result.get("tx_hashes"), list) else []
    candidates = [
        *tx_hashes,
        trade_result.get("transaction_hash"),
        order.get("transaction_hash"),
    ]
    return next((str(value).lower() for value in candidates if value), "")


def _fetch_data_api_trades(
    *,
    user: str,
    day_start_ts: float,
    day_end_ts: float,
    limit: int,
    max_pages: int,
    timeout_s: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not user:
        return [], {"status": "UNAVAILABLE", "reason": "POLYMARKET_PROXY_missing"}
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    for page in range(max_pages):
        query = urllib.parse.urlencode({"user": user, "takerOnly": "false", "limit": int(limit), "offset": page * int(limit)})
        url = f"https://data-api.polymarket.com/trades?{query}"
        urls.append(url)
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
            payload = json.loads(response.read().decode("utf-8"))
        page_rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        for row in page_rows:
            ts = _parse_ts(row.get("timestamp"))
            if day_start_ts <= ts < day_end_ts:
                rows.append(row)
        if len(page_rows) < int(limit):
            return rows, {"status": "OK", "pages": page + 1, "urls": urls, "truncated": False}
        oldest_ts = min((_parse_ts(row.get("timestamp")) for row in page_rows), default=0.0)
        if oldest_ts and oldest_ts < day_start_ts:
            return rows, {"status": "OK", "pages": page + 1, "urls": urls, "truncated": False}
    return rows, {"status": "OK", "pages": int(max_pages), "urls": urls, "truncated": True}


def _outcome_side(outcome: str) -> str:
    normalized = str(outcome or "").upper()
    if normalized in {"UP", "YES"}:
        return "YES"
    if normalized in {"DOWN", "NO"}:
        return "NO"
    return normalized


def _actual_trade_summary(trades: list[dict[str, Any]], resolutions: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for trade in trades:
        tx = str(trade.get("transactionHash") or trade.get("transaction_hash") or "").lower()
        if not tx:
            continue
        item = grouped.setdefault(
            tx,
            {
                "tx": tx,
                "trades": 0,
                "actual_cost_usd": 0.0,
                "actual_size_shares": 0.0,
                "actual_payout_usd": 0.0,
                "markets": set(),
                "outcomes": set(),
            },
        )
        item["trades"] += 1
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or "")
        outcome = str(trade.get("outcome") or "")
        side = str(trade.get("side") or "").upper()
        size = num(trade.get("size"), 0.0)
        price = num(trade.get("price"), 0.0)
        if side == "BUY":
            item["actual_cost_usd"] += size * price
            item["actual_size_shares"] += size
            if winner_from_resolution(resolutions.get(condition_id)) == _outcome_side(outcome):
                item["actual_payout_usd"] += size
        item["markets"].add(str(trade.get("slug") or trade.get("eventSlug") or ""))
        item["outcomes"].add(outcome)
    for item in grouped.values():
        item["actual_cost_usd"] = round(float(item["actual_cost_usd"]), 6)
        item["actual_size_shares"] = round(float(item["actual_size_shares"]), 6)
        item["actual_payout_usd"] = round(float(item["actual_payout_usd"]), 6)
        item["markets"] = sorted(value for value in item["markets"] if value)
        item["outcomes"] = sorted(value for value in item["outcomes"] if value)
    return grouped


def _item4_residual_closeout(path: str = DEFAULT_ITEM4_CASH_AUDIT) -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict) or loaded.get("status") != "PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED":
        return {"status": "MISSING_OR_NOT_PASS", "path": path}
    summary = loaded.get("classification_summary") if isinstance(loaded.get("classification_summary"), dict) else {}
    residual = loaded.get("residual_reconciliation") if isinstance(loaded.get("residual_reconciliation"), dict) else {}
    one_page = loaded.get("one_page_summary") if isinstance(loaded.get("one_page_summary"), dict) else {}
    other = summary.get("other_counterparty") if isinstance(summary.get("other_counterparty"), dict) else {}
    other_rows = int(other.get("rows") or 0) if other else 0
    direct_matches = residual.get("direct_tx_matches") if isinstance(residual.get("direct_tx_matches"), list) else []
    if other_rows != 0 or not direct_matches:
        return {
            "status": "PASS_BUT_NOT_CLOSED",
            "path": path,
            "other_counterparty_rows": other_rows,
            "direct_tx_matches": len(direct_matches),
        }
    overlay_refresh = load_json(DEFAULT_SELF_FEED_OVERLAY_REFRESH, default={})
    before_residual = (
        overlay_refresh.get("before_residual")
        if isinstance(overlay_refresh.get("before_residual"), dict)
        else {}
    )
    after_residual = (
        overlay_refresh.get("after_residual")
        if isinstance(overlay_refresh.get("after_residual"), dict)
        else {}
    )
    return {
        "status": "REOPENED_FALSIFIED_BY_FRESH_REBUILD",
        "path": path,
        "generated_at": loaded.get("generated_at"),
        "previous_status": "CLOSED_STALE_FILL_CASH_DIFF_ACCOUNTING_TERM",
        "previous_residual_classification": "stale_fill_cash_diff_accounting_term",
        "replacement_residual_classification": "unaccounted_one_time_cash_movement",
        "falsification_evidence": {
            "overlay_refresh_generated_at": overlay_refresh.get("generated_at") if isinstance(overlay_refresh, dict) else None,
            "before_residual_generated_at": before_residual.get("generated_at"),
            "after_residual_generated_at": after_residual.get("generated_at"),
            "before_account_value_residual_usd": before_residual.get("account_value_residual_usd"),
            "after_account_value_residual_usd": after_residual.get("account_value_residual_usd"),
            "reason": "fresh self-feed overlay rebuild left the account-value residual unchanged; stale-fill cause disproven",
        },
        "other_counterparty_rows": other_rows,
        "canonical_residual_usd": residual.get("canonical_residual_usd"),
        "h2_post_external_redeem_residual_usd": residual.get("h2_post_external_redeem_residual_usd"),
        "direct_tx_matches": direct_matches,
        "verdict": one_page.get("verdict"),
    }


def _residual_trend(
    output_path: str,
    generated_at: str,
    residual_usd: float | None,
    *,
    basis: str,
    writer: str = "scripts/report_today_fill_cash_diff.py",
) -> list[dict[str, Any]]:
    previous = load_json(output_path, default={})
    previous_summary = previous.get("summary") if isinstance(previous, dict) else {}
    previous_trend = (
        previous_summary.get("scorecard_delta_residual_trend")
        if isinstance(previous_summary, dict)
        else []
    )
    if not isinstance(previous_trend, list):
        previous_trend = []
    trend = [row for row in previous_trend if isinstance(row, dict)]
    trend.append(
        {
            "generated_at": generated_at,
            "cash_diff_residual_usd": residual_usd,
            "basis": str(basis or "unknown"),
            "writer": writer,
        }
    )
    return trend[-RESIDUAL_TREND_LIMIT:]


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    day_start, day_end = _day_window(str(args.day or ""))
    day_start_ts = day_start.timestamp()
    day_end_ts = day_end.timestamp()
    ledger = load_json(args.ledger, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    all_orders = [row for row in ledger.get("orders") or [] if isinstance(row, dict)]
    day_orders = [
        row
        for row in all_orders
        if day_start_ts <= _parse_ts(row.get("submitted_at") or row.get("updated_at")) < day_end_ts
    ]
    day_fills = [row for row in day_orders if str(row.get("final_status") or row.get("status") or "").upper() == "FILLED"]
    resolutions = load_resolutions(str(args.resolutions))
    truth = build_pnl_truth({"orders": day_orders}, resolutions)
    events_by_order = {str(event.get("order_id") or ""): event for event in truth.get("events") or [] if isinstance(event, dict)}
    ledger_by_tx: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "orders": 0,
            "order_ids": [],
            "ledger_cost_usd": 0.0,
            "canonical_payout_usd": 0.0,
            "canonical_pnl_usd": 0.0,
            "annotated_actual_cost_usd": 0.0,
            "annotated_actual_sources": set(),
        }
    )
    missing_tx = 0
    for order in day_fills:
        tx = _order_tx(order)
        if not tx:
            missing_tx += 1
            continue
        event = events_by_order.get(str(order.get("order_id") or ""))
        if not event:
            continue
        item = ledger_by_tx[tx]
        item["orders"] += 1
        item["order_ids"].append(str(order.get("order_id") or ""))
        item["ledger_cost_usd"] += num(event.get("cost_usd"), 0.0)
        item["canonical_payout_usd"] += num(event.get("payout_usd"), 0.0)
        item["canonical_pnl_usd"] += num(event.get("pnl_usd"), 0.0)
        annotated_cost = num(order.get("actual_trade_cost_usd"), 0.0)
        if annotated_cost <= 0:
            result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
            annotated_cost = num(result.get("actual_trade_cost_usd"), 0.0)
            annotated_source = str(result.get("actual_trade_cost_source") or "")
        else:
            annotated_source = str(order.get("actual_trade_cost_source") or "")
        if annotated_cost > 0:
            item["annotated_actual_cost_usd"] += annotated_cost
            if annotated_source:
                item["annotated_actual_sources"].add(annotated_source)

    user = _load_dotenv_value("POLYMARKET_PROXY")
    trades, trade_fetch = _fetch_data_api_trades(
        user=user,
        day_start_ts=day_start_ts,
        day_end_ts=day_end_ts,
        limit=int(args.limit),
        max_pages=int(args.max_pages),
        timeout_s=float(args.timeout_s),
    )
    actual_by_tx = _actual_trade_summary(trades, resolutions)
    rows: list[dict[str, Any]] = []
    for tx, ledger_item in ledger_by_tx.items():
        actual = actual_by_tx.get(tx, {})
        ledger_cost = round(float(ledger_item["ledger_cost_usd"]), 6)
        canonical_payout = round(float(ledger_item["canonical_payout_usd"]), 6)
        if not actual and num(ledger_item.get("annotated_actual_cost_usd"), 0.0) > 0:
            actual = {
                "trades": int(ledger_item["orders"]),
                "actual_cost_usd": round(num(ledger_item.get("annotated_actual_cost_usd"), 0.0), 6),
                "actual_payout_usd": canonical_payout,
                "markets": [],
                "outcomes": [],
                "source": sorted(str(value) for value in ledger_item["annotated_actual_sources"]),
            }
        actual_cost = num(actual.get("actual_cost_usd"), 0.0)
        actual_payout = num(actual.get("actual_payout_usd"), 0.0)
        join_status = "NO_TRADE_RECORD"
        if actual_by_tx.get(tx):
            join_status = "JOINED_DATA_API"
        elif actual:
            join_status = "JOINED_CLOB_ASSOCIATE_TRADE"
        cost_delta = round(ledger_cost - actual_cost, 6) if actual else None
        payout_delta = round(actual_payout - canonical_payout, 6) if actual else None
        total_delta = round((cost_delta or 0.0) + (payout_delta or 0.0), 6) if actual else None
        rows.append(
            {
                "tx": tx,
                "order_ids": ledger_item["order_ids"],
                "orders": int(ledger_item["orders"]),
                "trade_records": int(actual.get("trades") or 0),
                "ledger_cost_usd": ledger_cost,
                "actual_cost_usd": round(actual_cost, 6) if actual else None,
                "ledger_cost_minus_actual_cost_usd": cost_delta,
                "canonical_payout_usd": canonical_payout,
                "actual_payout_usd": round(actual_payout, 6) if actual else None,
                "actual_payout_minus_canonical_payout_usd": payout_delta,
                "explained_surplus_usd": total_delta,
                "scorecard_delta_contribution_usd": total_delta,
                "canonical_pnl_usd": round(float(ledger_item["canonical_pnl_usd"]), 6),
                "markets": actual.get("markets") or [],
                "outcomes": actual.get("outcomes") or [],
                "actual_source": actual.get("source") or "data_api_trades_by_tx_hash" if actual else "",
                "join_status": join_status,
            }
        )
    joined_rows = [row for row in rows if str(row["join_status"]).startswith("JOINED")]
    scorecard = load_fresh_scorecard(args.scorecard)
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    ledger_cost_delta = round(sum(num(row.get("ledger_cost_minus_actual_cost_usd"), 0.0) for row in joined_rows), 6)
    payout_delta = round(sum(num(row.get("actual_payout_minus_canonical_payout_usd"), 0.0) for row in joined_rows), 6)
    explained = round(ledger_cost_delta + payout_delta, 6)
    scorecard_delta = chain.get("delta_vs_expected_usd")
    scorecard_delta_num = num(scorecard_delta, 0.0) if scorecard_delta is not None else None
    residual = round(scorecard_delta_num - explained, 6) if scorecard_delta_num is not None else None
    item4_closeout = _item4_residual_closeout()
    residual_classification = (
        "fully_explained_by_joined_fill_cost_or_payout"
        if residual is not None and abs(residual) <= 0.000001
        else "stale_fill_cash_diff_accounting_term"
        if item4_closeout.get("status") == "CLOSED_STALE_FILL_CASH_DIFF_ACCOUNTING_TERM"
        else "unaccounted_one_time_cash_movement"
    )
    residual_next_action = (
        "closed by item4 non-fill pUSD audit; report as named stale accounting term, no ledger rewrite"
        if residual_classification == "stale_fill_cash_diff_accounting_term"
        else (
            "audit account-value timing, non-fill cash movements, fees/rounding, or balance sampling; "
            "do not absorb this residual into fill PnL"
        )
    )
    rows_sorted = sorted(rows, key=lambda row: abs(num(row.get("scorecard_delta_contribution_usd"), 0.0)), reverse=True)
    generated_at = utc_now_iso()
    day_pnl_basis = scorecard.get("day_pnl_basis") if isinstance(scorecard.get("day_pnl_basis"), dict) else {}
    trend_basis = str(day_pnl_basis.get("primary_basis") or scorecard.get("cost_basis_source") or "unknown")
    residual_trend = _residual_trend(str(args.output), generated_at, residual, basis=trend_basis)
    return {
        "generated_at": generated_at,
        "kind": "wallet_copy_today_fill_cash_diff",
        "flow_stage": "LIVE/SELF-DEV",
        "day_utc": day_start.date().isoformat(),
        "inputs": {
            "ledger": str(args.ledger),
            "resolutions": str(args.resolutions),
            "scorecard": str(args.scorecard),
            "trade_source": "https://data-api.polymarket.com/trades",
        },
        "trade_fetch": trade_fetch,
        "summary": {
            "ledger_fills_today": len(day_fills),
            "ledger_fills_missing_tx": missing_tx,
            "ledger_tx_groups": len(ledger_by_tx),
            "trade_records_today": len(trades),
            "joined_tx_groups": len(joined_rows),
            "unjoined_tx_groups": len(rows) - len(joined_rows),
            "sum_ledger_cost_minus_actual_cost_usd": ledger_cost_delta,
            "sum_actual_payout_minus_canonical_payout_usd": payout_delta,
            "sum_explained_surplus_usd": explained,
            "scorecard_reconciliation_delta_usd": scorecard_delta,
            "scorecard_delta_explained_by_fill_cost_payout_usd": explained,
            "scorecard_delta_residual_after_fill_cost_payout_usd": residual,
            "scorecard_delta_residual_classification": residual_classification,
            "scorecard_delta_residual_next_action": residual_next_action,
            "scorecard_delta_residual_trend": residual_trend,
            "item4_nonfill_cash_audit_closeout": item4_closeout,
            "classification": "actual_trade_records_joined_by_tx_hash_or_clob_associate_trade; ledger math uses actual_trade_record where backfilled",
        },
        "top_offenders": rows_sorted[:10],
        "rows": rows_sorted,
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
