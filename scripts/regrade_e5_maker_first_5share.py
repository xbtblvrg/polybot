#!/usr/bin/env python3
"""Build the immutable exact-five-share E5 maker-first regrade packet.

Flow stage: PROMOTE/LEARN. The replay is paper-only and never submits orders.
It reuses the prospectively enforced no-fallback/book-hash E5 quote cohort,
applies the wallet-copy 60-second freshness gate independently, and fills an
exact five-share resting order only from cumulative later RTDS SELL volume at
or below the quote before the cancel-at-window-end-minus-30s deadline.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, TextIO


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.models import num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_PAPER_STATE = "data/research/maker_first_btc5m_paper_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/e5_maker_first_5share_regrade_latest.json"
DEFAULT_CURRENT_RTDS = "data/research/polymarket_activity_ws_capture_vpn_burnin_20260703T180934Z.jsonl"
DEFAULT_ARCHIVE_GLOB = (
    "data/research/log_archives/"
    "polymarket_activity_ws_capture_vpn_burnin_20260703T180934Z_*.jsonl.gz"
)

EXACT_SHARES = 5.0
MIN_PRICE = 0.25
MAX_PRICE_EXCLUSIVE = 0.50
MAX_NOTIONAL_USD = 2.50
MAX_FRESHNESS_S = 60.0
MIN_RESOLVED_EXECUTIONS = 150
MIN_TERMINAL_FILL_RATE_PCT = 90.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-state", default=DEFAULT_PAPER_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--rtds-source", action="append", default=[])
    parser.add_argument(
        "--recent-archive-count",
        type=int,
        default=1,
        help="Use this many newest rotated RTDS archives before the current capture.",
    )
    parser.add_argument("--archive-glob", default=DEFAULT_ARCHIVE_GLOB)
    return parser.parse_args()


def _candidate(order: dict[str, Any]) -> dict[str, Any] | None:
    quote = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
    top = quote.get("top_of_book") if isinstance(quote.get("top_of_book"), dict) else {}
    signal_gate = quote.get("signal_gate") if isinstance(quote.get("signal_gate"), dict) else {}
    price = num(quote.get("quote_price") or order.get("limit_price"))
    quote_ts = num(quote.get("quote_ts"))
    window_end_s = num(quote.get("window_end_s"))
    cancel_before_close_s = num(quote.get("cancel_before_close_s"), 30.0)
    source_age_s = num(
        signal_gate.get("source_signal_age_s"),
        num(quote.get("source_signal_age_s"), 1e18),
    )
    route = top.get("route_report") if isinstance(top.get("route_report"), dict) else {}
    direct_fallback = str(route.get("fallback_source") or "") == "direct_clob_after_primary_failure"
    if not bool(quote.get("enforced_no_fallback_book")):
        return None
    if str(quote.get("book_evidence_mode") or "") != "enforced_no_fallback":
        return None
    if str(top.get("status") or "") != "OK" or not str(top.get("book_hash") or "") or direct_fallback:
        return None
    if not (MIN_PRICE <= price < MAX_PRICE_EXCLUSIVE):
        return None
    if source_age_s > MAX_FRESHNESS_S:
        return None
    if quote_ts <= 0 or window_end_s <= 0:
        return None
    notional = round(EXACT_SHARES * price, 6)
    return {
        "source_order_id": str(order.get("order_id") or ""),
        "source_intent_id": str(order.get("intent_id") or ""),
        "quote_id": str(quote.get("quote_id") or ""),
        "market_slug": str(order.get("market_slug") or quote.get("market_slug") or ""),
        "condition_id": str(order.get("condition_id") or quote.get("condition_id") or ""),
        "outcome": str(order.get("outcome") or quote.get("outcome") or ""),
        "token_id": str(quote.get("token_id") or ""),
        "quote_price": round(price, 6),
        "quote_ts": quote_ts,
        "cancel_ts": window_end_s - cancel_before_close_s,
        "window_end_s": window_end_s,
        "source_signal_age_s": round(source_age_s, 6),
        "book_hash": str(top.get("book_hash") or ""),
        "size_shares": EXACT_SHARES,
        "size_usd": notional,
        "sizing_policy_id": "fixed_shares_5",
    }


def select_candidates(paper_state: dict[str, Any]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for order in paper_state.get("orders") or []:
        if not isinstance(order, dict):
            continue
        row = _candidate(order)
        if row and row["quote_id"]:
            selected[row["quote_id"]] = row
    return sorted(selected.values(), key=lambda row: (row["quote_ts"], row["quote_id"]))


def _open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


def _iter_trade_events(paths: Iterable[Path]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    rows_seen = 0
    bad_json = 0
    min_ts: float | None = None
    max_ts: float | None = None
    source_rows: list[dict[str, Any]] = []
    for path in paths:
        file_rows = 0
        file_trade_rows = 0
        file_min: float | None = None
        file_max: float | None = None
        with _open_text(path) as handle:
            for line in handle:
                file_rows += 1
                rows_seen += 1
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue
                if not isinstance(row, dict) or row.get("event") != "rtds_trade_event":
                    continue
                raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
                observed_ts = num(row.get("received_at_s"))
                if observed_ts <= 0:
                    continue
                file_trade_rows += 1
                file_min = observed_ts if file_min is None else min(file_min, observed_ts)
                file_max = observed_ts if file_max is None else max(file_max, observed_ts)
                event_id = str(row.get("event_id") or "")
                if not event_id:
                    event_id = stable_id(
                        "rt",
                        {
                            "market_slug": row.get("market_slug"),
                            "outcome": row.get("outcome"),
                            "side": row.get("side"),
                            "price": row.get("price"),
                            "size": row.get("size"),
                            "received_at_s": observed_ts,
                            "transaction_hash": row.get("transaction_hash"),
                        },
                    )
                events[event_id] = {
                    "event_id": event_id,
                    "market_slug": str(row.get("market_slug") or ""),
                    "outcome": str(row.get("outcome") or raw.get("outcome") or ""),
                    "token_id": str(row.get("asset") or raw.get("asset") or ""),
                    "side": str(row.get("side") or "").upper(),
                    "price": num(row.get("price")),
                    "size": num(row.get("size")),
                    "observed_ts": observed_ts,
                    "event_ts": num(row.get("event_ts")),
                    "transaction_hash": str(row.get("transaction_hash") or ""),
                }
        min_ts = file_min if min_ts is None else min(min_ts, file_min) if file_min is not None else min_ts
        max_ts = file_max if max_ts is None else max(max_ts, file_max) if file_max is not None else max_ts
        source_rows.append(
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "rows_seen": file_rows,
                "trade_rows": file_trade_rows,
                "min_observed_ts": file_min,
                "max_observed_ts": file_max,
            }
        )
    return sorted(events.values(), key=lambda row: (row["observed_ts"], row["event_id"])), {
        "sources": source_rows,
        "rows_seen": rows_seen,
        "bad_json": bad_json,
        "distinct_trade_events": len(events),
        "min_observed_ts": min_ts,
        "max_observed_ts": max_ts,
    }


def replay_candidate(candidate: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    residual = EXACT_SHARES
    fills: list[dict[str, Any]] = []
    for event in events:
        if event["observed_ts"] <= candidate["quote_ts"]:
            continue
        if event["observed_ts"] > candidate["cancel_ts"]:
            break
        if event["side"] != "SELL" or event["price"] > candidate["quote_price"]:
            continue
        if event["market_slug"] != candidate["market_slug"] or event["outcome"] != candidate["outcome"]:
            continue
        if candidate["token_id"] and event["token_id"] != candidate["token_id"]:
            continue
        executed = min(residual, max(0.0, event["size"]))
        if executed <= 0:
            continue
        fills.append(
            {
                "event_id": event["event_id"],
                "observed_ts": event["observed_ts"],
                "event_ts": event["event_ts"],
                "price": event["price"],
                "available_sell_shares": event["size"],
                "executed_shares": round(executed, 6),
                "transaction_hash": event["transaction_hash"],
            }
        )
        residual = max(0.0, residual - executed)
        if residual <= 1e-9:
            break
    filled_shares = round(EXACT_SHARES - residual, 6)
    return {
        **candidate,
        "execution_id": stable_id("e5x5", {"quote_id": candidate["quote_id"]}),
        "filled_shares": filled_shares,
        "residual_cancelled_shares": round(residual, 6),
        "filled_size_usd": round(filled_shares * candidate["quote_price"], 6),
        "execution_status": (
            "FULL_FILL" if residual <= 1e-9 else "PARTIAL_FILL_CANCEL_RESIDUAL" if filled_shares > 0 else "CANCELLED"
        ),
        "fill_events": fills,
        "fill_rule": "cumulative post-quote RTDS SELL volume at or below maker price through cancel_ts",
        "cancel_rule": "cancel residual at window_end_minus_30s",
    }


def score_execution(execution: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    synthetic_order = {
        "order_id": execution["execution_id"],
        "intent_id": stable_id("ci", {"e5_5share_quote_id": execution["quote_id"]}),
        "source_wallet": "E5_MAKER_FIRST",
        "wallet_name": "e5_maker_first_btc5m_v1",
        "condition_id": execution["condition_id"],
        "market_slug": execution["market_slug"],
        "outcome": execution["outcome"],
        "token_id": execution["token_id"],
        "filled_size_usd": execution["filled_size_usd"],
        "filled_shares": execution["filled_shares"],
        "final_status": "FILLED" if execution["filled_shares"] > 0 else "CANCELLED",
    }
    scored = score_order(synthetic_order, resolutions)
    fee = (
        expected_polymarket_buy_fee_usd(
            shares=execution["filled_shares"],
            price=execution["quote_price"],
        )
        if execution["filled_shares"] > 0
        else 0.0
    )
    return {
        **execution,
        "intent_id": synthetic_order["intent_id"],
        "intent_contract": {
            "size_shares": EXACT_SHARES,
            "size_usd": execution["size_usd"],
            "size_usd_rule": "round(5 * limit_price, 6)",
            "sizing_policy_id": "fixed_shares_5",
            "parity_status": (
                "PASS"
                if execution["size_shares"] == EXACT_SHARES
                and execution["size_usd"] == round(EXACT_SHARES * execution["quote_price"], 6)
                else "FAIL"
            ),
        },
        "canonical_resolution": scored.get("resolution"),
        "resolved": bool(scored.get("resolved")),
        "winner": scored.get("winner"),
        "win": scored.get("win"),
        "gross_payout_usd": scored.get("payout_usd"),
        "gross_pnl_usd": scored.get("pnl_usd"),
        "expected_fee_usd": round(fee, 6),
        "post_fee_pnl_usd": round(num(scored.get("pnl_usd")) - fee, 6),
    }


def build_packet(
    *,
    paper_state: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    source_paths: list[Path],
) -> dict[str, Any]:
    candidates = select_candidates(paper_state)
    events, coverage = _iter_trade_events(source_paths)
    min_ts = num(coverage.get("min_observed_ts"))
    max_ts = num(coverage.get("max_observed_ts"))
    covered = [
        row
        for row in candidates
        if min_ts <= row["quote_ts"] and row["cancel_ts"] <= max_ts
    ]
    scored = [score_execution(replay_candidate(row, events), resolutions) for row in covered]
    terminal = [row for row in scored if row["execution_status"] != "OPEN"]
    executions = [row for row in terminal if row["filled_shares"] > 0]
    resolved = [row for row in executions if row["resolved"]]
    post_fee_pnl = round(sum(row["post_fee_pnl_usd"] for row in resolved), 6)
    resolved_cost = round(sum(row["filled_size_usd"] for row in resolved), 6)
    fill_rate = round(100.0 * len(executions) / len(terminal), 6) if terminal else 0.0
    full_fill_rate = (
        round(100.0 * sum(row["execution_status"] == "FULL_FILL" for row in terminal) / len(terminal), 6)
        if terminal
        else 0.0
    )
    parity_violations = sum(row["intent_contract"]["parity_status"] != "PASS" for row in scored)
    fallback_violations = 0
    max_notional = max((row["size_usd"] for row in scored), default=0.0)
    checks = {
        "resolved_executions_gte_150": len(resolved) >= MIN_RESOLVED_EXECUTIONS,
        "post_fee_pnl_positive": post_fee_pnl > 0,
        "post_fee_roi_positive": post_fee_pnl > 0 and resolved_cost > 0,
        "terminal_maker_fill_rate_gte_90pct": fill_rate >= MIN_TERMINAL_FILL_RATE_PCT,
        "zero_fallback_violations": fallback_violations == 0,
        "zero_parity_violations": parity_violations == 0,
        "max_notional_lte_2p50": max_notional <= MAX_NOTIONAL_USD,
    }
    gate_pass = all(checks.values())
    cohort_digest = hashlib.sha256(
        json.dumps(
            [
                {
                    "quote_id": row["quote_id"],
                    "book_hash": row["book_hash"],
                    "quote_ts": row["quote_ts"],
                    "price": row["quote_price"],
                    "filled_shares": row["filled_shares"],
                    "post_fee_pnl_usd": row["post_fee_pnl_usd"],
                }
                for row in scored
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": 1,
        "kind": "e5_maker_first_5share_regrade",
        "flow_stage": "PROMOTE/LEARN",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "source": {
            "paper_state": DEFAULT_PAPER_STATE,
            "paper_state_updated_at": paper_state.get("updated_at"),
            "prospective_cohort_rule": (
                "enforced_no_fallback_book + book_hash + no direct fallback + "
                "0.25 <= quote_price < 0.50"
            ),
            "raw_event_coverage": coverage,
            "canonical_resolutions": DEFAULT_RESOLUTIONS,
            "cohort_sha256": cohort_digest,
        },
        "contract": {
            "size_shares": EXACT_SHARES,
            "sizing_policy_id": "fixed_shares_5",
            "size_usd_rule": "round(5 * limit_price, 6)",
            "max_notional_usd": MAX_NOTIONAL_USD,
            "wallet_copy_freshness_gate_s": MAX_FRESHNESS_S,
            "fill_rule": "cumulative post-quote executable SELL volume at or below maker price",
            "partial_fill_rule": "partial executions count; residual cancels at window_end_minus_30s",
            "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        },
        "summary": {
            "eligible_source_quotes": len(candidates),
            "coverage_complete_terminal_quotes": len(terminal),
            "executed_quotes_full_or_partial": len(executions),
            "full_fill_quotes": sum(row["execution_status"] == "FULL_FILL" for row in terminal),
            "partial_fill_quotes": sum(row["execution_status"] == "PARTIAL_FILL_CANCEL_RESIDUAL" for row in terminal),
            "cancelled_without_fill_quotes": sum(row["execution_status"] == "CANCELLED" for row in terminal),
            "resolved_distinct_executions": len(resolved),
            "resolved_cost_usd": resolved_cost,
            "resolved_expected_fee_usd": round(sum(row["expected_fee_usd"] for row in resolved), 6),
            "resolved_gross_pnl_usd": round(sum(num(row["gross_pnl_usd"]) for row in resolved), 6),
            "resolved_post_fee_pnl_usd": post_fee_pnl,
            "resolved_post_fee_roi_pct": (
                round(100.0 * post_fee_pnl / resolved_cost, 6) if resolved_cost > 0 else 0.0
            ),
            "terminal_maker_fill_rate_pct": fill_rate,
            "terminal_full_fill_rate_pct": full_fill_rate,
            "fallback_violations": fallback_violations,
            "copyintent_parity_violations": parity_violations,
            "max_notional_usd": round(max_notional, 6),
        },
        "gate": {
            "decision": "PASS_AUTO_PROMOTE_FIXED_SHARES_5" if gate_pass else "FAIL_DEMOTE_E5_PAPER_ONLY",
            "pass": gate_pass,
            "checks": checks,
            "resolved_executions_required": MIN_RESOLVED_EXECUTIONS,
            "terminal_maker_fill_rate_required_pct": MIN_TERMINAL_FILL_RATE_PCT,
        },
        "executions": scored,
    }


def _source_paths(args: argparse.Namespace) -> list[Path]:
    explicit = [Path(value) for value in args.rtds_source]
    if explicit:
        paths = explicit
    else:
        archives = sorted(
            ROOT.glob(str(args.archive_glob)),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[: max(0, int(args.recent_archive_count))]
        paths = list(reversed(archives)) + [ROOT / DEFAULT_CURRENT_RTDS]
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing RTDS sources: {missing}")
    return paths


def main() -> int:
    args = parse_args()
    paper_state = load_json(args.paper_state, default={})
    if not isinstance(paper_state, dict):
        raise ValueError(f"invalid paper state: {args.paper_state}")
    resolutions = load_resolutions(args.resolutions)
    packet = build_packet(
        paper_state=paper_state,
        resolutions=resolutions,
        source_paths=_source_paths(args),
    )
    output = Path(args.output)
    atomic_write_json(output, packet)
    immutable = output.with_name(
        f"e5_maker_first_5share_regrade_{packet['source']['cohort_sha256'][:16]}.json"
    )
    if immutable.exists():
        existing = load_json(immutable, default={})
        if not isinstance(existing, dict) or existing.get("source", {}).get("cohort_sha256") != packet["source"]["cohort_sha256"]:
            raise RuntimeError(f"immutable packet collision: {immutable}")
    else:
        atomic_write_json(immutable, packet)
    print(
        json.dumps(
            {
                "decision": packet["gate"]["decision"],
                "summary": packet["summary"],
                "immutable_packet": str(immutable),
            },
            sort_keys=True,
        )
    )
    return 0 if packet["gate"]["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
