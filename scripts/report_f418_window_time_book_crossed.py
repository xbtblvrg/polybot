#!/usr/bin/env python3
"""Book-cross the f418 offset near-miss cohort and apply day-bounded gates."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_f418_spread_elasticity_shadow import _book_index, _nearest  # noqa: E402
from scripts.report_f418_window_time_60s_near_miss_ev import (  # noqa: E402
    F418,
    FEE_RATE,
    PRICE_CELL_CUTS,
    PRICE_CELL_NAMES,
    WINDOW_RE,
    _bin,
    _cell,
    _rows,
    _timestamp,
    _winner_index,
)
from scripts.report_passive_at_source_holdout import day_bounded_split  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402

DEFAULT_OUTPUT = ROOT / "data/research/f418_window_time_book_crossed_latest.json"


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    return row.get("payload") if isinstance(row.get("payload"), dict) else {}


def _asset_map(rows: list[dict[str, Any]]) -> dict[tuple[str, str], str]:
    result: dict[tuple[str, str], str] = {}
    for row in rows:
        payload = _payload(row)
        profile = payload.get("wallet_copy_execute_live_profile")
        profile = profile if isinstance(profile, dict) else {}
        trade_result = row.get("trade_result") if isinstance(row.get("trade_result"), dict) else {}
        slug = str(row.get("market_slug") or profile.get("market_slug") or payload.get("market_slug") or "")
        outcome = str(row.get("outcome") or profile.get("outcome") or payload.get("outcome") or "").lower()
        asset = str(
            row.get("token_id")
            or row.get("market_id")
            or trade_result.get("market_id")
            or payload.get("token_id")
            or payload.get("market_id")
            or ""
        )
        if slug and outcome and asset and asset != str(row.get("condition_id") or ""):
            result[(slug, outcome)] = asset
    return result


def _cell_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnl = sum(float(row["post_fee_pnl_usd"]) for row in rows)
    top = sorted((float(row["post_fee_pnl_usd"]) for row in rows), reverse=True)[:2]
    return {
        "rows": len(rows),
        "distinct_days": len({str(row["submitted_at"])[:10] for row in rows}),
        "post_fee_pnl_usd": round(pnl, 6),
        "post_fee_ev_per_row_usd": round(pnl / len(rows), 6) if rows else None,
        "top2_pnl_usd": round(sum(top), 6),
        "pnl_after_top2_drop_usd": round(pnl - sum(top), 6),
        "pnl_concentration_top2_pct": round(100.0 * sum(top) / pnl, 6) if pnl > 0 else None,
    }


def build_report(
    event_rows: Iterable[dict[str, Any]],
    book_rows: Iterable[dict[str, Any]],
    resolution_rows: Iterable[dict[str, Any]],
    *,
    generated_at: str,
    max_book_lag_s: float = 5.0,
    min_rows_per_bin: int = 40,
    min_distinct_days_per_bin: int = 3,
) -> dict[str, Any]:
    events = list(event_rows)
    book_events = list(book_rows)
    assets = _asset_map(events)
    books = _book_index(book_events)
    winners = _winner_index(resolution_rows)
    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in events:
        if str(row.get("source_wallet") or "").lower() != F418:
            continue
        slug = str(row.get("market_slug") or "")
        event = str(row.get("event") or "")
        rejected = event == "wallet_copy_live_profit_latency_suppression_reject" and row.get("reject_reason") == "window_time_gte_60s"
        lifecycle = event == "wallet_copy_live_order"
        match = WINDOW_RE.search(slug)
        if not match or not (rejected or lifecycle):
            continue
        payload = _payload(row)
        offset = float(row.get("window_time_s") or payload.get("window_time_s") or (_timestamp(row.get("ts") or row.get("submitted_at")) - float(match.group(1))))
        if offset < 0:
            continue
        outcome = str(row.get("outcome") or payload.get("outcome") or "").lower()
        cohort = "suppressed" if rejected else "on_policy"
        unique.setdefault((slug, outcome, cohort), {**row, "_offset": offset, "_cohort": cohort})

    rows: list[dict[str, Any]] = []
    no_asset = no_book = unresolved = 0
    for row in unique.values():
        payload = _payload(row)
        slug = str(row.get("market_slug") or "")
        outcome = str(row.get("outcome") or payload.get("outcome") or "").lower()
        winner = winners.get(str(row.get("condition_id") or "").lower()) or winners.get(slug.lower())
        if not winner:
            unresolved += 1
            continue
        asset = assets.get((slug, outcome), "")
        if not asset:
            no_asset += 1
            continue
        observed_ts = _timestamp(row.get("ts") or row.get("submitted_at"))
        point = _nearest(books.get(asset, []), observed_ts, max_book_lag_s)
        if point is None:
            no_book += 1
            continue
        retained_price = float(row.get("limit_price") or payload.get("limit_price") or 0.0)
        ask = float(point["ask"])
        cost = float(row.get("copy_size_usd") or row.get("requested_size_usd") or payload.get("copy_size_usd") or 1.0)
        shares = cost / ask if 0 < ask < 1 else 0.0
        fee = FEE_RATE * shares * ask * (1.0 - ask)
        pnl = (shares if outcome == winner else 0.0) - cost - fee
        start = float(WINDOW_RE.search(slug).group(1))
        rows.append(
            {
                "market_slug": slug,
                "submitted_at": datetime.fromtimestamp(start, UTC).isoformat(),
                "outcome": outcome,
                "cohort": row["_cohort"],
                "offset_bin": _bin(row["_offset"]),
                "window_offset_s": round(row["_offset"], 6),
                "retained_limit_price": round(retained_price, 6),
                "price_cell": _cell(retained_price, PRICE_CELL_CUTS, PRICE_CELL_NAMES),
                "move_magnitude_usd": round(cost, 6),
                "move_magnitude_basis": "copy_intent_requested_size_usd",
                "book_cross_best_ask": round(ask, 6),
                "book_lag_s": round(abs(float(point["ts"]) - observed_ts), 6),
                "expected_fee_usd": round(fee, 6),
                "post_fee_pnl_usd": round(pnl, 6),
                "executable_price_basis": "observed_best_ask_at_event_offset",
            }
        )
    rows.sort(key=lambda row: (row["submitted_at"], row["market_slug"], row["outcome"], row["cohort"]))

    book_timestamps = [
        _timestamp(row.get("captured_at_s") or row.get("captured_at_iso"))
        for row in book_events
        if row.get("captured_at_s") is not None or row.get("captured_at_iso")
    ]
    first_book_ts = min(book_timestamps) if book_timestamps else None
    candidate_timestamps = [
        _timestamp(row.get("ts") or row.get("submitted_at"))
        for row in unique.values()
    ]
    before_book = sum(
        1
        for candidate_ts in candidate_timestamps
        if first_book_ts is not None and candidate_ts < first_book_ts
    )
    in_book_timestamps = [
        candidate_ts
        for candidate_ts in candidate_timestamps
        if first_book_ts is not None and candidate_ts >= first_book_ts
    ]
    in_book_days = {
        datetime.fromtimestamp(candidate_ts, UTC).date().isoformat()
        for candidate_ts in in_book_timestamps
    }

    matrix: dict[str, dict[str, Any]] = {}
    for price_cell in ("025_032", "032_040", "040_050", "050_070"):
        matrix[price_cell] = {}
        for offset_bin in ("lt_45", "45_59", "gte_60_suppressed"):
            selected = [row for row in rows if row["price_cell"] == price_cell and row["offset_bin"] == offset_bin]
            split = day_bounded_split(
                selected,
                min_rows_per_bin=min_rows_per_bin,
                min_distinct_days_per_bin=min_distinct_days_per_bin,
            )
            gate_pass = bool(split["sample_gate_pass"])
            aggregate = _cell_stats(selected)
            holdout = _cell_stats(split["holdout"])
            matrix[price_cell][offset_bin] = {
                "sample_gate": {
                    "status": "PASS" if gate_pass else "FAIL",
                    "minimum_rows_per_bin": min_rows_per_bin,
                    "minimum_distinct_days_per_bin": min_distinct_days_per_bin,
                    "development_rows": len(split["development"]),
                    "holdout_rows": len(split["holdout"]),
                    "distinct_days_per_bin": split["distinct_days_per_bin"],
                    "split_integrity": split["split_integrity"],
                },
                "aggregate": aggregate,
                "development": _cell_stats(split["development"]),
                "chronological_holdout": holdout,
                "survives_book_crossing": gate_pass and aggregate["post_fee_pnl_usd"] > 0 and holdout["post_fee_pnl_usd"] > 0,
            }
    survivors = [
        f"{price}/{offset}"
        for price, offsets in matrix.items()
        for offset, cell in offsets.items()
        if cell["survives_book_crossing"]
    ]
    sample_ready_cells = [
        f"{price}/{offset}"
        for price, offsets in matrix.items()
        for offset, cell in offsets.items()
        if cell["sample_gate"]["status"] == "PASS"
    ]
    terminal_unpriceable = bool(
        unique
        and first_book_ts is not None
        and len(in_book_days) < min_distinct_days_per_bin
    )
    verdict = (
        "TERMINAL_UNPRICEABLE_COHORT_NO_CONTEMPORANEOUS_BOOK"
        if terminal_unpriceable
        else "INSUFFICIENT_BOOK_COVERAGE"
    )
    if sample_ready_cells:
        verdict = (
            "BOOK_CROSSED_POSITIVE_CELL_SURVIVES"
            if survivors
            else "BOOK_CROSSING_COLLAPSES_ALL_SAMPLE_READY_CELLS"
        )
    return {
        "schema_version": 1,
        "kind": "f418_window_time_book_crossed",
        "generated_at": generated_at,
        "flow_stage": "MEASURE/LIVE",
        "paper_only": True,
        "live_mutation": False,
        "executable_price_basis": "observed_best_ask_at_event_offset",
        "coverage": {
            "unique_candidate_rows": len(unique),
            "book_crossed_rows": len(rows),
            "unresolved_rows": unresolved,
            "asset_join_missing_rows": no_asset,
            "fresh_book_missing_rows": no_book,
            "max_book_lag_s": max_book_lag_s,
        },
        "temporal_coverage_proof": {
            "first_book_at": (
                datetime.fromtimestamp(first_book_ts, UTC).isoformat()
                if first_book_ts is not None
                else None
            ),
            "candidate_rows_before_book_log": before_book,
            "candidate_rows_in_book_window": len(in_book_timestamps),
            "candidate_distinct_days_in_book_window": len(in_book_days),
            "candidate_days_in_book_window": sorted(in_book_days),
            "minimum_distinct_days_per_bin": min_distinct_days_per_bin,
            "terminal_unpriceable": terminal_unpriceable,
        },
        "matrix": matrix,
        "sample_ready_cells": sample_ready_cells,
        "surviving_cells": survivors,
        "verdict": verdict,
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--books", default="data/research/f418_spread_elasticity_books.jsonl")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT.relative_to(ROOT)))
    parser.add_argument("--max-book-lag-s", type=float, default=5.0)
    args = parser.parse_args()
    report = build_report(
        _rows(ROOT / args.events),
        _rows(ROOT / args.books),
        _rows(ROOT / args.resolutions),
        generated_at=datetime.now(UTC).isoformat(),
        max_book_lag_s=args.max_book_lag_s,
    )
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps({"coverage": report["coverage"], "surviving_cells": report["surviving_cells"], "verdict": report["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
