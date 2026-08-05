#!/usr/bin/env python3
"""Measure preregistered f418 post-fee EV by intent-time bid/ask spread."""

from __future__ import annotations

import argparse
import bisect
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
ACTIVATION_UTC = "2026-07-24T08:20:00Z"
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


def _winner(row: dict[str, Any]) -> str:
    value = str(row.get("winning_outcome") or row.get("direction") or "").strip().lower()
    return value if value in {"up", "down"} else ""


def _resolution_index(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in rows:
        winner = _winner(row)
        if not winner:
            continue
        for value in (row.get("condition_id"), row.get("market"), row.get("market_slug")):
            key = str(value or "").strip().lower()
            if key:
                out[key] = winner
    return out


def _book_index(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, float]]]:
    out: dict[str, list[dict[str, float]]] = defaultdict(list)
    for row in rows:
        if str(row.get("event_type") or "") != "best_bid_ask":
            continue
        asset = str(row.get("asset_id") or "")
        captured = _ts(row.get("captured_at_s") or row.get("captured_at_iso"))
        try:
            bid = float(row.get("best_bid"))
            ask = float(row.get("best_ask"))
        except (TypeError, ValueError):
            continue
        if asset and captured is not None and 0 <= bid <= ask <= 1:
            out[asset].append({"ts": captured, "bid": bid, "ask": ask})
    for points in out.values():
        points.sort(key=lambda point: point["ts"])
    return dict(out)


def _nearest(points: list[dict[str, float]], observed_ts: float, max_lag_s: float) -> dict[str, float] | None:
    if not points:
        return None
    timestamps = [point["ts"] for point in points]
    index = bisect.bisect_left(timestamps, observed_ts)
    candidates = points[max(0, index - 1) : min(len(points), index + 1)]
    if not candidates:
        return None
    point = min(candidates, key=lambda item: abs(item["ts"] - observed_ts))
    return point if abs(point["ts"] - observed_ts) <= max_lag_s else None


def _spread_bin(spread: float) -> str:
    if spread <= 0.02 + 1e-12:
        return "tight_le_0.02"
    if spread <= 0.05 + 1e-12:
        return "medium_0.02_0.05"
    return "wide_gt_0.05"


def build_report(
    *,
    event_rows: Iterable[dict[str, Any]],
    book_rows: Iterable[dict[str, Any]],
    resolution_rows: Iterable[dict[str, Any]],
    generated_at: str,
    activation_utc: str = ACTIVATION_UTC,
    max_book_lag_s: float = 5.0,
    min_resolved_windows: int = 50,
) -> dict[str, Any]:
    activation_ts = _ts(activation_utc)
    if activation_ts is None:
        raise ValueError(f"invalid activation timestamp: {activation_utc}")
    books = _book_index(book_rows)
    resolutions = _resolution_index(resolution_rows)
    rows: list[dict[str, Any]] = []
    eligible_fills = 0
    resolved_without_book = 0
    seen: set[str] = set()

    for event in event_rows:
        if str(event.get("event") or "") != "wallet_copy_live_lifecycle":
            continue
        if str(event.get("status") or "") != "LIVE_FILLED":
            continue
        event_ts = _ts(event.get("ts"))
        if event_ts is None or event_ts < activation_ts:
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        profile = payload.get("wallet_copy_execute_live_profile")
        profile = profile if isinstance(profile, dict) else {}
        intent_id = str(event.get("intent_id") or "")
        if not intent_id or intent_id in seen:
            continue
        source_wallet = str(payload.get("source_wallet") or profile.get("source_wallet") or F418).lower()
        if source_wallet not in {"", F418}:
            continue
        slug = str(profile.get("market_slug") or payload.get("market_slug") or "")
        condition = str(profile.get("condition_id") or payload.get("condition_id") or "")
        outcome = str(profile.get("outcome") or payload.get("outcome") or "").strip().lower()
        winner = resolutions.get(condition.lower()) or resolutions.get(slug.lower())
        if not winner:
            continue
        eligible_fills += 1
        asset = str(payload.get("market_id") or payload.get("token_id") or "")
        point = _nearest(books.get(asset, []), event_ts, max_book_lag_s)
        if point is None:
            resolved_without_book += 1
            continue
        seen.add(intent_id)
        shares = float(payload.get("response_fill_size_shares") or 0.0)
        cost = float(payload.get("response_filled_size_usd") or 0.0)
        price = float(payload.get("response_fill_price") or 0.0)
        fee = FEE_RATE * shares * price * (1.0 - price)
        pnl = (shares if outcome == winner else 0.0) - cost - fee
        spread = point["ask"] - point["bid"]
        rows.append(
            {
                "intent_id": intent_id,
                "market_slug": slug,
                "asset_id": asset,
                "outcome": outcome,
                "winning_outcome": winner,
                "observed_ts": event_ts,
                "book_ts": point["ts"],
                "book_lag_s": round(abs(point["ts"] - event_ts), 6),
                "best_bid": round(point["bid"], 6),
                "best_ask": round(point["ask"], 6),
                "spread": round(spread, 6),
                "spread_bin": _spread_bin(spread),
                "post_fee_pnl_usd": round(pnl, 6),
            }
        )
    rows.sort(key=lambda row: (row["observed_ts"], row["intent_id"]))

    cells: list[dict[str, Any]] = []
    for name in ("tight_le_0.02", "medium_0.02_0.05", "wide_gt_0.05"):
        selected = [row for row in rows if row["spread_bin"] == name]
        split = max(1, int(len(selected) * 0.8)) if selected else 0
        holdout = selected[split:]
        total_pnl = sum(float(row["post_fee_pnl_usd"]) for row in selected)
        cells.append(
            {
                "spread_bin": name,
                "resolved_windows": len({str(row["market_slug"]) for row in selected}),
                "post_fee_pnl_usd": round(total_pnl, 6),
                "ev_per_window_usd": round(total_pnl / len(selected), 6) if selected else None,
                "holdout_rows": len(holdout),
                "holdout_post_fee_pnl_usd": round(
                    sum(float(row["post_fee_pnl_usd"]) for row in holdout), 6
                ),
            }
        )

    resolved_windows = len({str(row["market_slug"]) for row in rows})
    baseline_pnl = sum(float(row["post_fee_pnl_usd"]) for row in rows)
    qualified = [
        cell
        for cell in cells
        if cell["resolved_windows"] > 0
        and float(cell["post_fee_pnl_usd"]) > 0
        and int(cell["holdout_rows"]) > 0
        and float(cell["holdout_post_fee_pnl_usd"]) > 0
    ]
    gate_ready = resolved_windows >= min_resolved_windows
    status = "ACCRUING"
    if gate_ready:
        status = "GATE_READY_POSITIVE_CELL" if qualified else "GATE_FAILED_NO_POSITIVE_HOLDOUT_CELL"
    return {
        "schema_version": 1,
        "kind": "f418_spread_elasticity_shadow",
        "flow_stage": "LEARN/VOLUME",
        "generated_at": generated_at,
        "activation_utc": activation_utc,
        "source_wallet": F418,
        "experiment_id": "copy-f418-spread-elasticity-shadow",
        "paper_only": True,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
        "preregistered_spread_bins": {
            "tight_le_0.02": "spread <= 0.02",
            "medium_0.02_0.05": "0.02 < spread <= 0.05",
            "wide_gt_0.05": "spread > 0.05",
        },
        "status": status,
        "coverage": {
            "resolved_f418_fills": eligible_fills,
            "spread_classified_rows": len(rows),
            "resolved_without_fresh_book": resolved_without_book,
            "classified_pct": round(100.0 * len(rows) / eligible_fills, 6) if eligible_fills else 0.0,
            "max_book_lag_s": max_book_lag_s,
        },
        "gate": {
            "required_resolved_windows": min_resolved_windows,
            "resolved_windows": resolved_windows,
            "sample_pass": gate_ready,
            "qualified_positive_holdout_cells": [cell["spread_bin"] for cell in qualified],
            "live_change_allowed": False,
            "rule": "paper-only until sample gate, positive aggregate+holdout cell, and separate Fable ruling",
        },
        "baseline": {
            "post_fee_pnl_usd": round(baseline_pnl, 6),
            "ev_per_window_usd": round(baseline_pnl / len(rows), 6) if rows else None,
        },
        "cells": cells,
        "rows": rows[-200:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--book-log", default="data/research/f418_spread_elasticity_books.jsonl")
    parser.add_argument("--resolution-log", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/f418_spread_elasticity_shadow_latest.json")
    parser.add_argument("--activation-utc", default=ACTIVATION_UTC)
    parser.add_argument("--max-book-lag-s", type=float, default=5.0)
    parser.add_argument("--min-resolved-windows", type=int, default=50)
    args = parser.parse_args()
    report = build_report(
        event_rows=_jsonl(ROOT / args.event_log) or (),
        book_rows=_jsonl(ROOT / args.book_log) or (),
        resolution_rows=_jsonl(ROOT / args.resolution_log) or (),
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        activation_utc=args.activation_utc,
        max_book_lag_s=args.max_book_lag_s,
        min_resolved_windows=args.min_resolved_windows,
    )
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps({"status": report["status"], **report["coverage"], **report["gate"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
