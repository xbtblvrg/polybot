#!/usr/bin/env python3
"""Build the immutable four-cell delayed-offset E5 promotion decision."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


DEFAULT_STATE = "data/research/e5_delayed_offset_paper_state.json"
DEFAULT_OUTPUT = (
    "data/research/e5_delayed_offset_side_selective_promotion_packet_latest.json"
)
FEE_RATE = 0.069997697
CELL_BOUNDS = (("60_180", 60.0, 180.0), ("180_270", 180.0, 270.000001))


def _hash(value: dict[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def build_packet(state: dict[str, Any], *, now_s: float) -> dict[str, Any]:
    scored = {
        str(row.get("order_id")): row
        for row in (state.get("resolution_scoring") or {}).get("scored_orders") or []
        if isinstance(row, dict) and row.get("resolved") is True
    }
    cells: dict[str, list[dict[str, Any]]] = {
        f"{band}_{side}": [] for band, _, _ in CELL_BOUNDS for side in ("UP", "DOWN")
    }
    unresolved_old_by_cell = {key: 0 for key in cells}
    fallback_by_cell = {key: 0 for key in cells}
    for order in state.get("orders") or []:
        if not isinstance(order, dict) or str(order.get("final_status")).upper() != "FILLED":
            continue
        quote = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
        try:
            offset = float(quote.get("quote_ts")) - float(quote.get("window_start_s"))
        except (TypeError, ValueError):
            continue
        outcome = str(order.get("outcome") or quote.get("outcome") or "").upper()
        band = next((name for name, low, high in CELL_BOUNDS if low <= offset < high), "")
        key = f"{band}_{outcome}"
        if key not in cells:
            continue
        top = quote.get("top_of_book") if isinstance(quote.get("top_of_book"), dict) else {}
        book_ok = bool(
            quote.get("enforced_no_fallback_book") is True
            and top.get("status") == "OK"
            and top.get("book_hash")
        )
        if not book_ok:
            fallback_by_cell[key] += 1
        resolution = scored.get(str(order.get("order_id")))
        if not resolution:
            try:
                window_end_s = float(quote.get("window_end_s"))
            except (TypeError, ValueError):
                window_end_s = now_s
            if now_s > window_end_s + 300:
                unresolved_old_by_cell[key] += 1
            continue
        shares = float(resolution.get("shares") or order.get("filled_shares") or 0.0)
        price = float(order.get("limit_price") or 0.0)
        fee = FEE_RATE * shares * price * (1.0 - price)
        intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        cells[key].append(
            {
                "order_id": order.get("order_id"),
                "intent_hash": _hash(intent),
                "quote_ts": float(quote.get("quote_ts") or 0.0),
                "offset_s": round(offset, 6),
                "outcome": outcome,
                "fill_price": price,
                "shares": shares,
                "pre_fee_pnl_usd": float(resolution.get("pnl_usd") or 0.0),
                "fee_usd": round(fee, 9),
                "post_fee_pnl_usd": round(float(resolution.get("pnl_usd") or 0.0) - fee, 9),
                "book_hash": top.get("book_hash"),
                "canonical_resolution": resolution.get("resolution"),
            }
        )
    parity_violations = int(
        (state.get("book_aware_summary") or {}).get("copyintent_parity_violations") or 0
    )
    reports = []
    for key, rows in cells.items():
        rows.sort(key=lambda row: (row["quote_ts"], str(row["order_id"])))
        split = int(len(rows) * 0.7)
        train, holdout = rows[:split], rows[split:]
        total_pnl = round(sum(row["post_fee_pnl_usd"] for row in rows), 6)
        total_cost = sum(row["fill_price"] * row["shares"] for row in rows)
        holdout_pnl = round(sum(row["post_fee_pnl_usd"] for row in holdout), 6)
        checks = {
            "resolved_gte_50": len(rows) >= 50,
            "post_fee_total_positive": total_pnl > 0,
            "chronological_holdout_positive": holdout_pnl > 0,
            "no_unresolved_fill_older_than_one_window": unresolved_old_by_cell[key] == 0,
            "zero_fallback_book_rows": fallback_by_cell[key] == 0,
            "zero_copyintent_parity_violations": parity_violations == 0,
        }
        reports.append(
            {
                "cell": key,
                "resolved": len(rows),
                "post_fee_pnl_usd": total_pnl,
                "post_fee_roi_pct": round(100.0 * total_pnl / total_cost, 6)
                if total_cost
                else None,
                "train_resolved": len(train),
                "train_post_fee_pnl_usd": round(
                    sum(row["post_fee_pnl_usd"] for row in train), 6
                ),
                "holdout_resolved": len(holdout),
                "holdout_post_fee_pnl_usd": holdout_pnl,
                "unresolved_old_fills": unresolved_old_by_cell[key],
                "fallback_book_rows": fallback_by_cell[key],
                "copyintent_parity_violations": parity_violations,
                "unique_copyintent_hashes": len({row["intent_hash"] for row in rows}),
                "checks": checks,
                "pass": all(checks.values()),
            }
        )
    passing = sorted(
        (row for row in reports if row["pass"]),
        key=lambda row: (-float(row["post_fee_roi_pct"] or 0.0), row["cell"]),
    )
    return {
        "schema_version": 1,
        "kind": "e5_delayed_offset_side_selective_promotion_packet",
        "flow_stage": "LIVE/LEARN/PROMOTE",
        "generated_at": datetime.fromtimestamp(now_s, timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "immutable_source": DEFAULT_STATE,
        "decision_rule": "each offset×side cell passes independently; aggregate pooling forbidden",
        "cells": reports,
        "passing_cells": [row["cell"] for row in passing],
        "selected_cell": passing[0]["cell"] if passing else None,
        "decision": (
            "ADMIT_HIGHEST_ROI_PASSING_CELL_PENDING_SOLE_GUARD_ACTIVATION"
            if passing
            else "PARK_DELAYED_OFFSET_METHOD_PAPER_ONLY"
        ),
        "live_mutation_allowed": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    packet = build_packet(state, now_s=datetime.now(timezone.utc).timestamp())
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
    temporary.replace(target)
    print(json.dumps(packet, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
