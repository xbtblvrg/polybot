#!/usr/bin/env python3
"""Resolve entry-price gate shadow rows and report the preregistered decision metric."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


def _jsonl(path: Path):
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


def _parse_ts(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _winner_from_resolution_row(row: dict[str, Any]) -> str:
    winner = str(row.get("winning_outcome") or "").strip().lower()
    if winner:
        return winner
    direction = str(row.get("direction") or "").strip().upper()
    if direction == "UP":
        return "up"
    if direction == "DOWN":
        return "down"
    return ""


def build_report(
    *,
    event_rows: list[dict[str, Any]],
    resolution_rows: list[dict[str, Any]],
    generated_at: str,
    deadline_utc: str,
    min_resolved_windows: int = 100,
) -> dict[str, Any]:
    resolutions: dict[str, str] = {}
    for row in resolution_rows:
        winner = _winner_from_resolution_row(row)
        if not winner:
            continue
        for key in (
            row.get("market"),
            row.get("condition_id"),
            row.get("market_slug"),
        ):
            normalized = str(key or "").lower()
            if normalized:
                resolutions[normalized] = winner

    unique: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        if str(row.get("event") or "") != "wallet_copy_live_entry_price_band_gate_counterfactual":
            continue
        key = str(row.get("counterfactual_id") or row.get("intent_id") or "")
        if key and key not in unique:
            unique[key] = row

    # The live fill-cap is one per BTC-5m market. Keep the first shadow row per
    # market so repeated guard cycles cannot inflate the preregistered window n.
    by_window: dict[str, dict[str, Any]] = {}
    for row in unique.values():
        window = str(row.get("market_slug") or row.get("condition_id") or "")
        if window and window not in by_window:
            by_window[window] = row

    resolved_rows: list[dict[str, Any]] = []
    for row in by_window.values():
        winner = resolutions.get(str(row.get("condition_id") or "").lower()) or resolutions.get(
            str(row.get("market_slug") or "").lower()
        )
        if not winner:
            continue
        shares = float(row.get("shares") or 0.0)
        cost = float(row.get("copy_size_usd") or shares * float(row.get("limit_price") or 0.0))
        fee = float(row.get("expected_fee_usd") or 0.0)
        won = str(row.get("outcome") or row.get("side") or "").strip().lower() == winner
        post_fee = (shares if won else 0.0) - cost - fee
        resolved_rows.append(
            {
                "counterfactual_id": row.get("counterfactual_id"),
                "market_slug": row.get("market_slug"),
                "condition_id": row.get("condition_id"),
                "outcome": row.get("outcome"),
                "winning_outcome": winner,
                "won": won,
                "copy_size_usd": round(cost, 6),
                "expected_fee_usd": round(fee, 6),
                "ungated_counterfactual_post_fee_pnl_usd": round(post_fee, 6),
            }
        )

    counterfactual_pnl = round(
        sum(float(row["ungated_counterfactual_post_fee_pnl_usd"]) for row in resolved_rows), 6
    )
    gated_pnl = 0.0
    improvement = round(gated_pnl - counterfactual_pnl, 6)
    now = _parse_ts(generated_at) or datetime.now(timezone.utc)
    deadline = _parse_ts(deadline_utc)
    boundary_reached = len(resolved_rows) >= int(min_resolved_windows) or bool(deadline and now >= deadline)
    status = "PASS" if boundary_reached and improvement > 0 else "FAIL" if boundary_reached else "ACCRUING"
    return {
        "schema_version": 1,
        "kind": "entry_price_loss_band_gate_counterfactual",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "generated_at": generated_at,
        "status": status,
        "experiment_id": "entry-price-loss-band-gate-20260720",
        "deadline_utc": deadline_utc,
        "min_resolved_gated_flow_windows": int(min_resolved_windows),
        "decision_boundary_reached": boundary_reached,
        "unique_suppressed_intents": len(unique),
        "suppressed_windows": len(by_window),
        "resolved_gated_flow_windows": len(resolved_rows),
        "pending_resolution_windows": max(0, len(by_window) - len(resolved_rows)),
        "gated_live_post_fee_pnl_usd": gated_pnl,
        "ungated_counterfactual_post_fee_pnl_usd": counterfactual_pnl,
        "gated_minus_ungated_post_fee_pnl_usd": improvement,
        "copyintent_parity_violations": 0,
        "live_mutation": False,
        "rows": resolved_rows[-200:],
        "decision_rule": "keep only if gated-minus-ungated post-fee PnL is positive at the first sample/time boundary",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument(
        "--resolution-log",
        default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
        help="Canonical BTC-5m resolved-winner rows; the CLOB WS capture is not a live resolution feed.",
    )
    parser.add_argument("--output", default="data/research/entry_price_loss_band_gate_counterfactual_latest.json")
    parser.add_argument("--deadline-utc", default="2026-07-27T16:50:00Z")
    parser.add_argument("--min-resolved-windows", type=int, default=100)
    args = parser.parse_args()
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report = build_report(
        event_rows=list(_jsonl(ROOT / args.event_log) or []),
        resolution_rows=list(_jsonl(ROOT / args.resolution_log) or []),
        generated_at=generated_at,
        deadline_utc=args.deadline_utc,
        min_resolved_windows=args.min_resolved_windows,
    )
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps({key: report[key] for key in ("status", "resolved_gated_flow_windows", "gated_minus_ungated_post_fee_pnl_usd")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
