#!/usr/bin/env python3
"""Report the current-day e6db realized-fill payoff autopsy."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_scorecard import _day_bounds, _default_resolutions_path  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import build_pnl_truth  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402

E6DB = "0xe6db20932faf0f9780acf75d95c74c9984407dac"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_LATEST = ROOT / "data/research/e6db_loser_autopsy_latest.json"


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _round(value: float | int | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _pct(value: float | int | None) -> float | None:
    if value is None:
        return None
    return round(100.0 * float(value), 6)


def _side_outcome(side: Any) -> str:
    text = str(side or "").upper()
    if text == "YES":
        return "Up"
    if text == "NO":
        return "Down"
    return ""


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_positive(container: dict[str, Any], keys: tuple[str, ...]) -> tuple[float, str]:
    for key in keys:
        value = num(container.get(key))
        if value > 0:
            return round(value, 6), key
    return 0.0, ""


def _expected_fee(order: dict[str, Any]) -> tuple[float, str]:
    source_intent = _dict(order.get("source_intent"))
    metadata = _dict(source_intent.get("metadata"))
    containers = (
        ("order.expected_fee_gate", _dict(order.get("expected_fee_gate"))),
        ("order.expected_vs_realized_fee", _dict(order.get("expected_vs_realized_fee"))),
        ("source_intent.metadata.expected_fee_gate", _dict(metadata.get("expected_fee_gate"))),
    )
    keys = ("expected_fee_usd", "response_expected_fee_usd", "pre_submit_expected_fee_usd")
    for source, container in containers:
        amount, key = _first_positive(container, keys)
        if amount > 0:
            return amount, f"{source}.{key}"
    return 0.0, ""


def _entry_price(order: dict[str, Any], event: dict[str, Any]) -> tuple[float, str]:
    result = _dict(order.get("trade_result"))
    amount, key = _first_positive(result, ("response_fill_price", "entry_price"))
    if amount > 0:
        return amount, f"trade_result.{key}"
    return round(num(event.get("limit_price")), 6), "pnl_truth.limit_price"


def _truth_events(
    *,
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    day: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _, start_ts, end_ts = _day_bounds(day)
    truth = build_pnl_truth(
        {"orders": ledger.get("orders") if isinstance(ledger.get("orders"), list) else []},
        resolutions,
        start_ts=start_ts,
        end_ts=end_ts,
        receipt_costs={},
        actual_trade_costs={},
    )
    events = [
        row
        for row in truth.get("events", [])
        if isinstance(row, dict)
        and str(row.get("source_wallet") or "").lower() == E6DB
        and str(row.get("status") or "").upper() == "FILLED"
        and bool(row.get("resolved"))
        and str(row.get("market_slug") or "").startswith("btc-updown-5m-")
    ]
    return events, truth


def _orders_by_id(ledger: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        for key in (order.get("order_id"), order.get("intent_id")):
            text = str(key or "").strip()
            if text:
                rows[text] = order
    return rows


def _fill_row(event: dict[str, Any], order: dict[str, Any]) -> dict[str, Any]:
    fee, fee_source = _expected_fee(order)
    entry_price, entry_price_source = _entry_price(order, event)
    side = str(event.get("side") or "").upper()
    winner = str(event.get("winner") or "").upper()
    pnl = float(event.get("pnl_usd") or 0.0)
    return {
        "flow_stage": "LIVE/DEFEND",
        "market_slug": event.get("market_slug"),
        "order_id": event.get("order_id"),
        "intent_id": event.get("intent_id"),
        "submitted_at": event.get("submitted_at"),
        "side": side,
        "copied_outcome": _side_outcome(side),
        "entry_price": entry_price,
        "entry_price_source": entry_price_source,
        "size_usd": _round(float(event.get("cost_usd") or 0.0)),
        "shares": _round(float(event.get("shares") or 0.0)),
        "fee_usd": fee,
        "fee_source": fee_source or "not_recorded",
        "resolved_winner_side": winner,
        "resolved_outcome": _side_outcome(winner),
        "result": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT",
        "payout_usd": _round(float(event.get("payout_usd") or 0.0)),
        "pnl_usd": _round(pnl),
        "cost_basis_source": event.get("cost_basis_source"),
    }


def _window_rows(fill_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in fill_rows:
        grouped[str(row.get("market_slug") or "")].append(row)
    rows: list[dict[str, Any]] = []
    for slug, fills in grouped.items():
        fills.sort(key=lambda row: str(row.get("submitted_at") or ""))
        cost = sum(float(row.get("size_usd") or 0.0) for row in fills)
        shares = sum(float(row.get("shares") or 0.0) for row in fills)
        fee = sum(float(row.get("fee_usd") or 0.0) for row in fills)
        payout = sum(float(row.get("payout_usd") or 0.0) for row in fills)
        pnl = sum(float(row.get("pnl_usd") or 0.0) for row in fills)
        side_counts = defaultdict(int)
        for row in fills:
            side_counts[str(row.get("side") or "")] += 1
        sides = sorted(side_counts)
        primary = fills[0]
        rows.append(
            {
                "flow_stage": "LIVE/DEFEND",
                "market_slug": slug,
                "fills": len(fills),
                "order_ids": [row.get("order_id") for row in fills],
                "submitted_at": primary.get("submitted_at"),
                "side": sides[0] if len(sides) == 1 else "MIXED",
                "side_counts": dict(sorted(side_counts.items())),
                "copied_outcome": primary.get("copied_outcome") if len(sides) == 1 else "Mixed",
                "entry_price": _round(cost / shares) if shares > 0 else primary.get("entry_price"),
                "size_usd": _round(cost),
                "shares": _round(shares),
                "fee_usd": _round(fee),
                "resolved_winner_side": primary.get("resolved_winner_side"),
                "resolved_outcome": primary.get("resolved_outcome"),
                "result": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT",
                "payout_usd": _round(payout),
                "pnl_usd": _round(pnl),
            }
        )
    rows.sort(key=lambda row: str(row.get("market_slug") or ""))
    return rows


def _classification(summary: dict[str, Any]) -> dict[str, Any]:
    realized = float(summary.get("realized_pnl_usd") or 0.0)
    expected_fee = float(summary.get("expected_fee_usd") or 0.0)
    pre_fee = float(summary.get("diagnostic_pre_expected_fee_pnl_usd") or 0.0)
    actual = float(summary.get("actual_win_rate") or 0.0)
    required = float(summary.get("required_win_rate_at_payoff_shape") or 0.0)
    gap = actual - required
    if realized < 0 and pre_fee >= 0 and expected_fee >= abs(realized):
        label = "fee_drag"
    elif realized < 0 and gap < -0.02 and pre_fee < 0:
        label = "structural_negative_edge"
    else:
        label = "variance"
    return {
        "classification": label,
        "rule": (
            "fee_drag if expected fees alone flip the sign; structural_negative_edge if pre-fee diagnostic remains "
            "negative and actual win rate trails required payoff-shape breakeven by more than 2pp; otherwise variance"
        ),
        "actual_minus_required_win_rate_pp": _round(100.0 * gap),
        "pre_expected_fee_still_negative": pre_fee < 0,
        "fees_flip_sign": realized < 0 <= pre_fee,
    }


def build_autopsy(*, ledger_path: Path, resolutions_path: Path, day: str) -> dict[str, Any]:
    ledger = load_json(ledger_path, default={})
    if not isinstance(ledger, dict):
        ledger = {}
    resolutions = load_resolutions(resolutions_path)
    events, truth = _truth_events(ledger=ledger, resolutions=resolutions, day=day)
    order_index = _orders_by_id(ledger)
    fill_rows = [_fill_row(event, order_index.get(str(event.get("order_id") or ""), {})) for event in events]
    window_rows = _window_rows(fill_rows)
    pnl_values = [float(row.get("pnl_usd") or 0.0) for row in window_rows]
    wins = [pnl for pnl in pnl_values if pnl > 0]
    losses = [pnl for pnl in pnl_values if pnl < 0]
    gross_win = sum(wins)
    gross_loss_abs = abs(sum(losses))
    avg_win = gross_win / len(wins) if wins else 0.0
    avg_loss_abs = gross_loss_abs / len(losses) if losses else 0.0
    required_win_rate = avg_loss_abs / (avg_loss_abs + avg_win) if (avg_loss_abs + avg_win) > 0 else None
    actual_win_rate = len(wins) / len(window_rows) if window_rows else None
    expected_fee = sum(float(row.get("fee_usd") or 0.0) for row in fill_rows)
    realized = sum(pnl_values)
    gross_swing = gross_win + gross_loss_abs
    gap_pp = (actual_win_rate - required_win_rate) * 100.0 if actual_win_rate is not None and required_win_rate is not None else None
    gap_sigma_pp = (
        100.0 * math.sqrt(required_win_rate * (1.0 - required_win_rate) / len(window_rows))
        if required_win_rate is not None and window_rows
        else None
    )
    summary = {
        "flow_stage": "LIVE/DEFEND",
        "source_wallet": E6DB,
        "day_utc": day,
        "fills": len(fill_rows),
        "resolved_windows": len(window_rows),
        "winning_windows": len(wins),
        "losing_windows": len(losses),
        "flat_windows": len([pnl for pnl in pnl_values if pnl == 0.0]),
        "realized_pnl_usd": _round(realized),
        "gross_win_pnl_usd": _round(gross_win),
        "gross_loss_abs_usd": _round(gross_loss_abs),
        "avg_win_per_winner_usd": _round(avg_win),
        "avg_loss_per_loser_abs_usd": _round(avg_loss_abs),
        "expected_fee_usd": _round(expected_fee),
        "diagnostic_pre_expected_fee_pnl_usd": _round(realized + expected_fee),
        "expected_fee_share_of_gross_pnl_swing_pct": _pct(expected_fee / gross_swing) if gross_swing else None,
        "expected_fee_share_of_realized_loss_pct": _pct(expected_fee / abs(realized)) if realized < 0 else None,
        "actual_win_rate": _round(actual_win_rate),
        "actual_win_rate_pct": _pct(actual_win_rate),
        "required_win_rate_at_payoff_shape": _round(required_win_rate),
        "required_win_rate_at_payoff_shape_pct": _pct(required_win_rate),
        "gap_sigma_pp": _round(gap_sigma_pp),
        "gap_in_sigma": _round(gap_pp / gap_sigma_pp) if gap_pp is not None and gap_sigma_pp else None,
        "basis": "response_filled_size_usd realized PnL; expected_fee_usd is diagnostic and not subtracted again",
    }
    summary.update(_classification(summary))
    return {
        "kind": "e6db_loser_autopsy",
        "flow_stage": "LIVE/DEFEND",
        "generated_at": utc_now_iso(),
        "ledger": _rel(ledger_path),
        "resolutions": _rel(resolutions_path),
        "truth_generated_at": truth.get("generated_at"),
        "summary": summary,
        "per_window": window_rows,
        "per_fill": fill_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="", help="UTC day YYYY-MM-DD; default is today UTC.")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default="", help="Resolution JSONL path; default matches daily_scorecard.")
    parser.add_argument("--output", default=str(DEFAULT_LATEST))
    parser.add_argument("--day-output", default="", help="Optional dated output path; defaults to data/research/e6db_loser_autopsy_<day>.json.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    day, _, _ = _day_bounds(args.day)
    resolutions = Path(args.resolutions or _default_resolutions_path())
    output = Path(args.output)
    day_output = Path(args.day_output or ROOT / "data/research" / f"e6db_loser_autopsy_{day}.json")
    report = build_autopsy(ledger_path=Path(args.ledger), resolutions_path=resolutions, day=day)
    atomic_write_json(output, report)
    if day_output != output:
        atomic_write_json(day_output, report)
    summary = dict(report["summary"])
    summary["output"] = _rel(output)
    summary["day_output"] = _rel(day_output)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
