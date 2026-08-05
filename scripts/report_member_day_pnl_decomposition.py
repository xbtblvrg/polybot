#!/usr/bin/env python3
"""Report day-scoped live PnL split by active-set member.

Flow stage: LIVE/LEARN/DEFEND. This is a reporting-only artifact for Fable's
20:00Z clause checks and midnight verdicts; it never mutates live routing.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import order_ts, score_order, validate_resolutions_nonempty_for_fills  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT_DIR = ROOT / "data/research"


def _default_day() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _day_bounds(day: str) -> tuple[str, float, float]:
    start = datetime.strptime(day or _default_day(), "%Y-%m-%d").replace(tzinfo=UTC)
    end = start + timedelta(days=1)
    return start.date().isoformat(), start.timestamp(), end.timestamp()


def _default_resolutions_path() -> Path:
    candidates = [
        path
        for path in (ROOT / "data/research").glob("btc_resolutions_*.jsonl")
        if path.is_file()
    ]
    if not candidates:
        return ROOT / "data/research/btc_resolutions_from_gamma_live_ledger_20260705_1857.jsonl"
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return text or "unknown"


def _expected_fee_usd(order: dict[str, Any]) -> float:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    candidates = [
        order.get("expected_fee_gate"),
        order.get("expected_vs_realized_fee"),
        trade_result.get("expected_fee_gate"),
        trade_result.get("expected_vs_realized_fee"),
        metadata.get("expected_fee_gate"),
        metadata.get("expected_vs_realized_fee"),
    ]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in ("expected_fee_usd", "pre_submit_expected_fee_usd", "response_expected_fee_usd"):
            value = num(candidate.get(key), 0.0)
            if value > 0:
                return value
    return 0.0


def _empty_member(wallet: str, active_member: dict[str, Any] | None = None) -> dict[str, Any]:
    active_member = active_member or {}
    return {
        "source_wallet": wallet,
        "candidate_id": active_member.get("candidate_id", ""),
        "policy_id": active_member.get("policy_id", ""),
        "enabled": bool(active_member.get("enabled", False)) if active_member else False,
        "orders": 0,
        "fills": 0,
        "rejects": 0,
        "resolved_fills": 0,
        "unresolved_fills": 0,
        "filled_cost_usd": 0.0,
        "resolved_cost_usd": 0.0,
        "payout_usd": 0.0,
        "pnl_usd": 0.0,
        "expected_fee_usd": 0.0,
        "resolved_expected_fee_usd": 0.0,
        "avg_filled_size_usd": 0.0,
        "avg_resolved_filled_size_usd": 0.0,
        "expected_fee_share_of_filled_cost_pct": 0.0,
        "resolved_expected_fee_share_of_resolved_cost_pct": 0.0,
        "expected_fee_share_of_day_fee_pct": 0.0,
        "resolved_expected_fee_share_of_day_fee_pct": 0.0,
        "win_fills": 0,
        "loss_fills": 0,
        "flat_fills": 0,
        "latest_submitted_at": "",
    }


def _active_members(guard_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    runtime = guard_state.get("active_set_runtime") if isinstance(guard_state.get("active_set_runtime"), dict) else {}
    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for member in members:
        if not isinstance(member, dict):
            continue
        wallet = _norm_wallet(member.get("source_wallet"))
        if wallet and wallet != "unknown":
            out[wallet] = member
    return out


def _round_member(row: dict[str, Any], *, total_fee: float, total_resolved_fee: float) -> dict[str, Any]:
    fills = int(row.get("fills") or 0)
    resolved_fills = int(row.get("resolved_fills") or 0)
    filled_cost = float(row.get("filled_cost_usd") or 0.0)
    resolved_cost = float(row.get("resolved_cost_usd") or 0.0)
    expected_fee = float(row.get("expected_fee_usd") or 0.0)
    resolved_expected_fee = float(row.get("resolved_expected_fee_usd") or 0.0)
    row["filled_cost_usd"] = round(filled_cost, 6)
    row["resolved_cost_usd"] = round(resolved_cost, 6)
    row["payout_usd"] = round(float(row.get("payout_usd") or 0.0), 6)
    row["pnl_usd"] = round(float(row.get("pnl_usd") or 0.0), 6)
    row["expected_fee_usd"] = round(expected_fee, 6)
    row["resolved_expected_fee_usd"] = round(resolved_expected_fee, 6)
    row["avg_filled_size_usd"] = round(filled_cost / fills, 6) if fills else 0.0
    row["avg_resolved_filled_size_usd"] = round(resolved_cost / resolved_fills, 6) if resolved_fills else 0.0
    row["expected_fee_share_of_filled_cost_pct"] = round(100.0 * expected_fee / filled_cost, 6) if filled_cost else 0.0
    row["resolved_expected_fee_share_of_resolved_cost_pct"] = (
        round(100.0 * resolved_expected_fee / resolved_cost, 6) if resolved_cost else 0.0
    )
    row["expected_fee_share_of_day_fee_pct"] = round(100.0 * expected_fee / total_fee, 6) if total_fee else 0.0
    row["resolved_expected_fee_share_of_day_fee_pct"] = (
        round(100.0 * resolved_expected_fee / total_resolved_fee, 6) if total_resolved_fee else 0.0
    )
    return row


def build_report(
    *,
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    guard_state: dict[str, Any],
    day: str,
) -> dict[str, Any]:
    day, start_ts, end_ts = _day_bounds(day)
    active = _active_members(guard_state)
    by_member: dict[str, dict[str, Any]] = {
        wallet: _empty_member(wallet, member)
        for wallet, member in active.items()
    }
    status_counts: Counter[str] = Counter()
    cost_basis_counts: Counter[str] = Counter()
    events: list[dict[str, Any]] = []
    total_fee = 0.0
    total_resolved_fee = 0.0
    total = _empty_member("TOTAL")
    total["enabled"] = False

    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        ts = order_ts(order)
        if ts is None or ts < start_ts or ts >= end_ts:
            continue
        event = score_order(order, resolutions, receipt_costs={}, actual_trade_costs={})
        wallet = _norm_wallet(event.get("source_wallet"))
        row = by_member.setdefault(wallet, _empty_member(wallet, active.get(wallet)))
        status = str(event.get("status") or "").upper()
        status_counts[status] += 1
        row["orders"] += 1
        total["orders"] += 1
        row["latest_submitted_at"] = max(str(row.get("latest_submitted_at") or ""), str(event.get("submitted_at") or ""))
        total["latest_submitted_at"] = max(str(total.get("latest_submitted_at") or ""), str(event.get("submitted_at") or ""))
        if status == "REJECTED":
            row["rejects"] += 1
            total["rejects"] += 1
        elif status == "FILLED":
            fee = _expected_fee_usd(order)
            cost = float(event.get("cost_usd") or 0.0)
            payout = float(event.get("payout_usd") or 0.0)
            pnl = float(event.get("pnl_usd") or 0.0)
            row["fills"] += 1
            total["fills"] += 1
            row["filled_cost_usd"] = round(float(row["filled_cost_usd"]) + cost, 6)
            total["filled_cost_usd"] = round(float(total["filled_cost_usd"]) + cost, 6)
            row["expected_fee_usd"] = round(float(row["expected_fee_usd"]) + fee, 6)
            total["expected_fee_usd"] = round(float(total["expected_fee_usd"]) + fee, 6)
            total_fee = round(total_fee + fee, 6)
            cost_basis_counts[str(event.get("cost_basis_source") or "unknown")] += 1
            if event.get("resolved"):
                row["resolved_fills"] += 1
                total["resolved_fills"] += 1
                row["resolved_cost_usd"] = round(float(row["resolved_cost_usd"]) + cost, 6)
                total["resolved_cost_usd"] = round(float(total["resolved_cost_usd"]) + cost, 6)
                row["payout_usd"] = round(float(row["payout_usd"]) + payout, 6)
                total["payout_usd"] = round(float(total["payout_usd"]) + payout, 6)
                row["pnl_usd"] = round(float(row["pnl_usd"]) + pnl, 6)
                total["pnl_usd"] = round(float(total["pnl_usd"]) + pnl, 6)
                row["resolved_expected_fee_usd"] = round(float(row["resolved_expected_fee_usd"]) + fee, 6)
                total["resolved_expected_fee_usd"] = round(float(total["resolved_expected_fee_usd"]) + fee, 6)
                total_resolved_fee = round(total_resolved_fee + fee, 6)
                if pnl > 0:
                    row["win_fills"] += 1
                    total["win_fills"] += 1
                elif pnl < 0:
                    row["loss_fills"] += 1
                    total["loss_fills"] += 1
                else:
                    row["flat_fills"] += 1
                    total["flat_fills"] += 1
            else:
                row["unresolved_fills"] += 1
                total["unresolved_fills"] += 1
        events.append(
            {
                "submitted_at": event.get("submitted_at"),
                "source_wallet": wallet,
                "status": status,
                "market_slug": event.get("market_slug"),
                "order_id": event.get("order_id"),
                "cost_usd": event.get("cost_usd"),
                "pnl_usd": event.get("pnl_usd"),
                "resolved": bool(event.get("resolved")),
                "expected_fee_usd": _expected_fee_usd(order) if status == "FILLED" else 0.0,
            }
        )

    members = [
        _round_member(row, total_fee=total_fee, total_resolved_fee=total_resolved_fee)
        for row in by_member.values()
    ]
    members.sort(key=lambda row: (float(row.get("pnl_usd") or 0.0), str(row.get("source_wallet") or "")))
    return {
        "kind": "wallet_copy_member_day_pnl_decomposition",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "reporting_only": True,
        "generated_at": utc_now_iso(),
        "day_utc": day,
        "basis": "response_filled_size_usd",
        "window": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_iso": datetime.fromtimestamp(start_ts, tz=UTC).isoformat().replace("+00:00", "Z"),
            "end_iso": datetime.fromtimestamp(end_ts, tz=UTC).isoformat().replace("+00:00", "Z"),
        },
        "status_counts": dict(sorted(status_counts.items())),
        "cost_basis_counts": dict(sorted(cost_basis_counts.items())),
        "active_member_count": len(active),
        "active_wallets": sorted(active),
        "total": _round_member(total, total_fee=total_fee, total_resolved_fee=total_resolved_fee),
        "members": members,
        "recent_events": events[-20:],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default=_default_day(), help="UTC day YYYY-MM-DD; default is today UTC.")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--resolutions", default=str(_default_resolutions_path()))
    parser.add_argument("--output", default="", help="Output path; default is day-scoped under data/research.")
    parser.add_argument("--write-latest", action="store_true", help="Also write wallet_copy_member_day_pnl_decomposition_latest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ledger = load_json(args.ledger, default={})
    guard_state = load_json(args.guard_state, default={})
    resolutions = load_resolutions(args.resolutions)
    validate_resolutions_nonempty_for_fills(ledger, resolutions, resolutions_path=args.resolutions)
    report = build_report(
        ledger=ledger if isinstance(ledger, dict) else {},
        guard_state=guard_state if isinstance(guard_state, dict) else {},
        resolutions=resolutions,
        day=str(args.day),
    )
    day = str(report.get("day_utc") or args.day)
    output = Path(args.output) if args.output else DEFAULT_OUTPUT_DIR / f"wallet_copy_member_day_pnl_decomposition_{day}.json"
    atomic_write_json(output, report)
    print(f"wrote {output}")
    if args.write_latest:
        latest = DEFAULT_OUTPUT_DIR / "wallet_copy_member_day_pnl_decomposition_latest.json"
        atomic_write_json(latest, report)
        print(f"wrote {latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
