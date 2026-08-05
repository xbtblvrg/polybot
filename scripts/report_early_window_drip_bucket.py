#!/usr/bin/env python3
"""Report the preregistered early-window drip bucket for live scaling."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.mission import WALLET_COPY_MISSION_CONTRACT
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.pnl_truth import score_order

DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/wallet_copy_early_window_drip_bucket_latest.json"
DEFAULT_START_ISO = "2026-07-09T01:55:00+00:00"
DEFAULT_MAX_ENTRY_OFFSET_S = 60.0
DEFAULT_MIN_RESOLVED_FILLS_TO_RAISE = 10
DEFAULT_CURRENT_TRANCHE_USD = 1.25
DEFAULT_TARGET_TRANCHE_USD = 2.5


def parse_utc_ts(value: str) -> float:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).astimezone(UTC).timestamp()


def btc5m_window_start_from_slug(slug: str) -> int | None:
    match = re.search(r"(\d{9,})$", str(slug or ""))
    return int(match.group(1)) if match else None


def active_member_tranche_overrides() -> list[dict[str, Any]]:
    active_set = (
        WALLET_COPY_MISSION_CONTRACT.get("current_runtime_phase_contract", {}).get("active_live_set", {})
        if isinstance(WALLET_COPY_MISSION_CONTRACT, dict)
        else {}
    )
    members = active_set.get("members") if isinstance(active_set, dict) else []
    rows: list[dict[str, Any]] = []
    for member in members if isinstance(members, list) else []:
        if not isinstance(member, dict) or member.get("enabled") is False:
            continue
        policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
        tranche = member.get("drip_max_tranche_usd", policy.get("drip_max_tranche_usd"))
        if tranche is None:
            continue
        rows.append(
            {
                "candidate_id": member.get("candidate_id"),
                "source_wallet": str(member.get("source_wallet") or "").lower(),
                "policy_id": member.get("policy_id"),
                "drip_max_tranche_usd": float(tranche),
            }
        )
    return rows


def build_report(
    ledger: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    start_iso: str = DEFAULT_START_ISO,
    max_entry_offset_s: float = DEFAULT_MAX_ENTRY_OFFSET_S,
    min_resolved_fills_to_raise: int = DEFAULT_MIN_RESOLVED_FILLS_TO_RAISE,
    current_tranche_usd: float = DEFAULT_CURRENT_TRANCHE_USD,
    target_tranche_usd: float = DEFAULT_TARGET_TRANCHE_USD,
) -> dict[str, Any]:
    start_ts = parse_utc_ts(start_iso)
    rows: list[dict[str, Any]] = []
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        submitted_at = str(order.get("submitted_at") or order.get("updated_at") or "")
        if not submitted_at:
            continue
        submitted_ts = parse_utc_ts(submitted_at)
        if submitted_ts < start_ts:
            continue
        window_start = btc5m_window_start_from_slug(str(order.get("market_slug") or ""))
        if window_start is None:
            continue
        entry_offset_s = submitted_ts - float(window_start)
        if entry_offset_s < 0 or entry_offset_s >= float(max_entry_offset_s):
            continue
        event = score_order(order, resolutions)
        rows.append(
            {
                "submitted_at": submitted_at,
                "market_slug": event.get("market_slug"),
                "entry_offset_s": round(entry_offset_s, 6),
                "source_wallet": event.get("source_wallet"),
                "side": event.get("side"),
                "limit_price": event.get("limit_price"),
                "status": event.get("status"),
                "resolved": bool(event.get("resolved")),
                "winner": event.get("winner"),
                "cost_usd": event.get("cost_usd"),
                "shares": event.get("shares"),
                "pnl_usd": event.get("pnl_usd"),
                "cost_basis_source": event.get("cost_basis_source"),
            }
        )
    resolved = [row for row in rows if row.get("resolved")]
    cost = round(sum(float(row.get("cost_usd") or 0.0) for row in resolved), 6)
    pnl = round(sum(float(row.get("pnl_usd") or 0.0) for row in resolved), 6)
    roi = round((100.0 * pnl / cost), 6) if cost > 0 else 0.0
    raise_gate_pass = len(resolved) >= int(min_resolved_fills_to_raise) and roi > 0.0
    return {
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE",
        "bucket_id": "p1d_preregistered_early_window_drip_bucket",
        "basis": {
            "start_iso": start_iso,
            "max_entry_offset_s": float(max_entry_offset_s),
            "min_resolved_fills_to_raise": int(min_resolved_fills_to_raise),
            "current_tranche_usd": float(current_tranche_usd),
            "target_tranche_usd": float(target_tranche_usd),
            "source": "2026-07-09T02:05Z fable preregistration; early entry is submitted_at minus BTC 5m slug start",
        },
        "metrics": {
            "fills": len(rows),
            "resolved_fills": len(resolved),
            "cost_usd": cost,
            "pnl_usd": pnl,
            "roi_pct": roi,
            "raise_gate_pass": raise_gate_pass,
            "verdict": "RAISE_TO_TARGET_TRANCHE" if raise_gate_pass else "NO_RAISE_WAIT_FOR_N",
        },
        "runtime_tranche_overrides": active_member_tranche_overrides(),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--resolutions", type=Path, default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--start-iso", default=DEFAULT_START_ISO)
    parser.add_argument("--max-entry-offset-s", type=float, default=DEFAULT_MAX_ENTRY_OFFSET_S)
    args = parser.parse_args()

    ledger = json.loads(args.ledger.read_text())
    resolutions = load_resolutions(args.resolutions)
    report = build_report(
        ledger,
        resolutions,
        start_iso=args.start_iso,
        max_entry_offset_s=args.max_entry_offset_s,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["metrics"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
