#!/usr/bin/env python3
"""Explain the prospective E5 paper/live outcome divergence."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


LANE = "e5_maker_first_btc5m_v1"


def _price_band(price: float) -> str:
    if price <= 0.25:
        return "00_00_25"
    if price <= 0.40:
        return "01_25_40"
    if price <= 0.45:
        return "02_40_45"
    return "03_45_50"


def _offset_band(offset_s: float) -> str:
    if offset_s < 60:
        return "00_0_60s"
    if offset_s < 180:
        return "01_60_180s"
    return "02_180_270s"


def _aggregate(rows: Iterable[dict[str, Any]], field: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row[field])].append(row)
    result: dict[str, Any] = {}
    for key, items in sorted(groups.items()):
        resolved = [row for row in items if row["resolved"]]
        cost = sum(float(row["cost_usd"]) for row in resolved)
        pnl = sum(float(row["pnl_usd"]) for row in resolved)
        result[key] = {
            "orders": len(items),
            "filled_orders": sum(bool(row["filled"]) for row in items),
            "resolved_fills": len(resolved),
            "cost_usd": round(cost, 6),
            "pnl_usd": round(pnl, 6),
            "roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
        }
    return result


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row["resolved"]]
    filled = [row for row in rows if row["filled"]]
    cost = sum(float(row["cost_usd"]) for row in resolved)
    pnl = sum(float(row["pnl_usd"]) for row in resolved)
    timing = [float(row["fill_observation_delay_s"]) for row in filled if row["fill_observation_delay_s"] is not None]
    return {
        "orders": len(rows),
        "filled_orders": len(filled),
        "fill_rate_pct": round(100.0 * len(filled) / len(rows), 6) if rows else None,
        "resolved_fills": len(resolved),
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
        "median_fill_observation_delay_s": round(statistics.median(timing), 6) if timing else None,
    }


def build_report(paper: dict[str, Any], ledger: dict[str, Any], resolutions: list[dict[str, Any]]) -> dict[str, Any]:
    winners = {
        str(row.get("market_slug")): str(row.get("direction", "")).title()
        for row in resolutions
        if row.get("market_slug") and row.get("direction")
    }
    paper_rows: list[dict[str, Any]] = []
    for row in paper.get("executions", []):
        filled = float(row.get("filled_shares") or 0.0) > 0
        resolved = bool(row.get("resolved")) and filled
        events = row.get("fill_events") or []
        delay = float(events[0]["event_ts"]) - float(row["quote_ts"]) if events else None
        paper_rows.append(
            {
                "price_band": _price_band(float(row["quote_price"])),
                "side": str(row.get("outcome") or "").upper(),
                "window_offset_band": _offset_band(float(row["quote_ts"]) - (float(row["window_end_s"]) - 300.0)),
                "lifecycle": str(row.get("execution_status") or "UNKNOWN"),
                "filled": filled,
                "resolved": resolved,
                "cost_usd": float(row.get("filled_size_usd") or 0.0) if resolved else 0.0,
                "pnl_usd": float(row.get("post_fee_pnl_usd") or 0.0) if resolved else 0.0,
                "fill_observation_delay_s": delay,
            }
        )

    live_rows: list[dict[str, Any]] = []
    for row in ledger.get("orders", []):
        if (row.get("maker_cancel") or {}).get("execution_lane") != LANE:
            continue
        lifecycle = row.get("lifecycle") or []
        accepted = any(event.get("status") == "LIVE_SUBMITTED" for event in lifecycle)
        if not accepted or str(row.get("order_id", "")).startswith(("skip_", "lo_")):
            continue
        submitted = datetime.fromisoformat(str(row["submitted_at"]).replace("Z", "+00:00")).timestamp()
        fill_event = next((event for event in lifecycle if event.get("status") == "LIVE_MAKER_FILLED"), None)
        shares = float(row.get("fill_size_shares") or 0.0)
        filled = shares > 0
        winner = winners.get(str(row.get("market_slug")), "")
        outcome = str(row.get("outcome") or "").title()
        resolved = bool(winner) and filled
        cost = float(row.get("filled_size_usd") or 0.0) if resolved else 0.0
        payout = shares if resolved and outcome == winner else 0.0
        window_start = float(str(row["market_slug"]).rsplit("-", 1)[-1])
        delay = None
        if fill_event:
            delay = datetime.fromisoformat(str(fill_event["ts"]).replace("Z", "+00:00")).timestamp() - submitted
        status = str(row.get("final_status") or "UNKNOWN").upper()
        if filled and shares + 1e-6 < float(row.get("requested_shares") or 5.0):
            status = "PARTIAL_FILL_CANCEL_RESIDUAL"
        live_rows.append(
            {
                "price_band": _price_band(float(row["limit_price"])),
                "side": outcome.upper(),
                "window_offset_band": _offset_band(submitted - window_start),
                "lifecycle": status,
                "filled": filled,
                "resolved": resolved,
                "cost_usd": cost,
                "pnl_usd": payout - cost if resolved else 0.0,
                "fill_observation_delay_s": delay,
            }
        )

    paper_summary = _summary(paper_rows)
    live_summary = _summary(live_rows)
    paper_roi = paper_summary["roi_pct"]
    live_roi = live_summary["roi_pct"]
    paper_fill = paper_summary["fill_rate_pct"]
    live_fill = live_summary["fill_rate_pct"]
    return {
        "schema_version": 1,
        "kind": "e5_paper_live_divergence",
        "flow_stage": "LEARN/ROTATE",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lane": LANE,
        "decision": "diagnose historical-paper optimism; live lane remains demoted",
        "paper": {
            "source_contract": paper.get("contract"),
            "summary": paper_summary,
            "by_entry_price": _aggregate(paper_rows, "price_band"),
            "by_side": _aggregate(paper_rows, "side"),
            "by_window_offset": _aggregate(paper_rows, "window_offset_band"),
            "by_partial_cancel": _aggregate(paper_rows, "lifecycle"),
        },
        "live": {
            "summary": live_summary,
            "by_entry_price": _aggregate(live_rows, "price_band"),
            "by_side": _aggregate(live_rows, "side"),
            "by_window_offset": _aggregate(live_rows, "window_offset_band"),
            "by_partial_cancel": _aggregate(live_rows, "lifecycle"),
        },
        "optimism_components": {
            "realized_roi_gap_percentage_points": round(float(paper_roi) - float(live_roi), 6)
            if paper_roi is not None and live_roi is not None
            else None,
            "terminal_fill_rate_gap_percentage_points": round(float(paper_fill) - float(live_fill), 6)
            if paper_fill is not None and live_fill is not None
            else None,
            "named_primary_component": "POST_QUOTE_TRADE_THROUGH_IS_NOT_QUEUE_FILL",
            "interpretation": (
                "The paper regrade treats cumulative later SELL volume at/below the quote as our fill. "
                "Live results include queue position, partial/cancel lifecycle, actual entry timing, and realized winner."
            ),
        },
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=Path("data/research/wallet_copy_live_execution_state.json"))
    parser.add_argument("--resolutions", type=Path, default=Path("data/research/btc_resolutions_from_btcusdt_ticks.jsonl"))
    parser.add_argument("--output", type=Path, default=Path("data/research/e5_paper_live_divergence_latest.json"))
    args = parser.parse_args()
    report = build_report(
        json.loads(args.paper.read_text()),
        json.loads(args.ledger.read_text()),
        _read_jsonl(args.resolutions),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["optimism_components"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
