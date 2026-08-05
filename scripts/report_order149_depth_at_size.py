#!/usr/bin/env python3
"""Measure observed CLOB depth/slippage at the $100/day and $300/day sizes."""

from __future__ import annotations

import argparse
import glob
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json

TARGETS = (12.138, 36.41)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _sweep(asks: list[dict[str, Any]], target_usd: float, max_price: float = 0.99) -> dict[str, float]:
    remaining = target_usd
    spent = 0.0
    shares = 0.0
    for row in sorted(asks, key=lambda item: float(item.get("price") or 0.0)):
        price = float(row.get("price") or 0.0)
        size = float(row.get("size") or 0.0)
        if price <= 0 or size <= 0 or price > max_price:
            continue
        take = min(remaining, price * size)
        spent += take
        shares += take / price
        remaining -= take
        if remaining <= 1e-9:
            break
    return {
        "spent": spent,
        "shares": shares,
        "fill_ratio": spent / target_usd if target_usd > 0 else 0.0,
        "vwap": spent / shares if shares else 0.0,
    }


def build_report(rows: Iterable[dict[str, Any]], targets: tuple[float, ...] = TARGETS) -> dict[str, Any]:
    band_rows = []
    assets: set[str] = set()
    for row in rows:
        asks = [item for item in (row.get("asks") or []) if isinstance(item, dict)]
        positive = [float(item.get("price") or 0.0) for item in asks if float(item.get("price") or 0.0) > 0]
        best_ask = min(positive) if positive else 0.0
        if not (0.25 <= best_ask <= 0.50):
            continue
        band_rows.append((row, asks, best_ask))
        assets.add(str(row.get("asset_id") or ""))
    target_reports = []
    for target in targets:
        slippages: list[float] = []
        capped_depth: list[float] = []
        full = 0
        capped_full = 0
        for _row, asks, best_ask in band_rows:
            sweep = _sweep(asks, target)
            if sweep["fill_ratio"] >= 0.999:
                full += 1
                slippages.append((sweep["vwap"] / best_ask - 1.0) * 10_000.0)
            cap_price = min(0.99, best_ask * 1.025)
            cap_sweep = _sweep(asks, target, max_price=cap_price)
            capped_depth.append(cap_sweep["spent"])
            if cap_sweep["fill_ratio"] >= 0.999:
                capped_full += 1
        count = len(band_rows)
        target_reports.append({
            "target_usd": target,
            "snapshot_count": count,
            "full_book_fill_count": full,
            "full_book_fill_rate": round(full / count, 6) if count else None,
            "within_250bps_of_best_ask_fill_count": capped_full,
            "within_250bps_of_best_ask_fill_rate": round(capped_full / count, 6) if count else None,
            "vwap_impact_bps_vs_best_ask": {
                "p50": round(_quantile(slippages, 0.5), 6) if slippages else None,
                "p95": round(_quantile(slippages, 0.95), 6) if slippages else None,
                "max": round(max(slippages), 6) if slippages else None,
            },
            "depth_usd_within_250bps_of_best_ask": {
                "p05": round(_quantile(capped_depth, 0.05), 6) if capped_depth else None,
                "p50": round(_quantile(capped_depth, 0.5), 6) if capped_depth else None,
                "p95": round(_quantile(capped_depth, 0.95), 6) if capped_depth else None,
            },
        })
    return {
        "schema_version": 1,
        "kind": "order149_depth_at_size",
        "flow_stage": "MEASURE/LIVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "entry_band": [0.25, 0.50],
        "snapshot_count": len(band_rows),
        "asset_count": len({asset for asset in assets if asset}),
        "targets": target_reports,
        "verdict": "MEASURED" if band_rows else "NO_IN_BAND_SNAPSHOTS",
        "interpretation_fence": "best ask anchors impact because snapshots do not carry source intent price; no cap raise is authorised by this report alone",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--book-snapshots", action="append", required=True)
    parser.add_argument("--output", default="data/research/order149_depth_at_size_latest.json")
    args = parser.parse_args()
    paths = sorted({Path(item) for pattern in args.book_snapshots for item in glob.glob(pattern)})
    rows = []
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    report = build_report(rows)
    report["source_files"] = [str(path) for path in paths]
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
