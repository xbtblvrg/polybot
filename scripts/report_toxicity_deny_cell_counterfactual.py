#!/usr/bin/env python3
"""Report active-member toxicity deny cells without changing the denylist."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json


def _load_json(path: str | Path, default: Any) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        return json.loads(target.read_text())
    except Exception:
        return default


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _bucket_for_price(price: float | None) -> str | None:
    if price is None:
        return None
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    if price <= 1.0:
        return "03_70_100"
    return None


def _active_wallets(guard_state: dict[str, Any]) -> set[str]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    return {
        str(member.get("source_wallet") or "").lower()
        for member in members
        if isinstance(member, dict) and member.get("source_wallet")
    }


def _group_index(report: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    groups = report.get("groups") if isinstance(report.get("groups"), list) else []
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in groups:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        bucket = str(row.get("price_bucket") or "")
        if wallet and bucket:
            index[(wallet, bucket)] = row
    return index


def _blocked_index(guard_state: dict[str, Any], active_wallets: set[str]) -> dict[tuple[str, str], dict[str, Any]]:
    participation = (
        guard_state.get("window_participation")
        if isinstance(guard_state.get("window_participation"), dict)
        else {}
    )
    rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    blocked: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("dominant_skip_reason") != "toxicity_protection":
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if wallet not in active_wallets:
            continue
        price = _as_float(row.get("source_inventory_vwap")) or _as_float(row.get("limit_price"))
        bucket = _bucket_for_price(price)
        if not bucket:
            continue
        key = (wallet, bucket)
        entry = blocked.setdefault(
            key,
            {
                "blocked_intents_this_cycle": 0,
                "blocked_usd_this_cycle": 0.0,
                "sample_intents": [],
                "sample_markets": [],
            },
        )
        entry["blocked_intents_this_cycle"] += 1
        entry["blocked_usd_this_cycle"] = round(
            float(entry["blocked_usd_this_cycle"]) + float(_as_float(row.get("guard_sized_copy_usd")) or 0.0),
            6,
        )
        if row.get("intent_id"):
            entry["sample_intents"].append(row.get("intent_id"))
        if row.get("market_slug"):
            entry["sample_markets"].append(row.get("market_slug"))
    return blocked


def _metric(source: dict[str, Any], metric_name: str) -> dict[str, Any]:
    metric = source.get(metric_name) if isinstance(source.get(metric_name), dict) else {}
    return {
        "count": _as_int(metric.get("count")),
        "roi_pct": _as_float(metric.get("roi_pct")),
        "pnl_usd": _as_float(metric.get("pnl_usd")),
        "stake_usd": _as_float(metric.get("stake_usd")),
    }


def _flag(row: dict[str, Any]) -> str:
    deny_rule = str(row.get("deny_rule") or "")
    live = row.get("live_fills") if isinstance(row.get("live_fills"), dict) else {}
    live_roi = _as_float(live.get("roi_pct"))
    live_n = _as_int(live.get("count"))
    toxicity_roi = _as_float(row.get("toxicity_roi_pct"))
    if deny_rule.startswith("signals_") and live_n > 0 and (live_roi or 0.0) > 0.0 and (toxicity_roi or 0.0) > 0.0:
        return "SIGNALS_DENY_BUT_LIVE_POSITIVE"
    return "OK_DENY_EVIDENCE"


def _line(row: dict[str, Any]) -> str:
    live = row["live_fills"]
    signals = row["all_signals"]
    return (
        f"wallet={row['source_wallet']} bucket={row['price_bucket']} "
        f"blocked_usd_this_cycle={row['blocked_usd_this_cycle']:.6f} "
        f"blocked_intents={row['blocked_intents_this_cycle']} "
        f"live_fills_roi_pct={live['roi_pct']} live_fills_n={live['count']} "
        f"all_signals_roi_pct={signals['roi_pct']} all_signals_n={signals['count']} "
        f"toxicity_roi_pct={row['toxicity_roi_pct']} deny_rule={row['deny_rule']} flag={row['flag']}"
    )


def build_report(
    *,
    toxicity_report: dict[str, Any],
    denylist: dict[str, Any],
    guard_state: dict[str, Any],
) -> dict[str, Any]:
    wallets = _active_wallets(guard_state)
    groups = _group_index(toxicity_report)
    blocked = _blocked_index(guard_state, wallets)
    cells = denylist.get("cells") if isinstance(denylist.get("cells"), list) else []
    rows: list[dict[str, Any]] = []
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        wallet = str(cell.get("source_wallet") or "").lower()
        bucket = str(cell.get("price_bucket") or "")
        if wallet not in wallets or not bucket:
            continue
        source = groups.get((wallet, bucket), cell)
        blocked_cell = blocked.get((wallet, bucket), {})
        row = {
            "source_wallet": wallet,
            "price_bucket": bucket,
            "deny_rule": cell.get("deny_rule"),
            "reason": cell.get("reason"),
            "blocked_usd_this_cycle": float(blocked_cell.get("blocked_usd_this_cycle") or 0.0),
            "blocked_intents_this_cycle": int(blocked_cell.get("blocked_intents_this_cycle") or 0),
            "sample_intents": list(blocked_cell.get("sample_intents") or [])[:5],
            "sample_markets": sorted(set(blocked_cell.get("sample_markets") or []))[:5],
            "live_fills": _metric(source, "live_fills"),
            "all_signals": _metric(source, "all_signals"),
            "toxicity_roi_pct": _as_float(source.get("toxicity_roi_pct")),
        }
        row["flag"] = _flag(row)
        row["line"] = _line(row)
        rows.append(row)
    rows.sort(key=lambda row: (-float(row["blocked_usd_this_cycle"]), row["source_wallet"], row["price_bucket"]))
    return {
        "schema_version": 1,
        "kind": "wallet_copy_toxicity_deny_cell_counterfactual",
        "flow_stage": "LIVE/LEARN",
        "generated_at": utc_now_iso(),
        "source_report_generated_at": toxicity_report.get("generated_at"),
        "denylist_generated_at": denylist.get("generated_at"),
        "active_wallet_count": len(wallets),
        "denied_active_cells": len(rows),
        "positive_live_measure_flags": sum(1 for row in rows if row.get("flag") == "SIGNALS_DENY_BUT_LIVE_POSITIVE"),
        "rows": rows,
        "line_table": [row["line"] for row in rows],
        "next": "send with 01:39Z fast-feed page packet; no toxicity denylist edits before Fable ruling",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--toxicity-report", default="data/research/wallet_copy_fill_toxicity_latest.json")
    parser.add_argument("--denylist", default="configs/wallet_copy/toxicity_denylist.json")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--output", default="data/research/wallet_copy_toxicity_deny_cell_counterfactual_latest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_report(
        toxicity_report=_load_json(args.toxicity_report, {}),
        denylist=_load_json(args.denylist, {}),
        guard_state=_load_json(args.guard_state, {}),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
