#!/usr/bin/env python3
"""Measure whether the effective stake cap makes maker orders impossible."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/maker_min_share_cap_choke_latest.json"
ERROR_CLASS = "maker_min_share_bump_exceeds_policy_cap"


def _parse_ts(value: Any) -> dt.datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _error_detail(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        if value.get("error_class") == ERROR_CLASS:
            return value
        for child in value.values():
            found = _error_detail(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _error_detail(child)
            if found is not None:
                return found
    return None


def build_report(ledger: dict[str, Any], *, day: dt.date) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for order in ledger.get("orders", []):
        if not isinstance(order, dict):
            continue
        observed_at = _parse_ts(order.get("updated_at") or order.get("ts"))
        if observed_at is None or observed_at.date() != day:
            continue
        detail = _error_detail(order)
        if detail is None:
            continue
        required = float(detail.get("maker_min_share_bump_cost_usd") or 0.0)
        cap = float(detail.get("maker_min_share_effective_cap_usd") or 0.0)
        rows.append(
            {
                "intent_id": order.get("intent_id"),
                "market_slug": order.get("market_slug"),
                "observed_at": observed_at.isoformat(),
                "source_wallet": str(
                    (order.get("alternate_transport_attribution") or {}).get("source_wallet")
                    or ""
                ).lower(),
                "limit_price": detail.get("entry_price") or order.get("limit_price"),
                "maker_min_shares": 5.0,
                "required_notional_usd": round(required, 6),
                "effective_cap_usd": round(cap, 6),
                "required_minus_cap_usd": round(required - cap, 6),
                "cap_forecloses_maker": required > cap + 1e-9,
                "venue_order_id": str(detail.get("order_id") or ""),
                "eligibility_basis": "reached_live_executor_after_guard",
            }
        )
    rows.sort(key=lambda row: (str(row["observed_at"]), str(row["intent_id"])))
    by_wallet: dict[str, dict[str, Any]] = {}
    for row in rows:
        wallet = str(row["source_wallet"])
        summary = by_wallet.setdefault(
            wallet,
            {
                "eligible_intents": 0,
                "maker_foreclosed_intents": 0,
                "empty_venue_order_ids": 0,
                "required_notional_min_usd": None,
                "required_notional_max_usd": None,
            },
        )
        summary["eligible_intents"] += 1
        summary["maker_foreclosed_intents"] += int(bool(row["cap_forecloses_maker"]))
        summary["empty_venue_order_ids"] += int(not row["venue_order_id"])
        required = float(row["required_notional_usd"])
        current_min = summary["required_notional_min_usd"]
        current_max = summary["required_notional_max_usd"]
        summary["required_notional_min_usd"] = required if current_min is None else min(current_min, required)
        summary["required_notional_max_usd"] = required if current_max is None else max(current_max, required)
    all_foreclosed = bool(rows) and all(bool(row["cap_forecloses_maker"]) for row in rows)
    return {
        "schema_version": 1,
        "kind": "maker_min_share_cap_choke",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "day_utc": day.isoformat(),
        "flow_stage": "LIVE/MEASURE",
        "summary": {
            "eligible_maker_intents": len(rows),
            "maker_foreclosed_intents": sum(bool(row["cap_forecloses_maker"]) for row in rows),
            "empty_venue_order_ids": sum(not row["venue_order_id"] for row in rows),
            "all_observed_eligible_maker_intents_foreclosed": all_foreclosed,
            "verdict": "TAKER_ONLY_OR_NO_LANE_AT_EFFECTIVE_CAP" if all_foreclosed else "MAKER_REMAINS_ARITHMETICALLY_REACHABLE",
        },
        "by_wallet": by_wallet,
        "rows": rows,
        "rule": "A maker intent is arithmetically foreclosed when 5 * limit_price exceeds the effective policy cap; empty venue_order_id proves local refusal before venue submission.",
        "live_path_mutated": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--day", default=dt.datetime.now(dt.timezone.utc).date().isoformat())
    args = parser.parse_args()
    report = build_report(json.loads(args.ledger.read_text()), day=dt.date.fromisoformat(args.day))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
