#!/usr/bin/env python3
"""Report a689 fit-vs-bind split under the exchange 5-share minimum."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


A689_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/a689_fit_vs_bind_split_latest.json"
DEFAULT_WINDOW_BUDGET_USD = 2.0
EXCHANGE_MIN_SHARES = 5.0


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = (len(sorted_values) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (index - lower)


def _numeric_summary(values: Iterable[float]) -> dict[str, Any]:
    cleaned = sorted(float(v) for v in values if v is not None and float(v) > 0)
    if not cleaned:
        return {"count": 0}
    return {
        "count": len(cleaned),
        "min": _round(cleaned[0]),
        "p50": _round(_percentile(cleaned, 0.50)),
        "p90": _round(_percentile(cleaned, 0.90)),
        "max": _round(cleaned[-1]),
        "avg": _round(sum(cleaned) / len(cleaned)),
    }


def _participation_rows(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    participation = guard_state.get("window_participation")
    participation = participation if isinstance(participation, dict) else {}
    rows = participation.get("rows")
    if not isinstance(rows, list):
        rows = participation.get("window_rollups")
    return [row for row in rows or [] if isinstance(row, dict)]


def _row_ts(row: dict[str, Any]) -> float:
    return (
        num(row.get("effective_latest_observed_ts"))
        or num(row.get("latest_observed_ts"))
        or num(row.get("source_detection_observed_ts"))
        or num(row.get("alternate_observed_ts"))
        or num(row.get("window_start_s"))
    )


def _ts_to_iso(ts: float) -> str | None:
    if ts <= 0:
        return None
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _price_for_row(row: dict[str, Any]) -> float:
    for key in ("source_inventory_vwap", "inventory_best_ask", "best_ask", "limit_price"):
        value = num(row.get(key))
        if value > 0:
            return value
    target_usd = num(row.get("target_usd_at_vwap"))
    target_shares = num(row.get("target_shares"))
    if target_usd > 0 and target_shares > 0:
        return target_usd / target_shares
    effective_min = num(row.get("effective_min_tranche_usd"))
    if effective_min > 0:
        return effective_min / EXCHANGE_MIN_SHARES
    return 0.0


def _a689_rows(guard_state: dict[str, Any], *, lookback_start_ts: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _participation_rows(guard_state):
        if str(raw.get("source_wallet") or "").lower() != A689_WALLET:
            continue
        if _row_ts(raw) < lookback_start_ts:
            continue
        market_slug = str(raw.get("market_slug") or "")
        outcome = str(raw.get("outcome") or "")
        if not market_slug.startswith("btc-updown-5m-") or outcome not in {"Up", "Down"}:
            continue
        if int(raw.get("wallet_eligible_orders") or 0) <= 0:
            continue
        price = _price_for_row(raw)
        if price <= 0:
            continue
        observed_window_budget = num(raw.get("window_budget_usd")) or None
        window_budget = DEFAULT_WINDOW_BUDGET_USD
        exchange_min_notional = price * EXCHANGE_MIN_SHARES
        row = dict(raw)
        row["fit_vs_bind_price"] = round(price, 8)
        row["fit_vs_bind_observed_window_budget_usd"] = (
            round(observed_window_budget, 6) if observed_window_budget is not None else None
        )
        row["fit_vs_bind_window_budget_usd"] = round(window_budget, 6)
        row["fit_vs_bind_exchange_min_notional_usd"] = round(exchange_min_notional, 6)
        row["fit_under_window_budget"] = exchange_min_notional <= window_budget + 1e-9
        rows.append(row)
    return sorted(rows, key=lambda row: (_row_ts(row), row.get("market_slug") or "", row.get("outcome") or ""))


def build_report(*, guard_state_path: Path, lookback_hours: float) -> dict[str, Any]:
    guard_state = load_json(guard_state_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    generated_at = utc_now_iso()
    generated_ts = dt.datetime.fromisoformat(generated_at.replace("Z", "+00:00")).timestamp()
    lookback_start_ts = generated_ts - float(lookback_hours) * 3600.0
    rows = _a689_rows(guard_state, lookback_start_ts=lookback_start_ts)
    fit_rows = [row for row in rows if row.get("fit_under_window_budget")]
    bind_rows = [row for row in rows if not row.get("fit_under_window_budget")]
    prices = [num(row.get("fit_vs_bind_price")) for row in rows]
    min_notional = [num(row.get("fit_vs_bind_exchange_min_notional_usd")) for row in rows]
    distinct_windows = {int(num(row.get("window_start_s"))) for row in rows if num(row.get("window_start_s")) > 0}
    return {
        "schema_version": 1,
        "kind": "a689_fit_vs_bind_split",
        "flow_stage": "LIVE/MEASURE/DEFEND",
        "generated_at": generated_at,
        "source_of_truth": "docs/agents/HANDOFF.md 2026-07-16T06:42Z fable DIRECTION a689 corollary",
        "guard_state": _display(guard_state_path),
        "source_wallet": A689_WALLET,
        "lookback_hours": float(lookback_hours),
        "lookback_start_iso": _ts_to_iso(lookback_start_ts),
        "exchange_min_shares": EXCHANGE_MIN_SHARES,
        "window_budget_rule": "fit when 5 * entry_price <= fixed current a689 $2 budget; observed row window_budget_usd is retained only as context",
        "live_path_mutated": False,
        "summary": {
            "rows": len(rows),
            "distinct_windows": len(distinct_windows),
            "wallet_eligible_orders": sum(int(row.get("wallet_eligible_orders") or 0) for row in rows),
            "fit_rows": len(fit_rows),
            "bind_rows": len(bind_rows),
            "fit_ratio": round(len(fit_rows) / len(rows), 6) if rows else 0.0,
            "bind_ratio": round(len(bind_rows) / len(rows), 6) if rows else 0.0,
            "fit_wallet_eligible_orders": sum(int(row.get("wallet_eligible_orders") or 0) for row in fit_rows),
            "bind_wallet_eligible_orders": sum(int(row.get("wallet_eligible_orders") or 0) for row in bind_rows),
            "dominant_skip_reason_counts": dict(
                sorted(Counter(str(row.get("dominant_skip_reason") or "unknown") for row in rows).items())
            ),
        },
        "entry_price": _numeric_summary(prices),
        "exchange_min_notional_usd": _numeric_summary(min_notional),
        "sample_rows": [
            {
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "window_start_s": row.get("window_start_s"),
                "wallet_eligible_orders": row.get("wallet_eligible_orders"),
                "dominant_skip_reason": row.get("dominant_skip_reason"),
                "observed_window_budget_usd": row.get("fit_vs_bind_observed_window_budget_usd"),
                "window_budget_usd": row.get("fit_vs_bind_window_budget_usd"),
                "entry_price": row.get("fit_vs_bind_price"),
                "exchange_min_notional_usd": row.get("fit_vs_bind_exchange_min_notional_usd"),
                "fit_under_window_budget": row.get("fit_under_window_budget"),
            }
            for row in rows[:20]
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", type=Path, default=DEFAULT_GUARD_STATE)
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(guard_state_path=args.guard_state, lookback_hours=float(args.lookback_hours))
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
