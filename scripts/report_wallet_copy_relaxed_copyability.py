#!/usr/bin/env python3
"""Measure paper-only relaxed copyability profiles from live-tracker states.

This report is diagnostic only. It does not change admission gates or live
execution behavior; it quantifies whether stale/slippage blockers would clear
under named paper-only profiles.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


DEFAULT_STATES = [
    "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
    "data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
    "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    "data/research/wallet_copy_registry_sweep_live_tracking_state.json",
    "data/research/wallet_copy_live_tracking_state.json",
]


DEFAULT_PROFILES = [
    "strict_live:10:150:0.999",
    "paper_30s_500bps:30:500:0.999",
    "paper_30s_1000bps:30:1000:0.999",
    "paper_60s_1500bps:60:1500:0.999",
    "paper_120s_2000bps:120:2000:0.999",
]


def _profile(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in str(raw).split(":")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("profile must be name:max_age_s:max_slippage_bps:min_fill_ratio")
    name, max_age, max_slippage, min_fill_ratio = parts
    if not name:
        raise argparse.ArgumentTypeError("profile name must be non-empty")
    return {
        "name": name,
        "max_event_age_s": float(max_age),
        "max_slippage_bps": float(max_slippage),
        "min_clob_fill_ratio": float(min_fill_ratio),
    }


def _nested(mapping: dict[str, Any], *keys: str) -> Any:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _event_scores(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _nested(state, "summary", "copy_efficiency", "current_poll", "event_scores")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _min_slippage_to_fill_bps(row: dict[str, Any]) -> float | None:
    details = row.get("copyability_details") if isinstance(row.get("copyability_details"), dict) else {}
    value = details.get("min_slippage_to_fill_bps")
    if value is not None:
        return num(value, -1.0)
    best_ask = num(row.get("clob_best_ask"), 0.0)
    source_price = num(row.get("source_price"), 0.0)
    if best_ask <= 0 or source_price <= 0:
        return None
    return max(0.0, (best_ask / source_price - 1.0) * 10_000.0)


def _copyability_profile_decision(row: dict[str, Any], profile: dict[str, Any]) -> tuple[str, str | None]:
    details = row.get("copyability_details") if isinstance(row.get("copyability_details"), dict) else {}
    if str(row.get("wallet_action") or "").upper() != "BUY":
        return "not_buy", None
    if row.get("profit_policy_accepted") is False:
        return "profit_policy_rejected", None
    age_s = num(row.get("event_age_s"), -1.0)
    if age_s < 0:
        return "missing_event_age", None
    if age_s > float(profile["max_event_age_s"]):
        return "event_age_above_profile_cap", None
    if str(row.get("clob_book_status") or "") != "OK":
        return "book_missing_or_not_ok", None
    best_ask = num(row.get("clob_best_ask"), 0.0)
    if best_ask <= 0:
        return "no_ask_liquidity", None
    min_slippage = _min_slippage_to_fill_bps(row)
    if min_slippage is None:
        return "missing_slippage_estimate", None
    if min_slippage > float(profile["max_slippage_bps"]):
        return "best_ask_above_profile_slippage", None
    fill_ratio = num(row.get("clob_fill_ratio"), 0.0)
    if fill_ratio < float(profile["min_clob_fill_ratio"]):
        blocking_reason = str(
            row.get("clob_blocking_reason")
            or details.get("clob_blocking_reason")
            or ""
        )
        if blocking_reason == "price_above_slippage_cap":
            return "accepted", "relaxed_best_ask_from_min_slippage"
        return "depth_below_profile_min_fill_ratio", None
    return "accepted", "strict_or_cached_fill_ratio"


def _summarize_path(path: Path, profiles: list[dict[str, Any]]) -> dict[str, Any]:
    state = load_json(path, default={})
    if not isinstance(state, dict):
        state = {}
    rows = _event_scores(state)
    buy_rows = [row for row in rows if str(row.get("wallet_action") or "").upper() == "BUY"]
    profit_buy_rows = [row for row in buy_rows if row.get("profit_policy_accepted") is not False]
    by_profile = []
    for profile in profiles:
        reason_counts: Counter[str] = Counter()
        acceptance_basis_counts: Counter[str] = Counter()
        accepted_rows: list[dict[str, Any]] = []
        for row in profit_buy_rows:
            reason, acceptance_basis = _copyability_profile_decision(row, profile)
            reason_counts[reason] += 1
            if reason == "accepted":
                accepted_rows.append(row)
                acceptance_basis_counts[str(acceptance_basis or "accepted")] += 1
        ages = [num(row.get("event_age_s"), -1.0) for row in accepted_rows if num(row.get("event_age_s"), -1.0) >= 0]
        slippages = [
            value
            for row in accepted_rows
            for value in [_min_slippage_to_fill_bps(row)]
            if value is not None and value >= 0
        ]
        by_profile.append(
            {
                **profile,
                "accepted_buy_rows": len(accepted_rows),
                "accept_rate_pct": round((len(accepted_rows) / len(profit_buy_rows) * 100.0), 6)
                if profit_buy_rows
                else 0.0,
                "reject_reason_counts": dict(sorted(reason_counts.items())),
                "acceptance_basis_counts": dict(sorted(acceptance_basis_counts.items())),
                "accepted_event_age_max_s": round(max(ages), 6) if ages else None,
                "accepted_min_slippage_to_fill_bps_max": round(max(slippages), 6) if slippages else None,
            }
        )
    current_ladder = _nested(state, "summary", "current_poll_diagnostics", "current_poll_ladder") or _nested(
        state,
        "summary",
        "copy_efficiency",
        "current_poll",
        "ladder",
    )
    return {
        "path": str(path),
        "generated_at": state.get("generated_at"),
        "raw_event_scores": len(rows),
        "buy_rows": len(buy_rows),
        "profit_policy_buy_rows": len(profit_buy_rows),
        "current_poll_ladder": current_ladder if isinstance(current_ladder, dict) else {},
        "profiles": by_profile,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", action="append", default=[], help="Live-tracker state path to inspect.")
    parser.add_argument("--profile", action="append", default=[], help="name:max_age_s:max_slippage_bps:min_fill_ratio")
    parser.add_argument(
        "--output",
        default="data/research/wallet_copy_relaxed_copyability_report.json",
        help="Where to write the diagnostic report.",
    )
    args = parser.parse_args()

    profiles = [_profile(raw) for raw in (args.profile or DEFAULT_PROFILES)]
    paths = [Path(raw) for raw in (args.state or DEFAULT_STATES)]
    states = [_summarize_path(path, profiles) for path in paths if path.exists()]
    aggregate_profiles = []
    for profile in profiles:
        accepted = 0
        total = 0
        reasons: Counter[str] = Counter()
        acceptance_basis: Counter[str] = Counter()
        for state in states:
            total += int(state.get("profit_policy_buy_rows") or 0)
            match = next((row for row in state["profiles"] if row["name"] == profile["name"]), None)
            if not match:
                continue
            accepted += int(match.get("accepted_buy_rows") or 0)
            reasons.update(match.get("reject_reason_counts") or {})
            acceptance_basis.update(match.get("acceptance_basis_counts") or {})
        aggregate_profiles.append(
            {
                **profile,
                "accepted_buy_rows": accepted,
                "profit_policy_buy_rows": total,
                "accept_rate_pct": round((accepted / total * 100.0), 6) if total else 0.0,
                "reject_reason_counts": dict(sorted(reasons.items())),
                "acceptance_basis_counts": dict(sorted(acceptance_basis.items())),
            }
        )
    payload = {
        "generated_at": utc_now_iso(),
        "status": "PASS" if any(row["accepted_buy_rows"] for row in aggregate_profiles) else "ANALYZE",
        "paper_only": True,
        "live_orders_allowed": False,
        "role": "paper_only_relaxed_copyability_measurement_not_live_admission",
        "profiles": aggregate_profiles,
        "states": states,
    }
    atomic_write_json(args.output, payload)
    print(__import__("json").dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
