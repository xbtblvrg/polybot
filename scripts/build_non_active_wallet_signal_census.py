#!/usr/bin/env python3
"""Build AM's non-active-wallet signal census for empty active-set windows.

Flow stage: PROMOTE/ROTATE/LIVE. This is a paper-only research report: it
reads the AL opportunity census and RTDS capture, then ranks tracked wallets
outside the current active set that fired in windows classified
NO_SOURCE_SIGNAL for the active set. It never submits orders or mutates live
configuration.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_active_set_expansion_shortlist import (  # noqa: E402
    _active_set_wallets,
    _leaderboard_pnl,
    _registry_wallets,
)
from scripts.run_whale_consensus_paper_lane import (  # noqa: E402
    DEFAULT_RTDS_JSONL,
    _feed_event_from_rtds,
    _iter_recent_jsonl,
)
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_CENSUS = "data/research/wallet_copy_elapsed_window_opportunity_census_2026-07-06.json"
DEFAULT_ALPHA = "data/research/alpha_decay_report.json"
DEFAULT_REGISTRY = "configs/wallet_copy/wallets.json"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_GUARD = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = "data/research/non_active_wallet_signal_census_2026-07-06.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--opportunity-census", default=DEFAULT_CENSUS)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--wallet-registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY)
    parser.add_argument("--alpha-decay-report", default=DEFAULT_ALPHA)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--scan-limit", type=int, default=500_000)
    parser.add_argument("--scan-max-bytes", type=int, default=1_073_741_824)
    parser.add_argument("--top-n", type=int, default=25)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _history_wallets(history_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rows = history_state.get("wallet_results") if isinstance(history_state.get("wallet_results"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet_row = row.get("wallet") if isinstance(row.get("wallet"), dict) else {}
        wallet = _norm_wallet(wallet_row.get("address"))
        if wallet:
            out[wallet] = {
                "history_events": int(row.get("events") or 0),
                "history_copy_intents": int(row.get("copy_intents") or 0),
                "history_latest_event_ts": num(row.get("latest_event_ts")),
            }
    return out


def _no_signal_windows(opportunity_census: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = opportunity_census.get("rows") if isinstance(opportunity_census.get("rows"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("classification") != "NO_SOURCE_SIGNAL":
            continue
        slug = str(row.get("market_slug") or "")
        if slug:
            out[slug] = row
    return out


def _profiles_by_wallet(alpha_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    execution = alpha_report.get("execution_profiles") if isinstance(alpha_report.get("execution_profiles"), dict) else {}
    profiles = execution.get("profiles_by_wallet") if isinstance(execution.get("profiles_by_wallet"), dict) else {}
    return {_norm_wallet(wallet): profile for wallet, profile in profiles.items() if _norm_wallet(wallet) and isinstance(profile, dict)}


def _event_rows_for_output(window_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for slug, entry in sorted(window_rows.items(), key=lambda item: num(item[1].get("window_start_s"))):
        rows.append(
            {
                "market_slug": slug,
                "window_start_s": entry.get("window_start_s"),
                "buy_events": entry.get("buy_events"),
                "source_usd": round(num(entry.get("source_usd")), 6),
                "outcomes": sorted(entry.get("outcomes") or []),
                "min_price": entry.get("min_price"),
                "max_price": entry.get("max_price"),
            }
        )
    return rows


def build_report(
    *,
    rtds_rows: list[dict[str, Any]],
    opportunity_census: dict[str, Any],
    registry: dict[str, Any],
    history_state: dict[str, Any],
    alpha_report: dict[str, Any],
    guard_state: dict[str, Any],
    top_n: int = 25,
    diagnostics: dict[str, int] | None = None,
) -> dict[str, Any]:
    no_signal = _no_signal_windows(opportunity_census)
    registry_by_wallet = _registry_wallets(registry)
    history_by_wallet = _history_wallets(history_state)
    tracked_wallets = set(registry_by_wallet) | set(history_by_wallet)
    active_wallets = _active_set_wallets(guard_state)
    profiles = _profiles_by_wallet(alpha_report)
    counts: Counter[str] = Counter(diagnostics or {})
    by_wallet: dict[str, dict[str, Any]] = {}

    for row in rtds_rows:
        event = _feed_event_from_rtds(row)
        if event is None:
            counts["not_btc5m_trade"] += 1
            continue
        if event.side != "BUY":
            counts["non_buy_trade"] += 1
            continue
        if event.market_slug not in no_signal:
            counts["outside_no_signal_window"] += 1
            continue
        wallet = _norm_wallet(event.source_wallet)
        if not wallet:
            counts["missing_wallet"] += 1
            continue
        if wallet in active_wallets:
            counts["active_set_wallet"] += 1
            continue
        if wallet not in tracked_wallets:
            counts["untracked_wallet"] += 1
            continue
        entry = by_wallet.setdefault(
            wallet,
            {
                "wallet": wallet,
                "buy_events": 0,
                "source_usd": 0.0,
                "first_event_ts": event.event_ts,
                "latest_event_ts": event.event_ts,
                "windows": {},
            },
        )
        entry["buy_events"] += 1
        entry["source_usd"] += event.source_usd
        entry["first_event_ts"] = min(num(entry["first_event_ts"]), event.event_ts)
        entry["latest_event_ts"] = max(num(entry["latest_event_ts"]), event.event_ts)
        window = entry["windows"].setdefault(
            event.market_slug,
            {
                "market_slug": event.market_slug,
                "window_start_s": event.window_start_s,
                "buy_events": 0,
                "source_usd": 0.0,
                "outcomes": set(),
                "min_price": event.price,
                "max_price": event.price,
            },
        )
        window["buy_events"] += 1
        window["source_usd"] += event.source_usd
        window["outcomes"].add(event.outcome)
        window["min_price"] = min(num(window["min_price"]), event.price)
        window["max_price"] = max(num(window["max_price"]), event.price)
        counts["matched_non_active_events"] += 1

    rows: list[dict[str, Any]] = []
    for wallet, entry in by_wallet.items():
        registry_row = registry_by_wallet.get(wallet, {})
        history_row = history_by_wallet.get(wallet, {})
        profile = profiles.get(wallet, {})
        pnl = _leaderboard_pnl(registry_row) if registry_row else {"resolved_pnl": 0.0, "source": "missing", "period": "missing", "rank": None, "volume": None}
        windows = entry["windows"]
        alpha_eligible = bool(profile.get("eligible"))
        resolved_pnl = num(pnl.get("resolved_pnl"))
        meets_existing_bar_hint = alpha_eligible and resolved_pnl > 0.0
        rows.append(
            {
                "wallet": wallet,
                "name": registry_row.get("name") or "",
                "source_active_windows_in_empty_set": len(windows),
                "buy_events_in_empty_set": int(entry["buy_events"]),
                "source_usd_in_empty_set": round(num(entry["source_usd"]), 6),
                "first_event_iso": _iso_from_ts(num(entry["first_event_ts"])),
                "latest_event_iso": _iso_from_ts(num(entry["latest_event_ts"])),
                "resolved_pnl": resolved_pnl,
                "resolved_pnl_source": pnl.get("source"),
                "resolved_pnl_period": pnl.get("period"),
                "leaderboard_rank": pnl.get("rank"),
                "leaderboard_volume": pnl.get("volume"),
                "history_events": int(history_row.get("history_events") or 0),
                "history_copy_intents": int(history_row.get("history_copy_intents") or 0),
                "alpha_eligible": alpha_eligible,
                "alpha_status": profile.get("status"),
                "copyable_rate_pct": profile.get("copyable_rate_pct"),
                "fill_sample": int(profile.get("fill_sample") or 0),
                "eligible_move_slices": int(profile.get("eligible_move_slice_count") or 0),
                "mean_edge": profile.get("mean_edge"),
                "median_edge": profile.get("median_edge"),
                "meets_existing_bar_hint": meets_existing_bar_hint,
                "windows": _event_rows_for_output(windows)[:20],
            }
        )

    rows.sort(
        key=lambda row: (
            not bool(row.get("meets_existing_bar_hint")),
            -int(row.get("source_active_windows_in_empty_set") or 0),
            -int(row.get("buy_events_in_empty_set") or 0),
            -num(row.get("resolved_pnl")),
            str(row.get("wallet") or ""),
        )
    )
    top = rows[: max(1, int(top_n))]
    return {
        "schema_version": 1,
        "kind": "non_active_wallet_signal_census_v1",
        "flow_stage": "PROMOTE/ROTATE/LIVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "inputs": {
            "opportunity_census": DEFAULT_CENSUS,
            "rtds_jsonl": DEFAULT_RTDS_JSONL,
            "wallet_registry": DEFAULT_REGISTRY,
            "history_state": DEFAULT_HISTORY,
            "alpha_decay_report": DEFAULT_ALPHA,
            "guard_state": DEFAULT_GUARD,
        },
        "summary": {
            "no_source_signal_windows": len(no_signal),
            "tracked_wallets": len(tracked_wallets),
            "active_set_wallets": len(active_wallets),
            "non_active_wallets_seen": len(rows),
            "candidates_meeting_existing_bar_hint": sum(1 for row in rows if row.get("meets_existing_bar_hint")),
            "top_count": len(top),
            "diagnostics": dict(sorted(counts.items())),
        },
        "ranking": {
            "primary": "meets_existing_bar_hint_then_source_active_windows_desc",
            "tie_breakers": ["buy_events_desc", "resolved_pnl_desc"],
            "scope": "RTDS BUY events by tracked non-active wallets in AL NO_SOURCE_SIGNAL windows only",
        },
        "top_candidates": top,
        "candidate_rows": rows,
    }


def main() -> int:
    args = parse_args()
    alpha_report = load_json(args.alpha_decay_report, default={}) or {}
    require_fresh_alpha_report(alpha_report, path=args.alpha_decay_report)
    rows, diagnostics = _iter_recent_jsonl(args.rtds_jsonl, limit=int(args.scan_limit), max_bytes=int(args.scan_max_bytes))
    report = build_report(
        rtds_rows=rows,
        opportunity_census=load_json(args.opportunity_census, default={}) or {},
        registry=load_json(args.wallet_registry, default={}) or {},
        history_state=load_json(args.history_state, default={}) or {},
        alpha_report=alpha_report,
        guard_state=load_json(args.guard_state, default={}) or {},
        top_n=int(args.top_n),
        diagnostics={str(key): int(value) for key, value in diagnostics.items()},
    )
    report["inputs"].update(
        {
            "opportunity_census": args.opportunity_census,
            "rtds_jsonl": args.rtds_jsonl,
            "wallet_registry": args.wallet_registry,
            "history_state": args.history_state,
            "alpha_decay_report": args.alpha_decay_report,
            "guard_state": args.guard_state,
            "scan_limit": int(args.scan_limit),
            "scan_max_bytes": int(args.scan_max_bytes),
        }
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
