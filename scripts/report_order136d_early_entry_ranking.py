#!/usr/bin/env python3
"""Rank observed BTC-5m source wallets by source-time early-entry rate."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json


DEFAULT_GUARD_EVENTS = "data/research/wallet_copy_live_guard_wallet_events.jsonl"
DEFAULT_WATCH_EVENTS = "data/research/wallet_copy_watch_tier_wallet_events.jsonl"
DEFAULT_DEADMAN = "data/research/order_flow_deadman_state.json"
DEFAULT_TEMPORAL = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_LIVE_GUARD = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = "data/research/order136d_early_entry_ranking_latest.json"
POOL_PATH = (
    "policy_choke_fire_drill",
    "rung_c_full_pool_liveness_drill",
    "candidate_evidence",
    "rows",
)
BTC5M_SLUG = re.compile(r"^btc-updown-5m-(\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-events", default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--watch-events", default=DEFAULT_WATCH_EVENTS)
    parser.add_argument("--deadman", default=DEFAULT_DEADMAN)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL)
    parser.add_argument("--live-guard", default=DEFAULT_LIVE_GUARD)
    parser.add_argument("--lead-probe", default="", help="Fresh one-wallet liveness probe JSON")
    parser.add_argument("--lead-wallet", default="", help="Wallet whose F1-F4 evidence is reported")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--wallet", default="", help="Optional single-wallet first-pass filter")
    parser.add_argument("--min-n", type=int, default=20)
    parser.add_argument("--early-before-s", type=float, default=60.0)
    parser.add_argument("--recency-days", type=float, default=7.0)
    parser.add_argument("--now", default="", help="UTC ISO timestamp; defaults to current UTC")
    return parser.parse_args()


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _parse_now(value: str) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _pool_wallets(deadman: dict[str, Any]) -> set[str]:
    node: Any = deadman
    for key in POOL_PATH:
        node = node.get(key) if isinstance(node, dict) else None
    return {
        wallet
        for row in node or []
        if isinstance(row, dict) and (wallet := _wallet(row.get("wallet")))
    }


def _lead_gate_verification(
    *,
    wallet: str,
    now: datetime,
    temporal: dict[str, Any],
    probe: dict[str, Any],
    live_guard: dict[str, Any],
    deadman: dict[str, Any],
) -> dict[str, Any] | None:
    wallet = _wallet(wallet)
    if not wallet:
        return None
    temporal_row = next(
        (row for row in temporal.get("wallets") or [] if _wallet(row.get("wallet")) == wallet),
        {},
    )
    probe_row = next(
        (row for row in probe.get("rows") or [] if _wallet(row.get("wallet")) == wallet),
        {},
    )
    regime = "weekend" if now.weekday() >= 5 else "weekday"
    regime_row = (temporal_row.get("regime_profiles") or {}).get(regime) or {}
    venue_row = ((temporal_row.get("venue_executable") or {}).get("regime_profiles") or {}).get(regime)
    if not venue_row:
        venue_row = (temporal_row.get("venue_executable") or {}).get(regime) or {}
    active_wallets = {
        _wallet(member.get("source_wallet") or member.get("wallet"))
        for member in ((live_guard.get("active_set") or {}).get("members") or [])
        if isinstance(member, dict) and member.get("enabled") is not False
    }
    cooloffs = deadman.get("policy_choke_rung_b_cooloffs") or {}
    cooloff_active = wallet in cooloffs and bool(cooloffs.get(wallet))
    fading = str(temporal_row.get("classification") or "").upper() == "FADING"
    f1 = bool(
        int(regime_row.get("resolved_trades") or 0) >= 200
        and float(regime_row.get("pnl_usd") or 0.0) > 0
        and float(regime_row.get("roi_pct") or 0.0) > 0
        and int(venue_row.get("resolved_trades") or 0) >= 200
        and float(venue_row.get("pnl_usd") or 0.0) > 0
        and float(venue_row.get("roi_pct") or 0.0) > 0
    )
    fresh_30m = int(probe_row.get("btc5m_buys_30m") or 0)
    compatible_24h = int(probe_row.get("policy_compatible_inband_buy_rows_24h") or 0)
    age_h = probe_row.get("latest_trade_age_h")
    f2 = bool(fresh_30m >= 10 and compatible_24h > 0)
    f3 = bool(wallet not in active_wallets and not cooloff_active and not fading)
    f4 = bool(probe_row.get("status") == "PASS" and age_h is not None and float(age_h) <= 24.0)
    return {
        "wallet": wallet,
        "regime": regime,
        "eligible": all((f1, f2, f3, f4)),
        "checks": {
            "f1_measured_positive_regime_cell": f1,
            "f2_fresh_rows_and_own_policy_copyable": f2,
            "f3_not_enabled_or_cooloff_or_fading": f3,
            "f4_external_liveness": f4,
        },
        "evidence": {
            "regime": {
                "resolved_trades": regime_row.get("resolved_trades"),
                "pnl_usd": regime_row.get("pnl_usd"),
                "roi_pct": regime_row.get("roi_pct"),
            },
            "venue_executable_regime": {
                "resolved_trades": venue_row.get("resolved_trades"),
                "pnl_usd": venue_row.get("pnl_usd"),
                "roi_pct": venue_row.get("roi_pct"),
            },
            "source": {
                "btc5m_buys_30m": fresh_30m,
                "policy_compatible_inband_buy_rows_24h": compatible_24h,
                "latest_trade_age_h": age_h,
                "probe_generated_at": probe.get("generated_at"),
            },
            "f3": {
                "active": wallet in active_wallets,
                "cooloff_active": cooloff_active,
                "temporal_classification": temporal_row.get("classification"),
            },
        },
        "rule": "F1 current-regime and venue-executable n>=200/PnL>0/ROI>0; F2 fresh BUYs>=10 plus policy-compatible rows>0; F3 inactive/no-cooloff/not-FADING; F4 external age<=24h",
    }


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                yield row


def _new_stats() -> dict[str, Any]:
    return {"n": 0, "early": 0, "discarded": 0, "first": None, "last": None}


def _add(stats: dict[str, Any], *, event_ts: float, valid: bool, early: bool) -> None:
    if not valid:
        stats["discarded"] += 1
        return
    stats["n"] += 1
    stats["early"] += int(early)
    stats["first"] = event_ts if stats["first"] is None else min(stats["first"], event_ts)
    stats["last"] = event_ts if stats["last"] is None else max(stats["last"], event_ts)


def _table(
    stats: dict[tuple[str, str], dict[str, Any]],
    *,
    pool: set[str],
    min_n: int,
) -> list[dict[str, Any]]:
    rows = []
    for (wallet, stream), value in stats.items():
        n = int(value["n"])
        if n < min_n:
            continue
        rows.append(
            {
                "wallet": wallet,
                "n": n,
                "early_entry_rate": round(int(value["early"]) / n, 9),
                "discarded": int(value["discarded"]),
                "stream": stream,
                "in_134_pool": wallet in pool,
                "first_event_ts": _iso(value["first"]),
                "last_event_ts": _iso(value["last"]),
            }
        )
    return sorted(rows, key=lambda row: (-row["early_entry_rate"], -row["n"], row["wallet"], row["stream"]))


def build_report(
    *,
    streams: list[tuple[str, Path]],
    deadman: dict[str, Any],
    now: datetime,
    wallet_filter: str = "",
    min_n: int = 20,
    early_before_s: float = 60.0,
    recency_days: float = 7.0,
    lead_gate_verification: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pool = _pool_wallets(deadman)
    selected_wallet = _wallet(wallet_filter)
    cutoff = (now - timedelta(days=recency_days)).timestamp()
    now_s = now.timestamp()
    all_stats: dict[tuple[str, str], dict[str, Any]] = defaultdict(_new_stats)
    recent_stats: dict[tuple[str, str], dict[str, Any]] = defaultdict(_new_stats)
    observed_wallets: set[str] = set()
    input_counts: dict[str, dict[str, int]] = {}

    for stream, path in streams:
        counts = {"rows_read": 0, "filtered_rows": 0, "invalid_json_or_schema": 0}
        for row in _iter_jsonl(path):
            counts["rows_read"] += 1
            wallet = _wallet(row.get("source_wallet"))
            if selected_wallet and wallet != selected_wallet:
                continue
            if (
                not wallet
                or str(row.get("row_type") or "").lower() != "trade"
                or str(row.get("action") or "").upper() != "BUY"
                or str(row.get("duration") or "").lower() != "5m"
                or str(row.get("asset") or "").upper() != "BTC"
            ):
                continue
            counts["filtered_rows"] += 1
            match = BTC5M_SLUG.match(str(row.get("market_slug") or ""))
            try:
                event_ts = float(row.get("event_ts"))
            except (TypeError, ValueError):
                event_ts = 0.0
            if not match or event_ts <= 0:
                counts["invalid_json_or_schema"] += 1
                continue
            observed_wallets.add(wallet)
            window_time_s = event_ts - float(match.group(1))
            valid = 0.0 <= window_time_s < 300.0
            key = (wallet, stream)
            _add(all_stats[key], event_ts=event_ts, valid=valid, early=window_time_s < early_before_s)
            if cutoff <= event_ts <= now_s:
                _add(recent_stats[key], event_ts=event_ts, valid=valid, early=window_time_s < early_before_s)
        input_counts[stream] = counts

    intersection = observed_wallets & pool
    return {
        "schema_version": 1,
        "kind": "order136d_observed_universe_early_entry_ranking",
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "flow_stage": "DISCOVER/LEARN",
        "direction_id": "2026-08-01T14:57Z-fable-order136-d-amended",
        "report_only": True,
        "quality_bars_changed": False,
        "formula": "window_time_s = event_ts - integer suffix of btc-updown-5m-<epoch>",
        "filters": {
            "row_type": "trade",
            "action": "BUY",
            "duration": "5m",
            "asset": "BTC",
            "valid_window_time_s": "0 <= window_time_s < 300",
            "early_entry": f"window_time_s < {early_before_s:g}",
            "min_n": min_n,
            "wallet": selected_wallet or None,
        },
        "recency": {"days": recency_days, "cutoff": _iso(cutoff), "through": _iso(now_s)},
        "inputs": {name: {"path": str(path), **input_counts[name]} for name, path in streams},
        "pool_coverage": {
            "pool_wallets": len(pool),
            "observed_wallets": len(observed_wallets),
            "observed_intersection_pool": len(intersection),
            "coverage_ratio": round(len(intersection) / len(pool), 9) if pool else None,
            "intersection_wallets": sorted(intersection),
        },
        "lead_gate_verification": lead_gate_verification,
        "all_time": {"rows": _table(all_stats, pool=pool, min_n=min_n)},
        "last_7d": {"rows": _table(recent_stats, pool=pool, min_n=min_n)},
    }


def main() -> int:
    args = parse_args()
    now = _parse_now(args.now)
    deadman = load_json(args.deadman, default={})
    lead_wallet = args.lead_wallet or args.wallet
    lead_verification = _lead_gate_verification(
        wallet=lead_wallet,
        now=now,
        temporal=load_json(args.temporal, default={}),
        probe=load_json(args.lead_probe, default={}) if args.lead_probe else {},
        live_guard=load_json(args.live_guard, default={}),
        deadman=deadman,
    )
    report = build_report(
        streams=[("guard", Path(args.guard_events)), ("watch_tier", Path(args.watch_events))],
        deadman=deadman,
        now=now,
        wallet_filter=args.wallet,
        min_n=max(1, args.min_n),
        early_before_s=args.early_before_s,
        recency_days=args.recency_days,
        lead_gate_verification=lead_verification,
    )
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, "pool_coverage": report["pool_coverage"], "all_time_rows": len(report["all_time"]["rows"]), "last_7d_rows": len(report["last_7d"]["rows"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
