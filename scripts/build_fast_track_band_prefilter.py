#!/usr/bin/env python3
"""Build the RULING 6 band-compatible next-5 replacement shortlist.

Flow stage: PROMOTE/ROTATE/LEARN. This is paper-only measurement. It fetches
recent source-wallet BTC-5m BUY rows and filters candidates whose recent tail
is structurally outside the 0.25 live replacement band.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.ingest import WalletHistoryClient  # noqa: E402
from src.wallet_copy.models import WalletSpec, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_SHORTLIST = "data/research/active_set_expansion_shortlist.json"
DEFAULT_OUTPUT = "data/research/active_set_expansion_next5_band_prefilter_20260705_2012.json"
DEFAULT_EXCLUDED = (
    "0x960bf404c1eca257411203164357a925e33fae8d",
    "0x04a162e06d1e82745a08b95e247bf3a965693527",
)
SOURCE_ROUTE_ENV_VARS = (
    "POLYMARKET_DATA_API_BASE_URL",
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_CLOB_API_BASE_URL",
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shortlist", default=DEFAULT_SHORTLIST)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--max-candidates", type=int, default=40)
    parser.add_argument("--min-band-compatible-fraction", type=float, default=0.30)
    parser.add_argument("--max-price", type=float, default=0.25)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--exclude-wallet", action="append", default=list(DEFAULT_EXCLUDED))
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _candidate_rows(shortlist: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("top_candidates", "top", "pnl_only_no_lane_evidence"):
        value = shortlist.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    by_wallet: dict[str, dict[str, Any]] = {}
    for row in rows:
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if not wallet:
            continue
        prior = by_wallet.get(wallet)
        if prior is None or _num(row.get("resolved_pnl")) > _num(prior.get("resolved_pnl")):
            by_wallet[wallet] = row
    return sorted(
        by_wallet.values(),
        key=lambda row: (_num(row.get("resolved_pnl")), int(row.get("complementary_fills") or 0)),
        reverse=True,
    )


def _clear_source_overrides() -> list[str]:
    cleared: list[str] = []
    for env_var in SOURCE_ROUTE_ENV_VARS:
        if os.environ.get(env_var):
            cleared.append(env_var)
        if env_var in os.environ:
            os.environ[env_var] = ""
    return cleared


def _band_report(row: dict[str, Any], *, args: argparse.Namespace) -> dict[str, Any]:
    wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
    name = str(row.get("name") or f"candidate_{wallet[-10:]}")
    client = WalletHistoryClient(
        WalletSpec(name=name, address=wallet),
        timeout_s=float(args.timeout_s),
        retries=max(1, int(args.retries)),
    )
    error: dict[str, Any] | None = None
    events = []
    try:
        events = client.fetch_events(
            limit=max(1, int(args.limit)),
            pages=max(1, int(args.pages)),
            include_activity=False,
            parallel_sources=False,
            trade_query_keys=("user",),
        )
    except Exception as exc:  # noqa: BLE001 - artifact should record source route failures.
        error = {"type": type(exc).__name__, "message": str(exc)[:500]}
    buy_events = [event for event in events if event.is_buy]
    compatible = [event for event in buy_events if float(event.price) <= float(args.max_price)]
    fraction = (len(compatible) / len(buy_events)) if buy_events else 0.0
    return {
        "wallet": wallet,
        "name": name,
        "resolved_pnl": _num(row.get("resolved_pnl")),
        "resolved_pnl_source": row.get("resolved_pnl_source"),
        "resolved_pnl_period": row.get("resolved_pnl_period"),
        "complementary_fills": int(row.get("complementary_fills") or 0),
        "recent_fill_windows": int(row.get("recent_fill_windows") or 0),
        "recent_tail_buy_rows": len(buy_events),
        "band_compatible_buy_rows": len(compatible),
        "band_compatible_fraction": round(fraction, 6),
        "band_compatible_pct": round(fraction * 100.0, 3),
        "max_price": float(args.max_price),
        "passes_band_filter": fraction >= float(args.min_band_compatible_fraction),
        "latest_buy_event_ts": max((event.event_ts for event in buy_events if event.event_ts is not None), default=None),
        "price_sample": [round(float(event.price), 6) for event in buy_events[:12]],
        "route_status": client.last_fetch_report.get("source_route_status_by_source")
        if isinstance(client.last_fetch_report, dict)
        else {},
        "error": error,
        "source": "direct_data_api_user_trade_tail",
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    shortlist = load_json(args.shortlist, default={})
    if not isinstance(shortlist, dict):
        shortlist = {}
    excluded = {_norm_wallet(wallet) for wallet in args.exclude_wallet}
    excluded.discard("")
    cleared_env_vars = _clear_source_overrides()
    ranked = [
        row
        for row in _candidate_rows(shortlist)
        if _norm_wallet(row.get("wallet") or row.get("source_wallet")) not in excluded
    ]
    reports: list[dict[str, Any]] = []
    for row in ranked[: max(1, int(args.max_candidates))]:
        reports.append(_band_report(row, args=args))
        if sum(1 for report in reports if report.get("passes_band_filter")) >= int(args.top_n):
            break
    selected = [report for report in reports if report.get("passes_band_filter")][: max(1, int(args.top_n))]
    rejected = [report for report in reports if not report.get("passes_band_filter")]
    return {
        "schema_version": 1,
        "kind": "active_set_expansion_next5_band_prefilter",
        "flow_stage": "PROMOTE/ROTATE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source": str(args.shortlist),
        "ranking": "resolved_pnl_desc_then_band_compatibility",
        "excluded_wallets": sorted(excluded),
        "gate": {
            "top_n": int(args.top_n),
            "max_price": float(args.max_price),
            "min_band_compatible_fraction": float(args.min_band_compatible_fraction),
            "recent_tail_source": "Data API /trades user, BTC-5m BUY rows",
        },
        "selected_count": len(selected),
        "selected": selected,
        "rejected": rejected,
        "evaluated": reports,
        "diagnostics": {
            "candidate_pool_size": len(ranked),
            "evaluated_count": len(reports),
            "cleared_source_route_env_vars": cleared_env_vars,
            "status": "PASS" if len(selected) >= int(args.top_n) else "ANALYZE_INSUFFICIENT_BAND_COMPATIBLE_CANDIDATES",
        },
        "next_action": (
            "use selected as the zero-latency widening queue if both current fast-track candidates fail at 22:30Z"
            if len(selected) >= int(args.top_n)
            else "broaden candidate pool or relax only with Fable direction; do not promote from this artifact"
        ),
    }


def main() -> int:
    args = parse_args()
    payload = build(args)
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if int(payload.get("selected_count") or 0) >= int(args.top_n) else 2


if __name__ == "__main__":
    raise SystemExit(main())
